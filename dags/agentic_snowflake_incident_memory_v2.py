"""
## Agentic Snowflake Incident DAG v2 (platform + path routed, with incident memory)

A standalone variant of agentic_snowflake_incident_memory that routes both investigation (by
platform) and response (by severity) instead of using one fixed diagnose-then-ticket flow for
every failure. Does not replace or modify agentic_snowflake_incident_memory.py — that DAG is
untouched; this is a separate, independently triggerable pipeline used to prototype the routed
design.

Triggered automatically when a pipeline's on_failure_callback points at this DAG (see
include/novamart_utils.py:trigger_incident_dag_v2), or manually from the UI.

### Flow
1. gather_context           — pre-fetch failed task logs + pipeline source via the Airflow REST
                               API. Also reads the failing DAG's own tags to determine
                               is_critical (a "critical" tag on that DAG).
2. recall_prior_incidents   — recurrence detection (incident memory).
3. classify_platform        — @task.llm_branch: routes to investigate_aws / investigate_api /
                               investigate_snowflake based on the failure's exception text. This
                               decision is about *how to investigate* — which tailored instructions
                               (include/incident_instructions/<platform>.md) and which toolset apply
                               — not what action to take on the result.
4. investigate_<platform>   — @task.agent, platform-specific toolset:
                                 - snowflake: SQLToolset(db_conn_id="snowflake_default")
                                 - aws:       HookToolset over S3Hook (list_keys/read_key/etc.)
                                 - api:       no live tool yet — reasons from logs/source only
                               Only one of these three actually runs per DAG run; the other two are
                               skipped by classify_platform.
5. merge_diagnosis          — picks whichever of the three produced a diagnosis (trigger_rule=
                               none_failed_min_one_success, since two of three upstream are skipped
                               by design).
6. decide_path              — @task.llm_branch: routes to exactly one of:
                                 - propose_code_fix: root cause is a genuine bug in the pipeline's
                                   OWN code (not credentials/permissions/external-service) ->
                                   propose a corrected file -> open_pr opens a real GitHub PR as a
                                   draft -> notify_slack_fix posts the PR link, tagging the
                                   incident owner. Review happens on the PR itself (GitHub's own
                                   diff view), not via an Airflow-side approval gate.
                                 - urgent_slack_post: pipeline is critical AND the failure is NOT
                                   a code bug -> immediate tagged Slack alert, no ticket.
                                 - create_ticket_low_priority: not critical -> a normal-priority
                                   backlog item, no Slack post.
7. record_incident          — records the resolved incident in memory regardless of which path
                               ran (trigger_rule=none_failed_min_one_success).

### Required Airflow Connections (set in airflow_settings.yaml)
- snowflake_default : Snowflake, key-pair auth
- aws_default       : AWS, used by investigate_aws's S3 toolset
- jira_api          : HTTP, qbizinc.atlassian.net, Basic auth
- slack_api         : Slack, bot token
- airflow_api       : HTTP, host.docker.internal:8080, admin/admin
- github_api        : HTTP, api.github.com, Bearer token (fine-grained PAT scoped to this repo,
                       Contents + Pull requests read/write). Optional — open_pr degrades to a
                       clear no-op + Slack note if this connection isn't configured.

### Required environment variable (set in .env, no AIRFLOW_VAR_ prefix)
- ANTHROPIC_API_KEY  — read directly by the Anthropic SDK

### Required Airflow Variables (set in airflow_settings.yaml)
- SLACK_INCIDENT_CHANNEL
- NOVAMART_INCIDENT_OWNER_SLACK_ID (optional — @mentions this Slack member ID on incident posts)
"""

import base64
import json
import os
from datetime import datetime

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.sdk import Variable, dag, task

# Register @task.agent / @task.llm_branch with the airflow.sdk task namespace
from airflow.providers.common.ai.decorators import agent as _agent_decorator  # noqa: F401
from airflow.providers.common.ai.decorators import llm_branch as _llm_branch_decorator  # noqa: F401
from airflow.providers.common.ai.toolsets.hook import HookToolset
from airflow.providers.common.ai.toolsets.sql import SQLToolset

from include import incident_memory

INSTRUCTIONS_DIR = "/usr/local/airflow/include/incident_instructions"
GITHUB_REPO = "Qbizinc/novamart-pipelines"


def _load_instructions(platform: str) -> str:
    with open(os.path.join(INSTRUCTIONS_DIR, f"{platform}.md"), encoding="utf-8") as f:
        return f.read()


def _build_investigation_prompt(ctx: dict, prior_incidents: str, platform: str) -> str:
    """Shared prompt scaffolding for all three investigate_* agents — context + platform-specific
    instructions loaded from include/incident_instructions/<platform>.md."""
    tasks_summary = "\n".join(
        f"  - {t['task_id']} ({t['state']})" for t in ctx["failed_tasks"]
    )
    logs_section = "\n\n".join(
        f"=== {tid} ===\n{log}" for tid, log in ctx.get("task_logs", {}).items()
    )
    prior_section = f"{prior_incidents}\n\n" if prior_incidents else ""
    return (
        f"{ctx['failed_dag_id']} has failed.\n"
        f"DAG run ID: {ctx['failed_dag_run_id']}\n"
        f"Failed tasks:\n{tasks_summary}\n\n"
        f"Task logs:\n{logs_section}\n\n"
        f"Pipeline source code ({ctx['failed_dag_id']}.py):\n"
        f"```python\n{ctx.get('dag_source', '')}\n```\n\n"
        f"{prior_section}"
        f"{_load_instructions(platform)}"
    )


@dag(
    dag_id="agentic_snowflake_incident_memory_v2",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "agentic", "incident", "snowflake", "incident-memory", "router"],
)
def agentic_snowflake_incident_memory_v2():

    @task
    def gather_context(dag_run=None) -> dict:
        """Find the failed run to investigate and pre-fetch logs for each failed task.

        When triggered by a pipeline's on_failure_callback, failed_dag_id/failed_dag_run_id
        come from the triggering conf. When triggered manually with no conf, falls back to
        the most recent failed run of novamart_snowflake_sales.
        """
        conf = (dag_run.conf if dag_run else None) or {}
        failed_dag_id = conf.get("failed_dag_id", "novamart_snowflake_sales")
        failed_run_id = conf.get("failed_dag_run_id")

        # Pull credentials from the airflow_api connection — no Variables
        conn = HttpHook(http_conn_id="airflow_api").get_connection("airflow_api")
        base_url = f"{conn.schema or 'http'}://{conn.host}:{conn.port or 8080}"

        token_r = requests.post(
            f"{base_url}/auth/token",
            json={"username": conn.login, "password": conn.password},
            timeout=10,
        )
        token_r.raise_for_status()
        headers = {"Authorization": f"Bearer {token_r.json()['access_token']}"}

        if not failed_run_id:
            r = requests.get(
                f"{base_url}/api/v2/dags/{failed_dag_id}/dagRuns",
                params={"state": "failed", "limit": 1, "order_by": "-start_date"},
                headers=headers,
                timeout=10,
            )
            r.raise_for_status()
            runs = r.json().get("dag_runs", [])
            if not runs:
                raise ValueError(f"No failed runs found for {failed_dag_id}.")
            failed_run_id = runs[0]["dag_run_id"]

        r2 = requests.get(
            f"{base_url}/api/v2/dags/{failed_dag_id}/dagRuns/{failed_run_id}/taskInstances",
            headers=headers,
            timeout=10,
        )
        r2.raise_for_status()
        instances = r2.json().get("task_instances", [])
        failed_tasks = [
            {"task_id": t["task_id"], "state": t["state"]}
            for t in instances
            if t["state"] == "failed"
        ]

        task_logs: dict[str, str] = {}
        for t in failed_tasks:
            log_r = requests.get(
                f"{base_url}/api/v2/dags/{failed_dag_id}/dagRuns/{failed_run_id}"
                f"/taskInstances/{t['task_id']}/logs/1",
                headers=headers,
                timeout=15,
            )
            task_logs[t["task_id"]] = log_r.text[:4000]

        source_r = requests.get(
            f"{base_url}/api/v2/dagSources/{failed_dag_id}",
            headers=headers,
            timeout=10,
        )
        source_r.raise_for_status()
        dag_source = source_r.json().get("content", "")

        # Criticality signal for decide_path: a "critical" tag on the failing DAG itself.
        dag_r = requests.get(f"{base_url}/api/v2/dags/{failed_dag_id}", headers=headers, timeout=10)
        dag_r.raise_for_status()
        dag_tags = [t["name"] if isinstance(t, dict) else t for t in dag_r.json().get("tags", [])]
        is_critical = "critical" in dag_tags

        print(f"[gather_context] dag={failed_dag_id}, run={failed_run_id}, "
              f"failed={[t['task_id'] for t in failed_tasks]}, is_critical={is_critical}")
        return {
            "failed_dag_id": failed_dag_id,
            "failed_dag_run_id": failed_run_id,
            "is_critical": is_critical,
            "failed_tasks": failed_tasks,
            "task_logs": task_logs,
            "dag_source": dag_source,
        }

    @task
    def recall_prior_incidents(ctx: dict) -> str:
        """Search incident memory for prior occurrences on this pipeline (recurrence detection).

        Deterministic pre-fetch: runs before investigation and feeds any matches into its prompt so
        the recurrence context is guaranteed present without relying on the agent to look it up.
        Returns an empty string when there's no prior history (or if the memory is unavailable).
        """
        return incident_memory.recall_similar_incidents(
            ctx["failed_dag_id"], ctx.get("task_logs", {})
        )

    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        model_id="anthropic:claude-haiku-4-5",
        system_prompt=(
            "You are triaging a data pipeline failure. Classify which platform the failure "
            "belongs to, based on the failed task's exception text. Choose exactly one:\n"
            "- investigate_aws: botocore/boto3 exceptions, S3 or IAM errors (e.g. ClientError, "
            "AccessDenied, NoSuchKey).\n"
            "- investigate_api: an upstream HTTP service failure — requests exceptions, timeouts, "
            "connection errors, 4xx/5xx status codes.\n"
            "- investigate_snowflake: Snowflake connector errors, SQL compilation errors, "
            "ProgrammingError.\n"
            "Route to exactly one of these three tasks — do not pick more than one."
        ),
    )
    def classify_platform(ctx: dict) -> str:
        """Return the prompt for the platform-classification branch decision."""
        tasks_summary = "\n".join(
            f"  - {t['task_id']} ({t['state']})" for t in ctx["failed_tasks"]
        )
        logs_section = "\n\n".join(
            f"=== {tid} ===\n{log}" for tid, log in ctx.get("task_logs", {}).items()
        )
        return (
            f"{ctx['failed_dag_id']} has failed.\n"
            f"Failed tasks:\n{tasks_summary}\n\n"
            f"Task logs:\n{logs_section}"
        )

    @task.agent(
        toolsets=[
            HookToolset(
                hook=S3Hook(aws_conn_id="aws_default"),
                allowed_methods=["check_for_key", "list_keys", "read_key", "get_bucket_tagging"],
            ),
        ],
        llm_conn_id="pydanticai_default",
        model_id="anthropic:claude-haiku-4-5",
    )
    def investigate_aws(ctx: dict, prior_incidents: str) -> str:
        """Diagnose an AWS (S3/IAM) platform failure. Only runs when classify_platform routes here."""
        return _build_investigation_prompt(ctx, prior_incidents, "aws")

    @task.agent(
        toolsets=[],
        llm_conn_id="pydanticai_default",
        model_id="anthropic:claude-haiku-4-5",
    )
    def investigate_api(ctx: dict, prior_incidents: str) -> str:
        """Diagnose an upstream API platform failure. Only runs when classify_platform routes here."""
        return _build_investigation_prompt(ctx, prior_incidents, "api")

    @task.agent(
        toolsets=[
            SQLToolset(db_conn_id="snowflake_default"),
        ],
        llm_conn_id="pydanticai_default",
        model_id="anthropic:claude-haiku-4-5",
    )
    def investigate_snowflake(ctx: dict, prior_incidents: str) -> str:
        """Diagnose a Snowflake platform failure. Only runs when classify_platform routes here."""
        return _build_investigation_prompt(ctx, prior_incidents, "snowflake")

    @task(trigger_rule="none_failed_min_one_success")
    def merge_diagnosis(diag_aws: str | None, diag_api: str | None, diag_snowflake: str | None) -> str:
        """Pick whichever platform-specific investigation actually ran.

        classify_platform skips two of the three investigate_* tasks, so exactly one of these
        three arguments is non-None. trigger_rule=none_failed_min_one_success lets this task run
        despite two skipped upstreams (the default all_success would skip it too).
        """
        for diagnosis in (diag_aws, diag_api, diag_snowflake):
            if diagnosis is not None:
                return diagnosis
        raise ValueError("merge_diagnosis: no platform investigation produced a diagnosis.")

    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        model_id="anthropic:claude-haiku-4-5",
        system_prompt=(
            "Decide how to respond to this diagnosed pipeline failure. Choose exactly one:\n"
            "- propose_code_fix: the root cause is a genuine bug in the pipeline's OWN code (e.g. "
            "a typo, wrong dict/field key, wrong type handling) that can be safely corrected by "
            "editing that code — NOT a credentials, permissions, or external-service problem.\n"
            "- urgent_slack_post: the pipeline is marked CRITICAL and the root cause is NOT a code "
            "bug (e.g. expired credentials, access denied, an upstream service down, external "
            "schema drift) — needs a human's immediate attention right now, not a queued ticket.\n"
            "- create_ticket_low_priority: everything else — the pipeline is not critical, so this "
            "can be tracked as a normal backlog item instead of paging anyone.\n"
            "Route to exactly one of these three tasks."
        ),
    )
    def decide_path(diagnosis: str, ctx: dict) -> str:
        """Return the prompt for the fix/escalate/ticket branch decision."""
        criticality = "CRITICAL pipeline" if ctx.get("is_critical") else "not marked critical"
        return f"Pipeline: {ctx['failed_dag_id']} ({criticality})\n\nDiagnosis:\n{diagnosis}"

    @task.agent(
        toolsets=[],
        llm_conn_id="pydanticai_default",
        model_id="anthropic:claude-haiku-4-5",
    )
    def propose_code_fix(ctx: dict, diagnosis: str) -> str:
        """Propose a corrected version of the failing file's source.

        Review happens on the resulting GitHub PR itself (opened as a draft by open_pr, with a
        Slack notification), not via an Airflow-side approval gate — a PR is easily discarded/
        closed and GitHub's own diff view is a better review surface than pasting text into
        Airflow's HITL panel.
        """
        return (
            f"The pipeline {ctx['failed_dag_id']} failed due to a bug in its own source code.\n\n"
            f"Diagnosis:\n{diagnosis}\n\n"
            f"Current source ({ctx['failed_dag_id']}.py):\n"
            f"```python\n{ctx.get('dag_source', '')}\n```\n\n"
            "Return the corrected file, in exactly this format (this is the final answer):\n"
            "[SUMMARY] one-line description of the fix\n"
            "[FILE_PATH] dags/<filename>.py\n"
            "[NEW_CONTENT]\n"
            "<the complete corrected file content, and nothing else after this>"
        )

    @task
    def open_pr(proposed_fix: str, failed_dag_id: str, failed_dag_run_id: str) -> dict:
        """Open a PR on the human-approved fix via the GitHub REST API.

        Deterministic — the agent only proposed content (and a human approved it via HITL
        review); this task is the only thing that actually writes to the repo. Degrades to a
        clear no-op if the github_api connection isn't configured yet.
        """
        try:
            conn = HttpHook(http_conn_id="github_api").get_connection("github_api")
        except Exception:
            print("[open_pr] github_api connection not configured — skipping PR creation.")
            return {"url": None, "number": None}

        summary = "Automated fix"
        file_path = None
        new_content = None
        body_text = proposed_fix
        if body_text.startswith("[SUMMARY]"):
            first_line, _, body_text = body_text.partition("\n")
            summary = first_line.removeprefix("[SUMMARY]").strip() or summary
        if "[FILE_PATH]" in body_text and "[NEW_CONTENT]" in body_text:
            _, _, after_path = body_text.partition("[FILE_PATH]")
            path_line, _, after_content_marker = after_path.partition("[NEW_CONTENT]")
            file_path = path_line.strip()
            new_content = after_content_marker.strip("\n")

        if not file_path or not new_content:
            raise ValueError("open_pr: could not parse [FILE_PATH]/[NEW_CONTENT] from proposed fix.")

        headers = {
            "Authorization": f"Bearer {conn.password}",
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        base_url = f"https://api.github.com/repos/{GITHUB_REPO}"

        repo_r = requests.get(base_url, headers=headers, timeout=10)
        repo_r.raise_for_status()
        default_branch = repo_r.json()["default_branch"]

        ref_r = requests.get(f"{base_url}/git/ref/heads/{default_branch}", headers=headers, timeout=10)
        ref_r.raise_for_status()
        base_sha = ref_r.json()["object"]["sha"]

        branch_name = f"incident-fix/{failed_dag_id}-{failed_dag_run_id}".replace(":", "-")
        branch_r = requests.post(
            f"{base_url}/git/refs",
            headers=headers,
            json={"ref": f"refs/heads/{branch_name}", "sha": base_sha},
            timeout=10,
        )
        branch_r.raise_for_status()

        file_r = requests.get(
            f"{base_url}/contents/{file_path}",
            headers=headers,
            params={"ref": branch_name},
            timeout=10,
        )
        file_r.raise_for_status()
        file_sha = file_r.json()["sha"]

        commit_r = requests.put(
            f"{base_url}/contents/{file_path}",
            headers=headers,
            json={
                "message": f"fix({failed_dag_id}): {summary}",
                "content": base64.b64encode(new_content.encode()).decode(),
                "sha": file_sha,
                "branch": branch_name,
            },
            timeout=10,
        )
        commit_r.raise_for_status()

        pr_r = requests.post(
            f"{base_url}/pulls",
            headers=headers,
            json={
                "title": f"fix({failed_dag_id}): {summary}",
                "head": branch_name,
                "base": default_branch,
                "draft": True,
                "body": (
                    f"Automated fix proposed by agentic_snowflake_incident_memory_v2 for a "
                    f"failure in `{failed_dag_id}` (run `{failed_dag_run_id}`).\n\n"
                    f"**Unreviewed** — opened as a draft. Review the diff and mark ready for "
                    f"review (or close it) before merging."
                ),
            },
            timeout=10,
        )
        pr_r.raise_for_status()
        pr = pr_r.json()
        print(f"[open_pr] Opened PR #{pr['number']}: {pr['html_url']}")
        return {"url": pr["html_url"], "number": pr["number"], "title": pr["title"], "summary": summary}

    @task
    def notify_slack_fix(pr: dict, diagnosis: str, failed_dag_id: str) -> dict:
        """Post to Slack that a PR was opened (or, if GitHub isn't configured yet, that one
        would have been). Returns pr unchanged so record_incident can link it."""
        slack = SlackHook(slack_conn_id="slack_api")
        channel = Variable.get("SLACK_INCIDENT_CHANNEL", default="#qbiz_slackbot_testing")
        owner_id = Variable.get("NOVAMART_INCIDENT_OWNER_SLACK_ID", default="")
        owner_mention = f" <@{owner_id}>" if owner_id else ""

        if pr.get("url"):
            pr_line = f"  |  <{pr['url']}|PR #{pr.get('number')}>"
            headline = f":hammer_and_wrench: *NovaMart — {failed_dag_id} auto-fixed*{pr_line}{owner_mention}"
            detail_lines = [f"*{pr.get('title', pr.get('summary', ''))}*"]
        else:
            headline = (
                f":hammer_and_wrench: *NovaMart — {failed_dag_id} has a proposed code fix* "
                f"(GitHub connection not configured yet — PR not opened){owner_mention}"
            )
            detail_lines = []

        header_ts = slack.call("chat.postMessage", json={"channel": channel, "text": headline})["ts"]

        if detail_lines:
            slack.call(
                "chat.postMessage",
                json={"channel": channel, "text": "\n".join(detail_lines), "thread_ts": header_ts},
            )
        slack.call(
            "chat.postMessage",
            json={"channel": channel, "text": f"```{diagnosis[:3800]}```", "thread_ts": header_ts},
        )
        return pr

    @task
    def urgent_slack_post(diagnosis: str, failed_dag_id: str) -> None:
        """Post an urgent, tagged Slack alert — no Jira ticket. Only for critical pipelines
        whose root cause is not a fixable code bug."""
        slack = SlackHook(slack_conn_id="slack_api")
        channel = Variable.get("SLACK_INCIDENT_CHANNEL", default="#qbiz_slackbot_testing")
        owner_id = Variable.get("NOVAMART_INCIDENT_OWNER_SLACK_ID", default="")
        owner_mention = f" <@{owner_id}>" if owner_id else ""

        header_ts = slack.call(
            "chat.postMessage",
            json={
                "channel": channel,
                "text": (
                    f":rotating_light::rotating_light: *CRITICAL — {failed_dag_id} Pipeline "
                    f"Failure*{owner_mention}"
                ),
            },
        )["ts"]
        slack.call(
            "chat.postMessage",
            json={"channel": channel, "text": f"```{diagnosis[:3800]}```", "thread_ts": header_ts},
        )

    @task
    def create_ticket_low_priority(diagnosis: str, failed_dag_id: str) -> dict:
        """Create a low-priority Jira ticket — no Slack post. Only for non-critical pipelines."""
        summary = f"[Low priority] {failed_dag_id} pipeline failure"
        body = diagnosis
        if diagnosis.startswith("[SUMMARY]"):
            first_line, _, rest = diagnosis.partition("\n")
            title = first_line.removeprefix("[SUMMARY]").strip() or f"{failed_dag_id} pipeline failure"
            summary = f"[Low priority] {title}"
            body = rest.strip()
        body = f"(Low priority — non-critical pipeline)\n\n{body}"

        response = HttpHook(method="POST", http_conn_id="jira_api").run(
            endpoint="/rest/api/3/issue",
            data=json.dumps({
                "fields": {
                    "project": {"key": "AD"},
                    "summary": summary,
                    "description": {
                        "type": "doc",
                        "version": 1,
                        "content": [
                            {"type": "paragraph", "content": [{"type": "text", "text": body}]}
                        ],
                    },
                    "issuetype": {"name": "Bug"},
                }
            }),
            headers={"Content-Type": "application/json"},
        )
        data = response.json()
        ticket = {"key": data["key"], "url": f"https://qbizinc.atlassian.net/browse/{data['key']}"}
        print(f"[create_ticket_low_priority] Created {ticket['key']}: {ticket['url']}")
        return ticket

    @task(trigger_rule="none_failed_min_one_success")
    def record_incident(
        diagnosis: str,
        failed_dag_id: str,
        failed_dag_run_id: str,
        fix_result: dict | None,
        ticket: dict | None,
        _escalate_done: None,
    ) -> None:
        """Record the resolved incident in memory (best-effort), regardless of which path ran.

        Exactly one of fix_result/ticket is meaningfully non-None (urgent_slack_post has neither
        — nothing to link, just an alert — _escalate_done is unused, taken only so this task has
        an explicit dependency edge on that branch too). trigger_rule=none_failed_min_one_success
        lets this run despite two of the three decide_path branches being skipped.
        """
        ticket_like = fix_result if (fix_result and fix_result.get("url")) else (ticket or {})
        incident_memory.record_incident(
            failed_dag_id, failed_dag_run_id, diagnosis, ticket_like, status="open"
        )

    ctx = gather_context()
    prior = recall_prior_incidents(ctx)

    classification = classify_platform(ctx)
    diag_aws = investigate_aws(ctx, prior)
    diag_api = investigate_api(ctx, prior)
    diag_snowflake = investigate_snowflake(ctx, prior)
    classification >> [diag_aws, diag_api, diag_snowflake]

    diagnosis = merge_diagnosis(diag_aws, diag_api, diag_snowflake)

    decision = decide_path(diagnosis, ctx)
    proposed_fix = propose_code_fix(ctx, diagnosis)
    escalate_done = urgent_slack_post(diagnosis, ctx["failed_dag_id"])
    ticket = create_ticket_low_priority(diagnosis, ctx["failed_dag_id"])
    decision >> [proposed_fix, escalate_done, ticket]

    pr = open_pr(proposed_fix, ctx["failed_dag_id"], ctx["failed_dag_run_id"])
    fix_result = notify_slack_fix(pr, diagnosis, ctx["failed_dag_id"])

    record_incident(
        diagnosis, ctx["failed_dag_id"], ctx["failed_dag_run_id"], fix_result, ticket, escalate_done
    )


agentic_snowflake_incident_memory_v2()
