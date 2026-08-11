"""
## Agentic Incident DAG v2 (platform + path routed, with incident memory)

A standalone variant of agentic_snowflake_incident_memory that routes both investigation (by
platform) and response (by severity) instead of using one fixed diagnose-then-ticket flow for
every failure. Does not replace or modify agentic_snowflake_incident_memory.py — that DAG is
untouched; this is a separate, independently triggerable pipeline used to prototype the routed
design.

Triggered automatically when a pipeline's on_failure_callback points at this DAG (see
include/incident_callbacks.py:trigger_incident_dag_v2), or manually from the UI.

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
                                 - snowflake: SQLToolset(db_conn_id="snowflake_default") + dag_lookup_toolset
                                 - aws:       HookToolset over S3Hook (list_keys/read_key/etc.) + dag_lookup_toolset
                                 - api:       no live tool yet — reasons from logs/source only
                               dag_lookup_toolset (FunctionToolset: list_dag_ids, get_dag_source) lets the
                               agent find and fetch ANY DAG's source by dag_id, not just the one that
                               failed — for tracing a root cause to an upstream pipeline.
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
                                   Jira ticket, plus a quiet Slack FYI (no @mention) linking it.
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
import re
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
from pydantic_ai.toolsets.function import FunctionToolset

from include import diagnosis_format, harness_audit, incident_memory

INSTRUCTIONS_DIR = "/usr/local/airflow/include/incident_instructions"
GITHUB_REPO = "Qbizinc/novamart-pipelines"

# The only two models this DAG's agent/branch steps are allowed to use — see
# harness_audit.new_model_policy() for which activity is capped/floored at which tier, and why.
MODEL_WEAK = "anthropic:claude-haiku-4-5"
MODEL_FRONTIER = "anthropic:claude-sonnet-5"

harness_audit.enforce_model_policy({
    "classify_platform": MODEL_WEAK,
    "investigate_aws": MODEL_WEAK,
    "investigate_api": MODEL_WEAK,
    "investigate_snowflake": MODEL_FRONTIER,
    "decide_path": MODEL_FRONTIER,
    "propose_code_fix": MODEL_FRONTIER,
})


class ResilientHookToolset(HookToolset):
    """A HookToolset that turns a tool call's own exception into an error string the agent can
    reason about, instead of an uncaught exception that crashes the whole investigation.

    This matters specifically for investigate_aws: when the actual incident IS an AWS auth/
    credentials problem, the diagnostic tools built on the same connection fail the same way the
    pipeline did. Without this, the agent can never diagnose "the AWS session is expired" — the
    investigation itself would always crash first instead of observing and reporting that.
    """

    async def call_tool(self, name, tool_args, ctx, tool):
        try:
            return await super().call_tool(name, tool_args, ctx, tool)
        except Exception as exc:
            return f"ERROR calling tool {name!r}: {type(exc).__name__}: {exc}"


class ResilientSQLToolset(SQLToolset):
    async def call_tool(self, name, tool_args, ctx, tool):
        try:
            return await super().call_tool(name, tool_args, ctx, tool)
        except Exception as exc:
            return f"ERROR calling tool {name!r}: {type(exc).__name__}: {exc}"


def _load_instructions(platform: str) -> str:
    with open(os.path.join(INSTRUCTIONS_DIR, f"{platform}.md"), encoding="utf-8") as f:
        return f.read()


def _airflow_api_headers(base_url: str) -> dict:
    conn = HttpHook(http_conn_id="airflow_api").get_connection("airflow_api")
    token_r = requests.post(
        f"{base_url}/auth/token",
        json={"username": conn.login, "password": conn.password},
        timeout=10,
    )
    token_r.raise_for_status()
    return {"Authorization": f"Bearer {token_r.json()['access_token']}"}


def _airflow_api_base_url() -> str:
    conn = HttpHook(http_conn_id="airflow_api").get_connection("airflow_api")
    return f"{conn.schema or 'http'}://{conn.host}:{conn.port or 8080}"


def list_dag_ids() -> list[str] | str:
    """List every known dag_id in this Airflow instance. Use this to find the exact dag_id of a
    suspected upstream/related pipeline before calling get_dag_source."""
    try:
        base_url = _airflow_api_base_url()
        headers = _airflow_api_headers(base_url)
        r = requests.get(f"{base_url}/api/v2/dags", headers=headers, timeout=10)
        r.raise_for_status()
        return [d["dag_id"] for d in r.json().get("dags", [])]
    except Exception as exc:
        return f"ERROR listing dags: {type(exc).__name__}: {exc}"


def get_dag_source(dag_id: str) -> str:
    """Fetch the Python source code of any Airflow DAG by its dag_id — not just the DAG that
    failed. Use this to inspect an upstream/related pipeline when the evidence points there.
    If the exact dag_id isn't known, call list_dag_ids first."""
    try:
        base_url = _airflow_api_base_url()
        headers = _airflow_api_headers(base_url)
        r = requests.get(f"{base_url}/api/v2/dagSources/{dag_id}", headers=headers, timeout=10)
        r.raise_for_status()
        return r.json().get("content", "")
    except Exception as exc:
        return f"ERROR fetching source for dag_id={dag_id!r}: {type(exc).__name__}: {exc}"


def find_blast_radius(dag_id: str) -> list[dict] | str:
    """Find every DAG that transitively depends on dag_id's output via Airflow Assets (a DAG
    scheduled to run when dag_id, or something downstream of it, produces an asset), and whether
    each one is tagged critical. Use this to check whether a failure in a NON-critical pipeline
    still cascades into something critical several hops downstream, even if nothing downstream
    has actually run/failed yet — a failed producer means its asset never updates, so anything
    scheduled off it silently never runs, rather than failing loudly itself."""
    try:
        base_url = _airflow_api_base_url()
        headers = _airflow_api_headers(base_url)
        r = requests.get(f"{base_url}/api/v2/assets", headers=headers, params={"limit": 1000}, timeout=10)
        r.raise_for_status()
        assets = r.json().get("assets", [])

        downstream: set[str] = set()
        seen = {dag_id}
        frontier = {dag_id}
        while frontier:
            next_frontier: set[str] = set()
            for asset in assets:
                producers = {
                    t.get("dag_id") for t in asset.get("producing_tasks", []) if t.get("dag_id")
                }
                if not (producers & frontier):
                    continue
                for consumer in asset.get("scheduled_dags", []):
                    consumer_id = consumer.get("dag_id") if isinstance(consumer, dict) else consumer
                    if consumer_id and consumer_id not in seen:
                        downstream.add(consumer_id)
                        next_frontier.add(consumer_id)
                        seen.add(consumer_id)
            frontier = next_frontier

        result = []
        for downstream_dag_id in sorted(downstream):
            dag_r = requests.get(
                f"{base_url}/api/v2/dags/{downstream_dag_id}", headers=headers, timeout=10
            )
            dag_r.raise_for_status()
            tags = [t["name"] if isinstance(t, dict) else t for t in dag_r.json().get("tags", [])]
            result.append({"dag_id": downstream_dag_id, "is_critical": "critical" in tags})
        return result
    except Exception as exc:
        return f"ERROR computing blast radius for dag_id={dag_id!r}: {type(exc).__name__}: {exc}"


dag_lookup_toolset = FunctionToolset(tools=[get_dag_source, list_dag_ids, find_blast_radius])


def _build_investigation_prompt(ctx: dict, prior_incidents: dict, platform: str) -> str:
    """Shared prompt scaffolding for all three investigate_* agents — context + platform-specific
    instructions loaded from include/incident_instructions/<platform>.md."""
    tasks_summary = "\n".join(
        f"  - {t['task_id']} ({t['state']})" for t in ctx["failed_tasks"]
    )
    logs_section = "\n\n".join(
        f"=== {tid} ===\n{log}" for tid, log in ctx.get("task_logs", {}).items()
    )
    prior_text = prior_incidents.get("text", "")
    prior_section = f"{prior_text}\n\n" if prior_text else ""
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
    dag_id="agentic_incident_memory_v2",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "agentic", "incident", "snowflake", "incident-memory", "router"],
)
def agentic_incident_memory_v2():

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
    def recall_prior_incidents(ctx: dict) -> dict:
        """Search incident memory for prior occurrences on this pipeline (recurrence detection).

        Deterministic pre-fetch: runs before investigation and feeds any matches into its prompt so
        the recurrence context is guaranteed present without relying on the agent to look it up.
        Returns {"text": ..., "tickets": [...]}, both empty when there's no prior history (or if
        the memory is unavailable).
        """
        return incident_memory.recall_similar_incidents(
            ctx["failed_dag_id"], ctx.get("task_logs", {})
        )

    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        model_id=MODEL_WEAK,
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
            ResilientHookToolset(
                hook=S3Hook(aws_conn_id="aws_default"),
                allowed_methods=["check_for_key", "list_keys", "read_key", "get_bucket_tagging"],
            ),
            dag_lookup_toolset,
        ],
        llm_conn_id="pydanticai_default",
        model_id=MODEL_WEAK,
    )
    def investigate_aws(ctx: dict, prior_incidents: dict) -> str:
        """Diagnose an AWS (S3/IAM) platform failure. Only runs when classify_platform routes here."""
        return _build_investigation_prompt(ctx, prior_incidents, "aws")

    @task.agent(
        toolsets=[dag_lookup_toolset],
        llm_conn_id="pydanticai_default",
        model_id=MODEL_WEAK,
    )
    def investigate_api(ctx: dict, prior_incidents: dict) -> str:
        """Diagnose an upstream API platform failure. Only runs when classify_platform routes here."""
        return _build_investigation_prompt(ctx, prior_incidents, "api")

    @task.agent(
        toolsets=[
            ResilientSQLToolset(db_conn_id="snowflake_default"),
            dag_lookup_toolset,
        ],
        llm_conn_id="pydanticai_default",
        model_id=MODEL_FRONTIER,
    )
    def investigate_snowflake(ctx: dict, prior_incidents: dict) -> str:
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

    @task
    def check_cross_pipeline_reference(diagnosis: str, ctx: dict) -> str:
        """Deterministically check whether the diagnosis text names a dag_id other than the one
        that actually failed, so decide_path gets an explicit fact instead of having to notice
        it itself by reading two loosely related blocks of prose."""
        failed_dag_id = ctx["failed_dag_id"]
        try:
            base_url = _airflow_api_base_url()
            headers = _airflow_api_headers(base_url)
            r = requests.get(f"{base_url}/api/v2/dags", headers=headers, timeout=10)
            r.raise_for_status()
            all_dag_ids = [d["dag_id"] for d in r.json().get("dags", [])]
        except Exception as exc:
            print(f"[check_cross_pipeline_reference] could not list dags (continuing without check): {exc}")
            return ""

        # Match on word boundaries, not raw substring: a dag_id that is a prefix of another
        # (novamart_gold vs novamart_gold_sales_by_region) would otherwise report a cross-pipeline
        # reference that the diagnosis never made. \b treats underscores as word chars, so the
        # longer id doesn't match the shorter one.
        mentioned = [
            d for d in all_dag_ids
            if d != failed_dag_id and re.search(rf"\b{re.escape(d)}\b", diagnosis)
        ]
        if not mentioned:
            return ""
        note = (
            f"AUTOMATED CHECK (not an opinion, a fact): this diagnosis text mentions the "
            f"following OTHER pipeline(s), distinct from the one that actually failed "
            f"({failed_dag_id}): {', '.join(mentioned)}. If the root cause is a genuine code bug "
            f"in one of those pipelines, propose_code_fix can still apply — but it must fetch "
            f"that pipeline's REAL source via get_dag_source before proposing any change to it, "
            f"and the FILE_PATH must point to that pipeline's actual file, not {failed_dag_id}'s."
        )
        print(f"[check_cross_pipeline_reference] {note}")
        return note

    @task
    def check_blast_radius(ctx: dict) -> str:
        """Deterministically check whether anything downstream of the failed pipeline (via
        Airflow Assets, possibly several hops away) is tagged critical — so decide_path can treat
        this as urgent even when the failed pipeline itself isn't marked critical. A failed
        producer never emits its asset, so a downstream consumer silently never runs today rather
        than failing loudly itself — this is the only way that gets surfaced."""
        result = find_blast_radius(ctx["failed_dag_id"])
        if isinstance(result, str):
            print(f"[check_blast_radius] {result}")
            return ""
        critical_downstream = [r["dag_id"] for r in result if r["is_critical"]]
        if not critical_downstream:
            return ""
        note = (
            f"AUTOMATED CHECK (not an opinion, a fact): {ctx['failed_dag_id']} is not itself "
            f"tagged critical, but the following downstream pipeline(s) — reached via Airflow "
            f"Asset dependencies, possibly several hops away — ARE tagged critical and depend on "
            f"its output: {', '.join(critical_downstream)}. Because {ctx['failed_dag_id']} "
            f"failed, it never produced the asset those pipelines depend on, so they will "
            f"silently never run today rather than failing loudly themselves. Treat this the "
            f"same as if {ctx['failed_dag_id']} were itself marked critical."
        )
        print(f"[check_blast_radius] {note}")
        return note

    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        model_id=MODEL_FRONTIER,
        system_prompt=(
            "Decide how to respond to this diagnosed pipeline failure. Choose exactly one:\n"
            "FIRST check blast radius: if a BLAST RADIUS check below shows a critical pipeline "
            "depends downstream on this one's output — even several hops away, even if it never "
            "actually ran or failed itself — that overrides everything below. Route to "
            "urgent_slack_post regardless of whether the bug is otherwise fixable. A critical "
            "downstream pipeline silently never running today is urgent right now; a draft PR "
            "someone reviews later is not an adequate response to that on its own — page a human "
            "first, a code fix can still follow separately.\n"
            "Otherwise, choose exactly one:\n"
            "- propose_code_fix: the root cause is a genuine, fixable bug in a pipeline's OWN "
            "code (e.g. a typo, wrong dict/field key, wrong type handling, an unintended "
            "aggregation) — NOT a credentials, permissions, or external-service problem. This "
            "can be the pipeline named as 'Pipeline:' below, OR a different pipeline it depends "
            "on (e.g. an upstream producer), if the diagnosis traced the defect to that "
            "pipeline's own code — propose_code_fix has a tool to fetch and fix that other "
            "pipeline's real file too, it is not limited to editing only the failed pipeline. "
            "NOT eligible for propose_code_fix, regardless of which pipeline: a fix that would "
            "weaken, remove, or work around a validation/QA/uniqueness/grain check to make the "
            "failure stop, rather than fixing what the check correctly caught — that check is "
            "doing its job; if the only available 'fix' is loosening the check, this is not a "
            "fixable code bug, route to urgent_slack_post/create_ticket_low_priority instead.\n"
            "- urgent_slack_post: (if not already routed here by the blast radius rule above) the "
            "pipeline is marked CRITICAL AND the root cause is NOT a fixable code bug (e.g. "
            "expired credentials, access denied, an upstream service down, external schema drift, "
            "or a fix that would require weakening a QA check) — needs a human's immediate "
            "attention right now, not a queued ticket.\n"
            "- create_ticket_low_priority: everything else — not critical, and no critical blast "
            "radius, so this can be tracked as a normal backlog item instead of paging anyone.\n"
            "Route to exactly one of these three tasks."
        ),
    )
    def decide_path(diagnosis: str, ctx: dict, cross_ref_note: str, blast_radius_note: str) -> str:
        """Return the prompt for the fix/escalate/ticket branch decision."""
        criticality = "CRITICAL pipeline" if ctx.get("is_critical") else "not marked critical"
        note_section = f"\n\n{cross_ref_note}" if cross_ref_note else ""
        blast_section = f"\n\n{blast_radius_note}" if blast_radius_note else ""
        return (
            f"Pipeline: {ctx['failed_dag_id']} ({criticality})\n\nDiagnosis:\n{diagnosis}"
            f"{note_section}{blast_section}"
        )

    @task.agent(
        toolsets=[dag_lookup_toolset],
        llm_conn_id="pydanticai_default",
        model_id=MODEL_FRONTIER,
    )
    def propose_code_fix(ctx: dict, diagnosis: str) -> str:
        """Propose a corrected version of whichever file actually contains the bug — which may
        be a different pipeline than the one that failed.

        Review happens on the resulting GitHub PR itself (opened as a draft by open_pr, with a
        Slack notification), not via an Airflow-side approval gate — a PR is easily discarded/
        closed and GitHub's own diff view is a better review surface than pasting text into
        Airflow's HITL panel.
        """
        return (
            f"{ctx['failed_dag_id']} failed.\n\n"
            f"Diagnosis:\n{diagnosis}\n\n"
            f"Source of the pipeline that failed ({ctx['failed_dag_id']}.py):\n"
            f"```python\n{ctx.get('dag_source', '')}\n```\n\n"
            "The bug may be in this file, or — if the diagnosis traces the root cause to a "
            "different pipeline (e.g. an upstream producer) — in that pipeline's own file "
            "instead. If so, use list_dag_ids/get_dag_source to fetch that pipeline's REAL "
            "current source before proposing anything against it. Never invent or guess file "
            "content you have not actually fetched.\n\n"
            "Do not weaken, remove, or work around any validation/QA/uniqueness/grain check "
            "(any assertion, raise, or comparison that gates data quality) to make the failure "
            "stop. Those checks exist to catch real problems. If two values are expected to "
            "match and don't, fix whichever side's logic actually produces the wrong value — "
            "do not just change the reported/metadata value to match the wrong one. To judge "
            "which side is wrong, look for internal inconsistencies in the suspect code itself: "
            "e.g. a field that should identify one specific record (an id, a key) but is "
            "populated from a group/aggregation of several records is a sign the aggregation "
            "was not the intended design.\n\n"
            f"[FILE_PATH] must be the path of whichever file actually contains the bug — this "
            f"may or may not be {ctx['failed_dag_id']}.py.\n\n"
            "Return the corrected file, in exactly this format (this is the final answer). "
            "[FILE_PATH] must contain ONLY the path, nothing else on that line:\n"
            "[SUMMARY] one-line description of the fix\n"
            "[FILE_PATH] dags/<filename>.py\n"
            "[NEW_CONTENT]\n"
            "<the complete corrected file content, and nothing else after this>"
        )

    @task
    def validate_proposed_fix(proposed_fix: str, failed_dag_id: str, failed_dag_run_id: str) -> dict:
        """Parse the agent's [SUMMARY]/[FILE_PATH]/[NEW_CONTENT] text into structured fields and
        mechanically validate them before anything writes to the repo.

        Centralizing the parsing here (instead of inline in open_pr) means there is one place
        that turns agent prose into structured data, and that structured data can be checked by
        validate_output — checks on structured fields are non-bypassable in a way scraping raw
        text is not. check_format's type check alone won't catch a present-but-empty
        file_path/new_content (an empty string still satisfies `str`), so an explicit non-empty
        check follows it — "content must be non-blank" is a domain rule the mechanical
        shape-checker doesn't express.
        """
        incident_id = f"{failed_dag_id}:{failed_dag_run_id}"
        audit = harness_audit.new_audit_log()

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
            # Keep only a well-formed path token, in case the model appends stray prose after it
            # (e.g. an explanatory clause) on the same line.
            path_match = re.match(r"\s*([\w\-./]+\.py)", path_line)
            file_path = path_match.group(1) if path_match else path_line.strip()
            new_content = re.sub(
                r"\A```[a-zA-Z]*\n|\n```\s*\Z", "", after_content_marker.strip("\n")
            )

        parsed = {"summary": summary, "file_path": file_path or "", "new_content": new_content or ""}

        harness_audit.guard_output(
            audit,
            response=parsed,
            expected_schema={"summary": str, "file_path": str, "new_content": str},
            action="validate_proposed_fix",
            incident_id=incident_id,
            job_id=failed_dag_run_id,
        )
        # Beyond "non-blank": the path must actually stay inside dags/ — mechanical validation
        # doesn't know *which* file was supposed to be fixed, only that a write to an arbitrary
        # repo path (or outside it, via "..") should never reach open_pr's GitHub API calls.
        path_in_bounds = (
            parsed["file_path"].startswith("dags/")
            and ".." not in parsed["file_path"]
        )
        if not parsed["file_path"] or not parsed["new_content"] or not path_in_bounds:
            from qbiz_harness import OutputRejectedError

            reason = (
                "file_path/new_content present but blank"
                if (not parsed["file_path"] or not parsed["new_content"])
                else f"file_path {parsed['file_path']!r} is outside dags/"
            )
            exc = OutputRejectedError(f"validate_proposed_fix: {reason}.")
            audit.record_intervention(
                agent_id=harness_audit.DAG_ID,
                action="validate_proposed_fix",
                component="output_validator",
                prevented=str(exc),
                incident_id=incident_id,
                cohort=harness_audit.COHORT,
                job_id=failed_dag_run_id,
            )
            raise exc

        audit.record(
            agent_id=harness_audit.DAG_ID,
            action="validate_proposed_fix",
            decision="allowed",
            incident_id=incident_id,
            cohort=harness_audit.COHORT,
            job_id=failed_dag_run_id,
            outputs={"summary": summary, "file_path": parsed["file_path"]},
        )
        return parsed

    @task
    def open_pr(validated_fix: dict, diagnosis: str, failed_dag_id: str, failed_dag_run_id: str) -> dict:
        """Open a PR on the validated fix via the GitHub REST API.

        Deterministic — the agent only proposed content (validated by validate_proposed_fix);
        this task is the only thing that actually writes to the repo. Degrades to a clear no-op
        if the github_api connection isn't configured yet.
        """
        incident_id = f"{failed_dag_id}:{failed_dag_run_id}"
        audit = harness_audit.new_audit_log()

        try:
            conn = HttpHook(http_conn_id="github_api").get_connection("github_api")
        except Exception:
            print("[open_pr] github_api connection not configured — skipping PR creation.")
            audit.record(
                agent_id=harness_audit.DAG_ID,
                action="open_pr",
                decision="skipped",
                incident_id=incident_id,
                cohort=harness_audit.COHORT,
                job_id=failed_dag_run_id,
                outputs={"reason": "github_api connection not configured"},
            )
            return {"url": None, "number": None}

        summary = validated_fix["summary"]
        file_path = validated_fix["file_path"]
        new_content = validated_fix["new_content"]

        governor = harness_audit.new_action_governor({"prs_opened": 1})
        harness_audit.guard_action(
            governor,
            audit,
            kind="prs_opened",
            action="open_pr",
            incident_id=incident_id,
            job_id=failed_dag_run_id,
        )

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

        pr_body = diagnosis_format.build_pr_body(
            dag_id=failed_dag_id,
            run_id=failed_dag_run_id,
            sections=diagnosis_format.parse_diagnosis(diagnosis),
            run_url=diagnosis_format.airflow_run_url(failed_dag_id, failed_dag_run_id),
        )
        pr_r = requests.post(
            f"{base_url}/pulls",
            headers=headers,
            json={
                "title": f"fix({failed_dag_id}): {summary}",
                "head": branch_name,
                "base": default_branch,
                "draft": True,
                "body": pr_body,
            },
            timeout=10,
        )
        pr_r.raise_for_status()
        pr = pr_r.json()
        print(f"[open_pr] Opened PR #{pr['number']}: {pr['html_url']}")

        try:
            requests.post(
                f"{base_url}/issues/{pr['number']}/labels",
                headers=headers,
                json={"labels": ["automated-fix"]},
                timeout=10,
            ).raise_for_status()
        except Exception as exc:
            # A missing/misnamed label on the repo shouldn't fail an otherwise-successful PR.
            print(f"[open_pr] Could not apply 'automated-fix' label (continuing): {exc}")

        result = {"url": pr["html_url"], "number": pr["number"], "title": pr["title"], "summary": summary}
        audit.record(
            agent_id=harness_audit.DAG_ID,
            action="open_pr",
            decision="allowed",
            incident_id=incident_id,
            cohort=harness_audit.COHORT,
            job_id=failed_dag_run_id,
            outputs={"url": result["url"], "number": result["number"]},
        )
        return result

    @task
    def notify_slack_fix(pr: dict, diagnosis: str, failed_dag_id: str, failed_dag_run_id: str) -> dict:
        """Post to Slack that a PR was opened (or, if GitHub isn't configured yet, that one
        would have been). Returns pr unchanged so record_incident can link it."""
        incident_id = f"{failed_dag_id}:{failed_dag_run_id}"
        audit = harness_audit.new_audit_log()
        governor = harness_audit.new_action_governor({"messages_sent": 5})

        slack = SlackHook(slack_conn_id="slack_api")
        channel = Variable.get("SLACK_INCIDENT_CHANNEL", default="#qbiz_slackbot_testing")
        owner_id = Variable.get("NOVAMART_INCIDENT_OWNER_SLACK_ID", default="")
        owner_mention = f"<@{owner_id}>" if owner_id else ""

        sections = diagnosis_format.parse_diagnosis(diagnosis)
        links = [("View the failed Airflow run", diagnosis_format.airflow_run_url(failed_dag_id, failed_dag_run_id))]
        if pr.get("url"):
            links.insert(0, (f"PR #{pr.get('number')}: {pr.get('title', pr.get('summary', ''))}", pr["url"]))
            severity = "fix"
        else:
            severity = "fix"  # GitHub not configured — still the FIX path, just without a PR link

        blocks, fallback_text = diagnosis_format.build_incident_blocks(
            severity=severity, dag_id=failed_dag_id, run_id=failed_dag_run_id,
            sections=sections, owner_mention=owner_mention, links=links,
        )

        harness_audit.guard_action(
            governor, audit, kind="messages_sent", action="notify_slack_fix",
            incident_id=incident_id, job_id=failed_dag_run_id,
        )
        slack.call("chat.postMessage", json={"channel": channel, "blocks": blocks, "text": fallback_text})
        audit.record(
            agent_id=harness_audit.DAG_ID, action="notify_slack_fix", decision="allowed",
            incident_id=incident_id, cohort=harness_audit.COHORT, job_id=failed_dag_run_id,
            outputs={"channel": channel, "pr_url": pr.get("url")},
        )
        return pr

    @task
    def urgent_slack_post(diagnosis: str, failed_dag_id: str, failed_dag_run_id: str, prior_incidents: dict) -> None:
        """Post an urgent, tagged Slack alert — no Jira ticket. Only for critical pipelines
        whose root cause is not a fixable code bug."""
        incident_id = f"{failed_dag_id}:{failed_dag_run_id}"
        audit = harness_audit.new_audit_log()
        governor = harness_audit.new_action_governor({"messages_sent": 5})

        slack = SlackHook(slack_conn_id="slack_api")
        channel = Variable.get("SLACK_INCIDENT_CHANNEL", default="#qbiz_slackbot_testing")
        owner_id = Variable.get("NOVAMART_INCIDENT_OWNER_SLACK_ID", default="")
        owner_mention = f"<@{owner_id}>" if owner_id else ""

        sections = diagnosis_format.parse_diagnosis(diagnosis)
        links = [("View the failed Airflow run", diagnosis_format.airflow_run_url(failed_dag_id, failed_dag_run_id))]
        blocks, fallback_text = diagnosis_format.build_incident_blocks(
            severity="critical", dag_id=failed_dag_id, run_id=failed_dag_run_id,
            sections=sections, owner_mention=owner_mention, links=links,
            prior_tickets=prior_incidents.get("tickets", []),
        )

        harness_audit.guard_action(
            governor, audit, kind="messages_sent", action="urgent_slack_post",
            incident_id=incident_id, job_id=failed_dag_run_id,
        )
        slack.call("chat.postMessage", json={"channel": channel, "blocks": blocks, "text": fallback_text})
        audit.record(
            agent_id=harness_audit.DAG_ID, action="urgent_slack_post", decision="allowed",
            incident_id=incident_id, cohort=harness_audit.COHORT, job_id=failed_dag_run_id,
            outputs={"channel": channel},
        )

    @task
    def create_ticket_low_priority(diagnosis: str, failed_dag_id: str, failed_dag_run_id: str, prior_incidents: dict) -> dict:
        """Create a low-priority Jira ticket, plus a quiet (no @mention) Slack FYI linking it.
        Only for non-critical pipelines."""
        incident_id = f"{failed_dag_id}:{failed_dag_run_id}"
        audit = harness_audit.new_audit_log()
        governor = harness_audit.new_action_governor({"tickets_created": 1, "messages_sent": 1})

        sections = diagnosis_format.parse_diagnosis(diagnosis)
        summary = f"[Low priority] {sections['summary'] or f'{failed_dag_id} pipeline failure'}"

        harness_audit.guard_action(
            governor, audit, kind="tickets_created", action="create_ticket_low_priority",
            incident_id=incident_id, job_id=failed_dag_run_id,
        )
        response = HttpHook(method="POST", http_conn_id="jira_api").run(
            endpoint="/rest/api/3/issue",
            data=json.dumps({
                "fields": {
                    "project": {"key": "AD"},
                    "summary": summary,
                    "description": diagnosis_format.build_adf_description(sections),
                    "issuetype": {"name": "Bug"},
                    "priority": {"name": "Low"},
                    "labels": ["automated", "incident-response"],
                }
            }),
            headers={"Content-Type": "application/json"},
        )
        data = response.json()
        ticket = {"key": data["key"], "url": f"https://qbizinc.atlassian.net/browse/{data['key']}"}
        print(f"[create_ticket_low_priority] Created {ticket['key']}: {ticket['url']}")

        channel = Variable.get("SLACK_INCIDENT_CHANNEL", default="#qbiz_slackbot_testing")
        links = [
            (f"Jira ticket {ticket['key']}", ticket["url"]),
            ("View the failed Airflow run", diagnosis_format.airflow_run_url(failed_dag_id, failed_dag_run_id)),
        ]
        blocks, fallback_text = diagnosis_format.build_incident_blocks(
            severity="ticket", dag_id=failed_dag_id, run_id=failed_dag_run_id,
            sections=sections, owner_mention="", links=links,
            prior_tickets=prior_incidents.get("tickets", []),
        )
        harness_audit.guard_action(
            governor, audit, kind="messages_sent", action="create_ticket_low_priority",
            incident_id=incident_id, job_id=failed_dag_run_id,
        )
        SlackHook(slack_conn_id="slack_api").call(
            "chat.postMessage", json={"channel": channel, "blocks": blocks, "text": fallback_text}
        )

        audit.record(
            agent_id=harness_audit.DAG_ID, action="create_ticket_low_priority", decision="allowed",
            incident_id=incident_id, cohort=harness_audit.COHORT, job_id=failed_dag_run_id,
            outputs={"ticket_key": ticket["key"], "ticket_url": ticket["url"]},
        )
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

        # Which branch ran is decided by which XCom is non-None (skipped tasks yield None) — NOT by
        # whether a PR URL came back. open_pr degrades to {"url": None} when github_api isn't
        # configured, and keying off the URL would mislabel that still-took-the-fix-path run as an
        # escalation in the audit trail.
        path = "fix" if fix_result is not None else ("ticket" if ticket else "escalate")
        audit = harness_audit.new_audit_log()
        audit.record(
            agent_id=harness_audit.DAG_ID,
            action="record_incident",
            decision="allowed",
            incident_id=f"{failed_dag_id}:{failed_dag_run_id}",
            cohort=harness_audit.COHORT,
            job_id=failed_dag_run_id,
            outputs={"path": path},
        )

    ctx = gather_context()
    prior = recall_prior_incidents(ctx)

    classification = classify_platform(ctx)
    diag_aws = investigate_aws(ctx, prior)
    diag_api = investigate_api(ctx, prior)
    diag_snowflake = investigate_snowflake(ctx, prior)
    classification >> [diag_aws, diag_api, diag_snowflake]

    diagnosis = merge_diagnosis(diag_aws, diag_api, diag_snowflake)
    cross_ref_note = check_cross_pipeline_reference(diagnosis, ctx)
    blast_radius_note = check_blast_radius(ctx)

    decision = decide_path(diagnosis, ctx, cross_ref_note, blast_radius_note)
    proposed_fix = propose_code_fix(ctx, diagnosis)
    escalate_done = urgent_slack_post(diagnosis, ctx["failed_dag_id"], ctx["failed_dag_run_id"], prior)
    ticket = create_ticket_low_priority(diagnosis, ctx["failed_dag_id"], ctx["failed_dag_run_id"], prior)
    decision >> [proposed_fix, escalate_done, ticket]

    validated_fix = validate_proposed_fix(proposed_fix, ctx["failed_dag_id"], ctx["failed_dag_run_id"])
    pr = open_pr(validated_fix, diagnosis, ctx["failed_dag_id"], ctx["failed_dag_run_id"])
    fix_result = notify_slack_fix(pr, diagnosis, ctx["failed_dag_id"], ctx["failed_dag_run_id"])

    record_incident(
        diagnosis, ctx["failed_dag_id"], ctx["failed_dag_run_id"], fix_result, ticket, escalate_done
    )


agentic_incident_memory_v2()
