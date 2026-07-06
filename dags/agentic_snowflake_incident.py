"""
## Agentic Snowflake Incident DAG

Triggered automatically when novamart_snowflake_sales fails, or manually
from the UI. Claude investigates using pre-fetched Airflow task logs and
live Snowflake queries; the DAG then deterministically opens a Jira ticket
from that diagnosis and posts to Slack.

Uses apache-airflow-providers-common-ai:
- @task.agent  — LLM agent loop (pydantic-ai, Anthropic backend), used only
  for investigation (Snowflake queries). Ticket creation and Slack posting
  are plain, deterministic tasks that run strictly after the investigation
  finishes — not decisions the agent makes, so they happen exactly once.
- SQLToolset   — Snowflake queries via snowflake_default connection

### Required Airflow Connections (set in airflow_settings.yaml)
- snowflake_default : Snowflake, key-pair auth
- jira_api          : HTTP, qbizinc.atlassian.net, Basic auth
- slack_api         : Slack, bot token
- airflow_api       : HTTP, host.docker.internal:8080, admin/admin

### Required environment variable (set in .env, no AIRFLOW_VAR_ prefix)
- ANTHROPIC_API_KEY  — read directly by the Anthropic SDK

### Required Airflow Variable (set in airflow_settings.yaml)
- SLACK_INCIDENT_CHANNEL
"""

import json
from datetime import datetime

import requests
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.slack.hooks.slack import SlackHook
from airflow.sdk import Variable, dag, task

# Register @task.agent with the airflow.sdk task namespace
from airflow.providers.common.ai.decorators import agent as _agent_decorator  # noqa: F401
from airflow.providers.common.ai.toolsets.sql import SQLToolset


@dag(
    dag_id="agentic_snowflake_incident",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "agentic", "incident", "snowflake"],
)
def agentic_snowflake_incident():

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

        print(f"[gather_context] dag={failed_dag_id}, run={failed_run_id}, "
              f"failed={[t['task_id'] for t in failed_tasks]}")
        return {
            "failed_dag_id": failed_dag_id,
            "failed_dag_run_id": failed_run_id,
            "failed_tasks": failed_tasks,
            "task_logs": task_logs,
            "dag_source": dag_source,
        }

    @task.agent(
        toolsets=[
            SQLToolset(db_conn_id="snowflake_default"),
        ],
        llm_conn_id="pydanticai_default",
        model_id="anthropic:claude-sonnet-4-6",
    )
    def investigate(ctx: dict) -> str:
        """Return the prompt for the agent based on the gathered context.

        This agent only has the SQL tool — it cannot create a Jira ticket or post to
        Slack itself. Those happen in separate, deterministic tasks that run strictly
        after this one finishes, so the investigation always completes before any
        ticket/message is created, and exactly once.
        """
        tasks_summary = "\n".join(
            f"  - {t['task_id']} ({t['state']})" for t in ctx["failed_tasks"]
        )
        logs_section = "\n\n".join(
            f"=== {tid} ===\n{log}" for tid, log in ctx.get("task_logs", {}).items()
        )
        return (
            f"{ctx['failed_dag_id']} has failed.\n"
            f"DAG run ID: {ctx['failed_dag_run_id']}\n"
            f"Failed tasks:\n{tasks_summary}\n\n"
            f"Task logs:\n{logs_section}\n\n"
            f"Pipeline source code ({ctx['failed_dag_id']}.py):\n"
            f"```python\n{ctx.get('dag_source', '')}\n```\n\n"
            "Investigation steps:\n"
            "1. Read the pipeline source code above first. Identify exactly how each write/"
            "match/dedup key is constructed (e.g. is it a stable business key, or something "
            "regenerated fresh on every run?). Your root cause must be grounded in what the "
            "code actually does, not just inferred from the shape of the data — a plausible-"
            "looking guess (e.g. \"missing a GROUP BY\") is wrong if the code doesn't show that.\n"
            "2. Use the SQL tool to check for schema drift on any table referenced in the "
            "failed task logs, e.g.:\n"
            "   DESCRIBE TABLE SANDBOX_DATA_PIPELINE.NOVAMART_RAW.<table_name>\n"
            "3. Use the SQL tool to check data freshness/state on that table, e.g.:\n"
            "   SELECT COUNT(*), MAX(loaded_at) FROM SANDBOX_DATA_PIPELINE.NOVAMART_RAW.<table_name>\n"
            "4. Use the Snowflake evidence to confirm (or rule out) the mechanism you identified "
            "in step 1 — the data pattern should match what the code predicts, not just look "
            "superficially similar.\n"
            "5. Return your findings as plain structured text, in exactly this format "
            "(this is the final answer — you have no other tools to call after this):\n"
            "   [SUMMARY] one-line ticket title\n"
            "   [DIAGNOSIS] what went wrong\n"
            "   [ROOT CAUSE] why it happened\n"
            "   [IMPACT] what data is missing or affected\n"
            "   [RECOMMENDED FIX] concrete steps to resolve"
        )

    @task
    def create_jira_ticket(diagnosis: str, failed_dag_id: str) -> dict:
        """Create exactly one Jira Bug ticket from the agent's diagnosis.

        Deterministic, not an agent decision — runs once, strictly after investigate()
        completes, so ticket creation can never be repeated or interleaved with the
        investigation the way it was when the agent held its own Jira tool.
        """
        summary = f"{failed_dag_id} pipeline failure"
        body = diagnosis
        if diagnosis.startswith("[SUMMARY]"):
            first_line, _, rest = diagnosis.partition("\n")
            summary = first_line.removeprefix("[SUMMARY]").strip() or summary
            body = rest.strip()

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
        print(f"[create_jira_ticket] Created {ticket['key']}: {ticket['url']}")
        return ticket

    @task
    def post_to_slack(diagnosis: str, failed_dag_id: str, ticket: dict) -> None:
        """Post the agent's diagnosis, with the Jira ticket link, to the Slack incident channel."""
        slack = SlackHook(slack_conn_id="slack_api")
        channel = Variable.get("SLACK_INCIDENT_CHANNEL", default="#qbiz_slackbot_testing")
        jira_line = f"  |  <{ticket['url']}|{ticket['key']}>" if ticket.get("key") else ""

        header_ts = slack.call(
            "chat.postMessage",
            json={
                "channel": channel,
                "text": f":rotating_light: *NovaMart — {failed_dag_id} Pipeline Failure*{jira_line}",
            },
        )["ts"]

        slack.call(
            "chat.postMessage",
            json={
                "channel": channel,
                "text": f"```{diagnosis[:3800]}```",
                "thread_ts": header_ts,
            },
        )

    ctx = gather_context()
    diagnosis = investigate(ctx)
    ticket = create_jira_ticket(diagnosis, ctx["failed_dag_id"])
    post_to_slack(diagnosis, ctx["failed_dag_id"], ticket)


agentic_snowflake_incident()
