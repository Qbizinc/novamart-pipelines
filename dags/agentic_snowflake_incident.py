"""
## Agentic Snowflake Incident DAG

Triggered automatically when novamart_snowflake_sales fails, or manually
from the UI. Claude investigates using Airflow task logs and live Snowflake
queries, opens a Jira ticket with a structured diagnosis, and posts to Slack.

### Required Airflow Variables
- SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
- SNOWFLAKE_DATABASE, SNOWFLAKE_SCHEMA, SNOWFLAKE_WAREHOUSE, SNOWFLAKE_ROLE
- JIRA_URL, JIRA_EMAIL, JIRA_API_TOKEN, JIRA_PROJECT_KEY
- SLACK_BOT_TOKEN, SLACK_INCIDENT_CHANNEL
- ANTHROPIC_API_KEY
- AIRFLOW_BASE_URL, AIRFLOW_ADMIN_USER, AIRFLOW_ADMIN_PASSWORD
"""

import base64
import json
import os
from datetime import datetime

import requests
import snowflake.connector
from airflow.sdk import Variable, dag, task

# ---------------------------------------------------------------------------
# Tool definitions for the Claude agent
# ---------------------------------------------------------------------------

AGENT_TOOLS = [
    {
        "name": "get_task_logs",
        "description": "Retrieve logs for a specific failed Airflow task instance.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "dag_run_id": {"type": "string"},
                "task_id": {"type": "string"},
            },
            "required": ["dag_id", "dag_run_id", "task_id"],
        },
    },
    {
        "name": "query_snowflake",
        "description": (
            "Execute a read-only SQL query against the NovaMart Snowflake database. "
            "Use to inspect table schema (DESCRIBE TABLE), row counts, data freshness "
            "(MAX loaded_at), and samples. Only SELECT, DESCRIBE, and SHOW are allowed."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "sql": {"type": "string", "description": "Read-only SQL (SELECT / DESCRIBE / SHOW)."},
            },
            "required": ["sql"],
        },
    },
    {
        "name": "create_jira_ticket",
        "description": (
            "Create a Jira bug ticket to track this incident. "
            "Call this exactly once after you have a complete diagnosis."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "summary": {"type": "string", "description": "Short one-line ticket title."},
                "description": {
                    "type": "string",
                    "description": (
                        "Full diagnosis using this format:\n"
                        "[DIAGNOSIS] what went wrong\n"
                        "[ROOT CAUSE] why it happened\n"
                        "[IMPACT] what data is missing or corrupted\n"
                        "[RECOMMENDED FIX] concrete steps to resolve"
                    ),
                },
            },
            "required": ["summary", "description"],
        },
    },
]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _airflow_headers() -> dict:
    airflow_url = Variable.get("AIRFLOW_BASE_URL", default="http://host.docker.internal:8080")
    r = requests.post(
        f"{airflow_url}/auth/token",
        json={
            "username": Variable.get("AIRFLOW_ADMIN_USER", default="admin"),
            "password": Variable.get("AIRFLOW_ADMIN_PASSWORD", default="admin"),
        },
        timeout=10,
    )
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _snowflake_conn():
    return snowflake.connector.connect(
        account=Variable.get("SNOWFLAKE_ACCOUNT"),
        user=Variable.get("SNOWFLAKE_USER"),
        private_key_file=Variable.get("SNOWFLAKE_PRIVATE_KEY_PATH"),
        database=Variable.get("SNOWFLAKE_DATABASE"),
        schema=Variable.get("SNOWFLAKE_SCHEMA", default="NOVAMART_RAW"),
        warehouse=Variable.get("SNOWFLAKE_WAREHOUSE"),
        role=Variable.get("SNOWFLAKE_ROLE"),
    )


def _execute_tool(tool_name: str, tool_input: dict) -> str:
    airflow_url = Variable.get("AIRFLOW_BASE_URL", default="http://host.docker.internal:8080")

    if tool_name == "get_task_logs":
        dag_id = tool_input["dag_id"]
        run_id = tool_input["dag_run_id"]
        task_id = tool_input["task_id"]
        try:
            headers = _airflow_headers()
            r = requests.get(
                f"{airflow_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1",
                headers=headers,
                timeout=15,
            )
            return r.text[:4000]
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    if tool_name == "query_snowflake":
        sql = tool_input["sql"].strip()
        first_word = sql.split()[0].upper() if sql else ""
        if first_word not in ("SELECT", "DESCRIBE", "DESC", "SHOW"):
            return json.dumps({"error": "Only SELECT, DESCRIBE, and SHOW statements are permitted."})
        try:
            conn = _snowflake_conn()
            try:
                cur = conn.cursor()
                cur.execute(sql)
                rows = cur.fetchmany(50)
                cols = [desc[0] for desc in cur.description] if cur.description else []
                return json.dumps({"columns": cols, "rows": [list(r) for r in rows]}, default=str)
            finally:
                conn.close()
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    if tool_name == "create_jira_ticket":
        jira_url = Variable.get("JIRA_URL")
        email = Variable.get("JIRA_EMAIL")
        token = Variable.get("JIRA_API_TOKEN")
        project = Variable.get("JIRA_PROJECT_KEY", default="DATA")
        creds = base64.b64encode(f"{email}:{token}".encode()).decode()
        try:
            r = requests.post(
                f"{jira_url}/rest/api/3/issue",
                headers={"Authorization": f"Basic {creds}", "Content-Type": "application/json"},
                json={
                    "fields": {
                        "project": {"key": project},
                        "summary": tool_input["summary"],
                        "description": {
                            "type": "doc",
                            "version": 1,
                            "content": [
                                {
                                    "type": "paragraph",
                                    "content": [{"type": "text", "text": tool_input["description"]}],
                                }
                            ],
                        },
                        "issuetype": {"name": "Bug"},
                    }
                },
                timeout=15,
            )
            r.raise_for_status()
            data = r.json()
            ticket_url = f"{jira_url}/browse/{data['key']}"
            print(f"[create_jira_ticket] Created {data['key']}: {ticket_url}")
            return json.dumps({"key": data["key"], "url": ticket_url})
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    return json.dumps({"error": f"Unknown tool: {tool_name}"})


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------


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
    def gather_context() -> dict:
        """Find the most recent failed novamart_snowflake_sales run and its failed tasks."""
        failed_dag_id = "novamart_snowflake_sales"
        airflow_url = Variable.get("AIRFLOW_BASE_URL", default="http://host.docker.internal:8080")
        headers = _airflow_headers()

        r = requests.get(
            f"{airflow_url}/api/v2/dags/{failed_dag_id}/dagRuns",
            params={"state": "failed", "limit": 1, "order_by": "-start_date"},
            headers=headers,
            timeout=10,
        )
        runs = r.json().get("dag_runs", [])
        if not runs:
            raise ValueError(f"No failed runs found for {failed_dag_id}.")

        failed_dag_run_id = runs[0]["dag_run_id"]

        r2 = requests.get(
            f"{airflow_url}/api/v2/dags/{failed_dag_id}/dagRuns/{failed_dag_run_id}/taskInstances",
            headers=headers,
            timeout=10,
        )
        instances = r2.json().get("task_instances", [])
        failed_tasks = [
            {"task_id": t["task_id"], "state": t["state"]}
            for t in instances
            if t["state"] == "failed"
        ]

        print(f"[gather_context] run={failed_dag_run_id}, failed_tasks={failed_tasks}")
        return {
            "failed_dag_id": failed_dag_id,
            "failed_dag_run_id": failed_dag_run_id,
            "failed_tasks": failed_tasks,
        }

    @task
    def run_agent(ctx: dict) -> dict:
        """Claude investigates via Airflow logs + Snowflake queries, then opens a Jira ticket."""
        import anthropic

        api_key = os.environ.get("ANTHROPIC_API_KEY") or Variable.get("ANTHROPIC_API_KEY")
        client = anthropic.Anthropic(api_key=api_key)

        system_prompt = (
            "You are a data platform reliability engineer at NovaMart. "
            "A pipeline that loads daily sales data into Snowflake has failed.\n\n"
            "Investigation steps:\n"
            "1. Call get_task_logs for each failed task to read the exact error and traceback.\n"
            "2. Call query_snowflake to inspect the current table state:\n"
            "   - DESCRIBE TABLE DAILY_SALES  (check for schema drift or missing columns)\n"
            "   - SELECT COUNT(*), MAX(loaded_at) FROM DAILY_SALES  (check freshness and row count)\n"
            "3. Determine the root cause from the evidence.\n"
            "4. Call create_jira_ticket exactly once with a complete, structured description.\n\n"
            "IMPORTANT: Never ask for more information. Use only the tools provided. "
            "If a tool call fails, note it and continue with available evidence."
        )

        user_message = (
            f"novamart_snowflake_sales has failed.\n"
            f"DAG run ID: {ctx['failed_dag_run_id']}\n"
            f"Failed tasks: {json.dumps(ctx['failed_tasks'])}\n\n"
            "Investigate and open a Jira ticket with your findings."
        )

        messages = [{"role": "user", "content": user_message}]
        jira_ticket = None

        for _ in range(12):
            response = client.messages.create(
                model="claude-sonnet-4-6",
                max_tokens=2048,
                system=system_prompt,
                tools=AGENT_TOOLS,
                messages=messages,
            )
            messages.append({"role": "assistant", "content": response.content})

            if response.stop_reason == "end_turn":
                diagnosis = next((b.text for b in response.content if hasattr(b, "text")), "")
                return {"diagnosis": diagnosis, "jira_ticket": jira_ticket}

            if response.stop_reason == "tool_use":
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        result = _execute_tool(block.name, block.input)
                        if block.name == "create_jira_ticket":
                            parsed = json.loads(result)
                            if "key" in parsed:
                                jira_ticket = parsed
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })
                messages.append({"role": "user", "content": tool_results})

        return {"diagnosis": "Agent reached the iteration limit without a conclusion.", "jira_ticket": jira_ticket}

    @task
    def post_to_slack(result: dict) -> None:
        """Post the diagnosis and Jira ticket link to the Slack incident channel."""
        from slack_sdk import WebClient

        token = os.environ.get("SLACK_BOT_TOKEN") or Variable.get("SLACK_BOT_TOKEN")
        channel = Variable.get("SLACK_INCIDENT_CHANNEL", default="#data-incidents")
        slack = WebClient(token=token)

        jira = result.get("jira_ticket")
        jira_line = f"  |  :jira: <{jira['url']}|{jira['key']}>" if jira else ""

        header_ts = slack.chat_postMessage(
            channel=channel,
            text=f":rotating_light: *NovaMart — Snowflake Sales Pipeline Failure*{jira_line}",
        )["ts"]

        slack.chat_postMessage(
            channel=channel,
            text=f"```{result.get('diagnosis', 'No diagnosis produced.')}```",
            thread_ts=header_ts,
        )

    ctx = gather_context()
    result = run_agent(ctx)
    post_to_slack(result)


agentic_snowflake_incident()
