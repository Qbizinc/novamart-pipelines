"""
## Agentic Incident DAG

Trigger this DAG manually from the Airflow UI whenever you want to investigate
NovaMart pipeline failures. No conf needed — the DAG scans all NovaMart pipelines
for recent failures, investigates each one, and posts a diagnosis to Slack.

### Required Airflow Variables
- `SLACK_BOT_TOKEN` — Slack bot token
- `SLACK_INCIDENT_CHANNEL` — channel to post to
- `ANTHROPIC_API_KEY` — fallback if not in env
- `AIRFLOW_BASE_URL` — Airflow REST API base (default: http://localhost:8080)
- `MOCK_SALES_API_URL` — (default: http://host.docker.internal:5001)
- `MOCK_CUSTOMER_API_URL` — (default: http://host.docker.internal:5002)
- `MOCK_MARKETING_API_URL` — (default: http://host.docker.internal:5003)
"""

import json
import os
from datetime import datetime

import requests as http_requests
from airflow.sdk import Variable
from airflow.sdk import dag, task

# ---------------------------------------------------------------------------
# Pipeline registry — maps service names to their mock API config
# ---------------------------------------------------------------------------

PIPELINES: dict[str, dict] = {
    "sales": {
        "name": "Daily Sales (POS)",
        "url_var": "MOCK_SALES_API_URL",
        "default_url": "http://host.docker.internal:5001",
        "endpoint": "/api/v1/sales",
        "headers": {},
    },
    "customer": {
        "name": "Customer / Loyalty (CRM)",
        "url_var": "MOCK_CUSTOMER_API_URL",
        "default_url": "http://host.docker.internal:5002",
        "endpoint": "/api/v1/customers",
        "headers": {"Authorization": "Bearer nvmt_live_token_2026"},
    },
    "marketing": {
        "name": "Marketing Ads API",
        "url_var": "MOCK_MARKETING_API_URL",
        "default_url": "http://host.docker.internal:5003",
        "endpoint": "/api/v1/campaigns",
        "headers": {},
    },
}

# Known NovaMart pipeline DAG IDs — used when scanning for failures
PIPELINES_DAG_IDS = [
    "novamart_daily_sales",
    "novamart_customer_loyalty",
    "novamart_marketing_ads",
]

# ---------------------------------------------------------------------------
# Tool definitions for the Claude agent
# ---------------------------------------------------------------------------

AGENT_TOOLS = [
    {
        "name": "get_dag_runs",
        "description": "List recent runs for a specific Airflow DAG to inspect its failure history.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string", "description": "The Airflow DAG ID to inspect."},
                "limit": {"type": "integer", "default": 5, "description": "Number of runs to return."},
            },
            "required": ["dag_id"],
        },
    },
    {
        "name": "get_task_instances",
        "description": "List all task instances for a DAG run, showing which tasks failed.",
        "input_schema": {
            "type": "object",
            "properties": {
                "dag_id": {"type": "string"},
                "dag_run_id": {"type": "string"},
            },
            "required": ["dag_id", "dag_run_id"],
        },
    },
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
]

# ---------------------------------------------------------------------------
# Tool executor — called during the agent loop
# ---------------------------------------------------------------------------

def _get_airflow_headers() -> dict:
    """Get a JWT auth header for the Airflow REST API."""
    airflow_url = Variable.get("AIRFLOW_BASE_URL", default="http://host.docker.internal:8080")
    r = http_requests.post(
        f"{airflow_url}/auth/token",
        json={
            "username": Variable.get("AIRFLOW_ADMIN_USER", default="admin"),
            "password": Variable.get("AIRFLOW_ADMIN_PASSWORD", default="admin"),
        },
        timeout=10,
    )
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['access_token']}"}


def _execute_tool(tool_name: str, tool_input: dict) -> str:
    """Dispatch a tool call and return the result as a JSON string."""
    airflow_url = Variable.get("AIRFLOW_BASE_URL", default="http://host.docker.internal:8080")
    headers = _get_airflow_headers()

    if tool_name == "get_dag_runs":
        dag_id = tool_input["dag_id"]
        limit = tool_input.get("limit", 5)
        try:
            r = http_requests.get(
                f"{airflow_url}/api/v2/dags/{dag_id}/dagRuns",
                params={"limit": limit, "order_by": "-start_date"},
                headers=headers,
                timeout=10,
            )
            return json.dumps(r.json())
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    if tool_name == "get_task_instances":
        dag_id = tool_input["dag_id"]
        run_id = tool_input["dag_run_id"]
        try:
            r = http_requests.get(
                f"{airflow_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances",
                headers=headers,
                timeout=10,
            )
            instances = r.json().get("task_instances", [])
            summary = [
                {"task_id": t["task_id"], "state": t["state"], "duration": t.get("duration")}
                for t in instances
            ]
            return json.dumps(summary)
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    if tool_name == "get_task_logs":
        dag_id = tool_input["dag_id"]
        run_id = tool_input["dag_run_id"]
        task_id = tool_input["task_id"]
        try:
            r = http_requests.get(
                f"{airflow_url}/api/v2/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/1",
                headers=headers,
                timeout=15,
            )
            return r.text[:4000]
        except Exception as exc:
            return json.dumps({"error": str(exc)})

    return json.dumps({"error": f"Unknown tool: {tool_name}"})


# ---------------------------------------------------------------------------
# DAG
# ---------------------------------------------------------------------------

@dag(
    dag_id="agentic_incident_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "agentic", "incident"],
)
def agentic_incident_dag():

    @task
    def gather_context() -> dict:
        """Scan all NovaMart DAGs for recent failures and snapshot health of affected services."""
        airflow_url = Variable.get("AIRFLOW_BASE_URL", default="http://host.docker.internal:8080")

        # Airflow 3 uses JWT auth — get a token first
        token_r = http_requests.post(
            f"{airflow_url}/auth/token",
            json={
                "username": Variable.get("AIRFLOW_ADMIN_USER", default="admin"),
                "password": Variable.get("AIRFLOW_ADMIN_PASSWORD", default="admin"),
            },
            timeout=10,
        )
        token_r.raise_for_status()
        jwt_token = token_r.json()["access_token"]
        headers = {"Authorization": f"Bearer {jwt_token}"}

        failed_runs: dict = {}
        for dag_id in PIPELINES_DAG_IDS:
            try:
                url = f"{airflow_url}/api/v2/dags/{dag_id}/dagRuns"
                r = http_requests.get(
                    url,
                    params={"state": "failed", "limit": 1, "order_by": "-start_date"},
                    headers=headers,
                    timeout=10,
                )
                print(f"[gather_context] {dag_id} → HTTP {r.status_code}: {r.text[:300]}")
                runs = r.json().get("dag_runs", [])
                if runs:
                    run = runs[0]
                    failed_runs[dag_id] = {"run_id": run["dag_run_id"], "start_date": run["start_date"]}
                else:
                    print(f"[gather_context] {dag_id} → no failed runs found")
            except Exception as exc:
                print(f"[gather_context] {dag_id} → ERROR: {exc}")

        print(f"[gather_context] failed_runs: {failed_runs}")

        if not failed_runs:
            raise ValueError(
                f"No failed runs found across NovaMart DAGs. "
                f"Airflow URL used: {airflow_url}. Check task logs for API response details."
            )

        dag_to_pipeline = {
            "novamart_daily_sales": "sales",
            "novamart_customer_loyalty": "customer",
            "novamart_marketing_ads": "marketing",
        }
        affected_services = [dag_to_pipeline[d] for d in failed_runs if d in dag_to_pipeline]

        return {
            "failed_runs": failed_runs,
            "affected_services": affected_services,
        }

    @task
    def run_agent(ctx: dict) -> dict:
        """Run the Claude agent with Airflow + mock API tools to diagnose the incident."""
        import anthropic

        api_key = os.environ.get("ANTHROPIC_API_KEY") or Variable.get("ANTHROPIC_API_KEY")
        client = anthropic.Anthropic(api_key=api_key)

        failed_runs = ctx["failed_runs"]
        affected_services = ctx["affected_services"]

        system_prompt = (
            "You are a data platform reliability engineer at NovaMart, a retail company. "
            "You have been dispatched to investigate one or more failed data pipelines.\n\n"
            "Your investigation approach:\n"
            "1. For each failed DAG run, call get_task_instances to identify which task failed.\n"
            "2. Call get_task_logs on the failed task to read the actual error message and traceback.\n"
            "3. Based on what you read in the logs, determine the root cause.\n"
            "4. Investigate each failed pipeline separately.\n\n"
            "IMPORTANT: Never ask the user for more information. You have all the tools you need. "
            "If you cannot retrieve logs, say so in the diagnosis and explain what you could not access.\n\n"
            "Once you have enough evidence, write your conclusion using EXACTLY this format "
            "(one block per failed pipeline):\n\n"
            "--- [DAG NAME] ---\n"
            "DIAGNOSIS: <what went wrong>\n"
            "ROOT CAUSE: <why it happened>\n"
            "RECOMMENDED FIX: <concrete steps>\n"
            "CONFIDENCE: <High / Medium / Low>"
        )

        user_message = (
            f"The following NovaMart DAG runs have failed:\n{json.dumps(failed_runs, indent=2)}\n\n"
            "Investigate each one by reading the task logs and produce a diagnosis."
        )

        messages = [{"role": "user", "content": user_message}]

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
                diagnosis = next(
                    (b.text for b in response.content if hasattr(b, "text")), ""
                )
                return {"diagnosis": diagnosis, "affected_services": ctx["affected_services"]}

            if response.stop_reason == "tool_use":
                tool_results = []
                for block in response.content:
                    if block.type == "tool_use":
                        result = _execute_tool(block.name, block.input)
                        tool_results.append({
                            "type": "tool_result",
                            "tool_use_id": block.id,
                            "content": result,
                        })
                messages.append({"role": "user", "content": tool_results})

        return {
            "diagnosis": "Agent reached the iteration limit without a conclusion.",
            "affected_services": ctx["affected_services"],
        }

    @task
    def post_to_slack(result: dict) -> None:
        """Post the agent's diagnosis to the Slack incident channel."""
        from slack_sdk import WebClient

        token = os.environ.get("SLACK_BOT_TOKEN") or Variable.get("SLACK_BOT_TOKEN")
        channel = Variable.get("SLACK_INCIDENT_CHANNEL", default="#data-incidents")

        slack = WebClient(token=token)
        services = result.get("affected_services", [])
        label = ", ".join(s.upper() for s in services) if services else "UNKNOWN"
        diagnosis = result.get("diagnosis", "No diagnosis produced.")

        header_ts = slack.chat_postMessage(
            channel=channel,
            text=f":rotating_light: *NovaMart Incident — {label}*",
        )["ts"]

        slack.chat_postMessage(
            channel=channel,
            text=f"```{diagnosis}```",
            thread_ts=header_ts,
        )

    ctx = gather_context()
    result = run_agent(ctx)
    post_to_slack(result)


agentic_incident_dag()
