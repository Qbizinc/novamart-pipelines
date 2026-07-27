"""
Shared utilities for NovaMart pipeline DAGs.

Note on retries: these are toy/demo DAGs used to showcase the agentic
incident-response pattern, not production pipelines. Their failures are
deterministic (a Variable/Connection was deliberately misconfigured for the
demo), so retrying wouldn't change the outcome — it would only delay
trigger_incident_dag firing and burn extra Snowflake/API/AWS calls for no
benefit. DAGs using trigger_incident_dag intentionally omit `retries` from
default_args (leaving it at the implicit default of 0) to keep the demo
fast and cheap to run.
"""

from datetime import datetime, timezone

import requests
from airflow.providers.http.hooks.http import HttpHook


def trigger_incident_dag(context) -> None:
    """Task on_failure_callback — fires agentic_snowflake_incident_memory via the Airflow REST API."""
    try:
        conn = HttpHook(http_conn_id="airflow_api").get_connection("airflow_api")
        base_url = f"{conn.schema or 'http'}://{conn.host}:{conn.port or 8080}"

        token_r = requests.post(
            f"{base_url}/auth/token",
            json={"username": conn.login, "password": conn.password},
            timeout=10,
        )
        token_r.raise_for_status()
        jwt = token_r.json()["access_token"]

        dag_run = context.get("dag_run")
        run_id = dag_run.run_id if dag_run else "unknown"
        failed_dag_id = context["dag"].dag_id
        resp = requests.post(
            f"{base_url}/api/v2/dags/agentic_snowflake_incident_memory/dagRuns",
            json={
                "dag_run_id": f"incident__{run_id}",
                "logical_date": datetime.now(timezone.utc).isoformat(),
                "conf": {"failed_dag_id": failed_dag_id, "failed_dag_run_id": run_id},
            },
            headers={"Authorization": f"Bearer {jwt}"},
            timeout=10,
        )
        print(f"[on_failure_callback] HTTP {resp.status_code}: {resp.text[:300]}")
    except Exception as exc:
        print(f"[on_failure_callback] Could not trigger incident DAG: {exc}")


def trigger_incident_dag_v2(context) -> None:
    """Task on_failure_callback — fires agentic_incident_memory_v2 (router prototype)."""
    try:
        conn = HttpHook(http_conn_id="airflow_api").get_connection("airflow_api")
        base_url = f"{conn.schema or 'http'}://{conn.host}:{conn.port or 8080}"

        token_r = requests.post(
            f"{base_url}/auth/token",
            json={"username": conn.login, "password": conn.password},
            timeout=10,
        )
        token_r.raise_for_status()
        jwt = token_r.json()["access_token"]

        dag_run = context.get("dag_run")
        run_id = dag_run.run_id if dag_run else "unknown"
        failed_dag_id = context["dag"].dag_id
        resp = requests.post(
            f"{base_url}/api/v2/dags/agentic_incident_memory_v2/dagRuns",
            json={
                "dag_run_id": f"incident__{run_id}",
                "logical_date": datetime.now(timezone.utc).isoformat(),
                "conf": {"failed_dag_id": failed_dag_id, "failed_dag_run_id": run_id},
            },
            headers={"Authorization": f"Bearer {jwt}"},
            timeout=10,
        )
        print(f"[on_failure_callback] HTTP {resp.status_code}: {resp.text[:300]}")
    except Exception as exc:
        print(f"[on_failure_callback] Could not trigger incident DAG: {exc}")
