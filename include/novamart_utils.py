"""
Shared utilities for NovaMart pipeline DAGs.
"""

from datetime import datetime, timezone

import requests
from airflow.providers.http.hooks.http import HttpHook


def trigger_incident_dag(context) -> None:
    """Task on_failure_callback — fires agentic_snowflake_incident via the Airflow REST API."""
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
            f"{base_url}/api/v2/dags/agentic_snowflake_incident/dagRuns",
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
