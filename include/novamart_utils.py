"""
Shared utilities for NovaMart pipeline DAGs.
"""

import os

import requests


# Map each pipeline DAG to the service name the incident DAG understands
_DAG_TO_PIPELINE = {
    "novamart_daily_sales": "sales",
    "novamart_customer_loyalty": "customer",
    "novamart_marketing_ads": "marketing",
}

_AIRFLOW_URL = os.environ.get("AIRFLOW_VAR_AIRFLOW_BASE_URL", "http://localhost:8080")
_AIRFLOW_USER = os.environ.get("AIRFLOW_VAR_AIRFLOW_ADMIN_USER", "admin")
_AIRFLOW_PASSWORD = os.environ.get("AIRFLOW_VAR_AIRFLOW_ADMIN_PASSWORD", "admin")


def trigger_incident_dag(context) -> None:
    """
    on_failure_callback that fires the agentic_incident_dag whenever a
    NovaMart pipeline task fails. Passes the failed DAG/run/task info so
    the agent can pull Airflow logs as part of its investigation.
    """
    dag_run = context["dag_run"]
    task_instance = context["task_instance"]
    dag_id = dag_run.dag_id

    pipeline = _DAG_TO_PIPELINE.get(dag_id, "all")

    conf = {
        "pipeline": pipeline,
        "dag_id": dag_id,
        "run_id": dag_run.run_id,
        "failed_task": task_instance.task_id,
    }

    try:
        response = requests.post(
            f"{_AIRFLOW_URL}/api/v2/dags/agentic_incident_dag/dagRuns",
            json={"conf": conf},
            auth=(_AIRFLOW_USER, _AIRFLOW_PASSWORD),
            timeout=10,
        )
        response.raise_for_status()
        print(f"[incident] Triggered agentic_incident_dag for {dag_id} — run_id: {response.json().get('run_id')}")
    except Exception as exc:
        # Log but don't re-raise — we don't want the callback itself to obscure the original failure
        print(f"[incident] WARNING: Failed to trigger agentic_incident_dag: {exc}")
