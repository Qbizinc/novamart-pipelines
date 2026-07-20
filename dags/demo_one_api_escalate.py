"""
## Demo One — API Timeout -> RAG-Assisted Diagnosis -> Tagged Slack Escalation

Single-task pipeline that always fails calling a mock API with a real requests.Timeout. Tagged
**critical**, and a timeout is an external-service problem, not a bug in this DAG's own code — so
decide_path routes to urgent_slack_post (tags NOVAMART_INCIDENT_OWNER_SLACK_ID), not a PR or a
ticket.

Seeded with one prior incident (see include/incident_memory.py's SEED_INCIDENTS — run the
novamart_incident_memory_seed DAG once) so recall_prior_incidents finds a real semantic match on
the very first real run, instead of demoing an empty-memory pipeline.

On any failure this DAG triggers agentic_snowflake_incident_memory_v2 via trigger_incident_dag_v2.

Required Airflow Connections: airflow_api (used by the failure callback).

**How to trigger:** just run this DAG — it always fails the same way, no Variable to set.
"""

from datetime import datetime

import requests
from airflow.sdk import dag, task

from include.novamart_utils import trigger_incident_dag_v2


@dag(
    dag_id="demo_one_api_escalate",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "demo", "critical"],  # critical: escalate non-fixable failures
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def demo_one_api_escalate():

    @task
    def fetch_from_api() -> list[dict]:
        """Pull a batch of POS transactions from sales_api. Always times out."""
        raise requests.exceptions.Timeout(
            "sales_api did not respond within 5s (demo_one_api_escalate)"
        )

    fetch_from_api()


demo_one_api_escalate()
