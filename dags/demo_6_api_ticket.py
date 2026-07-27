"""Demo 6 — API Ticket. Fetches sales_api; toggle it unhealthy to fail -> routes to a low-priority Jira ticket."""

from datetime import datetime

import requests
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag_v2

SALES_API_DEFAULT = "http://host.docker.internal:5001"


@dag(
    dag_id="demo_6_api_ticket",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "api", "demo"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def demo_6_api_ticket():

    @task
    def fetch_transactions() -> list[dict]:
        base_url = Variable.get("MOCK_SALES_API_URL", default=SALES_API_DEFAULT)
        response = requests.get(f"{base_url}/api/v1/sales", timeout=30)
        response.raise_for_status()
        transactions = response.json()["transactions"]
        print(f"Fetched {len(transactions)} transactions from sales_api.")
        return transactions

    fetch_transactions()


demo_6_api_ticket()
