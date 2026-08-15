"""Sales API Ingest. Fetches sales_api transactions; failures route to a low-priority Jira ticket."""

import time
from datetime import datetime

import requests
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag_v2

SALES_API_DEFAULT = "http://host.docker.internal:5001"

MAX_ATTEMPTS = 3
INITIAL_DELAY_SECONDS = 1


@dag(
    dag_id="novamart_sales_api_ingest",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "api"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def novamart_sales_api_ingest():

    @task
    def fetch_transactions() -> list[dict]:
        base_url = Variable.get("SALES_API_URL", default=SALES_API_DEFAULT)
        delay = INITIAL_DELAY_SECONDS
        last_exc: Exception | None = None

        for attempt in range(1, MAX_ATTEMPTS + 1):
            try:
                response = requests.get(f"{base_url}/api/v1/sales", timeout=30)
                response.raise_for_status()
            except requests.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else "unknown"
                last_exc = RuntimeError(f"sales_api request failed with HTTP {status}")
                last_exc.__cause__ = exc
            except requests.RequestException as exc:
                last_exc = RuntimeError(f"sales_api request failed: {type(exc).__name__}")
                last_exc.__cause__ = exc
            else:
                transactions = response.json()["transactions"]
                print(f"Fetched {len(transactions)} transactions from sales_api.")
                return transactions

            if attempt < MAX_ATTEMPTS:
                print(
                    f"Attempt {attempt}/{MAX_ATTEMPTS} failed ({last_exc}); "
                    f"retrying in {delay}s."
                )
                time.sleep(delay)
                delay *= 2

        raise last_exc

    fetch_transactions()


novamart_sales_api_ingest()