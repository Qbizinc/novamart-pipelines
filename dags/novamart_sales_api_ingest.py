"""Sales API Ingest. Fetches sales_api transactions; failures route to a low-priority Jira ticket."""

from datetime import datetime

import requests
from airflow.sdk import Variable, dag, task
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from include.incident_callbacks import trigger_incident_dag_v2

SALES_API_DEFAULT = "http://host.docker.internal:5001"


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

        session = requests.Session()
        retry = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["GET"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        session.mount("http://", adapter)
        session.mount("https://", adapter)

        try:
            response = session.get(f"{base_url}/api/v1/sales", timeout=30)
            response.raise_for_status()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            raise RuntimeError(f"sales_api request failed with HTTP {status}") from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"sales_api request failed: {type(exc).__name__}") from exc
        transactions = response.json()["transactions"]
        print(f"Fetched {len(transactions)} transactions from sales_api.")
        return transactions

    fetch_transactions()


novamart_sales_api_ingest()