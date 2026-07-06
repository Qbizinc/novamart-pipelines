"""
## NovaMart API Pagination — Incident Demo

Fetches paginated transactions from the NovaMart transactions mock API and
loads them into Snowflake. The paginator expects the API's page size to stay
at 1,000 records/page: it keeps requesting pages until it gets back a page
shorter than 1,000, which it treats as the last page.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- MOCK_TRANSACTIONS_API_URL : base URL of the transactions mock API

**Failure mode — silent volume anomaly, no error raised:**
The mock API silently reduced its page size from 1,000 to 100. Since every
page now comes back shorter than the paginator's expected 1,000, it treats
the very first page as the last one and stops — loading roughly 10% of the
day's records with no exception anywhere. `validate_volume` is the only
thing standing between this and a silently truncated table: it compares
the number of records actually loaded against the `total_count` the API
reports in its response metadata and raises if they don't match.

**How to trigger:**
1. On the mock API service, set the environment variable
   `MOCK_API_PAGE_SIZE=100` and restart/reload the service.
2. Trigger this DAG manually. `fetch_all_transactions` will stop after the
   first (short) page, and `validate_volume` will raise a `ValueError`
   because loaded count << reported total_count.
3. Unset `MOCK_API_PAGE_SIZE` (or set back to 1000) to restore health.
"""

from datetime import datetime

import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.novamart_utils import trigger_incident_dag

TRANSACTIONS_API_DEFAULT = "http://host.docker.internal:5004"
EXPECTED_PAGE_SIZE = 1000


@dag(
    dag_id="novamart_api_pagination",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "api"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_api_pagination():

    @task
    def fetch_all_transactions() -> dict:
        """Page through the mock API until a page shorter than EXPECTED_PAGE_SIZE is seen."""
        base_url = Variable.get("MOCK_TRANSACTIONS_API_URL", default=TRANSACTIONS_API_DEFAULT)
        transactions: list[dict] = []
        total_count = None
        page = 1
        while True:
            response = requests.get(
                f"{base_url}/api/v1/transactions", params={"page": page}, timeout=30
            )
            response.raise_for_status()
            data = response.json()
            page_records = data["data"]["transactions"]
            total_count = data["data"].get("metadata", {}).get("total_count", total_count)
            transactions.extend(page_records)
            print(f"Page {page}: fetched {len(page_records)} record(s).")
            if len(page_records) < EXPECTED_PAGE_SIZE:
                break
            page += 1
        return {"transactions": transactions, "total_count": total_count}

    @task
    def validate_volume(fetch_result: dict) -> list[dict]:
        """Raise if the number of records loaded doesn't match the API's reported total."""
        transactions = fetch_result["transactions"]
        total_count = fetch_result["total_count"]
        if total_count is not None and len(transactions) < total_count:
            raise ValueError(
                f"Volume anomaly: loaded {len(transactions)} of {total_count} reported "
                "transactions — pagination likely stopped early."
            )
        print(f"Volume check passed: {len(transactions)} record(s) loaded.")
        return transactions

    @task
    def load_to_snowflake(transactions: list[dict]) -> None:
        """Load transactions into TRANSACTIONS_RAW."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS TRANSACTIONS_RAW (
                    transaction_id VARCHAR(64) NOT NULL,
                    sku            VARCHAR(64) NOT NULL,
                    total_price    FLOAT       NOT NULL,
                    loaded_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            cur.executemany(
                "INSERT INTO TRANSACTIONS_RAW (transaction_id, sku, total_price) VALUES (%s, %s, %s)",
                [(t["transaction_id"], t["metadata"]["sku"], t["total_price"]) for t in transactions],
            )
            print(f"Loaded {len(transactions)} transaction(s) into TRANSACTIONS_RAW.")
        finally:
            conn.close()

    fetched = fetch_all_transactions()
    validated = validate_volume(fetched)
    load_to_snowflake(validated)


novamart_api_pagination()
