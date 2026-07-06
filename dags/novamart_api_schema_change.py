"""
## NovaMart API Schema Change — Incident Demo

Fetches transactions from the NovaMart transactions mock API, parses out the
`sku` field, and loads the result into Snowflake.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- MOCK_TRANSACTIONS_API_URL : base URL of the transactions mock API

**Failure mode — undocumented schema change on an upstream field:**
The mock API changed `data.transactions[].metadata.sku` from a plain string
(`"SKU-001"`) to an object (`{"id": "SKU-001", "category": "electronics"}`).
`parse_transactions` expects a string and raises a `TypeError`/`ValueError`
when it hits the new shape.

**How to trigger:**
1. On the mock API service, set the environment variable
   `MOCK_API_SKU_FORMAT=object` and restart/reload the service.
2. Trigger this DAG manually. `parse_transactions` will fail while reading
   `metadata.sku` as a string.
3. Unset (or set back to `string`) `MOCK_API_SKU_FORMAT` to restore health.
"""

from datetime import datetime

import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.novamart_utils import trigger_incident_dag

TRANSACTIONS_API_DEFAULT = "http://host.docker.internal:5004"


@dag(
    dag_id="novamart_api_schema_change",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "api"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_api_schema_change():

    @task
    def fetch_transactions() -> list[dict]:
        """Pull transactions from the mock API."""
        base_url = Variable.get("MOCK_TRANSACTIONS_API_URL", default=TRANSACTIONS_API_DEFAULT)
        response = requests.get(f"{base_url}/api/v1/transactions", timeout=30)
        response.raise_for_status()
        data = response.json()
        transactions = data["data"]["transactions"]
        print(f"Fetched {len(transactions)} transaction(s).")
        return transactions

    @task
    def parse_transactions(transactions: list[dict]) -> list[dict]:
        """Extract sku (expected to be a plain string) from each transaction's metadata."""
        parsed = []
        for t in transactions:
            sku = t["metadata"]["sku"]
            if not isinstance(sku, str):
                raise TypeError(
                    f"Expected metadata.sku to be a string, got {type(sku).__name__}: {sku!r}"
                )
            parsed.append({
                "transaction_id": t["transaction_id"],
                "sku": sku,
                "total_price": t["total_price"],
            })
        print(f"Parsed {len(parsed)} transaction(s).")
        return parsed

    @task
    def load_to_snowflake(parsed: list[dict]) -> None:
        """Load parsed transactions into TRANSACTIONS_RAW."""
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
                [(p["transaction_id"], p["sku"], p["total_price"]) for p in parsed],
            )
            print(f"Loaded {len(parsed)} transaction(s) into TRANSACTIONS_RAW.")
        finally:
            conn.close()

    raw = fetch_transactions()
    parsed = parse_transactions(raw)
    load_to_snowflake(parsed)


novamart_api_schema_change()
