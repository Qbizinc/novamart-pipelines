"""
## NovaMart Contract Type Change — Incident Demo

Fetches transactions from the NovaMart transactions mock API, validates
each record against the DAILY_SALES data contract, and loads them into
Snowflake.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- MOCK_TRANSACTIONS_API_URL : base URL of the transactions mock API

**Failure mode — contract violation: numeric field becomes a currency string:**
The mock API changed `total_price` from a FLOAT (`49.99`) to a STRING with
a currency symbol (`"$49.99"`). `validate_contract` checks that
`total_price` is numeric per the DAILY_SALES contract and raises a
`TypeError` before the bad type ever reaches the Snowflake `INSERT`
(which would otherwise fail with a type-casting error deep in the load).

**How to trigger:**
1. On the mock API service, set the environment variable
   `MOCK_API_PRICE_FORMAT=string` and restart/reload the service.
2. Trigger this DAG manually. `validate_contract` will raise a `TypeError`
   when `total_price` comes back as `"$49.99"` instead of `49.99`.
3. Unset `MOCK_API_PRICE_FORMAT` (or set back to `float`) to restore the
   contract.
"""

from datetime import datetime

import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.novamart_utils import trigger_incident_dag

TRANSACTIONS_API_DEFAULT = "http://host.docker.internal:5004"


@dag(
    dag_id="novamart_contract_type_change",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "api", "data-contract"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_contract_type_change():

    @task
    def fetch_transactions() -> list[dict]:
        """Pull transactions from the mock API."""
        base_url = Variable.get("MOCK_TRANSACTIONS_API_URL", default=TRANSACTIONS_API_DEFAULT)
        response = requests.get(f"{base_url}/api/v1/transactions", timeout=30)
        response.raise_for_status()
        transactions = response.json()["data"]["transactions"]
        print(f"Fetched {len(transactions)} transaction(s).")
        return transactions

    @task
    def validate_contract(transactions: list[dict]) -> list[dict]:
        """Enforce the DAILY_SALES contract: total_price must be numeric."""
        for t in transactions:
            price = t["total_price"]
            if not isinstance(price, (int, float)):
                raise TypeError(
                    f"Contract violation on transaction {t.get('transaction_id')}: "
                    f"total_price must be numeric, got {type(price).__name__}: {price!r}"
                )
        print(f"Validated {len(transactions)} transaction(s) against the contract.")
        return transactions

    @task
    def load_to_snowflake(transactions: list[dict]) -> None:
        """Load validated transactions into DAILY_SALES."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS DAILY_SALES_FROM_API (
                    transaction_id VARCHAR(64) NOT NULL,
                    total_price    FLOAT       NOT NULL
                )
            """)
            cur.executemany(
                "INSERT INTO DAILY_SALES_FROM_API (transaction_id, total_price) VALUES (%s, %s)",
                [(t["transaction_id"], t["total_price"]) for t in transactions],
            )
            print(f"Loaded {len(transactions)} transaction(s) into DAILY_SALES_FROM_API.")
        finally:
            conn.close()

    raw = fetch_transactions()
    validated = validate_contract(raw)
    load_to_snowflake(validated)


novamart_contract_type_change()
