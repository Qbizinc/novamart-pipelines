"""
## NovaMart Contract Null Explosion — Incident Demo

Fetches transactions from the NovaMart transactions mock API, including
`discount_pct`, and loads them into Snowflake. Historically `discount_pct`
is populated on nearly every record.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- MOCK_TRANSACTIONS_API_URL : base URL of the transactions mock API

**Failure mode — silent null explosion in a normally-populated field:**
`discount_pct` was always populated historically; suddenly ~95% of records
arrive as null. Nothing about this breaks the `INSERT` — the pipeline would
happily load the nulls and the margins dashboard downstream would just go
quietly wrong. `validate_null_rate` is the data-quality gate here: it
computes the null rate for `discount_pct` across the batch and raises a
`ValueError` if it exceeds a sane threshold, so the incident DAG gets
triggered instead of a downstream dashboard silently breaking days later.

**How to trigger:**
1. On the mock API service, set the environment variable
   `MOCK_API_DISCOUNT_NULL_RATE=0.95` and restart/reload the service.
2. Trigger this DAG manually. `validate_null_rate` will raise a
   `ValueError` once it sees ~95% of records with `discount_pct = null`.
3. Unset `MOCK_API_DISCOUNT_NULL_RATE` (or set it back near 0) to restore
   normal discount population.
"""

from datetime import datetime

import requests
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag

TRANSACTIONS_API_DEFAULT = "http://host.docker.internal:5004"
MAX_ACCEPTABLE_NULL_RATE = 0.10


@dag(
    dag_id="novamart_contract_null_explosion",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "api", "data-contract"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_contract_null_explosion():

    @task
    def fetch_transactions() -> list[dict]:
        """Pull transactions (including discount_pct) from the mock API."""
        base_url = Variable.get("MOCK_TRANSACTIONS_API_URL", default=TRANSACTIONS_API_DEFAULT)
        response = requests.get(f"{base_url}/api/v1/transactions", timeout=30)
        response.raise_for_status()
        transactions = response.json()["data"]["transactions"]
        print(f"Fetched {len(transactions)} transaction(s).")
        return transactions

    @task
    def validate_null_rate(transactions: list[dict]) -> list[dict]:
        """Raise if discount_pct's null rate exceeds the historical norm."""
        if not transactions:
            raise ValueError("No transactions fetched — cannot validate discount_pct null rate.")
        null_count = sum(1 for t in transactions if t.get("discount_pct") is None)
        null_rate = null_count / len(transactions)
        if null_rate > MAX_ACCEPTABLE_NULL_RATE:
            raise ValueError(
                f"discount_pct null rate is {null_rate:.0%} ({null_count}/{len(transactions)}), "
                f"exceeding the {MAX_ACCEPTABLE_NULL_RATE:.0%} threshold. discount_pct is "
                "historically populated on nearly every record — this looks like an upstream break."
            )
        print(f"Null-rate check passed: {null_rate:.0%} of records have null discount_pct.")
        return transactions

    @task
    def load_to_snowflake(transactions: list[dict]) -> None:
        """Load transactions (with discount_pct) into DAILY_SALES_WITH_DISCOUNTS."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS DAILY_SALES_WITH_DISCOUNTS (
                    transaction_id VARCHAR(64) NOT NULL,
                    total_price    FLOAT,
                    discount_pct   FLOAT
                )
            """)
            cur.executemany(
                "INSERT INTO DAILY_SALES_WITH_DISCOUNTS (transaction_id, total_price, discount_pct) "
                "VALUES (%s, %s, %s)",
                [(t["transaction_id"], t.get("total_price"), t.get("discount_pct")) for t in transactions],
            )
            print(f"Loaded {len(transactions)} transaction(s) into DAILY_SALES_WITH_DISCOUNTS.")
        finally:
            conn.close()

    raw = fetch_transactions()
    validated = validate_null_rate(raw)
    load_to_snowflake(validated)


novamart_contract_null_explosion()
