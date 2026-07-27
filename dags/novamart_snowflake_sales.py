"""
## NovaMart Snowflake Sales Pipeline

Generates synthetic daily sales transactions and loads them into Snowflake
(NOVAMART_RAW.DAILY_SALES). No external API dependency — runs standalone.

On any failure the DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

All credentials come from Airflow Connections (airflow_settings.yaml):
- snowflake_default → Snowflake key-pair auth
- airflow_api       → Airflow REST API (used by the failure callback)

**Failure modes to try:**
- Schema drift:  ALTER TABLE DAILY_SALES DROP COLUMN sku
- Bad creds:     Remove the Snowflake private key file
- Bad data:      Set NOVAMART_INJECT_BAD_DATA=true to generate records missing fields
"""

import random
import uuid
from datetime import datetime, timezone

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag

SKUS = ["SKU-001", "SKU-002", "SKU-003", "SKU-004", "SKU-005"]


@dag(
    dag_id="novamart_snowflake_sales",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "sales", "snowflake"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_snowflake_sales():

    @task
    def generate_sales() -> list[dict]:
        """Generate synthetic daily transactions — no API dependency."""
        inject_bad_data = Variable.get("NOVAMART_INJECT_BAD_DATA", default="false").lower() == "true"
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        transactions = []
        for _ in range(random.randint(20, 50)):
            record = {
                "transaction_id": str(uuid.uuid4()),
                "sku": random.choice(SKUS),
                "quantity": random.randint(1, 10),
                "total_price": round(random.uniform(5.0, 500.0), 2),
                "timestamp": f"{today}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
            }
            if inject_bad_data:
                record.pop("sku")
            transactions.append(record)
        print(f"Generated {len(transactions)} transactions for {today}" +
              (" [BAD DATA MODE]" if inject_bad_data else ""))
        return transactions

    @task
    def validate_sales(transactions: list[dict]) -> list[dict]:
        """Ensure each record has the required fields."""
        required = {"transaction_id", "sku", "quantity", "total_price", "timestamp"}
        for record in transactions:
            missing = required - record.keys()
            if missing:
                raise ValueError(f"Record {record.get('transaction_id')} missing fields: {missing}")
        print(f"Validated {len(transactions)} records — all fields present.")
        return transactions

    @task
    def ensure_table() -> None:
        """Create DAILY_SALES in Snowflake if it doesn't exist yet."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            conn.cursor().execute("""
                CREATE TABLE IF NOT EXISTS DAILY_SALES (
                    transaction_id  VARCHAR(64)   NOT NULL,
                    sku             VARCHAR(64)   NOT NULL,
                    quantity        INTEGER       NOT NULL,
                    total_price     FLOAT         NOT NULL,
                    event_timestamp TIMESTAMP_NTZ NOT NULL,
                    business_date   DATE          NOT NULL,
                    loaded_at       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            print("Table DAILY_SALES ready.")
        finally:
            conn.close()

    @task
    def load_to_snowflake(transactions: list[dict]) -> None:
        """Replace today's rows in DAILY_SALES — delete then insert for idempotency."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            business_date = transactions[0]["timestamp"][:10]
            deleted = cur.execute(
                "DELETE FROM DAILY_SALES WHERE business_date = %s", (business_date,)
            ).rowcount
            print(f"Deleted {deleted} existing rows for {business_date}.")
            rows = [
                (
                    t["transaction_id"],
                    t["sku"],
                    t["quantity"],
                    t["total_price"],
                    t["timestamp"],
                    t["timestamp"][:10],
                )
                for t in transactions
            ]
            cur.executemany(
                "INSERT INTO DAILY_SALES "
                "(transaction_id, sku, quantity, total_price, event_timestamp, business_date) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                rows,
            )
            print(f"Loaded {len(rows)} rows into DAILY_SALES for {business_date}.")
        finally:
            conn.close()

    raw = generate_sales()
    validated = validate_sales(raw)
    ready = ensure_table()
    ready >> load_to_snowflake(validated)


novamart_snowflake_sales()
