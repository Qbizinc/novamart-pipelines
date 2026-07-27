"""Demo 1 — Bronze Sales. Creates + populates BRONZE_SALES."""

import random
import uuid
from datetime import datetime, timezone

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

from include.incident_callbacks import trigger_incident_dag_v2

BRONZE_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.BRONZE_SALES"

SKUS = ["SKU-001", "SKU-002", "SKU-003", "SKU-004", "SKU-005"]
REGIONS = ["east", "west", "central"]
CHANNELS = ["web", "store"]


@dag(
    dag_id="demo_1_bronze_sales",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "snowflake", "medallion", "demo", "bronze"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def demo_1_bronze_sales():

    @task
    def generate_orders() -> list[dict]:
        """Generate synthetic raw orders — no API dependency."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        orders = []
        for _ in range(100):
            orders.append({
                "order_id": str(uuid.uuid4()),
                "sku": random.choice(SKUS),
                "quantity": random.randint(1, 10),
                "total_price": round(random.uniform(5.0, 500.0), 2),
                "region": random.choice(REGIONS),
                "channel": random.choice(CHANNELS),
                "timestamp": f"{today}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00",
            })
        print(f"Generated {len(orders)} raw orders for {today}")
        return orders

    @task
    def ensure_table() -> None:
        """Create BRONZE_SALES in Snowflake if it doesn't exist yet."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            conn.cursor().execute(f"""
                CREATE TABLE IF NOT EXISTS {BRONZE_TABLE} (
                    order_id        VARCHAR(64)   NOT NULL,
                    sku             VARCHAR(64)   NOT NULL,
                    quantity        INTEGER       NOT NULL,
                    total_price     FLOAT         NOT NULL,
                    region          VARCHAR(32)   NOT NULL,
                    channel         VARCHAR(32)   NOT NULL,
                    event_timestamp TIMESTAMP_NTZ NOT NULL,
                    business_date   DATE          NOT NULL,
                    loaded_at       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            print(f"Table {BRONZE_TABLE} ready.")
        finally:
            conn.close()

    @task
    def load_to_snowflake(orders: list[dict]) -> None:
        """Replace today's rows in BRONZE_SALES — delete then insert for idempotency."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            business_date = orders[0]["timestamp"][:10]
            deleted = cur.execute(
                f"DELETE FROM {BRONZE_TABLE} WHERE business_date = %s", (business_date,)
            ).rowcount
            print(f"Deleted {deleted} existing rows for {business_date}.")
            rows = [
                (
                    o["order_id"],
                    o["sku"],
                    o["quantity"],
                    o["total_price"],
                    o["region"],
                    o["channel"],
                    o["timestamp"],
                    o["timestamp"][:10],
                )
                for o in orders
            ]
            cur.executemany(
                f"INSERT INTO {BRONZE_TABLE} "
                "(order_id, sku, quantity, total_price, region, channel, event_timestamp, "
                "business_date) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
                rows,
            )
            print(f"Loaded {len(rows)} rows into {BRONZE_TABLE} for {business_date}.")
        finally:
            conn.close()

    raw = generate_orders()
    ready = ensure_table()
    ready >> load_to_snowflake(raw)


demo_1_bronze_sales()
