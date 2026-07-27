"""Demo 3 — Gold Sales By Region. Aggregates SILVER_SALES; tagged critical -> urgent Slack escalation."""

from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

from include.incident_callbacks import trigger_incident_dag_v2

SILVER_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.SILVER_SALES"
GOLD_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.GOLD_SALES_BY_REGION"


@dag(
    dag_id="demo_3_gold_sales_by_region",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "snowflake", "medallion", "demo", "gold", "critical"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def demo_3_gold_sales_by_region():

    @task
    def ensure_table() -> None:
        """Create GOLD_SALES_BY_REGION in Snowflake if it doesn't exist yet."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            conn.cursor().execute(f"""
                CREATE TABLE IF NOT EXISTS {GOLD_TABLE} (
                    region         VARCHAR(32)   NOT NULL,
                    business_date  DATE          NOT NULL,
                    total_revenue  FLOAT         NOT NULL,
                    total_units    INTEGER       NOT NULL,
                    loaded_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            print(f"Table {GOLD_TABLE} ready.")
        finally:
            conn.close()

    @task
    def build_gold() -> None:
        """Aggregate SILVER_SALES by region — always references `region` by name."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"DELETE FROM {GOLD_TABLE}")
            row_count = cur.execute(f"""
                INSERT INTO {GOLD_TABLE}
                    (region, business_date, total_revenue, total_units)
                SELECT region, business_date, SUM(total_price), SUM(quantity)
                FROM {SILVER_TABLE}
                GROUP BY region, business_date
            """).rowcount
            print(f"Loaded {row_count} region/day rows into {GOLD_TABLE}.")
        finally:
            conn.close()

    ensure_table() >> build_gold()


demo_3_gold_sales_by_region()
