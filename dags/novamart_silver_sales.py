"""Silver Sales. Rebuilds SILVER_SALES from BRONZE_SALES via SELECT *."""

from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

from include.incident_callbacks import trigger_incident_dag_v2

BRONZE_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.BRONZE_SALES"
SILVER_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.SILVER_SALES"


@dag(
    dag_id="novamart_silver_sales",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "snowflake", "medallion", "silver"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def novamart_silver_sales():

    @task
    def build_silver() -> None:
        """Rebuild SILVER_SALES as a straight passthrough of BRONZE_SALES's current shape."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            # Intentionally SELECT * (no explicit column list): this layer doesn't hardcode
            # BRONZE_SALES's schema, so it absorbs upstream drift instead of failing on it.
            row_count = cur.execute(
                f"CREATE OR REPLACE TABLE {SILVER_TABLE} AS SELECT * FROM {BRONZE_TABLE}"
            ).rowcount
            print(f"Rebuilt {SILVER_TABLE} from {BRONZE_TABLE} ({row_count} rows).")
        finally:
            conn.close()

    build_silver()


novamart_silver_sales()
