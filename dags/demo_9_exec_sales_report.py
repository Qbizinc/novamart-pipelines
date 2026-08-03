"""Demo 9 — Executive Sales Report. Critical; auto-triggered when demo_8 produces MARKETING_DAILY_SUMMARY."""

from datetime import datetime, timezone

from airflow.sdk import Asset, dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.incident_callbacks import trigger_incident_dag_v2

SUMMARY_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.MARKETING_DAILY_SUMMARY"
REPORT_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.DAILY_EXEC_SALES_REPORT"
MARKETING_DAILY_SUMMARY_ASSET = Asset("marketing_daily_summary")


@dag(
    dag_id="demo_9_exec_sales_report",
    start_date=datetime(2026, 1, 1),
    schedule=[MARKETING_DAILY_SUMMARY_ASSET],
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "marketing", "demo", "critical"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def demo_9_exec_sales_report():

    @task
    def build_report() -> None:
        business_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
                    business_date   DATE          NOT NULL,
                    total_spend     FLOAT         NOT NULL,
                    total_clicks    INTEGER       NOT NULL,
                    cost_per_click  FLOAT         NOT NULL,
                    loaded_at       TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            cur.execute(f"DELETE FROM {REPORT_TABLE} WHERE business_date = %s", (business_date,))
            row_count = cur.execute(f"""
                INSERT INTO {REPORT_TABLE}
                    (business_date, total_spend, total_clicks, cost_per_click)
                SELECT business_date, total_spend, total_clicks,
                       total_spend / NULLIF(total_clicks, 0)
                FROM {SUMMARY_TABLE}
                WHERE business_date = %s
            """, (business_date,)).rowcount
            print(f"Loaded {row_count} row(s) into {REPORT_TABLE} for {business_date}.")
        finally:
            conn.close()

    build_report()


demo_9_exec_sales_report()
