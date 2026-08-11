"""Marketing Daily Summary. Auto-triggered when novamart_marketing_campaigns_ingest produces MARKETING_CAMPAIGNS; aggregates it."""

from datetime import datetime, timezone

from airflow.sdk import Asset, dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.incident_callbacks import trigger_incident_dag_v2

CAMPAIGNS_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.MARKETING_CAMPAIGNS"
SUMMARY_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.MARKETING_DAILY_SUMMARY"
MARKETING_CAMPAIGNS_ASSET = Asset("marketing_campaigns")
MARKETING_DAILY_SUMMARY_ASSET = Asset("marketing_daily_summary")


@dag(
    dag_id="novamart_marketing_daily_summary",
    start_date=datetime(2026, 1, 1),
    schedule=[MARKETING_CAMPAIGNS_ASSET],
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "marketing"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def novamart_marketing_daily_summary():

    @task(outlets=[MARKETING_DAILY_SUMMARY_ASSET])
    def build_summary() -> None:
        business_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {SUMMARY_TABLE} (
                    business_date    DATE          NOT NULL,
                    total_impressions INTEGER      NOT NULL,
                    total_clicks     INTEGER       NOT NULL,
                    total_spend      FLOAT         NOT NULL,
                    loaded_at        TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            cur.execute(f"DELETE FROM {SUMMARY_TABLE} WHERE business_date = %s", (business_date,))
            row_count = cur.execute(f"""
                INSERT INTO {SUMMARY_TABLE}
                    (business_date, total_impressions, total_clicks, total_spend)
                SELECT business_date, SUM(impressions), SUM(clicks), SUM(spend)
                FROM {CAMPAIGNS_TABLE}
                WHERE business_date = %s
                GROUP BY business_date
            """, (business_date,)).rowcount
            print(f"Loaded {row_count} row(s) into {SUMMARY_TABLE} for {business_date}.")
        finally:
            conn.close()

    build_summary()


novamart_marketing_daily_summary()
