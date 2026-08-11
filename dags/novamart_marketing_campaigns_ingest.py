"""Marketing Campaigns Ingest. Fetches marketing_api campaigns and loads them."""

from datetime import datetime, timezone

import requests
from airflow.sdk import Asset, Variable, dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include.incident_callbacks import trigger_incident_dag_v2

MARKETING_API_DEFAULT = "http://host.docker.internal:5003"
CAMPAIGNS_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.MARKETING_CAMPAIGNS"
MARKETING_CAMPAIGNS_ASSET = Asset("marketing_campaigns")


@dag(
    dag_id="novamart_marketing_campaigns_ingest",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "marketing"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def novamart_marketing_campaigns_ingest():

    @task
    def fetch_campaigns() -> dict:
        base_url = Variable.get("MARKETING_API_URL", default=MARKETING_API_DEFAULT)
        try:
            response = requests.get(f"{base_url}/api/v1/campaigns", timeout=30)
        except requests.RequestException as exc:
            raise RuntimeError(f"marketing_api request failed: {type(exc).__name__}") from exc

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "unknown")
            raise RuntimeError(
                f"Rate limit exceeded from Marketing Ads API. "
                f"Retry-After: {retry_after}s. Response: {response.json()}"
            )

        try:
            response.raise_for_status()
        except requests.HTTPError as exc:
            raise RuntimeError(
                f"marketing_api request failed with HTTP {response.status_code}"
            ) from exc
        data = response.json()
        print(f"Fetched {len(data['campaigns'])} campaigns for {data['metadata']['business_date']}.")
        return data

    @task(outlets=[MARKETING_CAMPAIGNS_ASSET])
    def load_campaigns(data: dict) -> None:
        campaigns = data["campaigns"]
        business_date = data["metadata"]["business_date"]

        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {CAMPAIGNS_TABLE} (
                    campaign_id   VARCHAR(64)   NOT NULL,
                    impressions   INTEGER       NOT NULL,
                    clicks        INTEGER       NOT NULL,
                    spend         FLOAT         NOT NULL,
                    business_date DATE          NOT NULL,
                    loaded_at     TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            cur.execute(f"DELETE FROM {CAMPAIGNS_TABLE} WHERE business_date = %s", (business_date,))
            rows = [
                (c["campaign_id"], c["impressions"], c["clicks"], c["spend"], business_date)
                for c in campaigns
            ]
            cur.executemany(
                f"INSERT INTO {CAMPAIGNS_TABLE} "
                "(campaign_id, impressions, clicks, spend, business_date) VALUES (%s, %s, %s, %s, %s)",
                rows,
            )
            print(f"Loaded {len(rows)} rows into {CAMPAIGNS_TABLE} for {business_date}.")
        finally:
            conn.close()

    load_campaigns(fetch_campaigns())


novamart_marketing_campaigns_ingest()
