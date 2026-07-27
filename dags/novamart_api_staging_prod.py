"""
## NovaMart API Staging/Prod — Incident Demo

Fetches order confirmations from the NovaMart mock API (via the `mock_api`
Airflow Connection) and loads them into the staging Snowflake table
ORDER_CONFIRMATIONS_STAGING.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- mock_api          : HTTP, should point at staging.api.novamart.com
- airflow_api       : Airflow REST API (used by the failure callback)

**Failure mode — staging pipeline accidentally points at production:**
Someone edited the `mock_api` Connection's host from
`staging.api.novamart.com` to `api.novamart.com` (e.g. while "fixing" what
looked like a typo). The pipeline now pulls real production order
confirmations and loads them straight into the staging Snowflake table —
no error is raised, but `validate_environment` checks the `environment`
tag the API echoes back in its response and raises when it doesn't say
`"staging"`, so the incident DAG still gets triggered.

**How to trigger:**
1. Open the Airflow UI → Admin → Connections → `mock_api`.
2. Change the Host field from `staging.api.novamart.com` to
   `api.novamart.com` and save. No DAG code change needed.
3. Trigger this DAG manually. `validate_environment` will raise a
   `ValueError` because the API reports `environment: "production"`.
4. Revert the Host field to `staging.api.novamart.com` to restore health.
"""

from datetime import datetime

import requests
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

from include.incident_callbacks import trigger_incident_dag


@dag(
    dag_id="novamart_api_staging_prod",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "api"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_api_staging_prod():

    @task
    def fetch_order_confirmations() -> dict:
        """Pull order confirmations using whatever host the mock_api Connection points at."""
        conn = HttpHook(http_conn_id="mock_api").get_connection("mock_api")
        base_url = f"{conn.schema or 'http'}://{conn.host}{f':{conn.port}' if conn.port else ''}"
        response = requests.get(f"{base_url}/api/v1/order-confirmations", timeout=30)
        response.raise_for_status()
        data = response.json()
        confirmations = data["data"]["confirmations"]
        environment = data["data"].get("metadata", {}).get("environment", "unknown")
        print(f"Fetched {len(confirmations)} confirmation(s) from environment={environment}.")
        return {"confirmations": confirmations, "environment": environment}

    @task
    def validate_environment(fetch_result: dict) -> list[dict]:
        """Raise if the API did not report the staging environment."""
        environment = fetch_result["environment"]
        if environment != "staging":
            raise ValueError(
                f"Expected mock_api to report environment=staging, got '{environment}'. "
                "The mock_api Connection host is likely pointing at production."
            )
        return fetch_result["confirmations"]

    @task
    def load_to_snowflake(confirmations: list[dict]) -> None:
        """Load confirmations into ORDER_CONFIRMATIONS_STAGING."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ORDER_CONFIRMATIONS_STAGING (
                    order_id   VARCHAR(64) NOT NULL,
                    status     VARCHAR(32) NOT NULL,
                    loaded_at  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            cur.executemany(
                "INSERT INTO ORDER_CONFIRMATIONS_STAGING (order_id, status) VALUES (%s, %s)",
                [(c["order_id"], c["status"]) for c in confirmations],
            )
            print(f"Loaded {len(confirmations)} confirmation(s) into ORDER_CONFIRMATIONS_STAGING.")
        finally:
            conn.close()

    fetched = fetch_order_confirmations()
    validated = validate_environment(fetched)
    load_to_snowflake(validated)


novamart_api_staging_prod()
