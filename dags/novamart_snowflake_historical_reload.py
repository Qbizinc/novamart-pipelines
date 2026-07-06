"""
## NovaMart Snowflake Historical Reload — Incident Demo

Extracts daily sales from DAILY_SALES and loads them into
SALES_REPORTING, a downstream reporting table. The extraction query is
normally scoped to `business_date = CURRENT_DATE()`.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- NOVAMART_DISABLE_DATE_FILTER : "true" to drop the date filter and extract all history

**Failure mode — the date filter gets dropped:**
Someone removed the `WHERE business_date = CURRENT_DATE()` clause from the
extraction query (e.g. while "simplifying" the query during a refactor).
Instead of today's ~20-50 rows, `extract_daily_sales` now pulls the entire
historical DAILY_SALES table. `validate_extract_size` catches this by
checking the extracted row count against a sane per-day ceiling and raises
before the full-history reload lands in SALES_REPORTING.

**How to trigger:**
1. Set Airflow Variable NOVAMART_DISABLE_DATE_FILTER=true.
2. Trigger this DAG manually. `extract_daily_sales` will query all of
   DAILY_SALES instead of just today, and `validate_extract_size` will
   raise a `ValueError` once the row count blows past the expected daily
   ceiling.
3. Set NOVAMART_DISABLE_DATE_FILTER=false to restore the date-scoped query.
"""

from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.novamart_utils import trigger_incident_dag

MAX_EXPECTED_DAILY_ROWS = 500


@dag(
    dag_id="novamart_snowflake_historical_reload",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "snowflake"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_snowflake_historical_reload():

    @task
    def extract_daily_sales() -> list[dict]:
        """Extract sales from DAILY_SALES, scoped to today unless the date filter is disabled."""
        disable_filter = Variable.get("NOVAMART_DISABLE_DATE_FILTER", default="false").lower() == "true"
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            if disable_filter:
                cur.execute(
                    "SELECT transaction_id, sku, quantity, total_price, business_date FROM DAILY_SALES"
                )
                print("NOVAMART_DISABLE_DATE_FILTER=true — extracting ALL of DAILY_SALES history.")
            else:
                cur.execute(
                    "SELECT transaction_id, sku, quantity, total_price, business_date "
                    "FROM DAILY_SALES WHERE business_date = CURRENT_DATE()"
                )
            rows = cur.fetchall()
            print(f"Extracted {len(rows)} row(s) from DAILY_SALES.")
            return [
                {
                    "transaction_id": r[0], "sku": r[1], "quantity": r[2],
                    "total_price": r[3], "business_date": str(r[4]),
                }
                for r in rows
            ]
        finally:
            conn.close()

    @task
    def validate_extract_size(rows: list[dict]) -> list[dict]:
        """Raise if the extract looks like a full-history reload instead of a single day."""
        if len(rows) > MAX_EXPECTED_DAILY_ROWS:
            raise ValueError(
                f"Extracted {len(rows)} rows, exceeding the {MAX_EXPECTED_DAILY_ROWS}-row daily "
                "ceiling. The date filter on extract_daily_sales is likely disabled, pulling the "
                "full historical table instead of just today."
            )
        print(f"Extract size check passed: {len(rows)} row(s).")
        return rows

    @task
    def load_to_reporting(rows: list[dict]) -> None:
        """Load validated rows into SALES_REPORTING."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS SALES_REPORTING (
                    transaction_id VARCHAR(64) NOT NULL,
                    sku            VARCHAR(64) NOT NULL,
                    quantity       INTEGER     NOT NULL,
                    total_price    FLOAT       NOT NULL,
                    business_date  DATE        NOT NULL
                )
            """)
            cur.executemany(
                "INSERT INTO SALES_REPORTING (transaction_id, sku, quantity, total_price, business_date) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(r["transaction_id"], r["sku"], r["quantity"], r["total_price"], r["business_date"])
                 for r in rows],
            )
            print(f"Loaded {len(rows)} row(s) into SALES_REPORTING.")
        finally:
            conn.close()

    extracted = extract_daily_sales()
    validated = validate_extract_size(extracted)
    load_to_reporting(validated)


novamart_snowflake_historical_reload()
