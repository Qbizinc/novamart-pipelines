"""
## NovaMart Snowflake Wrong Date — Incident Demo

Loads daily transactions into DAILY_TRANSACTIONS, tagging each row with a
`business_date` computed from the configured NOVAMART_TIMEZONE offset. The
company standard is that business_date always reflects the current date in
UTC, regardless of what timezone the pipeline happens to run in.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- NOVAMART_TIMEZONE : offset string like "UTC" (default) or "UTC+14"

**Failure mode — timezone misconfiguration shifts the business date:**
Someone set NOVAMART_TIMEZONE to a large positive offset (e.g. UTC+14,
matching Pacific/Kiritimati). Late in the UTC day this pushes the computed
local date a full day ahead of the UTC business_date the warehouse expects,
so today's transactions get tagged with tomorrow's (or, depending on time
of day, yesterday's) date. `validate_business_date` compares the computed
business_date against the true UTC calendar date and raises when they
diverge.

**How to trigger:**
1. Set Airflow Variable NOVAMART_TIMEZONE=UTC+14.
2. Trigger this DAG manually. `compute_business_date` will compute a date
   offset from true UTC, and `validate_business_date` will raise a
   `ValueError` when the two dates don't match.
3. Set NOVAMART_TIMEZONE=UTC to restore correct dating.
"""

import random
import uuid
from datetime import datetime, timedelta, timezone

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.novamart_utils import trigger_incident_dag

SKUS = ["SKU-001", "SKU-002", "SKU-003", "SKU-004", "SKU-005"]


def _parse_offset_hours(tz_string: str) -> int:
    """Parse offset strings like 'UTC', 'UTC+14', 'UTC-5' into an hour offset."""
    tz_string = tz_string.strip().upper()
    if tz_string in ("UTC", ""):
        return 0
    sign = 1 if "+" in tz_string else -1
    offset_str = tz_string.replace("UTC", "").replace("+", "").replace("-", "")
    return sign * int(offset_str)


@dag(
    dag_id="novamart_snowflake_wrong_date",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "snowflake"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_snowflake_wrong_date():

    @task
    def compute_business_date() -> str:
        """Compute business_date by applying the configured timezone offset."""
        tz_string = Variable.get("NOVAMART_TIMEZONE", default="UTC")
        offset_hours = _parse_offset_hours(tz_string)
        local_now = datetime.now(timezone.utc) + timedelta(hours=offset_hours)
        business_date = local_now.strftime("%Y-%m-%d")
        print(f"NOVAMART_TIMEZONE={tz_string} -> business_date={business_date}")
        return business_date

    @task
    def validate_business_date(business_date: str) -> str:
        """Raise if the computed business_date doesn't match the true UTC calendar date."""
        true_utc_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if business_date != true_utc_date:
            raise ValueError(
                f"business_date mismatch: computed '{business_date}' but true UTC date is "
                f"'{true_utc_date}'. NOVAMART_TIMEZONE is likely misconfigured."
            )
        return business_date

    @task
    def load_daily_transactions(business_date: str) -> None:
        """Generate and load synthetic transactions tagged with business_date."""
        transactions = [
            {
                "transaction_id": str(uuid.uuid4()),
                "sku": random.choice(SKUS),
                "quantity": random.randint(1, 10),
                "total_price": round(random.uniform(5.0, 500.0), 2),
                "business_date": business_date,
            }
            for _ in range(random.randint(20, 50))
        ]
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS DAILY_TRANSACTIONS (
                    transaction_id VARCHAR(64) NOT NULL,
                    sku            VARCHAR(64) NOT NULL,
                    quantity       INTEGER     NOT NULL,
                    total_price    FLOAT       NOT NULL,
                    business_date  DATE        NOT NULL
                )
            """)
            cur.executemany(
                "INSERT INTO DAILY_TRANSACTIONS (transaction_id, sku, quantity, total_price, business_date) "
                "VALUES (%s, %s, %s, %s, %s)",
                [(t["transaction_id"], t["sku"], t["quantity"], t["total_price"], t["business_date"])
                 for t in transactions],
            )
            print(f"Loaded {len(transactions)} transaction(s) for business_date={business_date}.")
        finally:
            conn.close()

    business_date = compute_business_date()
    validated_date = validate_business_date(business_date)
    load_daily_transactions(validated_date)


novamart_snowflake_wrong_date()
