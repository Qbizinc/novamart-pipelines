"""
## NovaMart Snowflake Duplicate Load — Incident Demo

Loads daily sales aggregates into Snowflake (DAILY_SALES_AGGREGATES). The
write strategy is controlled by the NOVAMART_USE_MERGE Airflow Variable,
simulating two different pipelines/teams writing to the same table with
different deduplication logic — one using DELETE+INSERT, the other MERGE.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- NOVAMART_USE_MERGE : "true" to switch to MERGE-based dedup, "false" (default) for DELETE+INSERT

**Failure mode — inconsistent numbers depending on execution order:**
With DELETE+INSERT, a run always fully replaces today's rows, so the table
reflects exactly the last run. With MERGE, a run only updates rows that
match on transaction_id and inserts new ones — it never deletes rows that
disappeared upstream (e.g. cancelled orders). If runs alternate between the
two strategies (someone flips NOVAMART_USE_MERGE mid-day, or two DAGs with
different strategies both write to this table), row counts and totals
become inconsistent and depend on which strategy ran last.
`validate_row_count` compares the freshly generated batch size against
what's actually in the table after the write and raises if they diverge,
surfacing the drift instead of letting it pass silently.

**How to trigger:**
1. Set Airflow Variable NOVAMART_USE_MERGE=true.
2. Trigger this DAG manually — it now upserts via MERGE instead of
   DELETE+INSERT.
3. Trigger it again (or run a parallel novamart_snowflake_sales-style load
   using DELETE+INSERT) with a different-sized batch — `validate_row_count`
   will raise a `ValueError` when the table's row count for today doesn't
   match the batch just generated.
4. Set NOVAMART_USE_MERGE=false to return to the DELETE+INSERT strategy.
"""

import random
import uuid
from datetime import datetime, timezone

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.novamart_utils import trigger_incident_dag

SKUS = ["SKU-001", "SKU-002", "SKU-003", "SKU-004", "SKU-005"]


@dag(
    dag_id="novamart_snowflake_duplicate_load",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "snowflake"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_snowflake_duplicate_load():

    @task
    def generate_aggregates() -> list[dict]:
        """Generate a synthetic batch of daily sales aggregates."""
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        batch = [
            {
                "transaction_id": str(uuid.uuid4()),
                "sku": sku,
                "total_quantity": random.randint(10, 100),
                "total_revenue": round(random.uniform(500.0, 5000.0), 2),
                "business_date": today,
            }
            for sku in SKUS
        ]
        print(f"Generated {len(batch)} aggregate row(s) for {today}.")
        return batch

    @task
    def load_aggregates(batch: list[dict]) -> int:
        """Write the batch using DELETE+INSERT or MERGE depending on NOVAMART_USE_MERGE."""
        use_merge = Variable.get("NOVAMART_USE_MERGE", default="false").lower() == "true"
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS DAILY_SALES_AGGREGATES (
                    transaction_id VARCHAR(64) NOT NULL,
                    sku            VARCHAR(64) NOT NULL,
                    total_quantity INTEGER     NOT NULL,
                    total_revenue  FLOAT       NOT NULL,
                    business_date  DATE        NOT NULL
                )
            """)
            rows = [
                (r["transaction_id"], r["sku"], r["total_quantity"], r["total_revenue"], r["business_date"])
                for r in batch
            ]
            if use_merge:
                for row in rows:
                    cur.execute(
                        """
                        MERGE INTO DAILY_SALES_AGGREGATES t
                        USING (SELECT %s AS transaction_id, %s AS sku, %s AS total_quantity,
                                      %s AS total_revenue, %s AS business_date) s
                        ON t.transaction_id = s.transaction_id
                        WHEN MATCHED THEN UPDATE SET
                            total_quantity = s.total_quantity, total_revenue = s.total_revenue
                        WHEN NOT MATCHED THEN INSERT (transaction_id, sku, total_quantity, total_revenue, business_date)
                        VALUES (s.transaction_id, s.sku, s.total_quantity, s.total_revenue, s.business_date)
                        """,
                        row,
                    )
                print(f"MERGE-loaded {len(rows)} row(s) into DAILY_SALES_AGGREGATES.")
            else:
                business_date = batch[0]["business_date"]
                cur.execute(
                    "DELETE FROM DAILY_SALES_AGGREGATES WHERE business_date = %s", (business_date,)
                )
                cur.executemany(
                    "INSERT INTO DAILY_SALES_AGGREGATES "
                    "(transaction_id, sku, total_quantity, total_revenue, business_date) "
                    "VALUES (%s, %s, %s, %s, %s)",
                    rows,
                )
                print(f"DELETE+INSERT-loaded {len(rows)} row(s) into DAILY_SALES_AGGREGATES.")
            return len(rows)
        finally:
            conn.close()

    @task
    def validate_row_count(expected_count: int) -> None:
        """Raise if today's row count in the table doesn't match what was just generated."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT COUNT(*) FROM DAILY_SALES_AGGREGATES WHERE business_date = CURRENT_DATE()"
            )
            actual_count = cur.fetchone()[0]
            if actual_count != expected_count:
                raise ValueError(
                    f"Row count drift detected: expected {expected_count} rows for today, "
                    f"found {actual_count}. DELETE+INSERT and MERGE writers are likely out of sync."
                )
            print(f"Row count check passed: {actual_count} row(s) for today.")
        finally:
            conn.close()

    batch = generate_aggregates()
    written = load_aggregates(batch)
    validate_row_count(written)


novamart_snowflake_duplicate_load()
