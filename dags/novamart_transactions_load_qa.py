"""Transactions Load + QA. Loads the transactions CSV into Snowflake; QA-gates on transaction_id uniqueness."""

import csv
import io
import json
from datetime import datetime, timezone

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag_v2

S3_PREFIX = "transactions"
TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.TRANSACTIONS_SUMMARY"


@dag(
    dag_id="novamart_transactions_load_qa",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "snowflake", "qa"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def novamart_transactions_load_qa():

    @task
    def read_csv_and_manifest() -> dict:
        business_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        bucket = Variable.get("NOVAMART_S3_BUCKET")
        hook = S3Hook(aws_conn_id="aws_default")

        csv_key = f"{S3_PREFIX}/transactions_{business_date}.csv"
        manifest_key = f"{S3_PREFIX}/transactions_{business_date}.manifest.json"
        csv_body = hook.read_key(key=csv_key, bucket_name=bucket)
        manifest = json.loads(hook.read_key(key=manifest_key, bucket_name=bucket))

        reader = csv.DictReader(io.StringIO(csv_body))
        rows = list(reader)
        print(f"Read {len(rows)} rows from s3://{bucket}/{csv_key}; "
              f"manifest reports source_record_count={manifest['source_record_count']}.")
        return {"rows": rows, "manifest": manifest, "csv_key": csv_key, "manifest_key": manifest_key}

    @task
    def load_to_snowflake(payload: dict) -> dict:
        rows = payload["rows"]
        business_date = payload["manifest"]["business_date"]
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {TABLE} (
                    transaction_id VARCHAR(64)   NOT NULL,
                    sku            VARCHAR(64)   NOT NULL,
                    channel        VARCHAR(32)   NOT NULL,
                    quantity       INTEGER       NOT NULL,
                    total_price    FLOAT         NOT NULL,
                    business_date  DATE          NOT NULL,
                    loaded_at      TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            cur.execute(f"DELETE FROM {TABLE} WHERE business_date = %s", (business_date,))
            cur.executemany(
                f"INSERT INTO {TABLE} (transaction_id, sku, channel, quantity, total_price, business_date) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [(r["transaction_id"], r["sku"], r["channel"], int(r["quantity"]),
                  float(r["total_price"]), r["business_date"]) for r in rows],
            )
            print(f"Loaded {len(rows)} rows into {TABLE} for {business_date}.")
        finally:
            conn.close()
        return payload

    @task
    def qa_check_uniqueness(payload: dict) -> None:
        business_date = payload["manifest"]["business_date"]

        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                f"SELECT transaction_id, COUNT(*) FROM {TABLE} "
                f"WHERE business_date = %s GROUP BY transaction_id HAVING COUNT(*) > 1",
                (business_date,),
            )
            duplicates = cur.fetchall()
        finally:
            conn.close()

        print(f"QA check: {len(duplicates)} duplicate transaction_id(s) in {TABLE} "
              f"for business_date={business_date}.")
        if duplicates:
            dup_ids = ", ".join(row[0] for row in duplicates)
            raise ValueError(
                f"Uniqueness check failed for business_date={business_date}: "
                f"transaction_id must be unique per row in {TABLE}, but the following "
                f"appear more than once: {dup_ids}. Source file: {payload['csv_key']}."
            )

    qa_check_uniqueness(load_to_snowflake(read_csv_and_manifest()))


novamart_transactions_load_qa()
