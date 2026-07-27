"""
## NovaMart S3 Empty File — Incident Demo

Reads a CSV file deposited by an upstream system in S3, transforms it, and
loads it into Snowflake. `deposit_upstream_file` stands in for the upstream
system that's supposed to write the file.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- aws_default       : AWS, used for S3
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- NOVAMART_S3_BUCKET             : bucket the upstream system writes to
- NOVAMART_S3_INJECT_EMPTY_FILE  : "true" to simulate the upstream system writing a 0-byte file

**Failure mode — upstream reports success but writes an empty file:**
The upstream system had an error while writing the CSV but still reported a
successful upload — the object exists at the expected key with the correct
name, just 0 bytes. `read_and_transform` reads the (empty) file and raises
a `ValueError` because there's no header row / no data to parse, instead of
silently loading zero rows.

**How to trigger:**
1. Set Airflow Variable NOVAMART_S3_INJECT_EMPTY_FILE=true.
2. Trigger this DAG manually. `deposit_upstream_file` will write a 0-byte
   object, and `read_and_transform` will raise a `ValueError` when it finds
   no content to parse.
3. Set NOVAMART_S3_INJECT_EMPTY_FILE=false to restore normal file delivery.
"""

import csv
import io
import random
import uuid
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag

SKUS = ["SKU-001", "SKU-002", "SKU-003", "SKU-004", "SKU-005"]
UPSTREAM_KEY = "upstream-drops/daily_sales.csv"


@dag(
    dag_id="novamart_s3_empty_file",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "aws", "s3"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_s3_empty_file():

    @task
    def deposit_upstream_file() -> None:
        """Stand-in for the upstream system depositing today's CSV drop in S3."""
        bucket = Variable.get("NOVAMART_S3_BUCKET", default="novamart-pipeline-demo")
        inject_empty = Variable.get("NOVAMART_S3_INJECT_EMPTY_FILE", default="false").lower() == "true"
        hook = S3Hook(aws_conn_id="aws_default")

        if inject_empty:
            hook.load_string(string_data="", key=UPSTREAM_KEY, bucket_name=bucket, replace=True)
            print(f"[simulated upstream error] Wrote 0-byte file to s3://{bucket}/{UPSTREAM_KEY}")
            return

        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["transaction_id", "sku", "quantity", "total_price"])
        for _ in range(random.randint(20, 50)):
            writer.writerow([
                str(uuid.uuid4()), random.choice(SKUS),
                random.randint(1, 10), round(random.uniform(5.0, 500.0), 2),
            ])
        hook.load_string(string_data=buf.getvalue(), key=UPSTREAM_KEY, bucket_name=bucket, replace=True)
        print(f"Wrote upstream CSV drop to s3://{bucket}/{UPSTREAM_KEY}")

    @task
    def read_and_transform() -> list[dict]:
        """Read the upstream CSV drop and parse it into records."""
        bucket = Variable.get("NOVAMART_S3_BUCKET", default="novamart-pipeline-demo")
        content = S3Hook(aws_conn_id="aws_default").read_key(key=UPSTREAM_KEY, bucket_name=bucket)
        if not content.strip():
            raise ValueError(
                f"s3://{bucket}/{UPSTREAM_KEY} is empty — the upstream system likely reported "
                "success but wrote a 0-byte file."
            )
        reader = csv.DictReader(io.StringIO(content))
        records = list(reader)
        print(f"Parsed {len(records)} record(s) from the upstream drop.")
        return records

    @task
    def load_to_snowflake(records: list[dict]) -> None:
        """Load parsed records into DAILY_SALES_FROM_UPSTREAM."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS DAILY_SALES_FROM_UPSTREAM (
                    transaction_id VARCHAR(64) NOT NULL,
                    sku            VARCHAR(64) NOT NULL,
                    quantity       INTEGER     NOT NULL,
                    total_price    FLOAT       NOT NULL
                )
            """)
            cur.executemany(
                "INSERT INTO DAILY_SALES_FROM_UPSTREAM (transaction_id, sku, quantity, total_price) "
                "VALUES (%s, %s, %s, %s)",
                [(r["transaction_id"], r["sku"], int(r["quantity"]), float(r["total_price"])) for r in records],
            )
            print(f"Loaded {len(records)} record(s) into DAILY_SALES_FROM_UPSTREAM.")
        finally:
            conn.close()

    deposited = deposit_upstream_file()
    parsed = read_and_transform()
    deposited >> parsed
    load_to_snowflake(parsed)


novamart_s3_empty_file()
