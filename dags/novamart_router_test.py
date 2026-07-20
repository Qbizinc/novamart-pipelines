"""
## NovaMart Router Test — AI Triage Router Prototype Target

A real 3-stage ETL pipeline (API -> S3 -> Snowflake) built to exercise
agentic_snowflake_incident_memory_v2's platform-classification routing (aws / api / snowflake).
Each stage can fail independently and realistically:

1. fetch_from_api    — pulls a randomized batch of POS transactions from sales_api (mock-apis-repo).
                        Failure: toggle sales_api unhealthy -> real requests.Timeout.
2. upload_to_s3       — writes the batch to S3 as JSON. Failure: NOVAMART_ROUTER_TEST_CODE_BUG=true
                        -> a real KeyError from a deliberate bug in this file's own code (references
                        a nonexistent "prices" field instead of "total_price") — not a config/
                        credentials problem, for testing the FIX path (propose_code_fix / open_pr)
                        rather than ESCALATE/TICKET.
3. download_from_s3   — reads the object back. Failure: NOVAMART_ROUTER_TEST_S3_ACCESS_DENIED=true
                        -> a real botocore.exceptions.ClientError shaped as AccessDenied
                        (synthetic — not a real bucket policy — but an authentic exception shape).
4. load_to_snowflake  — inserts into SANDBOX_DATA_PIPELINE.NOVAMART_RAW.ROUTER_TEST_ORDERS.
                        Failure: manually run
                        `ALTER TABLE ROUTER_TEST_ORDERS DROP COLUMN channel;` in Snowflake first
                        -> a real Snowflake ProgrammingError (column count mismatch).

On any failure this DAG triggers agentic_snowflake_incident_memory_v2 (not the original
agentic_snowflake_incident_memory) via trigger_incident_dag_v2.

Required Airflow Connections (set in airflow_settings.yaml):
- aws_default       : AWS, used for S3
- snowflake_default : Snowflake, key-pair auth
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- MOCK_SALES_API_URL                     : sales_api base URL (default host.docker.internal:5001)
- NOVAMART_S3_BUCKET                     : S3 bucket to read/write (real bucket, must already exist)
- NOVAMART_ROUTER_TEST_S3_ACCESS_DENIED  : "true" to simulate AccessDenied on the S3 read
- NOVAMART_ROUTER_TEST_CODE_BUG          : "true" to simulate a real bug in this pipeline's code

**How to trigger each failure:**
- API:        POST http://localhost:5001/toggle-error {"healthy": false}  (toggle back with true)
- Code bug:   set NOVAMART_ROUTER_TEST_CODE_BUG=true                       (toggle back with false)
- S3/IAM:     set NOVAMART_ROUTER_TEST_S3_ACCESS_DENIED=true               (toggle back with false)
- Snowflake:  ALTER TABLE SANDBOX_DATA_PIPELINE.NOVAMART_RAW.ROUTER_TEST_ORDERS DROP COLUMN channel;
              (restore with: ALTER TABLE ... ADD COLUMN channel VARCHAR;)
"""

import json
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task
from botocore.exceptions import ClientError

from include.novamart_utils import trigger_incident_dag_v2

import requests

SALES_API_DEFAULT = "http://host.docker.internal:5001"
S3_KEY = "router-test/orders.json"


@dag(
    dag_id="novamart_router_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "router-test", "critical"],  # critical: escalate non-fixable failures
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def novamart_router_test():

    @task
    def fetch_from_api() -> list[dict]:
        """Pull a randomized batch of POS transactions from sales_api."""
        base_url = Variable.get("MOCK_SALES_API_URL", default=SALES_API_DEFAULT)
        response = requests.get(f"{base_url}/api/v1/sales", timeout=30)
        response.raise_for_status()
        transactions = response.json()["transactions"]
        print(f"Fetched {len(transactions)} transactions from sales_api.")
        return transactions

    @task
    def upload_to_s3(transactions: list[dict]) -> None:
        """Write the fetched batch to S3 as JSON."""
        if Variable.get("NOVAMART_ROUTER_TEST_CODE_BUG", default="false").lower() == "true":
            # Deliberate bug (not a config/credentials problem): "prices" isn't a real field on
            # a transaction — should be "total_price". A genuine bug in our own code, for testing
            # the FIX path (propose_code_fix / open_pr) rather than ESCALATE/TICKET.
            batch_value = sum(t["total_price"] for t in transactions)
            print(f"Batch value: {batch_value}")

        bucket = Variable.get("NOVAMART_S3_BUCKET")
        S3Hook(aws_conn_id="aws_default").load_string(
            string_data=json.dumps(transactions),
            key=S3_KEY,
            bucket_name=bucket,
            replace=True,
        )
        print(f"Uploaded {len(transactions)} transactions to s3://{bucket}/{S3_KEY}")

    @task
    def download_from_s3(_uploaded: None) -> list[dict]:
        """Read the batch back from S3."""
        if Variable.get("NOVAMART_ROUTER_TEST_S3_ACCESS_DENIED", default="false").lower() == "true":
            raise ClientError(
                {
                    "Error": {
                        "Code": "AccessDenied",
                        "Message": "User is not authorized to perform: s3:GetObject on this object",
                    }
                },
                "GetObject",
            )

        bucket = Variable.get("NOVAMART_S3_BUCKET")
        content = S3Hook(aws_conn_id="aws_default").read_key(key=S3_KEY, bucket_name=bucket)
        transactions = json.loads(content)
        print(f"Downloaded {len(transactions)} transactions from s3://{bucket}/{S3_KEY}")
        return transactions

    @task
    def load_to_snowflake(transactions: list[dict]) -> None:
        """Load transactions into ROUTER_TEST_ORDERS."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS ROUTER_TEST_ORDERS (
                    transaction_id VARCHAR(64) NOT NULL,
                    sku            VARCHAR(64) NOT NULL,
                    quantity       INTEGER     NOT NULL,
                    total_price    FLOAT       NOT NULL,
                    order_ts       VARCHAR(64) NOT NULL,
                    channel        VARCHAR(32) NOT NULL
                )
            """)
            cur.executemany(
                "INSERT INTO ROUTER_TEST_ORDERS "
                "(transaction_id, sku, quantity, total_price, order_ts, channel) "
                "VALUES (%s, %s, %s, %s, %s, %s)",
                [
                    (t["transaction_id"], t["sku"], int(t["quantity"]), float(t["total_price"]),
                     t["timestamp"], t["channel"])
                    for t in transactions
                ],
            )
            print(f"Loaded {len(transactions)} transactions into ROUTER_TEST_ORDERS.")
        finally:
            conn.close()

    fetched = fetch_from_api()
    uploaded = upload_to_s3(fetched)
    downloaded = download_from_s3(uploaded)
    load_to_snowflake(downloaded)


novamart_router_test()