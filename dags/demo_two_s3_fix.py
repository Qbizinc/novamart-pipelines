"""
## Demo Two — S3 Upload Code Bug -> No Prior RAG Match -> Auto-fix PR

Single-task pipeline that always fails writing a batch of synthetic transactions to S3. Has a
real bug in its own code — references a nonexistent "totall_price" field (typo for
"total_price") — a genuine KeyError, not an S3 permissions/credentials problem. decide_path
treats this as a fixable bug in the pipeline's own code regardless of criticality, so it always
routes to propose_code_fix -> open_pr, never to escalate/ticket.

Not tagged critical (doesn't matter for this path) and has no seeded prior incident, so
recall_prior_incidents naturally returns nothing on this pipeline — demonstrating a fresh-memory
diagnosis, in contrast to demo_one_api_escalate's seeded recall.

On any failure this DAG triggers agentic_snowflake_incident_memory_v2 via trigger_incident_dag_v2.

Required Airflow Connections: aws_default (S3), airflow_api (used by the failure callback).
Required Airflow Variables:
- NOVAMART_S3_BUCKET     : S3 bucket to write to (real bucket, must already exist)

**How to trigger:** just run this DAG — it always fails the same way, no Variable to set.
"""

import json
from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable, dag, task

from include.novamart_utils import trigger_incident_dag_v2

S3_KEY = "demo-two/orders.json"

SAMPLE_TRANSACTIONS = [
    {"transaction_id": "T-1001", "sku": "SKU-42", "quantity": 2, "total_price": 39.98,
     "timestamp": "2026-07-19T10:00:00Z", "channel": "web"},
    {"transaction_id": "T-1002", "sku": "SKU-17", "quantity": 1, "total_price": 12.50,
     "timestamp": "2026-07-19T10:05:00Z", "channel": "store"},
]


@dag(
    dag_id="demo_two_s3_fix",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "demo"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def demo_two_s3_fix():

    @task
    def upload_to_s3() -> None:
        """Write a synthetic batch of transactions to S3. Always fails first."""
        # Deliberate bug: "totall_price" isn't a real field — should be "total_price". A genuine
        # bug in our own code, for testing the FIX path (propose_code_fix / open_pr).
        batch_value = sum(t["total_price"] for t in SAMPLE_TRANSACTIONS)
        print(f"Batch value: {batch_value}")

        bucket = Variable.get("NOVAMART_S3_BUCKET")
        S3Hook(aws_conn_id="aws_default").load_string(
            string_data=json.dumps(SAMPLE_TRANSACTIONS),
            key=S3_KEY,
            bucket_name=bucket,
            replace=True,
        )
        print(f"Uploaded {len(SAMPLE_TRANSACTIONS)} transactions to s3://{bucket}/{S3_KEY}")

    upload_to_s3()


demo_two_s3_fix()