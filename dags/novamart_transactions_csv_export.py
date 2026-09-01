"""Transactions CSV Export. Fetches sales_api transactions, writes a CSV + manifest to S3."""

import json
from datetime import datetime, timezone

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag_v2

SALES_API_DEFAULT = "http://host.docker.internal:5001"
S3_PREFIX = "transactions"


@dag(
    dag_id="novamart_transactions_csv_export",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "s3", "csv"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def novamart_transactions_csv_export():

    @task
    def fetch_transactions() -> list[dict]:
        base_url = Variable.get("SALES_API_URL", default=SALES_API_DEFAULT)
        try:
            response = requests.get(f"{base_url}/api/v1/sales", timeout=30)
            response.raise_for_status()
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "unknown"
            raise RuntimeError(f"sales_api request failed with HTTP {status}") from exc
        except requests.RequestException as exc:
            raise RuntimeError(f"sales_api request failed: {type(exc).__name__}") from exc
        transactions = response.json()["transactions"]
        print(f"Fetched {len(transactions)} transactions from sales_api.")
        return transactions

    @task
    def write_csv_and_manifest(transactions: list[dict]) -> None:
        business_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        source_record_count = len(transactions)

        header = "transaction_id,sku,channel,quantity,total_price,business_date"
        lines = [header]
        for t in transactions:
            lines.append(
                f"{t['transaction_id']},{t['sku']},{t['channel']},{t['quantity']},"
                f"{t['total_price']:.2f},{business_date}"
            )

        # Flag today's highest-value transaction for the fraud-review queue.
        # NOTE: this is informational only — it must NOT add another row for the
        # same transaction_id to the CSV, since each transaction_id must appear
        # exactly once (source_record_count and downstream uniqueness QA rely on this).
        highest = max(transactions, key=lambda t: t["total_price"])
        print(f"Flagging highest-value transaction for fraud review: "
              f"{highest['transaction_id']} (${highest['total_price']:.2f})")

        csv_body = "\n".join(lines)

        manifest = {"business_date": business_date, "source_record_count": source_record_count}

        bucket = Variable.get("NOVAMART_S3_BUCKET")
        hook = S3Hook(aws_conn_id="aws_default")
        csv_key = f"{S3_PREFIX}/transactions_{business_date}.csv"
        manifest_key = f"{S3_PREFIX}/transactions_{business_date}.manifest.json"
        hook.load_string(string_data=csv_body, key=csv_key, bucket_name=bucket, replace=True)
        hook.load_string(string_data=json.dumps(manifest), key=manifest_key, bucket_name=bucket, replace=True)
        print(f"Wrote {len(transactions)} rows to s3://{bucket}/{csv_key} "
              f"(source_record_count={source_record_count}).")

    write_csv_and_manifest(fetch_transactions())


novamart_transactions_csv_export()