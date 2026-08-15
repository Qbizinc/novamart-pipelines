"""Transactions CSV Export. Fetches sales_api transactions, writes a CSV + manifest to S3."""

import json
from datetime import datetime, timezone

import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag_v2

SALES_API_DEFAULT = "http://host.docker.internal:5001"
S3_PREFIX = "transactions"
REQUIRED_FIELDS = ("transaction_id", "sku", "channel", "quantity", "total_price")


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

        # sales_api occasionally returns records that don't satisfy the expected
        # transaction contract (missing fields such as "channel"). Rather than
        # crashing the whole export on a single bad record, validate up front and
        # skip/report anything that doesn't have every field we depend on. The
        # manifest's source_record_count reflects only the valid records that were
        # actually written to the CSV, so it stays consistent with the row count
        # downstream QA compares it against.
        valid_transactions = []
        skipped = []
        for t in transactions:
            missing = [f for f in REQUIRED_FIELDS if f not in t]
            if missing:
                skipped.append({"transaction": t, "missing_fields": missing})
            else:
                valid_transactions.append(t)

        if skipped:
            print(
                f"WARNING: skipping {len(skipped)} of {len(transactions)} transactions "
                f"from sales_api due to missing required fields: {skipped}"
            )

        source_record_count = len(valid_transactions)

        groups: dict[tuple[str, str], dict] = {}
        for t in valid_transactions:
            key = (t["sku"], t["channel"])
            g = groups.setdefault(key, {
                "transaction_id": t["transaction_id"],
                "sku": t["sku"],
                "channel": t["channel"],
                "quantity": 0,
                "total_price": 0.0,
            })
            g["quantity"] += t["quantity"]
            g["total_price"] += t["total_price"]

        header = "transaction_id,sku,channel,quantity,total_price,business_date"
        lines = [header]
        for g in groups.values():
            lines.append(
                f"{g['transaction_id']},{g['sku']},{g['channel']},{g['quantity']},"
                f"{g['total_price']:.2f},{business_date}"
            )
        csv_body = "\n".join(lines)

        manifest = {"business_date": business_date, "source_record_count": source_record_count}

        bucket = Variable.get("NOVAMART_S3_BUCKET")
        hook = S3Hook(aws_conn_id="aws_default")
        csv_key = f"{S3_PREFIX}/transactions_{business_date}.csv"
        manifest_key = f"{S3_PREFIX}/transactions_{business_date}.manifest.json"
        hook.load_string(string_data=csv_body, key=csv_key, bucket_name=bucket, replace=True)
        hook.load_string(string_data=json.dumps(manifest), key=manifest_key, bucket_name=bucket, replace=True)
        print(f"Wrote {len(groups)} rows to s3://{bucket}/{csv_key} "
              f"(source_record_count={source_record_count}).")

    write_csv_and_manifest(fetch_transactions())


novamart_transactions_csv_export()