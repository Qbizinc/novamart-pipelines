"""
## NovaMart Daily Sales Pipeline

Fetches daily transaction batches from the NovaMart POS system (mock API on port 5001)
and loads them into the data warehouse.

**Failure mode:** connection timeout — when the sales API is toggled into error mode
the server blocks indefinitely, causing a `requests.exceptions.Timeout`.

Toggle error on:  POST http://localhost:5001/toggle-error  {"healthy": false}
Toggle error off: POST http://localhost:5001/toggle-error  {"healthy": true}
"""

from datetime import datetime

import requests
from airflow.sdk import Variable
from airflow.sdk import dag, task

SALES_API_DEFAULT = "http://host.docker.internal:5001"


@dag(
    dag_id="novamart_daily_sales",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "sales"],
)
def novamart_daily_sales():

    @task
    def extract_sales() -> list[dict]:
        """Pull daily transactions from the POS API."""
        base_url = Variable.get("MOCK_SALES_API_URL", default=SALES_API_DEFAULT)
        response = requests.get(f"{base_url}/api/v1/sales", timeout=30)
        response.raise_for_status()
        data = response.json()
        transactions = data["transactions"]
        print(f"Extracted {len(transactions)} transactions for {data['metadata']['business_date']}")
        return transactions

    @task
    def validate_sales(transactions: list[dict]) -> list[dict]:
        """Ensure each record has required fields."""
        required = {"transaction_id", "sku", "quantity", "total_price", "timestamp"}
        for record in transactions:
            missing = required - record.keys()
            if missing:
                raise ValueError(f"Record {record.get('transaction_id')} missing fields: {missing}")
        print(f"Validated {len(transactions)} records — all fields present.")
        return transactions

    @task
    def load_sales(transactions: list[dict]) -> None:
        """Load validated transactions into the warehouse (stubbed for demo)."""
        print(f"Loading {len(transactions)} transactions into warehouse...")
        # Warehouse write goes here
        print("Load complete.")

    raw = extract_sales()
    validated = validate_sales(raw)
    load_sales(validated)


novamart_daily_sales()
