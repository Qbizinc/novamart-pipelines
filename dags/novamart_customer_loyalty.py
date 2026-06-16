"""
## NovaMart Customer Loyalty Pipeline

Extracts customer loyalty tier updates from the NovaMart CRM API (mock on port 5002)
and syncs them to the data warehouse.

**Failure mode:** expired credentials — when toggled into error mode the API returns
HTTP 401 with `AUTH_CREDENTIALS_EXPIRED`, raising an `HTTPError`.

Toggle error on:  POST http://localhost:5002/toggle-error  {"healthy": false}
Toggle error off: POST http://localhost:5002/toggle-error  {"healthy": true}
"""

from datetime import datetime

import requests
from airflow.sdk import Variable
from airflow.sdk import dag, task

CUSTOMER_API_DEFAULT = "http://host.docker.internal:5002"
VALID_TOKEN = "Bearer nvmt_live_token_2026"


@dag(
    dag_id="novamart_customer_loyalty",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "customer"],
)
def novamart_customer_loyalty():

    @task
    def extract_customers() -> list[dict]:
        """Pull loyalty tier updates from the CRM API using a bearer token."""
        base_url = Variable.get("MOCK_CUSTOMER_API_URL", default=CUSTOMER_API_DEFAULT)
        response = requests.get(
            f"{base_url}/api/v1/customers",
            headers={"Authorization": VALID_TOKEN},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()
        customers = data["customers"]
        print(f"Extracted {len(customers)} customer records from {data['metadata']['source_system']}")
        return customers

    @task
    def validate_customers(customers: list[dict]) -> list[dict]:
        """Ensure each customer record has required fields."""
        required = {"customer_id", "name", "tier", "points"}
        for record in customers:
            missing = required - record.keys()
            if missing:
                raise ValueError(f"Customer {record.get('customer_id')} missing fields: {missing}")
        print(f"Validated {len(customers)} customer records.")
        return customers

    @task
    def load_customers(customers: list[dict]) -> None:
        """Upsert validated customer records into the warehouse (stubbed for demo)."""
        print(f"Upserting {len(customers)} customer records...")
        # Warehouse upsert goes here
        print("Upsert complete.")

    raw = extract_customers()
    validated = validate_customers(raw)
    load_customers(validated)


novamart_customer_loyalty()
