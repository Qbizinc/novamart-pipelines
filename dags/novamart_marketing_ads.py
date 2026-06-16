"""
## NovaMart Marketing Ads Pipeline

Fetches ad campaign performance metrics from the NovaMart Ads API (mock on port 5003)
and loads them into the data warehouse for reporting.

**Failure mode:** rate limiting — when toggled into error mode the API returns
HTTP 429 with a `Retry-After: 3600` header, raising an `HTTPError`.

Toggle error on:  POST http://localhost:5003/toggle-error  {"healthy": false}
Toggle error off: POST http://localhost:5003/toggle-error  {"healthy": true}
"""

from datetime import datetime

import requests
from airflow.sdk import Variable
from airflow.sdk import dag, task

MARKETING_API_DEFAULT = "http://host.docker.internal:5003"


@dag(
    dag_id="novamart_marketing_ads",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "marketing"],
)
def novamart_marketing_ads():

    @task
    def extract_campaigns() -> list[dict]:
        """Pull ad campaign metrics from the Marketing Ads API."""
        base_url = Variable.get("MOCK_MARKETING_API_URL", default=MARKETING_API_DEFAULT)
        response = requests.get(f"{base_url}/api/v1/campaigns", timeout=30)

        if response.status_code == 429:
            retry_after = response.headers.get("Retry-After", "unknown")
            raise RuntimeError(
                f"Rate limit exceeded from Marketing Ads API. "
                f"Retry-After: {retry_after}s. Response: {response.json()}"
            )

        response.raise_for_status()
        data = response.json()
        campaigns = data["campaigns"]
        print(f"Extracted {len(campaigns)} campaigns for {data['metadata']['business_date']}")
        return campaigns

    @task
    def validate_campaigns(campaigns: list[dict]) -> list[dict]:
        """Ensure each campaign record has required metrics."""
        required = {"campaign_id", "impressions", "clicks", "spend"}
        for record in campaigns:
            missing = required - record.keys()
            if missing:
                raise ValueError(f"Campaign {record.get('campaign_id')} missing fields: {missing}")
        print(f"Validated {len(campaigns)} campaign records.")
        return campaigns

    @task
    def load_campaigns(campaigns: list[dict]) -> None:
        """Load validated campaign metrics into the warehouse (stubbed for demo)."""
        print(f"Loading {len(campaigns)} campaign records...")
        # Warehouse write goes here
        print("Load complete.")

    raw = extract_campaigns()
    validated = validate_campaigns(raw)
    load_campaigns(validated)


novamart_marketing_ads()
