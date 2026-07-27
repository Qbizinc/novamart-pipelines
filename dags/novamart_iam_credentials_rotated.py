"""
## NovaMart IAM Credentials Rotated — Incident Demo

Reads a sales summary from Snowflake and uploads it to S3. The Snowflake
password used to connect is fetched at runtime from AWS Secrets Manager
(secret NOVAMART_SNOWFLAKE_SECRET_ID) rather than the static password
stored on the `snowflake_default` Airflow Connection.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- aws_default       : AWS, used for Secrets Manager + S3
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- NOVAMART_SNOWFLAKE_SECRET_ID : Secrets Manager secret ID (default "novamart/snowflake/password")
- NOVAMART_S3_BUCKET           : destination bucket for the summary upload

**Failure mode — credentials rotated out from under the pipeline:**
Someone rotated the Snowflake password in AWS Secrets Manager (e.g. as part
of routine credential hygiene) but nobody updated anything on the Airflow
side. Since this pipeline always fetches the *current* secret value at
runtime, it should still work... until the secret and the actual Snowflake
account password fall out of sync (e.g. the rotation only updated the
secret, not the account, or vice versa). The result is a Snowflake
`ProgrammingError` / `DatabaseError` for incorrect username/password.

**How to trigger (no code change needed):**
1. Open the AWS Console → Secrets Manager → the secret named in
   NOVAMART_SNOWFLAKE_SECRET_ID.
2. Edit the secret value's `password` field to any incorrect string
   (simulating a rotation that wasn't propagated to the real Snowflake
   account) and save.
3. Trigger this DAG manually. `fetch_snowflake_secret` will pull the bad
   password and `extract_sales_summary` will fail to authenticate.
4. Restore the original secret value to return the pipeline to healthy.
"""

import json
from datetime import datetime

from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag


@dag(
    dag_id="novamart_iam_credentials_rotated",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "aws", "iam"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_iam_credentials_rotated():

    @task
    def fetch_snowflake_secret() -> str:
        """Retrieve the current Snowflake password from AWS Secrets Manager."""
        secret_id = Variable.get("NOVAMART_SNOWFLAKE_SECRET_ID", default="novamart/snowflake/password")
        client = AwsBaseHook(aws_conn_id="aws_default", client_type="secretsmanager").get_conn()
        secret = client.get_secret_value(SecretId=secret_id)
        password = json.loads(secret["SecretString"])["password"]
        print(f"Fetched current Snowflake password from secret {secret_id}.")
        return password

    @task
    def extract_sales_summary(snowflake_password: str) -> dict:
        """Query a summary of today's sales using the freshly-fetched secret."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default", password=snowflake_password).get_conn()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT business_date, COUNT(*), SUM(total_price) "
                "FROM DAILY_SALES WHERE business_date = CURRENT_DATE() "
                "GROUP BY business_date"
            )
            row = cur.fetchone()
            summary = {
                "business_date": str(row[0]) if row else None,
                "transaction_count": row[1] if row else 0,
                "total_revenue": float(row[2]) if row and row[2] is not None else 0.0,
            }
            print(f"Sales summary: {summary}")
            return summary
        finally:
            conn.close()

    @task
    def upload_summary_to_s3(summary: dict) -> None:
        """Write the sales summary to S3 as a JSON object."""
        bucket = Variable.get("NOVAMART_S3_BUCKET", default="novamart-pipeline-demo")
        key = f"sales-summaries/{summary.get('business_date') or 'unknown'}.json"
        S3Hook(aws_conn_id="aws_default").load_string(
            string_data=json.dumps(summary),
            key=key,
            bucket_name=bucket,
            replace=True,
        )
        print(f"Uploaded summary to s3://{bucket}/{key}")

    secret = fetch_snowflake_secret()
    summary = extract_sales_summary(secret)
    upload_summary_to_s3(summary)


novamart_iam_credentials_rotated()
