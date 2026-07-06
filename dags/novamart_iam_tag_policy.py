"""
## NovaMart IAM Tag Policy — Incident Demo

Reads processed sales files from S3 and loads them into Snowflake. Access to
the S3 bucket is granted via a tag-based IAM policy that only allows
`s3:ListBucket` / `s3:GetObject` when the bucket carries the tag
`environment=production`.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- snowflake_default : Snowflake, key-pair/password auth
- aws_default       : AWS, used for S3
- airflow_api       : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- NOVAMART_S3_BUCKET : bucket holding processed sales files (prefix "processed/")

**Failure mode — tag drift silently revokes access:**
Someone renamed the bucket tag from `environment=production` to
`environment=prod` (e.g. during a "tag standardization" cleanup). The
tag-based IAM policy attached to the Airflow execution role only matches
the exact value `production`, so every S3 call starts returning
`AccessDenied` even though nothing in Airflow or the pipeline code changed.

**How to trigger (no code change needed):**
1. Open the AWS Console → S3 → the bucket named in NOVAMART_S3_BUCKET → Tags.
2. Change the `environment` tag value from `production` to `prod` and save.
3. Trigger this DAG manually. `list_processed_files` will raise a
   `ClientError: AccessDenied` on `ListObjectsV2`.
4. Revert the tag to `environment=production` to restore access.
"""

from datetime import datetime

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import Variable, dag, task

from include.novamart_utils import trigger_incident_dag


@dag(
    dag_id="novamart_iam_tag_policy",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "aws", "iam"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_iam_tag_policy():

    @task
    def list_processed_files() -> list[str]:
        """List processed sales files waiting to be loaded."""
        bucket = Variable.get("NOVAMART_S3_BUCKET", default="novamart-pipeline-demo")
        keys = S3Hook(aws_conn_id="aws_default").list_keys(bucket_name=bucket, prefix="processed/") or []
        print(f"Found {len(keys)} processed file(s) in s3://{bucket}/processed/")
        return keys

    @task
    def download_processed_files(keys: list[str]) -> list[str]:
        """Download each processed file's contents from S3."""
        bucket = Variable.get("NOVAMART_S3_BUCKET", default="novamart-pipeline-demo")
        hook = S3Hook(aws_conn_id="aws_default")
        contents = [hook.read_key(key=key, bucket_name=bucket) for key in keys]
        print(f"Downloaded {len(contents)} file(s) from s3://{bucket}/")
        return contents

    @task
    def load_to_snowflake(file_contents: list[str]) -> None:
        """Load the downloaded CSV contents into PROCESSED_SALES."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS PROCESSED_SALES (
                    raw_content VARCHAR,
                    loaded_at   TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            cur.executemany(
                "INSERT INTO PROCESSED_SALES (raw_content) VALUES (%s)",
                [(content,) for content in file_contents],
            )
            print(f"Loaded {len(file_contents)} file(s) into PROCESSED_SALES.")
        finally:
            conn.close()

    keys = list_processed_files()
    contents = download_processed_files(keys)
    load_to_snowflake(contents)


novamart_iam_tag_policy()
