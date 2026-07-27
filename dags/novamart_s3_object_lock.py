"""
## NovaMart S3 Object Lock — Incident Demo

Corrects historical sales data by overwriting a previously-written S3
object with a fixed version, then verifies the overwrite actually took
effect.

On any failure this DAG automatically triggers agentic_snowflake_incident,
which diagnoses the root cause, opens a Jira ticket, and posts to Slack.

Required Airflow Connections (set in airflow_settings.yaml):
- aws_default : AWS, used for S3
- airflow_api : Airflow REST API (used by the failure callback)

Required Airflow Variables:
- NOVAMART_S3_BUCKET : bucket holding the historical correction file

**Failure mode — Object Lock silently blocks the overwrite:**
The S3 bucket has Object Lock enabled in Compliance mode with a retention
period on the target object. `write_corrected_file` calls `put_object`
expecting to overwrite the file in place. Under Object Lock, S3 either
rejects the overwrite or (depending on versioning configuration) creates a
new version while the "current" version the rest of the pipeline reads
back is still the old, uncorrected one — so the call can return success
while the data readers see is unchanged. `verify_correction_applied` reads
the object back and raises if its content doesn't match what was just
written, catching the silent no-op.

**How to trigger:**
1. Open the AWS Console → S3 → the bucket named in NOVAMART_S3_BUCKET →
   Properties → enable Object Lock in Compliance mode (or apply a
   Compliance-mode retention/legal hold directly to the target object,
   `historical-corrections/sales_correction.json`).
2. Trigger this DAG manually. `write_corrected_file` will appear to
   succeed, but `verify_correction_applied` will raise a `ValueError`
   because the object it reads back still has the old content.
3. Remove the retention/legal hold (or wait for it to expire) to restore
   normal overwrite behavior.
"""

import json
from datetime import datetime, timezone

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.sdk import Variable, dag, task

from include.incident_callbacks import trigger_incident_dag

CORRECTION_KEY = "historical-corrections/sales_correction.json"


@dag(
    dag_id="novamart_s3_object_lock",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "aws", "s3"],
    default_args={"on_failure_callback": trigger_incident_dag},
)
def novamart_s3_object_lock():

    @task
    def write_corrected_file() -> str:
        """Overwrite the historical correction file with a fresh, uniquely-stamped payload."""
        bucket = Variable.get("NOVAMART_S3_BUCKET", default="novamart-pipeline-demo")
        payload = {
            "corrected_at": datetime.now(timezone.utc).isoformat(),
            "note": "historical sales correction",
        }
        content = json.dumps(payload)
        S3Hook(aws_conn_id="aws_default").load_string(
            string_data=content, key=CORRECTION_KEY, bucket_name=bucket, replace=True,
        )
        print(f"Wrote correction to s3://{bucket}/{CORRECTION_KEY}: {content}")
        return content

    @task
    def verify_correction_applied(expected_content: str) -> None:
        """Read the object back and raise if it doesn't match what was just written."""
        bucket = Variable.get("NOVAMART_S3_BUCKET", default="novamart-pipeline-demo")
        actual_content = S3Hook(aws_conn_id="aws_default").read_key(key=CORRECTION_KEY, bucket_name=bucket)
        if actual_content != expected_content:
            raise ValueError(
                f"Correction did not apply: s3://{bucket}/{CORRECTION_KEY} still holds stale "
                f"content ({actual_content!r}) instead of the value just written "
                f"({expected_content!r}). The bucket may have Object Lock in Compliance mode "
                "blocking the overwrite."
            )
        print("Correction verified — object content matches what was written.")

    written = write_corrected_file()
    verify_correction_applied(written)


novamart_s3_object_lock()
