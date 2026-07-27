"""
## Demo Three — Snowflake Schema Drift -> Low-Priority Jira Ticket

Single-task pipeline that always fails loading into DEMO_THREE_ORDERS. The table is created (on
first run) with only two columns; the INSERT always references a third, "region", that was never
part of the table — simulating a schema that drifted out from under this pipeline externally, not
a bug introduced by this pipeline's own code. This DAG is deliberately NOT tagged critical, so
decide_path routes to create_ticket_low_priority (a Jira ticket + a quiet, non-@mention Slack
FYI), never to a PR or an urgent page.

On any failure this DAG triggers agentic_incident_memory_v2 via trigger_incident_dag_v2.

Required Airflow Connections: snowflake_default (key-pair auth), airflow_api (used by the failure
callback).

**How to trigger:** just run this DAG — it always fails the same way, no Variable to set. The
table is created once on the first run and stays without the `region` column from then on, so
every run fails identically.
"""

from datetime import datetime

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.sdk import dag, task

from include.incident_callbacks import trigger_incident_dag_v2

SAMPLE_ORDERS = [
    ("O-2001", 54.00, "east"),
    ("O-2002", 19.99, "west"),
]


@dag(
    dag_id="demo_three_snowflake_ticket",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "demo"],  # deliberately NOT critical -> routes to TICKET
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def demo_three_snowflake_ticket():

    @task
    def load_to_snowflake() -> None:
        """Load orders into DEMO_THREE_ORDERS. The table (created here, once) never has a
        `region` column, but the INSERT always references one — as if the column had already
        been dropped externally before this pipeline ever ran."""
        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute("""
                CREATE TABLE IF NOT EXISTS DEMO_THREE_ORDERS (
                    order_id VARCHAR(64) NOT NULL,
                    amount   FLOAT       NOT NULL
                )
            """)
            cur.executemany(
                "INSERT INTO DEMO_THREE_ORDERS (order_id, amount, region) VALUES (%s, %s, %s)",
                SAMPLE_ORDERS,
            )
            print(f"Loaded {len(SAMPLE_ORDERS)} orders into DEMO_THREE_ORDERS.")
        finally:
            conn.close()

    load_to_snowflake()


demo_three_snowflake_ticket()
