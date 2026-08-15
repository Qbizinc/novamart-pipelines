"""Executive Sales Report. Critical; auto-triggered when novamart_marketing_daily_summary produces MARKETING_DAILY_SUMMARY."""

from datetime import datetime, timezone

from airflow.sdk import Asset, Variable, dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from include import harness_audit
from include.incident_callbacks import trigger_incident_dag_v2

SUMMARY_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.MARKETING_DAILY_SUMMARY"
REPORT_TABLE = "SANDBOX_DATA_PIPELINE.NOVAMART_RAW.DAILY_EXEC_SALES_REPORT"
MARKETING_DAILY_SUMMARY_ASSET = Asset("marketing_daily_summary")

# The default model summarize_report uses — templating already-validated numbers into a paragraph
# needs no reasoning, so this activity is capped at WEAK by harness_audit.new_model_policy()'s
# "summarize_exec_report" entry (cost control: a trivial step shouldn't self-escalate to a
# frontier model). Read from an Airflow Variable at task RUNTIME rather than hardcoded, so the
# model can be changed without a redeploy — see NOVAMART_EXEC_SUMMARY_MODEL in airflow_settings.yaml.
MODEL_WEAK = "anthropic:claude-haiku-4-5"


@dag(
    dag_id="novamart_exec_sales_report",
    start_date=datetime(2026, 1, 1),
    schedule=[MARKETING_DAILY_SUMMARY_ASSET],
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "marketing", "critical"],
    default_args={"on_failure_callback": trigger_incident_dag_v2},
)
def novamart_exec_sales_report():

    @task
    def build_report() -> dict:
        business_date = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            cur = conn.cursor()
            cur.execute(f"""
                CREATE TABLE IF NOT EXISTS {REPORT_TABLE} (
                    business_date      DATE          NOT NULL,
                    total_spend        FLOAT         NOT NULL,
                    total_clicks       INTEGER       NOT NULL,
                    cost_per_click     FLOAT         NOT NULL,
                    executive_summary  VARCHAR(2000),
                    loaded_at          TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
                )
            """)
            # Table may already exist from before executive_summary was added — ADD COLUMN IF NOT
            # EXISTS covers that without needing a one-off migration step.
            cur.execute(f"ALTER TABLE {REPORT_TABLE} ADD COLUMN IF NOT EXISTS executive_summary VARCHAR(2000)")
            cur.execute(f"DELETE FROM {REPORT_TABLE} WHERE business_date = %s", (business_date,))
            row_count = cur.execute(f"""
                INSERT INTO {REPORT_TABLE}
                    (business_date, total_spend, total_clicks, cost_per_click)
                SELECT business_date, total_spend, total_clicks,
                       total_spend / NULLIF(total_clicks, 0)
                FROM {SUMMARY_TABLE}
                WHERE business_date = %s
            """, (business_date,)).rowcount
            print(f"Loaded {row_count} row(s) into {REPORT_TABLE} for {business_date}.")

            cur.execute(f"""
                SELECT total_spend, total_clicks, cost_per_click
                FROM {REPORT_TABLE} WHERE business_date = %s
            """, (business_date,))
            row = cur.fetchone()
        finally:
            conn.close()

        total_spend, total_clicks, cost_per_click = row if row else (0.0, 0, 0.0)
        return {
            "business_date": business_date,
            "total_spend": float(total_spend),
            "total_clicks": int(total_clicks),
            "cost_per_click": float(cost_per_click or 0.0),
        }

    @task
    def summarize_report(report: dict, dag_run=None) -> str:
        """Turn the report's numbers into a one-paragraph executive-facing blurb — genuinely
        mechanical (templating already-validated numbers into prose, no reasoning, no tools), the
        same category as harness_audit.new_model_policy()'s other WEAK-capped activities.

        Reads its model from NOVAMART_EXEC_SUMMARY_MODEL (Variable, checked at runtime) rather
        than a hardcoded constant. If that Variable is ever set to a model outside this activity's
        policy, the task fails before making any API call — which, like any other failure on this
        critical pipeline, cascades into agentic_incident_memory_v2 via on_failure_callback.
        """
        run_id = dag_run.run_id if dag_run else "manual"
        model_id = Variable.get("NOVAMART_EXEC_SUMMARY_MODEL", default=MODEL_WEAK)

        harness_audit.check_model_policy(
            "summarize_exec_report", model_id,
            agent_id="novamart_exec_sales_report",
            incident_id=f"novamart_exec_sales_report:{run_id}",
            job_id=run_id,
        )

        from pydantic_ai import Agent

        agent = Agent(model_id)
        prompt = (
            "Write a single concise executive-facing paragraph (2-3 sentences, plain factual "
            "business tone, no bullet points, no speculation beyond these numbers) summarizing "
            f"marketing performance for {report['business_date']}: total spend "
            f"${report['total_spend']:,.2f}, {report['total_clicks']:,} clicks, cost-per-click "
            f"${report['cost_per_click']:.2f}."
        )
        result = agent.run_sync(prompt)
        summary = str(getattr(result, "output", None) or getattr(result, "data", None) or result)

        conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
        try:
            conn.cursor().execute(
                f"UPDATE {REPORT_TABLE} SET executive_summary = %s WHERE business_date = %s",
                (summary, report["business_date"]),
            )
        finally:
            conn.close()

        print(f"[summarize_report] model={model_id}\n{summary}")
        return summary

    summarize_report(build_report())


novamart_exec_sales_report()
