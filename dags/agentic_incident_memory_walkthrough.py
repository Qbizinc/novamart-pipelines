"""
Incident Response — Architecture Walkthrough (NOT the real pipeline).
"""

from datetime import datetime

from airflow.sdk import dag, task
from pydantic_ai.toolsets.function import FunctionToolset

MODEL_WEAK = "anthropic:claude-haiku-4-5"
MODEL_FRONTIER = "anthropic:claude-sonnet-5"


def list_dag_ids() -> list[str]:
    return ["novamart_transactions_csv_export", "novamart_transactions_load_qa"]


def get_dag_source(dag_id: str) -> str:
    return "# (mock) source of another pipeline, fetched on demand"


def find_blast_radius(dag_id: str) -> list[dict]:
    return []


dag_lookup_toolset = FunctionToolset(tools=[list_dag_ids, get_dag_source, find_blast_radius])


def list_s3_keys(prefix: str) -> list[str]:
    return ["transactions/transactions_2026-06-03.csv"]


aws_toolset = FunctionToolset(tools=[list_s3_keys])


def run_sql(query: str) -> list[dict]:
    return [{"transaction_id": "TXN-4A0BA5E6", "occurrences": 2}]


snowflake_toolset = FunctionToolset(tools=[run_sql])


@dag(
    dag_id="agentic_incident_memory_walkthrough",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    doc_md=__doc__,
    tags=["reference", "presentation", "novamart"],
)
def agentic_incident_memory_walkthrough():

    @task
    def gather_context() -> dict:
        # Collect what failed: dag_id, run_id, logs, source code, criticality.
        return {"failed_dag_id": "novamart_transactions_load_qa", "is_critical": False}

    @task
    def recall_prior_incidents(ctx: dict) -> dict:
        # Search incident memory for a similar past failure on this pipeline.
        return {"open_duplicate": None}

    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        model_id=MODEL_FRONTIER,
        system_prompt="Pick which investigator matches where this failure happened: "
                       "investigate_aws, investigate_api, or investigate_snowflake.",
    )
    def classify_platform(ctx: dict) -> str:
        return f"Pipeline: {ctx['failed_dag_id']}. It fails a Snowflake uniqueness check."

    @task.agent(toolsets=[aws_toolset, dag_lookup_toolset], llm_conn_id="pydanticai_default", model_id=MODEL_WEAK)
    def investigate_aws(ctx: dict) -> str:
        return "Diagnose an AWS/S3 failure and return one sentence."

    @task.agent(toolsets=[dag_lookup_toolset], llm_conn_id="pydanticai_default", model_id=MODEL_WEAK)
    def investigate_api(ctx: dict) -> str:
        return "Diagnose an external API failure and return one sentence."

    @task.agent(toolsets=[snowflake_toolset, dag_lookup_toolset], llm_conn_id="pydanticai_default", model_id=MODEL_WEAK)
    def investigate_snowflake(ctx: dict) -> str:
        return "A duplicate row broke a uniqueness check. Diagnose it in one sentence."

    @task(trigger_rule="none_failed_min_one_success")
    def merge_diagnosis(aws: str | None, api: str | None, snowflake: str | None) -> str:
        return aws or api or snowflake

    @task.llm_branch(
        llm_conn_id="pydanticai_default",
        model_id=MODEL_FRONTIER,
        system_prompt="Pick one: propose_code_fix (a genuine, fixable code bug), "
                      "urgent_slack_post (critical + not fixable here), or "
                      "create_ticket_low_priority (everything else).",
    )
    def decide_path(diagnosis: str, prior_incidents: dict) -> str:
        return diagnosis

    @task.agent(toolsets=[dag_lookup_toolset], llm_conn_id="pydanticai_default", model_id=MODEL_FRONTIER)
    def propose_code_fix(diagnosis: str) -> str:
        return f"In one sentence, describe the fix for: {diagnosis}"

    @task
    def urgent_slack_post(diagnosis: str) -> dict:
        # Page a human immediately: this pipeline is critical.
        return {"ticket": "AD-XXX", "priority": "Highest"}

    @task
    def create_ticket_low_priority(diagnosis: str) -> dict:
        # Not urgent, not fixable here: file it for the backlog.
        return {"ticket": "AD-XXX", "priority": "Low"}

    @task(trigger_rule="none_failed_min_one_success")
    def record_incident(fix: str | None, escalation: dict | None, ticket: dict | None) -> None:
        # Whichever path ran, remember it so the next similar failure recalls it.
        print(f"Recorded: {fix or escalation or ticket}")

    ctx = gather_context()
    prior = recall_prior_incidents(ctx)

    classification = classify_platform(ctx)
    diag_aws = investigate_aws(ctx)
    diag_api = investigate_api(ctx)
    diag_snowflake = investigate_snowflake(ctx)
    classification >> [diag_aws, diag_api, diag_snowflake]

    diagnosis = merge_diagnosis(diag_aws, diag_api, diag_snowflake)
    decision = decide_path(diagnosis, prior)

    fix = propose_code_fix(diagnosis)
    escalation = urgent_slack_post(diagnosis)
    ticket = create_ticket_low_priority(diagnosis)
    decision >> [fix, escalation, ticket]

    record_incident(fix, escalation, ticket)


agentic_incident_memory_walkthrough()
