"""Harness wiring for agentic_incident_memory_v2's consequential actions — Slack posts,
Jira ticket creation, GitHub PR opening. Uses qbiz_harness (CostGovernor + AuditLog +
validate_output) as a plain library, same pattern as include/incident_memory.py.

Every task that needs a cap or audit trail constructs its own fresh CostGovernor/AuditLog —
Airflow tasks are separate processes, and these are in-memory Python objects that can't be
shared across tasks via XCom (XCom is JSON-serialized). Cross-task correlation happens through
shared data in every audit event (incident_id, cohort, job_id), not a shared object.

The audit log persists as local JSONL under RAG_DATA_DIR's sibling directory, on the same
Astro-mounted include/ volume as the RAG index — meaning, same caveat as incident_memory.py:
this assumes a single host with a shared volume across worker containers, not a durable
multi-host store.
"""
from __future__ import annotations

AUDIT_LOG_PATH = "/usr/local/airflow/include/.harness-audit/audit.jsonl"
DAG_ID = "agentic_incident_memory_v2"
COHORT = "novamart"

# CostGovernor requires token/spend limits even when a task only uses the action-count dimension.
# The four tasks that use this helper make no LLM calls, so these are inert placeholders —
# pre_call/post_call (the only methods that check them) are never invoked here.
_NO_LLM_TOKEN_LIMIT = 0
_NO_LLM_SPEND_LIMIT = 0.0


def new_audit_log():
    # Deferred import so DAG *parsing* never loads qbiz_harness — only task execution does.
    from qbiz_harness import AuditLog

    return AuditLog(path=AUDIT_LOG_PATH)


def new_action_governor(action_limits: dict[str, int]):
    from qbiz_harness import CostGovernor

    return CostGovernor(
        token_limit=_NO_LLM_TOKEN_LIMIT,
        spend_limit_usd=_NO_LLM_SPEND_LIMIT,
        action_limits=action_limits,
    )


def guard_action(governor, audit, *, kind: str, action: str, incident_id: str, job_id: str) -> None:
    """Record one action against the governor's cap for `kind`.

    On BudgetExceededError, logs the intervention and re-raises — a tripped cap must fail the
    task, per the harness rule that a caught HarnessError routes to re-prompt/escalate/halt, not
    a workaround. Never catch-and-continue past this call.
    """
    from qbiz_harness import BudgetExceededError

    try:
        governor.record_action(kind)
    except BudgetExceededError as exc:
        audit.record_intervention(
            agent_id=DAG_ID,
            action=action,
            component="cost_governor",
            prevented=str(exc),
            incident_id=incident_id,
            cohort=COHORT,
            job_id=job_id,
        )
        raise


def guard_output(audit, *, response, expected_schema: dict, action: str, incident_id: str, job_id: str):
    """Validate `response` against `expected_schema`; return it unchanged on success.

    On OutputRejectedError, logs the intervention and re-raises — same never-work-around rule as
    guard_action.
    """
    from qbiz_harness import OutputRejectedError, validate_output

    try:
        validate_output(response, expected_schema=expected_schema)
    except OutputRejectedError as exc:
        audit.record_intervention(
            agent_id=DAG_ID,
            action=action,
            component="output_validator",
            prevented=str(exc),
            incident_id=incident_id,
            cohort=COHORT,
            job_id=job_id,
        )
        raise
    return response


def new_model_policy():
    """Build the model-tier policy for this DAG's agent/branch steps.

    Unlike the other constructors here, this is meant to be called at DAG *parse* time (see
    agentic_incident_memory_v2.py's module-level enforce_model_policy() call) — a model-tier
    violation should fail DAG parsing outright, not wait for a task to run. That is a deliberate
    exception to this module's usual "qbiz_harness only loads at task execution" rule.

    classify_platform/investigate_aws/investigate_api are cheap triage/classification steps and
    are capped at WEAK on purpose (cost control — a trivial step shouldn't self-escalate).
    investigate_snowflake/decide_path/propose_code_fix are hard-floored at FRONTIER: this session
    found by direct experiment that a WEAK model on these steps finds the right evidence and then
    reasons past it anyway (see git history) — that floor is evidence-backed, not a preference.
    """
    from qbiz_harness import ActivityBand, ModelPolicy, Tier

    tier_map = {
        "anthropic:claude-haiku-4-5": Tier.WEAK,
        "anthropic:claude-sonnet-5": Tier.FRONTIER,
    }
    activities = {
        "classify_platform": ActivityBand(max_tier=Tier.WEAK),
        "investigate_aws": ActivityBand(max_tier=Tier.WEAK),
        "investigate_api": ActivityBand(max_tier=Tier.WEAK),
        "investigate_snowflake": ActivityBand(max_tier=Tier.FRONTIER, min_tier=Tier.FRONTIER, floor_hard=True),
        "decide_path": ActivityBand(max_tier=Tier.FRONTIER, min_tier=Tier.FRONTIER, floor_hard=True),
        "propose_code_fix": ActivityBand(max_tier=Tier.FRONTIER, min_tier=Tier.FRONTIER, floor_hard=True),
    }
    return ModelPolicy(tier_map=tier_map, activities=activities)


def enforce_model_policy(model_by_activity: dict[str, str]) -> None:
    """Check every activity's configured model against the policy; raises ModelPolicyError on
    the first violation. Called at DAG parse time, not per-task — see new_model_policy()."""
    policy = new_model_policy()
    for activity, model in model_by_activity.items():
        policy.check(activity, model)
