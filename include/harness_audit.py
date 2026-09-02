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

import json

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


def find_intervention(job_id: str) -> dict | None:
    """The harness intervention recorded against `job_id`, if the failure was one.

    When a control blocks a step, the *failing* DAG writes a `harness_intervention` record
    carrying that run's job_id. Reading it back lets the incident response tell a governance
    event ("someone pointed a capped step at a frontier model") apart from a data incident
    ("the gold table is broken"). They want different handling: a config violation is already
    prevented and belongs on the harness console, not in the incident queue as a work item
    nobody needs to triage.

    Best-effort by design, like the rest of this module: an unreadable trail degrades to
    "treat it as a normal incident", which is the safe direction to be wrong in.
    """
    try:
        with open(AUDIT_LOG_PATH, encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                record = json.loads(line)
                if record.get("job_id") != job_id:
                    continue
                if record.get("event_type") == "agent_action":
                    continue
                intervention = record.get("intervention") or {}
                return {
                    "component": intervention.get("component", "unknown"),
                    "prevented": intervention.get("prevented", ""),
                    "action": record.get("action", ""),
                }
    except Exception as exc:
        print(f"[harness_audit] could not scan the trail for {job_id} (continuing): {exc}")
    return None


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
    """Build the model-tier policy shared across every DAG that uses this module — one policy
    object, not one per DAG, the same way a real org would centrally govern model-tier spend
    rather than let each pipeline define its own rules.

    classify_platform/investigate_cloud/investigate_external_service are cheap triage/classification steps and
    are capped at WEAK on purpose (cost control — a trivial step shouldn't self-escalate).
    investigate_database/decide_path/propose_code_fix are hard-floored at FRONTIER: this session
    found by direct experiment that a WEAK model on these steps finds the right evidence and then
    reasons past it anyway (see git history) — that floor is evidence-backed, not a preference.
    summarize_exec_report (novamart_exec_sales_report) is capped at WEAK for the opposite reason:
    templating already-validated numbers into a paragraph is mechanical, no reasoning required, so
    a frontier model there is pure cost with no quality upside — the textbook case for a ceiling
    rather than a floor.
    """
    from qbiz_harness import ActivityBand, ModelPolicy, Tier

    tier_map = {
        "anthropic:claude-haiku-4-5": Tier.WEAK,
        "anthropic:claude-sonnet-5": Tier.FRONTIER,
    }
    activities = {
        "classify_platform": ActivityBand(max_tier=Tier.WEAK),
        "investigate_cloud": ActivityBand(max_tier=Tier.WEAK),
        "investigate_external_service": ActivityBand(max_tier=Tier.WEAK),
        "investigate_database": ActivityBand(max_tier=Tier.FRONTIER, min_tier=Tier.FRONTIER, floor_hard=True),
        "decide_path": ActivityBand(max_tier=Tier.FRONTIER, min_tier=Tier.FRONTIER, floor_hard=True),
        "propose_code_fix": ActivityBand(max_tier=Tier.FRONTIER, min_tier=Tier.FRONTIER, floor_hard=True),
        "summarize_exec_report": ActivityBand(max_tier=Tier.WEAK),
    }
    return ModelPolicy(tier_map=tier_map, activities=activities)


def enforce_model_policy(model_by_activity: dict[str, str]) -> None:
    """Check every activity's configured model against the policy; raises ModelPolicyError on
    the first violation. Called at DAG *parse* time (agentic_incident_memory_v2.py's module-level
    call) — for DAGs whose model choice is a hardcoded constant, a violation should fail DAG
    parsing outright rather than wait for a task to run. That is a deliberate exception to this
    module's usual "qbiz_harness only loads at task execution" rule.

    For a DAG whose model choice is only known at task RUNTIME (e.g. read from an Airflow
    Variable, so it can be toggled without a redeploy) use check_model_policy() instead — same
    policy, but checked per-task-run so a violation fails that task (and can cascade into
    on_failure_callback) instead of blocking the DAG from ever parsing.
    """
    policy = new_model_policy()
    for activity, model in model_by_activity.items():
        policy.check(activity, model)


def check_model_policy(activity: str, model_id: str, *, agent_id: str, incident_id: str, job_id: str) -> None:
    """Runtime counterpart to enforce_model_policy() — see that function's docstring for when to
    use which. On violation, best-effort logs the intervention (never let audit logging itself
    mask the real policy violation) then re-raises — same never-work-around rule as
    guard_action/guard_output: a caught HarnessError routes to re-prompt/escalate/halt, not a
    silent fallback to a different model.
    """
    from qbiz_harness import ModelPolicyError

    try:
        new_model_policy().check(activity, model_id)
    except ModelPolicyError as exc:
        try:
            new_audit_log().record_intervention(
                agent_id=agent_id,
                action=activity,
                component="model_policy",
                prevented=str(exc),
                incident_id=incident_id,
                cohort=COHORT,
                job_id=job_id,
            )
        except Exception:
            pass
        raise
