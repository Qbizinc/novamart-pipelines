"""Incident Governance — Scalability Demo (NOT wired into the live pipeline).

Exercises every governance primitive in include/incident_governance.py against a set of
simulated failures — no real Airflow API calls, no real Jira/Slack/GitHub, so it's safe to
trigger any time. Shows what agentic_incident_memory_v2 would need at fleet scale (thousands
of pipelines instead of four demo scenarios), as a progressive triage instead of "call an
agent for every failure":

  duplicate callback?         -> drop
  correlates with an
    already-active incident?  -> attach, reuse its diagnosis, no new LLM call
  enough deterministic
    evidence to resolve alone? -> resolve, no LLM call
  otherwise                   -> the "LLM" is invoked (simulated), under a per-incident budget,
                                  and its confidence is checked before anything acts on it

Paused on creation; nothing triggers it and it isn't wired to any other DAG.
"""

from datetime import datetime

from airflow.sdk import dag, task

from include import incident_governance as gov

SIMULATED_FAILURES = [
    {
        "dag_id": "orders_ingest_eu",
        "run_id": "run_001",
        "task_id": "fetch",
        "exception_type": "ConnectionError",
        "exception_message": "upstream payments-api timed out after 30s (attempt 4821)",
        "category": "external_service",
        "deterministic_evidence": ["4 timeouts in 2 minutes"],
        "diagnosis": {"summary": "payments-api is unreachable", "confidence": 0.91, "evidence": ["4 timeouts in 2 minutes", "payments-api status page shows an incident"]},
    },
    {
        "dag_id": "orders_ingest_us",
        "run_id": "run_002",
        "task_id": "fetch",
        "exception_type": "ConnectionError",
        "exception_message": "upstream payments-api timed out after 30s (attempt 77)",
        "category": "external_service",
        "deterministic_evidence": ["timeout on every retry"],
        "diagnosis": None,  # never gets here -- correlation reuses failure 1's diagnosis
    },
    {
        "dag_id": "orders_ingest_eu",
        "run_id": "run_001",
        "task_id": "fetch",
        "exception_type": "ConnectionError",
        "exception_message": "upstream payments-api timed out after 30s (attempt 4822)",
        "category": "external_service",
        "deterministic_evidence": [],
        "diagnosis": None,  # never gets here -- dropped as a duplicate callback
    },
    {
        "dag_id": "reporting_sync",
        "run_id": "run_014",
        "task_id": "load",
        "exception_type": "ThrottlingException",
        "exception_message": "rate exceeded on warehouse connection pool",
        "category": "database",
        "deterministic_evidence": ["exact match to resolved INC pattern", "known transient pool exhaustion", "auto-recovers within 5 min historically"],
        "diagnosis": None,  # never gets here -- resolved from deterministic evidence alone
    },
    {
        "dag_id": "warehouse_sync",
        "run_id": "run_009",
        "task_id": "load",
        "exception_type": "ProgrammingError",
        "exception_message": "column TOTAL_PRICE is VARCHAR, expected NUMBER",
        "category": "database",
        "deterministic_evidence": [],
        "diagnosis": {"summary": "maybe a schema issue somewhere", "confidence": 0.38, "evidence": []},
    },
]


@dag(
    dag_id="incident_governance_demo",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    is_paused_upon_creation=True,
    doc_md=__doc__,
    tags=["reference", "presentation", "novamart", "scalability"],
)
def incident_governance_demo():

    @task
    def reset_state() -> None:
        import shutil
        if gov.STATE_DIR.exists():
            shutil.rmtree(gov.STATE_DIR)

    @task
    def process_failures() -> list[str]:
        log: list[str] = []
        active_investigations = 0
        llm_investigations = 0

        for i, failure in enumerate(SIMULATED_FAILURES, start=1):
            tag = f"[failure {i}: {failure['dag_id']}]"

            # 1. Idempotency -- a repeated callback for a failure we already handled.
            idem_key = gov.compute_idempotency_key(failure["dag_id"], failure["run_id"], failure["task_id"])
            if gov.is_duplicate(idem_key):
                log.append(f"{tag} DROPPED — duplicate callback for an already-processed failure.")
                continue
            gov.mark_processed(idem_key)

            try:
                gov.check_concurrency_limit(active_investigations, max_concurrent=25)
            except gov.IncidentGovernanceError as exc:
                log.append(f"{tag} QUEUED — {exc}")
                continue
            active_investigations += 1
            owner = gov.resolve_owner(failure["category"])

            # 2. Correlation -- does this match an incident already being worked?
            signature = gov.compute_correlation_signature(failure["exception_type"], failure["exception_message"])
            existing = gov.find_active_incident(signature)
            if existing:
                incident = gov.attach_to_incident(existing.incident_id, failure["dag_id"], failure["run_id"])
                log.append(
                    f"{tag} CORRELATED — matches active {incident.incident_id} (root cause already known: "
                    f"{incident.root_cause!r}). Attached and reused its diagnosis — no new LLM call."
                )
                continue

            # 3. Deterministic evidence -- can this resolve without ever calling the model?
            if gov.assess_deterministic_evidence(failure["deterministic_evidence"], min_signals=3):
                incident = gov.open_incident(signature, failure["dag_id"], failure["run_id"], owner=owner)
                gov.resolve_incident(incident.incident_id, root_cause="matched a known, well-evidenced pattern")
                log.append(
                    f"{tag} RESOLVED FROM EVIDENCE ALONE ({incident.incident_id}) — "
                    f"{len(failure['deterministic_evidence'])} deterministic signals were enough. No LLM call."
                )
                continue

            # 4. Only now does the (simulated) LLM get involved, under a budget.
            incident = gov.open_incident(signature, failure["dag_id"], failure["run_id"], owner=owner)
            try:
                gov.check_and_record_llm_call(incident.incident_id, estimated_tokens=1200, max_llm_calls=5)
            except gov.IncidentGovernanceError as exc:
                log.append(f"{tag} BUDGET EXCEEDED — {exc}")
                continue
            llm_investigations += 1

            try:
                gov.check_confidence(failure["diagnosis"], min_confidence=0.7)
            except gov.IncidentGovernanceError as exc:
                log.append(f"{tag} LOW CONFIDENCE ({incident.incident_id}) — {exc} Escalating to {owner}.")
                continue

            # Deliberately NOT resolved here -- a real investigation stays "investigating" so a
            # later correlated failure (like failure 2, arriving while this is still being
            # worked) can still find and attach to it. Resolving happens on its own timeline
            # (e.g. once the PR merges), not the instant the model answers once.
            try:
                gov.check_global_rate_limit("tickets_created", window_seconds=3600, max_per_window=50)
                gov.record_action(incident.incident_id, f"ticket filed for {failure['dag_id']}")
                log.append(f"{tag} INVESTIGATED ({incident.incident_id}, owner={owner}) — ticket filed.")
            except gov.IncidentGovernanceError as exc:
                log.append(f"{tag} ACTION BLOCKED — {exc}")

        rate = gov.compute_llm_investigation_rate(len(SIMULATED_FAILURES), llm_investigations)
        log.append(
            f"\nSUMMARY: {len(SIMULATED_FAILURES)} failures in -> {llm_investigations} LLM investigation(s) out "
            f"-> {rate:.0f} LLM investigations per 1,000 failures."
        )

        for line in log:
            print(line)
        return log

    reset_state() >> process_failures()


incident_governance_demo()
