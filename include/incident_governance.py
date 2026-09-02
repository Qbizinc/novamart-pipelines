"""Incident governance primitives — standalone, not wired into the live incident-response flow.

Addresses the scalability/trust gaps that come up once you picture agentic_incident_memory_v2
running across thousands of pipelines instead of four demo scenarios: recognizing when several
failing pipelines share one root cause, refusing to spin up duplicate work for a retried/repeated
callback, keeping a real queryable incident record instead of a flat memory entry, capping how many
investigations and external-system actions can happen at once, routing ownership, and requiring the
model to show its work before a diagnosis is trusted.

Nothing here is called by agentic_incident_memory_v2.py or include/incident_callbacks.py — it is
exercised only by dags/incident_governance_demo.py. Wiring any of it into the live pipeline is a
deliberate, separate decision this module does not make on its own.
"""

from __future__ import annotations

import hashlib
import json
import re
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path


class IncidentGovernanceError(Exception):
    """Raised when a governance check refuses to let something proceed."""


STATE_DIR = Path(__file__).parent / ".incident-governance"
STATE_FILE = STATE_DIR / "incidents.json"


# --- Idempotency -----------------------------------------------------------------

def compute_idempotency_key(dag_id: str, run_id: str, task_id: str) -> str:
    """One key per (pipeline, run, task). A repeated callback for the exact same failure — a
    retried on_failure_callback, a duplicate trigger — hashes to the same key, so the caller can
    refuse to launch a second investigation of a failure it already handled."""
    raw = f"{dag_id}:{run_id}:{task_id}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def is_duplicate(idempotency_key: str) -> bool:
    """Has this exact (dag, run, task) failure already been processed? True means a repeated
    callback for it should be dropped, not turned into a second investigation."""
    processed_file = STATE_DIR / "processed_keys.json"
    if not processed_file.exists():
        return False
    return idempotency_key in json.loads(processed_file.read_text())


def mark_processed(idempotency_key: str) -> None:
    processed_file = STATE_DIR / "processed_keys.json"
    keys = json.loads(processed_file.read_text()) if processed_file.exists() else []
    if idempotency_key not in keys:
        keys.append(idempotency_key)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    processed_file.write_text(json.dumps(keys))


# --- Correlation / cross-pipeline dedup -------------------------------------------

_NUMERIC = re.compile(r"\d+")


def compute_correlation_signature(exception_type: str, exception_message: str) -> str:
    """A fuzzier signature than the idempotency key: strips numbers/ids out of the exception
    message so the SAME underlying failure (e.g. one upstream outage) hitting DIFFERENT
    pipelines hashes to the same signature, instead of each pipeline's failure looking
    unrelated. Deliberately simple — good enough to catch the common case (identical exception
    type, near-identical message) — a real version would use embedding similarity, the same way
    the incident memory's own RAG index already does for same-pipeline recurrence."""
    normalized = _NUMERIC.sub("#", exception_message.strip().lower())[:200]
    raw = f"{exception_type}:{normalized}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


# --- Persistent incident state -----------------------------------------------------

@dataclass
class Incident:
    incident_id: str
    correlation_signature: str
    status: str  # "investigating" | "resolved"
    root_cause: str | None = None
    affected_dags: list[str] = field(default_factory=list)
    owner: str | None = None
    agent_runs: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    opened_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)


def _load_state() -> dict[str, dict]:
    if not STATE_FILE.exists():
        return {}
    return json.loads(STATE_FILE.read_text())


def _save_state(state: dict[str, dict]) -> None:
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    STATE_FILE.write_text(json.dumps(state, indent=2))


def find_active_incident(correlation_signature: str) -> Incident | None:
    """Is there already an investigating incident for this same underlying failure? If so, the
    caller should attach to it instead of opening a new one — this is what real cross-pipeline
    dedup looks like, as opposed to the existing same-pipeline-only recurrence check."""
    for record in _load_state().values():
        if record["correlation_signature"] == correlation_signature and record["status"] == "investigating":
            return Incident(**record)
    return None


def open_incident(correlation_signature: str, dag_id: str, agent_run_id: str, owner: str | None = None) -> Incident:
    state = _load_state()
    incident_id = f"INC-{len(state) + 1:04d}"
    incident = Incident(
        incident_id=incident_id,
        correlation_signature=correlation_signature,
        status="investigating",
        affected_dags=[dag_id],
        agent_runs=[agent_run_id],
        owner=owner,
    )
    state[incident_id] = asdict(incident)
    _save_state(state)
    return incident


def attach_to_incident(incident_id: str, dag_id: str, agent_run_id: str) -> Incident:
    """A new failure matched an already-active incident — fold it in instead of investigating
    from scratch."""
    state = _load_state()
    record = state[incident_id]
    if dag_id not in record["affected_dags"]:
        record["affected_dags"].append(dag_id)
    record["agent_runs"].append(agent_run_id)
    record["updated_at"] = time.time()
    state[incident_id] = record
    _save_state(state)
    return Incident(**record)


def record_action(incident_id: str, action: str) -> None:
    state = _load_state()
    record = state[incident_id]
    record["actions"].append(action)
    record["updated_at"] = time.time()
    state[incident_id] = record
    _save_state(state)


def resolve_incident(incident_id: str, root_cause: str) -> None:
    state = _load_state()
    record = state[incident_id]
    record["status"] = "resolved"
    record["root_cause"] = root_cause
    record["updated_at"] = time.time()
    state[incident_id] = record
    _save_state(state)


# --- Concurrency control -----------------------------------------------------------

def check_concurrency_limit(active_count: int, max_concurrent: int = 25) -> None:
    """Refuses to launch another investigation once too many are already running. The
    callback + incident DAG pairing has no limit of its own today beyond Airflow's generic
    max_active_runs, so a real failure storm has nothing stopping hundreds of simultaneous LLM
    investigations."""
    if active_count >= max_concurrent:
        raise IncidentGovernanceError(
            f"Refusing to open another investigation: {active_count} already active "
            f"(limit {max_concurrent}). This failure should queue, not launch immediately."
        )


# --- Fleet-wide rate limiting on external systems -----------------------------------

def check_global_rate_limit(action_kind: str, window_seconds: int = 3600, max_per_window: int = 50) -> None:
    """A cap across ALL incidents, not per-incident. The harness's cost governor already caps
    tickets/messages/PRs per SINGLE incident — that's no protection against many different
    incidents each opening their own one ticket in the same storm. Checks (and records) actions
    of one kind against a shared, fleet-wide counter."""
    counter_file = STATE_DIR / f"rate_{action_kind}.json"
    now = time.time()
    events = json.loads(counter_file.read_text()) if counter_file.exists() else []
    events = [t for t in events if now - t < window_seconds]
    if len(events) >= max_per_window:
        raise IncidentGovernanceError(
            f"Fleet-wide rate limit hit for {action_kind!r}: {len(events)} in the last "
            f"{window_seconds}s (limit {max_per_window}). Refusing to take this action."
        )
    events.append(now)
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    counter_file.write_text(json.dumps(events))


# --- Ownership routing ---------------------------------------------------------------

_OWNER_ROUTING = {
    "database": "data-platform-team",
    "cloud": "infra-team",
    "external_service": "integrations-team",
}
_DEFAULT_OWNER = "on-call"


def resolve_owner(category: str) -> str:
    """Which team owns acting on this incident, based on its failure category. A real system
    would look this up from an on-call schedule/PagerDuty rotation; this is a static routing
    table standing in for that."""
    return _OWNER_ROUTING.get(category, _DEFAULT_OWNER)


# --- Per-incident budget ------------------------------------------------------------

def _budget_file(incident_id: str) -> Path:
    return STATE_DIR / f"budget_{incident_id}.json"


def check_and_record_llm_call(incident_id: str, estimated_tokens: int, max_llm_calls: int = 5, max_tokens: int = 50_000) -> None:
    """Per-incident ceiling on how much reasoning an investigation can burn before it must stop
    and escalate to a human, instead of an ambiguous incident consuming resources indefinitely."""
    bf = _budget_file(incident_id)
    budget = json.loads(bf.read_text()) if bf.exists() else {"llm_calls": 0, "tokens": 0}
    if budget["llm_calls"] + 1 > max_llm_calls or budget["tokens"] + estimated_tokens > max_tokens:
        raise IncidentGovernanceError(
            f"Incident {incident_id} hit its investigation budget "
            f"({budget['llm_calls']} calls / {budget['tokens']} tokens so far) — stop and escalate to a human."
        )
    budget["llm_calls"] += 1
    budget["tokens"] += estimated_tokens
    STATE_DIR.mkdir(parents=True, exist_ok=True)
    bf.write_text(json.dumps(budget))


# --- Deterministic-evidence-first triage ----------------------------------------------

def assess_deterministic_evidence(evidence: list[str], min_signals: int = 3) -> bool:
    """Before spending a single LLM call: is there already enough deterministic evidence (log
    matches, known error codes, prior-incident hits) to resolve this without reasoning about it
    at all? The model should be the exception, not the default."""
    return len(evidence) >= min_signals


# --- Fleet economics -------------------------------------------------------------------

def compute_llm_investigation_rate(total_failures: int, llm_investigations: int) -> float:
    """The one metric that actually says whether this scales economically: how many of every
    1,000 failures required an LLM call at all, versus being resolved by correlation,
    known-pattern reuse, or deterministic evidence alone."""
    if total_failures == 0:
        return 0.0
    return (llm_investigations / total_failures) * 1000


# --- Confidence / evidence guard -------------------------------------------------------

def check_confidence(parsed_output: dict, min_confidence: float = 0.7) -> None:
    """The harness's existing output validator only checks an LLM response's shape (right
    fields, right types) — never whether the conclusion is actually correct. This is a step
    toward that: require the model to state its own confidence and cite evidence, and refuse to
    trust a low-confidence or evidence-free conclusion instead of silently acting on it.
    Modeled on qbiz_harness.output_validator's raise-on-violation pattern."""
    confidence = parsed_output.get("confidence")
    evidence = parsed_output.get("evidence")
    if confidence is None:
        raise IncidentGovernanceError("Diagnosis carries no confidence score — refusing to trust it.")
    if confidence < min_confidence:
        raise IncidentGovernanceError(
            f"Diagnosis confidence {confidence:.2f} is below the {min_confidence:.2f} threshold — "
            f"escalate to a human instead of acting on this automatically."
        )
    if not evidence:
        raise IncidentGovernanceError("Diagnosis cites no evidence — refusing to trust it.")
