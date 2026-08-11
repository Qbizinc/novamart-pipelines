"""Incident memory — thin helpers over the qbiz-agents RAG engine, used as a *library*.

Phase 1 of RAG_INCIDENT_MEMORY_PLAN.md. The agentic incident DAG calls these to:
  - **recall** prior incidents on a pipeline before diagnosing (recurrence detection), and
  - **record** each resolved incident, keyed by its Jira ticket, after the ticket is opened.

We use `rag_mcp` as a plain library (no MCP server): the `qbiz-rag-mcp` package exposes an
import-clean engine (`from rag_mcp.index import get_index`) that embeds + searches in-process. The
index and ledger persist under `RAG_DATA_DIR` — set to a path on the Astro-mounted `include/` dir so
it survives across DAG runs and is shared across worker containers on one host. See the plan's
Phase 2 (pgvector) for when a single-host volume is no longer enough.

Every call is **best-effort**: incident memory must never break incident response, so recall/record
failures are caught and logged, not raised.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

# Where the vector index + ledger live. The authoritative value is the Dockerfile ENV; this default
# keeps the module usable if that ENV is ever missing. A dedicated dir keeps incident records
# separate from any other RAG corpus.
os.environ.setdefault("RAG_DATA_DIR", "/usr/local/airflow/include/.rag-incidents")

INCIDENT_TAG = "incident"

# How close a recalled incident's symptom text has to be (cosine similarity, 0-1) to the current
# failure before it's treated as the SAME still-open incident rather than just "related history".
# search() already scopes recall to this dag_id alone, so this threshold is the only thing standing
# between "worth mentioning" and "skip the duplicate ticket" — tune here if it over/under-fires.
DUPLICATE_SCORE_THRESHOLD = 0.55


def _index():
    # Deferred import so DAG *parsing* never loads rag_mcp/fastembed — only task execution does.
    from rag_mcp.index import get_index

    return get_index()


def _symptom_from_logs(task_logs: dict[str, str]) -> str:
    """A compact symptom string for semantic recall — the tail of each failed task's log."""
    tails = [(log or "")[-400:] for log in (task_logs or {}).values()]
    return " ".join(tails)[:1500].strip()


def _ledger_status(title: str) -> str | None:
    """Best-effort direct read of the ledger for a hit's status tag ('open'/'closed').

    search() hits don't carry tags (only source/title/ordinal/score/text), so this reads the
    ledger file directly rather than adding an unverified method call onto the raw Index object —
    ingest()/search() are the only calls into that object this module otherwise relies on.
    """
    try:
        ledger_path = os.path.join(os.environ["RAG_DATA_DIR"], "ledger.json")
        with open(ledger_path, encoding="utf-8") as f:
            ledger = json.load(f)
        entry = ledger.get(f"text:{title}", {})
        tags = entry.get("tags", [])
        return "open" if "open" in tags else ("closed" if "closed" in tags else None)
    except Exception:
        return None


def recall_similar_incidents(dag_id: str, task_logs: dict[str, str], k: int = 3) -> dict:
    """Search incident memory for prior occurrences on this pipeline.

    Returns {"text": prompt-ready section, "tickets": [ticket keys], "open_duplicate": {...} | None}.
    open_duplicate is set when the best-matching prior incident is still open AND close enough
    (DUPLICATE_SCORE_THRESHOLD) to treat as a recurrence of the SAME issue, not just related
    history — callers use it to add a comment to the existing ticket instead of opening a new one.
    All fields are empty/None if there are no matches or on any error, so the investigation and
    ticket-creation flow both proceed normally.
    """
    try:
        symptom = _symptom_from_logs(task_logs)
        # RAG tag filtering is ANY-of, so scope by the dag_id tag ALONE — in a dedicated incident
        # index that uniquely selects this pipeline's incidents. Including INCIDENT_TAG here would
        # broaden the match to *every* incident (via the shared 'incident' tag), which is wrong.
        hits = _index().search(f"{dag_id} {symptom}".strip(), k=k, tags=[dag_id])
    except Exception as exc:  # best-effort — never block the investigation
        print(f"[incident_memory] recall failed (continuing without prior context): {exc}")
        return {"text": "", "tickets": [], "open_duplicate": None}

    if not hits:
        print(f"[incident_memory] no prior incidents on record for {dag_id}")
        return {"text": "", "tickets": [], "open_duplicate": None}

    # hits are per-chunk, not per-source: a record split into several chunks can match more than
    # once, each with the same title. Dedupe by title (keeping the first/highest-relevance
    # occurrence) so the same prior incident doesn't get quoted or listed multiple times.
    seen_titles: set[str] = set()
    unique_hits = []
    for h in hits:
        title = h.get("title")
        if title and title in seen_titles:
            continue
        if title:
            seen_titles.add(title)
        unique_hits.append(h)

    print(f"[incident_memory] {len(unique_hits)} prior incident(s) for {dag_id}: "
          f"{[h.get('title') for h in unique_hits]}")
    lines = [
        "Prior incidents on this pipeline (from incident memory — treat each as a LEAD to confirm "
        "against the current evidence, not as established fact):",
    ]
    for h in unique_hits:
        lines.append(
            f"\n--- {h.get('title', '?')} (similarity {h.get('score', 0.0):.2f}) ---\n"
            f"{(h.get('text') or '').strip()[:800]}"
        )
    tickets = [h["title"] for h in unique_hits if h.get("title")]

    open_duplicate = None
    top = unique_hits[0]
    top_title = top.get("title")
    top_score = top.get("score", 0.0)
    if top_title and top_score >= DUPLICATE_SCORE_THRESHOLD and _ledger_status(top_title) == "open":
        open_duplicate = {
            "key": top_title,
            "url": f"https://qbizinc.atlassian.net/browse/{top_title}",
            "score": top_score,
        }
        print(f"[incident_memory] treating this as a recurrence of open ticket {top_title} "
              f"(score={top_score:.2f} >= {DUPLICATE_SCORE_THRESHOLD})")

    return {"text": "\n".join(lines), "tickets": tickets, "open_duplicate": open_duplicate}


def build_record(dag_id: str, run_id: str, diagnosis: str, ticket: dict) -> str:
    """Format the stored incident record. The agent's diagnosis is already structured
    ([SUMMARY]/[DIAGNOSIS]/[ROOT CAUSE]/...), so we just add identity + provenance around it."""
    key = ticket.get("key", "UNKNOWN")
    url = ticket.get("url", "")
    detected = datetime.now(timezone.utc).isoformat()
    return (
        f"# Incident {key} — {dag_id}\n"
        f"- Detected: {detected}\n"
        f"- DAG run: {run_id}\n"
        f"- Ticket: {key} {url}\n\n"
        f"{diagnosis.strip()}\n"
    )


def record_incident(dag_id: str, run_id: str, diagnosis: str, ticket: dict,
                    status: str = "open") -> dict | None:
    """Record (or update) an incident in memory, keyed by its Jira ticket. Best-effort.

    The ticket key is the ledger title, and re-ingesting the same title *replaces* the record — so
    a later close-sync can update the same entry in place (flip status to ``closed``) rather than
    creating a duplicate.
    """
    title = ticket.get("key") or f"{dag_id}:{run_id}"
    try:
        result = _index().ingest(
            text=build_record(dag_id, run_id, diagnosis, ticket),
            title=title,
            tags=[INCIDENT_TAG, dag_id, status],
        )
        print(f"[incident_memory] recorded {title} ({status}) -> "
              f"{result.get('chunks_indexed')} chunk(s) in incident memory")
        return result
    except Exception as exc:  # best-effort — the ticket + Slack post already happened
        print(f"[incident_memory] record failed for {title} (continuing): {exc}")
        return None


# --- Seeding ------------------------------------------------------------------------------------
# The one real baseline incident so recurrence detection has something to find on the FIRST real
# investigation. AD-40 is a real Jira ticket (project AD), not a placeholder — created once via
# the same REST call create_ticket_low_priority/urgent_slack_post use. Idempotent (re-ingest by
# title replaces).

SEED_INCIDENTS: list[dict] = [
    {
        "dag_id": "novamart_gold_sales_by_region",
        "key": "AD-40",
        "status": "closed",
        "text": (
            "[SUMMARY] novamart_gold_sales_by_region failed — SUM() error on a column expected to be numeric\n"
            "[DIAGNOSIS] The gold aggregation task failed running SUM() over a column sourced from "
            "SILVER_SALES; the column held non-numeric text instead of the numeric type it was "
            "defined with.\n"
            "[ROOT CAUSE] BRONZE_SALES's column was VARCHAR holding non-numeric text, not the numeric "
            "type novamart_bronze_sales's code defines — a discrepancy between declared and observed "
            "schema. novamart_silver_sales rebuilds SILVER_SALES via `SELECT *` with no explicit "
            "column list, so it carried that discrepancy straight through without erroring itself — "
            "the break only surfaced downstream, at the gold aggregation.\n"
            "[IMPACT] GOLD_SALES_BY_REGION was not refreshed for the affected run.\n"
            "[RECOMMENDED FIX] Compare BRONZE_SALES's current column types/sample values against "
            "what novamart_bronze_sales defines, focusing on whatever column the failing aggregation "
            "uses. Rebuild BRONZE_SALES with that column restored to its correct numeric type."
        ),
    },
]


def seed() -> None:
    """Ingest the baseline incident(s). Safe to re-run."""
    for inc in SEED_INCIDENTS:
        record_incident(
            dag_id=inc["dag_id"],
            run_id="seed",
            diagnosis=inc["text"],
            ticket={"key": inc["key"], "url": f"https://qbizinc.atlassian.net/browse/{inc['key']}"},
            status=inc["status"],
        )


def reset_to_seed() -> None:
    """Wipe every incident record and re-seed just the real baseline ticket(s) in SEED_INCIDENTS,
    so the index starts from the same known state instead of accumulating test/demo tickets.

    Deletes the on-disk index files directly rather than removing sources one by one — ingest()
    is the only index-mutation entry point this module otherwise relies on, and a missing index
    is the same cold-start state ingest() already has to handle on a brand new RAG_DATA_DIR.
    """
    data_dir = os.environ["RAG_DATA_DIR"]
    for filename in ("ledger.json", "chunks.jsonl", "vectors.npy"):
        path = os.path.join(data_dir, filename)
        if os.path.exists(path):
            os.remove(path)
    seed()
