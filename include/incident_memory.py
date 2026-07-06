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

import os
from datetime import datetime, timezone

# Where the vector index + ledger live. The authoritative value is the Dockerfile ENV; this default
# keeps the module usable if that ENV is ever missing. A dedicated dir keeps incident records
# separate from any other RAG corpus.
os.environ.setdefault("RAG_DATA_DIR", "/usr/local/airflow/include/.rag-incidents")

INCIDENT_TAG = "incident"


def _index():
    # Deferred import so DAG *parsing* never loads rag_mcp/fastembed — only task execution does.
    from rag_mcp.index import get_index

    return get_index()


def _symptom_from_logs(task_logs: dict[str, str]) -> str:
    """A compact symptom string for semantic recall — the tail of each failed task's log."""
    tails = [(log or "")[-400:] for log in (task_logs or {}).values()]
    return " ".join(tails)[:1500].strip()


def recall_similar_incidents(dag_id: str, task_logs: dict[str, str], k: int = 3) -> str:
    """Search incident memory for prior occurrences on this pipeline.

    Returns a prompt-ready section (empty string if there are no matches or on any error, so the
    investigation proceeds normally).
    """
    try:
        symptom = _symptom_from_logs(task_logs)
        # RAG tag filtering is ANY-of, so scope by the dag_id tag ALONE — in a dedicated incident
        # index that uniquely selects this pipeline's incidents. Including INCIDENT_TAG here would
        # broaden the match to *every* incident (via the shared 'incident' tag), which is wrong.
        hits = _index().search(f"{dag_id} {symptom}".strip(), k=k, tags=[dag_id])
    except Exception as exc:  # best-effort — never block the investigation
        print(f"[incident_memory] recall failed (continuing without prior context): {exc}")
        return ""

    if not hits:
        print(f"[incident_memory] no prior incidents on record for {dag_id}")
        return ""

    print(f"[incident_memory] {len(hits)} prior incident(s) for {dag_id}: "
          f"{[h.get('title') for h in hits]}")
    lines = [
        "Prior incidents on this pipeline (from incident memory — treat each as a LEAD to confirm "
        "against the current evidence, not as established fact):",
    ]
    for h in hits:
        lines.append(
            f"\n--- {h.get('title', '?')} (similarity {h.get('score', 0.0):.2f}) ---\n"
            f"{(h.get('text') or '').strip()[:800]}"
        )
    return "\n".join(lines)


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


# --- Seeding (demo convenience) ---------------------------------------------------------------
# A couple of realistic past incidents so the recurrence demo has something to find on the FIRST
# real investigation. Run the `novamart_incident_memory_seed` DAG once. Idempotent (re-ingest by
# title replaces).

SEED_INCIDENTS: list[dict] = [
    {
        "dag_id": "novamart_snowflake_sales",
        "key": "AD-1001",
        "status": "closed",
        "text": (
            "[SUMMARY] novamart_snowflake_sales load failed — DAILY_SALES schema drift (missing sku)\n"
            "[DIAGNOSIS] load_to_snowflake raised a column/value mismatch inserting into DAILY_SALES.\n"
            "[ROOT CAUSE] The sku column was dropped from DAILY_SALES while the loader still inserts "
            "it; the INSERT column list no longer matches the table.\n"
            "[IMPACT] No sales rows loaded for the affected business_date until the schema was fixed.\n"
            "[RECOMMENDED FIX] Restore the sku column (ALTER TABLE DAILY_SALES ADD COLUMN sku ...) or "
            "align the loader's INSERT list with the current table; add a schema-contract check."
        ),
    },
    {
        "dag_id": "novamart_snowflake_sales",
        "key": "AD-1002",
        "status": "closed",
        "text": (
            "[SUMMARY] novamart_snowflake_sales validation failed — records missing required fields\n"
            "[DIAGNOSIS] validate_sales raised 'missing fields' because generated records lacked sku.\n"
            "[ROOT CAUSE] NOVAMART_INJECT_BAD_DATA was left set to true, so generate_sales dropped the "
            "sku field from every record.\n"
            "[IMPACT] The run aborted at validation; no data reached Snowflake.\n"
            "[RECOMMENDED FIX] Set NOVAMART_INJECT_BAD_DATA=false; the toggle is a demo fault injector, "
            "not a production setting."
        ),
    },
]


def seed() -> None:
    """Ingest the example incidents. Safe to re-run."""
    for inc in SEED_INCIDENTS:
        record_incident(
            dag_id=inc["dag_id"],
            run_id="seed",
            diagnosis=inc["text"],
            ticket={"key": inc["key"], "url": f"https://qbizinc.atlassian.net/browse/{inc['key']}"},
            status=inc["status"],
        )
