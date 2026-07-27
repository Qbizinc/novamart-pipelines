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


def recall_similar_incidents(dag_id: str, task_logs: dict[str, str], k: int = 3) -> dict:
    """Search incident memory for prior occurrences on this pipeline.

    Returns {"text": prompt-ready section, "tickets": [ticket keys]} — both empty if there are no
    matches or on any error, so the investigation proceeds normally.
    """
    try:
        symptom = _symptom_from_logs(task_logs)
        # RAG tag filtering is ANY-of, so scope by the dag_id tag ALONE — in a dedicated incident
        # index that uniquely selects this pipeline's incidents. Including INCIDENT_TAG here would
        # broaden the match to *every* incident (via the shared 'incident' tag), which is wrong.
        hits = _index().search(f"{dag_id} {symptom}".strip(), k=k, tags=[dag_id])
    except Exception as exc:  # best-effort — never block the investigation
        print(f"[incident_memory] recall failed (continuing without prior context): {exc}")
        return {"text": "", "tickets": []}

    if not hits:
        print(f"[incident_memory] no prior incidents on record for {dag_id}")
        return {"text": "", "tickets": []}

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
    tickets = [h["title"] for h in hits if h.get("title")]
    return {"text": "\n".join(lines), "tickets": tickets}


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
    {
        "dag_id": "demo_one_api_escalate",
        "key": "AD-1003",
        "status": "closed",
        "text": (
            "[SUMMARY] demo_one_api_escalate failed — sales_api request timed out\n"
            "[DIAGNOSIS] fetch_from_api raised requests.exceptions.Timeout calling sales_api's "
            "/api/v1/sales endpoint.\n"
            "[ROOT CAUSE] sales_api was unresponsive/overloaded at the time of the request; not a "
            "bug in this pipeline's own code.\n"
            "[IMPACT] No transactions fetched for this run; downstream load did not occur.\n"
            "[RECOMMENDED FIX] Confirm sales_api's health/capacity with its owning team; retry the "
            "run once it recovers. No code change needed in this pipeline."
        ),
    },
    {
        "dag_id": "demo_3_gold_sales_by_region",
        "key": "AD-1004",
        "status": "closed",
        "text": (
            "[SUMMARY] demo_3_gold_sales_by_region failed — SUM() error on a column expected to be numeric\n"
            "[DIAGNOSIS] The gold aggregation task failed running SUM() over a column sourced from "
            "SILVER_SALES; the column held non-numeric text instead of the numeric type it was "
            "defined with.\n"
            "[ROOT CAUSE] BRONZE_SALES's column was VARCHAR holding non-numeric text, not the numeric "
            "type demo_1_bronze_sales's code defines — a discrepancy between declared and observed "
            "schema. demo_2_silver_sales rebuilds SILVER_SALES via `SELECT *` with no explicit "
            "column list, so it carried that discrepancy straight through without erroring itself — "
            "the break only surfaced downstream, at the gold aggregation.\n"
            "[IMPACT] GOLD_SALES_BY_REGION was not refreshed for the affected run.\n"
            "[RECOMMENDED FIX] Compare BRONZE_SALES's current column types/sample values against "
            "what demo_1_bronze_sales defines, focusing on whatever column the failing aggregation "
            "uses. Rebuild BRONZE_SALES with that column restored to its correct numeric type."
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
