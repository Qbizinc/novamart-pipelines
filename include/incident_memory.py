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


def _ledger_tags(title: str) -> list[str]:
    """Best-effort direct read of a record's tags from the ledger.

    search() hits don't carry tags (only source/title/ordinal/score/text), so this reads the
    ledger file directly rather than adding an unverified method call onto the raw Index object —
    ingest()/search() are the only calls into that object this module otherwise relies on.
    """
    try:
        ledger_path = os.path.join(os.environ["RAG_DATA_DIR"], "ledger.json")
        with open(ledger_path, encoding="utf-8") as f:
            ledger = json.load(f)
        return list(ledger.get(f"text:{title}", {}).get("tags", []))
    except Exception:
        return []


def _ledger_status(title: str) -> str | None:
    """'open'/'closed' from the tag stored on the record at ingest.

    The status drives behaviour — an open incident dedupes and teaches nothing, a closed one
    teaches and never dedupes — so this tag has to agree with the ticket tracker.

    It is a snapshot, not a live read: nothing updates it after ingest, and record_incident()
    always writes "open". So an incident the agent resolves later stays "open" here forever,
    which means it will keep deduping to a fixed ticket and never graduate into knowledge.
    That is fine while the knowledge base is the two curated seeds in SEED_INCIDENTS, whose
    statuses are maintained by hand; it does not scale past them.

    The fix is close-tracking — a sync that reconciles these tags with the tracker (see
    RAG_INCIDENT_MEMORY_PLAN.md), or resolving status at read time instead of storing it. Either
    way, the tracker owns state and this copy of it will drift until something reconciles them.
    """
    tags = _ledger_tags(title)
    return "open" if "open" in tags else ("closed" if "closed" in tags else None)


_STATUS_TAGS = {"open", "closed"}


def _ledger_dag_id(title: str) -> str | None:
    """Which pipeline a stored record belongs to. Records carry [INCIDENT_TAG, dag_id, status],
    so the dag_id is whatever isn't the shared incident tag or a status tag."""
    for tag in _ledger_tags(title):
        if tag != INCIDENT_TAG and tag not in _STATUS_TAGS:
            return tag
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
    empty = {"text": "", "tickets": [], "open_duplicate": None,
             "closed_recurrence": None, "related": []}
    try:
        symptom = _symptom_from_logs(task_logs)
        query = f"{dag_id} {symptom}".strip()

        # TIER 1 — this pipeline's own history. RAG tag filtering is ANY-of, so scope by the
        # dag_id tag ALONE; adding INCIDENT_TAG would broaden it to *every* incident.
        # Only this tier can suppress a ticket: "same pipeline, same symptom" is a recurrence.
        hits = _index().search(query, k=k, tags=[dag_id])

        # TIER 2 — everything else. A similar failure on a DIFFERENT pipeline is a lead worth
        # reading (often the same root cause and the same fix), but it is never the same incident,
        # so these are hints only and can never dedupe a ticket. Merging incidents across
        # pipelines on semantic similarity alone is how a real outage gets filed as a duplicate
        # and silently dropped.
        cross_hits = _index().search(query, k=k + 3, tags=[INCIDENT_TAG])
    except Exception as exc:  # best-effort — never block the investigation
        print(f"[incident_memory] recall failed (continuing without prior context): {exc}")
        return empty

    related = _cross_pipeline_hints(cross_hits, exclude_dag_id=dag_id)

    if not hits:
        print(f"[incident_memory] no prior incidents on record for {dag_id}")
        if not related:
            return empty
        print(f"[incident_memory] {len(related)} similar incident(s) on OTHER pipelines: "
              f"{[r['key'] for r in related]}")
        return {"text": _render_related_section(related), "tickets": [],
                "open_duplicate": None, "closed_recurrence": None, "related": related}

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

    # Only RESOLVED incidents become knowledge. An open ticket on this pipeline is still being
    # worked — it holds a symptom, not an answer — so it feeds duplicate detection below and
    # nothing else. Showing one to the agent would invite it to present someone's unfinished
    # investigation as a confirmed fix.
    resolved = [h for h in unique_hits if _ledger_status(h.get("title")) == "closed"]
    skipped_open = len(unique_hits) - len(resolved)
    if skipped_open:
        print(f"[incident_memory] ignoring {skipped_open} unresolved prior incident(s) as a "
              f"knowledge source (still open — no resolution to reuse)")

    lines = []
    if resolved:
        lines.append(
            "Prior RESOLVED incidents on this pipeline (from incident memory — each was fixed and "
            "closed. Treat each as a LEAD to confirm against the current evidence, not as "
            "established fact):"
        )
        for h in resolved:
            lines.append(
                f"\n--- {h.get('title', '?')} (similarity {h.get('score', 0.0):.2f}) ---\n"
                f"{(h.get('text') or '').strip()[:800]}"
            )
    tickets = [h["title"] for h in resolved if h.get("title")]

    open_duplicate = None
    closed_recurrence = None
    top = unique_hits[0]
    top_title = top.get("title")
    top_score = top.get("score", 0.0)
    top_status = _ledger_status(top_title) if top_title else None

    if top_title and top_score >= DUPLICATE_SCORE_THRESHOLD:
        if top_status == "open":
            open_duplicate = {
                "key": top_title,
                "url": f"https://qbizinc.atlassian.net/browse/{top_title}",
                "score": top_score,
            }
            print(f"[incident_memory] treating this as a recurrence of open ticket {top_title} "
                  f"(score={top_score:.2f} >= {DUPLICATE_SCORE_THRESHOLD})")
        elif top_status == "closed":
            # A closed incident recurring is a NEW incident, not a duplicate: the original has a
            # finished timeline someone signed off on, and reopening it would blur two separate
            # occurrences into one record. Open a fresh ticket that cites the original instead.
            closed_recurrence = {
                "key": top_title,
                "url": f"https://qbizinc.atlassian.net/browse/{top_title}",
                "score": top_score,
            }
            print(f"[incident_memory] recurrence of CLOSED ticket {top_title} "
                  f"(score={top_score:.2f}) — will open a new ticket citing it")

    if related:
        print(f"[incident_memory] {len(related)} resolved incident(s) on OTHER pipelines: "
              f"{[r['key'] for r in related]}")
        lines.append(_render_related_section(related))

    # An open duplicate means this is already being worked. Hand back no knowledge at all in that
    # case — the response is "comment on the existing ticket", not "diagnose it again from a
    # half-finished record".
    text = "" if open_duplicate else "\n".join(lines)

    return {"text": text, "tickets": tickets, "open_duplicate": open_duplicate,
            "closed_recurrence": closed_recurrence,
            "related": [] if open_duplicate else related}


# How close a cross-pipeline incident has to be before it is worth showing the agent at all.
#
# Calibrated against observed pairs rather than guessed. Embeddings of incident text share a lot
# of pipeline vocabulary, so scores run high across the board and a low floor admits almost
# anything: at 0.40, a row-count grain mismatch pulled in a 503 outage at 0.70 — unrelated in
# every respect, and the agent then had to spend reasoning disproving it. Measured on this index:
#
#   0.83  same failure, same pipeline        (503 -> AD-45)          genuinely a recurrence
#   0.78  same failure, different pipeline   (503 -> AD-45)          genuinely transferable
#   0.70  unrelated failure                  (grain check -> AD-45)  noise
#
# 0.75 keeps the first two and drops the third. A weak match is worse than no match: it anchors
# the agent on a wrong hypothesis, so this floor is deliberately conservative. Revisit with more
# data — three points is a calibration, not a study.
RELATED_SCORE_THRESHOLD = 0.75


def _cross_pipeline_hints(hits: list[dict], *, exclude_dag_id: str) -> list[dict]:
    """Distinct RESOLVED incidents from OTHER pipelines, best first.

    Closed only, deliberately. An open ticket has no resolution written into it yet — there is
    nothing in it to reuse, and offering one as a "similar incident" invites the agent to infer a
    fix nobody has actually confirmed. Open tickets earn their keep as duplicate detection
    (same pipeline, see open_duplicate); knowledge transfer is the job of closed ones.

    The source pipeline does not have to still exist: a resolved incident on a since-renamed or
    deleted DAG is exactly as instructive as one on a live pipeline.
    """
    seen: set[str] = set()
    out: list[dict] = []
    for h in hits or []:
        title = h.get("title")
        if not title or title in seen:
            continue
        if h.get("score", 0.0) < RELATED_SCORE_THRESHOLD:
            continue
        if _ledger_status(title) != "closed":
            continue  # unresolved — nothing to learn from it
        hit_dag_id = _ledger_dag_id(title)
        if hit_dag_id == exclude_dag_id:
            continue  # same pipeline — tier 1 already covers it
        seen.add(title)
        out.append({
            "key": title,
            "dag_id": hit_dag_id,
            "score": h.get("score", 0.0),
            "text": (h.get("text") or "").strip()[:800],
        })
    return out


def _render_related_section(related: list[dict]) -> str:
    """Prompt section for tier-2 hits, worded so the agent treats them as transferable *approaches*
    rather than as this pipeline's own history."""
    lines = [
        "\nSimilar incidents on OTHER pipelines (different systems, so NOT a recurrence of this "
        "one — but the root cause and fix may transfer. If one applies, say so explicitly and name "
        "the ticket; if none do, say that too):",
    ]
    for r in related:
        lines.append(
            f"\n--- {r['key']} on {r.get('dag_id') or 'unknown pipeline'} "
            f"(similarity {r['score']:.2f}) ---\n{r['text']}"
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


# --- Seeding ------------------------------------------------------------------------------------
# The one real baseline incident so recurrence detection has something to find on the FIRST real
# investigation. AD-40 is a real Jira ticket (project AD), not a placeholder — created once via
# the same REST call create_ticket_low_priority/urgent_slack_post use. Idempotent (re-ingest by
# title replaces).

SEED_INCIDENTS: list[dict] = [
    {
        "dag_id": "novamart_gold_sales_by_region",
        "key": "AD-40",
        # Mirrors AD-40's real Jira status (To Do). The status here drives behaviour — an open
        # ticket dedupes and teaches nothing, a closed one teaches and never dedupes — so a tag
        # that disagrees with Jira makes the agent act on a false premise. Nothing syncs these
        # automatically yet; if AD-40 is resolved in Jira, flip this to "closed" too.
        "status": "open",
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
    {
        # Pairs with the sales_api leg of the demo sequence (the mock is toggled unhealthy so the
        # ingest fails on 503s), giving that pipeline a prior incident to recall on its first
        # failure. AD-45 is a real Jira ticket, like AD-40 — it previously lived in the index as a
        # hand-ingested record keyed "AD-1005", which nothing in code recreated, so reset_to_seed()
        # silently dropped it.
        "dag_id": "novamart_sales_api_ingest",
        "key": "AD-45",
        "status": "closed",
        "text": (
            "[SUMMARY] novamart_sales_api_ingest failed — sales_api returned connection errors on "
            "every request\n"
            "[DIAGNOSIS] fetch_transactions raised a connection/HTTP error calling sales_api's "
            "/api/v1/sales endpoint; every request during this run failed the same way.\n"
            "[ROOT CAUSE] sales_api's upstream load balancer was mid-rollout (a deploy in progress) "
            "and returned 503s for several minutes — not a bug in this pipeline's own code.\n"
            "[IMPACT] No transactions fetched for this run; downstream load did not occur.\n"
            "[FIX] Wrapped the sales_api request in a short retry with backoff (2-3 attempts, short "
            "delay between). That resolved it — the pipeline now tolerates a brief upstream blip "
            "instead of failing the whole run on the first error."
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
    so the index starts from the same known state instead of accumulating stale records.

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
