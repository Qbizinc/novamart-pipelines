"""Incident memory — thin helpers over a Snowflake table.

Used by agentic_snowflake_incident_memory.py (see its doc_md for what this is for and how to
demo it). Recall is recency-scoped per dag_id (the most recent incidents on *this* pipeline), not
semantic similarity search. Every demo pipeline in this repo has one deterministic failure mode per
dag_id, so "the last incident on this pipeline" already finds the same prior occurrence a similarity
search would — without a vector index, an embedding model, or any dependency beyond the
snowflake_default connection this repo already uses everywhere.

Every call is **best-effort**: incident memory must never break incident response, so recall/record
failures are caught and logged, not raised.
"""
from __future__ import annotations

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

TABLE = "INCIDENT_MEMORY"


def _ensure_table(cur) -> None:
    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {TABLE} (
            ticket_key   VARCHAR(32)   NOT NULL,
            dag_id       VARCHAR(128)  NOT NULL,
            run_id       VARCHAR(255),
            status       VARCHAR(16)   NOT NULL,
            diagnosis    VARCHAR,
            ticket_url   VARCHAR,
            detected_at  TIMESTAMP_NTZ NOT NULL DEFAULT CURRENT_TIMESTAMP()
        )
    """)


def recall_similar_incidents(dag_id: str, task_logs: dict[str, str], k: int = 3) -> str:
    """Look up the most recent prior incidents recorded for this pipeline.

    Returns a prompt-ready section (empty string if there are no matches or on any error, so the
    investigation proceeds normally). `task_logs` is unused here — kept in the signature so this
    function is a drop-in replacement regardless of recall strategy.
    """
    conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
    try:
        cur = conn.cursor()
        _ensure_table(cur)
        cur.execute(
            f"SELECT ticket_key, ticket_url, status, diagnosis, detected_at "
            f"FROM {TABLE} WHERE dag_id = %s ORDER BY detected_at DESC LIMIT %s",
            (dag_id, k),
        )
        rows = cur.fetchall()
    except Exception as exc:  # best-effort — never block the investigation
        print(f"[incident_memory] recall failed (continuing without prior context): {exc}")
        return ""
    finally:
        conn.close()

    if not rows:
        print(f"[incident_memory] no prior incidents on record for {dag_id}")
        return ""

    print(f"[incident_memory] {len(rows)} prior incident(s) for {dag_id}: "
          f"{[r[0] for r in rows]}")
    lines = [
        "Prior incidents on this pipeline (from incident memory — treat each as a LEAD to confirm "
        "against the current evidence, not as established fact):",
    ]
    for ticket_key, ticket_url, status, diagnosis, detected_at in rows:
        lines.append(
            f"\n--- {ticket_key} ({status}, detected {detected_at}) ---\n"
            f"{(diagnosis or '').strip()[:800]}"
        )
    return "\n".join(lines)


def record_incident(dag_id: str, run_id: str, diagnosis: str, ticket: dict,
                    status: str = "open") -> bool:
    """Record (or update) an incident in memory, keyed by its Jira ticket. Best-effort.

    Re-recording the same ticket key updates the row in place (via MERGE), so a later close-sync
    can flip status to ``closed`` rather than creating a duplicate.
    """
    ticket_key = ticket.get("key") or f"{dag_id}:{run_id}"
    ticket_url = ticket.get("url", "")
    conn = SnowflakeHook(snowflake_conn_id="snowflake_default").get_conn()
    try:
        cur = conn.cursor()
        _ensure_table(cur)
        cur.execute(
            f"""
            MERGE INTO {TABLE} t
            USING (SELECT %s AS ticket_key, %s AS dag_id, %s AS run_id, %s AS status,
                          %s AS diagnosis, %s AS ticket_url,
                          CURRENT_TIMESTAMP() AS detected_at) s
            ON t.ticket_key = s.ticket_key
            WHEN MATCHED THEN UPDATE SET
                status = s.status, diagnosis = s.diagnosis, detected_at = s.detected_at
            WHEN NOT MATCHED THEN INSERT (ticket_key, dag_id, run_id, status, diagnosis, ticket_url, detected_at)
            VALUES (s.ticket_key, s.dag_id, s.run_id, s.status, s.diagnosis, s.ticket_url, s.detected_at)
            """,
            (ticket_key, dag_id, run_id, status, diagnosis, ticket_url),
        )
        print(f"[incident_memory] recorded {ticket_key} ({status}) in incident memory")
        return True
    except Exception as exc:  # best-effort — the ticket + Slack post already happened
        print(f"[incident_memory] record failed for {ticket_key} (continuing): {exc}")
        return False
    finally:
        conn.close()


# --- Seeding (demo convenience) ---------------------------------------------------------------
# A couple of realistic past incidents so the recurrence demo has something to find on the FIRST
# real investigation. Run the `novamart_incident_memory_seed` DAG once. Idempotent (re-record by
# ticket_key replaces).

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
    """Record the example incidents. Safe to re-run."""
    for inc in SEED_INCIDENTS:
        record_incident(
            dag_id=inc["dag_id"],
            run_id="seed",
            diagnosis=inc["text"],
            ticket={"key": inc["key"], "url": f"https://qbizinc.atlassian.net/browse/{inc['key']}"},
            status=inc["status"],
        )
