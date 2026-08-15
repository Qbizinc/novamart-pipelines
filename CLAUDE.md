# Claude instructions for novamart-pipelines

This is the Novamart data pipeline project, built on Apache Airflow using the Astronomer platform.

## Qbiz Skills

This project uses skills from the [qbiz-agents](https://github.com/qbiz/qbiz-agents) repository.
Installed skills live in `.claude/skills/` and are automatically available in this session.

To find and install more skills:
```bash
qba agent skills list
qba agent skills search <query>
qba agent skills add <skill-name>
```

To install the `qba` CLI if you don't have it:
```bash
pipx install qba-agents
```

## Incident memory (RAG)

`agentic_snowflake_incident` has a persistent memory of past incidents: it **recalls** prior
incidents on a pipeline before diagnosing (recurrence detection) and **records** each one, keyed by
its Jira ticket, after the ticket is opened. What it's for and how to demo it is in
[`README.md`](README.md#incident-memory-rag); the full design is in
[`RAG_INCIDENT_MEMORY_PLAN.md`](RAG_INCIDENT_MEMORY_PLAN.md).

When working on this code, know:

- The logic lives in [`include/incident_memory.py`](include/incident_memory.py). It uses the
  qbiz-agents RAG engine **as a library** — `from rag_mcp.index import get_index` — **not** the MCP
  server. The import is deferred (inside functions) so DAG parsing never needs `rag_mcp`.
- **Every memory call is best-effort.** Recall/record must never raise into the incident flow — a
  memory failure should degrade to "no prior context" / "not recorded", never break ticket creation
  or the Slack post. Keep new code inside that try/except contract.
- **⚠️ RAG `search(tags=…)` is ANY-of (OR), not AND.** To scope recall to one pipeline, filter by the
  **`dag_id` tag alone** — `tags=[dag_id]`. Do **not** add the shared `"incident"` tag to the recall
  filter: it would match *every* incident. (Records still carry `["incident", dag_id, status]` so a
  cross-pipeline "all incidents" query is possible; it's the *recall scoping* that must use `dag_id`.)
- The index persists under `RAG_DATA_DIR` (`include/.rag-incidents`, git-ignored). A different
  `RAG_DATA_DIR` = a different, empty memory.
- **⚠️ One writer at a time.** The index is three plain files (`ledger.json`, `chunks.jsonl`,
  `vectors.npy`) that each process loads into memory and rewrites **whole**. There is no locking
  and no re-read on change, so two writers silently clobber each other: a `rag` MCP server started
  before a demo holds a stale snapshot, and one write from it drops every incident the DAGs
  recorded since it started. Observed in practice — a full run's incident records vanished this
  way, leaving only the seeds.
  - To clear/re-seed the memory, use `incident_memory.reset_to_seed()` **inside the Airflow
    container** (the demo driver's `--reset-memory` flag does exactly this), never the MCP tools
    while Airflow is running.
  - The `rag` MCP server is fine for *reading* (the `rag-incident-memory` skill's on-call use), but
    treat any MCP **write** as unsafe while the DAGs are live.
- `SEED_INCIDENTS` in `incident_memory.py` is the only thing `reset_to_seed()` restores. A record
  ingested by hand is **not** in there and will not survive a reset — if a baseline record matters,
  it belongs in that list, keyed by a real Jira ticket (AD-40, AD-45).

For **interactive / on-call** use (querying the incident memory yourself, outside the DAG), there's a
skill for that: `qba agent skills add rag-incident-memory` (drives the `rag` MCP server directly).
