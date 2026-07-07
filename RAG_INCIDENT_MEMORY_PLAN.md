# RAG Incident Memory — Wiring the qbiz-agents RAG into novamart-pipelines

## Why this plan exists

Give the in-Airflow incident agent a **persistent memory of past incidents and the tickets they
opened** — so it detects recurrence at diagnosis time and accumulates an institutional record —
**and**, in doing so, use novamart-pipelines as the first real-world test of
[`qbiz-agents`](https://github.com/Qbizinc/qbiz-agents) as a *component template*: does the reusable
RAG engine actually drop into a client project?

> **Architecture correction.** An earlier plan
> (`qbiz-agents/mcp/mcp_rag/INCIDENT_MEMORY_PLAN.md`) assumed the incident response was the
> standalone `incident_demo.py` driver connecting MCP servers over stdio. **That is not how
> novamart works.** Here the incident response runs **inside Airflow** as agentic DAGs. The
> *concepts* from that plan still hold (incident-record schema, ticket-key-as-ledger-key, the
> close-tracking gap); the *wiring* is different. This plan supersedes it for novamart.

---

## What we're integrating with (grounded map)

**Trigger chain:** a pipeline DAG fails → `on_failure_callback = trigger_incident_dag`
([`include/novamart_utils.py`](include/novamart_utils.py)) → fires `agentic_snowflake_incident` via
the Airflow REST API with `conf={failed_dag_id, failed_dag_run_id}`.

**`agentic_snowflake_incident`** ([`dags/agentic_snowflake_incident.py`](dags/agentic_snowflake_incident.py)) — the polished, Jira-wired path:

| Task | Kind | What it does |
|---|---|---|
| `gather_context` | deterministic | Pulls the failed run's task logs + DAG source via Airflow REST |
| `investigate` | `@task.agent` (pydantic-ai, `anthropic:claude-sonnet-4-6`, `SQLToolset` on Snowflake) | Diagnoses; **returns a structured `[SUMMARY]/[DIAGNOSIS]/[ROOT CAUSE]/[IMPACT]/[RECOMMENDED FIX]` block** |
| `create_jira_ticket` | deterministic | One Jira Bug via REST (`project AD`), returns `{key, url}` |
| `post_to_slack` | deterministic | Posts diagnosis + Jira link to the incident channel |

The agent is deliberately **single-tool** (SQL only); ticket creation and Slack posting are
deterministic tasks *after* it, so they happen exactly once. This is the key design constraint to
respect — see "Where RAG plugs in."

**Secondary path:** `agentic_incident_dag` ([`dags/agentic_incident_dag.py`](dags/agentic_incident_dag.py)) — a hand-rolled Anthropic tool loop that scans multiple pipelines and posts to Slack, **no Jira**. Treat `agentic_snowflake_incident` as canonical for this work; confirm with the DAG's owner.

**Scenario DAGs:** the `novamart_*` DAGs are deliberately-broken pipelines, each demonstrating a
failure mode (schema drift, null explosion, type change, duplicate load, historical reload, wrong
date, IAM, S3, API pagination/schema). They're what generate the incidents RAG will remember.

---

## Key finding: the RAG engine imports as a plain library

`rag_mcp.index.RagIndex` / `get_index()` depend only on the engine modules (`config`, `embeddings`,
`ingest`, `ledger`, `store`) + `numpy` + `fastembed`. **MCP is confined to `_app.py` / `server.py`
/ `tools/rag.py`.** So an Airflow task can:

```python
from rag_mcp.index import get_index
get_index().ingest(text=record, title=ticket_key, tags=["incident", dag_id, "open"])
hits = get_index().search(f"{dag_id} {symptom}", tags=["incident", dag_id])
```

No MCP server, no stdio subprocess inside the worker. This is what makes the library-embed path
clean, and it's the first thing the template test validates.

---

## The main decision: two integration flavors

**Flavor A — Library embed (recommended for MVP).** Vendor `rag_mcp` as a Python dependency;
deterministic tasks import `get_index()` and call `search` / `ingest` directly.
- ➕ Simplest, deterministic, robust; ingest is a plain task (not an agent decision — matches the
  DAG's "exactly once" design). No stdio/shared-subprocess concerns.
- ➖ Exercises reuse of the *engine*, less of the MCP/CLI packaging.

**Flavor B — MCP toolset to the agent.** Register the RAG MCP (`qba agent mcp add rag`) and give the
`@task.agent` a RAG *search* tool so it decides when to recall.
- ➕ Exercises the *full* template path (mcp.yaml + `qba` CLI + skill) — the stronger "our template
  works end-to-end" story; aligns with novamart's `CLAUDE.md` already referencing `qba`.
- ➖ Adds a stdio subprocess in the worker and a shared-state/persistence concern; makes recall a
  non-deterministic agent choice.

**Recommendation:** **A for MVP** (deterministic recall + record), **add B in Phase 3** if we want
the agent to pull memory on demand. Hybrid is natural — deterministic record always; agent-driven
search later.

---

## Where RAG plugs into `agentic_snowflake_incident`

Two new **deterministic** tasks, no change to the agent's toolset:

```
gather_context → recall_similar_incidents → investigate → create_jira_ticket → record_incident → post_to_slack
```

1. **`recall_similar_incidents`** (new, after `gather_context`): `search("<failed_dag> <symptom
   from logs>", tags=["incident", failed_dag_id])`. Pass the hits into `investigate` by extending
   the prompt string it returns with a *"Prior incidents on this pipeline"* section. Because it's a
   deterministic pre-fetch, `investigate` stays single-tool and the recurrence context is guaranteed
   present — no reliance on the agent choosing to look.
2. **`record_incident`** (new, after `create_jira_ticket`): `ingest(text=<record>, title=<jira
   key>, tags=["incident", failed_dag_id, "open"])`.

**Happy alignment:** `investigate` already emits a clean structured block
(`[SUMMARY]/[DIAGNOSIS]/[ROOT CAUSE]/[IMPACT]/[RECOMMENDED FIX]`) — that *is* the incident record
body, near-zero transformation to store.

---

## The incident record (reused from the qbiz-agents plan)

- **`title` = the Jira ticket key** (e.g. `AD-123`). Text ingests are keyed `text:{title}` and
  re-ingest replaces in place → the ticket key makes "update on close" a plain re-ingest.
- **Body:** the agent's structured diagnosis block + `failed_dag_id`, `run_id`, detected-at
  timestamp, ticket URL.
- **Tags:** `["incident", <dag_id>, <status>]` — lets recall scope to *this pipeline*, and lets a
  status flip (`open` → `closed`) happen on the close-sync re-ingest.
- Point this at its **own `RAG_DATA_DIR`** (e.g. `.rag-incidents`) so it never collides with any
  document-RAG corpus.

---

## Persistence — the real infra decision

The engine store is numpy files (`vectors.npy`, `chunks.jsonl`, `ledger.json`) under `RAG_DATA_DIR`.
It must persist **across DAG runs** and be shared between the `recall` (read) and `record` (write)
tasks — potentially across worker containers.

- **MVP:** a mounted volume, e.g. `/usr/local/airflow/include/.rag-incidents`, declared in the Astro
  compose/override. Fine for a single-worker local demo.
- **Phase 2 (the "real use" story):** swap the store to **pgvector** — **novamart already runs
  Postgres** in the Astro stack, so incident memory lives in a genuinely shared, persistent,
  concurrently-writable store. This exercises the `store.py` scale-swap the RAG plan documents, and
  is the honest answer for a multi-worker fleet. Snowflake is a second candidate if we'd rather keep
  it in the warehouse. **Design now written up** in qbiz-agents at
  `mcp/mcp_rag/PGVECTOR_STORE_PLAN.md` (env-selected `get_store` factory + `[pgvector]` extra). ⚠️
  **Scope note it surfaced:** the pgvector swap is *not* just the vector store — the **ledger must
  also move to Postgres**, or a `recall` task on one worker won't see incidents a `record` task wrote
  on another (tag-scoped search + `list_sources` resolve through the ledger). Treat Phase 2 as
  "store **and** ledger to pgvector," written in one transaction.

Seed 1–2 past incidents so recurrence is demonstrable on the first run.

---

## Embeddings

Keep the **fastembed** default (ONNX, local, **no API key**). It sidesteps the LLM-provider question
entirely — the incident agent's Anthropic key stays only for the LLM, not for embeddings. Cost:
`onnxruntime` + a one-time model download; **bake the model into the Docker image** at build time so
the first task run doesn't pay the download latency.

---

## Packaging — how to vendor `rag_mcp` (this is the template test)

- **Option 1 (recommended): install from the git subdirectory** in `requirements.txt`:
  ```
  qbiz-rag-mcp @ git+https://github.com/Qbizinc/qbiz-agents.git#subdirectory=mcp/mcp_rag
  ```
  Keeps qbiz-agents the single source of truth — the truest reuse test. **As of 2026-07-06 the base
  package is engine-only**: `mcp` + `python-dotenv` now live behind a `[server]` extra, so this line
  installs **only** `numpy` + `fastembed` — no MCP runtime in the Airflow image. (Do *not* add
  `[server]` — that's only for running the MCP server, which the DAG doesn't.)
- **Option 2: vendor-copy the package into `include/`.** Faster, but forks the code and weakens the
  template test. Avoid unless the git-install proves painful in the image build.

Add Variables `RAG_DATA_DIR` and `RAG_EMBED_BACKEND=fastembed` to `airflow_settings.yaml`.

---

## Close tracking — better fit here than in the standalone plan

Same gap as before: there's no close *write* path — a human closes the Jira ticket, and the incident
tools only *read* status. But novamart has something the one-shot demo driver didn't: **a
scheduler.** So add a small scheduled DAG:

- **`novamart_incident_memory_sync`** (`@hourly` or `@daily`): query Jira for the incident tickets
  recorded in memory, and for any now-resolved ticket, re-`ingest` its record with resolution +
  time-to-close and tags flipped to `closed`. Airflow's scheduler is the natural home for this
  polling — a cleaner answer than the manual re-run the qbiz plan fell back to.

---

## Phased build

- **Phase 1 — MVP (library embed).** Vendor `rag_mcp`; add `recall_similar_incidents` +
  `record_incident` to `agentic_snowflake_incident`; numpy store on a mounted volume; fastembed baked
  into the image; seed data. No agent-tool changes.
- **Phase 2 — Persistence + lifecycle.** pgvector store swap (reuse the Astro Postgres);
  `novamart_incident_memory_sync` close-tracking DAG.
- **Phase 3 — Agentic recall + metrics.** Give the in-Airflow `@task.agent` a RAG tool (Flavor B —
  a pydantic-ai toolset, distinct from the CLI skill) so `investigate` can pull memory on demand;
  optional structured metrics table for recurrence-rate / MTTR / worst-offender DAGs (the reporting
  RAG alone can't do). *Note:* the interactive/on-call consumption path is already covered by the
  `rag-incident-memory` skill (a human running Claude Code / Gemini CLI with the `rag` MCP) — that
  ships now and needs no DAG change.

---

## What this proves for qbiz-agents (template validation) + feedback to capture

- Confirms the RAG engine is a genuine drop-in: **import-clean and key-free.**
- Exercises the **git-subdirectory install path** from `mcp.yaml` in a real image build.
- Feedback fed back to qbiz-agents — **already actioned 2026-07-06** (this integration was the forcing
  function):
  1. ✅ **Engine-only base install** — `mcp` moved to a `[server]` extra; the `requirements.txt` line
     above now installs engine-only. *Done, unblocks Phase 1.*
  2. 📋 **pgvector `store.py`** — design written (`mcp/mcp_rag/PGVECTOR_STORE_PLAN.md`), not built.
     *Feeds Phase 2.*
  3. ✅ **`rag-incident-memory` skill** — a `rag-research` specialization for incident recall.
     *Done — but note its consumer:* it's a Claude Code / Gemini-CLI **skill** for an **interactive
     on-call** agent (a human running the CLI with the `rag` MCP connected), **not** something the
     in-Airflow `@task.agent` imports. It's a *complementary* delivery — the same shared incident
     memory queried by a person during an incident — layered on top of the DAG's automated
     record/recall, not part of the DAG. Relevant to Phase 3 (Flavor B) and to on-call UX generally.

---

## Open questions (for the coworker who owns the DAG code)

1. **Flavor A vs B for MVP** — recommend A (deterministic recall + record).
2. **Persistence now** — mounted-volume numpy for MVP, or go straight to pgvector since Postgres is
   already running? (Volume is simpler; pgvector makes the scalability story real sooner.)
3. **Canonical DAG** — target `agentic_snowflake_incident` (has Jira); confirm `agentic_incident_dag`
   isn't the demo path.
4. **Ownership/coordination** — this touches `agentic_snowflake_incident.py`, `requirements.txt`,
   `airflow_settings.yaml` Variables, the Docker image (baked embeddings + volume), and adds a new
   sync DAG. Split with the DAG owner accordingly.

---

## Out of scope

- Changing the `investigate` model/provider.
- Full metrics/reporting UI (Phase 3 sketch only).
- Multi-tenant isolation beyond a dedicated `RAG_DATA_DIR` / pgvector schema.
