# novamart-pipelines

Airflow DAG pipelines for Novamart, managed with the [Astro CLI](https://www.astronomer.io/docs/astro/cli/overview) (Astronomer).

## Prerequisites

| Tool | Install |
|------|---------|
| [Docker Desktop](https://www.docker.com/products/docker-desktop/) | Required to run Airflow containers |
| [Astro CLI](https://www.astronomer.io/docs/astro/cli/install-cli) | Manages local Airflow environment |
| Python 3.8–3.12 + venv | For local development tooling |

## Quick Start

```bash
# 1. Clone the repo
git clone <repo-url>
cd novamart-pipelines

# 2. Create and activate the Python virtual environment
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate

# 3. Install Python dev dependencies (linting, testing, etc.)
pip install -r requirements-dev.txt   # create this file as needed

# 4. Start Airflow locally
astro dev start
```

Airflow UI will be available at **http://localhost:8080** (user: `admin`, password: `admin`).  
Postgres is available at `localhost:5432` (user/pass: `postgres`).

## Project Structure

```
novamart-pipelines/
├── dags/            # Airflow DAGs
├── include/         # Shared helpers, SQL, configs
├── plugins/         # Custom Airflow plugins
├── tests/           # DAG unit tests
├── Dockerfile       # Astro Runtime base image
├── requirements.txt # Python packages installed inside Airflow containers
├── packages.txt     # OS-level packages installed inside Airflow containers
└── airflow_settings.yaml  # Local connections/variables (git-ignored)
```

## Local Setup: Connections & Secrets

`airflow_settings.yaml` and `.env` are git-ignored (they hold credentials), so
pulling the repo does **not** give you a working environment on its own.
Every teammate needs to (re)create the following locally before DAGs will run:

| What | Where it goes | Why |
|------|----------------|-----|
| `snowflake_default` connection (key-pair auth) | `airflow_settings.yaml` | Snowflake access for all sales/reporting DAGs |
| `keys/snowflake_private_key.p8` | `keys/` directory | The private key file `snowflake_default` points to — not stored in the YAML itself |
| `jira_api` connection (Basic auth) | `airflow_settings.yaml` | Incident DAG opens Jira tickets |
| `slack_api` connection (bot token) | `airflow_settings.yaml` | Incident DAG posts to Slack |
| `airflow_api` connection | `airflow_settings.yaml` | Incident DAG + failure callbacks call the Airflow REST API |
| `pydanticai_default` connection (`conn_type: pydanticai`, no credentials needed) | `airflow_settings.yaml` | Required by `@task.agent`'s `llm_conn_id` in `agentic_snowflake_incident.py` — must exist even though the model itself resolves from `model_id` + `ANTHROPIC_API_KEY` |
| `aws_default` connection | `airflow_settings.yaml` | S3 / Secrets Manager / IAM incident-demo DAGs (`novamart_iam_*`, `novamart_s3_*`) — needs real AWS credentials (profile, env vars, or IAM role) added locally, since the connection itself carries none |
| `mock_api` connection | `airflow_settings.yaml` | `novamart_api_staging_prod` — should point at the staging mock host |
| `ANTHROPIC_API_KEY` | `.env` | Read directly by the Anthropic SDK / pydantic-ai for the incident agent |
| `SLACK_BOT_TOKEN` (if not inlined in the connection) | `.env` as `AIRFLOW_VAR_SLACK_BOT_TOKEN` | Slack posting |
| Mock API services running (`mock-apis-repo`) | separate repo, ports 5001–5004 | Sales/customer/marketing/transactions mock APIs the pipeline DAGs call |
| `MOCK_TRANSACTIONS_API_URL`, `NOVAMART_S3_BUCKET`, `NOVAMART_SNOWFLAKE_SECRET_ID`, `NOVAMART_USE_MERGE`, `NOVAMART_DISABLE_DATE_FILTER`, `NOVAMART_TIMEZONE`, `NOVAMART_S3_INJECT_EMPTY_FILE` Variables | `airflow_settings.yaml` | Config/toggles for the incident-demo DAGs — see each DAG's `doc_md` for what each one does |

**Docker Desktop vs. Linux Docker Engine:** connections/variables above use
`host.docker.internal` so containers can reach services running on the host
(the mock APIs, the Airflow REST API). This resolves automatically on Docker
Desktop (macOS/Windows). On native Linux Docker Engine it does **not**
resolve by default — add `extra_hosts: ["host.docker.internal:host-gateway"]`
to the relevant service config, or substitute the host's actual IP.

**Secrets in `airflow_settings.yaml`:** connection passwords/tokens in this
file are plaintext local dev config — never commit it (it's git-ignored for
this reason), and avoid pasting live tokens into chat/tickets when sharing
setup steps with teammates.

## Incident Memory (RAG)

The agentic incident response has a **persistent memory of past incidents**, so it can detect
recurrence instead of re-diagnosing a solved problem, and leaves an institutional record of every
outage (symptom → root cause → fix → ticket).

**How it works** — in `agentic_snowflake_incident`, two deterministic tasks wrap the agent:

- `recall_prior_incidents` runs **before** `investigate` and searches the memory for prior incidents
  on the failed pipeline; any matches are fed into the agent's prompt as a leading hypothesis to
  confirm.
- `record_incident` runs **after** the Jira ticket is created and stores the diagnosis, keyed by the
  ticket. (Re-recording the same ticket key updates the record in place — how a future close-sync
  would flip it to `closed`.)

It uses the [qbiz-agents](https://github.com/Qbizinc/qbiz-agents) **RAG engine as a library** (no MCP
server) — see [`include/incident_memory.py`](include/incident_memory.py). Local embeddings
(`fastembed`), so **no API key** is needed for the memory itself. The index + ledger persist under
`RAG_DATA_DIR` (default `include/.rag-incidents`, set in the `Dockerfile`); it's on the Astro-mounted
`include/` dir, so it survives across runs and is shared by the containers on one host. The directory
is git-ignored.

**Try it:**

1. Trigger `novamart_incident_memory_seed` once to preload a couple of example incidents.
2. Break `novamart_snowflake_sales` (e.g. `ALTER TABLE DAILY_SALES DROP COLUMN sku`, or set
   `NOVAMART_INJECT_BAD_DATA=true`) and let it fail.
3. Watch `agentic_snowflake_incident` recall the matching prior incident before it diagnoses, then
   record the new one.

Design and roadmap (pgvector for multi-worker scale, close-tracking sync DAG):
[`RAG_INCIDENT_MEMORY_PLAN.md`](RAG_INCIDENT_MEMORY_PLAN.md). Memory is **best-effort** — if it's
unavailable the incident response still runs; it just skips recall/record.

## Common Commands

```bash
astro dev start      # Start local Airflow
astro dev stop       # Stop containers
astro dev restart    # Restart after config changes
astro dev logs       # View logs
astro dev ps         # Check container status
```

## Adding Dependencies

- **Python packages** (available in DAGs): add to `requirements.txt`
- **OS packages**: add to `packages.txt`
- **Dev-only tools**: add to `requirements-dev.txt` (not deployed)

After changing `requirements.txt` or `packages.txt`, restart with `astro dev restart`.
