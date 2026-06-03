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
