FROM astrocrpublic.azurecr.io/runtime:3.2-4

# Incident memory (RAG) persists here — on the Astro-mounted include/ dir, so the index survives
# across DAG runs and is shared by the scheduler/worker/triggerer containers on one host.
# See include/incident_memory.py and RAG_INCIDENT_MEMORY_PLAN.md.
ENV RAG_DATA_DIR=/usr/local/airflow/include/.rag-incidents

# NOTE: the first ingest/search downloads the fastembed model (~90 MB, BAAI/bge-small-en-v1.5) and
# caches it in the container. For a snappier first run you can pre-fetch it at build time; doing so
# reliably needs a fixed cache dir the engine doesn't yet expose (a planned qbiz-agents knob), so
# it's deliberately left to first-use here.
