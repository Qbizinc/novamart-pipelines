"""
## Seed Incident Memory

Preloads a couple of realistic past incidents into the RAG incident memory so the recurrence demo
has prior history to find on the **first** real investigation. Trigger manually once.

Idempotent — records are keyed by ticket, so re-running replaces rather than duplicates. See
`include/incident_memory.py` and `RAG_INCIDENT_MEMORY_PLAN.md`.
"""

from datetime import datetime

from airflow.sdk import dag, task


@dag(
    dag_id="novamart_incident_memory_seed",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident", "seed"],
)
def novamart_incident_memory_seed():

    @task
    def seed() -> None:
        from include import incident_memory

        incident_memory.seed()
        print("[novamart_incident_memory_seed] seeded example incidents into incident memory.")

    seed()


novamart_incident_memory_seed()
