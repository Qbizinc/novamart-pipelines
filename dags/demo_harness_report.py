"""
## Demo Harness Report — human-readable summary of the harness audit trail

Reads include/.harness-audit/audit.jsonl (written by agentic_incident_memory_v2's
harness-guarded tasks — see include/harness_audit.py) and prints a readable report to this task's
log: every incident recorded so far, grouped in order, showing every guarded action's outcome and
every harness intervention (a cost-governor cap or output-validator rejection that actually
fired), plus a final tally of interventions by component.

Read-only — never writes to the audit log. Safe to re-run any time; always reflects whatever the
audit log currently contains. Trigger manually to see the current state of the trail.
"""

import json
import os
from datetime import datetime

from airflow.sdk import dag, task

AUDIT_LOG_PATH = "/usr/local/airflow/include/.harness-audit/audit.jsonl"


@dag(
    dag_id="demo_harness_report",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    tags=["novamart", "incident-demo", "demo", "harness"],
)
def demo_harness_report():

    @task
    def print_report() -> None:
        """Parse the raw JSONL audit log and print a grouped, readable summary."""
        if not os.path.exists(AUDIT_LOG_PATH):
            print("No audit log found yet — no harness-guarded task has run.")
            return

        events: list[dict] = []
        with open(AUDIT_LOG_PATH, encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))

        print(f"{'=' * 78}\nHARNESS AUDIT REPORT — {len(events)} total event(s)\n{'=' * 78}\n")

        # Group by incident_id, preserving first-seen order (dict insertion order in Python 3.7+)
        incidents: dict[str, list[dict]] = {}
        for e in events:
            incidents.setdefault(e.get("incident_id") or "(no incident_id)", []).append(e)

        print(f"Incidents recorded: {len(incidents)}\n")
        for incident_id, evs in incidents.items():
            print(f"--- {incident_id} ---")
            for e in evs:
                is_intervention = e.get("event_type") == "harness_intervention"
                marker = "  [INTERVENTION]" if is_intervention else "  "
                detail = ""
                if is_intervention and e.get("intervention"):
                    comp = e["intervention"].get("component")
                    prevented = e["intervention"].get("prevented")
                    detail = f" — {comp}: {prevented}"
                elif e.get("outputs"):
                    detail = f" — {e['outputs']}"
                print(f"{marker}{e.get('ts', '?')}  {e.get('action', '?'):<28} "
                      f"decision={e.get('decision', '?')}{detail}")
            print()

        interventions = [e for e in events if e.get("event_type") == "harness_intervention"]
        print(f"{'=' * 78}\nTotal harness interventions: {len(interventions)}")
        if interventions:
            counts: dict[str, int] = {}
            for e in interventions:
                comp = (e.get("intervention") or {}).get("component", "unknown")
                counts[comp] = counts.get(comp, 0) + 1
            for comp, n in counts.items():
                print(f"  - {comp}: {n}")
        print(f"{'=' * 78}")

    print_report()


demo_harness_report()
