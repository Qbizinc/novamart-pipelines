"""Shared formatting for the incident DAG's outward-facing messages — Slack, Jira, GitHub PRs.

Every investigate_* agent (include/incident_instructions/*.md) is instructed to return exactly:

    [SUMMARY] one-line ticket title
    [DIAGNOSIS] what went wrong
    [ROOT CAUSE] why it happened
    [IMPACT] what data is missing or affected
    [RECOMMENDED FIX] concrete steps to resolve

Previously each of Slack/Jira/PR either dumped that whole block as raw text or only picked off
[SUMMARY]. parse_diagnosis splits it into named sections once; the render_* functions below build
each destination's actual structured format (Slack Block Kit, Jira ADF, a PR body) from that same
parsed dict, so root cause and recommended fix read as distinct fields everywhere instead of one
undifferentiated wall of text.
"""
from __future__ import annotations

import re

from airflow.sdk import Variable

_SECTION_RE = re.compile(r"\[(SUMMARY|DIAGNOSIS|ROOT CAUSE|IMPACT|RECOMMENDED FIX)\]\s*")

# Despite api.md/aws.md/snowflake.md instructing "return your findings as plain structured
# text... this is the final answer", models sometimes wrap that block in a markdown code fence
# anyway (often after unrequested narrative preamble). Regex-searching for the tags anywhere in
# the text already discards the preamble (nothing before the first match is kept), but the LAST
# section still runs to the end of the raw string, which pulls in a trailing closing "```" if the
# model added one. Strip it defensively rather than showing a stray fence in every destination.
_TRAILING_FENCE_RE = re.compile(r"\n?```\s*$")

# Slack section-block text fields cap at 3000 chars; leave headroom for the surrounding label.
_MAX_FIELD_LEN = 2500


def parse_diagnosis(text: str) -> dict[str, str]:
    """Split the agent's tagged diagnosis text into named sections.

    Falls back to putting everything under 'diagnosis' if the tags are missing/malformed — a
    model that skips a tag should degrade the formatting, not crash the notification.
    """
    matches = list(_SECTION_RE.finditer(text or ""))
    if not matches:
        return {
            "summary": "", "diagnosis": (text or "").strip(),
            "root_cause": "", "impact": "", "recommended_fix": "",
        }

    sections: dict[str, str] = {}
    for i, m in enumerate(matches):
        key = m.group(1).lower().replace(" ", "_")
        start = m.end()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
        value = _TRAILING_FENCE_RE.sub("", text[start:end].strip()).strip()
        sections[key] = value[:_MAX_FIELD_LEN]

    return {
        "summary": sections.get("summary", ""),
        "diagnosis": sections.get("diagnosis", ""),
        "root_cause": sections.get("root_cause", ""),
        "impact": sections.get("impact", ""),
        "recommended_fix": sections.get("recommended_fix", ""),
    }


def airflow_run_url(dag_id: str, run_id: str) -> str:
    """Best-effort browser-clickable link to the triggering DAG run.

    AIRFLOW_BASE_URL is set to the Docker-internal host (host.docker.internal) because
    gather_context/trigger_incident_dag_v2 call the API *from inside* the containers. That
    hostname doesn't resolve in a human's browser, so links meant for Slack/Jira/GitHub swap it
    for localhost — a heuristic, not a guarantee, for whatever setup is actually running Astro.
    """
    base = Variable.get("AIRFLOW_BASE_URL", default="http://host.docker.internal:8080")
    external_base = base.replace("host.docker.internal", "localhost")
    return f"{external_base}/dags/{dag_id}/runs/{run_id}"


def build_incident_blocks(
    *,
    severity: str,
    dag_id: str,
    run_id: str,
    sections: dict[str, str],
    owner_mention: str = "",
    links: list[tuple[str, str]] | None = None,
) -> tuple[list[dict], str]:
    """Build a Slack Block Kit payload for one incident notification.

    Returns (blocks, fallback_text) — Slack requires a plain-text `text` alongside `blocks` for
    notifications/screen readers, so both are returned together.
    """
    icon, label = {
        "critical": (":rotating_light:", "CRITICAL"),
        "fix": (":hammer_and_wrench:", "Auto-fixed"),
        "ticket": (":ticket:", "Ticketed (low priority)"),
    }[severity]
    fallback_text = f"{icon} {dag_id} — {label}"

    blocks: list[dict] = [
        {"type": "header", "text": {"type": "plain_text", "text": fallback_text[:150]}},
        {
            "type": "section",
            "fields": [
                {"type": "mrkdwn", "text": f"*Pipeline:*\n{dag_id}"},
                {"type": "mrkdwn", "text": f"*Run:*\n{run_id}"},
            ],
        },
        {"type": "divider"},
    ]
    if sections.get("root_cause"):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Root cause:*\n{sections['root_cause']}"}})
    if sections.get("recommended_fix"):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Recommended fix:*\n{sections['recommended_fix']}"}})
    if not sections.get("root_cause") and not sections.get("recommended_fix") and sections.get("diagnosis"):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"*Diagnosis:*\n{sections['diagnosis']}"}})

    for text, url in (links or []):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"<{url}|{text}>"}})

    if owner_mention.strip():
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": owner_mention.strip()}]})

    return blocks, fallback_text


def build_adf_description(sections: dict[str, str]) -> dict:
    """Jira Atlassian Document Format body: a heading + paragraph per section, instead of one
    undifferentiated paragraph."""

    def heading(text: str) -> dict:
        return {"type": "heading", "attrs": {"level": 3}, "content": [{"type": "text", "text": text}]}

    def para(text: str) -> dict:
        return {"type": "paragraph", "content": [{"type": "text", "text": text or "—"}]}

    content: list[dict] = []
    for label, key in [
        ("Diagnosis", "diagnosis"),
        ("Root Cause", "root_cause"),
        ("Impact", "impact"),
        ("Recommended Fix", "recommended_fix"),
    ]:
        content.append(heading(label))
        content.append(para(sections.get(key, "")))
    return {"type": "doc", "version": 1, "content": content}


def build_pr_body(*, dag_id: str, run_id: str, sections: dict[str, str], run_url: str) -> str:
    """GitHub PR body: root cause + what changed as their own sections, plus a link back to the
    triggering Airflow run, instead of one boilerplate sentence."""
    return (
        f"### Root cause\n{sections.get('root_cause') or '—'}\n\n"
        f"### What changed\n{sections.get('recommended_fix') or '—'}\n\n"
        f"### Context\n"
        f"- Pipeline: `{dag_id}`\n"
        f"- Triggering run: `{run_id}`\n"
        f"- [View the failed Airflow run]({run_url})\n\n"
        f"**⚠️ Unreviewed** — opened as a draft by `agentic_snowflake_incident_memory_v2`. "
        f"Review the diff and mark ready for review (or close it) before merging."
    )
