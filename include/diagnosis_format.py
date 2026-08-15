"""Shared formatting for the incident DAG's outward-facing messages — Slack, Jira, GitHub PRs.

Every investigate_* agent (include/incident_instructions/*.md) is instructed to return exactly:

    [SUMMARY] one-line ticket title
    [PRIOR INCIDENT] which recalled incident (if any) was reused, and how
    [DIAGNOSIS] what went wrong
    [ROOT CAUSE] why it happened
    [IMPACT] what data is missing or affected
    [BLAST RADIUS] what else, downstream, is affected or at risk (optional — omitted when there
        is none worth calling out)
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

_SECTION_RE = re.compile(
    r"\[(SUMMARY|PRIOR INCIDENT|DIAGNOSIS|ROOT CAUSE|IMPACT|BLAST RADIUS|RECOMMENDED FIX)\]\s*"
)

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
            "summary": "", "prior_incident": "", "diagnosis": (text or "").strip(),
            "root_cause": "", "impact": "", "blast_radius": "", "recommended_fix": "",
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
        "prior_incident": sections.get("prior_incident", ""),
        "diagnosis": sections.get("diagnosis", ""),
        "root_cause": sections.get("root_cause", ""),
        "impact": sections.get("impact", ""),
        "blast_radius": sections.get("blast_radius", ""),
        "recommended_fix": sections.get("recommended_fix", ""),
    }


_TICKET_KEY_RE = re.compile(r"\b([A-Z][A-Z0-9]+-\d+)\b")

# Phrases that mean "I looked and none of them applied". Only consulted as a fallback, for a
# model that ignores the APPLIED/NONE verdict — see reused_ticket_keys.
_REJECTION_RE = re.compile(
    r"\b(neither|none (?:of these |of them )?(?:apply|applies|applied)|"
    r"no prior|not applicable|does not apply|do not apply|did not apply|doesn't apply|"
    r"none on record)\b",
    re.I,
)


def reused_ticket_keys(prior_incident: str | None) -> list[str]:
    """Ticket keys the agent actually REUSED, from its [PRIOR INCIDENT] verdict.

    The agent is instructed to open the line with `APPLIED: <KEY>` or `NONE`, so the verdict is
    read rather than inferred. This matters: an earlier version scraped every ticket key out of
    the prose, which meant a diagnosis saying "neither AD-45 nor AD-40 applies" produced a Slack
    alert announcing both as reused — claiming credit for knowledge the agent had explicitly
    rejected. A wrong attribution here is worse than none, so anything that isn't a clear APPLIED
    verdict returns nothing.
    """
    text = (prior_incident or "").strip()
    if not text:
        return []

    verdict, _, rest = text.partition(":")
    if verdict.strip().upper() == "APPLIED":
        body = rest                    # explicit verdict — read what follows it
    elif verdict.strip().upper().startswith("NONE") or text.upper().startswith("NONE"):
        return []                      # explicit rejection
    else:
        # Freeform. Models follow NONE reliably but routinely skip APPLIED, writing e.g.
        # "AD-45 (same pipeline): identical failure pattern; prior fix was retry-with-backoff".
        # Requiring the keyword lost every genuine reuse, so fall back to intent: a named ticket
        # counts as reuse UNLESS the text explicitly rejects it. That ordering matters — reading
        # keys without checking for rejection is what once announced "Reused AD-45, AD-40" on a
        # diagnosis saying neither applied.
        body = text

    if _REJECTION_RE.search(body):
        return []

    seen, out = set(), []
    for key in _TICKET_KEY_RE.findall(body):
        if key not in seen:
            seen.add(key)
            out.append(key)
    return out


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
    duplicate_of: str | None = None,
) -> tuple[list[dict], str]:
    """Build a short Slack Block Kit alert for one incident notification.

    Slack is a brief "something happened, here's where to look" alert — the header, a one-line
    summary, and links to the Jira ticket / PR / Airflow run. Root cause, impact, and recommended
    fix are NOT repeated here; those live in the Jira ticket description or PR body, which are the
    comprehensive record for root-cause analysis.

    Returns (blocks, fallback_text) — Slack requires a plain-text `text` alongside `blocks` for
    notifications/screen readers, so both are returned together.
    """
    icon, label = {
        "critical": (":rotating_light:", "CRITICAL — P1 ticket"),
        "fix": (":hammer_and_wrench:", "Auto-fixed"),
        "ticket": (":ticket:", "Ticketed — P2"),
    }[severity]
    # A recurrence of an open incident creates nothing, so the severity label would contradict
    # the body: a header reading "Ticketed — P2" above "No new ticket created" is the first
    # thing a reader sees and the last thing they should have to reconcile.
    if duplicate_of:
        icon, label = ":repeat:", "Already tracked — no action taken"
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
    ]

    if duplicate_of:
        # Slack is the ONLY output for a recurrence of an open incident — the ticket is left
        # untouched, so this line has to carry the whole story: what happened, that it is already
        # tracked, and that nothing was created.
        blocks.append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Already tracked* — this is a recurrence of `{duplicate_of}`, which is "
                        f"still open. No new ticket created and the ticket was not modified.",
            },
        })
    else:
        summary = sections.get("summary") or sections.get("diagnosis", "")[:200] or "See ticket for details."
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": summary}})

    # Slack is an alert, not a record: just the ticket keys, no prose. The reasoning behind the
    # reuse belongs in the Jira description and the PR body, which are where someone goes to
    # actually evaluate it — repeating it here only makes the alert longer to skim.
    reused_keys = reused_ticket_keys(sections.get("prior_incident"))
    if reused_keys and not duplicate_of:
        blocks.append({"type": "context", "elements": [
            {"type": "mrkdwn", "text": f":brain: Reused {', '.join(reused_keys)} — see ticket"},
        ]})

    for text, url in (links or []):
        blocks.append({"type": "section", "text": {"type": "mrkdwn", "text": f"<{url}|{text}>"}})

    if owner_mention.strip():
        blocks.append({"type": "context", "elements": [{"type": "mrkdwn", "text": owner_mention.strip()}]})

    return blocks, fallback_text


JIRA_BROWSE = "https://qbizinc.atlassian.net/browse/"


def build_adf_description(
    sections: dict[str, str], *, links: list[tuple[str, str]] | None = None,
    related: list[dict] | None = None, recurrence_of: str | None = None,
) -> dict:
    """Jira Atlassian Document Format body: a heading + paragraph per section, instead of one
    undifferentiated paragraph. `links` (e.g. a PR opened for this incident) render as their own
    clickable-link paragraph under a "Related Links" heading, appended after the usual sections.

    `recurrence_of` and `related` record what the agent knew going in — the closed ticket this
    repeats, and similar incidents on other pipelines it was shown. They lead the description
    rather than trailing it: a reader deciding whether to trust an automated ticket wants "we have
    seen this before, here is which one" first, not buried under the diagnosis.
    """

    def heading(text: str) -> dict:
        return {"type": "heading", "attrs": {"level": 3}, "content": [{"type": "text", "text": text}]}

    def para(text: str) -> dict:
        return {"type": "paragraph", "content": [{"type": "text", "text": text or "—"}]}

    def link_para(text: str, url: str) -> dict:
        return {"type": "paragraph", "content": [{"type": "text", "text": text, "marks": [{"type": "link", "attrs": {"href": url}}]}]}

    content: list[dict] = []

    # Only report prior knowledge that was actually USED. Being *shown* a similar incident is not
    # interesting — the memory offers candidates on nearly every failure, so listing them all put
    # a "Prior Knowledge Used" section on every ticket that half the time concluded "none of these
    # applied". A section that is usually noise gets skipped by readers, taking the signal with it.
    applied = reused_ticket_keys(sections.get("prior_incident"))
    applied_related = [r for r in (related or []) if r.get("key") in applied]

    if recurrence_of or applied_related:
        content.append(heading("Prior Knowledge Used"))
        if recurrence_of:
            content.append({"type": "paragraph", "content": [
                {"type": "text", "text": "This is a recurrence of "},
                {"type": "text", "text": recurrence_of,
                 "marks": [{"type": "link", "attrs": {"href": JIRA_BROWSE + recurrence_of}}]},
                {"type": "text", "text": " — the same failure on this pipeline, previously closed."},
            ]})
        for r in applied_related:
            key = r.get("key", "?")
            content.append({"type": "paragraph", "content": [
                {"type": "text", "text": "Reused the resolution from "},
                {"type": "text", "text": key,
                 "marks": [{"type": "link", "attrs": {"href": JIRA_BROWSE + key}}]},
                {"type": "text",
                 "text": f" on {r.get('dag_id') or 'another pipeline'} "
                         f"(similarity {float(r.get('score', 0.0)):.2f})."},
            ]})

        # The agent's own account of what it reused — only alongside an actual reuse.
        if sections.get("prior_incident"):
            content.append(heading("What The Agent Reused"))
            content.append(para(sections["prior_incident"]))

    # Only render sections the agent actually filled in. Blast Radius is explicitly optional —
    # most failures have nothing downstream worth calling out — and a heading followed by an
    # em dash is worse than no heading: it reads as missing information rather than as "none".
    for label, key in [
        ("Diagnosis", "diagnosis"),
        ("Root Cause", "root_cause"),
        ("Impact", "impact"),
        ("Blast Radius", "blast_radius"),
        ("Recommended Fix", "recommended_fix"),
    ]:
        value = (sections.get(key) or "").strip()
        if not value or value == "—":
            continue
        content.append(heading(label))
        content.append(para(value))

    if links:
        content.append(heading("Related Links"))
        for text, url in links:
            content.append(link_para(text, url))

    return {"type": "doc", "version": 1, "content": content}



def build_pr_body(*, dag_id: str, run_id: str, sections: dict[str, str], run_url: str) -> str:
    """GitHub PR body: root cause + what changed as their own sections, plus a link back to the
    triggering Airflow run, instead of one boilerplate sentence."""
    prior = (sections.get("prior_incident") or "").strip()
    # A reviewer's first question about an agent-authored fix is "where did this come from" —
    # answer it above the diff rather than leaving the reuse implicit.
    prior_block = (
        f"### Prior incident reused\n{prior}\n\n"
        if prior and prior.lower().rstrip(".") != "none on record" else ""
    )
    return (
        f"{prior_block}"
        f"### Root cause\n{sections.get('root_cause') or '—'}\n\n"
        f"### What changed\n{sections.get('recommended_fix') or '—'}\n\n"
        f"### Context\n"
        f"- Pipeline: `{dag_id}`\n"
        f"- Triggering run: `{run_id}`\n"
        f"- [View the failed Airflow run]({run_url})\n\n"
        f"**⚠️ Unreviewed** — opened as a draft by `agentic_incident_memory_v2`. "
        f"Review the diff and mark ready for review (or close it) before merging."
    )
