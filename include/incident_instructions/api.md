Investigation steps for an **API** platform failure (an upstream HTTP service the pipeline
depends on):

1. If prior incidents are listed above, treat the most similar as your leading hypothesis to
   CONFIRM or rule out against the evidence below — do not just accept it. Read the pipeline
   source code above first. Identify exactly which endpoint was called, with what timeout, and
   what the code does with the response (does it check status codes? Required fields? Retries?).
2. Read the exception text carefully — distinguish between:
   - `Timeout` / `ConnectionError` — the service was unreachable or too slow, not a bad
     response. This points at the upstream service's availability, not the pipeline's logic.
   - `401` / `403` — an auth problem (expired token, missing header) — not an availability
     problem.
   - `429` — rate limiting — the caller is sending too many requests, or a shared quota was
     exhausted; this is not a code bug.
   - `4xx` other than the above / malformed JSON in a 200 response — a contract/schema
     mismatch between what the pipeline expects and what the API actually returned.
3. You do not have a live tool to re-query the failing endpoint itself — base the diagnosis of
   the API call on the task logs and pipeline source code alone. Do not guess at upstream
   behavior you cannot see evidence for in the logs.
4. You do have tools to look at OTHER pipelines: list_dag_ids, get_dag_source, and
   find_blast_radius. Always call find_blast_radius(dag_id) for the failed pipeline before
   finishing — it tells you whether any pipeline downstream (via Airflow Assets, possibly several
   hops away) is tagged critical. A failed producer never emits its asset, so a downstream
   consumer silently never runs today instead of failing loudly itself — this is easy to miss
   without checking. If it returns any critical downstream pipeline(s), name them in
   [BLAST RADIUS]. If you're not sure of a dag_id's exact spelling, call list_dag_ids first.
5. Return your findings as plain structured text, in exactly this format (this is the final
   answer — you have no other tools to call after this):
   ```
   [SUMMARY] A SHORT title, like a headline — aim for under 90 characters, never more
       than 120. It is the ticket title and the pull-request title, read on its own in a
       backlog list, so it must say what is wrong, not explain why. Do not write a sentence
       or a paragraph here; the explanation belongs in the sections below.
       Good:  "CSV grain does not match manifest row count"
       Good:  "SUM() failing on a text column in gold aggregation"
       Bad:   "novamart_transactions_csv_export aggregates transactions by (sku, channel)
               before writing the CSV, but stamps the manifest with the pre-aggregation raw
               transaction count, causing the downstream QA grain check to fail"
       Never omit this field — a missing summary leaves the ticket and PR titled "Automated fix".
   [PRIOR INCIDENT] This line MUST begin with one of two verdicts, in caps:
       APPLIED: <TICKET-KEY> — then how it applied. Use this ONLY for an incident whose
         root cause or fix you actually used, confirmed against this run's evidence. E.g.
         "APPLIED: AD-40 — same VARCHAR root cause, confirmed here; using its fix."
         List only tickets you genuinely reused; name several only if you used several.
       NONE — then one line on why. Use this when you were shown prior incidents but none
         applied, AND when none were shown at all. E.g.
         "NONE — AD-45 was a transient 503; this run's API call succeeded. Distinct cause."
       The verdict is read by tooling, so the first word decides what gets reported: writing
       APPLIED makes the system tell everyone you reused that ticket. Never claim to have used
       an incident that was not shown to you, and never leave this blank.
   [DIAGNOSIS] what went wrong — 1-2 sentences
   [ROOT CAUSE] why it happened — 1-2 sentences, in plain language. Name the specific table,
       column, field or function at fault; skip the reasoning that led you there.
   [IMPACT] what data is missing or affected — one sentence
   [BLAST RADIUS] other pipelines put at risk downstream (omit this line entirely if
       find_blast_radius found none)
   [RECOMMENDED FIX] concrete steps to resolve — 1-3 sentences

   Keep every section tight. These land in a Jira ticket and a pull request that people read on
   a screen, often projected, and a wall of prose does not get read at all. Say the specific
   thing and stop. Evidence you gathered belongs in your reasoning, not in the report — quote a
   value or a type only when it IS the finding.
   ```
