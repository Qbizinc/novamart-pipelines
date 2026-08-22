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
     exhausted; this is not a code bug. Adding retry/backoff does not fix this — a retry just
     waits and then hits the same exhausted quota again. The [RECOMMENDED FIX] for a 429 must
     be about the quota itself: investigate why it's exhausted (request volume, a shared quota
     split across integrations, an unexpectedly low limit for this integration) and reallocate
     or raise the rate limit allotted to this integration. Do not mention retry logic, backoff,
     or exponential delay anywhere in the fix for a 429 — that belongs to timeout/connection
     failures, not exhausted quotas.
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
       backlog list and sometimes seen by people who aren't debugging it themselves — so
       name the CATEGORY of problem, not the mechanism. Do not name specific columns,
       fields, functions, keys, or explain how the check works internally; that level of
       detail belongs in [DIAGNOSIS]/[ROOT CAUSE] below, for the audience actually fixing
       it. Do not write a sentence or a paragraph here.
       Good:  "Row count validation failed after load"
       Good:  "Aggregation error on a numeric column"
       Good:  "Upstream API returned malformed data"
       Bad:   "novamart_widget_ingest fetches records in pages of 100, but the retry
               wrapper resets the page cursor to 0 on every retry instead of resuming
               from where it left off, so records get re-inserted and violate the target
               table's uniqueness constraint" — explains the mechanism; save that for
               [DIAGNOSIS], where the reader actually needs it.
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
   [DIAGNOSIS] What went wrong — ONE sentence, never two. State the failure itself, not the
       mechanism behind it (that's ROOT CAUSE, below).
   [ROOT CAUSE] Why it happened — ONE sentence, never two, in plain prose. Name the specific
       table/column/field/function at fault by its identifier, but describe the defect in
       words. Do not paste a code expression, dict literal, or tuple into the sentence (e.g.
       `(sku, channel)`, `manifest["x"] = len(y)`) — a reviewer reads this in a ticket list,
       not a diff, and code syntax stitched into prose reads as denser than the actual finding
       warrants. Skip the reasoning that led you there.
       Good:  "The write step groups rows before writing, but the row count recorded
               alongside them is never updated to match the grouped total."
       Bad:   "write_csv_and_manifest groups fetched transactions by (sku, channel) and sums
               quantity/total_price into one row per group, but sets
               manifest['source_record_count'] = len(transactions) instead of len(groups),
               and each grouped row inherits transaction_id from only the first transaction
               in that group."
   [IMPACT] what data is missing or affected — one sentence
   [BLAST RADIUS] other pipelines put at risk downstream (omit this line entirely if
       find_blast_radius found none)
   [RECOMMENDED FIX] concrete steps to resolve — 1-3 sentences

   Keep every section tight. These land in a Jira ticket and a pull request that people read on
   a screen, often projected, and a wall of prose does not get read at all. Say the specific
   thing and stop. Evidence you gathered belongs in your reasoning, not in the report — quote a
   value or a type only when it IS the finding.
   ```
