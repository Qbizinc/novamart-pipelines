Investigation steps for a **Snowflake** platform failure:

1. If prior incidents are listed above, treat the most similar as your leading hypothesis to
   CONFIRM or rule out against the evidence below — do not just accept it. Read the pipeline
   source code above first. Identify exactly how each write/match/dedup key is constructed (e.g.
   is it a stable business key, or something regenerated fresh on every run?). Your root cause
   must be grounded in what the code actually does, not just inferred from the shape of the data —
   a plausible-looking guess (e.g. "missing a GROUP BY") is wrong if the code doesn't show that.
2. Use the SQL tool to check for schema drift on any table referenced in the failed task logs,
   e.g.:
   `DESCRIBE TABLE SANDBOX_DATA_PIPELINE.NOVAMART_RAW.<table_name>`
3. Use the SQL tool to check data freshness/state on that table, e.g.:
   `SELECT COUNT(*), MAX(loaded_at) FROM SANDBOX_DATA_PIPELINE.NOVAMART_RAW.<table_name>`
4. Use the Snowflake evidence to confirm (or rule out) the mechanism you identified in step 1 —
   the data pattern should match what the code predicts, not just look superficially similar.
   State only what your queries and the source code actually show. If the pipeline code doesn't
   produce the state you observed, say so as a discrepancy (declared vs. observed) — do not guess
   at who or how it happened (e.g. "manually", "out-of-band") unless you have direct evidence
   (e.g. query history) for that claim.
   If the evidence (a file path, table name, or log reference) implicates a different pipeline
   than the one that failed — e.g. this pipeline only consumes what another DAG produced — use
   get_dag_source to fetch that DAG's own source and verify the actual defect there before
   naming it as the root cause. If you're not sure of its exact dag_id, call list_dag_ids first
   rather than guessing a name. Don't name a specific upstream defect you haven't actually read.
   When a check compares two numbers/values that disagree (e.g. a manifest/count vs. actual
   rows), don't just recommend making them match — figure out which one reflects the pipeline's
   intended behavior (read the code that produced each value) and recommend fixing whichever side
   is actually wrong. Never recommend weakening, removing, or working around the check itself
   (a validation/QA/uniqueness/grain check) to make it stop firing — it exists to catch exactly
   this kind of problem and is doing its job correctly.
   To judge which side is actually wrong, look for internal inconsistencies within the suspect
   code itself, not just which value looks more "official": e.g. a field that should identify one
   specific record (an id, a key) but is populated from a group/aggregation of several records is
   a sign that the aggregation was not the intended design — the identity field only makes sense
   at the finer grain. Ground your conclusion in a concrete inconsistency like this, not a guess.
5. Always call find_blast_radius(dag_id) for the failed pipeline before finishing — it tells you
   whether any pipeline downstream (via Airflow Assets, possibly several hops away) is tagged
   critical. A failed producer never emits its asset, so a downstream consumer silently never
   runs today instead of failing loudly itself — this is easy to miss without checking. If it
   returns any critical downstream pipeline(s), name them in [BLAST RADIUS].
6. Return your findings as plain structured text, in exactly this format (this is the final
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
