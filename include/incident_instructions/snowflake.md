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
5. Return your findings as plain structured text, in exactly this format (this is the final
   answer — you have no other tools to call after this):
   ```
   [SUMMARY] one-line ticket title
   [DIAGNOSIS] what went wrong
   [ROOT CAUSE] why it happened
   [IMPACT] what data is missing or affected
   [RECOMMENDED FIX] concrete steps to resolve
   ```
