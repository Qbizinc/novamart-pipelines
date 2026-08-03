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
   [SUMMARY] one-line ticket title
   [DIAGNOSIS] what went wrong
   [ROOT CAUSE] why it happened
   [IMPACT] what data is missing or affected
   [BLAST RADIUS] other pipelines put at risk downstream (omit this line entirely if
       find_blast_radius found none)
   [RECOMMENDED FIX] concrete steps to resolve
   ```
