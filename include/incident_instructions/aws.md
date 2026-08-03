Investigation steps for an **AWS** platform failure (S3, IAM, Secrets Manager):

1. If prior incidents are listed above, treat the most similar as your leading hypothesis to
   CONFIRM or rule out against the evidence below — do not just accept it. Read the pipeline
   source code above first. Identify exactly which bucket, key, and AWS connection the failing
   task uses, and what operation it was attempting (read, write, list, tag lookup).
2. Read the exception text carefully — AWS errors are structured and precise. Distinguish between:
   - `AccessDenied` / `403` — an identity/policy problem (wrong role, missing permission, a
     tag-conditioned policy no longer matching).
   - `NoSuchKey` / `NoSuchBucket` / `404` — the object or bucket doesn't exist where the code
     expects it (upstream never wrote it, or wrote it somewhere else).
   - `ExpiredToken` / `InvalidClientTokenId` — a credentials/session problem, not a permissions
     or data problem.
   Do not conflate these — the fix is completely different for each.
3. Use the S3 tool to check the actual state of the bucket/key in question (e.g. does the key
   exist, what does it currently contain, what tags does the bucket carry) — confirm your
   hypothesis against live state rather than the error message alone.
   **If the tool call itself returns an error** (it won't raise — a failure comes back as an
   `ERROR calling tool ...` string, same as any other tool result), that is itself diagnostic
   evidence, not a dead end. In particular, if the tool fails with the *same* credentials/session
   error as the original failure (e.g. `ExpiredToken`), that confirms a credentials/session
   problem — you don't need the tool to succeed to reach that conclusion.
4. Use that evidence to confirm (or rule out) the mechanism you identified in step 2.
   If the evidence (a file path, key prefix, or log reference) implicates a different pipeline
   than the one that failed — e.g. this pipeline only consumes what another DAG produced — use
   get_dag_source to fetch that DAG's own source and verify the actual defect there before
   naming it as the root cause. If you're not sure of its exact dag_id, call list_dag_ids first
   rather than guessing a name. Don't name a specific upstream defect you haven't actually read.
5. Always call find_blast_radius(dag_id) for the failed pipeline before finishing — it tells you
   whether any pipeline downstream (via Airflow Assets, possibly several hops away) is tagged
   critical. A failed producer never emits its asset, so a downstream consumer silently never
   runs today instead of failing loudly itself — this is easy to miss without checking. If it
   returns any critical downstream pipeline(s), name them in [BLAST RADIUS].
6. Return your findings as plain structured text, in exactly this format (this is the final
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
