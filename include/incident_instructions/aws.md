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
