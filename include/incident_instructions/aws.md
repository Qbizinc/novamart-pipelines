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
5. Return your findings as plain structured text, in exactly this format (this is the final
   answer — you have no other tools to call after this):
   ```
   [SUMMARY] one-line ticket title
   [DIAGNOSIS] what went wrong
   [ROOT CAUSE] why it happened
   [IMPACT] what data is missing or affected
   [RECOMMENDED FIX] concrete steps to resolve
   ```
