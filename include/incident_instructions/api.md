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
3. You do not currently have a live tool to re-query the failing endpoint — base your diagnosis
   on the task logs and pipeline source code alone. Do not guess at upstream behavior you cannot
   see evidence for in the logs.
4. Return your findings as plain structured text, in exactly this format (this is the final
   answer — you have no other tools to call after this):
   ```
   [SUMMARY] one-line ticket title
   [DIAGNOSIS] what went wrong
   [ROOT CAUSE] why it happened
   [IMPACT] what data is missing or affected
   [RECOMMENDED FIX] concrete steps to resolve
   ```
