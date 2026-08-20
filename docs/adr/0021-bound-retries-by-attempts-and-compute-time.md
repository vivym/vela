# Bound retries by attempts and compute time

The launch Retry Budget allows at most three compute Attempts and initially caps cumulative compute at twice the selected preset's certified p95 runtime. Early failures may therefore reach a third Attempt, while a late failure usually leaves budget for only one retry; non-retryable failure, repeated failure fingerprint, revision circuit opening, exhausted budget, or Job Expiry ends retry.

## Consequences

Artifact upload, validation, and commit recovery consume a separate finalization elapsed-time budget and never create another compute Attempt. Retry policy is immutable in each Accepted Job's ExecutionPolicySnapshot, cannot be enlarged per request, and its launch values require fault-injection and measured-runtime certification before production activation.
