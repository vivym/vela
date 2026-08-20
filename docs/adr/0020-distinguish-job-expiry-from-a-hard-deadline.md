# Distinguish Job Expiry from a Hard Deadline

Vela replaces the ambiguous internal `deadline_at` with immutable `job_expires_at` in ExecutionPolicySnapshot. Job Expiry bounds the total time spent queued, executing, retrying, and finalizing; when it arrives, Job Coordinator fences active work, stops further Retry, and terminates the Job as FAILED without a Charge.

## Consequences

The API may expose `job_expires_at` only as the time after which Vela will no longer continue the Job, never as a promise of completion before that time. Admission derives its lifetime as the ServiceClassRevision queue-and-retry allowance plus the certified-preset-based maximum total compute budget plus `max_attempts * max_finalization_seconds_per_attempt`. The resulting value cannot be silently extended after Admission.
