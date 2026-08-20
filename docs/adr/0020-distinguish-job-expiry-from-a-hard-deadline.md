# Distinguish Job Expiry from a Hard Deadline

Vela replaces the ambiguous internal `deadline_at` with immutable `job_expires_at` in ExecutionPolicySnapshot. Job Expiry bounds the total time spent queued, executing, retrying, and finalizing; when it arrives, Job Coordinator fences active work, stops further Retry, and terminates the Job as FAILED without a Charge.

## Consequences

The API may expose `job_expires_at` only as the time after which Vela will no longer continue the Job, never as a promise of completion before that time. Its value is derived from certified preset, service-class, and Retry Budget policy and cannot be silently extended after Admission.
