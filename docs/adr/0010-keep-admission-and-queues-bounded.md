# Keep admission and queues bounded

Vela returns `202 Accepted` only after the Job, PricingSnapshot, execution policy, and Credit Reservation commit together. Project quota exhaustion returns `429 Too Many Requests`, and insufficient preset capacity or excessive predicted queue delay returns `503 Service Unavailable` with `capacity_unavailable`; both include `Retry-After`, create no Job, and may re-evaluate the same Idempotency-Key later.

## Consequences

Queues have explicit Project and pool bounds, and an Accepted Job cannot later become REJECTED because of ordinary congestion. Transient Capacity Rejection is not cached as a permanent idempotent business result, while a lost response after successful Admission still resolves to the original Accepted Job.
