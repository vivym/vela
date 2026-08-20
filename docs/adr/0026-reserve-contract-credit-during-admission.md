# Reserve contract credit during Admission

Admission checks the Customer Organization's Contract Credit Limit and atomically creates the QUEUED Job, Credit Reservation, PricingSnapshot, execution policy, idempotency result, and Outbox event before returning `202 Accepted`. Insufficient credit returns `402 Payment Required` with `credit_limit_exceeded` and creates no Job; Credit Reservation remains until Job Coordinator converts it to a Charge or releases it at a non-billable terminal outcome.

## Consequences

Vela removes PENDING_AUTH, external payment holds, authorization expiry and renewal, and CAPTURE_PENDING from the launch Job and billing state machines. Monthly Invoice export is asynchronous and cannot block Admission, Visible Completion, cancellation, failure, or Artifact access.
