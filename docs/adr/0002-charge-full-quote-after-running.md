# Charge the full quote after a Job starts running

A Customer Cancellation before RUNNING releases the Credit Reservation without a Charge. Once the Job has entered RUNNING, a Customer Cancellation creates a Charge for the full quoted amount, including during FINALIZING; a platform-initiated termination or terminal platform failure creates no Charge. This keeps customer pricing deterministic and avoids making unreliable stage progress or partial GPU telemetry part of the billing contract.

## Consequences

The transition to RUNNING is the Billable Start and must be durably committed before work is treated as customer-cancellable at full price. Commercial exceptions use an external credit note and do not rewrite the Job, Charge, or execution history.
