# Do not charge failed Jobs

Vela creates a Charge only for Visible Completion or for Customer Cancellation after Billable Start. A Job that reaches terminal FAILED because of platform faults, exhausted Retry Budget, deterministic input failure, unsupported input, or content-policy rejection releases its Credit Reservation without a Charge; invalid or abusive traffic is controlled through validation, limits, suspension, and contract enforcement instead of failure fees.

## Consequences

Failure attribution does not become a billing dispute or payment rule. Platform-caused retries remain internal cost, deterministic customer failures are not retried, and operations must retain enough failure classification and audit evidence to explain the outcome without turning that classification into a price calculation.
