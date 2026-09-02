# Separate Internal Usage Cost From Customer Charge

Date: 2026-08-29

Status: Accepted and implemented in the repository; production acceptance remains `0/9 PASS`.

## Context

Stage disaggregation creates operational costs that a single Job-level runtime
cannot explain: per-stage GPU execution, idle resident models, load/warm-up,
retries, network transfer, L2 storage, CPU media work, and finalization. Vela's
customer contract is deliberately simpler: one fixed Admission-time quote and
one Charge on Visible Completion or post-Billable-Start Customer Cancellation.

Mixing internal cost into Charge creation would make customer price depend on
placement, cache hits, failures, or platform efficiency. Omitting internal cost
would prevent capacity and cache decisions from being evaluated economically.

## Decision

Customer billing and internal cost remain separate Modules.

`UsageCostLedger` has two Interfaces:

```text
record_usage(resource_receipt)             -> UsageIdentity
value(usage_identity, cost_model_revision) -> CostAllocation
```

- `ResourceUsageRecord` is an immutable measured fact.
- `CostModelRevision` is a versioned internal valuation policy.
- `CostAllocationRecord` values one or more usage records without modifying
  them.
- Revaluation creates new allocation records; it never rewrites measured usage
  or a customer Charge.
- Usage identity and idempotency bind the source authority, resource kind,
  time/byte quantity, StageAttempt or pool cohort, and evidence digest.

Direct Job-attributable usage includes GPU-seconds times device count, CPU and
memory time, network bytes, object operations, L2 byte-time, and finalization.
Shared pool usage includes idle residency, model load/warm-up, minimum warm
capacity, drain, and failed reconfiguration. Retry and cancellation waste are
explicit classifications.

Cache avoided-compute is a versioned counterfactual estimate, not negative
actual usage. It is reported separately from storage and read-transfer cost.

## Allocation

Shared residency allocation is policy, not measurement. A CostModelRevision may
allocate it by occupied time, completed normalized work, reserved capacity, or
another declared method. Reports must show direct usage and allocated shared
cost separately so a policy change cannot be mistaken for a physical change.

## Consequences

- Customer pricing remains stable when placement, retry, or cache behavior
  changes.
- Operators can compare stage layouts, warm capacity, cache policies, and
  transfer choices using measured facts.
- Ledger storage and telemetry volume increase and require retention and
  reconciliation.
- Internal cost is not a Production Gate or accounting ledger by itself.

## Rejected alternatives

- Do not introduce internal cost: rejected because utilization alone cannot
  compare residency, retry, storage, and transfer tradeoffs.
- Charge per stage or actual GPU time: rejected because it breaks the accepted
  fixed-price product contract.
- Record cache hit as negative usage: rejected because avoided work is a
  counterfactual, not a consumed resource.
- Put high-rate GPU telemetry in PostgreSQL: rejected because only bounded,
  immutable usage receipts belong in durable authority storage.

## Evidence boundary

No cost weights, allocation method, or cache savings are calibrated by this
ADR. CostModelRevision activation requires versioned evidence and shadow
comparison; customer RateCardRevision remains independent.
