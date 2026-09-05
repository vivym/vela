# Runtime Usage Accounting

Migrations 00076 and 00077 add transactional runtime receipts to the internal
Usage/Cost Ledger. Customer Charges retain their Admission-time fixed quote.
These receipts are produced by state transitions, so response loss and replay
cannot create a second receipt for the same transition.

| Receipt | Quantity and unit | Attribution | Evidence boundary |
| --- | --- | --- | --- |
| Released StageAllocation | `(released_at - allocated_at)` in `ALLOCATION_NANOSECOND` | DIRECT, consuming Job and StageAttempt | One exclusive logical WorkerInstance allocation; includes pre-start waiting and time until authority release |
| Stage exact-cache reference | Source physical allocation duration in `ALLOCATION_NANOSECOND` | COUNTERFACTUAL, consuming Job | Versioned source-duration estimate; not observed work, guaranteed savings, or negative usage |
| Consumed TransferTicket | Exact successful payload size in `BYTE` | DIRECT, destination pin's Job and Attempt | Successful logical payload once per ticket; excludes wire framing, failed reads and repeated transport bytes |

Allocation time does not measure physical GPU utilization, device-count-weighted
GPU time, CPU process time, model residency or energy. In particular, a CPU mock
using a GPU-shaped catalog creates no `GPU_NANOSECOND` receipt. The allocation
unit applies to one WorkerInstance regardless of its DeviceSet size.

`allocation-occupancy-v1`, `cache-source-allocation-estimate-v1`, and
`completed-transfer-payload-v1` bind the receipt digest to their source identities
and measured or estimated quantities. Receipts have deterministic identities and
content-free digests. No Customer Content or object key enters these receipts.
Only positive durations are recorded; equal microsecond timestamps have zero
measurable occupancy at the authority clock's precision.

New transitions produce receipts after migration 00077. Earlier history is not
silently backfilled. Active allocations are unclosed intervals and are not yet
included in completed-interval summaries. A cost report must disclose its cutoff
and unresolved intervals.

Monetary valuation still requires an explicit immutable `CostModelRevision`.
Neither the lab's synthetic cache admission scores (`expected_saved_compute_minor`
and `carry_cost_minor`) nor a source-duration estimate is a measured monetary
benefit. Cache storage carry, residency, failed transfer bytes, CPU execution
telemetry and full operational cost reconciliation remain separate coverage gaps.
No runtime accounting or CPU mock result advances a Production Gate.

The physical/cache runtime regression also values every produced receipt through
the public ledger API using explicitly synthetic unit rates. It checks exact
valuation replay, separate direct/counterfactual totals, zero unvalued receipts,
and byte-for-byte unchanged customer Charge records. This verifies the producer
to valuation contract; it does not calibrate rates or close the omitted telemetry
dimensions above.
