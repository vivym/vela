# H3 Capacity Simulator Runbook

This runbook operates the repository-only capacity decision aid. It does not
benchmark H3, modify Fleet desired state, call Kubernetes, create a Launch
Receipt, or advance a Production Gate.

## Build and validate

Run from the repository root:

```bash
go build -o ./bin/vela-capacity-sim ./cmd/vela-capacity-sim

./bin/vela-capacity-sim validate \
  --scenario examples/capacitysim/h3-synthetic/scenario.json \
  --trace examples/capacitysim/h3-synthetic/trace.ndjson \
  --calibration examples/capacitysim/h3-synthetic/calibration.json
```

The checked-in example is deliberately classified `SYNTHETIC` or `ASSUMED`.
Its numbers exercise Encoder, single-GPU DiT, VAE Decoder, cross-node transfer,
Project-scoped exact cache, fixed customer price comparison, and advisory warm
residency output. They are not measurements or recommended production counts.

## Model scope and accounting

The current algorithm is `capacity-sim-v2`, with scheduler
`reserved-service-v1`. Previous v1 receipts require the previous executable;
the new executable rejects that revision instead of replaying it with changed
semantics. Schema version remains 1.

The scheduler model gives retries priority, then chooses the Organization with
the least reserved service and stable arrival/identity ties. Service is charged
at dispatch, including failed attempts and the part inside the observation
window. This is an equal-share synthetic model. It does not replay production
Organization/ServiceClass/Project deficit snapshots, protected lanes, score
terms, domain-dependent placement, or correlated node failure. Those limits
are carried in the receipt's `validation.unsupported_inputs`; advisory proposals
also carry `SIMPLIFIED_SCHEDULER_MODEL`.

Queue waiting starts after transfer completes. Transfer includes connector
concurrency waiting and `ceil(bytes * 1e9 / bytes_per_second)` payload time.
Sensitivity factors remain rational through this conversion, including 0.5
bytes/s. At equal timestamps the order is expiry, finalization, residency,
stage completion, arrival, transfer completion, Stage READY, retry READY.
Completed Jobs therefore release Admission slots before equal-time arrivals;
expiry wins an exact-deadline completion race.
Workers remain occupied through service, seal, and materialization; expiry
removes queued work, while an already running attempt occupies its worker until
its scheduled completion. Busy/resource cost is clipped to the observation
window. Percentiles are nearest-rank; stage service samples are predicted full
attempt durations, whereas completed Job latency excludes unfinished Jobs.

Resource-time and byte-time products accumulate exactly before unit conversion
and integer flooring. Unrepresentable resource-time, throughput, or cost fails
explicitly. Cache pins protect both produced and reused input objects until Job
termination; buffer credit is consumed once per dependent Stage, including
retries. TTL prevents hits; physical cache eviction is lazy and pressure-driven.
Object-operation and warm-up costs are not modeled. Nonzero object-operation
rates and non-unit Job resource multipliers are rejected, and the synthetic v2
fixture explicitly uses zero object-operation rates.

Fairness share error compares all admitted Organization cohorts to an equal
share over the whole window; it is not a demand-normalized production fairness
estimate. Maximum waiting includes queued work at expiry and window end.
Observed calibration with no predicted samples returns
`NO_PREDICTED_SAMPLES`, with no fabricated zero-duration prediction.

## Produce replay evidence

Create outputs outside the input directory. The CLI rejects input overwrite and
writes receipt/proposal files atomically with owner-only permissions.

```bash
output_dir="$(mktemp -d)"

./bin/vela-capacity-sim run \
  --scenario examples/capacitysim/h3-synthetic/scenario.json \
  --trace examples/capacitysim/h3-synthetic/trace.ndjson \
  --calibration examples/capacitysim/h3-synthetic/calibration.json \
  --out "${output_dir}/receipt.json" \
  --proposal-out "${output_dir}/proposal.json"
```

Retain the exact three input files with `receipt.json`. Re-running identical
bytes with the same algorithm revision and seed must produce identical receipt
bytes and digest. `proposal.json` always has `auto_apply=false`; approval and
conversion into Fleet inputs are separate operator-controlled operations.

## Compare scenarios

Run a candidate using a separately versioned scenario or calibration bundle,
then compare immutable receipts:

```bash
./bin/vela-capacity-sim compare \
  --baseline "${output_dir}/receipt.json" \
  --candidate "${output_dir}/candidate-receipt.json" \
  --out "${output_dir}/comparison.json"
```

The comparison reports metric deltas and preserves source classifications. It
does not choose a winner or apply a ResidencyPlan.

## Evidence rules

- Do not relabel `SYNTHETIC`, `ASSUMED`, or `DERIVED` inputs as `MEASURED`.
- Do not publish calibration without an exact stage/profile/request-cohort
  model and held-out error report.
- Inspect per-stage p50/p95/p99 error; aggregate agreement cannot hide a stage
  mismatch.
- Run the mandatory transfer 0.5x/1x/2x/outage sensitivity even when compute is
  expected to dominate.
- Treat `ResidencyProposal` as advisory. Never feed it directly to Fleet
  `Apply`; Fleet proposal recording, human approval, actuation, drain, warm-up,
  canary, rollback, and observed-result receipts remain separate authorities.
- Preserve healthy resident models during routine scheduling and rollback.

## Failure handling

Validation or simulation failure is fail-closed. Do not remove bounds, invent a
missing calibration model, suppress a source classification, or use an invalid
receipt for planning. Correct the versioned input, retain the rejected bytes and
error, and run again under a new input digest.
