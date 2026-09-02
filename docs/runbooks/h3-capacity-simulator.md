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
