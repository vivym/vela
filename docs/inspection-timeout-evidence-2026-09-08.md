# Separate inspection semantics from deadline testing

Date: 2026-09-08. Baseline: `6e07a7d`. This is a test-only increment.

The prior [remote integration receipt](journal-remote-integration-evidence-2026-09-08.md)
retains a failed concurrent full race run: the `invalid` inspection case received
`context deadline exceeded` instead of its expected domain rejection. It exposed
neither an observed authority nor inspection evidence. The original test applied
a 30 ms deadline to all five fault scenarios, including immediate semantic
failures. The exact scheduling cause of that run was not instrumented.

The test now gives `error`, `unknown`, `both-known` and `invalid` a bounded five
seconds. Only the deliberately blocked `timeout` inspector retains 30 ms.
All assertions remain: no unproven envelope or inspection can escape; invalid
and ambiguous evidence must be rejected; the timeout must preserve
`context.DeadlineExceeded`; subsequent valid inspection must work without
changing backend history. No production timeout or runtime behavior changed.

```sh
go test -race ./internal/modelruntime \
  -run '^TestAllocationExecutionDiscoveryRejectsAmbiguousOrUnavailableObservations$' \
  -count=10 -v
```

Result: **PASS**, ten main runs and 50 fault subcases, 5.540 seconds, no skips or
race reports. [Raw output](evidence/inspection-timeout-2026-09-08/race-recheck.log)
SHA256: `ebeaba0c67c1a6559dd6cda848d18a305af3bf70749973be4533c889c27062ed`.
The changed test file SHA256 is
`6a6750713805f17f52b614c23ccc4bff4b90025e4a4c2d6812440411b23c6d14`.

The broader [context validation](journal-context-evidence-2026-09-08.md) belongs
to `6e07a7d`, before this test-only edit. Its complete race run had already passed
with the old test. These repetitions validate the corrected test contract; they
do not prove a 30 ms production inspection SLA, resolve the historical
`11ce026` STALE campaign, or close the separate integration-tag lint backlog.
Production Gates remain **0/9**.
