# Default Worker scratch retention

Local CPU-only containment over `223188d`, database schema 90. Production Gates
remain **0/9**. No GPU or remote deployment was used.

## Correctness Finding

The default Worker assembled `FilesystemScratchRetirer` directly into its
materialization path. After a confirmed COMMIT it deleted shared StageRun inputs
and the StageAttempt output; after confirmed SOURCE_LOST it deleted the old
output. Confirmation establishes that the source is dispensable, but neither
response proves that all local writers have exited or that delayed authority
cannot admit another writer. The signed floors and historical inspection added
so far do not supply that complete proof.

## Current Behavior

`vela-stage-worker-agent` now assembles `RetainScratchRetirer`. Both retirement
operations return `ErrScratchRetirementUnproven` without filesystem access. The
existing materialization path first persists the confirmed disposition, then
propagates that error without deleting its recovery record. It retains the exact
StageAuthority, local receipt, materialization authority, original command IDs,
object version and confirmation/source-loss evidence.

For COMMIT, `L2Published` remains true. `Committed` and `SourceLostReported` remain
false while local completion is pending, even though the journal can already
hold a confirmed Control disposition. These existing result fields therefore
must not be interpreted as a new rejection or rollback of the Control result.
Recovery reads the persisted disposition and retries retirement directly;
expired materialization authority does not cause resealing, republication or
another Control command for already confirmed work.

This is a deliberate availability restriction. `ProductionAgent.Run` retries
pending materialization with backoff before discovery, so one pending retirement
pauses subsequent Acquire calls on that Worker. Direct Stream admission also
rejects once its bounded materialization journal is full. There is no bypass
flag or expiry-based permission to delete retained state. This increment does
not establish sustained throughput or a global bound on scratch bytes, especially
for failed/canceled executions outside the materialization journal.

`FilesystemScratchRetirer` remains a destructive low-level primitive for the
existing filesystem and controlled fixture tests. It is no longer directly
assembled by a command. It still does not validate writer drain; any future live
use must be behind the complete durable retirement protocol. In particular,
`internal/integration/cpu_mock_load_campaign_test.go` and
`stage_stream_materialization_replay_test.go` explicitly configure that primitive;
their successful cleanup is not proof of the current default command's
throughput or adversarial writer exclusion. Historical source-bound load
receipts remain historical evidence.

## Validation

- Full `go test ./...`: PASS; this excludes build-tagged integration campaigns.
- `go test -race ./internal/stageworkeragent ./cmd/vela-stage-worker-agent`: PASS.
- `make lint`: PASS after correcting the new error string's capitalization.
- Linux arm64 non-root execution of the new retention regression: PASS.
- The command assembly smoke test requires the retaining policy and the explicit
  StageAttempt-owned output contract.

The regression exercises both COMMITTED and SOURCE_LOST with Control response
failure, confirmation persistence failure, subsequent durable confirmation,
two file-journal reopens, expired materialization authority and production retry.
It checks exact retained records, preserved input/output/neighbor files, no
repeated Seal/publication/confirmed Control command, full-journal admission
rejection and no production Acquire while retirement is pending. Journal reopen
is tested within one process; this is not an all-writer process-crash recovery
or durable drain test.

The Linux binary was built with
`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c` at
`/tmp/vela-scratch-retention-linux.test`. It ran under image
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`
with `--network none --read-only --cap-drop ALL --user 65534:65534`, private
`/tmp` tmpfs and the binary mounted read-only. Selection:
`^TestUnprovenScratchRetirementRetainsConfirmedHistoryAndBackpressure$`.

## Remaining Work

Complete the [terminal retirement protocol](terminal-scratch-retirement-design-2026-09-05.md):
execution-specific writer drain that preserves model residency; durable evidence
retained before Seal/reusable status forgets an execution; complete signed
Worker/Runtime admission floors; input-writer exclusion; bounded retirement
records; trusted first bootstrap and default assembly; crash recovery and pending
record reclamation. Restoring automatic cleanup and normal Worker progression
requires that proof. Changing the retaining policy alone is not closure.
