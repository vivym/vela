# Terminal live recovery and Runtime slot reuse

This CPU-only increment follows `c569441`. PostgreSQL schema stays 94, Worker
journal 5, Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Problem and behavior

Before this change, terminal retirement only collected existing drain and
non-admission proofs. An admitted live backend without a checkpoint left
retirement at INTENT even when the latest allocation grant could discover its
actual backend envelope. Two-member tests reproduced this after renewal failure
before application on one member and response loss after application on another.
Both complete and partial scenarios failed with `member execution exclusion is
unproven`. A separate regression reproduced readiness returning true while a
terminal CANCELING execution still occupied the shared Runtime slot.

The retirement coordinator now enables live recovery only after input writers
finish and every Runtime floor acknowledgement is validated. Existing read-only
and checkpoint-only collectors keep their prior semantics. Recovery first reads
durable proof; when non-admission cannot be proven, the original current Runtime
discovers the exact live envelope, cancels unless already STOPPED or OUTPUT_SEALED,
and drains that exact identity. The collector validates every reply and re-reads
the persisted allocation proof with its original query. Query and observed
authority digests are never rewritten to match one another.

After exact drain persistence, Runtime can reconcile CANCELING or unclassified
FAILED only through a valid exact STOPPED observation. An explicit
`WorkerReusable=false` survives cancellation and cleanup; a validated Status is
required to clear that denial. Inspection alone is not drain or permission to
release the slot. Known sealed output preserves its validated local receipt
before releasing its active identity. Terminal executions at or below the floor
block readiness across all shared resident profiles while they remain unusable.

A persisted checkpoint followed by a failed stop observation stays retryable.
The coordinator retries exact drain at the original current Runtime, which
reuses the checkpoint and retries only slot reconciliation. Replacement owners
can replay historical evidence without entering a backend for an old epoch.
Partial collections retain INTENT and scratch; complete proof still passes the
existing durable READY and RETIRED transitions.

## Validation

- Two-member UDS recovery handles different actual renewal envelopes, partial
  drain failure and a lost stop observation after checkpoint persistence. Retry
  preserves exact member checkpoints, avoids repeated completed backend drain
  or cancellation, retains resident models and resumes completed Worker proof
  offline after reopening its journal.
- Five negative stop-observation cases cover backend error, unknown, PREPARED,
  malformed and timeout results. Persisted drain alone cannot restore readiness
  or admit the next execution; a subsequent valid observation restores reuse.
- FAILED -> Cancel -> Drain is tested with both reusable and explicitly unhealthy
  status. Scratch writer drain may complete while readiness and execution reuse
  remain blocked for an unhealthy Worker.
- The older expired-cancellation RPC test assumed no stop observation could
  restore reuse. It now requires real drain followed by exact STOPPED inspection
  and successful next Prepare; cancel acknowledgement alone still produces no
  checkpoint. The negative inspection tests establish the new boundary.
- Actual compiled CPU `h3-encoder` tests cover both renewal outcomes and now
  require the resident process to accept the next allocation after recovery.
- Full `go test ./...` passes. ModelRuntime and Worker Agent race suites pass
  in `65.307s` and `68.357s`. Related Runtime transport, member transport and Worker
  command race suites pass. An initial race invocation also named nonexistent
  `internal/stagememberagent`; the real `internal/stageworkermembertransport`
  package was run separately and passed.
- Four PostgreSQL integrations pass in `23.495s`: authenticated terminal
  disposition, replacement-Runtime terminal recovery, latest-renewal lease
  cancellation/expiry, and compiled Worker bootstrap binding.
- `make lint` reports zero issues after two staticcheck style corrections;
  `make verify-generated` and `git diff --check` pass.
- Linux arm64 Runtime and Worker tests pass using the cross-built
  `/tmp/vela-terminal-live-runtime-linux.test` and
  `/tmp/vela-terminal-live-worker-linux.test`, with the compiled H3 command in
  `/tmp/vela-renewal-recovery-linux`. The container image is
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
  Both containers run as UID/GID 65534 with no network or capabilities,
  read-only root/binaries, private tmpfs and `no-new-privileges`.

## Automatic recovery follow-up

After committing the implementation as `0811ed5`, a separate test-only increment
exercises ProductionAgent -> StreamAgent -> terminal retirement -> actual Runtime
UDS. It covers both renewal outcomes and six scenarios each: normal recovery,
malformed discovery correlation, malformed cancellation identity, malformed drain
checkpoint, lost drain response after persistence and failed stop observation
after durable drain. The Control service and terminal history reader are test
doubles; this is not a PostgreSQL-backed full Job execution receipt.

All twelve scenarios validate latest-grant/original-Acquire query identity,
capacity withdrawal before history, zero capacity and preserved scratch during
INTENT, restored capacity and Acquire only after RETIRED, and exactly one physical
cancel/drain. After Acquire returns the fixture's NoWork response, the test passes
a new allocation through the real Worker admission gate and Runtime Prepare to
verify that the advertised slot is usable. The original actual signed backend
checkpoint remains exact after that next Prepare. No materialization I/O is
performed or inferred from this terminal cleanup.

Three further counterexamples prepare live backends but leave an active input
writer, unknown input-writer history, or one lost floor acknowledgement. All
retain INTENT/scratch with zero backend inspect/cancel/drain calls. Completing
the active writer or recovering the lost floor reply permits a successful retry;
unknown writer history remains blocked.

The added tests and existing ProductionAgent terminal recovery regressions pass
under the race detector (`9.141s`) and in the same restricted Linux arm64 image,
using `/tmp/vela-terminal-live-production-linux.test`. Lint and diff checks pass.
The implementation and generated contracts are unchanged from `0811ed5`.

## Limits and continuation

The new live path is exercised through the retirement coordinator and actual
Runtime UDS, including the automatic ProductionAgent scenarios above. This does
not establish every late-reply or distributed failure interleaving. Candidate
identities remain in memory; restart cannot infer unknown historical
writers from an empty backend. Durable renewal candidate restoration, physical
containment, sealed receipt persistence, bounded history reclamation and default
Fleet durable activation remain open. No remote deployment, push, GPU execution
or Production Gate acceptance occurred. The overall correctness goal remains
incomplete.
