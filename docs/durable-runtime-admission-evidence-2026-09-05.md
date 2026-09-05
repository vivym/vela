# Durable Runtime execution admission evidence

Follow-up: the [signed floor RPC](runtime-floor-rpc-evidence-2026-09-06.md) adds
private-socket delivery and explicit server assembly. The evidence below remains
the earlier journal checkpoint.

Status: local CPU-only increment over `ded07f4`, schema 90. An explicit
`ExecutionFloorConfig.State` persists the member-wide execution watermark and
signed floor. The default constructor still uses process-local admission; floor
RPC, default production assembly and automatic startup reconciliation remain
open. Production Gates remain **0/9**.

## Persistence contract

- Recovery is the default when State is configured. Missing state or lock files
  fail construction. `Initialize: true` is a separate first-bootstrap operation
  into an existing trusted, private, empty directory. An empty directory is not
  proof of a new Worker; callers must not infer initialization from missing files.
- One lifetime `flock` owns `execution-admission.lock`. Its independent 36-byte
  journal UUID must match `execution-admission.json`. The canonical JSON binds
  its schema, Worker/member/device topology, root and lock inode identities,
  highest allocation sequence with its signed authority, and installed floor
  with its signed terminal disposition. The authority witness is limited to
  64 KiB and the complete JSON to 5 MiB.
- Scope includes Worker/member epochs, devices, membership, member identity and
  device-subset digests. It deliberately excludes individual resident profiles
  and local Runtime epochs so restrictions survive their replacement. Recovery
  verifies historical signatures and complete trusted Worker/member scope; it
  does not grant historical execution or prove replacement-process drain.
- A new Prepare durably consumes its sequence before backend entry. Backend
  failure cannot reuse it. Admission checks freshness after queued locks and
  filesystem checks, and again after persistence: if fsync consumes the remaining
  lifetime, the sequence remains consumed but the backend is not entered.
- A private exclusive temporary file is synced, renamed and followed by directory
  fsync. Publication verifies that the target is still the just-written inode.
  A floor installation reports `Durable=true` only after persistence succeeds.
  Recovery resyncs state, lock and directory before reopening admission, including
  when the previous process saw rename but not successful directory fsync.
- Every admission checks the bound directory, lock UUID/inode and state
  inode/content hash. I/O or binding failures are sticky and require recovery;
  restoring a pathname cannot clear them. Prepare, Start, Status, Seal and
  renewal fail closed. Exact historical cancellation remains available without
  installing a new renewal. Invalid or oversized authority is rejected before
  writing and does not poison a healthy journal.
- Readiness checks admission health before and after the backend probe. A failed
  journal or closed Service cannot advertise Ready. A healthy nonzero floor
  still allows readiness for allocations above the cutoff.
- Shutdown closes admission before closing backends. Failed backend shutdown
  retains the journal lock. Successful shutdown with admitted calls still in
  flight also retains it and reports incomplete shutdown; the last returning
  call releases it. This is process shutdown, not a Stage retirement API.
- Recovery removes only private single-link regular unpublished files tagged
  with the same journal UUID. Traversal is batched; unrelated files are retained
  and linked/nonregular candidate files are rejected.

`ValidateTerminalDispositionSignature` checks canonical shape and signature for
retained restrictions. `ValidateTerminalDispositionEnvelope` additionally retains
its strict observation/expiry checks. Neither grants deletion authority.

## Validation

The final source, including readiness admission checks, passed:

- `go test ./...`: PASS. The focused final `go test ./internal/modelruntime`
  completed in 6.467 s and was cached by the all-package run; stageworkeragent
  completed in 5.607 s and cmd/vela-model-runtime in 0.684 s.
- `go test -race ./internal/modelruntime ./internal/stageauthority ./internal/stageworkeragent ./internal/stageworkermembertransport ./internal/stageworkertransport ./cmd/vela-model-runtime`:
  PASS. Uncached packages completed in 17.336 s, 15.063 s and 4.215 s for
  modelruntime, stageworkeragent and cmd/vela-model-runtime respectively.
- `make lint`: PASS, 0 issues.
- Linux arm64 cross-build and execution as UID/GID 65534: PASS using the local
  image below, no network, read-only root, dropped capabilities and private
  test state under tmpfs. No PostgreSQL server was started.

```sh
env GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  go test -c -o /tmp/vela-runtime-state-linux.test ./internal/modelruntime
docker run --rm --network none --read-only --cap-drop ALL \
  --user 65534:65534 --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --mount type=bind,src=/tmp/vela-runtime-state-linux.test,dst=/runtime.test,readonly \
  --entrypoint /runtime.test \
  sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  -test.run '^(TestDurableExecutionState|TestExecutionFloor|TestModelRuntime(RechecksAuthority|QueuedRenewal))' \
  -test.count=1
```

Tests observe persisted watermarks while Prepare is still blocked, and restore
state after a subprocess calls `os.Exit(0)` without Release or Shutdown. They
cover expired historical proofs, replaced profiles/Runtime epochs, missing or
corrupt state, signature/watermark mismatch, lock UUID mismatch, copied roots,
symlinks, hardlinks, FIFOs, live replacement, non-root write failure, and sticky
failure after pathname restoration. Injected directory-fsync failures exercise
uncertain rename recovery; another hook replaces the published inode with the
same bytes. Manual-clock tests expire authority during fsync. Other regressions
cover shutdown with blocked admitted calls, readiness after state loss, oversized
authority, and recovery of 80 owned orphan files while retaining another owner.

## Evidence limits and next work

Persistent restrictions and `WaitAcceptedOperations` are not DRAINED. The latter
joins only calls registered at installation, not asynchronous backend tasks,
descendant writers, input resolvers or later cancellation. Stage retirement must
preserve resident models and cannot substitute Service.Shutdown for an
execution-specific drain proof. No scratch deletion or pending-record retirement
is authorized by this increment.

Recovery requires the verifier keys for retained signatures. The existing
`FileEpochStore` still initializes a missing epoch file; this increment does not
close every epoch/restart risk. Fsync calls, injected failures, tmpfs and process
exit do not prove real power-loss durability or protection from consistent
rollback by a trusted filesystem owner.

Remaining work includes signed floor RPC, complete trusted Fleet bindings and
default command assembly, Worker signed cutoff, read-only historical inspection,
execution-specific writer drain, the terminal retirement journal, safe pending
admission record reclamation and automatic startup reconciliation. No new
database integration/load campaign, GPU test or remote deployment was performed.
Earlier source-bound receipts retain their original scope.
