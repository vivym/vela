# Durable assignment admission component evidence

Status: local CPU-only component, based on checkpoint `d63674f`, schema 90.
The component is not yet connected to StreamAgent or the production command.
Production Gates remain **0/9**. These results establish neither execution drain
nor permission to remove scratch files.

## Contract and validation

`FileAssignmentAdmission` binds a bounded journal to one Worker/member epoch and
three existing, trusted, non-overlapping state/input/output directories. It
validates the complete assignment, V2 signature, execution time, every member's
trusted Runtime binding and identity digest before advancing the Worker-wide
allocation watermark. Invalid or busy requests do not consume that watermark.
Runtime bindings come from trusted configuration; deriving them from an incoming
assignment would defeat this check.

The journal records the original Acquire ID, signed original authority and latest
renewal without delivery payloads, execution parameters or input URLs. An exact
INPUTS_PENDING assignment can resume after input failure or process restart.
Renewals must preserve execution identity, and older envelopes cannot replace a
recorded renewal. Before the first Runtime RPC, EnterRuntime durably records
RUNTIME_ENTERED and rechecks validity after obtaining the admission lock.
Reopening a RUNTIME_ENTERED record requires recovery, including for a newer
allocation. CLOSED is an execution-admission fence, not a stopped receipt.

CloseExecution accepts an expired signed historical identity under the same
bounded future-skew policy used by Begin. The initial tests reproduced a mismatch
where Begin accepted an authority issued 500 ms ahead under a 1 s allowance but
CloseExecution rejected it as stale. That local closure mismatch is repaired.
The read-only Control terminal-disposition query retains its separate zero-future
clock policy. No execution validity window is extended by closing local admission.

Closing admission does not release the active input writer handle. Release is
the caller's assertion that all resolver tasks and writable handles have exited;
WaitReleased only observes that assertion. The real-file test holds a descriptor
across closure, verifies late Runtime entry and new admission are blocked, then
finishes writing and closes it before releasing the slot. This component cannot
detect a caller that releases too early, and it does not join Runtime descendants.

Older records remain CLOSED in a bounded backlog across profile changes. Capacity
exhaustion applies backpressure without evicting records. There is no retirement
or record-removal API yet, so enabling this component alone would eventually stop
new work when that bound is reached.

File tests cover missing/partial/corrupt state, changed Worker/configuration,
unknown/duplicate/noncanonical JSON, reused scratch roots, overlapping roots,
symlinks, hardlinks, non-private files, writable roots, marker mismatches, FIFO
rejection without blocking, live root/lock/state replacement and changed mtime.
Binding and persistence failures remain sticky until close/reopen. A permissions
failure before the atomic state replacement does not admit a writer or advance
the recovered watermark. A subprocess cannot acquire an already held lock, and
an abrupt process exit after EnterRuntime leaves the durable recovery fence.

## Local checks

- `go test ./...`: PASS; changed stageworkeragent package 5.447 s.
- `go test -race ./internal/stageworkeragent ./internal/stageauthority ./internal/stageassignment`:
  PASS; stageworkeragent 10.887 s, stageassignment 2.011 s, authority cached.
- `make lint`: PASS, 0 issues.
- Linux arm64 test binary built with CGO disabled; all admission tests PASS in
  the existing `postgres:17-alpine` image, as UID/GID 65534, with no network, a
  read-only root filesystem and a disposable writable `/tmp`. No PostgreSQL
  service was started. The write-permission fault test ran rather than skipping.

This is process-restart and selected filesystem-fault coverage. Linux scratch
was tmpfs; it is not power-loss durability evidence. Disk-full, mid-rename/fsync
failure injection, cross-component Stop/Resolve/Prepare recovery and full
multi-node workloads have not been validated for this component. No protocol or
database migration changed in this increment.

## Acquire identity handoff

The follow-up to `3c12e18` gives each production Acquire request its canonical UUID
before Exchange. A returned assignment must echo exactly that ID, and
DiscoveryResult now returns it as AcquireCommandID alongside a cloned assignment.
The identity is the original transmitted command ID, not a later reconstruction.
Missing, malformed, nil, different and noncanonical response IDs reject. A direct
discovery-to-journal test verifies the recorded ID equals the actual request ID;
distinct polls use distinct IDs.

For this follow-up, `go test ./...` and `make lint` pass. Race checks of
stageworkeragent, stageworkertransport and the production Worker command pass
(12.722 s, cached, and 5.572 s respectively). The transport already preserves
canonical caller-selected request IDs; the existing production execution mock
now echoes the request ID as the real transport does.

## Required integration

Carry DiscoveryResult.AcquireCommandID through Run/startAndMonitor into the
durable gate. The existing RunAssignment API also needs explicit original lookup
evidence before it can use that gate. Wire Begin before any Resolve/download, EnterRuntime before Prepare,
and closure into Stop, failure and materialization, without holding the admission
mutex across downloads or Control RPCs. Preserve correct recovery on reattach and
provide trusted Runtime binding refresh as Fleet epochs change.

The [terminal retirement design](terminal-scratch-retirement-design-2026-09-05.md)
still requires signed Runtime floors, distinct FLOOR_INSTALLED and DRAINED
evidence, execution-specific drain that preserves the resident model, and a
crash-safe retirement journal before removing pending records or files. A signed
INPUTS_UNUSED disposition and CLOSED admission are insufficient by themselves.
