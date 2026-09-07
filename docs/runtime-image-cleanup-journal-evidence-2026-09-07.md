# Durable image cleanup journal and kernel completion

This increment follows `664bb41`. A real busy-mount experiment disproved the
previous assumption that `Deactivate(NotFound)` plus synchronous lease deletion
was sufficient evidence of completed image-observation cleanup.

## Reproduced failure

`TestRuntimeImageBusyRecovery` retains an open file on an expired observation's
actual native mount. The first recovery attempts a real kernel unmount and
returns `EBUSY`; containerd has already removed the activation metadata.
The second recovery previously returned **`1, nil`**, deleted the view and
lease, and left the busy kernel mount behind. The pre-fix test exited nonzero
with `retry reported recovery despite a still-busy kernel mount: 1 <nil>`.
The private daemon logged `failed to finish collection context` with
`device or resource busy` for that same mount.

The pinned upstream source explains both parts of the failure:

- [`core/mount/manager/manager.go`](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/core/mount/manager/manager.go)
  commits activation deletion before unmounting. Its orphan cleanup also
  commits metadata changes before filesystem cleanup.
- [`core/metadata/gc.go`](https://github.com/containerd/containerd/blob/64b425cf570b3b8dd1d4cc46da7c1fce65c6651a/core/metadata/gc.go)
  logs collector `Finish` errors without returning them to the synchronous
  lease-delete caller. A zero RPC result is therefore not an unmount receipt.

## Correctness change

New observations use lease protocol **v2**. The one-hour deadline is stored in
`vela.ai/runtime-image-expires`; they no longer set `containerd.io/gc.expire`.
Containerd GC must retain the lease, snapshot view and cleanup journal until
Vela verifies the mount has disappeared. Periodic independent maintenance owns
expiry-driven recovery. The 30-second observation bound, cleanup deadlines and
32-record recovery batch bound remain unchanged.

The native view's persistent labels record the Node boot ID and an allocation
state. `VIEW` is written atomically with view creation. `ACTIVATING` is confirmed
before the mount RPC; `MOUNTED` and the exact mount path are confirmed afterward,
before producing an executable observation. When recovering an activation that
was acknowledged by containerd but not yet recorded locally, the Node first
persists its path from the authenticated mount API, then attempts deactivation.

| Retained evidence | Cleanup decision |
| --- | --- |
| View and activation both absent | No remaining allocation in the retained v2 protocol |
| `VIEW`, activation absent | The producer had not issued activation |
| `ACTIVATING`, activation present | Persist its path before deactivation |
| `ACTIVATING`, activation absent, same host boot | Return unresolved and retain the view/lease |
| Activation absent after an independently changed host boot ID | Old kernel mounts cannot have survived that boot |
| `MOUNTED`, activation metadata absent | Trigger orphan GC with an empty bounded lease, then inspect the kernel mount table |
| Invalid journal or changed activation identity | Fail before cleanup mutation |

Mount metadata is never treated as a kernel completion receipt. The observer
reads `/proc/self/mountinfo` through the existing pinned `prometheus/procfs`
parser and checks the exact recorded mountpoint. Kernel whitespace/backslash
escaping is preserved in the comparison. A mount still present after GC yields
an error and retains the durable view/lease. Once it is absent, removal proceeds
in order: view, then lease. The code never directly unmounts a caller path or
adds a Node capability; containerd remains the mount actuator.

The GC trigger is an empty, independently named lease with a one-minute
containerd expiry. Its uncertain response cannot discard the observation's
journal. Garbage collection may still log an orphan-unmount failure, but Vela
now independently checks the resulting kernel state before claiming completion.

Marked v1 leases are recognized as incompatible with this protection protocol
and fail closed for operator reconciliation. They are not silently rewritten
or counted as recovered. Unmarked legacy resources remain outside this API's
ownership. No PostgreSQL/Worker/Runtime/Registry/release schema version changes.

## Validation

The corrected busy-mount test passes: the first and second attempts both return
errors with zero completed recoveries while the file remains open. The lease
and view survive even though activation metadata is already gone. After the
file is closed, a new recovery removes the orphan mount and completes once.
The independent live control and source image remain valid.

The real image-layer campaign now injects a lost committed journal response
and a blocked journal write. Neither produces usable image evidence. A blocked
write or inconsistent activation response retains the resources until a fresh,
consistent observer completes recovery. Concurrent observations still use
distinct lease/view/journal identities.

The state-reset harness now separates two cases. A recorded mount path allows
recovery after complete seed-sandbox destruction. If the observer is killed
between the mount RPC and journal commit and the state is then destroyed,
the unchanged real host boot ID cannot prove absence of that unrecorded mount;
recovery reports unresolved and preserves its durable resources. The outer
test knows the sandbox was destroyed, but that fact is not an input to the
production API and is not substituted for missing production evidence.

The complete ordinary CPU wrapper passed in **147.244 seconds** with all
**37 required main tests** and both state-reset cases, without selected skips.
The main-contract subtest took 107.72s; recorded/unresolved state-reset subtests
took 16.25s and 16.96s. Ownership, invalid journal, missing activation, partial
progress, and batch tests pass. A real bind-mount test covers space, tab,
newline and backslash in the mountpoint and verifies absence after unmount.

The environment remains Linux/arm64, Docker `28.3.2`, containerd `v2.3.1`
revision `64b425cf570b3b8dd1d4cc46da7c1fce65c6651a`, runc `1.4.2` and sandbox image
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`.
All disposable sandboxes have private runtime state, no network or host runtime
socket, and use no GPU, model weights or production data.

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

Full repository `go test -p 2 ./...`, `go vet ./...` and pinned
`golangci-lint v2.13.1` pass; lint reports zero issues. Integration-tag vet and
lint also pass for the Linux/arm64 Node Agent library and command. The
Linux/amd64 Node Agent command builds successfully. `go mod tidy -diff` and
`git diff --check` are clean; the existing `prometheus/procfs v0.21.1` is now a
direct dependency without a version change.

Final-source Linux/arm64 static race binaries were built for both the test
package and actual Node Agent command using builder
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`
with `-race -ldflags '-linkmode external -extldflags=-static' -p=2`.
They ran in the same pinned, network-disabled CPU sandbox with the two binaries
mounted read-only, a 180-second test deadline and the same resource bounds.
All 36 main tests other than `TestRuntimeImageLayerExecutableIdentity` passed
together. Its five scenarios ran separately to bound the instrumented image
payload workload:

| Race image scenario | Reported subtest time | Result |
| --- | --- | --- |
| `regular-overlay` | 17.02s | PASS |
| `whiteout-recreate` | 16.37s | PASS |
| `opaque-directory` | 15.20s | PASS |
| `hardlink-copy-up` | 16.89s | PASS |
| `relative-symlink` | 17.06s | PASS |

Each race sandbox exited zero, every selected test passed, and none reported a
skip or data race. The two state-reset scenarios ran in the ordinary wrapper;
no state-reset race execution is claimed. The static-glibc NSS linker warnings
and read-only module stat-cache warning did not prevent the successful builds;
these test artifacts do not qualify production static linking. All six race
sandboxes were removed on exit. The two pre-existing stopped PostgreSQL and Go
builder containers were preserved.

## Remaining scope

Actual daemon death inside its mount transaction, arbitrary loss of the mount
database while unrelated mounts survive, real host reboot/power loss, and
effective systemd operation remain unqualified. An unrecorded same-boot
activation loss intentionally remains unresolved; automatic resolution needs
additional independently trusted containment evidence. A persistent failure
can still stop a recovery pass and requires operator attention.

This journal owns image-observation cleanup only. It grants no startup,
readiness, execution, scratch reset or incarnation retirement authority.
Effective Runtime configuration/message binding, durable incarnation ownership
and independent retirement remain open. `UNRESOLVED` and `LEGACY_UNKNOWN` remain
recovery-only. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry
binding 1, release bundle schema 3 and Production Gates **0/9** remain unchanged.

The subsequent [daemon mount-table increment](runtime-image-daemon-mount-evidence-2026-09-07.md)
reproduces a false completion when the maintenance process hides the mount in
its own namespace. It replaces the caller-local mount-table check with a read
from the live, kernel-pinned containerd peer and adds a capability-free private
namespace experiment. This document's original caller-local check is superseded.
