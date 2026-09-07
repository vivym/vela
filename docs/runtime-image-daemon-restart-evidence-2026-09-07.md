# Containerd restart recovery and socket inode lifetime

This increment follows `5bdb81d`. A real graceful containerd restart reused its
previous filesystem socket inode in the CPU sandbox. The observer retained only
an `os.FileInfo` containing device/inode numbers, so `os.SameFile` could not
distinguish that replacement. A root-owned socket and a root kernel peer alone
do not establish that a connection belongs to the original daemon endpoint.

## Correctness change

The shared CRI/image observer now opens the validated socket with Linux
`O_PATH | O_NOFOLLOW | O_CLOEXEC`. It requires the descriptor's `fstat` identity
to match the previously validated socket before dialing, and retains the
descriptor for the observer lifetime. The filesystem cannot reuse a referenced
inode after unlink; all subsequent path checks therefore distinguish a newly
created socket. Failed setup and explicit `Close` release both the socket pin
and the trusted directory handle.

Existing directory, owner, mode, `SO_PEERCRED`, host boot and pre/post-query
checks remain in force. A reconnect through a changed pathname fails closed.
Recovery requires a freshly authenticated observer, as used by the independent
maintenance command. This does not authenticate a malicious root operator,
attest loaded process memory, or turn a CRI observation into startup authority.

Darwin cannot open a Unix socket with an equivalent `O_EVTONLY` inode pin; the
kernel returns `operation not supported on socket`. The host observation entry
therefore explicitly requires Linux. The four CRI conformance tests previously
using the Darwin mock are compiled on Linux and included in the mandatory CPU
selection, alongside the actual host-boot/peer tests and compiled CLI. A Darwin
test requires an explicit unsupported-platform error. No permissive pinning
fallback or test-only production switch was added.

## Restart experiment

The disposable fixture can now stop and restart its exact private containerd
process while retaining the original root/state directories and namespace.
Cleanup always stops the current process. The test verifies a graceful zero
exit for SIGTERM and the actual SIGKILL exit signal for a crash; a stop timeout
is a failed experiment, not evidence of a successful restart.

For each mode, `TestRuntimeImageDaemonRestart`:

1. Imports a minimal CPU image and retains a separate non-expiring live control
   view. An observer child creates a lease, native view and kernel mount, then
   dies by SIGKILL after acknowledging the completed allocation.
2. Confirms all observation resources exist and cannot be reclaimed before
   expiry. The private fixture validates the requested production one-hour TTL,
   then substitutes 15 seconds to include the daemon downtime. Other crash
   cases continue using six seconds; production timing is unchanged.
3. Stops containerd, requires the retained mount's executable to remain readable,
   and requires a bounded offline recovery attempt to fail with zero completed
   cleanups. It starts a new daemon on the same root/state directories.
4. Requires a different socket inode while the old pin is retained, rejection by
   the old observer, and successful authentication by a new observer. The new
   observer still refuses to collect the unexpired resources.
5. Forces GC before expiry, verifies protection, then waits past expiry while
   the daemon is idle and confirms the abandoned resources still exist.
6. Runs the actual Node Agent maintenance command with no Linux capabilities
   and `NoNewPrivs=1`. It requires one completed recovery and clean SIGTERM exit.
7. Checks absence of lease, snapshot view, mount path and corresponding kernel
   mount, unchanged live-control bytes, and a successful new source-image
   observation.

The final ordinary CPU run observed graceful daemon PIDs `1268 -> 1314` and forced
restart PIDs `1314 -> 1348`; both old observers were rejected and fresh observers
authenticated. The two cases passed in 14.91 and 14.99 seconds respectively.
These PIDs are local fixture observations, not durable process identities.

`TestRuntimeContainerObserverPinsSocketLifetime` additionally requires exactly
one descriptor referencing the original socket inode while the observer lives.
Eight unlink/rebind cycles must not reuse that inode or produce an observation.
Closing twice leaves zero matching descriptors. Repeated failed boot validation
and a failed connection deadline also leave zero socket pins.

## Verification

The pinned sandbox uses Linux/arm64, Docker `28.3.2`, containerd `v2.3.1` revision
`64b425cf570b3b8dd1d4cc46da7c1fce65c6651a`, and runc `1.4.2`. No GPU, network or
host runtime socket is used. The existing wrapper now requires **33 top-level
tests**, including every migrated CRI test, to pass without skips and verifies
the container exit status:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

All 33 passed in **94.400 seconds**, including fixture build and cleanup.
Full `go test -p 2 ./...` and `go vet ./...` passed. Full repository lint and
Linux/arm64 integration-tag lint for the Node Agent library and command both
passed with zero issues; the command also cross-built for Linux/amd64. Node Agent
library and command race tests passed on macOS; Linux-specific behavior is
proved by the sandbox, not those host tests.

The static Linux race campaign uses the same final source and Go `1.26.7`
builder image
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`.
Both the test executable and actual Node Agent command use
`-race -ldflags '-linkmode external -extldflags=-static' -p=2`; the test build
also uses `-tags=integration -c`. Source and module-cache mounts are read-only,
`GOCACHE` is a disposable output cache, and `GOTOOLCHAIN=local` prevents toolchain
downloads. The existing static-glibc NSS linker warnings do not qualify DNS,
user-database lookup or production static linking.

Two combined race attempts reached the 180-second suite deadline. The first
stack was inside Go fixture gzip compression; fixture generation now writes
the same tar payload through the sandbox's `gzip -1 --stdout` before passing
the compressed file to `tarball.LayerFromFile`. The second attempt progressed
through the restart cases and timed out in the comparison flattener's gzip
decompression. Neither incomplete run is counted as a successful campaign.
No production timeout, lease TTL or image assertion was relaxed.

The final race selection is split into one invocation for the other 32 wrapper
names and five invocations of `TestRuntimeImageLayerExecutableIdentity`, each
selecting one existing subtest with
`-test.run='^TestRuntimeImageLayerExecutableIdentity$/^SCENARIO$'`. Every run
uses a fresh disposable sandbox, the same two read-only binaries, the wrapper's
two environment variables, the same resource limits and a 180-second test
deadline. The 32-test invocation passed without skips or data races; daemon
restart passed in 34.02 seconds (`daemon-term` 16.90s, `daemon-kill` 16.00s).
All five image-layer invocations passed, including every nested failure and
concurrency assertion in `regular-overlay`, without skips or data races:

| Selected layer scenario | Complete test time, including fixture cleanup |
| --- | --- |
| `regular-overlay` | 21.72s |
| `whiteout-recreate` | 21.25s |
| `opaque-directory` | 21.22s |
| `hardlink-copy-up` | 21.13s |
| `relative-symlink` | 21.72s |

Every one of the six final race invocations exited zero. Together they cover
all 33 required top-level tests and all five layer scenarios. This is a split
race campaign, not a successful single 180-second invocation.

All disposable sandbox containers have exited and been removed. The 562 MiB
temporary race binaries/cache were moved to the local Trash for reversible
cleanup; the pre-existing stopped PostgreSQL and Go-builder containers remain.

## Remaining scope

This experiment restarts containerd after a fully acknowledged mount allocation.
It does not inject death inside containerd's mount syscall/metadata transaction,
delete the volatile state directory, reboot the host, run systemd as PID 1 or
qualify overlayfs. Those failures need separate experiments and invariants;
same-directory process recovery is not proof of reboot or power-loss recovery.

Effective configuration/message binding, durable incarnation ownership and
independent retirement remain open. `UNRESOLVED` and `LEGACY_UNKNOWN` remain
recovery-only. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry
binding 1, release bundle schema 3 and Production Gates **0/9** are unchanged.

The subsequent [volatile state reset experiment](runtime-image-state-reset-evidence-2026-09-07.md)
destroys the entire seed sandbox and attaches its persistent root to a fresh
sandbox. It covers loss of state and the old mount context; physical host reboot
and mount-metadata loss with surviving mounts remain separate requirements.
