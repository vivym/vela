# Runtime image executable reader

This increment follows `8279aec`. The
[image-layer experiment](runtime-image-provenance-evidence-2026-09-07.md)
identified two incorrect executable identities from flattened OCI tar output.
Node now has a Linux library implementation that reads the trusted local
containerd's already-unpacked, read-only `native` image snapshot instead.
The same five real image/task cases consume this implementation and compare it
with independently expected bytes and the kernel executable observation.

## Implemented boundary

`DialRuntimeImageObserver` takes a trusted local socket, Node identity, explicit
containerd namespace and snapshotter. It shares the existing authenticated
local connection implementation: trusted root-owned socket/directory, kernel
peer UID, retained socket identity, bounded gRPC messages and direct host boot
ID. `RuntimeContainerObserver` retains its read-only container operations.
The separate image observer can create a temporary lease, snapshot view and
mount activation; it cannot pull/unpack an image or launch/stop a task.

`InspectExecutable` requires a single-platform manifest SHA-256, independent
config SHA-256 and canonical absolute image path from a verified release.
It performs the following bounded collection:

1. Create an unpredictable observation lease in the explicit namespace, with
   a one-hour GC expiration. Caller context metadata cannot select another
   namespace or attach the operation to another lease.
2. Retain manifest/config content, read contiguous bounded Content streams,
   and recompute SHA-256. Reject missing, oversized, truncated, overlapping,
   corrupt, duplicate-key, trailing or invalid UTF-8 JSON content. Each JSON
   blob is limited to 1 MiB; each image to 128 layers.
3. Require an OCI or Docker schema-2 image manifest with the exact config
   descriptor, supported layer descriptors and matching ordered DiffID count.
   Require Linux and the observer's architecture, without platform variants or
   OS-specific feature/version extensions. Indexes and artifact manifests do
   not substitute for the single-platform runtime image.
4. Compute OCI `identity.ChainID` from the config's ordered SHA-256 DiffIDs.
   Retain the committed snapshot and require its exact name, kind and parent.
   Create a read-only view and activate that exact view through containerd.
5. Validate the returned native bind description, activation identity and
   materialized mount path. Open the actual mount without host symlink
   traversal and require kernel `ST_RDONLY` on the retained view.
6. Resolve the executable with `openat2(RESOLVE_IN_ROOT |
   RESOLVE_NO_MAGICLINKS | RESOLVE_NO_XDEV)`. Absolute image symlinks resolve
   inside the retained image root. Nested mounts cannot redirect the read.
   `O_PATH` validates the inode before any data open; devices/FIFOs/directories,
   empty/non-executable files and files above 256 MiB are rejected. A fixed
   host procfs FD directory reopens only that retained regular-file descriptor.
7. Hash the file with bounded reads; recheck metadata, the current path's inode
   and kernel read-only state. Recheck activation, committed/view snapshot
   metadata, host boot ID, trusted socket and the 30-second collection interval.
8. Close descriptors, deactivate the exact mount, remove the temporary view,
   and synchronously delete the lease before returning success. Cleanup has
   its own ten-second context even after cancellation. Any error returns a
   zero observation. If deactivation/removal fails, later destructive cleanup
   stops and the lease is retained until explicit cleanup or its GC expiration;
   the error includes its observation ID.

The committed snapshot, image records, source content and unrelated resources
are never explicitly deleted by this reader. As with ordinary containerd lease
release, no-longer-referenced resources may become eligible for daemon GC.

The first implementation compared entire Activate/Info responses and rejected
healthy views. Inspection of containerd `v2.3.1` `putActiveMount` confirmed it
persists Type, MountPoint and MountedAt, but omits the active mount's Source,
Target and Options. The reader validates the full initial Activate response
against the snapshot mounts, then compares its documented persisted projection
against Info, including the complete System bind specification. It does not
discard mount-path/type/time or System changes to make the comparison pass.

## Trust and remaining work

This observation trusts the authenticated local containerd, its unpacker and
its committed snapshot storage to associate the ChainID with the materialized
rootfs. It does not independently reconstruct/re-hash the snapshot against all
layer bytes, detect privileged out-of-band snapshot tampering, or turn the
snapshot's name into a cryptographic rootfs proof. Release-layer stored hashes
and decoded DiffIDs remain the separate release validation boundary.

Only `native` is accepted. `overlayfs`, remote/lazy snapshotters, multi-platform
index selection and an observer outside the daemon's mount namespace are not
supported. Runtime/kernel qualification here is only the tested local CPU
environment; it is not production RKE2 qualification.

The result is an image file observation, not a container's effective writable
rootfs/configuration, dynamic library or loaded-memory attestation. It cannot
authenticate the executable that sent an earlier message across an ABA exec.
It is not a startup grant, durable incarnation ownership or retirement proof.
The Node serving endpoint and launch authorization assembly still do not call
this library. Observer-process death and subsequent lease-expiration/daemon-GC
recovery require a separate crash experiment; explicit error/cancel cleanup
alone does not establish that recovery property. The subsequent
[crash-recovery increment](runtime-image-crash-recovery-evidence-2026-09-07.md)
adds actual process-death evidence and explicit expired-observation recovery;
expiry alone is not a collection deadline, and deployed periodic integration
and daemon/host restart remain open.

## Validation

The ordinary Linux CPU wrapper now requires 19 named top-level tests to pass
and rejects every skip. It passes in `18.45s`; the expanded five-image test
takes `11.04s`. The five native snapshot observations all match the expected
kernel executable bytes, preserving the earlier whiteout and hardlink
counterexamples against the pinned generic tar flattener.

The new fault cases inject missing file, mismatched config, corrupt content,
lost lease/view/activation replies after real successful RPCs, cancellation
after real activation and during cleanup, changed activation metadata and
deactivation failure.
Every rejection returns zero evidence. Containerd lease, snapshot and mount
inventories prove explicit cleanup, including cleanup with a fresh context
after cancellation. The deactivation-failure case checks that the lease still
protects content/snapshots, then explicitly retries cleanup. Two concurrent
observations through one authenticated client return the expected digest and
leave no observation resources behind.

Separate tests exercise bounded/contiguous content streams and image path
resolution. A real read-only Linux bind view accepts relative/absolute image
symlinks and rejects a host-path symlink, loop, nested mount, empty file,
non-executable file, FIFO, device, oversized sparse file and missing path.
Writable views are rejected before hashing.

Node and Node-command unit tests pass. Linux integration-tag Node lint reports
`0 issues`, and tagged Node/Node-command vet passes. All 19 selected Linux
static race tests also pass without skips or data races; the five-image test
takes `56.94s`, including concurrent observations and the real cleanup faults.
The race binary uses the existing builder image
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`
and the [static build recipe](node-runtime-caller-evidence-2026-09-06.md#validation),
with all 19 names in `TestRuntimeContainerdSandbox` selected. Its known static
glibc NSS linker warnings do not establish DNS/user-database compatibility.

One final-code race attempt was interrupted by local disk exhaustion: the
macOS data volume reached 118 MiB free, nested containerd content sync returned
`input/output error`, and Docker's metadata database also returned an I/O
error. That run is failed environment evidence, not a passing test result.
The already-authorized `go clean -cache` removed only rebuildable Go cache
(about 29 GiB); the module cache, repository and pre-existing stopped
containers were preserved. Docker Desktop required a local restart after the
failed test process had exited. Normal shutdown timed out; only the inventoried
Docker application/backend processes were terminated, without deleting its
disk/images/volumes. Restart recovered the daemon and its two original stopped
containers; the failed ephemeral test container was gone. Free space recovered
to about 46 GiB. The same final-code race binary was then rerun successfully.
The failure log is retained as
`/tmp/vela-runtime-image-reader-race-disk-failure.log`.

The CPU fixture remains Docker `28.3.2`, containerd `v2.3.1` revision
`64b425cf570b3b8dd1d4cc46da7c1fce65c6651a`, runc `1.4.2`, Linux arm64. It uses
the existing digest-pinned sandbox image
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`,
`--pull never`, no network, no host runtime socket, private cgroups, two CPUs,
1 GiB memory and 256 PIDs. The disposable daemon/mount fixture is privileged;
this does not validate a production Node privilege profile. No GPU, model
weight, dependency upgrade, registry publication or remote deployment is used.

Reproduce the ordinary selection with:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

CPU and lint logs are `/tmp/vela-runtime-image-reader-cpu.log` and
`/tmp/vela-runtime-image-reader-lint.log`; race output is
`/tmp/vela-runtime-image-reader-race.log` and the corresponding `-race-build.log`.
The disposable containers, daemon trees and static race binary are removed;
the two pre-existing stopped PostgreSQL/Go-builder containers are preserved.
No full-repository unit/integration campaign is claimed for this increment.
PostgreSQL 94, Worker journal 5,
Runtime journal 6, Registry binding 1 and Production Gates `0/9` are unchanged.
`UNRESOLVED` / `LEGACY_UNKNOWN` startup remains recovery-only.
