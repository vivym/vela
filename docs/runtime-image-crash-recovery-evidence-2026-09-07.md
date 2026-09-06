# Runtime image observer crash recovery

This increment follows `87d3204`. It closes a concrete gap in the image
observer's recovery mechanism: a containerd lease expiration makes resources
eligible for GC, but does not itself schedule a collection. An idle daemon can
retain an expired observation's lease, snapshot view and mounted rootfs.

## Result and implementation

The pinned containerd `v2.3.1` metadata collector's `scanRoots` stops marking a
lease after its `containerd.io/gc.expire` timestamp. Its GC scheduler skips a
scheduled collection when there is no new trigger, deletion or sufficient
mutation activity. Therefore the reader's one-hour expiration is not a
one-hour reclamation deadline. A later ordinary synchronous lease deletion can
trigger GC, but a restarted idle Node must not depend on unrelated activity.

`RuntimeImageObserver.RecoverExpired` now provides that explicit recovery
operation. New observation leases carry version `v1`, the SHA-256 of the trusted
Node identity, and the qualified snapshotter name in addition to the existing
one-hour expiry. Recovery:

1. Uses the same authenticated local containerd connection and explicit
   namespace, discarding caller-supplied namespace/lease metadata.
2. Lists leases by all three ownership labels. It validates the returned
   canonical random UUID names, complete ownership labels, creation timestamp,
   canonical expiry, bounded lifetime and absence of duplicate identities
   before issuing any cleanup operation. Unexpected ownership or malformed
   records return an error; a name prefix alone grants no cleanup permission.
3. Selects only leases already expired when the call started. It processes at
   most 32 records per call in stable order, within a 30-second call deadline.
4. Deactivates the exact mount, removes the exact native snapshot view, then
   synchronously deletes the lease. Any missing resource is already clean.
   An earlier failure stops later cleanup and reports an error; the return
   count preserves the number of completed cleanups before a later failure.
5. Rechecks the trusted connection and host boot identity. Repeated calls are
   idempotent. Concurrent GC may already have removed a selected record;
   successful cleanup confirms its absence, not exclusive attribution for
   physically removing it.

Unmarked legacy records are not explicitly selected. As with ordinary
containerd lease release, synchronous deletion invokes the daemon's normal GC
and may collect any otherwise eligible unreferenced resources, including
expired legacy records. It does not override the normal protection of live
leases, image references or committed snapshot parents.

The Node service must invoke recovery at startup and periodically, and retry
after partial failures or full batches. This is a tested library operation;
the serving endpoint and its maintenance loop have not been assembled yet.
Consequently automatic deployed-node recovery is still unproven. No new
background goroutine or implicit cleanup is hidden inside observer dialing.

## Real crash experiment

`TestRuntimeImageCrashRecovery` starts the pinned private containerd and imports
a minimal CPU image. It retains one independent non-expiring view and mount as
a live control. A separate child process runs the actual image observer.
Interceptors stop it immediately after real successful RPCs at three boundaries:
lease creation, snapshot-view creation and mount activation.

The child reports the exact resource ID through a private pipe and blocks
without returning. The parent sends `SIGKILL`, waits for the actual process
exit, and verifies the terminating signal. No Go deferred cleanup can execute.
The test intercepts lease creation to first check that production requested a
one-hour expiry, then substitutes a six-second wall-clock TTL in this private
fixture only. The protocol and garbage collector are real; it does not wait
one hour or establish a production one-hour timing receipt.

For each boundary the test verifies resource existence immediately after death,
refusal to recover the unexpired lease, and continued protection through a
completed forced GC before expiry. After expiry, it leaves the daemon idle for
another 800 ms and confirms the lease/view/activation still exist. This finite
idle observation agrees with the scheduler source; it does not claim the
resources can never be collected by future unrelated mutations.

For the three recovery cases a newly connected observer calls `RecoverExpired`.
Each returns one completed cleanup, and a second invocation returns zero.
An additional activation case triggers ordinary containerd GC by synchronously
deleting a new, unrelated test trigger lease. This independently establishes
that expired observation resources are GC-eligible, while the first three
cases establish the production recovery operation.

All cases require the crashed lease and view to be absent from native APIs.
For an activated view, the test additionally requires the materialized path to
be removed and no matching kernel mount in `/proc/self/mountinfo`. The unrelated
live view remains present with unchanged file bytes; the original image can be
measured again through a fresh observation. Metadata disappearance alone is not
accepted as successful reclamation.

Two fixture corrections were needed. Testing cancels `t.Context` before cleanup
callbacks, so the private daemon now has an independently bounded lifetime and
is stopped by its registered cleanup after dependent resources are released.
An early child helper used self-`SIGSTOP` followed by `t.Fatal`; that could permit
Go cleanup if the stop did not suspend execution before the fatal path. The
final helper blocks indefinitely after its acknowledgement and can only reach
the parent-controlled `SIGKILL` termination. Preliminary failures using these
fixtures are not evidence of a production cleanup failure.

## Validation and limits

The expanded CPU selection includes 23 required top-level tests and rejects
skips. It covers the prior five OCI layer/kernel comparisons, four real crash
cases, and focused recovery ownership/batching/failure tests. Unit cases reject
foreign Node labels, unknown versions, other snapshotters, unmarked legacy
responses, extra labels, malformed/duplicate IDs and invalid expiry metadata
before mutation. They verify the 32-record bound, unexpired-record protection,
discarded caller metadata, exact cleanup order, missing-resource idempotence,
failure retention, partial-progress counts, cancellation and closed clients.

The final ordinary wrapper passes in `41.31s`; the four crash cases take
`25.02s`. Node/Node-command unit tests pass (`3.710s` / `0.578s`), as do Linux
integration-tag vet and Node lint (`0 issues`). All 23 selected Linux static
race tests also pass without skips or data races; the four crash cases take
`25.17s`. The existing static-glibc NSS linker warnings do not qualify
DNS/user-database lookup behavior.
The environment remains the existing digest-pinned Linux/arm64 CPU sandbox,
containerd `v2.3.1` revision `64b425cf570b3b8dd1d4cc46da7c1fce65c6651a`,
runc `1.4.2`, Docker `28.3.2`; builds use Go `1.26.7` and the existing dependencies.
Commands use `--pull never`, no network or host runtime socket and no GPU/model
weights. This is a privileged disposable mount experiment, not qualification
of the production Node privilege profile or RKE2 runtime.

Reproduce the complete ordinary selection with:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

The static race build uses the same
[existing recipe](node-runtime-caller-evidence-2026-09-06.md#validation), with
all 23 wrapper names selected and a 180-second test timeout. Logs are
`/tmp/vela-runtime-image-crash-{cpu,lint,race,race-build}.log`.
Disposable containers, private daemon trees and the race binary are removed.
The two pre-existing stopped PostgreSQL/Go-builder containers remain. There is
no new full-repository unit/integration or production-environment claim.

Daemon crash/restart during an in-flight RPC or mount, recovery after a host
reboot, deployed periodic maintenance, other snapshotters, effective runtime
configuration/message binding and startup/retirement authorization remain
unfinished. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding
1 and Production Gates `0/9` remain unchanged. `UNRESOLVED` and `LEGACY_UNKNOWN`
remain recovery-only; image observation recovery grants no Runtime startup.
