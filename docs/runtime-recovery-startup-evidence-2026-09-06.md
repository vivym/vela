# Recovery startup without replacement model loading

This CPU-only increment follows `609364e`. PostgreSQL schema 94, Worker and
Runtime journals 5, Registry binding 1 and Production Gates `0/9` remain.

## Reproduced startup gap

`StartRuntimeServer` already opened and validated the persistent execution
journal before allocating Runtime epochs. However, it then invoked every
configured backend factory before attaching the Supervisor's admission gate.
That gate rejected readiness and new execution for pending historical writers,
but model loading and backend process creation had already happened.

The Registry-bound recovery test was extended to assert zero backend starts when
the original journal has one pending execution and an installed terminal floor.
On the previous startup path, this command failed:

```sh
go test ./internal/modelruntime \
  -run '^TestRuntimeServerRecoversExistingSupervisorJournalBeforeNewEpochs$' \
  -count=1 -v
```

```text
pending historical writers allowed replacement backend startup: calls=2
```

This is a startup-order defect, not a lost journal or floor-classification
defect: the existing offline inspection correctly reported one pending
execution. The two calls were the configured shared-slot Encoder and VAE
backend factories. Refusing later Stage admission cannot undo their startup.

The existing escaped-child cleanup test also passes while demonstrating a real
limit: an escaped child continues writing after process-group cleanup, and Close
reports incomplete stdout drain. Process-group signaling cannot establish full
writer containment.

## Implementation

When the already-validated journal indicates any pending execution,
`StartRuntimeServer` creates an internal `recoveryOnlyBackend` for every profile
instead of invoking the configured factory. This backend owns no process,
device, model or writer and rejects every backend operation with
`ErrExecutionDrainUnproven`. It implements no inspection/drain capability that
could invent an observation or checkpoint.

The regular Service/Supervisor path still allocates fresh endpoint epochs,
retains the original journal lock, verifies the Registry binding and serves the
private UDS. Identity discovery now describes endpoint identities; it does not
claim that the model was loaded. The existing admission gate continues to deny
readiness and execution, while historical queries and authenticated floor
installation stay available for recovery. No new public startup option or
implicit backend activation is introduced.

Empty history and fully drained history still use the configured backend
factory. One pending record blocks startup for the whole shared member,
independent of its terminal floor, profile or renewal-history completeness.
The recovery endpoint never upgrades itself into an executing backend. Restart
revalidates the durable journal; mere process disappearance cannot change the
decision.

## Validation

- The original failing Registry-bound recovery test now reports zero backend
  factory calls while preserving fresh epochs, journal discovery and UDS access.
- Seven real startup scenarios cover pending, terminal-pending, legacy
  original-only pending, replaced profile/residency, mixed drained/pending,
  empty and fully drained histories. Explicit offline schema-4 migration
  precedes Registry-bound serving in the legacy case.
- All four readiness checks remain false for both AUX endpoints with pending
  history. Fresh correctly signed current-epoch Prepare rejects with the pending
  drain error; Start, Status, Cancel and Seal do not grant execution, cancellation
  acknowledgement or a receipt. History and drain reads preserve their exact
  meaning. Journal byte comparison, exclusive-lock competition and post-shutdown
  inspection verify that these operations do not erase history or ownership.
- A subprocess starts a real stdio driver, persists a Prepare grant and exits
  with `os.Exit(72)` before cleanup. During Prepare the driver starts a separate
  process group whose CPU writer remains active after owner exit. The parent
  verifies that its output file continues growing while the recovery server
  serves historical queries and installs a signed terminal floor. The default
  process factory does not launch either replacement driver.
- The test then stops its own writer through a private test gate. Neither that
  marker nor another Runtime restart creates a drain checkpoint or permits a
  replacement process. An independent empty-journal positive control starts the
  same driver commands through the default process factory, proving that the
  negative startup observation was not an unusable command or fixture.
- `go test ./...` passes, including Runtime `67.595s` and Worker Agent `77.156s`.
  Race suites pass: Runtime `95.791s`, Worker Agent `94.333s`, member transport
  `17.073s`. The focused actual process scenario passes in `0.19s` on the local
  host; that is a test duration, not a service performance claim.
- Four PostgreSQL integrations pass in `35.477s`: authenticated terminal
  disposition, replacement-Runtime terminal recovery, latest-renewal lease
  cancellation/expiry, and compiled Worker bootstrap binding.
- `make lint` reports zero issues; `make verify-generated` and diff checks pass.
  No protobuf, database or journal schema changes are required.
- `/tmp/vela-recovery-startup-runtime-linux.test` passes all `TestRuntimeServer`
  tests on Linux arm64, including the actual owner-exit and escaped-writer test.
  The image is
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
  Execution uses UID/GID 65534, no network/capabilities, read-only root/binary,
  private executable tmpfs and `no-new-privileges`.

## Remaining lifecycle work

This prevents replacement model startup alongside an unresolved *recorded
execution*. It neither stops the old physical writer nor proves complete
process/container/device containment. The lifecycle before first Prepare,
during failed model initialization and while idle can have no pending execution
record; this gate alone cannot establish physical exclusion for those cases.

Complete recovery needs a durable backend/container incarnation recorded before
model startup, a containment mechanism covering descendants outside the initial
process group, and independently verified quiescence for that exact incarnation
before releasing its namespace/device. PID reuse, an absent current process,
process-group signal success and an empty replacement backend are insufficient
proofs. The current repository has no complete cross-epoch implementation of
that contract; the next work must close it without relabeling journal history
as physical evidence.

Durable unhealthy-worker state, sealed receipt recovery, bounded history
reclamation, renewal write-amplification measurements and default Fleet durable
activation also remain open. No GPU use, remote deployment, push or Launch
Receipt occurred. The overall architecture and correctness goal remains active.
