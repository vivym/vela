# Stage Worker Scratch Retirement

Default command assembly uses `RetainScratchRetirer`: it preserves scratch and
confirmed materialization records until automatic retirement/recovery wiring is
complete. The filesystem and journal semantics below do not enable deletion by
themselves. An explicit combined coordinator is described below.

## Ownership Contract

Production `vela-stage-worker-agent` explicitly selects
`attempt-owned-filesystem-scratch/v1` through
`MaterializationConfig.OutputOwnershipContract`. Its filesystem scratch
retirer requires every output locator to start with the manifest lineage's
`<StageAttemptUUID>/`. Inputs belong to `stage-runs/<StageRunUUID>/`.

This is a production filesystem ownership contract, not a change to the generic
`LocalOutputManifestV1` parser or materialization authority issuer. Generic v1
locators such as `outputs/encoder.bin` remain valid without filesystem scratch
retirement. They do not prove unique ownership of a reusable output pathname.

The StreamAgent validates this contract before requesting materialization
authority or publishing output. If a runtime produces a nonconforming locator,
the local sealed receipt and payload remain in the recovery journal and scratch;
the operation fails before L2 publication or COMMIT. The driver must satisfy the
selected ownership contract before production use. Do not rename signed manifests
or discard their journals to bypass this check.

## Confirmation and Recovery

For successful materialization, the worker receives an accepted or replayed
COMMIT response, durably records `COMMITTED`, then retires the completed
StageRun's inputs and the exact output named by its manifest.

For source loss, the worker receives an accepted or replayed SOURCE_LOST response,
durably records `SOURCE_LOST`, then retires only the old StageAttempt output.
The same StageRun can retry, so its input namespace remains available. A journaled
SOURCE_LOST request continues that operation even if the old source reappears.

In both cases, journal deletion follows successful local retirement. Control
response loss or failure to persist confirmation preserves scratch. Cleanup
failure or cleanup response loss preserves the confirmed recovery record. A
confirmed record retries local cleanup after restart without requiring a fresh
control request, including after its materialization authority expires.

Before the first COMMIT or SOURCE_LOST request, its stable canonical command ID
is persisted together with the exact timestamp or source-loss evidence. Retries
and process restarts use that same ID and payload. A timed-out transport exchange
closes its Connect stream before retrying; request IDs remain unique within each
stream. If an ACK was received but journal persistence failed, the existing
same-stream duplicate rejection forces reconnect before durable command replay.

Retirement binds each directory component by inode. Recursive input removal is
confined to the exact StageRun handle, and output removal uses one leaf name
relative to its bound parent. Parent-path replacement cannot redirect payload
deletion into another StageRun or StageAttempt.

## Journal Compatibility

New workers write materialization journal `schema_version=2`. The new reader
accepts schema 1 records as unconfirmed and schema 2 records with or without a
confirmed disposition. A schema 1 record claiming the new confirmation field is
rejected. Existing generic v1 output manifests are not rewritten during recovery.

Reading an old record does not recover an unknown historical command ID. An
unconfirmed legacy record with `committed_at` or source-loss evidence but no
original command ID returns `ErrLegacyMaterializationCommandIdentity` and remains
intact. It needs authoritative reconciliation of the original control command;
the worker does not guess the old transport-generated UUID. If no COMMIT or
SOURCE_LOST request was prepared, the new worker can persist a new stable ID
before its first send. A previously confirmed disposition can finish local
cleanup without a historical command ID.

Old workers reject schema 2 records. This is forward recovery compatibility,
not binary downgrade compatibility. Roll forward using the new worker with its
existing journal. To roll back to an old worker, first drain pending records with
the new worker and verify the journal is empty. Do not erase confirmation fields,
change the version marker, or delete pending records for rollback. Drain legacy
generic-locator records with their compatible worker before adopting the new
production filesystem contract. Different worker generations must not operate
the same journal concurrently.

## Explicit Terminal Coordinator

`NewTerminalScratchRetirement` requires a durable `FileAssignmentAdmission`,
configured Runtime Agent and `attempt-owned-filesystem-scratch/v1`. `Retire`
accepts signed terminal history, available original execution envelopes and
trusted current readers. It persists INTENT and closes the StageRun namespace,
then requires input completion, every Runtime floor and every allocation/member
execution proof. READY binds the actual directory identities before deletion;
RETIRED is written only after cleanup and directory sync.

`Resume` takes a StageRun UUID and can finish a durable READY record after crash,
expiry or Runtime profile changes. It cannot promote INTENT without fresh
history and complete proof. Replaced/new namespaces reject. Do not remove or
rewrite the journal or root markers to bypass recovery. Completed records are
still bounded and retained; journal capacity reclamation is not implemented.
Missing namespaces still require syncing their bound surviving parent before
RETIRED. A sync error retains READY and must not be replaced by a manual phase
edit merely because the directory is absent.

Worker admission journal is now schema 5, independently of materialization
journal schema 2. It binds complete Worker/member/device topology even without
assignment history or a floor. Runtime epoch/profile/residency replacement and
configuration ordering do not change this scope. Signed history is independently
matched to the configured topology on recovery.

Explicit `AssignmentAdmissionConfig.UpgradeV4`, `UpgradeV3` or `UpgradeV2`
preserves older evidence without creating retirement or writer proof. It now
requires an existing signed floor proving the complete original topology,
including member identity and device subset digests. Old empty journals or
journals containing only assignments cannot be upgraded from replacement
configuration. Preserve their files and scratch for authoritative reconciliation;
never treat this rejection as permission to initialize. Ordinary recovery does
not migrate, and schema 1 still requires separate reconciliation. Older binaries
reject schema 5; use the newer reader for forward recovery. See
[topology evidence](../worker-journal-topology-evidence-2026-09-06.md) and
[Durable Terminal Retirement](../durable-terminal-retirement-evidence-2026-09-06.md)
for the output lifetime premise, tests and remaining default assembly work.

## Offline Admission Journal Preparation

`vela-stage-worker-agent journal` validates local history without loading the
serving environment, contacting Control/peers or starting a ModelRuntime.
Supply the approved launch manifest as visible in the Worker filesystem and
the public StageAuthority verifier keyring. Its input/output directories come
from that manifest; all local Runtime entries must use the same pair.

For an existing journal, with deployment-specific file paths:

```sh
vela-stage-worker-agent journal \
  --action recover \
  --launch-manifest-file /run/vela/launch.json \
  --verifier-keyring-file /run/vela/verifier.json \
  --directory /var/lib/vela/stage-worker/admission \
  --max-records 64
```

Use the same `--max-records` used at first initialization. Recovery validates
and syncs the existing journal, then releases its exclusive lock. It fails if
the directories, state, ownership markers, topology, signature/proof or lock
cannot be validated. It does not create missing directories or state.

`--action initialize` is separate first-use provisioning and requires independent
first-use authority plus existing empty private state/input/output directories.
It is not a restart fallback. If the command's output is lost, use `recover` to
inspect whether initialization persisted. `upgrade-v2`, `upgrade-v3` and
`upgrade-v4` retain the signed-floor requirement described above; do not use
initialization to bypass a failed upgrade. Concurrent online/offline owners
cannot share the journal lock.

The JSON output reports local journal metadata, unproven inputs and retirement
phase counts. Success is not readiness, Runtime epoch observation or writer
drain. Preparation leaves INTENT/READY/RETIRED unchanged and never deletes
scratch. Later serving startup must reacquire and validate the journal with
complete trusted current Runtime bindings. Default Worker serving assembly and
Fleet provisioning do not yet select this path. See
[preparation evidence](../worker-journal-preparation-evidence-2026-09-06.md).

## Explicit Stream Reconciliation

`DurableStreamConfig.TerminalRetirement` must reference the coordinator built
from that same Stream's admission gate and Runtime Agent, with the supported
output contract. Its `RetireTerminalScratch` serializes cleanup with sealing and
publication and validates matching materialization records before deletion.

In this configured path, `ResumeMaterializations` first recovers READY/RETIRED
and removes obsolete matching materialization records only after durable RETIRED.
INTENT still needs fresh history and complete drain, and blocks ordinary replay
and the Production discovery loop. A journal-delete failure leaves the permanent
retirement proof available for retry. `TerminalRecordsRetired` is local cleanup,
not a COMMIT/SOURCE_LOST confirmation; do not use it to generate billing or
publication receipts. See [reconciliation evidence](../terminal-materialization-reconciliation-evidence-2026-09-06.md).
Default command assembly does not yet enable this path.

Set `DurableStreamConfig.TerminalHistory` to the authenticated Control client to
enable fresh collection in this recovery loop. It queries retained execution
envelopes, prefers the latest signed renewal and preserves original Acquire IDs.
By default it derives Runtime targets from trusted historical bindings. After
Runtime epoch/profile replacement, set `ExecutionFloorConfig.CurrentReaders`
to the complete current member-to-Runtime identity map, with each entry matching
exactly one trusted `Bindings` entry. It is validated and cloned at construction;
historical signed allocations cannot supply current reader authority. No
execution authority is invented for an undelivered retry. Fresh complete history can advance
INTENT; RETAIN or missing retained query evidence leaves it blocked. The pass
uses the Runtime floor timeout. Existing READY/RETIRED recover before fresh
collection and require no online Control or Runtime. Missing writer proof still
requires writer recovery, not repeated cleanup or a forced journal deletion.
See [automatic recovery evidence](../automatic-terminal-recovery-evidence-2026-09-06.md).
The Production loop first resumes complete local checkpoints. Before a fresh
history query, it synchronizes the Control session through a confirmed
zero-capacity report, independently of Runtime readiness. An open stream is
insufficient: a changed session epoch or lost response requires confirmation.
Incomplete recovery keeps capacity unavailable; readiness registration, usable
capacity and acquisition follow successful recovery. A Runtime that rejects
readiness because it needs recovery therefore cannot block that recovery.
This uses the existing Fleet identity/lifecycle conditions for capacity reports;
it does not bypass them for removed or non-READY Fleet identities.

Terminal recovery now uses execution-floor RPC v2 to restrict those current
durable journal owners. V1 retains its resident-historical-route requirement.
V2 still requires exact current identity, fresh signed complete topology and
matching-version durable replies from every member. It does not prove that an
old writer stopped. Missing original drain/non-admission proof leaves INTENT
and scratch intact after replacement. See
[replacement Runtime evidence](../runtime-replacement-floor-evidence-2026-09-06.md).

The newer Worker reader accepts retained v1 and v2 floor replies. Before schema
5, some older schema-4 readers already rejected retained v2 replies;
the version number alone therefore did not establish compatibility. Do not relabel, strip or erase proof
to force binary rollback. V1-only Runtime/member hops cannot finish v2
retirement and retain scratch until upgraded.

## Remaining Lifecycle Boundary

The [authenticated member discovery RPC](../member-discovery-evidence-2026-09-06.md)
can observe current remote Runtime identities before assignment or readiness.
It requires complete configured `MemberBinding.IdentityDigest` values and the
authenticated deterministic Leader. Match the results to approved launch routes
and topology before using them as admission bindings or `CurrentReaders`.
Discovery is not proof of readiness, a durable journal or drained writers.
If the local UDS identity set changes after member-server startup, discovery
rejects until Worker topology is reconstructed. An old peer's Unimplemented
response cannot be interpreted as successful discovery of no runtimes.

The [offline Runtime journal command](../runtime-journal-preparation-evidence-2026-09-06.md)
provides separate `initialize`, `recover`, `upgrade-v2` and `upgrade-v3` actions.
It needs trusted launch/verifier files and a provisioned private directory, but
no backend, Runtime epoch store or server socket. Only independently authorized
first use may select initialization. Normal Runtime serving can select the same
journal through `VELA_MODEL_RUNTIME_EXECUTION_STATE_DIRECTORY` and always uses
recovery; missing state never triggers initialization. Default deployment
activation and durable Worker assembly remain incomplete.

For an explicitly durable Runtime server, journal validation and locking now
precede epoch allocation and model startup. Missing, corrupt, locked or
wrong-scope state therefore rejects without starting a backend. Keep the same
journal for ordinary recovery; do not set `Initialize` on every Pod restart or
delete files to force startup. A clean startup rollback releases the journal;
failed backend shutdown still requires independent containment and must not be
treated as proof that the device or old writers can be reused. See
[Runtime startup evidence](../runtime-journal-startup-evidence-2026-09-06.md).

The schema-87 public-gRPC probe reproduced Prepare/Start with the same still-valid
authority after both Seal and STOPPED. Both paths returned RUNNING. Schema 88
now rejects this Runtime RPC reentry through signed allocation order and
persistent Runtime epochs. It still does not gate Worker input writers or prove
backend execution drain. The confirmation and inode-bound deletion behavior
above therefore remains partial. The successful 64-wave campaign did not inject
those writer races. See the
[Checkpoint](../schema87-validation-checkpoint-2026-09-05.json),
[Execution Order](../runtime-execution-order-2026-09-05.md), and
[Retirement Design](../terminal-scratch-retirement-design-2026-09-05.md).

No mtime-based or generic TTL/quota sweeper is added. Committed input files from
failed, canceled or expired StageRuns require an explicit control-plane terminal
retirement disposition plus proof that compute has stopped and no recoverable
materialization remains. A cancellation acknowledgment alone is insufficient.
Every retirement path also needs persistent prevention of future Resolve/Prepare/
Start calls, drain of already admitted writers, and a Runtime restart barrier.
Until those conditions hold, successful-load scratch measurements do not
establish adversarial cleanup safety or bounded scratch across failure campaigns.

ProcessBackend shutdown is a separate whole-driver operation. Its process group
contains the resident model, so normal Stage cancellation cannot kill that group
while preserving residency. A Stage stop checkpoint needs execution-specific
task/child/handle drain; whole-driver teardown evidence does not authorize
scratch retirement.

These CPU/mock checks do not establish real GPU readiness or Production Gate
completion.
