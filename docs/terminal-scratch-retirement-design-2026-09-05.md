# Terminal Stage scratch retirement: minimum closure

Status: the [history reader](terminal-history-evidence-2026-09-05.md), verification
of the complete signed original, and the [signed terminal disposition over
authenticated Control](terminal-disposition-evidence-2026-09-05.md) are implemented
and locally tested at schema 90. The standalone [persistent Worker admission
component](assignment-admission-evidence-2026-09-05.md) now has local replay,
filesystem-fault and process-restart tests. The explicit [durable Stream
integration](durable-stream-admission-evidence-2026-09-05.md) now covers local
execution, Stop, renewal and materialization. The [Runtime admission floor
component](runtime-execution-floor-evidence-2026-09-05.md) now provides a shared
Service boundary and explicit signed cutoff installation. The optional
[durable journal](durable-runtime-admission-evidence-2026-09-05.md) persists and
recovers that floor and the allocation watermark, with fail-closed state binding.
The [signed floor RPC](runtime-floor-rpc-evidence-2026-09-06.md) now supplies local
private-socket delivery with explicit durable server configuration.
The [member forwarding path](member-floor-forwarding-evidence-2026-09-06.md) adds
authenticated leader delivery and validates both acknowledgements. The explicit
[all-member collector](all-member-floor-evidence-2026-09-06.md) now validates
complete trusted history and reconfirms every member's durable installation.
The [launch topology contract](launch-topology-authority-evidence-2026-09-06.md)
now carries trusted member identity/subset digests from v2 Fleet actuation into
v2 Runtime manifests; explicit floor assembly derives its members from that
manifest and rejects conflicting configuration before backend startup.
The [Worker admission floor](worker-admission-floor-evidence-2026-09-06.md) now
persists the signed cutoff before Runtime dispatch, closes initial input/Runtime
entry and renewal, and retains outstanding input handles and execution records.
Its constructor requires explicit first bootstrap; normal recovery cannot
initialize empty replacement directories. A schema-1 Worker journal requires
separate validated migration, not deletion or reinitialization.
Default command assembly, automatic startup reconciliation,
execution drain and the retirement journal below remain open. These
prerequisites do not establish writer exclusion or bounded scratch usage across
terminal Stage executions, including delayed duplicates after success.

## Current ownership and evidence

`input_transfer_target.go` and `root_input_resolver.go` place inputs below
`stage-runs/<StageRunID>/inputs` and `stage-runs/<StageRunID>/root-inputs`.
Attempts of the same StageRun share these inputs. Output manifests bind a
StageAttempt and place its output below `<StageAttemptID>/`.

Confirmed COMMIT establishes that durable publication no longer needs the local
source. The implemented cleanup path uses that confirmation for successful
retirement. Confirmed SOURCE_LOST likewise makes the old StageAttempt output
dispensable, but its StageRun can enter `RETRY_WAIT`, so that confirmation alone
cannot retire its input subtree. Neither response proves local writer exclusion.

The schema-87 [Checkpoint](schema87-validation-checkpoint-2026-09-05.json) includes
a failing public-gRPC probe: both Seal and Cancel/STOPPED followed by the same
still-valid authority produce ACCEPTED/PREPARED and ACCEPTED/RUNNING. `SealOutput`
clears active authority and `PrepareStage` does not consult sealed history.
The schema-87 `installOrRenew` also permitted reusable stopped executions to be prepared again.
The 64-wave successful scratch measurements did not inject these duplicates;
they are not proof of adversarial retirement safety. The design below must cover
successful materialization and old SOURCE_LOST outputs as well as terminal inputs.

The local schema-88 [ordered execution repair](runtime-execution-order-2026-09-05.md)
now prevents this Runtime reentry using an immutable allocation sequence and a
per-epoch watermark. Persistent Runtime epoch advancement rejects old envelopes
after restart. The Worker/Runtime retirement path remains design work: the repair
neither gates Worker input resolution nor establishes backend descendant quiescence.

`StopStage` currently conveys expired or revoked authority. Either can lead to
retry, and `Agent.Cancel` explicitly returns `AllStopped=false` after signaling
members. `Agent.Status` checks every member's runtime identity, authority digest,
and STOPPED state, but `ModelRuntime.Status` rejects expired authority before
reading that state. A process restart does not currently supply durable proof
that a historical execution stopped.

The [input cancellation repair](input-stop-evidence-2026-09-05.md) now makes a
matching Stop visible during input resolution and prevents Runtime admission
after cancellation, even when Resolve returns nil. Its ephemeral input slot
does not persist a watermark, prohibit later replay, or authorize deletion.
Resolver completion is still required before treating its local writers as
drained; Stop returns without asserting AllStopped.

## Recommended boundary

Reuse the authenticated Connect transport, with a typed read-only request and
response. Do not overload StopStage reasons, command detail strings, or
`bounded_status_json` with deletion permission.

The control response states only that a particular StageRun is irreversibly
terminal and has no remaining input use. A role-scoped SQL reader verifies the
original lease, StageAttempt, allocation, Worker identity and permitted epoch,
then returns the terminal StageRun state, terminal fence/version, original
authority digest, and an explicit input-retirement disposition. Only
`SUCCEEDED`, `FAILED`, and `CANCELED` qualify; `RETRY_WAIT` never qualifies.
Expired signatures may authenticate an exact historical identity for this read,
but may not authorize execution or a state change. Missing identity, a future
issue time, a mismatched Worker, or an unrecognized schema yields no disposition.

The cutoff must also be verified by each Runtime, outside the original
Control-to-Worker connection and journal. The minimum disposition therefore
needs a domain-separated Ed25519 signature from Control, forwarded through the
existing authenticated member transport or local UDS. A bare `floor(C)` argument
would expand the Worker's authority: mTLS identifies the caller, while the
original StageAuthority signature does not bind a new cutoff. The execution
signature must never be repurposed as a retirement signature.

Reuse the existing key IDs and verifier-key distribution, with a dedicated
canonical message and signature domain such as
`vela-stage-terminal-disposition-v1\x00`. Bind the schema, original authority
digest, complete Job/Attempt/StageRun scope, immutable terminal state/fence/version,
Worker epoch, complete member/topology and resident Runtime scope, cutoff,
observation/validity times, and signing key ID. The message accepts no caller
paths. Verify shape, canonical bytes, signature, scope and time before installing
any floor; the signature alone never proves that writers stopped.

This reuses the existing trust model rather than establishing a new isolation
boundary. The current Worker command reads the original signing seeds to
construct materialization HMAC and TransferTicket verifiers
(`cmd/vela-stage-worker-agent/runtime.go`). A compromised Worker holding that
seed can derive the execution signing key. Reusing it for retirement preserves
protocol integrity checks but cannot establish Control-exclusive signing against
that Worker. Any stronger threat-model claim requires separate key distribution
and verifier-only Worker capabilities, assessed across those other protocols.

The Worker combines that control disposition with a persistent retirement gate
and an independent local stopped checkpoint. The checkpoint covers the original
allocation's complete member set, exact execution authority digest/nonce,
Worker/member/runtime epochs, and
verified STOPPED results. A cancellation ACK, a missing process record, or a
restarted runtime is insufficient. The backend must join in-process execution
tasks, close their writable handles, and account for any child writers before
reporting a stopped checkpoint. An execution-specific process group or cgroup
can supply part of that evidence when all writers are confined to it.

## Resident model and execution stop

Whole-driver teardown and Stage execution stop have different scopes. Closing a
ProcessBackend may terminate its entire driver process group. Doing that at
every Stage completion would unload the resident model and violate the accepted
Worker contract. A cgroup containing the resident driver cannot become empty
while that driver stays resident.

The CPU `h3stagemock` driver currently processes its requests and output
publication synchronously. Its cancellation code removes the unsealed output
before setting STOPPED; it does not create an asynchronous Stage worker tree.
That implementation explains its local stop semantics but provides no evidence
for an external driver that starts threads or child writers.

An asynchronous driver needs an execution-specific drain contract: reject new
tasks for the exact execution, join the tasks already admitted, close writable
handles, and terminate/reap any execution-owned child writers before its stop
checkpoint is accepted. Persistent model state may remain in memory. If helpers
can leave a process group, signalling that group does not prove they exited.
Supervision must either prevent that escape or retain the execution as unproven.

Even a complete driver drain does not cover Worker input resolvers, downloads,
or future duplicate assignments. Those require the independent namespace gate
below. Teardown tests, a shutdown ACK, and a successful SIGKILL call must never
be substituted for that combined retirement proof.

## Namespace gate and recovery

A past STOPPED observation is not proof of future absence of writers.
The schema-87 `ModelRuntime.installOrRenew` permitted a stopped execution to
be prepared again with the same authority. The schema-88 sequence fence excludes
that RPC reentry. The Worker must still persist an intent before cleanup and
atomically close admission to
Resolve/Prepare/Start for the entire target StageRun namespace. This gate must
serialize with ExecuteAssignment and all root-input/transfer resolution. Late
RPC responses must not install a runtime or write into a gated namespace.

The Runtime now retains the highest installed allocation sequence and advances
its persistent epoch on restart, without an unbounded per-attempt tombstone map.
Database PITR/data-loss recovery additionally requires new Runtime epochs before
signing resumes; an unseen old high sequence cannot be detected by the watermark.
The Worker must still drain already admitted input resolvers, download writers, runtime
processes, and other namespace users, waiting for their work to exit and file
descriptors to close. Only after both the admission gate and the runtime restart
barrier are established can the all-stopped checkpoint prove quiescence for
deletion. A process being stopped while another local writer remains active is
insufficient. If draining fails, keep the retirement intent pending and preserve
the files.

The existing ModelRuntime Status RPC can support bounded historical queries:
an expired signed authority may query the exact matching execution or return an
already persisted stopped receipt, without creating, installing, renewing, or
starting an execution. A stopped checkpoint obtained before restart can be
replayed from trusted local storage. Without such evidence after restart, the
Agent must not infer STOPPED from absence; a supervisor recovery path must first
prove that the old execution cannot still run.

A local retirement intent must retain the exact authority and expected membership
before they can be forgotten. Persist the control response, namespace admission
gate, runtime restart barrier, and all-stopped checkpoint before deleting files.
Keep the intent until the existing directory
binding and no-symlink retirement logic finishes. Replaying the intent after any
crash must remove only `stage-runs/<StageRunID>` on this Worker and the explicitly
owned old output directory. This is a new local terminal-retirement journal
boundary, not functionality already provided by FileProductionState, which
currently persists session and capacity state rather than active failures.

The server can remain read-only if terminal state and fence are irreversible and
the disposition is derived deterministically from those durable facts. A new SQL
command-receipt table is unnecessary for that form. If later requirements allow
reopening a terminal StageRun or revoking an issued disposition, this argument no
longer holds and a durable disposition protocol is required.

## Historical control reader contract

The typed Connect operation and signed response in this section are now
implemented. `ReadStageTerminalDispositionRequest` requires the original Acquire
ID for ASSIGN evidence; an exact recorded renewal can omit it. The result signs
only INPUTS_UNUSED. RETAIN has no signed disposition. Fresh observations are
valid for at most five minutes, with no future clock allowance. Expiry cannot
authorize lowering an already installed floor; floor installation and recovery
are still separate implementation work.

The request is
`ReadStageTerminalDispositionRequest{schema_version=1, StageAuthority authority}`
on the existing Connect stream. It accepts no caller-selected paths. The typed
response carries `INPUTS_UNUSED` or `RETAIN`, a reason, the exact original
authority digest, StageRun/StageAttempt/allocation/lease identities, terminal
state/fence/version, historical Worker epoch, authenticated member/current
control session, and `observed_at`. It must also carry the scoped retirement
cutoff and complete Runtime scope described below; an old allocation's digest
alone is insufficient. It conveys no output deletion permission.
Successful COMMIT and SOURCE_LOST retain their existing output dispositions but
must also pass the local writer gate before cleanup.

`vela_enforce_stage_run_authority` in migration 36 disallows transitions out of
`SUCCEEDED`, `FAILED`, and `CANCELED`. Same-state changes cannot modify fence or
version, but `updated_at` is not protected. Return an observation timestamp;
do not describe `updated_at` as an immutable terminal timestamp. Missing retained
identity evidence yields `RETAIN`, even if independent metadata expiry removed
the Job source row.

Use `non_content_attempt_roots` and `non_content_job_roots` for the historical
Job/Attempt identity. Migration 58 rebinds the retained StageRun, StageAttempt
and coordinator-command foreign keys to those roots. Absence of an expired
live `jobs` or `attempts` row alone is not missing history and must not force
retention when the immutable roots and complete execution evidence remain.

The implemented `vela_read_stage_terminal_history(jsonb)` is a read-only
`STABLE SECURITY DEFINER` function owned by
`vela_attempt_coordinator_owner`, with execute granted only to the Stage Worker
control role. Validate its fixed search path and effective role capabilities.
Use `ValidateEnvelopeForReplay` to verify signature, shape and future issue time,
while allowing expired identity for this read without granting execution.

The SQL reader must match the exact historical lease and the original ASSIGN
or an exact historical renewal digest, complete allocation/StageRun/Job scope,
member identity and Runtime barrier membership. Existing active-authority and
snapshot readers require current READY/capacity/latest-renewal state; they do not
provide this historical contract. Do not relax those existing execution readers
to make terminal inspection work.

The original-envelope lookup is implemented by the authenticated Go reader.
`stage_leases.token_digest` hashes the lease token, not the full StageAuthority.
Schema 90 retains canonical `authority_wire` separately from retireable
`assignment_wire`, while `stage_authority_renewals` retains a complete renewal
and its authority digest. Locate the original wire through retained assignment
identity, decode it with the existing protobuf API in trusted Go code, and verify
the complete canonical authority before signing a disposition. Missing original/renewal
evidence must yield RETAIN; substituting a token digest is insufficient. Verify
the reader owner's SELECT permissions on the retained identity roots as part of
the integration test, rather than relying on a privileged test connection.

Conservative v1 requires the current WorkerInstance epoch to equal the historical
lease epoch. A reconnected control session may differ from the original session
but must match the authenticated member's current database session. Cross-Worker
epoch recovery returns `RETAIN` until namespace ownership is independently proven.
Runtime restart also needs the separate local barrier described above; it does
not imply that the prior execution process or descendants exited.

Implementation enters through `proto/vela/v1/stage_worker_control.proto`,
`internal/stageworkercontrol/operation_descriptor.go`, `handler.go`, `executor.go`,
the PostgreSQL repository and `cmd/vela-control/stage_worker_control.go` wiring.
Keep the transport response typed and validate all response identity fields
against the request before persisting a retirement intent.

## Persistent local exclusion contract

There are two different identities to retire. A completed physical StageAttempt
must never restart, including after Seal or SOURCE_LOST. A terminal StageRun's
shared input namespace can be removed only after every physical attempt and
local input writer using that namespace has been excluded and drained. Retiring
one failed StageAttempt must not prevent a legitimate retry with a new
StageAttempt from reusing the still-live StageRun inputs.

For bounded local state, use one Worker-wide persistent sequence watermark,
one latest execution slot, and a capacity-limited set of pending retirement
intents. This state spans the resident profiles sharing the Worker; it must not
reset for each profile. The slot distinguishes `INPUTS_PENDING`,
`RUNTIME_ENTERED`, and `CLOSED`. Only the same verified immutable identity at
`INPUTS_PENDING` may resume an equal sequence after a download failure. Once
the Runtime entry is durably recorded, uncertain RPC outcomes recover through
Status/Reattach instead of repeating Resolve/Prepare. Renewals cannot reopen
a closed slot.

Validate the full assignment, V2 signature/time window, and current
Worker/member/topology/runtime binding before advancing state or writing files.
Invalid or busy requests must not consume the watermark. The existing command
composition already constructs a StageAuthority validator, but it is not
currently used by StreamAgent before Resolve.

Admission synchronization must not hold its state lock over downloads or
control exchanges. Stop must see and close an input-phase slot, cancel its
resolver, and wait for the admitted work to return and close its handles. Check
the slot again before entering Runtime and accepting late control responses.
Persist failure prevents side effects; missing or damaged state on a reused
root cannot be treated as first use. Keep unfinished retirement intents under
backpressure rather than evicting them. FileProductionState's current behavior
of initializing a missing session/capacity file is not this recovery contract.
The explicit assignment admission component now enforces that distinction for
its own journal. Its signed floor API records C and its witness, and the Stream
floor operation persists this local restriction before collecting Runtime
acknowledgements. Partial remote failure does not reopen input. Waiting for an
input handle's Release covers its in-process resolver only; after a Worker crash,
the persisted intent still needs independent recovery and writer-drain evidence.

An observed sequence watermark alone does not cover all issued attempts. For
example, this Worker may have observed allocation `n`, while Control has already
allocated retry `n+1` for the same StageRun. The StageRun can become terminal
before the Worker receives `n+1`. Retiring the namespace at local watermark `n`
would still admit that delayed assignment. Renewals are another envelope of the
same allocation sequence, so deleting an envelope-specific record does not solve
the problem.

The historical reader must additionally establish a retirement cutoff `C`: the
maximum execution sequence across every allocation for the terminal StageRun,
this Worker, and the permitted epoch. Its consistent snapshot must include
allocated-but-not-yet-signed rows and demonstrate that no further allocation can
be issued for the irreversible terminal StageRun. All retained identities must
be complete and numbered. Missing/V1 history or an unknown history-retention
boundary yields RETAIN; computing a maximum over an incomplete subset is not
proof. The global sequence's `last_value` is not a substitute because it spans
unrelated Workers and is not a scoped history-completeness check.

Before filtering the history to a Worker, cross-check the complete StageRun's
`stage_retry_budgets.attempts_consumed` against its unique, contiguous
`stage_attempts.physical_attempt_number` set. Each physical attempt must have
its matching ASSIGN coordinator command and exact allocation/lease identities.
The ASSIGN transaction writes those objects and consumes the budget together;
a gap, count mismatch, conflicting identity or unnumbered relevant allocation
must yield RETAIN. A zero budget cannot authenticate a request naming a physical
allocation. The cutoff query must include allocations whose signed envelope was
never delivered or recorded.

Compare ASSIGN `result.stage_attempt_id` values with the complete physical
attempt set one-to-one. Use LEFT JOIN or explicit anti-joins to detect missing
allocations, original leases and commands; an INNER JOIN must not silently
reduce the set being checked. Only physical attempt numbers are contiguous.
Execution sequences can have legitimate gaps from other Workers and rolled-back
transactions, so no sequence-contiguity requirement applies to C.

This completeness argument assumes the supported role-scoped issuance path and
the pinned schema. ASSIGN locks the StageRun and requires READY before its
transaction can commit; observing an irreversible terminal StageRun in one
consistent snapshot therefore excludes a later supported ASSIGN. The current
allocation identity triggers protect UPDATE, not arbitrary privileged
INSERT/DELETE. Repository migrations contain no execution-history deletion
path, but this is not evidence against hostile owner/superuser SQL. Any added
history compaction or alternate issuance path must preserve a durable
completeness bound or make the historical reader return RETAIN.

Persisting Worker watermark `C` only closes the input-resolution entry point.
Every relevant member/resident Runtime must also establish an irreversible
admission floor through `C`, and then drain any execution it already accepted.
Otherwise a delayed direct Prepare for `n+1` can pass the Runtime's still-local
watermark `n` after the Worker has closed input admission. A single older
allocation's STOPPED receipt does not cover the unseen attempt or that second
entry point. The Runtime set must cover the complete relevant allocation
history, not just the authority used to query the terminal reader.

Build that Runtime scope from the union of all scoped allocations, preserving
their selected profiles, residencies, runtime identities and complete historical
barrier registrations. `stage_allocations.model_runtime_epoch` identifies a
barrier generation, while each registration has its own
`local_model_runtime_epoch`; these must not be conflated. Bind member epoch,
identity digest and device subset digest as well. A SUPERSEDED barrier or a
currently non-READY member is still part of the history. Missing registrations
or an incomplete member set yield RETAIN, not a reduced Runtime scope.

Runtime installation has two independently observable states: `FLOOR_INSTALLED`
and `DRAINED`. The first closes admission through C across every resident profile
sharing the Worker member; it does not declare accepted work stopped. The
second requires an exact execution-specific drain checkpoint for the disposition's
complete scope. Partial member installation remains restrictive and is safe to
retry. Any missing installation or drain checkpoint prevents deletion; later
timeouts or expired request envelopes cannot lower an installed floor.

The shared execution admission component now covers Prepare, already PREPARED
executions reaching Start, and implicit renewal through Status/Seal, including
direct Service calls. Its optional signed floor constructor requires trusted
complete member identity/device-subset digests and exact current local Runtime
routes for every historical allocation. Missing or replaced runtimes are rejected.
It registers admitted calls under the common lock without holding that lock over
backend calls. Optional journal configuration now persists/replays restrictions
across profile and local Runtime epoch changes without granting historical
execution or drain. Missing/replaced state fails closed. The local signed floor
RPC now reports identity/digest-bound durable installation, including forwarding
through an authenticated member. An explicitly configured Worker collector now
validates complete historical Runtime routes and requires all-member durable
acknowledgements, with full retry after partial installation. Runtime launch v2
now supplies the trusted complete member digest configuration; subset digests
remain opaque approved values. Default command assembly still needs independent
bootstrap authority: a missing file, empty directory or repeated Pod init is not
evidence of first use. Recovery must never receive a reusable initialization
permission. Persistent retirement orchestration remains open. WaitAcceptedOperations joins
only the calls registered before that installation, not asynchronous backend
writers or later cancellation calls. Historical stop inspection must remain
read-only and cannot implicitly renew. Normal Stage drain retains model residency
and must not call Service.Shutdown as a shortcut.

An alternative is an independently proven barrier that invalidates every old
authority before execution drain. Advancing only a local counter, observing
one lease expire, or finding an empty active-execution map does not establish
that barrier. The local protocol now has a signed terminal-cutoff installation
RPC and explicit all-member collection; default wiring and combined retirement
recovery remain open.

The runtime epoch store persists an epoch, not terminal receipts or namespace
intent. The materialization journal covers sealed outputs, not every failed or
canceled execution. Neither can be described as the missing retirement journal.
Persist each intent before forgetting its authority or starting cleanup. Tie
recovery to the bound directory identity and exact complete member set.

The [ProcessBackend teardown repair](process-backend-teardown-evidence-2026-09-05.md)
signals the driver's group before reaping the direct child and bounds output
drain. Escaped descendants and blocked caller-owned writers remain explicitly
unproven. The backend STOPPED contract still needs independently verified
execution-specific writer quiescence; do not convert a group signal or driver
ACK into deletion permission.

Terminal replay suppression also needs bounded retained state. A fixed-size
receipt cache can evict a still-valid retired authority; an unbounded tombstone
map leaks over a resident runtime's lifetime. Any expiry policy must account for
unseen signed renewals and the full allowed authority validity/skew, backed by
an irreversible control cutoff or an equivalent generation fence. Do not reclaim
a tombstone merely because the last locally observed envelope expired. Schema 88
uses an immutable allocation sequence shared by every renewal, plus persistent
Runtime epoch advancement, instead of an envelope-based expiry policy.

## StageAttempt-owned inputs

Moving inputs to `stage-attempts/<StageAttemptID>/...` would eliminate the shared
input ownership between retries. After a verified stop, a failed physical attempt
could retire its own files while the next attempt resolves its own inputs.

That alternative changes transfer targets, root-input resolution, runtime paths,
materialization/recovery journals, and retry cache reuse. It can increase repeated
downloads and duplicate local storage. It is a coherent future ownership model,
but changing only the cleanup path would be unsafe for existing StageRun-owned
directories. The typed terminal disposition plus local stopped checkpoint is the
smaller change compatible with today's ownership model.

## Required verification before implementation closure

- Retry preserves shared inputs; terminal failure, cancellation, and expiry retire
  only the confirmed terminal StageRun after all members stop.
- Control response loss, local checkpoint persistence failure, and restart before
  or during deletion replay without broadening the path or execution identity.
- Missing members, CANCELING, failed Status RPCs, absent historical receipts,
  mismatched runtime epochs, and future-issued authority prevent deletion.
- Concurrent Resolve/Prepare/Start, delayed duplicate RPCs, and runtime restart
  cannot reopen a namespace once retirement is admitted. Cleanup waits for
  in-flight writers and descriptors; a past STOPPED result alone never suffices.
- Directory replacement, symlinks, and changed manifests preserve neighboring
  StageRun and StageAttempt files.
- Repeated failure/cancellation/expiry campaigns measure scratch bytes and
  journal entries after convergence. Successful-run measurements alone do not
  prove a bound for these paths.
