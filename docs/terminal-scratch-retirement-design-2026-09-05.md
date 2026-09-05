# Terminal Stage scratch retirement: minimum closure

Status: design only. This does not establish writer exclusion or bounded scratch
usage across terminal Stage executions, including delayed duplicates after success.

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
after restart. The rest of this document remains design work: the repair neither
gates Worker input resolution nor establishes backend descendant quiescence.

`StopStage` currently conveys expired or revoked authority. Either can lead to
retry, and `Agent.Cancel` explicitly returns `AllStopped=false` after signaling
members. `Agent.Status` checks every member's runtime identity, authority digest,
and STOPPED state, but `ModelRuntime.Status` rejects expired authority before
reading that state. A process restart does not currently supply durable proof
that a historical execution stopped.

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

No new cryptographic key hierarchy is required for the minimum design. A typed
response over the existing authenticated control connection, checked against the
exact request and persisted in the local journal, has the same trust model as a
confirmed COMMIT. A separate signed capability is useful only if the response
must later be verified outside that trusted connection/journal boundary. It does
not by itself prove that the runtime stopped.

The Worker combines that control disposition with a persistent retirement gate
and an independent local stopped checkpoint. The checkpoint covers the original
allocation's complete member set, exact execution authority digest/nonce,
Worker/member/runtime epochs, and
verified STOPPED results. A cancellation ACK, a missing process record, or a
restarted runtime is insufficient. The runtime backend must have reaped the
execution process group or verified its cgroup is empty before reporting STOPPED.

A past STOPPED observation is not proof of future absence of writers.
The schema-87 `ModelRuntime.installOrRenew` permitted a stopped execution to
be prepared again with the same authority. The schema-88 sequence fence excludes
that RPC reentry, while the Worker retirement design must still
therefore persist an intent before cleanup and atomically close admission to
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

The proposed request is
`ReadStageTerminalDispositionRequest{schema_version=1, StageAuthority authority}`
on the existing Connect stream. It accepts no caller-selected paths. The typed
response carries `INPUTS_UNUSED` or `RETAIN`, a reason, the exact original
authority digest, StageRun/StageAttempt/allocation/lease identities, terminal
state/fence/version, historical Worker epoch, authenticated member/current
control session, and `observed_at`. It conveys no output deletion permission.
Successful COMMIT and SOURCE_LOST retain their existing output dispositions but
must also pass the local writer gate before cleanup.

`vela_enforce_stage_run_authority` in migration 36 disallows transitions out of
`SUCCEEDED`, `FAILED`, and `CANCELED`. Same-state changes cannot modify fence or
version, but `updated_at` is not protected. Return an observation timestamp;
do not describe `updated_at` as an immutable terminal timestamp. Missing retained
identity evidence yields `RETAIN`, even if independent metadata expiry removed
the Job source row.

The proposed `vela_read_stage_terminal_disposition(jsonb)` is a read-only
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

The runtime epoch store persists an epoch, not terminal receipts or namespace
intent. The materialization journal covers sealed outputs, not every failed or
canceled execution. Neither can be described as the missing retirement journal.
Persist each intent before forgetting its authority or starting cleanup. Tie
recovery to the bound directory identity and exact complete member set.

`ProcessBackend.abort` currently calls the direct process's `Process.Kill`.
It does not establish that a process group was reaped or a cgroup is empty.
The backend STOPPED contract needs independently verified writer quiescence;
do not convert that Kill call or a driver ACK into deletion permission.

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
