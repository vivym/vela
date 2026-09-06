# Worker bootstrap authority and terminal outcomes

Status: repository implementation through schema 94; durable Fleet activation
and physical replacement remain incomplete. This contract does not establish a
Production Gate or GPU acceptance.

## Identity and first use

PostgreSQL owns the immutable bootstrap request, approved bundle bytes and
Worker/member/epoch/node identity. The registered Node Agent's mutual-TLS
principal determines the node and actor; actor strings in local configuration
cannot impersonate a different provisioner.

Only the committed insertion of a claim grants first use to the executing
provisioner. Exact replay and history queries carry no fresh grant. A member UUID
can appear in only one claim, even across Worker epochs. Missing local files,
timeouts and absent processes never grant another initialization.

The provisioner first retains a private durable local operation, then obtains a
fresh claim, initializes both journals and writes the original local pair.
Receipt recording/replay recovers and holds both journals while reporting their
IDs/scopes. Serving separately verifies the Control-signed historical binding
against its actual lifetime-locked journal before startup.

Durable Worker assembly also requires live Runtime discovery to return that
Registry binding after checking the held execution journal under its admission
mutex. The Worker independently verifies the signature and member scope; local
discovery must match its own recorded claim/pair, while each remote member uses
its own signed pair. Identity and epoch alone cannot satisfy durable assembly.
Discovery remains available for intact journals awaiting recovery even when
readiness is false. Closed, failed or replaced journal ownership rejects.
Member discovery also reports `worker_journal_binding` after observing the
forwarding Worker's actual admission handle before and after the Runtime RPC.
The durable Leader verifies both signatures, the member scope and equality of
their immutable Registry claim/pair. A nondurable Worker, failed/closed journal,
or ownership loss during forwarding cannot satisfy durable discovery.
These observations do not create a continuing ownership lease or attest
physical drain.

An existing Runtime journal with any retained execution lacking a durable drain
checkpoint starts only a recovery endpoint. Journal ownership and Registry
binding are validated before epoch allocation. The endpoint receives fresh
Runtime epochs and retains the same journal lifetime lock, but no configured
backend factory or driver process may start for any of the member's profiles.
The recovery backend has no process or device and rejects every backend
operation. Runtime identity discovery names current endpoints, not successful
model loading. Readiness remains false; historical reads and authenticated floor
installation remain available. Current-epoch Prepare, Start, renewal, Seal and
unproven cancellation cannot infer readiness or writer drain from this endpoint.

This restriction applies without a terminal floor, across profile/residency
replacement, to legacy original-only history and when only some retained
executions have drain proof. It does not activate a backend in place. A later
startup must independently validate complete journal evidence. Empty or fully
drained execution history is insufficient for model startup: the separate
schema-6 backend lifecycle must also permit it. Neither process disappearance
nor an empty backend can manufacture a missing checkpoint. Process/container
containment before Prepare, during model initialization and while idle remains
a separate lifecycle requirement; execution history alone does not establish it.

## Backend containment and replacement

The current Fleet Pod contract gives ModelRuntime an independent PID namespace:
`HostPID=false`, `ShareProcessNamespace=false`, no Runtime container command or
argument override, and the release-validated exact `vela-model-runtime` image
entrypoint. The executable is the namespace's PID 1. A surviving wrapper or
shared Pod PID namespace changes that containment boundary and must not be
substituted without a separately validated lifetime owner.

The local Linux CPU experiment confirms that exiting Runtime PID 1 terminates
the tested descendants even after they create separate process groups. A
Runtime below a surviving wrapper leaves the same identified writer active
during initialization, idle residency and admitted execution. The empty
execution journal previously permitted replacement drivers in the first two cases. A
successful idle `Close()` also releases the journal while its Runtime owner and
escaped writer can still live; a distinct replacement namespace previously
started drivers. Schema 6 now blocks those replacement factories. The original
[CPU containment evidence](../runtime-process-containment-evidence-2026-09-06.md)
records the earlier behavior; the [backend startup evidence](../runtime-backend-incarnation-evidence-2026-09-06.md)
records the restriction and its remaining recovery limits.

Runtime journal schema 6 adds a member-wide backend startup record. Authorized
fresh initialization creates `UNSTARTED`. While holding the original journal
lock, `StartRuntimeServer` persists `UNRESOLVED`, a UUID v4, the SHA-256 digest of
the canonical launch manifest and a UTC timestamp before its first configured
backend factory. Both AUX factories share that one intent. Persistence failure
or cancellation after persistence cannot dispatch a factory. Initialization
failure, ordinary `Close()` and successful execution drain never clear it.

An `UNRESOLVED` or `LEGACY_UNKNOWN` journal reopens only a process-free recovery
endpoint. Readiness and fresh execution reject; historical pending execution
retains its drain-specific rejection. Offline journal status exposes this
lifecycle separately from execution counts. Ordinary serving rejects older
journal schemas; explicit `upgrade-v2`, `upgrade-v3`, `upgrade-v4` or `upgrade-v5`
preserves validated evidence and adds `LEGACY_UNKNOWN`, including for empty
history. No upgrade infers a first-use grant.

The UUID and launch digest identify startup intent within the journal; they do
not prove a container identity, device quiescence or physical containment. No
retirement or reset operation exists yet. Consequently, a durable Runtime that
has attempted model startup remains recovery-only on subsequent process starts,
even after externally observed PID 1 exit. Normal durable restart availability
is not complete.

The remaining durable lifecycle implementation must satisfy these obligations:

- Record a non-reusable backend/container incarnation under the held,
  Registry-bound journal before the first configured backend factory can create
  a process or load a model. Bind it to the exact member, device ownership and
  containment owner; include failed and interrupted initialization, idle
  residency, execution and shutdown. Pending execution count is not its state.
- Preserve an unresolved incarnation through startup failure, cancellation,
  ordinary `Close()`, lost responses and owner crashes. Recovery endpoints may
  retain histories and restrictions without loading another backend. Empty or
  fully drained execution history cannot clear unresolved backend ownership.
- Accept quiescence only from an independently validated observation of that
  exact prior containment incarnation. A proposed implementation must bind the
  trusted Node/container-runtime observation to immutable container identity,
  node incarnation and journal ownership, and validate its freshness/replay
  semantics before permitting a replacement. Missing or garbage-collected
  container metadata is not positive termination evidence.
- Do not use a new namespace, PID disappearance, successful process-group
  signaling, `Close()` success or acquisition of the journal lock as prior-owner
  retirement. Namespace inode numbers can be reused; they are diagnostics rather
  than durable incarnation identities. Do not release an incarnation while its
  PID 1 or any permitted external writer remains active.
- Keep backend/container retirement separate from signed Stage execution drain,
  input disposition, sealed output, capacity release and physical device
  quiescence. The CPU namespace experiment establishes none of the GPU or
  external-writer guarantees required to release a DeviceSet.

The durable startup restriction implements intent retention, but binding that
intent to a trusted containment owner and independently proving its retirement
remain unimplemented across epochs. The experiment and Pod contract assertions
do not generate a drain checkpoint or a Launch Receipt.

## Node container observation

The Node command `inspect-runtime-container` now reads the standard CRI v1 API
through a root-owned local socket and verifies the socket's kernel-reported peer
UID. It pins socket and directory identities, bounds the connection and read
intervals, and reads the kernel boot ID before and after observation. There is
no TCP endpoint, container-supplied boot ID or configurable expected server UID.

The caller supplies an exact full container ID, sandbox ID, Pod UUID/name/
namespace, container name and container attempt from trusted inventory. Both
list and status must match those identities. Repeated container/sandbox reads
reject visible changes, missing metadata, unknown state, inconsistent times,
missing namespace configuration, garbage collection, cancellation or connection
identity loss. The output retains the collection interval and the original
state/namespace meanings; it grants no continuing lease.

This observation is not a backend startup binding or retirement receipt. The
configured Node identity is a label, not an authenticated Fleet claim. CRI
`LinuxPodSandboxStatus` reports sandbox namespace options, not the actual PID 1
or OCI namespace configuration of an individual container. Runtime-specific
`verbose` info is intentionally not interpreted as a portable proof. Similarly,
the reported CRI image reference is not a release-image verification. `EXITED`,
or a sandbox reporting `CONTAINER` PID scope, cannot unblock schema-6 backend
ownership. The observer invokes no start/stop/remove/exec/image operation.

The next binding layer must authenticate the Runtime caller, resolve its actual
host process to the exact container and certified runtime configuration, and
bind that observation to the Registry journal pair and durable startup intent
before factory dispatch. Independent old-owner termination and replay-safe
retirement remain separate steps. See the [Node CRI evidence](../node-runtime-container-evidence-2026-09-06.md).

The subsequent [real containerd CPU experiment](../node-containerd-process-evidence-2026-09-06.md)
adds constraints on that binding. Under containerd `v2.3.1`, mutable container
`spec` metadata can differ from the running task's original configuration, even
across identical repeated reads. Its native API can recreate a task under the
same container ID and `CreatedAt`. A socket peer equal to the task init PID may
still be a non-init member of a shared PID namespace. A wrapper's Runtime child
is also distinct from the task/namespace owner.

Consequently, container identity, task PID, namespace inode and metadata spec
must not independently authorize backend startup or retirement. The binding
must retain the actual process lifetime, prove the supported namespace-init
boundary and authenticate configuration from the trusted launch path. Native
task recreation is not evidence of CRI/kubelet restart behavior; the pinned CRI
start guard accepts only `CONTAINER_CREATED`. This experiment exercises synthetic
callers and native containerd, not production Node binding or Runtime retirement.

The Linux Node library now accepts a challenge-bound local seqpacket from an
expected non-root UID/GID. `SO_PEERPIDFD` pins its connection opener and
`SCM_PIDFD`/`SCM_CREDENTIALS` authenticate the actual message sender. Both must
name the same live process. Inherited connections, excess descriptor rights,
truncated/oversized messages and cancellation reject without retaining received
descriptors. An opaque caller handle supports bounded process observations and
stops yielding live observations after process exit or handle closure. Its
payload remains subject to Registry/launch validation.

This library is exercised by real containerd CPU tests, but has no serving
command or durable startup-grant integration yet. The Runtime client must also
authenticate the Node endpoint. A process observation alone cannot authorize
backend dispatch, namespace ownership, containment or retirement. See the
[caller evidence](../node-runtime-caller-evidence-2026-09-06.md).

`RuntimeContainerObserver.ObserveCaller` now correlates that opaque live caller
with the exact CRI container and native running task on the same authenticated
socket. The namespace is fixed to `k8s.io`; the task init PID must match the
retained caller, which must also be PID 1 in a nested PID namespace. The adapter
supports the tested containerd `v2.3.1`, repeats CRI/task/process reads within
one bounded interval and rejects visible changes or lost identities. Actual
CRI CPU tests accept direct init, reject wrapper/shared-PID callers, observe
original-process exit, and confirm that CRI cannot restart the exited container
under the same ID. Missing metadata still yields no observation. See the
[combined observation evidence](../node-runtime-container-caller-evidence-2026-09-06.md).

This combined observation is not immutable launch configuration, complete
containment, a continuing lifetime lock or Registry startup authority. Effective
configuration authentication, trusted client/endpoint assembly, durable journal
and startup-nonce binding before factory dispatch, and independent exact-owner
retirement remain required. No observation clears unresolved backend ownership.

`VerifyRuntimeLaunchPlan` now checks the canonical bundle preimage against the
Registry-signed digest and derives the exact member manifest and Pod through
the existing Fleet schema-v2 mapping. Its opaque plan checks complete manifest
content and retains independent copies. `ObservePlannedCaller` requires that
plan, a matching authenticated manifest declaration, exact API-observed Pod
content, and matching CRI/native task/process identity and UID/GID. Container
and sandbox selection comes from the derived Pod's status and CRI, rather than
Runtime-supplied IDs. Repeated Pod reads and a final pinned-process check reject
visible replacement, drift or exit. See the
[launch-plan evidence](../node-runtime-launch-plan-evidence-2026-09-06.md).

The Registry signature authenticates historical configuration, not current
activation. The caller declares a manifest; the adapter does not prove that
those bytes were loaded or that its actual OCI configuration matches. The real
CRI positive control uses Registry/Pod fixtures and a synthetic helper image,
so it is not live Kubernetes/release-image conformance. Effective launch
attestation, endpoint/client assembly, actual journal-lock/startup-nonce binding
and independent retirement remain unimplemented. No factory consumes this
observation as a grant.

## Forwarded command lifetime

A member configured with a durable Worker journal must retain its actual
Registry-bound admission handle for each forwarded `PrepareStage`, `StartStage`
and `Status` call. `Status` renews execution authority and is not a read-only
recovery operation. Authentication, current authority, local Runtime identity
and execution-spec checks precede retention. Unresolved Runtime routes, missing
binding, closed/failed ownership or an observer without retention capability
reject before Runtime forwarding.

Each call owns an independent reference. `Close` returns busy while any input
admission handle or command reference remains. Cancellation alone does not
release a reference; the forwarding call must return first. Release is
concurrency-safe and idempotent, always rechecks actual journal ownership even
when the request was canceled, and changes no durable history. Lost ownership
rejects an otherwise successful response. Observed file replacement remains
failed after restoring the path. No admission mutex is held across the RPC.

`InspectExecution`, exact cancellation, floor installation, drain and
non-admission recovery retain their own peer and Runtime authority checks and
remain available after Worker journal failure. This guard introduces no Worker
journal lock around those operations. Authorized CancelStage and the monotonic
watchdog interrupt the current generation's execution-call context before
waiting for the Runtime execution lock. The later backend cancellation remains
serialized. This does not release the admitted operation or journal ownership:
an uncooperative backend must actually return before its reference is released.
Runtime repeats cancellation admission/target validation after acquiring that
lock; a pending interruption cannot bypass a subsequently installed floor.

Cancellation itself grants no lifetime. With a healthy Runtime admission above
its floor, a fresh compatible successor can authorize a stop using the existing
backend authority; it cannot replace the accepted grant or reset the watchdog.
After observed monotonic expiry, only an exact accepted or retained backend envelope may cancel.
This holds even when the forwarding Worker's journal has failed. Runtime
journal failure or a terminal floor continues to require an exact retained
envelope. An acknowledged successor request does not create exact inspection
or drain evidence for that successor; recovery keeps the actual backend
envelope and may query allocation-level checkpoints to recover its proof.

An unacknowledged renewal retains the accepted grant and confirmed backend
envelope in Runtime memory and schema-6 execution history. Further distinct
renewal rejects until confirmation. Every execution call persists its candidate
pair before backend entry; backend acknowledgement is persisted before success
is returned. Expiry/cancellation during persistence cannot authorize dispatch,
and cancellation during acknowledgement persistence cannot return success.
Cancellation/watchdog recovery inspects both exact identities and requires one
unambiguous observation before backend Cancel, then rechecks request eligibility.
FAILED with unproven reuse remains cancellable. The authenticated leader can use
these recovery operations after Worker journal closure and terminal-floor
installation, but must possess the relevant signed envelopes. Exact drain
returns proof only for the envelope actually drained. A caller with only the
latest allocation envelope can use authenticated `InspectAllocationExecution`
to discover the live backend envelope. The read validates the returned signature,
same-execution relationship and exact nested observation independently at both
forwarding boundaries. It does not publish pending candidates as confirmed
identities, renew execution, take the execution lock or produce drain proof.
Missing live history stays unknown; a replacement Runtime cannot inspect an old
epoch. Restart can read the persisted candidate pair using the local
`InspectRetainedAllocationAuthorities` API or the owner-checked Runtime UDS and
leader-authenticated member mTLS `InspectStageAllocationAuthorities` RPC. The
historical scope names a trusted current journal reader separately from the
original execution. The response echoes the query digest and returns independently
signed original/accepted/confirmed envelopes, which need not equal that query.
Both forwarding boundaries validate wrapper fields, current reader identity,
candidate signatures, immutable execution relationships and the monotonic
original-to-confirmed-to-accepted interval. An accepted renewal requires a prior
confirmed envelope; an initial unconfirmed intent must equal the original.
Absent history stays unknown, including an ACCEPTED response; legacy history
contains only its original. Cancellation before forwarding, during result
validation or after a downstream reply prevents a successful read. These reads
report journal history, do not inspect a backend or restore an active execution,
and cannot release pending writer restrictions. Physical recovery across Runtime
epochs remains a separate requirement.

Runtime journal schema 6 retains the original allocation plus at most one
accepted and one confirmed signed envelope. Canonical encoding, signatures,
immutable execution scope, monotonic renewal relationships and drain membership
are validated on recovery. The 32-execution and 12 MiB journal bounds remain.
Explicit `upgrade-v2`, `upgrade-v3` and `upgrade-v4` preserve prior IDs, scopes,
watermarks, floors and proofs; missing candidate history remains unknown.
Ordinary serving never upgrades implicitly, and Registry-bound serving rejects
every upgrade flag. The read API cannot infer an accepted/confirmed envelope
for migrated history from its original grant or drain proof alone.

Terminal scratch retirement may recover an admitted live execution only after
Worker input writers finish and every signed Runtime floor acknowledgement is
validated. It first reads durable exclusion evidence. If non-admission cannot
be established, the original resident Runtime may discover the exact live
envelope, cancel it unless already STOPPED or OUTPUT_SEALED, and drain that exact
identity. The collector re-reads the allocation checkpoint using its original
query; it never relabels another envelope's correlation digest. Replacement
Runtime owners may replay historical proof but cannot execute this live recovery
against the previous epoch. Partial or invalid proof leaves retirement at INTENT.

An exact durable drain can reconcile a CANCELING or unclassified FAILED slot
only after a validated exact backend STOPPED observation. Explicit
`WorkerReusable=false` survives Cancel and drain, and only a validated Status
may clear that health denial. Inspection alone cannot release a slot. A terminal
execution at or below the installed floor blocks readiness across shared
resident profiles until it is reusable. A saved drain followed by a failed stop
observation remains retryable at the original current Runtime without repeating
the backend drain. Known OUTPUT_SEALED may release its slot only while preserving
its validated local receipt. Receipt persistence and historical writer recovery
after loss of live identity remain separate requirements.

Retention covers the forwarding call only: a timed-out UDS RPC can return while
backend descendants remain active. It proves neither physical containment nor
device reuse safety. Runtime durable admission, terminal restrictions and writer
drain evidence remain required for uncertain execution and later reclamation.

## Terminal outcomes

A claim has exactly one of these database states:

| State | Evidence | Permitted continuation |
| --- | --- | --- |
| Pending | Original immutable claim; no terminal row | Original local pair may be reported; explicit abandonment may be requested |
| Recorded | Immutable nonzero journal pair and timestamp | Exact receipt replay, signed binding lookup and independently validated local recovery |
| Abandoned | Immutable fence epoch and abandonment timestamp | Exact abandonment replay and history inspection |

An abandoned claim can never accept a receipt or acquire another fresh grant.
A recorded claim cannot be abandoned. Both terminal outcomes preserve the
original claim, node, actor and bundle. Unrecorded local initialization cannot be
completed by reconstructing a pair from a claim alone.

`AbandonWorkerBootstrap` is an explicit command for the original authenticated
Node Agent. Its database transaction locks the Worker, then the claim. It
requires either the original unobserved PROVISIONING Worker or that same
unobserved Worker already FENCED at original epoch + 1. Any retained Worker epoch,
member, residency or active device binding rejects the operation. PROVISIONING
also requires the original Control session and no observation or device set.

The transaction fences the whole Worker and permanently rejects completion of
the named claim. Fencing increments the Worker epoch once and prevents the old
Worker from registering. The deferred synchronous-quorum check covers the
abandonment insert; failure rolls back the fence and abandonment together.
Receipt recording follows the same Worker-before-claim lock order, so concurrent
record/abandon calls select only one terminal outcome. Exact replay returns the
original outcome without changing its timestamp or fencing again.

For a multi-member Worker, fencing applies to the entire Worker while the
abandonment row applies to the named claim. Other pending member claims remain
pending until their own recorded or abandoned outcome exists. This operation
does not reinterpret a peer's already-recorded pair or acknowledge its cleanup.

## Node command and recovery

`vela-node-agent bootstrap --action abandon --request-id <original UUID>` uses
the same Fleet address, server name, CA and registered client certificate/key
options as `history`. It accepts no preparation directories, manifests or
verifier settings. It inspects and mutates Registry authority only, preserving
all local files, including incomplete journals and unresolved inputs. The JSON
result identifies the original claim and its fenced epoch/timestamp.

Response loss does not undo a committed abandonment. A later invocation uses the
same request and principal to inspect or replay it. A failed/canceled command
does not print successful completion. `history` exposes an optional
`abandonment`; pair and abandonment together are invalid authoritative data.
The transport rejects malformed identities, epochs, timestamps and unknown
protobuf fields. Signed binding retrieval requires an actual recorded pair.

Schema 94 adds the abandonment table and a versioned history query. The original
history query remains available for old readers but confers no ability to
complete an abandoned claim. Internal pre-94 mutation functions are inaccessible
to the Fleet runtime role. Down to schema 93 is allowed only without abandonment
history; retained terminal outcomes prohibit rollback.

Database-only recovery quiescence counts claims with neither a recorded nor an
abandoned outcome. The closed recovery gate still prevents new claims; completing
or abandoning an existing claim can finish the database drain. A zero inventory
does not prove node/process/device drain, filesystem consistency or physical
backup completeness outside the database.

## Replacement and remaining work

Abandonment is permission to stop waiting for the named database completion. It
is not proof that an initializer, input writer or backend descendant has stopped,
and does not authorize filesystem deletion, device reuse or a replacement
journal under the same identity. A delayed initializer can still retain local
files, but cannot publish a receipt for its abandoned request.

Replacement needs independent containment and approved new Worker/member
identities with isolated persistent namespaces. The existing signed journal
identity binding does not replace those requirements. Lost Node credentials,
already-observed Workers, ownership loss after discovery, failed-backend
containment, successful terminal scratch retirement and bounded reclamation
remain separate work. Default durable Fleet provisioning stays disabled until
its complete activation and recovery contract is validated.
