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
journal lock around those operations. Runtime operation serialization can still
delay cancellation behind a blocked backend call.

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
