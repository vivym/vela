# Peer discovery checks both Worker and Runtime journals

This increment follows `9045779`. PostgreSQL schema remains 94, Worker journal 5,
Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Counterexamples

Three actual command-composition regressions showed the Leader completing
durable Stream construction when its pinned follower used a nondurable Worker,
had closed its Worker journal, or had replaced the Worker state file with the
same bytes on a new inode. Its Runtime still held a valid signed journal, so
Runtime-only discovery did not detect the missing Worker side.

## Implemented contract

`FileAssignmentAdmission.InspectJournalBinding` retains the verified Registry
binding from startup. Under the existing admission mutex it checks the actual
file bindings, then verifies the signature and Worker/member/epoch/journal/scope
against the held state. It returns a defensive copy and changes no durable
history. Deferred Runtime routes or pending input history do not block identity
observation. Closed, unbound or failed state returns no evidence; a detected
file replacement remains failed after restoring the original path. Cancellation
does not return success or poison an otherwise usable journal.

The member service receives this actual admission handle from durable Worker
command assembly. It authenticates the pinned Leader before observing local
state, checks the Worker journal before Runtime discovery, and checks the same
handle again after the RPC. Its response includes `worker_journal_binding` only
when the Worker binding is unchanged and its immutable claim/pair matches the
Runtime's binding. No admission mutex is held across the Runtime RPC. Closing
the handle while that RPC is in progress prevents a successful response.

The durable Leader configures its member client with independent Registry public
keys. That client requires both Worker and Runtime evidence, verifies both
signatures, checks the requested Worker/member/epochs, and requires the same
immutable Registry pair. Each follower uses its own pair. Missing, malformed,
untrusted, mismatched or identity-only replies cannot reach Stream assembly.
Existing mTLS identity pinning, route/topology validation and Runtime epoch
checks still apply. Old ordinary discovery remains available without a Registry
verifier, but cannot satisfy a durable Leader.

## Validation

- The three original counterexamples now reject before Leader Stream assembly.
  An additional actual mTLS-to-UDS test closes the Worker journal after Runtime
  discovery returns but before the member response; it also rejects.
- Valid single-member and two-member CPU command composition, Acquire,
  deferred-route binding, restart history checks and shutdown still pass.
- Journal tests cover pending/deferred state, missing Registry binding,
  cancellation, closed state, replaced state/lock/directory, sticky failure,
  immutable startup/response copies, retained file contents and exclusive locks.
- Transport tests cover observation before/after forwarding, authentication
  before journal access, closure/change during RPC, missing Runtime evidence,
  wrong scope, tampered signatures, unknown fields and independently signed
  mismatched journal pairs or Worker/member/epochs. Existing discovery recovery,
  cancellation and copy-isolation cases also pass.
- PostgreSQL/TLS plus the compiled Node binding command pass
  `TestWorkerBootstrapBindingCommandUsesCommittedRegistryIdentity` (`18.977s`).
  The actual held Worker journal now reports the same committed pair consumed by
  the CPU Runtime; Registry rows and local history remain unchanged.
- Full `go test ./...`, the three related package race suites, `make lint`,
  integration-tag changed-file lint, protobuf breaking-change checks against
  `9045779`, and `git diff --check` pass.
- Linux arm64 Worker journal observation and durable command composition pass
  as UID/GID 65534, without network/capabilities, with read-only root/binary,
  tmpfs and `no-new-privileges`. Image:
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

Focused evidence: `/tmp/vela-peer-journal-discovery-regressions.jsonl`,
`/tmp/vela-peer-journal-core-linux.log`, and
`/tmp/vela-peer-journal-worker-linux.log`. Linux test binaries are
`/tmp/vela-peer-journal-core-linux.test` and
`/tmp/vela-peer-journal-worker-linux.test`.

## Remaining work

The additive protobuf field establishes discovery-time observation. It does not
create a continuing lease, prove physical process containment or grant device
reuse. A historical signature still depends on the trusted serving code to
check the actual local handles. Ownership loss after discovery and its effects
on subsequent forwarded execution/recovery commands require further lifecycle
validation. Default Fleet durable activation, physical replacement, pending
input writer recovery, failed-backend recovery, sealed receipts, terminal
retirement and bounded reclamation remain open. No GPU, remote deployment, push
or Production Gate acceptance occurred. The full correctness goal is incomplete.
