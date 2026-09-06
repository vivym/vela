# Durable Runtime renewal candidates

This CPU-only increment follows `6732ba3`. Runtime execution journal schema is
now **5**. PostgreSQL schema 94, Worker journal 5, Registry binding 1 and
Production Gates `0/9` are unchanged.

## Reproduced gap

The prior Runtime journal recorded the original allocation and final drain
authority, while accepted and backend-confirmed renewal candidates existed only
in memory. A test inspects the journal at backend Status entry. Both failure
before renewal application and response loss after application reproduced
`renewal entered backend without durable authority candidates` on the preceding
implementation. A restart could retain its pending execution restriction but
lose the signed renewal envelopes needed to investigate that execution.

## Contract and implementation

Each new retained execution now has an accepted envelope and an optional
confirmed envelope, in addition to its immutable original authority and drain
checkpoint. An initial Prepare records its original as accepted before backend
entry and leaves confirmation absent until the backend acknowledges. A distinct
renewal is persisted with the previous confirmed envelope before dispatch.
Confirmation is persisted before returning success. Identical pairs do not
rewrite the journal. Only one unacknowledged renewal is allowed, preserving at
most two candidate identities rather than an unbounded renewal log.

The Service uses the existing admission mutex, private lifetime-locked journal,
canonical protobuf/JSON representation and atomic rename/directory-sync protocol.
No journal lock spans a backend call. Expiry and caller cancellation are checked
after pre-dispatch persistence. Confirmation persistence also checks the caller's
context before and after the write: a reproduced canceled-during-sync case
initially returned ACCEPTED and now rejects while retaining any already written
historical fact. Sync failures latch admission recovery. A backend may already
have accepted the grant when confirmation persistence fails; the durable
pre-dispatch pair still identifies both possible envelopes.

Recovery verifies canonical signatures and immutable execution scope for every
candidate. Accepted and confirmed envelopes must lie in the original monotonic
renewal interval. Confirmation cannot regress, drained candidate history cannot
change, and a drain checkpoint must bind one of the recorded candidates when a
candidate pair exists. The existing 32-execution and 12 MiB journal bounds remain.

`Supervisor.InspectRetainedAllocationAuthorities` is a local, read-only API. It
validates a historical query and journal ownership, then returns independently
decoded original/accepted/confirmed signed envelopes for that allocation. Caller
mutation cannot alter retained state. It never inspects or calls a backend,
installs authority, rebuilds an active Service, or releases capacity. Historical
queries can read after a Runtime epoch change while old-epoch execution/drain
entry remains rejected. This is journal history, not proof of live execution,
current backend identity, writer drain or device health.

Ordinary recovery requires schema 5. Explicit `UpgradeV2`, `UpgradeV3` and the new
`UpgradeV4` preserve journal IDs/scopes, original grants, watermarks, floors and
prior proofs. Legacy records retain no candidate pair: the original grant or a
drain proof does not establish which renewals previously reached a backend.
The offline Runtime journal command accepts `--action upgrade-v4`. Initialize
and upgrade modes remain mutually exclusive, and Registry-bound serving rejects
all upgrade flags. No database, protobuf, RPC or Registry binding schema changes.

## Validation

- Backend-entry tests inspect the actual journal before Status executes. Both
  renewal failure outcomes retain the exact signed candidate pair; a successful
  retry persists confirmation without changing the original allocation.
- Restart tests recover both uncertain and confirmed pairs at a new Runtime
  epoch. Returned values tolerate caller mutation; pending history still blocks
  readiness and Prepare, and old-epoch drain never enters the replacement backend.
- Persistence tests inject directory-sync failure before dispatch and after
  backend confirmation, expiry during pre-dispatch persistence, and caller
  cancellation during dispatch/confirmation persistence. They check backend call
  counts, rejected success, retained signed identity and restart restrictions.
- Subprocesses call `os.Exit(71)` immediately after successful directory sync at
  the dispatch and confirmation boundaries. The parent reopens the same journal
  and validates its candidate pair and restrictions. This exercises process exit,
  not a physical power-failure or storage-hardware durability campaign.
- Ten damaged-history cases reject altered signatures, missing accepted data,
  unrelated accepted/confirmed grants, future confirmation, regressed acceptance,
  unconfirmed renewal, candidate fields in a legacy schema and drain outside the
  recorded pair. Rejected recovery leaves file bytes unchanged.
- Explicit schema-2/3/4 upgrades preserve legacy proof and pending restrictions.
  Schema-4 pending and drained records both return original-only candidate
  history. Registry-bound startup rejects `UpgradeV4` before epoch/backend entry.
- Full `go test ./...` passes. Final Runtime and Worker Agent race suites pass in
  `90.001s` and `94.792s`; Runtime/member transports, both Runtime/Worker commands
  and Worker bootstrap race suites also pass. Focused renewal process/corruption
  tests pass before the final full rerun.
- Four PostgreSQL integrations pass in `45.571s`: authenticated terminal
  disposition, replacement-Runtime terminal recovery, latest-renewal lease
  cancellation/expiry, and compiled Worker bootstrap binding. Bootstrap checks
  now expect the actual Runtime journal schema 5 while preserving the recorded
  Registry identity.
- `make lint` reports zero issues; `make verify-generated` and diff checks pass.
- Linux arm64 tests pass using `/tmp/vela-renewal-journal-runtime-linux.test` and
  `/tmp/vela-renewal-journal-worker-linux.test`, including compiled `h3-encoder`
  renewal recovery from `/tmp/vela-renewal-recovery-linux`. The image is
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
  Both containers use UID/GID 65534, no network/capabilities, read-only root and
  binaries, private tmpfs and `no-new-privileges`.

## Remaining work

The authenticated Worker/member recovery interface does not yet expose the
retained candidate read. Live `InspectAllocationExecution` still reports a live
observation and cannot turn old journal history into an observation at a new
Runtime. Actual cross-epoch writer cleanup/containment remains unresolved; an
empty replacement backend is insufficient proof. Durable unhealthy-worker state,
sealed receipt recovery, bounded history reclamation, renewal write-amplification
measurements and default Fleet durable activation also remain open.

No remote deployment, push, GPU execution or Production Gate acceptance occurred.
The overall architecture/correctness goal remains incomplete.
