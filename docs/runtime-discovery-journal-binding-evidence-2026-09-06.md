# Durable Worker discovery verifies the Runtime journal binding

This increment follows `612a8e2`. PostgreSQL schema remains 94, Worker journal 5,
Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Counterexample and repair

A command-composition regression reproduced a durable Worker constructing its
Stream against a nondurable Runtime: discovery returned matching identities and
epochs, and startup succeeded without execution journal ownership. The existing
Worker startup check protected its own journal but did not verify the Runtime
side of the recorded pair.

`DiscoverRuntimeIdentities` now optionally returns `journal_binding`. The serving
Runtime retains its independently verified startup binding and, under the shared
execution admission mutex, checks the actual held journal's directory, lock,
identity, state inode and contents. It then verifies the signed Runtime journal
ID/scope and Worker/member/epochs before returning a defensive copy. Closed,
failed or replaced state returns no identities. Detecting lost ownership remains
fatal even if the original files are restored. Canceled calls return no success.

The durable Worker requires this evidence before local route binding or member
endpoint publication. It verifies the signature using its separately configured
Registry public keys, the expected Worker/member/epochs, and equality of the
local immutable claim and journal pair. The Leader applies the same signature
and member checks to each remote Runtime before Stream construction. Each peer
has its own Registry pair; the Leader's local pair is not reused for peers.
Member transport forwards the evidence intact and retains its existing mTLS
Leader authorization and pinned identity checks. The Worker caller owns signature
validation. Discovery rejects unbounded or unknown response/identity fields.

Discovery does not require readiness. An intact journal awaiting historical
writer drain remains discoverable, so the existing recovery-before-readiness
path can run. Discovery neither advances watermarks nor clears pending history.
Nondurable and unbound-journal modes can still expose ordinary identities, but
cannot satisfy durable discovery. A historical signature alone is not a fresh
physical process attestation; the live ownership assertion depends on the trusted
Runtime implementation reached through the existing UDS/mTLS boundary.

## Validation

- Local nondurable Runtime rejection occurs before durable Stream construction
  and releases the Worker journal. A pinned mTLS follower backed by a nondurable
  Runtime also rejects at Leader startup. Valid distinct member pairs succeed.
- Worker command fixtures now run the actual CPU fake Runtime server with both
  prepared journals and a signed pair. Startup, Acquire, shutdown, peer discovery
  and pending-history recovery regressions pass.
- Actual UDS tests cover bound, unbound-journal and nondurable servers; canceled
  calls; closed Supervisor; missing state, lock and directory; identical-byte
  state replacement; persistent failure after restoration; and defensive copies.
- Signature tampering, unknown keys/fields, wrong Worker/member/epochs, mismatched
  local pair/claim, missing evidence/verifier and oversized replies reject.
- Pending historical writer recovery returns verified discovery while readiness
  remains false and journal bytes remain unchanged.
- PostgreSQL/TLS and the compiled Node binding command supply a committed pair
  to actual Worker/CPU Runtime composition. The independently verifying discovery
  path passes `TestWorkerBootstrapBindingCommandUsesCommittedRegistryIdentity`
  (`18.526s`), retaining both journal locks and unchanged Registry/scratch state.
- Full `go test ./...`, related four-package race tests, `make lint`, changed-file
  integration-tag lint, `buf breaking --against '.git#ref=HEAD'` against the parent
  revision, and `git diff --check` pass.
- Linux arm64 Runtime discovery/recovery and durable Worker composition pass as
  UID/GID 65534, with no network/capabilities, a read-only root/binary, tmpfs and
  `no-new-privileges`. Image:
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

Focused evidence: `/tmp/vela-runtime-discovery-regressions.jsonl`,
`/tmp/vela-runtime-discovery-linux.log`, and
`/tmp/vela-worker-discovery-linux.log`. Linux test binaries:
`/tmp/vela-runtime-discovery-linux.test` and
`/tmp/vela-worker-discovery-linux.test`.

## Remaining boundaries

The protobuf field is additive. Durable assembly now requires Runtime and member
transport versions that preserve this evidence; an older peer cannot silently
fall back to identity-only discovery. This check is an observation at discovery,
not a lease guaranteeing future readiness or physical drain. Runtime commands
still enforce current authority and journal ownership at execution admission.
Remote Runtime evidence does not independently prove that the forwarding Worker
currently holds its own assignment journal.

Fleet durable activation, process containment/replacement, pending input writer
recovery, failed-backend recovery, sealed receipts, terminal retirement and
bounded reclamation remain open. No GPU, remote deployment, push or Production
Gate acceptance is part of this increment. The overall correctness goal remains
incomplete.
