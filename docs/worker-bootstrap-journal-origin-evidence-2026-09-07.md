# Retain original bootstrap journal storage

This repair follows `dfb641d` on `feature/vela-mock-hardening`. It closes a local
bootstrap identity gap; it does not establish independently trusted Node startup
authority. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1,
release bundle 3 and Production Gates `0/9` remain unchanged. A new local
`journal-origin.json` record has schema 1.

## Reproduced defect

Bootstrap originally retained journal UUIDs and scopes in `pair.json`. Each
journal also stored its own lock inode. Replacing a lock file, copying its
contents and updating only that journal's self-declared lock identity preserved
all UUID/scope checks. Reopening validated the replacement against its own
mutable metadata and accepted it as the original journal.

Eight CPU counterexamples reproduced this for both Worker and Runtime:
immediately after their preparation, after pair publication, during receipt
replay and during reconstruction of missing pair metadata. All eight returned
success before this repair. Fresh cases committed a receipt; replay called the
receipt API again; reconciliation recreated the pair without detecting the new
lock inode. The fixture preserves canonical JSON, UUIDs, scopes and other
journal fields so these are identity failures, not malformed-input tests.

## Repair and recovery

Both journal preparation APIs now return a `storage` observation containing the
actual held directory and lock device/inode. These values come from the existing
held journal objects, with their usual validation before release. They are local
observations, not fields added to the signed Registry protobuf.

After fresh authorized initialization, bootstrap writes
`bootstrap/journal-origin.json` before `pair.json`. The canonical private,
single-link record binds the original request, journal UUIDs/scopes and both
storage observations to the operation's already retained directories. Creation
uses no replacement; file and directory fsync precede the `origin-durable`
boundary. Replay rechecks the record and reconfirms its durability. Visible
replacement or byte changes during an operation reject.

Receipt publication/replay compares both actual recovered storage identities
with that retained initialization record while holding both journal locks.
`reconcile-pair` requires the same record, matches it to Registry history and the
actual recovered journals, and then recreates only missing pair metadata.
Changed journal lock metadata alone can no longer establish original storage.

Missing, malformed, duplicate/unknown-field, non-private, hardlinked or internally
inconsistent origin records reject. Neither replay nor reconciliation reconstructs
one from the later journal or Registry UUID/scope history. A crash after
`origin-durable` but before pair publication retains incomplete initialization;
it grants no repeated Claim or automatic continuation of first use.

This also affects older complete local bootstrap state without an origin record:
bootstrap replay/reconciliation now fail closed with no automatic adoption or
upgrade. Preserve that evidence for independently authorized reconciliation;
deleting/reinitializing journals is not a repair. Direct offline journal
inspection and existing journal schemas retain their prior contracts. The
preparation status JSON adds `storage`; Registry history/signatures do not.

## Validation

- All eight replacement counterexamples now reject before any new receipt call
  or pair reconstruction. Claim count remains one.
- Nine missing/corrupt/untrusted origin scenarios reject both receipt replay and
  pair reconciliation without changing evidence or Registry authority.
- Interrupted and actual process-exit initialization includes `origin-durable`.
  Retried operations preserve incomplete state and never reclaim first use.
- Origin-file replacement during receipt call or after receipt commit rejects;
  restoring the retained original file allows the existing receipt recovery path.
- Full `go test ./...`, `go vet ./...`, full golangci-lint `v2.13.1`, and Linux
  integration-tag lint for the affected modules/Node command pass with zero issues.
- Host race suites for bootstrap, ModelRuntime, Stage Worker and journal binding
  pass. All 19 non-helper bootstrap main tests also pass in the dedicated Linux
  static race binary without skips or race reports.
- Four PostgreSQL/mTLS integration main tests pass under race in 58.82 seconds:
  actual Node command across processes, committed Registry binding through the
  serving path, local paired-journal initialization and lost Claim/receipt replies.
  The command subprocesses use the ordinary compiled entrypoint.
- Linux/arm64 Node CPU wrapper: 57 mandatory main tests and both volatile-state
  reset scenarios pass without skips in 166.76 seconds, including actual Runtime
  startup with the extended preparation status.

The CPU image is
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`;
Docker is `28.3.2`, containerd `v2.3.1` and runc `1.4.2`. The Linux static race
build uses Go `1.26.7` image
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`.
Static glibc NSS warnings concern only that test build. No GPU or deployment was
used; process crashes do not simulate physical storage power loss.

Logs are retained under `/tmp/vela-startup-validation.syiSMk`:

| File | SHA-256 |
| --- | --- |
| `journal-origin-red.log` | `d88d0568f2dd6ea3b75e159d9c0d969eacb468cff67771a27d851570d21938a1` |
| `journal-origin-integration.log` | `b65ef7f3c64e9c83f445024ef497a216eaa2ed76c24aa6bf3eff85517ab9f42b` |
| `journal-origin-linux-cpu.log` | `efa5421fc91f9db86ac78b1cf50762f18941c1b5300bae358603c5e02f73f986` |
| `journal-origin-linux-race.log` | `d76f673a8cd76b0e9134d6a3822a38432d85b319d8f53adf8c59c1d6c586d30e` |

## Remaining trust boundary

The bootstrap command still runs as the intended journal-owner UID, and the new
origin record belongs to that same owner. This protects against rebinding mutable
journal metadata independently of the retained origin, not coordinated rewriting
of the operation/origin/journals by that trusted owner or an administrator.
Device/inode observations do not independently exclude inode reuse, remount
changes or administrative rollback. Parsing a preparation status JSON document
does not authenticate its origin.

Current Fleet Pods mount the entire scratch root into Worker and Runtime and
their initializers manage it under UID 10001. They still do not assemble the
durable bootstrap/Node startup protocol. A production issuer therefore needs
independently protected Node initialization records and an explicitly qualified
provisioning/mount/ownership transition, as well as current activation, effective
launch, at-most-once permission and exact-owner retirement. This local repair
does not issue startup permission, reset a Runtime or release a DeviceSet.
