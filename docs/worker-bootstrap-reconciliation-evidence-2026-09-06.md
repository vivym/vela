# Registry-backed local journal-pair reconciliation

This increment follows `4b1ab25`. Database schema remains **92**, Worker journal
5, Runtime journal 4, local bootstrap operation 1, materialization 2,
launch/Fleet manifest 2 and floor RPC v1/v2. Production Gates remain `0/9`.

## Recoverable evidence

An absent local pair file has two materially different cases. If Registry has
already recorded the original complete journal pair, it provides independent
identity evidence for local recovery. If it only records first-use consumption,
it does not establish which journal initializers completed or permit another
initialization. The new recovery path preserves that distinction.

`workerbootstrap.ReconcileRecordedPair` requires the original validated local
operation, an authenticated read-only `HistoryReader` and successful recovery of
both original journals. Its reader interface has no Claim or RecordReceipt
method. Operation opening explicitly disables creation; journal configuration
has no initialize or upgrade flags.

The function holds the operation lock, then the Worker and Runtime journal
lifetime locks in a fixed order. While both journals remain held, it reads
Registry history and matches request, original actor/node, Worker/member/epochs,
bundle digest, journal IDs and scope digests. History must have no fresh grant and
must contain a complete original receipt and timestamp.

An existing local pair must exactly match; malformed, partial or conflicting
data is preserved and rejects. Only a missing pair may be exclusively created,
fsynced and directory-synced from the matching Registry receipt. Replays retain
the original timestamp. Cancellation/failure yields no successful result,
including after a pair has already been published. The next invocation
revalidates the same evidence instead of repeating initialization.

`WithPreparedAssignmentJournal` and `WithPreparedExecutionJournal` extend the
existing offline inspectors with a callback executed under exclusive journal
ownership. They expose no admission/backend handle and revalidate bindings
before releasing the lock, including after callback failure. Existing standalone
preparation functions use these same wrappers. Recovery retains the existing
fsync and unpublished-temporary-file behavior, not read-only filesystem semantics.

The Node Agent adds explicit `--action reconcile-pair` with the same private
preparation configuration and certificate-derived identity. It does not query
or mutate journals through `history`, and ordinary `prepare` retains its rule
that a retained operation never repeats Claim or initialization.

## Verification

Passed:

- Full `go test ./...`, `make lint`, and changed-file integration-tag lint.
- Race tests for the coordinator, both journal modules and the Node Agent command.
- Original-pair byte restoration, unchanged operation/journal files and inode
  identities, idempotent replay, and competing opens proving both lifetime locks
  remain held during Registry lookup.
- Rejection at every incomplete original preparation boundary, including two
  initialized journals without a Registry receipt. No missing operation or
  journal is initialized by reconciliation.
- Mismatched claim/receipt identity, actor, node, epochs, bundle digest, journal
  ID/scope, missing receipt/timestamp, and malformed/conflicting local state.
- Cancellation before lookup, after history validation and after pair publication;
  abrupt subprocess exits before/after publication; retry with the original IDs.
- Inspector callback failure releases locks; replacing a held journal state file
  with an identical-byte new inode still rejects on final binding validation.
- Actual compiled Node Agent processes against PostgreSQL and TLS: missing-pair
  recovery after normal bootstrap and committed receipt-response loss; repeat
  reconciliation issues no Claim or RecordReceipt RPC and leaves Registry rows
  unchanged. Missing-operation and unrecorded-initialization commands reject.
  Existing normal/lost-claim/lost-receipt/timeout/SIGTERM cases remain verified.
- Linux arm64 reconciliation tests, including child process exits, as UID/GID
  65534, with no network/capabilities, read-only root/binary, tmpfs and
  `no-new-privileges`. Image:
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
- `git diff --check`.

The expanded command test first exposed a test-harness mistake: its signal
injector also waited for a committed Claim during the new missing-operation
reconciliation precheck, which makes no Claim. The parent now selects signal
injection only for `prepare`. The failing log is retained and the SIGTERM case
passes after the harness correction; command cancellation behavior was unchanged.

Local logs: `/tmp/vela-bootstrap-reconciliation-command.jsonl` (four successful
scenarios and the original harness failure),
`/tmp/vela-bootstrap-reconciliation-signal.jsonl` (corrected signal case), and
`/tmp/vela-bootstrap-reconciliation-linux.log`. Linux test binary:
`/tmp/vela-worker-bootstrap-reconcile-linux.test`.

## Remaining authority

This restores independently recorded metadata; it does not reconcile first
initialization that has no complete local pair or Registry receipt. That case
still needs an explicit authority lifecycle for rejection/replacement and
containment. It cannot be solved by a missing-file check or another fresh grant.

Serving processes still need to match their actual lifetime-locked journals to
the Registry-recorded pair before execution. Offline inspection locks end on
command return and confer no readiness, writer drain or serving capability.
Fleet default provisioning/activation therefore remains disabled. Missing or
pending writer/drain evidence, failed-backend containment, sealed receipt
recovery and bounded reclamation remain separate requirements.

These are CPU/mock and local PostgreSQL/TLS checks. They do not simulate physical
power loss or validate GPU behavior. No push, remote deployment or production
acceptance occurred, and the overall architecture audit remains incomplete.
