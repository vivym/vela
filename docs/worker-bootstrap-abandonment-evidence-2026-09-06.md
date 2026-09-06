# Explicit terminal abandonment for unobserved bootstrap authority

This increment follows `70dfcae` and advances the database to schema 94. Worker
journal 5, Runtime journal 4 and Registry binding 1 are unchanged. Production
Gates remain `0/9`.

Previously a committed first-use claim without a complete local pair or Registry
receipt could never complete database quiescence. Repeating initialization would
violate the unique first-use boundary. The new terminal outcome lets the original
authenticated Node Agent explicitly reject future completion while preserving
the claim and uncertain local state.

The [lifecycle contract](specs/0053-worker-bootstrap-lifecycle.md) defines pending,
recorded and abandoned outcomes. `AbandonWorkerBootstrap` accepts only an
unrecorded claim for an unobserved PROVISIONING Worker, or that same never-observed
Worker already fenced at original epoch + 1. Worker epoch/member/residency/device
history rejects. The transaction fences the Worker and inserts an immutable
abandonment with a deferred synchronous-quorum guard. Receipt recording and
abandonment use the same Worker-before-claim lock order. They cannot both commit.

Original node and actor scope the database operation and history lookup. The
Fleet RPC derives them from the registered mutual-TLS principal. The Node
`bootstrap --action abandon --request-id ...` command accepts no scratch or
preparation settings. Its outcome remains queryable and replayable after lost
responses; it does not read or change local journals or unresolved inputs.
Bindings cannot be signed for an abandoned claim, and conflicting pair plus
abandonment history is rejected by the service/transport/reconciliation paths.

Schema 94 retains the original read-only SQL history query and adds a v2 query.
The old mutation implementation is only callable by its NOLOGIN owner behind the
new locked guard. Down preserves ordinary claim/receipt history but rejects when
any abandonment exists. Database-only quiescence excludes claims with either
terminal outcome. It does not declare physical node or filesystem quiescence.

Passed:

- PostgreSQL tests for principal isolation, exact abandonment replay, late
  receipt rejection, no renewed first-use grant, immutable rows, blocked rollback,
  inaccessible old mutation functions, and quiescence while Admission is closed.
- Recorded and observed Workers reject abandonment. A previously fenced,
  never-observed Worker terminates without another epoch increment, and the old
  Worker observation cannot reactivate it.
- Eight competing abandonment/receipt calls select one immutable outcome;
  losing calls report authority conflicts. Commit-quorum failure returns no
  terminal result and preserves the original PROVISIONING Worker and claim.
- Real Node executable plus PostgreSQL/mutual TLS: lost Claim response followed
  by explicit abandonment, wrong-node/same-node-other-Agent rejection, lost
  abandonment response and exact replay, unchanged scratch file contents and no
  repeated Claim. The signed-binding RPC rejects abandoned authority.
- Malformed response identities, fence epochs, timestamps, unknown protobuf
  fields and mutually exclusive terminal outcomes reject. Reconciliation cannot
  reinterpret abandoned history as recorded-pair evidence.
- Full bootstrap integration suite: 16 top-level tests, 44 passing test/subtest
  results, 84.711 package seconds. Related race-enabled
  Registry, database recovery, local bootstrap and abandonment selection:
  125.918 seconds, 35 passing test/subtest results, no failures.
- A source database containing one claim and one abandonment was captured and
  restored into independent PostgreSQL 17. Dynamic table fingerprints retained
  the exact terminal rows; catalog checks preserved owners, grants and authority
  function bodies; Admission remained closed. Elapsed: 10.599 seconds. This is a
  database restore receipt, not a physical host restore or Production Gate.
- Full `go test ./...`, related module/command race tests, `make lint`,
  integration-tag changed-file lint, and `git diff --check`.
- Full `make generate` passes; regenerating OpenAPI, protobuf and sqlc leaves
  the generated diff unchanged.

Logs: `/tmp/vela-bootstrap-schema94.jsonl`,
`/tmp/vela-bootstrap-abandonment-regressions.jsonl`, and
`/tmp/vela-bootstrap-abandonment-restore.jsonl`.

This implements explicit database termination, not physical replacement. A
delayed initializer may still retain local state, and a peer's existing journal
identity is not drain evidence. Independent containment, replacement under new
approved identities and isolated namespaces, mixed durable/nondurable serving,
failed-backend recovery, terminal retirement and bounded reclamation remain
open. Fleet durable activation stays disabled. No GPU, push or remote deployment
was performed; the overall correctness goal remains incomplete.
