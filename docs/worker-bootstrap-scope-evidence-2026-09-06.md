# Bootstrap receipts require nonzero journal scopes

This increment follows `0639824` and advances the schema from 92 to 93.
Worker journal 5, Runtime journal 4, Registry binding 1 and Production Gates
`0/9` are unchanged.

The Registry binding verifier already rejected all-zero journal scopes, but the
Fleet service and database accepted them into immutable bootstrap receipts. A
receipt with either zero scope permanently consumed its request identity while
remaining unusable for signed binding retrieval. Four CPU/PostgreSQL
counterexamples reproduced this through both the service and direct SQL, for
both Worker and Runtime scopes.

The service now rejects zero scopes before database access. The transport uses
the same nonzero digest rule for incoming receipts and decoded claim/history
responses. Migration `00093` adds independent Worker and Runtime scope CHECK
constraints to the receipt table, covering SQL callers as well. Existing length,
identity, actor and immutable-history constraints remain in force.

The migration validates existing rows and fails transactionally on invalid
history, leaving schema 92 and receipt bytes unchanged. It does not repair or
reinterpret an immutable identity. Valid history survives Up and Down unchanged;
Down removes only the two new scope constraints.

Passed:

- Transport regression proves neither zero scope reaches the Registry service.
- Service and direct-SQL regressions reject both zero scopes without leaving a
  receipt. A subsequent valid receipt completes the same original claim, and an
  identical replay returns the original timestamp.
- Migration regression proves valid history survives schema 93, while invalid
  history causes SQLSTATE `23514` with no receipt mutation or schema advancement.
- All `TestWorkerBootstrap` PostgreSQL/TLS integration tests: 31 passing test
  and subtest results, package elapsed 53.667 seconds, no failures. This includes
  signed pair retrieval, command composition, principal isolation and recovery
  quiescence at schema 93.
- `go test ./...`, `go test -race ./internal/fleettransport`, `make lint`,
  integration-tag changed-file lint against `0639824`, and `git diff --check`.

Counterexample log: `/tmp/vela-bootstrap-zero-scope-red.jsonl`.
Passing integration log: `/tmp/vela-bootstrap-schema93.jsonl`.

This closes a receipt/verification contract inconsistency. It does not establish
fresh execution authority, recover unrecorded first initialization, drain pending
writers, enable Fleet durable provisioning, or complete terminal retirement and
bounded reclamation. No GPU or remote deployment was used.
