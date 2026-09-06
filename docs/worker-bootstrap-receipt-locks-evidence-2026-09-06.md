# Bootstrap receipt recording retains both journal locks

This increment follows `9de7678`. Schema 93, Worker journal 5, Runtime journal 4,
Registry binding 1 and Production Gates `0/9` are unchanged.

`workerbootstrap.Prepare` previously recovered each journal separately and
released both locks before calling `RecordWorkerBootstrapReceipt`. A competing
opener could acquire either journal during that call, and replacing either
state file with identical bytes on a new inode during recording or after commit
still returned a successful preparation result. CPU counterexamples reproduced
both the ownership gap on first use/replay and all four replacement cases.

Receipt recording now uses the existing `WithPreparedAssignmentJournal` and
`WithPreparedExecutionJournal` inspectors in the same Worker-then-Runtime order
as recorded-pair reconciliation. Both journal handles remain held during pair
comparison, the Registry call and local operation validation. Each inspector
revalidates its file binding before releasing its own lock. Callback errors,
cancellation and final binding errors yield an empty result, and normal cleanup
releases all acquired locks. Initialization permission and local pair formats
have not changed.

Passed:

- Fresh and replayed receipt calls exclude competing opens of both actual
  journals, including the post-commit boundary. Normal return releases them and
  leaves the reported identities and history recoverable.
- Identical-byte inode replacements of either state file during recording or
  after commit return errors and no successful preparation result. Restoring
  the original file permits exact receipt replay without a new Claim or history
  mutation.
- Existing partial-initialization boundaries, abrupt child process exits,
  lost responses, lock conflicts and retained-state corruption regressions.
- Real compiled Node Agent commands against PostgreSQL and mutual TLS. The
  server probes both journal locks from another process before and after every
  successful receipt transaction. Normal and lost-receipt replay, recorded-pair
  reconciliation, principal isolation and lost-claim cases pass.
- New post-receipt-commit timeout and SIGTERM cases return no success output;
  a later process recovers the original journal pair and Registry timestamp.
  SIGTERM follows graceful command cancellation, with exit code 1.
- Full `go test ./...`, coordinator/Node command race tests, `make lint`, and
  integration-tag changed-file lint against `9de7678`.
- `make verify-generated` leaves OpenAPI, protobuf and sqlc outputs unchanged;
  `git diff --check` passes.
- Linux arm64 preparation tests, including replacement and child process cases,
  as UID/GID 65534, no network/capabilities, read-only root/binary, private tmpfs
  and `no-new-privileges`. Image:
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

Logs:

- `/tmp/vela-bootstrap-receipt-locks-red.jsonl`
- `/tmp/vela-bootstrap-receipt-locks-command.jsonl` (23.497 package seconds)
- `/tmp/vela-bootstrap-receipt-interruption-command.jsonl` (9.346 seconds)
- `/tmp/vela-bootstrap-receipt-locks-linux.log`

Linux binary: `/tmp/vela-bootstrap-receipt-locks-linux.test`.

A local failure after Registry commit does not roll back the immutable receipt.
It withholds successful preparation; recovery must revalidate the original pair.
Offline ownership ends when preparation returns. A receipt is still historical
identity evidence, not serving readiness, a fresh lease or proof that all input
writers and backend descendants have drained.

Unrecorded first initialization still requires explicit failure/replacement
authority and containment. Fleet durable activation, pending-writer recovery,
backend containment, terminal retirement and bounded reclamation remain open.
No GPU or remote deployment was used; the overall correctness goal remains open.
