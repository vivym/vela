# Stop ordering against late control responses

Date: 2026-09-05
Base checkpoint: `3ee8008`
Scope: local CPU Stage Worker response ordering; schema remains 88.

## Reproduction and repair

The existing public StreamAgent liveness test verified that Stop reaches the
Runtime while a control response is blocked. It did not check the result after
releasing that response. Stronger assertions reproduced four failures: START,
HEARTBEAT, first REATTACH with no active authority, and REATTACH with a renewed
authority all reported acceptance after the matching Stop. Every Runtime member
was already CANCELING. The initial failing run took 0.697 package seconds.

StreamAgent now snapshots a stop generation for each pending control operation.
A matching Runtime-phase Stop advances it under runtimeMu before invoking
Cancel. Response validation and active-authority installation use the same lock.
An intervening Stop therefore prevents both success reporting and installation
of a renewed or reattached authority, including when Cancel fails or its result
is unknown. A Stop for an old or different envelope cannot advance the generation
for the current execution.

If the success response is installed before Stop obtains runtimeMu, Stop follows
that completed response and cancels its matching Runtime authority. This is the
intended ordering; the lock prevents a gap between the response check and install.

## Verification

Checks use the executable source introduced with this report:

- Focused public liveness regression: PASS, 0.665 s. Eleven cases cover all three
  control operations, first and renewed reattach, cancellation transport errors,
  direct Stop, stale Stop, and late renewed START/HEARTBEAT responses.
- `go test ./...`: PASS; Stage Worker Agent 2.043 s.
- `go test -race ./internal/stageworkeragent ./internal/stageworkertransport
  ./internal/stageworkermembertransport`: PASS; Agent 3.857 s, transports cached.
- `make lint`: PASS, 0 issues. `git diff --check`: PASS.
- Independent read-only review found no remaining same-response active-authority
  installation window in these three paths.

## Limits

This process-local generation is an ordering guard for pending control responses,
not an execution watermark or durable stopped receipt. It does not establish
physical stop when Cancel fails, prevent assignment replay after restart, or
authorize scratch deletion. Persistent Worker/Runtime exclusion and complete
writer drain remain open in the
[terminal scratch design](terminal-scratch-retirement-design-2026-09-05.md).

No GPU, remote lab, integration database, CNPG or load campaign was run for this
change. Existing load receipts keep their original source binding. Production
Gates remain 0/9.
