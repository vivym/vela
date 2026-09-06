# Worker journal ownership during forwarded commands

This increment follows `f622347`. PostgreSQL schema remains 94, Worker journal 5,
Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Counterexamples

Discovery verified journal ownership, but member Prepare, Start and Status did
not consult the actual Worker journal afterward. All nine combinations of these
three commands with a closed, same-byte-replaced or unbound journal forwarded
one Runtime call and returned success before the repair. Runtime Status enters
execution admission and renews active authority; treating it as harmless
inspection would leave the renewal bypass open.

## Implemented contract

`FileAssignmentAdmission.RetainJournalBinding` verifies the retained Registry
signature and actual held journal under the admission mutex, requires resolved
Runtime routes, and returns a defensive binding copy with one release callback.
Each call owns an independent reference. Normal Close returns busy while a
reference or existing input admission handle remains. Release decrements exactly
once, including concurrent/repeated cleanup, and checks actual ownership with a
non-canceled context. Detected replacement stays failed after path restoration.
Retention and observation leave durable history unchanged.

The member service retains this actual handle after authentication and authority
validation for Prepare, Start and Status. It validates the binding's local
Worker/member/epoch scope before forwarding. An observer without command
retention capability rejects. No journal mutex crosses a Runtime RPC. Release
always runs when forwarding returns; ownership loss discards a successful
Runtime reply. Canceled requests discard late success, while existing Runtime
errors are preserved. Ordinary composition without a Worker journal retains its
existing behavior and cannot satisfy durable discovery requirements.

Recovery operations do not acquire these command references. Exact inspection,
cancellation, floor installation, drain and non-admission evidence still enforce
their existing peer and Runtime authority. Worker journal failure therefore
does not disable the paths needed to inspect and restrict uncertain execution.

## Validation

- Nine original command/fault combinations now reject before any Runtime call.
- Blocking Runtime callbacks exercise all three commands with success, Runtime
  error, cancellation and journal replacement. Close stays busy until the
  callback returns; observation stays available; references do not leak.
- Journal tests exercise two independently held references, 16 concurrent
  releases of one reference, canceled retention, deferred/unbound/closed state,
  response-copy independence, unchanged history, exclusive locks and sticky
  replacement detection even after request cancellation.
- Wrong-scope bindings, observer-only providers and unauthorized peers reject
  before Runtime. Unauthorized requests do not retain journal ownership.
- Actual mTLS -> UDS -> durable CPU Runtime tests prepare an allocation, then
  close or replace the Worker journal. Prepare/Start/Status reject while exact
  inspection, floor, cancellation, durable drain and exact/allocation checkpoint
  inspection succeed. STOPPED alone creates no drain proof; unauthorized peer
  cancellation/drain still reject. Separate never-admitted history retains
  checkpoint creation/inspection without backend cancellation or unloading.
- PostgreSQL/TLS and the compiled Node binding command pass
  `TestWorkerBootstrapBindingCommandUsesCommittedRegistryIdentity` (`8.960s`).
  The actual Worker rejects retention until discovered Runtime routes bind,
  retains the committed Registry pair, blocks close during the retained call,
  and reopens the original journals after release without replacing history.
- Full `go test ./...` passes. Related race suites pass for
  `internal/stageworkeragent` (`52.460s`),
  `internal/stageworkermembertransport` (`10.829s`) and
  `cmd/vela-stage-worker-agent` (`19.536s`).
- `make lint`, integration-tag changed-code lint and `git diff --check` pass.
- Linux arm64 focused journal, forwarding and actual TLS/UDS recovery tests pass
  as UID/GID 65534 with no network or capabilities, read-only root/binary, tmpfs
  and `no-new-privileges`. Image:
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

Linux test binaries are `/tmp/vela-command-journal-worker-linux.test` and
`/tmp/vela-command-journal-member-linux.test`. The tests and commands above are
reproducible repository evidence; these temporary binaries are not release
artifacts. No database migration or protobuf change is needed.

## Remaining work

The reference lifetime ends when the forwarding call returns. A UDS timeout may
return while backend work continues, so release is not proof that descendants
stopped or devices are reusable. The new guard adds no Worker mutex around
recovery, but Runtime's existing operation mutex can still delay cancellation
behind a blocked backend call. Historical Registry signatures depend on trusted
serving code to inspect actual handles; they are not fresh process attestations.

Physical containment/replacement, pending input writer recovery, failed-backend
recovery, sealed receipts, terminal retirement, bounded reclamation and default
Fleet durable activation still require further lifecycle validation. No GPU,
remote deployment, push or Production Gate acceptance occurred. The full
correctness goal remains incomplete.
