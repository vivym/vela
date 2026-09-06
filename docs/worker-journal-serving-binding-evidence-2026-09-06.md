# Worker serving retains the Registry-bound journal through discovery

This increment follows `ed2c1b2`. Schema remains 92, Worker journal 5, Runtime
journal 4, Registry binding 1 and Production Gates `0/9`.

The explicitly durable Stage Worker requires
`VELA_STAGE_WORKER_JOURNAL_BINDING_FILE` and
`VELA_STAGE_WORKER_JOURNAL_BINDING_VERIFIER_KEYRING_FILE` with its launch manifest,
assignment journal directory and history limit. Its admission constructor
verifies the independent Registry signature, then matches Worker/member/epochs
and journal ID/scope under the actual journal lifetime lock. Missing/partial
settings, signature errors and independently signed wrong identities reject.
Bindings cannot be combined with initialization or journal upgrades.

Startup now opens this journal before external Artifact Store configuration,
Runtime discovery, member endpoint publication or Control connection. It retains
that same handle through discovery and serving. Follower history checks also
precede member publication. The held file binding is checked again immediately
before the member listener opens. Failure after acquiring the journal uses the
normal cleanup path, including errors previously occurring before runtime object
construction.

`DeferRuntimeRoutes` explicitly disables execution admission during discovery.
`BindRuntimeRoutes` completes this phase once, under the admission mutex, without
reopening the journal or changing persistent history. It requires a complete
local execution route, checks every supplied route, clones mutable data, and
recomputes the full original topology digest. Unopened peer placeholders are
allowed for followers but cannot supply execution authority. Cancellation and
held-file validation are checked again before exposing routes. Concurrent calls
have one winner; later rebinding is rejected. Current StageAuthority checks,
watermarks, pending input state, floors and retirement history remain mandatory.

Passed:

- Full `go test ./...`, `make lint`, changed-file integration-tag lint, and
  Worker admission/command race tests. The final route-validation refinement
  also passed focused deferred-binding race tests.
- Wrong Registry signature, journal ID/scope, Worker/member/epochs, missing
  binding/verifier, and initialization/upgrade combinations reject without an
  admission handle. Original journal recovery remains possible after rejection.
- Deferred admission rejects Begin and current-authority observation until
  discovery succeeds. Invalid topology, incomplete local routes, cancellation,
  closed/replaced journals and repeated binding reject. Discovery changes no
  history, retains the lifetime lock and owns independent copies of route data.
- Worker command composition owns the journal before member service construction.
  Replacing the held state file with identical bytes on a new inode rejects
  before endpoint publication. Early external configuration failure releases
  the journal. Existing single-member Acquire, Leader/Follower discovery,
  unapproved peer rejection and pending-history-before-readiness cases pass.
- Actual PostgreSQL/TLS plus the compiled Node binding command supply one signed
  pair to both Worker admission and the CPU fake Runtime. Both original journals
  remain locked during Runtime startup; observed Runtime epoch 2 is bound to the
  Worker without reopening it. Idle startup/shutdown preserves Registry rows
  and all local scratch file contents. This combines a real Node executable and
  Registry with Worker/Runtime library composition, not a deployed Fleet.
- Linux arm64 core binding tests and durable Worker command composition tests
  passed as UID/GID 65534, with no network/capabilities, read-only root/binary,
  tmpfs and `no-new-privileges`. Image:
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
- `git diff --check`.

Logs: `/tmp/vela-bootstrap-binding-worker-runtime-final.jsonl`,
`/tmp/vela-worker-binding-linux.log`, and
`/tmp/vela-worker-binding-command-linux.log`. Linux binaries:
`/tmp/vela-worker-binding-linux.test` and
`/tmp/vela-worker-binding-command-linux.test`.

These are immutable journal identity checks, not fresh process/drain receipts.
Nondurable library/command modes remain available and must not be mistaken for
the durable serving path. Fleet default durable provisioning/activation remains
disabled. First initialization without a complete local pair or Registry receipt
still needs explicit failure/replacement authority. Pending writers, failed
backend containment, sealed receipt recovery, terminal retirement and bounded
reclamation remain open. No GPU, push, remote deployment or production acceptance
occurred; the overall architecture/correctness goal remains incomplete.
