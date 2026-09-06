# Runtime startup verifies the lifetime-locked Registry journal

This increment follows `8937cda`. Schema remains 92, Worker journal 5, Runtime
journal 4, Registry binding 1 and Production Gates `0/9`.

The explicitly durable `vela-model-runtime` serving command now requires both
`VELA_MODEL_RUNTIME_JOURNAL_BINDING_FILE` and
`VELA_MODEL_RUNTIME_JOURNAL_BINDING_VERIFIER_KEYRING_FILE` when
`VELA_MODEL_RUNTIME_EXECUTION_STATE_DIRECTORY` is configured. Binding settings
without a journal, missing pairs of settings, invalid files or invalid signatures
fail before server startup. Offline journal commands retain their existing
explicit preparation behavior; initialization alone cannot authorize serving.

`StartRuntimeServer` accepts the binding and independent public verifier together.
It rejects partial settings and any attempt to combine binding verification with
initialization or journal upgrades. It verifies and clones the signed value,
opens the actual execution journal, and matches Worker/member/epochs plus journal
ID/scope while retaining that journal's lifetime lock. Epoch allocation, backend
factories and socket publication occur only after this match. The existing lock
continues through Supervisor ownership and is released on clean shutdown or
failed startup. A separately initialized journal with identical topology still
has another ID and cannot replace the Registry-recorded journal.

Low-level library tests and nondurable command mode retain optional journal
configuration. This does not activate Fleet durable provisioning, establish
writer drain, or certify a backend. Pending historical writers still suppress
readiness and new execution even with a valid Registry binding.

Passed:

- Full `go test ./...`, `make lint`, and changed-file integration-tag lint.
- Runtime and command race tests, including the signed pending-history path.
- Wrong signature, journal ID/scope, Worker/member/epochs, missing configuration,
  initialization/upgrade requests and locally substituted journals reject before
  any epoch/backend call or socket publication. Rejection retains journal bytes
  and releases the lock. Competing opens fail during epoch allocation, warmup and
  serving; clean shutdown recovers the original journal unchanged.
- The serving command test starts an actual protocol-speaking CPU helper process
  with fixture signatures, rejects an independently signed wrong journal, and
  verifies no epochs were allocated by that rejection.
- PostgreSQL/TLS plus the compiled Node binding command feed the real Registry
  signature into `StartRuntimeServer` with `NewFakeDiTRuntime`. Discovery returns
  epoch 2 above the fixture's approved floor 1, warmup is Ready, competing journal
  inspection fails during backend construction, and shutdown recovers the
  original journal. Registry rows and idle journal contents remain unchanged.
- Linux arm64 Runtime rejection/lock tests and serving command tests passed as
  UID/GID 65534 with no network/capabilities, read-only root and test binary,
  tmpfs and `no-new-privileges`. Image:
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
- `git diff --check`.

Logs: `/tmp/vela-bootstrap-binding-runtime.jsonl`,
`/tmp/vela-runtime-binding-linux.log`, and
`/tmp/vela-runtime-binding-command-linux.log`. Linux binaries:
`/tmp/vela-runtime-binding-linux.test` and
`/tmp/vela-runtime-binding-command-linux.test`.

Worker serving still needs equivalent signed journal checks before member
endpoint publication. Its dynamic Runtime discovery currently precedes opening
the final admission handle; retaining the journal lock across this discovery
requires explicit binding of discovered routes without reopening the journal.
Unrecorded first initialization, pending writer recovery, backend containment,
terminal retirement, bounded reclamation and Fleet activation remain open.
No GPU, push, remote deployment or production acceptance occurred.
