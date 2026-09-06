# Signed Registry journal identity

This increment follows `6c2c878`. Database schema remains 92, Worker journal 5,
Runtime journal 4 and Production Gates `0/9`.

The additive `WorkerBootstrapBinding` protobuf carries the immutable bootstrap
claim and complete committed journal pair. Its Ed25519 signature is domain
separated as `vela-worker-journal-binding-v1\x00`. A dedicated Control-only seed
is required: StageAuthority execution seeds are distributed to Workers and
cannot establish exclusive Registry attestation. Consumers receive public keys.

`internal/journalbinding` signs and verifies canonical identities, nonzero scope
digests, matching request/actor, epochs, timestamps, and a complete distinct pair.
It rejects unknown nested protobuf data, unsupported schema versions, malformed
keys, unsigned values and replacement of an existing signature. Returned values
and retained public keys are independently cloned. Zero-value signers/verifiers
fail without panicking. The private, bounded protobuf-JSON file reader rejects
unknown/duplicate fields, including alternate protobuf field spellings.

`VerifyJournal` matches Worker/member/epochs and the selected journal ID/scope.
The caller must recover that identity while holding the actual lifetime lock.
The binding has no expiry because it proves historical identity only; it grants
no first initialization, current execution, readiness, writer drain or recovery.

Passed: `make generate-proto`, `go test -race ./internal/journalbinding`, full
`go test ./...`, `make lint`, and `git diff --check`. Tests include tampering,
wrong keys/domain, malformed metadata, absent receipts, journal substitution,
zero-value configuration, clone isolation and strict file parsing.

This commit adds the protocol/library foundation only. The additive RPC remains
Unimplemented through the generated default handler. Control signing, authenticated
lookup, Node retrieval and serving startup enforcement remain to be wired and
verified. No GPU, remote deployment, push or production acceptance occurred.
