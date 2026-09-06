# Authenticated Registry journal binding retrieval

This increment follows `202c2c1`. Schema remains 92, Worker journal 5, Runtime
journal 4, binding schema 1 and Production Gates `0/9`.

`LookupWorkerBootstrapBinding` authenticates registered Node Agent mTLS peers,
then uses the existing request/node/original-actor scoped Registry history
lookup. Missing history returns NotFound, pending claims without a committed
pair return FailedPrecondition, and absent signing configuration returns
Unimplemented. Malformed authoritative metadata fails before a signature is
returned. Lookup calls no Claim or RecordReceipt mutation.

Control accepts optional `VELA_WORKER_BOOTSTRAP_SIGNING_KEYRING_FILE` and
`VELA_WORKER_BOOTSTRAP_ACTIVE_KEY_ID` together. Before opening database pools it
loads a private, bounded JSON key-id/base64-seed map. Every entry must contain
exactly 32 bytes and the active entry must exist. It rejects both raw execution
secrets and their StageAuthority-derived Ed25519 seeds, including inactive
Registry entries. These keys must remain Control-only; this check cannot prove
external secret provisioning or historical key distribution.

`vela-node-agent bootstrap --action binding --request-id <uuid>
--binding-verifier-keyring-file <private-public-keyring>` uses the existing Fleet
TLS connection arguments. It emits the protobuf-JSON binding to stdout only
after verifying the configured public key and original request/node/actor.
The public key file is independently provisioned, never trusted from the RPC
response. This action accepts no preparation settings, touches no journals and
grants no readiness, initialization, drain or execution authority. To consume
the result as a binding file, its owner/mode must meet `securefile.Read`.

Passed:

- Full `go test ./...`, `make lint`, and changed-file integration-tag lint.
- Race tests for Fleet transport, Control configuration and Node command.
- Unit checks for malformed requests, unconfigured signer, pending/malformed
  receipts, tampered responses and correctly signed but wrong request/node/actor.
- Actual compiled Node Agent commands against local PostgreSQL and TLS. A
  deliberately uncommitted receipt prevents signing; replay of preparation then
  commits the original pair. Other nodes, another registered Agent on the same
  node, and unregistered Agents cannot retrieve the binding. A lost binding
  response is recoverable without another Claim or receipt write. Wrong public
  keys reject. Repeated successful commands preserve the signed identity and
  recorded timestamp. All Registry claim rows and local scratch file contents
  remain unchanged during binding retrieval and failed/replayed retrieval.
- `git diff --check`.

Command evidence: `/tmp/vela-bootstrap-binding-command.jsonl`.

Serving processes still need to verify their actual lifetime-locked journals
against these signatures. Worker member endpoints currently start before the
final admission handle opens, so startup ordering must also be addressed.
Fleet default durable provisioning/activation remains disabled. Unrecorded first
initialization, pending-writer recovery, backend containment, terminal retirement
and bounded reclamation remain open. These are CPU/local checks, not GPU or
production acceptance. No push or remote deployment occurred.
