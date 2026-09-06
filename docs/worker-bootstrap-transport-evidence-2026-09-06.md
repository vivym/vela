# Authenticated Worker bootstrap transport and history

This increment follows `bf4c544`. Database schema is **92**. Worker journal 5,
Runtime journal 4, local bootstrap operation 1, materialization 2, launch/Fleet
manifest 2 and floor RPC v1/v2 are unchanged. Production Gates remain `0/9`.

## Authenticated authority

The existing Fleet Maintenance gRPC service now exposes three additive methods:
`ClaimWorkerBootstrap`, `RecordWorkerBootstrapReceipt` and
`LookupWorkerBootstrap`. `vela-control` explicitly supplies the existing Fleet
service as their authority. An adapter without `BootstrapService` returns
Unimplemented; there is no local initialization fallback.

Each method requires a verified, registered Node Agent TLS principal using the
existing canonical SPIFFE identity containing node identity and Agent UUID.
Fleet Controller credentials alone cannot invoke these methods. The server
derives the immutable `node-agent/<encoded-node>/<agent-uuid>` actor from the
certificate; requests have no actor field. A different Agent UUID, even on the
same node, cannot inspect or report another actor's original operation.

Before calling the first-use mutation, the server parses the complete canonical
bundle manifest, rejects unknown/duplicate JSON fields and noncanonical input,
and matches the requested Worker/member/epoch to the authenticated node.
Checking the node only after Claim returned would consume another node's unique
permission. PostgreSQL independently rechecks the exact bytes against the
approved Registry layout before committing the claim.

`fleetcontroller.ParseWorkerBundleActuationManifest` shares the existing
canonical encoder and restores its digest for normal bundle validation. The
digest preimage format is unchanged. The receive envelope is bounded to 4 MiB
plus 64 KiB protobuf overhead. The bootstrap client explicitly raises only its
Claim send limit. Existing plan/observation payload limits stay at 1 MiB.

## Read-only history

Migration 92 adds `vela_lookup_worker_bootstrap(request, node, actor)`. It is a
STABLE, SECURITY DEFINER SELECT with the existing dedicated Fleet owner and
EXECUTE grant. The broad internal role receives no access. The query returns
the original claim and optional journal receipt only when both node and actor
match. Missing and differently owned requests both return NotFound.

`fleet.Service.LookupWorkerBootstrap` never calls Claim and never returns
`Fresh=true`. The shared protobuf history message contains no fresh-permission
field; that boolean exists only on the Claim RPC response. The receipt endpoint
first reads matching immutable history, then invokes the existing idempotent
receipt mutation. Original schema-91 claim/receipt and commit-quorum semantics
remain unchanged.

History inspection works in a database read-only transaction. Reader migration
Down/Up preserves populated claim and receipt rows. Schema-92 recovery uses the
same complete bootstrap inventory required since schema 91; a read operation
does not add pending authority or release a pending claim.

## Client and lost responses

`Client.WorkerBootstrap(expectedSPIFFEIdentity)` constructs the node-bound
client. Its `ActorIdentity()` and `NodeIdentity()` supply the local coordinator's
configuration. Claim and receipt methods implement the existing offline
`workerbootstrap.Authority` interface. The server separately authenticates the
actual TLS peer, and the client verifies that returned node/actor match its
expected principal.

The client rejects mismatched request, Worker/member identity, epochs, node,
actor, bundle digest, journal pair or timestamp. Unknown protobuf fields and
missing nested messages also reject. Failed or malformed Claim responses return
an empty result, never usable initialization permission. Lookup returns history
without granting initialization.

Actual TLS tests combine the local coordinator, real Worker/Runtime journal
files and PostgreSQL. A server interceptor discards a successful committed Claim
or receipt response. After lost Claim response, both journals remain absent and
ordinary retry preserves the incomplete local operation. After lost receipt
response, retry recovers the original journal pair and database timestamp.
Direct RPC replay also returns `fresh=false`.

## Verification and observed failure

Passed:

- Full `go test ./...`; related Fleet, coordinator and Control command race
  regressions; ordinary and changed-file integration-tag lint (`0 issues`).
- Actual TLS 1.3 to PostgreSQL: correct node succeeds, registered other-node and
  unregistered callers create no claim, other actors cannot read/report the
  operation, and both committed-response-loss paths preserve the lifecycle.
- A 1,569,288-byte canonical bundle through the actual TLS client/server;
  oversized bootstrap and ordinary Fleet plan payloads reject.
- Principal-scoped read-only database lookup, reader migration Down/Up,
  existing first-use concurrency, immutability, commit-quorum, local journal,
  recovery-gate and Node Agent observation regressions.
- Schema-92 snapshot restore into independent PostgreSQL 17: passed in 14.202
  package seconds. This proves the existing restore drill's scope, not a live
  production recovery receipt or reconciliation of partially initialized nodes.
- Linux arm64 transport tests as UID/GID 65534, no network/capabilities,
  read-only root/binary, `no-new-privileges` and tmpfs, using image
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
  Binary: `/tmp/vela-bootstrap-transport-linux.test`.
- Full generated contracts and protobuf compatibility against `bf4c544`.
  Only the two expected Fleet protobuf outputs change; OpenAPI/sqlc are unchanged.
- `git diff --check`.

The wider regression initially failed with `close recovery Admission: conn
closed`. The existing bootstrap quiescence test reused a pgx connection after a
deadline interrupted a query. Cancellation may close that connection, while the
actual recovery CLI connects separately on every invocation. The test and the
adjacent Job drain test now assert DeadlineExceeded and reconnect using the
same durable operation ID. No timeout was relaxed and recovery runtime behavior
was not changed. The original failing case passed three consecutive race runs;
the Job drain/reopen and inflight-Admission lock regressions also passed.

Local logs retain the failed wider run and focused successful verification:

- `/tmp/vela-bootstrap-auth-regressions.jsonl`
- `/tmp/vela-bootstrap-quiescence-retry.jsonl`
- `/tmp/vela-bootstrap-auth-recovery.jsonl`
- `/tmp/vela-bootstrap-schema92-restore.jsonl`

## Remaining lifecycle

Apply schema 92 before enabling this code in a deployment. Node/Agent certificate
registration uses the existing Control configuration, not a new self-enrollment
API. Certificate provisioning/rotation and the actual node bootstrap command
are still required before activation. The existing node daemon does not yet
invoke this coordinator, and Fleet recurring init scripts do not initialize
these journals.

Partial-initialization reconciliation, serving checks against the Registry pair
while holding both journal lifetime locks, and controlled Fleet activation
remain open. A provisioner receipt reports inspected journal identity; it is not
readiness, writer drain or a Launch Receipt. Unknown writers, pending Runtime
recovery, failed-backend containment, sealed receipt recovery, bounded checkpoint
reclamation and successful command-level terminal retirement remain separate
unfinished requirements. No GPU, remote deployment, push or production
acceptance was performed.
