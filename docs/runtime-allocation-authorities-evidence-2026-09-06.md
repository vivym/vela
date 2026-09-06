# Authenticated retained allocation authority reads

This CPU-only increment follows `162e078`. PostgreSQL schema 94, Worker and
Runtime journals 5, Registry binding 1 and Production Gates `0/9` are unchanged.

## Gap and scope

The schema-5 Runtime journal persists the original allocation and bounded
accepted/confirmed renewal candidates. Before this increment only the local
`Supervisor.InspectRetainedAllocationAuthorities` API could read them after
restart. Authenticated callers had live backend discovery and historical drain
inspection, but neither operation could correctly represent retained renewal
history across a Runtime epoch change.

`InspectStageAllocationAuthorities` now exposes that local read through the
owner-checked Runtime Unix socket and leader-authenticated member mTLS service.
The new protobuf request uses `ModelRuntimeExecutionDrainScope` in historical
read mode. It retains three distinct identities:

- The current, trusted Runtime identity selects the member-wide journal reader.
- The response `authority_digest` correlates the signed query.
- `authorities.original`, `accepted` and `confirmed` retain the actual historical
  signed execution envelopes. The query may differ from all three.

The original grant is required whenever history exists. Optional accepted and
confirmed envelopes must be canonical signed V2 authorities for the same
immutable execution, each bounded to 64 KiB. The accepted envelope cannot precede
the original; a confirmed envelope must lie between the original and accepted
envelopes. A distinct accepted renewal requires prior confirmation. An initial
unconfirmed intent retains only the original as accepted. Legacy original-only
history does not infer any accepted/confirmed renewal. Missing history is
unknown even when the read decision is ACCEPTED. Rejected/stale responses cannot
carry history, and unknown decisions, unknown fields, malformed wrappers,
oversized or invalid detail, invalid signatures and future candidates reject.

The Runtime selects only a resident reader and independently matches the
historical Worker/device/member journal scope. The member server authenticates
the deterministic leader and checks the resident reader before forwarding.
Both forwarding boundaries independently validate every response against a
private copy of the expected scope and independently validate all candidate
signatures and renewal relationships. Returned values cannot mutate the source
response or retained journal.

## Cancellation regression

A deterministic validator-clock hook cancels the caller during request signature
validation. The first implementation returned Canceled but still invoked the
next boundary once, reproducing `canceled history query forwarded: calls=1` at
both forwarding layers. Each layer now rechecks context after authorization and
before dispatch. The same test passes with zero downstream calls. Separate
hooks exercise cancellation after a downstream reply and during validation of
the returned candidates; neither can produce a successful response.

## Validation

- Runtime UDS tests persist both renewal failure outcomes and successful
  confirmation, restart at a new epoch after grant expiry, query with an unseen
  successor envelope, and recover the original/accepted/confirmed identities.
  Caller mutation leaves subsequent reads intact. Byte comparison proves the
  read does not rewrite the journal. An unrelated nonce remains unknown.
- Actual mTLS-to-UDS tests recover all three outcomes after Runtime epoch,
  residency, profile and runtime-name replacement. A valid nonleader certificate
  is rejected, as is a stale current-reader identity.
- Explicit schema-4 upgrade followed by a Runtime UDS read preserves original-only
  history. Both forwarding boundaries separately accept the defined unknown,
  legacy, initial-intent, uncertain, confirmed, rejected and stale states.
- Damaged-response tests at both boundaries cover missing/partial histories,
  unknown wrapper/history/authority fields, identity/digest mismatch, every
  candidate signature and size bound, signed unrelated nonce/token/sequence/epoch,
  future candidates, reversed renewal ordering, invalid decisions and details,
  request mutation and late responses. Invalid request tests assert zero
  downstream calls. Tests use injected replies to isolate boundary validation;
  they are separate from the actual mTLS/UDS restart tests.
- Historical reads do not create a live observation or drain checkpoint. Old-epoch
  drain requests stay rejected; pending writer state still blocks readiness and
  new admission, with no entry into the replacement Runtime backend.
- Final `go test ./...` passes. Related race suites pass: Runtime `104.587s`,
  Runtime transport `1.954s`, Worker Agent `98.442s`; final member transport
  race rerun after the cancellation repair passes in `11.137s`.
- Four PostgreSQL integrations pass in `18.422s`: authenticated terminal
  disposition, replacement-Runtime terminal recovery, latest-renewal lease
  cancellation/expiry, and compiled Worker bootstrap binding.
- `make lint` reports zero issues. Protobuf generation and the Runtime protocol
  allowlist test pass. The post-commit generated-artifact check is the final
  repository consistency check.
- Linux arm64 binaries `/tmp/vela-allocation-authorities-runtime-linux.test` and
  `/tmp/vela-allocation-authorities-member-linux.test` pass the new RPC suites
  in image
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
  Both containers use UID/GID 65534, no network/capabilities, read-only root and
  binaries, a private executable `/tmp` tmpfs and `no-new-privileges`.

## Remaining boundary

This is a read interface for recovery investigation. It does not attach a
retained execution to a replacement backend, perform cross-epoch cancellation,
recover an old physical writer, or add an automatic retirement path. Candidate
history is neither a live backend observation nor writer-drain, output-seal,
device-health or capacity-release proof. An empty replacement backend remains
insufficient evidence that a previous writer stopped.

Cross-epoch physical cleanup/containment, durable unhealthy-worker state, sealed
receipt recovery, bounded history reclamation, renewal write-amplification
measurements and default Fleet durable activation remain open. No GPU execution,
remote deployment, push or Launch Receipt occurred. The overall architecture and
correctness goal remains incomplete.
