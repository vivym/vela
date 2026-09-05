# Authenticated member floor forwarding evidence

Status: local CPU-only increment over `0258028`, schema 90. Signed execution
floor delivery now crosses the existing StageWorkerMember mTLS service into the
target member's private Runtime socket. Production Gates remain **0/9**.

## Contract

The additive member RPC wraps the original versioned Runtime floor command with
an exact target member ID. The client requires a floor validator, authenticates
the signed terminal envelope and checks the target's identity digest in every
allocation against its independently pinned TLS peer identity. The production
Worker command supplies the already configured Stage authority validator.

The receiver authenticates the mTLS peer, verifies the signed envelope, requires
the exact current local target identity and complete configured membership/epochs,
and checks every historical allocation against a resident local Runtime route.
Only the deterministic smallest-UUID member's signed identity may invoke the
remote floor. Missing members, unknown fields, stale epochs/profiles, invalid
signatures and wrong peers fail before local Runtime delivery. Local Runtime
independently verifies its complete trusted topology and durable state.

Both forwarding hops validate schema, exact response identity, request disposition
digest, ACCEPTED, durable=true and installed cutoff >= requested cutoff. A canceled
context discards a late success. Request and acknowledgement validation are shared
with the direct UDS client. The forwarding call preserves the 4 MiB message
allowance required for complete terminal histories. No new model lifecycle or
execution entry point is used by installation.

## Validation

- `go test ./...`: PASS. Relevant package times: modelruntime 18.852 s,
  modelruntimetransport 7.842 s, stageworkermembertransport 7.097 s and the
  production Worker command 9.282 s.
- `go test -race ./internal/stageworkermembertransport ./internal/modelruntimetransport ./internal/modelruntime ./internal/stageworkeragent ./internal/stageworkertransport ./cmd/vela-stage-worker-agent`:
  PASS; 4.909, 2.834, 33.530, 20.297, 10.109 and 10.742 s respectively.
- `make lint`: PASS, 0 issues. `make generate-proto`, including Buf lint, and
  Buf breaking against `.git#ref=0258028,subdir=proto`: PASS.
- `make test-cross`: PASS for Linux amd64 compilation only.
- Linux arm64 tests as UID/GID 65534: PASS with the command below.

```sh
env GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  go test -c -o /tmp/vela-member-floor-linux.test ./internal/stageworkermembertransport
docker run --rm --network none --read-only --cap-drop ALL \
  --user 65534:65534 --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --mount type=bind,src=/tmp/vela-member-floor-linux.test,dst=/member.test,readonly \
  --entrypoint /member.test \
  sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  -test.run '^TestMemberFloor' -test.count=1
```

Unit tests cover leader authentication, complete scope, signatures, both hops'
acknowledgement binding, cancellation and state-error propagation. End-to-end
tests use real TLS 1.3 certificates and loopback TCP, PeerAuthenticator, the member
client's peer pinning, a private UDS client and a durable Supervisor journal.
Losing the first successful floor response still rejects the old Start; retry
returns the same restriction. Exact cancellation remains available while its
envelope is fresh, and installing the floor does not close the resident backend.
Restarting both transport services and the Supervisor preserves rejection of an
unseen allocation at the floor while allowing a higher sequence.

A second end-to-end test transports, persists and recovers a greater-than-1-MiB
history with 256 allocations and 64 synthetic member bindings. This exercises
one local target Runtime and one authenticated leader connection, not 64 live
members or physical multi-node placement. Keys are ephemeral test data. No GPU,
remote deployment or new database/load campaign was used.

## Remaining Work

All-member installation collection, complete trusted Fleet bindings for default
Runtime assembly, explicit bootstrap, Worker signed cutoff, automatic recovery,
historical read-only inspection, execution-specific writer drain and terminal
retirement/reclamation remain open. The existing remote Cancel authorizer still
requires a fresh envelope and blocks historical exact cancellation after expiry;
that needs a separate narrow repair with Runtime-owned replay checks.

An individual member acknowledgement is not an all-member barrier and never
proves DRAINED or authorizes deletion. Stack restart, fsync and injected response
loss do not establish physical power-loss durability or trusted-owner rollback
protection. Earlier evidence retains its source and scope.
