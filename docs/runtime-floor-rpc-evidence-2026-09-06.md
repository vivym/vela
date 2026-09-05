# Signed Runtime floor RPC evidence

Follow-up: [authenticated member floor forwarding](member-floor-forwarding-evidence-2026-09-06.md)
adds the mTLS member hop. This document retains the preceding local-RPC evidence.

Status: local CPU-only increment over `70b582b`, schema 90. The versioned
`InstallStageExecutionFloor` RPC now delivers a signed restriction over the
existing private ModelRuntime Unix socket. Explicit RuntimeServer configuration
can assemble the durable journal before publishing that socket. The default
command does not yet supply trusted complete floor configuration. Production
Gates remain **0/9**.

## Behavior

The request contains schema version 1, an exact current resident Runtime identity
and the complete signed terminal disposition. Supervisor verifies the target,
requires durable configuration, and applies the existing signed scope/freshness
and persistence boundary. Invalid shape, unknown fields, missing/non-durable
configuration, stale identity or invalid signatures cannot produce ACCEPTED.
Journal failure returns FailedPrecondition without an installation receipt.

The response binds its schema, current identity and the request's verified
disposition digest to the installed cutoff and `durable=true`. A valid repeat
may return ACCEPTED again; a lower signed request reports the higher existing
cutoff while retaining the lower request's own digest. The RPC does not wait for
accepted backend calls and has no DRAINED state or deletion command.

`modelruntimetransport.Client.InstallExecutionFloor` verifies the disposition and
target Worker/member scope before sending, clones the request, and validates the
returned schema, identity, digest, decision, durable flag and cutoff. Context
cancellation discards an acknowledgement even if the Runtime installed the
restriction. A lost or canceled response requires retry; it never lowers a floor.
The generated raw client alone does not perform this response validation.

RuntimeServer allows 4 MiB receive messages to carry terminal dispositions, whose
canonical signed shape remains capped at 3 MiB. Other methods retain a 1 MiB
decoded-message bound through an interceptor; the client's larger send option is
specific to floor delivery. Server send and normal client limits remain 1 MiB.

Startup rollback now shuts down an already constructed Supervisor, so a failure
after journal recovery but before socket publication releases its lifetime lock
when backends shut down successfully. It preserves a conflicting socket target
owned by another publisher. Backend shutdown failure continues to retain the
lock rather than claiming successful recovery.

The cross-member adapter explicitly returns Unimplemented for floor delivery,
consistent with its existing restricted discovery/readiness/sealing methods.
Authenticated forwarding, deterministic-leader authorization and all-member
installation collection remain a separate required increment.

## Validation

- `go test ./...`: PASS on final source. Uncached modelruntime completed in
  8.690 s, stageprotocol in 3.410 s and stageworkermembertransport in 3.991 s.
- `go test -race ./internal/modelruntime ./internal/modelruntimetransport ./internal/stageauthority ./internal/stageworkeragent ./internal/stageworkermembertransport ./internal/stageworkertransport ./cmd/vela-model-runtime`:
  PASS; final uncached modelruntime and stageworkermembertransport completed in
  19.575 s and 2.966 s. Other package results were cached for this source.
- `make lint`: PASS, 0 issues.
- `make test-cross`: PASS for Linux amd64 compilation. This target uses
  `-exec=/usr/bin/true` and does not execute Linux tests.
- `make generate-proto`: PASS, including Buf lint. Buf breaking validation
  against `.git#ref=70b582b,subdir=proto`: PASS.
- Linux arm64 execution as UID/GID 65534: PASS using the command below.

```sh
env GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  go test -c -o /tmp/vela-runtime-floor-rpc-linux.test ./internal/modelruntime
docker run --rm --network none --read-only --cap-drop ALL \
  --user 65534:65534 --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --mount type=bind,src=/tmp/vela-runtime-floor-rpc-linux.test,dst=/runtime.test,readonly \
  --entrypoint /runtime.test \
  sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  -test.run '^(TestDurableExecutionState|TestExecutionFloor|TestModelRuntime(RechecksAuthority|QueuedRenewal))' \
  -test.count=1
```

Tests deliver floor RPCs while Prepare remains blocked, replay the same and lower
cutoffs, recover persisted rejection after restart, reject stale/tampered requests
without consuming a floor, and reject mismatched/fabricated acknowledgements.
The actual StartRuntimeServer assembly accepts and persists a greater-than-1-MiB
signed history containing 256 allocations and 64 synthetic members, while
oversized ordinary Prepare is ResourceExhausted. The large fixture uses the LLM
topology; H3 AUX remains one member. This executes one local mock Runtime, not
64 remote members. A publication-conflict test confirms journal-lock release and
successful subsequent recovery startup.

## Remaining Closure

Complete trusted Fleet binding delivery, first-bootstrap authorization, default
Worker/Runtime command assembly, cross-member forwarding and automatic startup
reconciliation remain open. Existing local floor installation still requires
current local Runtime routes for every historical allocation; replacing a
Runtime does not automatically prove historical drain.

Worker signed cutoff, historical read-only inspection, execution-specific writer
drain and terminal retirement journal/reclamation remain necessary. Neither this
RPC nor its durable response authorizes scratch deletion. The persistence and
power-loss evidence limits of the preceding durable-journal checkpoint still
apply. No database integration/load campaign, remote deployment, GPU execution,
physical multi-member campaign or terminal scratch deletion was performed.
