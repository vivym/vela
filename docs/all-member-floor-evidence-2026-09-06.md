# All-member execution floor collection evidence

Status: local CPU-only increment over `8fcee01`, schema 90. Explicit Agent
configuration now supports collection of durable floor acknowledgements from
every configured member. Production Gates remain **0/9**.

## Contract

`stageworkeragent.Config.ExecutionFloor` supplies a validator, independently
trusted Runtime bindings, member identity/device-subset digests and a bounded
timeout. Configuration is copied and never learned from the terminal response.
The default command assembly does not yet enable this component.

`Agent.InstallExecutionFloor` verifies the signed fresh terminal envelope and
matches every historical allocation's complete member set, Worker/device scope,
residency, profile, Runtime identity and member-local epoch before dispatch.
Missing or ambiguous routes fail before any RPC. Barrier generation is never
substituted for a member-local Runtime epoch. Validation and dispatch recheck
context cancellation.

Each member receives its own copy of the complete signed history over its
existing Runtime client, with the 4 MiB send allowance. One installation call
per member is sufficient because the receiving Supervisor installs a shared
admission floor across all resident profiles and independently verifies every
historical route. The collector binds each response to the trusted target,
disposition digest, schema, ACCEPTED, durable=true and cutoff at least as high as
requested. Unknown fields, stale identities, partial success, transport errors,
timeouts and late success cannot produce `AllInstalled=true`.

`ExecutionFloorResult` contains only this call's acknowledgements. Retrying
reconfirms every member; it does not merge cached successes from earlier calls.
Partial remote installation remains restrictive even when the response is lost.
Neither the result nor its acknowledgement map is a persisted retirement receipt.

## Validation

- `go test ./...`: PASS; stageworkeragent 5.352 s, production Worker command
  1.691 s. These are package elapsed times, not performance measurements.
- `go test -race ./internal/stageworkeragent ./internal/stageworkermembertransport ./internal/modelruntimetransport ./internal/modelruntime ./internal/stageworkertransport ./cmd/vela-stage-worker-agent`:
  PASS; stageworkeragent 13.260 s, production Worker command 3.660 s.
- `make lint`: PASS, 0 issues.
- `make test-cross`: PASS for Linux amd64 compilation only.
- Linux arm64 non-root execution: PASS with the command below.

```sh
env GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  go test -c -o /tmp/vela-floor-collector-linux.test ./internal/stageworkeragent
docker run --rm --network none --read-only --cap-drop ALL \
  --user 65534:65534 --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --mount type=bind,src=/tmp/vela-floor-collector-linux.test,dst=/collector.test,readonly \
  --entrypoint /collector.test \
  sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  -test.run '^TestExecutionFloorCollection' -test.count=1
```

Unit tests cover concurrent dispatch and waiting, full retry after response loss,
complete historical scope, trusted configuration copying, malformed responses,
request mutation isolation, cancellation during validation and late success after
deadline. A cancellation injected during signature freshness validation initially
entered the dispatch phase with `RequiredMembers=2`; the regression now rejects
it before dispatch and passes.

The RPC test uses two live in-process Supervisors, each with two resident fake
profiles, independent journal directories and real private Unix sockets. The
history has different selected profiles and member-local epochs (9, 19 and 20),
with a separate barrier generation of 700. Losing the second member's successful
response leaves collection incomplete while both members reject historical
Prepare/Start, including the unseen later allocation. Full retry succeeds.
Both previously started fake backends remain RUNNING and are not closed by
installation. After explicit test teardown, rebuilding the same synthetic
topology in recovery mode preserves rejection and accepts a sequence above C.

This is a process-stack restart and local UDS campaign. The earlier member
forwarding evidence separately covers real TLS 1.3-to-UDS transport. This test
does not establish multi-node placement, physical power-loss durability, hostile
owner rollback protection or production epoch advancement. No GPU, remote
deployment or new database/load campaign was used.

## Remaining Work

Default trusted Runtime/Worker configuration and explicit bootstrap, Worker
signed cutoff persistence, historical read-only inspection, execution-specific
writer drain, the retirement journal and automatic recovery remain open.
All-member floor installation closes Runtime admission through C only. It does
not exclude Worker input writers, stop accepted compute, prove DRAINED or permit
scratch deletion. Model residency must be preserved by the later Stage drain;
the test's process teardown is not that drain API.
