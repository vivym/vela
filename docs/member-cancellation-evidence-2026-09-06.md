# Historical member cancellation evidence

Status: local CPU-only repair over `9f0168e`, schema 90. Production Gates remain
**0/9**. The earlier member-floor forwarding evidence retains its original scope.

## Failure and Repair

The remote member authorizer applied execution freshness to Cancel. After a
signed floor was installed and the original authority expired, an exact Cancel
failed with `FailedPrecondition: Stage Worker member authority is invalid or
stale`; the backend received zero Cancel calls. This blocked cancellation of an
execution that the local Runtime still recognized.

Only remote Cancel now validates the signed replay envelope. Signature,
future-skew, complete configured membership, deterministic leader and current
local Runtime binding checks remain enforced. Runtime owns the exact installed
authority check: an expired envelope cannot install an unseen renewal or
allocation. Prepare, Start and Status still require freshness.

## Validation

`TestMemberCancellationAfterFloorAllowsOnlyInstalledExpiredAuthority` uses real
TLS 1.3 loopback TCP, the authenticated member service, private Runtime UDS and
durable admission journal. After Prepare and floor installation, an atomic test
clock advances ten minutes. Exact Cancel reaches the backend once without
unloading the resident model. Expired execution/Status, unseen renewal or
allocation, future authority, wrong Runtime epoch, tampered signature and a
nonleader caller are rejected. A restarted Runtime with no active execution
returns STALE for the historical Cancel and does not acknowledge cancellation.

- `go test ./...`: PASS.
- `go test -race ./internal/stageworkermembertransport ./internal/modelruntimetransport ./internal/modelruntime ./internal/stageworkeragent ./internal/stageworkertransport ./cmd/vela-stage-worker-agent`:
  PASS.
- `make lint`: PASS, 0 issues.
- Linux arm64 non-root execution: PASS with the command below.

```sh
env GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  go test -c -o /tmp/vela-member-floor-linux.test ./internal/stageworkermembertransport
docker run --rm --network none --read-only --cap-drop ALL \
  --user 65534:65534 --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --mount type=bind,src=/tmp/vela-member-floor-linux.test,dst=/member.test,readonly \
  --entrypoint /member.test \
  sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  -test.run '^Test(MemberFloor|MemberCancellation)' -test.count=1
```

Cancellation acknowledgement is not execution writer drain or permission to
reclaim scratch. All-member floor collection, default trusted configuration,
historical read-only inspection, automatic recovery and terminal retirement
remain open. No GPU, remote deployment or new database/load campaign was used.
