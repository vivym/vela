# Node CRI container observation

This increment follows `739a13c` and adds the read-only Node command
`inspect-runtime-container`. It is the observation layer needed for subsequent
backend startup binding. It does not clear schema-6 `UNRESOLVED` or
`LEGACY_UNKNOWN` records. PostgreSQL schema 94, Runtime journal 6, Worker journal
5, Registry binding 1 and Production Gates `0/9` remain unchanged.

## Protocol findings

The adapter uses `k8s.io/cri-api v0.34.1`, matching the repository's Kubernetes
libraries. `go.sum` pins the module content to
`h1:n2bU++FqqJq0CNjP/5pkOs0nIx7aNpb1Xa053TecQkM=`.
The versioned primary contract is
[CRI api.proto](https://github.com/kubernetes/cri-api/blob/v0.34.1/pkg/apis/runtime/v1/api.proto).

- `ListContainers` supplies the container-to-sandbox association; the adapter
  verifies the full returned IDs rather than trusting the request filter.
- `ContainerStatus` supplies container metadata, state and lifecycle timestamps;
  absence must return an error. A `NotFound` response is not exit evidence.
- `PodSandboxStatus` supplies exact Pod metadata and Linux sandbox namespace
  options. The CRI documentation explicitly says sandbox security context does
  not apply to the containers in that Pod. These options do not prove the
  actual container process is PID 1 in its own namespace.
- `ContainerStatusResponse.info` is implementation-dependent JSON, useful for
  diagnostics such as PID. There is no portable standard schema for treating it
  as immutable process/OCI containment evidence. The adapter does not request
  verbose info or promote it into proof.
- CRI status reads are not a transaction or a lifetime lease. Equal repeated
  observations reject visible changes but cannot prove no intervening change
  or prevent changes after the observation finishes.

These findings rule out authorizing replacement merely from a sandbox PID mode
of `CONTAINER`, a reported `EXITED` state, or absence of the old metadata.

## Implemented boundary

`internal/nodeagent/runtime_container.go` exposes a typed exact target and an
observation. Its private reader interface permits only `Version`,
`ListContainers`, `ContainerStatus` and `PodSandboxStatus`. The command never
invokes CRI start, stop, remove, exec or image operations.

The current target format requires distinct, full 64-character lowercase hex
container/sandbox IDs, a nonzero canonical Pod UUID, valid Kubernetes names and
an exact container attempt. This is deliberately an explicitly supported ID
format, not a claim to support every CRI runtime's possible opaque ID syntax.
Target IDs and the configured Node name must come from trusted inventory;
echoing them in this observation does not authenticate a Fleet/Registry claim.

The transport accepts only a local root-owned Unix socket, mode `0600` or
`0660` with group 0, beneath validated trusted directories. It checks the actual
kernel-reported peer UID and pins socket/directory identities across connection
and observation. A reconnect cannot switch to a replacement socket. Linux uses
`SO_PEERCRED`; the portable local test path uses Darwin `LOCAL_PEERCRED`.
The public Node adapter reads `/proc/sys/kernel/random/boot_id` directly.
Private test hooks do not add configuration or environment bypasses to the
public command.

Every inspection checks the boot ID before/after, verifies container list and
status identity/state/timestamps/image-reference consistency, verifies exact Pod
metadata and sandbox creation order, and compares repeated container/sandbox
status. Unknown or inconsistent state, missing namespace status, lost records,
changed boot/socket identity, cancellation and oversized replies return no
observation. Connection and inspection each have a 10-second upper bound; the
command also bounds total lifetime and closes its connection before printing.

The output reports `CREATED`, `RUNNING` or `EXITED` using the CRI enum names,
timestamps, image reference, exact identity, sandbox state/PID scope and read
interval. It omits arbitrary labels, annotations, verbose info, command lines,
mounts and logs. The CRI image reference is reported as observed; it is not
equated with a certified Vela release digest. No `retired`, `quiescent`, `ready`
or replacement-permission result is produced.

## Validation and reproduction

The Go tests use a real Unix socket and generated CRI gRPC client/server, with
`vela-cri-mock` as the reported runtime. An RPC interceptor rejects any method
outside the four read operations. Tests cover three container states across
four sandbox namespace modes, malformed/mismatched/absent history, garbage
collection, changing status, boot identity loss, socket replacement, connection
closure, cancellation, bounded replies and untrusted socket paths/owners/modes.
Command tests cover exact argument forwarding, bounds and output/close failures.
Directory permissions are revalidated as well as inode identity; a previously
trusted directory made writable by others rejects subsequent observations.

```sh
go test ./... -count=1
go test -race ./internal/nodeagent ./cmd/vela-node-agent -count=1
go test -race ./internal/nodeagent ./cmd/vela-node-agent \
  -run '^TestRuntimeContainer' -count=1 -v
go vet ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1 run ./...
go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.13.1 \
  run --build-tags=integration ./internal/nodeagent ./cmd/vela-node-agent
```

Full unit and Node race suites pass (Node `25.550s`, command `3.088s` for the
full race run). Standard full lint, relevant tagged lint and vet pass. Focused
race tests are repeated after adding the two concurrent socket/close cases.
The two compiled Node bootstrap PostgreSQL regressions also pass (`27.842s`
package time): committed Registry identity and authority persistence across
processes. These are regression checks, not a Registry binding for CRI reads.
No Vela database or protobuf generation changes are required.

The Linux tests additionally use the actual public adapter, kernel boot ID and
`SO_PEERCRED`. One test changes only the socket file's owner; it confirms the
kernel peer remains root and that claiming the new file owner as the service
identity fails. The compiled Node command is executed against the mock CRI
socket in a separate process, with successful observation and `NotFound`
controls. These tests run as root solely inside an isolated CPU container.

```sh
CRI_TEST_DIR=$(mktemp -d /tmp/vela-cri-linux.XXXXXX)
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go test -c -o "$CRI_TEST_DIR/nodeagent.test" ./internal/nodeagent
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 \
  go build -o "$CRI_TEST_DIR/vela-node-agent" ./cmd/vela-node-agent
docker run --rm --network none --read-only --cap-drop ALL --cap-add CHOWN \
  --security-opt no-new-privileges:true --pids-limit 128 --memory 512m --cpus 2 \
  --user 0:0 --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --env VELA_TEST_NODE_AGENT_BINARY=/vela-node-agent \
  --mount "type=bind,src=$CRI_TEST_DIR/nodeagent.test,dst=/nodeagent.test,readonly" \
  --mount "type=bind,src=$CRI_TEST_DIR/vela-node-agent,dst=/vela-node-agent,readonly" \
  --entrypoint /nodeagent.test \
  sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  -test.run '^TestRuntimeContainer' -test.v -test.timeout=60s
rm "$CRI_TEST_DIR/nodeagent.test" "$CRI_TEST_DIR/vela-node-agent"
rmdir "$CRI_TEST_DIR"
```

The selected Linux arm64 tests pass without skips using Docker Engine `28.3.2`
and the existing immutable CPU image above. The image supplies a filesystem;
its normal entrypoint/service does not run. There is no host CRI socket mount,
network, GPU/device mapping or access to the user's existing containers from
inside the test. `CHOWN` is used only for the socket-file identity counterexample.

This proves the Node observer, CLI and Linux kernel credential/read boundaries
against a protocol mock. It does not prove production containerd behavior or
actual Vela container process containment. No production daemon, remote host,
GPU, Fleet deployment or Registry mutation is part of this experiment.

## Next dependencies

The next layer must resolve an authenticated Runtime caller's actual host
process to its exact container and verified individual-container configuration,
then bind that identity to the Registry journal pair and the startup nonce
before factory dispatch. Authenticating the CRI server UID does not authenticate
that Runtime caller. Version-specific containerd/OCI inspection must be tested
against its real implementation instead of assuming the verbose JSON layout.

Independent exact-owner termination, durable replay-safe retirement, lost
observations, Node/agent restarts and container metadata garbage collection
still need their own protocol. Stage drain and device reuse remain separate.
Durable Runtime restart availability, unhealthy Worker and sealed receipt
recovery, history reclamation, renewal write cost and Fleet durable activation
remain open. The full correctness and architecture goal remains active.
