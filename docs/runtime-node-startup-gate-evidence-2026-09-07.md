# Runtime Node gate before the first backend factory

This increment follows `982ed86` on `feature/vela-mock-hardening`. It connects
the authenticated Node channel to the actual `StartRuntimeServer` factory
boundary. It does not implement the Node authorization issuer. PostgreSQL 94,
Worker journal 5, Runtime journal 6, Registry binding 1, release bundle 3 and
Production Gates `0/9` remain unchanged.

## Startup behavior

Fresh Registry-bound startup now requires `RuntimeServerConfig.BackendStartupGate`.
A missing gate fails before epoch allocation or mutation of the backend intent.
The server verifies the Registry binding, retains the original journal lock,
persists its existing member-wide `UNRESOLVED` startup intent, and invokes the
gate before the first backend factory. One permit covers that invocation's AUX
factories, using the already frozen launch manifest. The original journal's
identity and contents, and the bounded context, are rechecked after the gate.

The canonical schema-1 JSON request binds the configured Node identity, SHA-256
of the deterministic verified Registry binding protobuf, journal UUID/scope,
persisted startup incarnation UUID, and frozen launch digest. These are Runtime
declarations, not independent proof of the journal lock or effective launch.

`NewNodeBackendStartupGate` uses `runtimechannel.Exchange`. Its response is
authenticated by the retained root Node identity and the transport challenge.
The schema-1 JSON decision must contain the SHA-256 of the exact request and
`permit=true`. Unknown fields, duplicate keys, malformed or noncanonical JSON,
unsupported schema, a different digest, and documents over 4096 bytes fail.
Transport denial, response loss and cancellation also stop factory dispatch.

Once recorded, the startup intent stays `UNRESOLVED` even on denial, failed
startup or clean shutdown. An epoch may already have advanced before a gate
denial; there is no rollback of that epoch or nonce. Subsequent opens expose
recovery-only endpoints without invoking a gate or factory. This includes an
unavailable Node socket or a nil library gate. There is no startup retry grant,
journal reset, backend retirement or DeviceSet release in this change.

## Command configuration

Durable `vela-model-runtime` serving now also requires the canonical absolute
path `VELA_MODEL_RUNTIME_NODE_STARTUP_SOCKET`, alongside the Registry binding,
public verifier and execution-state directory. A socket setting without a
durable journal is rejected. Recovery still requires syntactically valid
command configuration but does not connect to Node. Offline journal commands
remain usable. On non-Linux platforms the client rejects an actual gate call.

Unbound library mode, nondurable command mode and explicit journal preparation
retain their prior behavior. This is not universal authorization enforcement.
There is no production Node serving endpoint that issues these decisions yet;
fresh durable production startup therefore cannot complete this new gate.
Existing CPU command, Worker, Registry and lifecycle fixtures explicitly supply
mock permits. Those callbacks are test assembly, not production authorization.

## Validation

On Linux/arm64 with Docker `28.3.2`, the pinned CPU image
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`
runs a root Node listener and non-root Runtime as independent namespace PID 1.
The Runtime invokes real `StartRuntimeServer`, a signed Registry fixture,
its actual journal and the concrete Node gate. Before replying, the parent
checks the persisted request/nonce, absent factory marker, and an independent
`flock(LOCK_EX|LOCK_NB)` attempt returning `EWOULDBLOCK` on the known fixture
journal. This test observation does not implement Node-side lock authentication.

All nine scenarios pass without skips: permit, deny, wrong digest, malformed
decision, duplicate key, unknown field, noncanonical bytes, lost response and
cancellation. Only permit invokes the two AUX factories. Every scenario reopens
with the original unresolved nonce, without another gate or factory invocation.
The normal exchange took 0.26 seconds inside the sandbox, 2.96 seconds including
the wrapper's cross-build and Docker setup.

Other checks passed:

- Full `go test ./...` and `go vet ./...`.
- PostgreSQL/TLS `TestWorkerBootstrapBindingCommandUsesCommittedRegistryIdentity`
  with a real Registry signature and explicit mock startup permit: 6.36 seconds.
- All nine cases of `TestRuntimePIDNamespaceContainsBackendWriters`: 13.18 seconds.
  Original-journal recovery still withholds replacement drivers after owner
  crashes or `Close`, including an independently surviving writer.
- Static Linux race binary: eight selected main tests, including the actual
  exchange, held intent, binding rejection, discovery and recovery. No skips
  or race reports; the final exchange portion took 9.75 seconds. Built using Go `1.26.7` image
  `sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`,
  `-race -ldflags '-linkmode external -extldflags=-static' -p=2`.
- Pinned golangci-lint `v2.13.1`: ordinary full-repository lint, Linux
  integration-tag lint of ModelRuntime and the Runtime/Worker commands, and
  changed-line integration-package lint. Linux integration-tag vet also passed.
- `git diff --check`.

An additional unfiltered Linux integration-package lint scan reports 82 existing
issues (50 errcheck, 4 staticcheck, 28 unused) outside the modified lines. The
complete integration package is not claimed lint-clean. Static glibc NSS linker
warnings remain test-build warnings; no production linking claim is made.

The exchange is reproducible with:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/modelruntime \
  -run '^TestRuntimeBackendStartupSandbox$' -count=1 -v
```

Logs and the task-owned race cache/binary are retained in
`/tmp/vela-startup-validation.syiSMk`. No GPU, remote deployment, push or PR was
used. Existing stopped containers were not modified.

## Next authorization boundary

The Node must independently validate current activation, effective launch and
the actual held journal, retain the exact authenticated namespace owner, and
persist their association with this Registry pair and startup nonce before it
can issue permission. The existing namespace-owner handle and launch-plan
observation do not supply that transaction. A caller-supplied path, file copy or
matching digest cannot establish ownership of the original locked journal.

Node restart/handle loss and lost responses need durable recovery rules that
cannot issue duplicate factory permission. Replacement additionally requires
retirement of the exact prior owner, retained execution drain and required
device evidence. CPU permits and a successful gate exchange do not close those
obligations or any Production Gate.
