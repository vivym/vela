# Read-only historical execution inspection

Local CPU-only increment over `e8f9541`, database schema 90. Production Gates
remain **0/9**. No GPU, remote deployment or new database/load campaign was used.

## Contract

`ModelRuntimeService.InspectExecution` is separate from ordinary `Status`.
Status requires live authority and participates in renewal; historical inspection
validates signature, exact resident/member binding and the configured future
issue bound, while allowing elapsed wall and monotonic deadlines. Schema-1
requests contain only the signed authority, with no caller paths or commands.

Inspection looks up an exact envelope digest. It neither installs a newer
renewal nor recognizes an older superseded envelope as the active record. The
response binds the queried digest and independent Runtime/member identity,
decision, known flag, execution state, sequence and observation time. Unknown
has UNSPECIFIED state and zero sequence; a rejected query supplies no state
observation. ACCEPTED means the query was handled, not that the execution stopped.

An exact active record can be observed only through `BackendExecutionInspector`.
Its implementation must be read-only, handle concurrent execution operations and
context cancellation, and avoid driver termination on query timeout. There is
no fallback to `Backend.Status`. FakeRuntime implements this capability with an
exact digest lookup under its mutex. A cancel ACK stays CANCELING until the mock
explicitly finishes stopping.

Service does not take its execution operation lock while waiting on inspection,
so a slow inspector cannot hold cancellation or watchdog entry. It does not
enter execution admission, change the cutoff/watermark, update active state,
reset/stop the watchdog or mark the Worker reusable. Context cancellation and
backend failures cannot produce a successful state observation.

An existing sealed receipt can report OUTPUT_SEALED without a backend call.
These receipts remain the existing bounded 256-entry process-local cache, not a
durable history or writer-drain proof. The observation timestamp is query time,
not a new seal or stop timestamp. Missing records, superseded envelopes and
evicted history yield unknown. Missing state at the same epoch also yields
unknown; an old envelope targeting a different current Runtime epoch rejects.

## Transport

The operation is exposed through the existing private Runtime UDS and
`StageWorkerMemberService` forwarding path. The member server retains
authenticated deterministic-leader authorization, complete configured membership
and exact resident route validation. The client retains the pinned member
identity. Both forwarding boundaries validate response digest, independent
member/Runtime scope, schema, state/known consistency, sequence and timestamp;
unknown fields and malformed responses fail closed.

## Validation

- Full `go test ./...`: PASS.
- Race suite for ModelRuntime, both transports and Stage Worker Agent: PASS.
- `make lint`: PASS, 0 issues.
- `buf breaking` against `e8f9541`: PASS; the protobuf change is additive.
- Full `make generate`: PASS; hashes of tracked generated files are unchanged
  by regeneration of the final inputs.
- `make test-cross`: PASS, Linux amd64 compilation only.
- Linux arm64 non-root mTLS/UDS inspection tests: PASS.

The CPU regressions cover unseen renewals, expired exact authority, installed
floors, unchanged watchdog allocation, CANCELING versus STOPPED, no execution
slot release, superseded authority, receipt eviction after 257 executions,
same-epoch missing state, new-epoch rejection, unsupported inspectors, malformed
backend evidence, late cancellation and blocked inspectors with concurrent
cancellation/watchdog progress. The real mTLS/private-UDS chain covers historical
queries after a persisted floor and expiry, authenticated nonleader rejection,
resident backend preservation and restart returning unknown. Mutation tests
reject malformed results independently at both forwarding boundaries.

```sh
env GOOS=linux GOARCH=arm64 CGO_ENABLED=0 \
  go test -c -o /tmp/vela-execution-inspection-linux.test ./internal/stageworkermembertransport
docker run --rm --network none --read-only --cap-drop ALL \
  --user 65534:65534 --tmpfs /tmp:rw,nosuid,nodev,mode=1777 \
  --mount type=bind,src=/tmp/vela-execution-inspection-linux.test,dst=/inspection.test,readonly \
  --entrypoint /inspection.test \
  sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  -test.run '^TestMemberInspection' -test.count=1
```

## Remaining Work

ProcessBackend and CPU-media adapters do not yet implement the optional backend
inspection capability. In particular, `ProcessBackend.call` terminates the
resident driver on a canceled request or protocol failure, and H3 mock `status`
can install a renewed identity through `requireActive`. Reusing those operations
would break the read-only contract. A separate inspection design must preserve
resident models across timeout, malformed replies and backpressure, while
keeping execution cancellation responsive.

No observation here establishes DRAINED, descendant/writable-handle exclusion or
scratch deletion permission. Execution-specific backend drain, durable stopped
checkpoints, trusted first bootstrap and default command assembly, terminal
retirement journal, pending-record reclamation and automatic recovery remain
open. The Worker must eventually combine its input-writer exclusion and signed
floor with every Runtime's independently established drain evidence. No
Production Gate or complete terminal scratch lifecycle is claimed.
