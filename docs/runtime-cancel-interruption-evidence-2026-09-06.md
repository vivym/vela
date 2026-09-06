# Explicit cancellation interrupts blocked execution calls

This increment follows `fe08434`. PostgreSQL schema remains 94, Worker journal 5,
Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Reproduced failure

CancelStage acquired the Service execution mutex before doing anything that
could interrupt backend execution. Prepare, Start, Status and Seal held that
mutex while waiting for the backend. All four counterexamples therefore failed
twice with a fixed manual clock: `CancelStage waited behind backend without
interrupting its context`. The lease remained valid throughout the test.

Command:

```sh
go test ./internal/modelruntime -run '^TestModelRuntimeCancelInterruptsBlockedExecutionCalls$' -count=2 -timeout=20s
```

The two failing runs took `3.254s` combined. This distinguishes explicit
cancellation from the previously repaired monotonic watchdog.

## Architecture and implementation

Cancellation has two local phases. The first reuses the admission validator
under the shared admission mutex, then checks the exact installed target under
the Service state mutex. A valid target gets a pending cancellation reference;
its registered execution-call context is canceled while those checks remain
serialized with floor installation and generation changes. No backend RPC runs
under either mutex. The lock order is admission, then Service state.

The second phase acquires the existing execution mutex, registers its own
admitted operation, and repeats freshness, admission and target validation.
Only then does it invoke backend Cancel with a defensive copy of the installed
authority. The original admitted operation remains registered until it actually
returns. The interruption phase never overwrites the one-operation-per-Service
admission map. Pending references count concurrent cancellation requests and
prevent new execution entry or renewal until those requests finish.

Neither phase installs the caller's successor envelope or changes the original
watchdog. Exact installed authority still permits cancellation after Runtime
journal failure, terminal-floor installation or observed deadline expiry.
A healthy, above-floor Runtime also accepts a fresh compatible successor as
permission to stop the installed execution. Invalid signatures, identities,
reasons, future/unseen expired envelopes and already-canceled callers cannot
interrupt it.

The interruption and backend acknowledgement are separate facts. A floor or
expiry can arrive after interruption but before serialized dispatch; the
successor then rejects and exact installed cancellation can recover. Successful
late execution replies reject; successful late Seal retains its exact receipt
in memory without fabricating writer-drain evidence.

## Validation

- Four initial fixed-clock blocking counterexamples now pass.
- Thirteen admission/authority cases cover exact and successor requests,
  terminal floors, failed Runtime journals, superseded or unrelated identity,
  invalid signature/reason, future or unseen expired envelope and canceled
  caller. Allowed cases interrupt; disallowed cases return without touching
  the blocked context. An acknowledged successor does not become an installed
  inspection identity.
- Four uncooperative-backend cases receive concurrent exact Cancel requests.
  Floor installation stays available, but waiting for accepted operations times
  out until the original backend call actually returns. Late success rejects;
  late Seal keeps its identity and supports explicit original drain.
- A floor installed after successor interruption rejects that successor before
  backend Cancel; exact original cancellation still succeeds. The queued-time
  regression similarly observes interruption, advances wall time and verifies
  that an expired queued successor cannot dispatch or acquire execution time.
- An interrupted renewal not yet observed by the FakeRuntime backend rejects
  cancellation acknowledgement and retains the shared slot with no checkpoint.
  This is deliberate evidence of an unresolved backend outcome, not recovery
  completion.
- An actual blocked ProcessBackend and same-process-group child writer terminate
  after explicit interruption. Process abort returns no fabricated driver
  acknowledgement, drain checkpoint or permission to reuse shared capacity.
- An actual mTLS -> Unix socket -> durable Runtime test blocks Prepare while its
  Worker journal is held, rejects journal close, replaces the journal, rejects
  a nonleader cancel, and permits the authenticated leader to interrupt. The
  interrupted reply rejects lost Worker ownership. Subsequent explicit drain
  preserves the original authority without unloading the cooperative backend.
- Full `go test ./...` passes. Related race suites pass for ModelRuntime
  (`73.608s`), Runtime transport (cached), member transport (`15.906s`), Worker
  Agent (`76.478s`) and Worker command (`18.621s`).
- Four PostgreSQL integrations pass (`39.933s` combined): authenticated terminal
  disposition, replacement-Runtime terminal recovery, lease expiry with latest
  renewal during cancellation, and compiled Node bootstrap binding discovery.
- `make lint` reports zero issues and `git diff --check` passes.

Linux verification uses cross-built Runtime/member test binaries and the
`h3-encoder` CPU mock command under `/tmp/vela-cancel-preemption-*`, image
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`,
UID/GID 65534, no network or capabilities, read-only root/binaries, tmpfs and
`no-new-privileges`. The initial Runtime run omitted its existing native-command
fixture and failed because `go` was absent. The corrected run mounts the
cross-built command through `VELA_TEST_DRAIN_COMMAND_DIRECTORY`. Both Runtime
cancellation/watchdog tests and the member TLS/Unix tests pass, including the
real child-process and compiled H3 command cases.

## Remaining work

An uncooperative backend may remain in flight indefinitely; interrupting its
context does not return its call or establish physical containment. An aborted
ProcessBackend may terminate its resident driver through the existing exceptional
failure path. This is not ordinary residency eviction. The process evidence is
limited to descendants in the same process group and proves no GPU quiescence.

Interruption of a renewal can leave Runtime and backend observations different.
Retaining uncertain capacity is safe, but failed-backend reconciliation and
physical replacement still need closure. Ordinary backend errors with unproven
drain, pending input writers, durable sealed receipt recovery, terminal
retirement, bounded reclamation and default Fleet durable activation remain
open. Late Seal receipt retention remains in memory only. No remote deployment,
push, GPU use or Production Gate acceptance occurred. The full correctness goal
remains incomplete.
