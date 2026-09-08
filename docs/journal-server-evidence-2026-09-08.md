# Bounded authenticated Node journal service

Date: 2026-09-08. Baseline: `b9cf6d4`.
This extends the [journal endpoint](journal-endpoint-evidence-2026-09-08.md) and
[request-context boundary](journal-context-evidence-2026-09-08.md).
PostgreSQL 94, Worker journal 5, Runtime journal 8 and Production Gates **0/9**
remain unchanged. Full architecture/correctness closure remains in progress.

## Implemented behavior

The prior endpoint handled one authenticated request but left the accept loop,
handshake concurrency, transport cleanup and shutdown joining to its caller.
The actual Supervisor test manually implemented that loop. The new
`nodeagent.JournalServer` implements this service boundary as reusable Linux
production code; a new native test drives an actual remote Supervisor through it.

Trusted assembly supplies a previously approved `JournalEndpoint`, one or two
non-root credential pairs, an explicit concurrency limit from 1 to 64, an
exchange timeout up to 45 seconds, and a protected Unix seqpacket listener.
Configuration is copied. Credentials only filter connection admission: the
existing challenge, connection/message pidfds, original-process role checks,
canonical command validation and independent owner transitions still apply.
No client can name a role, journal path, process or permission grant.

The bound covers the whole exchange, including authentication, waiting for the
endpoint, handling and sending its reply. A slot is acquired before creating an
exchange goroutine or allocating its 192 KiB request buffer. When all slots are
occupied, the accept loop immediately closes the extra connection. There is no
Go waiting queue or goroutine for that overload. The exchange deadline begins
at acceptance and also observes server cancellation; the handshake retains its
existing shorter five-second limit.

For configured concurrency `N`, at most `N` exchange goroutines retain accepted
work. The accept loop can transiently hold one additional socket while rejecting
overload. Kernel listen backlog and socket buffers are separate OS resources;
this is not a bound on total Node RSS, filesystem cache or CPU consumption.
Repeated verification of the full journal remains a separate performance cost.

`Shutdown(ctx)` stops listener and accepted-connection I/O, then waits for every
accepted handler and its caller handles to close. If a handler or synchronous
filesystem operation has not returned, shutdown times out without claiming a
successful join. The owner and endpoint remain caller-owned and must be retained
until `Serve` actually returns. Server shutdown never issues a drain, retires a
Runtime, closes the owner, or unlinks the socket pathname. This avoids deleting
a replacement pathname during listener closure. Socket creation, ownership,
mounting and exact-identity retirement belong to protected startup assembly.

Atomic counters expose accepted connections, overloads, verified handshakes,
transmitted responses, failures, current in-flight work and peak concurrency.
A verified handshake is not an authorized role; a transmitted response can be
a domain rejection and does not prove receipt or a committed transition.
Concurrent snapshots are not transactional. After joining, the checked balance
is `Accepted = Overloaded + Replied + Failed` with `InFlight = 0`.

## Native evidence

The tests run in the pinned Linux/arm64 static race image with a root Node and
separate actual non-root PID-1 Runtime/Worker/sibling processes. Journal files
remain root-private. CRI inventory and backend execution are fixtures; these
tests do not establish Fleet/Registry approval or production effective launch.

`TestJournalServer` exercises both resource refusal and subsequent useful work:

1. Two idle connections receive real challenges and occupy both configured
   slots before submitting a payload. A same-UID sibling is used deliberately:
   it passes credential filtering but has no journal role.
2. Thirty-two additional authenticated-client connection attempts are refused.
   No additional handshake completes and descriptor count does not grow.
3. Disconnecting the idle peers releases both slots and their descriptors.
   The sibling cannot read the journal; Worker cannot issue Runtime admission.
4. Authorized Worker reads succeed. The actual remote Supervisor completes
   Prepare, Start, Seal and Drain; the Worker then installs its signed floor.
   The root owner records `Highest=1`, `Floor=1`, `PendingExecutions=0`.
5. Shutdown cancels two more idle handshakes, joins all work, keeps the owner
   usable, preserves a replacement socket pathname and prevents server restart.

Final counters: **73 accepted, 32 overloaded, 37 verified handshakes, 36 replies,
5 failed exchanges, zero in flight, peak two**. Replies include a Worker-role
domain rejection; these are not 36 successful mutations or completed Jobs.

`TestJournalServerTimeoutAndJoin` separately verifies idle-timeout slot recovery.
It then holds the endpoint mutex to model an operation that cannot immediately
observe cancellation. Shutdown reports its deadline while the handler remains
in flight, and joins only after the lock is released. This is deterministic
handler-blocking evidence, not an injected kernel fsync stall.

`TestJournalServerListenerFailure` proves a failed listener cancels and joins
pending handshakes. `TestJournalServerConfiguration` covers invalid bounds and
credentials, wrong socket type and shutdown before serving.

## Validation and boundaries

Final-source checks passed:

- Native race: 47 ModelRuntime behavioral main tests plus one helper entrypoint,
  and 11 Node main tests; zero skips and race reports.
- Ordinary repository tests, vet, ordinary lint and Linux changed-package lint.
- Linux/amd64 changed-package cross compilation and native-runner shell syntax.

An earlier Linux lint run reported two capitalization diagnostics in new error
strings. Both were fixed; the failed log is retained alongside the final pass.
The [receipt](journal-server-evidence-2026-09-08.json) binds the source digest and
[raw artifacts](evidence/journal-server-2026-09-08/). The native patch includes
new Go files and reverses against the validated tree. Only
`native/node-startup.log` and `native/source.patch` retain raw whitespace exempted
from the changed-file whitespace check; their exact bytes are hash-verified.

```sh
bash hack/run-journal-owner-native.sh
go test ./...
go vet ./...
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go test \
  ./internal/nodeagent ./internal/modelruntime ./internal/runtimechannel \
  -run '^$' -exec /usr/bin/true
```

This server is exercised by actual authenticated clients and Supervisor behavior
under explicit test assembly. It is not yet composed into `cmd/vela-node-agent`
or Fleet workload mounts, does not authorize startup, and does not take custody
of the separate Worker input/materialization journal. The finite overload test
does not prove sustained throughput, fairness under hostile traffic, total CPU
or memory limits, or a fully remote CPU Job/cache/fault campaign. Full process
replacement/reconciliation, outage containment, safe history reclamation and
sustained offered-load validation remain open. Prior CPU Job/cache evidence is
still pinned to its earlier commit; those campaigns were not rerun for this
Linux Node service addition.

The overload burst precedes construction of the test Supervisor. An already
attached Supervisor encountering overload still needs a separate recovery test:
its existing remote adapter treats generic read transport failures as recovery
errors. This test establishes server capacity recovery and subsequent legal
work, not uninterrupted progress of an existing Supervisor through overload.
