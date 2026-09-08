# Authenticated execution journal endpoint

Date: 2026-09-08. Baseline: `eda1bd8`. PostgreSQL 94, Worker journal 5,
Runtime journal 8. Production Gates remain **0/9**.

## Result and authority boundary

There is now an independently usable `ExecutionJournalOwner` and an authenticated
Linux Node `JournalEndpoint`. The owner holds the journal lock, validates signed
typed mutations and complete candidate state, and persists before acknowledging.
The endpoint retains separate original kernel pidfds for Runtime and Worker.
Closing the enrollment observations does not destroy those retained bindings;
closing the endpoint revokes them. A same-UID sibling is not the same process.

The canonical version-1 command permits exactly one of eight operations:

| Approved original process | Allowed operations |
| --- | --- |
| Runtime | admit, candidates, seal, drain, health |
| Worker | floor, execution non-admission, terminal non-admission |

The request contains no role, route, path, replacement snapshot, initialization,
owner timestamp or startup permission. Current routes and clock skew are trusted
Node configuration. Configured epochs must be positive and meet each manifest's
epoch floor. Incoming protobufs are bounded and canonical; duplicate/unknown JSON
fields, multiple operations and noncanonical framing are rejected.

Backend startup intent remains a separate trusted orchestration call. Neither
startup intent nor a journal receipt grants backend execution, restart, writer
exclusion, scratch deletion or renewed authority time. Retained exact admission
history may be acknowledged after expiration, but is never redispatched by this
API. New terminal non-admission still requires a fresh terminal disposition.

Worker non-admission requires the originally approved Runtime to remain live at
the endpoint check. A signed floor may still be installed after that process
exits because it only restricts admission. This is a sampled prerequisite, not
proof of process lifetime through fsync or a replacement-process protocol.

The existing channel retains its **32 KiB** default request and reply bounds.
An explicit trusted opt-in allows **192 KiB** requests, enough for two maximum
64 KiB protobuf envelopes plus JSON base64. The reply remains 32 KiB. The same
root peer/socket, challenge, credential, per-message pidfd and deadline checks
apply; there is no numeric-PID or UID-only fallback and no automatic retry.

Domain rejection leaves accepted state usable. Storage/publication uncertainty
poisons the owner. Cancellation after durable publication returns an uncertain
outcome to the caller; a later legal exact retry can acknowledge the retained
record. The client binds acknowledgements to its independently supplied journal
UUID/scope and the request digest, and rejects malformed or ambiguous responses.

## Validation

The portable API tests exercise all eight legal operations, exact retries without
inode replacement, malformed commands, wrong role/signature/scope/current epoch,
expired admission, required startup intent, closed owners, post-rename sync
failure, cancellation after successful sync, health denial and the 32-execution
history bound. Reopening an unresolved incarnation cannot issue another startup
intent. No journal API calls a backend.

Native Linux tests use a root Node and three separate non-root namespace PID-1
processes. They exercise actual kernel handles, enrollment disconnect, independent
retention, same-process role collision, Worker admission denial, same-UID sibling
denial, filesystem open/create denial, admission reply loss and reconnect replay,
client journal-identity mismatch, Runtime floor denial, Runtime exit and endpoint
closure. The exit test checks the specific owner-lifetime refusal, avoiding a
false pass caused solely by the admitted sequence's domain restriction.

The existing channel regressions also execute against the changed transport:
inherited/delegated senders, ancillary rights/truncation, changed sockets,
untrusted root replies, cancellation, one-shot replies and original caller loss.
An actual **196,608-byte** seqpacket round trip succeeds. The default receiver
rejects it, and the larger-request endpoint still rejects an oversized reply.

| Check on final source | Result |
| --- | --- |
| Full uncached host ModelRuntime race | PASS, 101.470 s |
| Ordinary repository tests | PASS; unchanged packages may use Go cache |
| Repository vet; ordinary lint; Linux-specific changed-package lint | PASS; both lint runs report 0 issues |
| Linux/amd64 cross compilation | PASS; compilation only |
| Native Linux/arm64 static race binaries | 38 behavioral ModelRuntime main tests plus one helper, 7 Node main tests; no skips or race reports |
| Original root Node/non-root startup exchange | Four scenarios pass: mock permit, denial, lost response, record uncertainty |
| PostgreSQL replacement-Runtime recovery | PASS, 7.960 s package run |
| Explicitly enabled CPU race campaigns | Three campaigns PASS, 59.208 s combined |

The first native attempt rejected a fixture route with epoch zero; the fixture
now explicitly supplies its approved live epoch. The first added health test
omitted confirmation of backend authority and was correctly rejected; the legal
workflow now confirms it first. These failed development attempts are retained
separately and are not counted as final validation. The first combined integration
command did not enable CPU campaigns, so its 7.960 s result only supports the
PostgreSQL recovery check. The subsequent opt-in run executes all three campaigns.

The final production-loop campaign uses two waves of eight Jobs, shared **30 s**
skew and a **-1 s** consumer verifier offset. All four Workers record 16 actually
future-issued assignments. It completes **16 Jobs / 64 Stages**, with exactly
**16 Charges** and **20,000 minor units**. At idle, scratch, input/output bytes,
active leases/allocations, reserved credit/storage and execution/finalization
pins are zero. Retained journals total **654,822 bytes**, so this is not proof of
bounded lifetime history. Direct and durable-stream cache campaigns each execute
four source stages and two target stages, with five consumed transfers and
their exact object/pin/billing assertions intact.

These CPU campaigns still use the existing local Supervisor/Worker persistence
path. They establish compatibility with this source, **not integration of the
new endpoint into the production Job path**. The initial historical `11ce026`
STALE failure remains causally unresolved. Ordinary lint does not supersede the
existing integration-tag backlog.

## Reproduction and provenance

[Machine-readable evidence](journal-endpoint-evidence-2026-09-08.json) records
full campaign receipts, raw artifact hashes and the native image identity.
[Raw evidence](evidence/journal-endpoint-2026-09-08/) includes the source patch,
build logs, successful checks and the two development fixture failures. Native
tests use the pinned builder and isolated scratch image described by
[the runner](../hack/run-journal-owner-native.sh); no GPU, network or host mounts
are available to those test containers. PID namespace tests explicitly receive
the required namespace/process-inspection capabilities. CRI/native task metadata
is fixture input, not independent Registry/Fleet approval.

The captured patch applies to `eda1bd8` and reconstructs all **181** files under
the three affected packages plus the runner byte-for-byte. The CPU campaign's
Go/SQL/toolchain/media source digest is
`80c719afde4829e52e8f742ea1cf81c01472b8f10e0518dcd92b4c879e3bbbbf`.
Source stayed fixed through the final full/native/campaign checks; subsequent
changes add only this evidence and documentation.

Raw bytes and hashes are preserved. Whitespace checking excludes only
`campaigns.log`, `initial-native-fixture-failure.log`, `native/node-startup.log`
and `native/source.patch` under this evidence directory: the logs retain emitted
indentation/trailing spaces and the patch retains Git context prefixes. All
other staged files pass the normal whitespace check.

```sh
go test -race ./internal/modelruntime -count=1
bash hack/run-journal-owner-native.sh
go test -tags=integration ./internal/integration \
  -run '^TestStageTerminalRecoveryThroughReplacementRuntimeRestoresCapacity$' -count=1
VELA_RUN_CPU_MOCK_CAMPAIGN=1 go test -race -tags=integration ./internal/integration \
  -run '^(TestCPUMockProductionLoopClockOffsetCampaign|TestCPUMockExactCacheSourceTargetCampaign|TestCPUMockDurableStreamExactCacheCampaign)$' \
  -count=1 -v -timeout=8m
```

## Remaining work in dependency order

1. Connect a tested remote persistence abstraction to live Supervisor and Worker
   operation ordering, including Node unavailability, timeouts and uncertain
   outcomes. A typed adapter alone cannot replace their admission/active-call
   synchronization or invent backend execution permission.
2. Assemble trusted Registry/Fleet route-to-process approval, first-use/adoption
   provenance and root-private mount/UID boundaries. Present root ownership and
   CRI correlation do not establish these facts. A Node accept loop must also
   bound concurrent handshakes before allocating large request buffers.
3. Complete Node restart reconciliation and original-process exit/replacement,
   with crash injection across startup permission and every durable response.
   Test a successful replacement path as well as refusals. Backend/resolver
   descendants and inherited filesystem access require explicit containment.
4. Reclaim retained history using independently proven terminal ownership, then
   exercise more than 32 executions and sustained arrivals with bounded queues,
   journals and resources. Compare direct and remote persistence under matched
   operation mixes and contention before making latency/throughput claims.
5. Re-run the full CPU Job/cache/fault chain through the actual protected owner,
   resolve the remaining validation backlog, and preserve separate production
   evidence requirements. This increment changes no schema, deployment initializer,
   workload mount or GPU state and advances no Production Gate.
