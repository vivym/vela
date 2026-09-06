# Automatic terminal history recovery

Local CPU/mock increment over `6c770c2`. Worker admission and Runtime journals
remain **4**, materialization journal **2**, database **90**, and launch/Fleet
schemas **2**. Production Gates remain **0/9**. No GPU or remote deployment.

## Behavior

`DurableStreamConfig.TerminalHistory` optionally supplies a
`TerminalDispositionReader`. The existing authenticated
`stageworkertransport.Client` implements that interface directly. Configuration
requires `TerminalRetirement` for the same Stream, admission gate and Runtime.
Default command assembly still retains scratch and does not enable this path.

After recovering existing READY/RETIRED checkpoints, `ResumeMaterializations`
groups retained admission and materialization execution envelopes by StageRun.
It uses the highest retained execution sequence as the query anchor, preserves
the original Acquire command ID when available, and selects the latest signed
renewal when the two journals retain different versions of one allocation.
Conflicting envelopes or Acquire IDs reject the pass. Expired authentic
execution envelopes remain usable as historical query evidence.

One Runtime floor timeout bounds fresh history queries and proof collection for
the pass. The transport checks the current authenticated Control stream/session;
the Stream independently revalidates signature, expiry, exact anchor/member
binding and verified digest before installing a floor. Runtime targets come
from trusted configured historical bindings, never from untrusted discovery
data in the response. The response still must cover every allocation/member.

Fresh complete history drives the existing INTENT -> READY -> RETIRED protocol,
including all input writers, persistent Worker/Runtime floors, execution drain
or non-admission, and inode-bound filesystem retirement. Available execution
envelopes are used as-is. An allocated but undelivered retry can use the existing
terminal non-admission protocol; recovery never synthesizes its authority.

RETAIN without INTENT preserves state and permits ordinary recovery. RETAIN
with INTENT remains incomplete and blocks ordinary replay/discovery. INTENT
without any retained query envelope returns `ErrAdmissionRecoveryRequired`.
Lost floor/proof replies retain INTENT for a fresh signed query and retry.
READY/RETIRED require no fresh history query or Runtime RPC, even after expiry
or when Control is offline. Context cancellation observed after a reader returns
prevents subsequent installation or deletion.

The Stream clears obsolete matching materialization records only after durable
RETIRED. `TerminalRecordsRetired` remains a count of local record deletion; it
does not acknowledge COMMIT, SOURCE_LOST, object publication or billing.

## Validation

Passed:

```sh
go test ./...
go test -race ./internal/stageworkeragent
make lint
go vet -tags=integration ./internal/integration
go test -tags=integration ./internal/integration -run '^TestStageTerminalHistoryCoversAllocatedUndeliveredRetry$' -count=1 -timeout=5m
go test -tags=integration ./internal/integration -run '^(TestStageTerminalHistoryReadsRenewalWithoutAcquireID|TestStageTerminalDispositionThroughAuthenticatedControl|TestStageMaterializationCannotStartAfterTerminalCancellation|TestStageMaterializationHandlerReplaysTerminalCommandsAfterTTLAndReconnect)$' -count=1 -timeout=5m
git diff --check
```

Focused unit/UDS checks cover automatic first collection, lost proof response,
Worker reopen with expired history and refreshed retry, persisted renewal versus
stale materialization envelope, one query for multiple records of a StageRun,
RETAIN, INTENT without a query envelope, offline READY/RETIRED recovery, malformed
reader results, missing trusted Runtime binding, cancellation after the query,
query mutation and conflicting signed retained envelopes. The first collection
test executes and seals the original allocation in the fake Runtime, while the
undelivered retry has no execution envelope and supplies non-admission proof.
Local materialization records and scratch bytes are constructed fixtures; this
does not claim an end-to-end backend output/publication campaign.

The PostgreSQL integration persists an actual issued assignment and its Acquire
ID in the Worker journal using the issuance-time test clock, then reopens with
the expired-envelope validator. `ResumeMaterializations` itself queries the real
authenticated mTLS Control transport, obtains the terminal history including an
allocated but undelivered retry, collects Runtime proof over a private UDS, and
retires exact scratch namespaces. A second recovery succeeds with Control
closed. This fixture has an empty materialization journal and no backend
execution intents; it proves automatic database-to-filesystem retirement, not
output production or pending-writer recovery. The Runtime floor/non-admission
prerequisites are also exercised separately before this automatic pass.

Linux arm64 passes
`^(TestTerminalRecovery|TestTerminalMaterialization|TestTerminalScratchRetirement|TestAssignmentAdmission|TestAssignmentInputDrain|TestAssignmentFloor)`
under UID/GID 65534, no network, read-only root/binary mounts, all capabilities
dropped, no-new-privileges and a private `/tmp` tmpfs. Image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

## Production recovery availability

Review after `8fa331a` reproduced a recovery stall when a Control stream exists
but its epoch has not been registered in PostgreSQL. The production loop checked
only connection availability before recovery. A registration failure after
opening the stream left it available, so subsequent iterations queried terminal
history without retrying registration. A reconnect during a query caused the
same problem. PostgreSQL correctly returns RETAIN for an unmatched session.

The `e1d423c` repair required successful readiness registration first. Further
review exposed a dependency cycle: a Runtime with pending recovery or full
execution history rejects readiness, so it could not reach that recovery. The
old sequence also advertised ready capacity before checking retained writers.

Production recovery now uses the existing zero-capacity report to synchronize
the authenticated PostgreSQL session independently of local Runtime readiness.
The sequence for an automatically configured Stream is:

1. Resume already complete local READY/RETIRED checkpoints.
2. If retained candidates require fresh history, confirm a zero-capacity lease
   in the current Control session. Synchronization is inside the bounded history
   pass and uses the durable production observation sequence.
3. Query history and complete input/floor/member exclusion and retirement.
4. Only after recovery succeeds, probe/register readiness, publish usable
   capacity and acquire work through the existing discovery path.

The internal recovery callback runs only when fresh history candidates exist;
the public Stream API keeps its existing signature. A fresh confirmed zero lease
can be reused across incomplete INTENT retries. Expired leases are renewed, and
session changes or lost responses require a confirmed report. Failure to persist
an accepted observation sequence locally preserves that exact sequence for
replay but invalidates lease reuse until confirmation/persistence succeeds.
Missing input or
Runtime proof keeps recovery incomplete without probing readiness or restoring
capacity. Already completed local checkpoints do not depend on an online
Control session or Runtime, including inside Production recovery; continuing
normal service after that cleanup still requires online readiness and authority.

`TestTerminalRecoveryProductionSynchronizesUnavailableSessionBeforeHistory`
reproduced premature ready-capacity publication and the readiness/recovery cycle
before this change. Its repaired cases cover an already-open stream, failed
registration after recovery, reconnect during history lookup, Runtime readiness
that only succeeds after retirement, a lost zero-capacity response, and local
sequence persistence failure followed by exact replay. Another
regression keeps an unknown input writer at INTENT across retries and verifies
zero capacity, no readiness probes, no registration/acquisition, retained
scratch and reuse of one confirmed zero lease.

The PostgreSQL-to-filesystem integration now runs the actual Production loop
with a durable session/observation source and a deliberately unavailable
readiness client. Its zero-capacity report synchronizes the real mTLS Control
session, automatic history lookup completes retirement, and only then is
readiness probed. PostgreSQL retains a zero vector in the current session and
rejects the original session's history lookup. The later incomplete-history
checks use the newly confirmed database session. No runtime registration or
usable capacity is invented for the unavailable backend.

After this change, full unit tests, Worker race, lint, integration vet,
`TestStageTerminalHistoryCoversAllocatedUndeliveredRetry` and
`TestStageTerminalDispositionThroughAuthenticatedControl` pass. Non-root Linux
`^(TestTerminalRecovery|TestTerminalMaterialization|TestProductionAgent)` also
passes with the same container restrictions and image listed above. This uses the
existing database session/capacity authority: the Worker and member still need
the persisted Fleet lifecycle/identity required by that authority. Recovery of
removed or non-READY Fleet identities is not established by these tests.

The [replacement-owner integration](runtime-replacement-floor-evidence-2026-09-06.md#postgresql-replacement-and-capacity-recovery)
also restarts the durable Runtime at a new epoch, selects an explicit trusted
`ExecutionFloorConfig.CurrentReaders` map and reopens the original Worker
admission history. It proves durable retirement before readiness, zero database
capacity throughout the four actual CPU mock probes, accepted new-epoch
registration, restored capacity and a durable `NO_WORK` Acquire result. No
terminal allocation is recreated. Complete non-admission checkpoints and the
approved mock readiness digest are test prerequisites, not generated production
writer or Fleet provisioning evidence. The related integration race selection
passes; broader integration-tag lint still has pre-existing findings as detailed
in that evidence document.

## Remaining scope

Default command/bootstrap assembly and explicit migrations, unknown historical
input writer recovery, pending Runtime writer drain and lost renewal recovery,
durable sealed receipt recovery, and bounded Worker/Runtime checkpoint
reclamation remain open. This collector observes existing execution completion;
it does not cancel a pending backend or manufacture its completion proof.
Admission/retirement checkpoints remain bounded and retained, so this increment
does not establish sustained Worker progress or bounded lifetime scratch use.
External asynchronous driver tasks, descendants and writable handles still
require their own containment/drain implementation.
