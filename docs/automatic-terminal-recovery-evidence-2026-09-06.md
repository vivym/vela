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

## Production session registration follow-up

Review after `8fa331a` reproduced a recovery stall when a Control stream exists
but its epoch has not been registered in PostgreSQL. The production loop checked
only connection availability before recovery. A registration failure after
opening the stream left it available, so subsequent iterations queried terminal
history without retrying registration. A reconnect during a query caused the
same problem. PostgreSQL correctly returns RETAIN for an unmatched session.

For a Stream with automatic terminal history enabled, the production loop now
requires successful readiness registration in its current Control session before
recovery. It tracks the epoch already recorded by the existing readiness path;
connection establishment alone cannot satisfy that condition. Failed
registration or a changed epoch retries through the existing bounded backoff.
The ordinary post-recovery discovery path remains responsible for acquisition.

`TestTerminalRecoveryProductionRegistersCurrentSessionBeforeHistory` failed
before the repair for all three triggers: an already-open unregistered stream,
registration failure with a surviving stream, and reconnect during history
lookup. The repaired tests finish retirement and resume acquisition without a
history query in a known unregistered session. These are ProductionAgent
CPU/mock loop tests using real local Runtime UDS and constructed materialization
records. The authenticated PostgreSQL test separately verifies RETAIN for a
mismatched database session.

After this repair, full unit tests, Worker race, lint,
`TestStageTerminalDispositionThroughAuthenticatedControl`, and Linux non-root
`^(TestTerminalRecovery|TestTerminalMaterialization|TestProductionAgent)` pass.
The offline READY/RETIRED guarantee belongs to `StreamAgent.ResumeMaterializations`;
the enclosing Production service loop still requires online registration and
readiness. This change does not provide a separate offline service startup mode.

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
