# Terminal recovery through replacement Runtime journal owners

Local CPU/mock increment over `23089f4`. Worker and Runtime journals remain
**4**, materialization journal **2**, database **90**, and launch/Fleet **2**.
Only the execution-floor RPC request/response adds schema **2**. Production
Gates remain **0/9**. No GPU or remote deployment.

## Failure and behavior

The regression `TestTerminalScratchRetirementRecoversThroughReplacementRuntimeOwners`
initially failed with `historical Runtime binding is missing or ambiguous`.
The Runtime journals held complete original non-admission proof, but terminal
retirement required every historical epoch/profile route to remain resident.
Restarting those journal owners therefore prevented otherwise proven cleanup.

An admission restriction and historical writer exclusion have different scope.
V1 floor installation retains the resident-historical-route requirement. V2
targets an exact current resident identity and restricts its durable member-wide
journal after validating fresh signed history against independently trusted
Worker/member/device topology. It uses the existing admission lock, checks state
ownership and only raises the floor. An in-memory Runtime cannot acknowledge V2.
Neither version drains writers or turns absent old history into non-admission.

The Worker validates all supplied current readers against its complete trusted
bindings and signed topology before dispatch. Member forwarding retains mTLS
leader authentication, pinned target identity and complete membership checks.
Both hops require the reply version to equal the requested version, along with
the exact identity, disposition digest, cutoff and durable acceptance. Unknown
versions reject. A lost reply or partial installation can be retried in full.

Terminal retirement uses V2 restriction installation while retaining all input
completion and per-allocation/member execution proof requirements. Complete
prior proof permits INTENT to advance through current owners after Runtime
epoch/profile replacement. A missing old checkpoint leaves INTENT and scratch
intact, even when all current floors are installed. Pending historical execution
continues to block readiness and new execution above the floor.

`ExecutionFloorConfig.CurrentReaders` optionally selects every current owner for
automatic terminal recovery. Each entry must match exactly one configured
binding; the constructor validates and clones the map and identities. Omission
keeps historical-target selection, which cannot recover absent historical routes.
Configuration must obtain these identities through a trusted assembly path;
signed historical allocations do not establish a replacement reader identity.

## Compatibility

New Worker readers accept retained V1 and V2 floor acknowledgements. Older Worker
readers reject a journal containing V2 acknowledgements even though the outer
Worker journal remains schema 4. Once such proof is retained, use the newer
reader for recovery; binary rollback is not supported by this increment. Do not
rewrite reply versions, strip proof or erase journals to make rollback succeed.
Mixed deployments with a V1-only Runtime/member hop cannot complete V2 retirement
and retain scratch. Protocol field layout is unchanged; this is not a claim of
backward operational compatibility.

## Validation

Passed `go test ./...`, `make lint` (0 issues), `go vet -tags=integration
./internal/integration` and race checks for `internal/modelruntime`,
`internal/modelruntimetransport`, `internal/stageworkeragent` and
`internal/stageworkermembertransport`.

Focused tests cover replacement epochs/profiles, V1 rejection of absent routes,
V2 floor persistence/replay, missing writer proof, invalid Worker/device/member
scope, invalid signatures/expiry, unknown readers/versions, unavailable journals,
ambiguous configuration and reply-version substitution at both transport hops.
Actual loopback mTLS to private UDS to durable state covers V2 reply loss/retry,
nonleader rejection and another Runtime restart. Automatic Stream recovery
restarts both Worker and Runtime after lost proof, refreshes expired signed
history and verifies current-reader configuration cloning.

PostgreSQL/Control integration passes the selected tests
`TestStageTerminalHistoryCoversAllocatedUndeliveredRetry`,
`TestStageTerminalDispositionThroughAuthenticatedControl` and
`TestStageTerminalHistoryPreservesHistoricalRuntimeScopesWhileDraining`.
The initial selection exercises automatic terminal recovery without replacing
its Runtime owner. The subsequent integration below also covers replacement.

Linux arm64 passes Runtime `^(TestExecutionFloor|TestDurableExecution)`, Worker
`^(TestExecutionFloor|TestTerminalScratchRetirement|TestTerminalRecovery)` and
member `^TestMember.*Floor` with UID/GID 65534, no network, read-only root/binary
mounts, all capabilities dropped, no-new-privileges and a private `/tmp` tmpfs.
Image: `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

Default Worker command assembly still uses scratch retention and lacks durable
Agent/Stream bootstrap and complete trusted historical/current topology wiring.
Fleet first-use provisioning, unknown input writer recovery, pending Runtime
drain/lost renewal recovery, durable sealed receipts, failed-backend containment
and bounded checkpoint reclamation remain open. These checks do not establish
deployed automatic retirement, sustained Worker throughput or Production Gates.

## PostgreSQL replacement and capacity recovery

A subsequent test increment over `0ee9cc2` adds
`TestStageTerminalRecoveryThroughReplacementRuntimeRestoresCapacity`. The original
owner checkpoints non-admission for both real database allocations, including
the retry with no issued execution envelope. It shuts down and a new Supervisor
opens the same journal at the next Runtime epoch. The Worker recovers its original
Acquire/input-completion journal with the new trusted current binding.

The actual Production loop confirms zero capacity/current Control session,
fetches fresh signed history over mTLS, collects proof through the replacement
Runtime UDS and persists RETIRED after deleting exact scratch namespaces. Every
subsequent readiness probe verifies that retirement is already durable and the
database still advertises zero capacity. Four actual CPU mock probes then allow
Control to register the new local Runtime epoch and publish the original usable
capacity vector. The real PostgreSQL assignment backend returns a durable
`NO_WORK` result in that session; no terminal StageRun allocation is recreated.
RETIRED replays after Control closes, unrelated scratch survives, and original
non-admission proof remains readable after another Runtime epoch change.

The fixture independently seeds the known CPU mock readiness digest as approved
Fleet evidence before replacement. Control still checks actual probe bytes
against that digest; arbitrary mock evidence was correctly rejected during test
construction. This setup does not exercise production Fleet provisioning or
certify GPU readiness. It has no admitted backend execution and an empty
materialization journal, so pending writer and sealed output recovery remain
outside its evidence boundary. The original unavailable-readiness integration
is retained and still requires zero capacity after local retirement.

The four related PostgreSQL tests, including the replacement case, pass with
`go test -race -tags=integration ./internal/integration` and the corresponding
anchored test-name selection. Integration `go vet` and `git diff --check` pass.
Generated artifacts reproduced cleanly with `make verify-generated` at `0ee9cc2`;
this follow-up changes tests and documentation only.

The broader integration-tag lint reports 82 existing findings in untouched
files: 50 errcheck, 4 staticcheck and 28 unused. Therefore the whole integration
package is not lint-clean. The same lint with
`--build-tags=integration --new-from-rev=0ee9cc2 --whole-files` reports 0 issues
for this increment. The earlier ordinary `make lint` result excludes this extra
build-tag scope; these results must not be conflated.
