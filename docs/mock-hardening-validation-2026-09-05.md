# Vela Architecture and CPU Mock Validation

Date: 2026-09-05
Baseline: `1a99484fbadffcb0ac6d6edd3bf2a5ea471ccda2`
Work branch: `feature/vela-mock-hardening`
Current local migration: `94`
Status: In progress

Latest production-code checkpoint (2026-09-08):
[typed startup/non-admission owner contract](journal-owner-contract-evidence-2026-09-08.md)
routes the remaining three ordinary Runtime journal mutations through owner
validation. A deterministic baseline failure proves a terminal disposition could
expire after ingress yet produce a new checkpoint; final-owner validation now
rejects without poisoning legal retry. Host/native race, startup exchange,
PostgreSQL recovery and current-source production-loop/cache checks pass.
This completes internal mutation discipline, not Node-private custody or startup issuance.

The preceding
[production clock-policy validation](clock-policy-evidence-2026-09-08.md)
aligns CPU campaign admission, Runtime, Control, transfer and materialization
with the shared production skew bound. Deterministic tests reproduce zero-skew
rejection and preserve rejection beyond the bound and after expiration. The
consumer-offset campaign records actually future-issued assignments separately
from its configured offset. The old `11ce026` failure remains causally unresolved;
this change does not establish a fix for that historical run.

The preceding
[profile-guided history lookup](journal-lookup-evidence-2026-09-08.md) preserves
full validation and exact historical identity while checking the newest record
first. All focused/full ModelRuntime and ordinary repository checks pass, as do
three race and three no-race 32-Job final-source campaigns plus exact-cache
compatibility. A separate old-version no-race run failed with a stale assignment;
its evidence is retained. The campaign's zero clock-skew defaults differed from
production's shared policy; the latest checkpoint addresses that discrepancy. No
full custody, process-replacement or sustained-operation closure is claimed.

The preceding
[local proof reuse](journal-proof-reuse-evidence-2026-09-08.md) avoids repeated
verification of identical authority bytes within one complete journal check.
New calls use fresh verifier/scope context. Current-source full ModelRuntime race,
ordinary repository checks and the serial 32-Job production-loop campaign pass.
The measured end-to-end improvement is modest; full-validation performance cost,
protected custody and sustained operation remain open.

The preceding
[journal transition validation](journal-transition-evidence-2026-09-08.md)
independently checks six typed mutations and validates the complete candidate
before every file publication. New regressions cover forged inputs, rejected
draft isolation and pre-write expiry without poisoning subsequent admission.
Whole-repository ordinary tests/vet/lint, Linux cross compilation, ModelRuntime
race and current-source production-loop/exact-cache campaigns pass. This prepares
the Node custody contract; it does not implement the protected owner or startup issuer.

The
[ProductionAgent.Run CPU campaign](cpu-production-loop-evidence-2026-09-08.md)
completes 32 Jobs/128 Stage executions under race detection, including production
readiness/capacity/heartbeat, automatic session advance and lost commit replay.
The preceding [durable stream campaign](cpu-durable-stream-evidence-2026-09-08.md)
also covers exact-cache source/target and explicit StreamAgent reconstruction.
Both match PostgreSQL/Worker/Runtime history with zero final payload scratch.
The 32-record Runtime bound, trusted custody/startup, full process recovery,
history reclamation and open-loop operation remain open. Runtime schema-8
[health durability](worker-health-durability-evidence-2026-09-08.md) is also retained.
Earlier measurements below remain bound
to their original revisions.

## Objective and evidence boundary

Complete an independent architecture/correctness review, repair confirmed
defects, and validate Vela through reproducible CPU-only integration and mock
campaigns. The scope includes runtime behavior, isolation, billing, storage,
recovery, deployment, and sustained control-plane operation. A passing narrow
test does not close the whole objective.

The expanded objective also requires scientific validity: state model invariants,
capacity/scheduling assumptions, metric definitions and units, analytical bounds,
and reproducible comparisons for architecture changes. Implement better
architecture where evidence supports it; preserve the full correctness scope.

Real GPU execution, real H3 quality/performance certification, and Production
Launch Receipts require separate evidence. CPU mock results do not advance the
nine Production Gates. Preserve existing lab data, evidence, and other workloads.

## Architecture judgment

The evidence supports retaining PostgreSQL durable authority, independent H3
Stages, exclusive WorkerInstance/DeviceSet ownership and resident ModelRuntimes.
The confirmed faults were missing lifecycle transitions, incomplete authority
guards, inconsistent lock order and unbounded transient work. No reviewed result
supports replacing this design wholesale with Kubernetes-owned Job state or a
monolithic execution fallback.

| Decision | Review conclusion | Required qualification |
| --- | --- | --- |
| PostgreSQL owns execution and commercial state | Admission, credit, completion and Charge can share transactional invariants; failover preserves durable replay | Every new authority write needs quorum, post-lock time validation and consistent parent/child locks |
| Independent Stages and resident Workers | Retain the accepted H3 ownership model; parallel CPU execution and deterministic capacity oracles exercise the topology | Mock timings do not calibrate GPU throughput, memory pressure or optimal placement |
| Immutable L2 StageArtifact before downstream work | Retain the recovery/cache boundary; separate Job-owned public copies isolate customer deletion from producer lifetime | Conditional storage publication and permanent deletion fences are necessary in addition to database CAS |
| Read-only acquisition hints before durable scheduling | Empty and wholly filtered queues do not need replay records; the actual assignment still captures fresh evidence and runs the existing scheduler | Hints may defer work by one poll; they never select a winner or replace authority checks |
| Fixed customer Charge separate from internal Usage/Cost | Retain the commercial contract; actual producer receipts now flow through independent valuation/replay | Allocation occupancy is not GPU time; synthetic rates, source-duration estimates and incomplete telemetry cannot establish full economic savings |

The original `/tmp/vela-handoff-2026-09-05.md` remains a schema-69 historical
handoff. Local commit `a9a1f7abb5613eac981625e1948ef162a647cd9b` preserves the
schema-88 hardening checkpoint and its evidence. Remote lab deployment state
has not been changed by this campaign; subsequent lifecycle work remains open.

## Acceptance matrix

| ID | Required behavior | Evidence required | Status |
| --- | --- | --- | --- |
| A1 | Explicit module authority and current contracts agree | Review of effective SQL, Go, protocol, and deployment ownership | In progress |
| A2 | Admission, capacity, fairness, and bounded WIP remain consistent | Concurrent capacity/running-limit/backpressure tests and mock load | Targeted concurrency/lock/quorum tests and same-source 512-Job load pass |
| A3 | Replay, restart, cancellation, and stale epochs cannot corrupt execution | Stage fault/recovery campaign with rejected stale probes and terminal convergence | Current dual-journal and native ProductionAgent session/replay campaigns pass; full process replacement, protected startup and comprehensive fault composition remain open |
| A4 | Exact cache performs miss/admit/hit/reuse through configured runtime | Two equivalent Jobs with distinct identities, fixed seed, exact output/pin bindings | Native CPU source 4 / target 2 stages, two exact cache entries and separate Job public copies pass; remote lab pending |
| A5 | Cache, transfer, retention, and deletion preserve content isolation | Project/Organization negative tests, expiry/deletion/pin race tests | Targeted and final local integration pass; scratch writer exclusion remains open |
| A6 | Completion and customer Charge occur at most once | Physical/cache/cancel/replay outcomes and independent Usage/Cost checks | Targeted regressions passed; accounting coverage partial |
| A7 | Quiescence and backup can be used for actual recovery | Closed Admission, consistent authority snapshot, isolated PostgreSQL restore and replay | Isolated database drill passed; object recovery separate |
| A8 | Idle and loaded operation have bounded resource growth | Polling/record lifecycle review, sustained mock observation, capacity evidence | Older 64-wave direct-service convergence passes; current ProductionAgent.Run reaches 32 records per Runtime with zero payload scratch but growing journal metadata; reclamation and sustained operation remain open |
| A9 | Deployment and verification exercise current entry points | Generated contracts, lint, build, all integration packages, CPU runtime composition | Historical schema-88 broad verification is retained; current native ProductionAgent.Run/Control stream campaigns pass; Registry-bound protected Node/executable composition remains separate |
| A10 | Review findings are repaired and independently rechecked | Finding ledger, regression tests, final requirement-by-requirement audit | Initial review: 38 findings, 34 fixed, 1 false positive, 2 partial, 1 open; subsequent content-lifecycle repair verified separately; overall closure pending |
| A11 | Capacity and scheduling models are scientifically defensible | Independent analytical/oracle checks, dimensional consistency, deterministic load and failure experiments | 18 public-API formula/oracle checks and random conservation pass; production scheduler replay remains unmodeled |
| A12 | Architecture improvements solve observed problems | Before/after invariants, overhead, convergence, and measured mock comparisons | In progress |

## Current closure priorities

The following priorities supersede the historical schema-89/90 list below.
Current evidence is PostgreSQL 94, Worker journal 5 and Runtime journal 8;
Production Gates remain **0/9**.

1. Implement and validate the [Node-private journal custody candidate](node-journal-custody-design-2026-09-08.md),
   including typed authenticated transitions, protected mounts and Registry/Fleet
   startup ordering. Current workload-owned files and passive lock observations
   do not establish independent ownership continuity after process loss.
2. Prove full Worker/Runtime/Node process replacement and startup outcome recovery,
   including lost replies and crashes on either side of every durable acknowledgement.
   Current production-loop replay retains live Worker and Runtime owners.
3. Implement safe history reclamation after exact terminal, writer-exclusion,
   namespace-retirement and health obligations are discharged. Floors alone are
   insufficient; deleting records or raising the 32-record limit is not closure.
4. Run sustained offered arrivals beyond that bound with disconnects and capacity
   pressure. Record offered/completed work, queue age, rejection/timeout rates,
   retained bytes and resource slopes. Drained-wave TPS is not sustained throughput.
5. Reconcile the full acceptance matrix and remaining Usage/Cost and inactive
   Worker history obligations against current source. Preserve separate CPU,
   deployment, real GPU certification and Production Launch Receipt evidence.

## Historical schema-89/90 closure priorities

The schema-89 [terminal history reader](terminal-history-evidence-2026-09-05.md)
now covers allocated but undelivered retries and historical Runtime scopes in
local PostgreSQL tests. Its SQL output is candidate evidence; the Go reader now
authenticates the complete signed original before returning typed history.
Disposition signing and local writer exclusion remain open. Control's exact
database startup privilege contract now requires schema 90 and completed legacy
assignment history backfill.
This incremental result does not supersede the schema-88 campaign receipts.

The history work also exposed retained Customer Content in Acquire assignment
wire and its historical replay path. The schema-90
[content lifecycle repair](assignment-content-lifecycle-evidence-2026-09-05.md)
separates immutable signed authority from deletable delivery, preserves terminal
history after deletion, and fences unmigrated legacy records. Its migration and
concurrent lifecycle regressions are tracked independently below.

1. Complete Worker namespace gating and drain all input resolvers/download writers
   before scratch deletion. Runtime RPC ordering alone cannot exclude these users.
2. Establish complete backend process stop proof, typed historical terminal
   disposition and exact local retirement journal recovery. A past STOPPED
   observation or confirmed COMMIT alone does not establish namespace quiescence.
3. Preserve the separate verified schema-87 and schema-88 source checkpoints.
   Both contain native exact-cache and 512-Job CPU load; schema 88 also reruns
   current V2 authority through ordinary synchronous CNPG failover. New fixes
   require their own evidence rather than inheriting these results.
4. Complete the remaining Usage/Cost telemetry and inactive Worker history
   lifecycle. Keep production calibration and remote deployment separate from
   the local CPU evidence.

Historical unconfirmed materialization journals with a persisted request time
but no original command ID cannot safely invent the missing ID. New journals
persist the ID before sending. Upgrade/rollback handling must preserve the old
record and its scratch until its original durable result can be reconciled.

## Initial observations

- The baseline worktree is clean and differs from deployed `f715e40` only in
  three documentation files. Remote cluster state has not been refreshed by
  this campaign.
- Lab bootstrap seeds a cache policy but no Project cache control. Lab assets
  omit the exact-cache reconciler keyring/configuration.
- The smoke request has no explicit seed. H3 derives its default seed from the
  idempotency key, so distinct smoke Jobs do not have equivalent frozen inputs.
- The baseline bootstrap integration test expected schema 67 and was outside
  the CI shard package selection. Its baseline failed against actual schema 69.
- Existing status documentation identifies idle polling record growth and the
  absence of a tested lab quiescence/isolated-restore workflow.

## Verification log

- Local Go: `go1.26.7 darwin/arm64`.
- Docker Desktop was stopped; started for isolated CPU integration tests.
- Baseline `go test ./...`: PASS (existing Go cache reused where valid).
- Baseline bootstrap integration: FAIL, actual schema 69 versus asserted 67.
- Cache control regression: FAIL before fix (no control row), PASS after fix.
- Cache asset/render regressions: FAIL before fix (missing keyring/config), PASS after fix.
- Fixed-seed smoke regression and lab deployment tests: PASS after fix.
- Whole-tree unit tests, `go vet ./...`, golangci-lint v2.13.1 (`0 issues`),
  Linux amd64 cross-compilation, and `make validate-deployment`: PASS after the
  main repairs. The final integration run is tracked separately below.
- `make generate`: PASS, with identical generated-file SHA-256 manifests before
  and after regeneration. At capture time the generated SQL changes were
  uncommitted. After checkpoint `a9a1f7a`, `make verify-generated` also passed
  against HEAD and the worktree remained clean.
- `VELA_REQUIRE_PINNED_FFPROBE=1 go test ./internal/artifactvalidator
  ./internal/h3stagemock -count=1`: PASS on macOS. Linux-only sandbox tests are
  outside that host run.
- Linux sandbox verification now also passed in a separate disposable QEMU
  arm64 guest running Debian Linux 6.1.0-52 with Landlock enabled, UID/GID 10001,
  pinned static ffprobe 8.0.1, and no network. Both packages exited zero with no
  skipped tests; the real video and thumbnail production-sandbox cases passed
  in 0.72 s. The guest and container exited and were removed. Direct Docker
  LinuxKit lacks Landlock and the amd64 translated attempt failed under
  production process limits, so neither earlier attempt is counted as success.
  See [Linux Sandbox Evidence](linux-sandbox-evidence-2026-09-05.json) for the
  tested binary/kernel hashes, retained harness and scope limitations.

## Findings and resolution

| Finding | Effect | Work |
| --- | --- | --- |
| Integration package selection excludes bootstrap command | A stale failing bootstrap test is absent from CI | Discovery now compares Go build-selected tests across all packages; shard behavior tests pass |
| Missing cache control, keyring and explicit smoke seed | Existing lab cannot exercise exact-equivalent request reuse | Bootstrap/assets/render/smoke repaired; two-Job database evidence campaign under verification |
| Expired StageLease lacks ordinary lost-worker convergence | Work and capacity can remain stuck | Migration 70: ASSIGNED/RUNNING loss, materialization expiry, concurrent reconciliation and replay regressions pass |
| Cancellation ignores latest signed renewal | Capacity may be released before renewed authority expires | Migration 70: real START/Heartbeat renewal and delayed release regression passes |
| StageArtifact cleanup absent from retention | Intermediate content/pins/cache may survive deletion and expiry | Migration 71 lifecycle, exact deletion/replay and final local integration tests pass |
| Cached downstream input uses obsolete physical winner field | Cached Encoder/DiT cannot feed actual Worker assignment correctly | Migration 73: cached Encoder -> physical DiT and cached Encoder/DiT -> physical VAE pass actual Acquire/TransferTicket/pull/commit/lineage paths |
| Terminal failure entry points diverge | Missing failure events or early release of a sibling's renewed capacity | Migration 75: all four failure entry points converge through one authority; parallel-root DAG, replay, quotas and latest-renewal capacity regressions pass |
| Worker and materialization paths lock children before Job | Concurrent completion/failure/renewal can deadlock | Migration 75: Start/Heartbeat/Reattach and Commit/SOURCE_LOST use parent-first locks; five two-transaction regressions pass |
| Snapshot and claim lock counter before their parent CapacityPool | Concurrent Admission instantiation and scheduler evidence insertion deadlock | Migration 80 acquires each FK parent lock first; actual CPU load reproduced both 40P01 cycles and deterministic two-transaction regressions passed after repair |
| Ordinary Stage graph Job Expiry is absent | Expired waiting Jobs retain credit and can receive new Stage authority | Migration 79: queued/assigned/running/materializing/finalizing deadlines converge, post-lock writes reject expiry and existing signed capacity remains reserved |
| Retention expires materialization before recovery | ACTIVE-only coordinator scans miss the expired upload and leave MATERIALIZING stuck | Migration 70 also recovers matching RETENTION_EXPIRED evidence; retry-allowed/exhausted real cleanup-order regressions pass |
| Finalization uses transaction-start time and child-first locks | Lock waits can cross claim expiry or invert graph terminalization locks | Parent Job locks precede claim children; post-lock PostgreSQL wall-clock checks reject expired completion without customer output or Charge |
| No tested lab quiescence/isolated restore | Retained state cannot be safely replaced using current teardown script | Migration 72 and dedicated operator pass four PostgreSQL 17 integration tests, including a real independent-cluster restore; retained remote state has not used this operator |
| Idle acquisition also writes scheduler snapshots | At least 3 records per poll, beyond the documented 2 | Migration 78 shares authority readers and probes empty idle queues without writes; 64 polls changed from 192 added rows to 0; replay/role/Down-Up checks passed |
| Usage/Cost Ledger has no runtime usage producer | Passing ledger tests do not establish measured runtime cost accounting | Migrations 76/77 emit allocation occupancy, consumed payload bytes and separate cache source-duration counterfactuals; actual GPU/CPU time and cache carry remain unmeasured |
| Simulator truncates wide arithmetic before unit conversion | Realistic byte/time/rate products overflow, distort costs and can produce negative totals | Algorithm v2 accumulates exact products before conversion, preserves rational transfer sensitivities, and rejects unrepresentable reported totals |
| Simulator preselects pools and miscounts queue/window boundaries | Parallel capacity is serialized, transfers are double-counted as waiting, and partial execution is idle | Dispatch chooses live capacity; transfer and queue samples are disjoint; occupancy is clipped; expiry/completion release queue/Admission credits at defined event boundaries |
| Simulator cache lifetime and fairness omit active/failed work | Live output can be evicted, retries consume fan-out credit twice, and failed/unstarted cohorts disappear | Producer/reuse pins last through Job termination; dependent credit is consumed once; exact storage/buffer/pin conservation, failed service and unfinished waiting are checked |
| Simulator claims production decision replay without required inputs | Advisory proposals can misstate fidelity or use another scenario's receipt | Versioned simplified scheduler is explicit; unsupported dimensions are carried in receipts; missing predictions are unavailable; proposals bind the exact Scenario digest |
| Complete Acquire delivery survives Customer Content deletion | The same command can return prompt and root URLs after deletion completes | Schema 90 separates immutable original authority from deletable delivery; deletion/retention/metadata expiry, late completion, historical backfill and lock-order regressions pass |

## Additional runtime evidence

- CNPG failover discovery reproduced a false success: commit `459a199` removed
  the legacy fault test, while the Make target still selected its name and Go
  returned `PASS [no tests to run]`. The harness now requires that exact name in
  Go's build-selected test list before cluster setup. Empty and near-match names
  fail the guard regression. The replacement uses current Stage acquisition,
  signed Start, cancellation, Charge and exact durable assignment replay.
- The first actual current-Stage CNPG run retained its Job/Stage/credit/Charge
  snapshot and replay through automatic primary replacement in 53.341 s. Its
  no-quorum phase failed: Admission returned SQLSTATE `55000`, while Stage
  acquisition waited until the 4-second client timeout. That cluster result
  does not establish a successful new authority grant without quorum.
- Independently, a single PostgreSQL fixture explicitly enabling the quorum
  GUC allowed all three Stage acquire/scheduler/coordinator entry points before
  migration 81. After repair the five commit guards reject new acquisition and
  signed renewal writes. The seven logical cases pass in 17.526 s, including
  zero residue/Billable Start, existing replay, read-only idle hints and exact
  existing guard-function preservation across repeated Down/Up. A separate
  real-cluster rerun subsequently passed with schema 82 in 111.612 s. Automatic
  primary replacement took 53.354 s and preserved Stage/Job/credit/Charge,
  signed renewal and scheduler snapshots plus exact assignment replay. With
  both standbys stopped, Admission returned HTTP 500 / SQLSTATE `55000` and
  Stage Acquire rejected with exactly `55000`. Recovery to two standbys kept
  the authority digest unchanged. The self-created cluster/containers/private
  kubeconfig were removed; the default kubeconfig hash remained unchanged.
  Full log: `/tmp/vela-cnpg-failover-stage-green-2026-09-05.log`.
- `make test-cnpg-pitr` passed using the pinned CloudNativePG 1.30.0, Barman
  plugin 0.14.0 and PostgreSQL 16.4 on a new four-node kind cluster with local
  MinIO. The base backup completed; target `2026-09-05T05:12:43.741171Z` and
  archived WAL `000000010000000000000008` restored marker counts `before=1`,
  `after=0`, with the declared Secret RBAC checks passing. The self-created
  cluster/containers/private kubeconfig were removed and the default kubeconfig
  SHA-256 remained unchanged. Full logs: `/tmp/vela-cnpg-pitr-2026-09-05.log`.
  This is a real local plugin/marker PITR test, not Stage recovery, independent
  object-store durability, site RPO/RTO or Production Gate evidence.
- Execution terminalization/lock/renewal/materialization batch: PASS, 56.836 s,
  `TestStageGraphFailure|TestStageGraphParentLocks|TestStageLeaseExpiryHonors|TestStageMaterializationExpiry`.
  Independent parallel graph roots demonstrate that logical sibling cancellation
  retains allocation occupancy until the latest signed renewal expires.
- Existing execution, registration, reattachment, cache-to-physical assignment,
  lease expiry and migration regressions: PASS, 47.359 s. All use PostgreSQL 17
  Testcontainers on CPU. These checks do not prove a disconnected physical GPU
  process has stopped; watchdog/cancellation contracts need their own runtime evidence.
- Job Expiry regression first demonstrated an expired queued Job receiving
  ASSIGNED authority. Independent short-lived catalog revisions now exercise
  natural queued, assigned, running, materializing and finalizing deadlines;
  no immutable Job deadline is rewritten. Reconciliation emits one failure
  event, returns reserved credit and counters once, advances the Job fence and
  retains disconnected physical allocations until the latest signed authority
  expires. Repeated migration Down restores actual schema-78 function bodies.
- Retention-before-materialization recovery failed before repair, then passed
  both retry-allowed and exhausted cases in 7.795 s. A naturally expired
  2-second finalization claim also rejects actual Complete after a held parent
  Job releases; a NOWAIT claim lock proves the command waited before locking
  the child. This and finalization deadline convergence passed in 9.165 s.
- The final combined Job/materialization/finalization expiry and retention-order
  regression batch passed in 79.836 s (`/tmp/vela-stage-expiry-final-green.log`).
  Source migrations were then frozen for the independent full integration run.

- The full physical source plus cached target now both complete with independent
  public output copies. `TestLabExactCacheEvidenceRequiresPhysicalSourceAndExactTargetBindings`
  passes using actual immutable Local object-store content. The remote lab script
  has not been executed by this campaign.
- Runtime usage regression first failed for absent allocation receipts, then for
  four missing consumed payload receipts. After repair the full fixture passes,
  with two counterfactual cache records, exact destination attribution and one
  fixed Charge for the cached Job. No GPU or CPU compute receipt is invented.
- The producer-to-valuation extension passed in 4.453 s using physical and cached
  runtime receipts, explicitly synthetic unit rates, exact valuation replay,
  separate direct/counterfactual sums, no unvalued receipts and byte-for-byte
  unchanged customer Charges (`/tmp/vela-runtime-usage-valuation-green.log`).
  Its first run exposed a fixture that used the macOS clock for issued authority
  while PostgreSQL created the Job on the VM clock. The direct assignment helper
  now samples PostgreSQL, matching production acquisition's durable intent clock;
  no production clock-skew allowance or database constraint was relaxed.
- Idle regression measured 64 ordinary polls: baseline 192 history rows added,
  repaired 0 added. Single local elapsed observations were 134 ms and 33 ms;
  these are diagnostic observations, not a calibrated throughput benchmark.
- Independent read review of migration 78 found that a queue row committed after
  the probe's statement snapshot causes at most one advisory NoWork/retry delay;
  it cannot bypass later assignment authority/CAS. An old client without the hint
  keeps the existing durable path on expanded schema. Independent execution
  review also checked authority-reader reuse, ACL restoration and the supported
  mixed-client/schema digest path; the migration replay evidence is listed below.
- See [Runtime Usage Accounting](runtime-usage-accounting.md) for units,
  producer coverage, cutoff semantics and remaining cost-accounting gaps.
- The 11-test idle/replay/role/runtime-usage batch passed in 29.031 s. Separate
  idle-to-work replay and migration rollback tests passed in 5.635 s.
- Independent PostgreSQL 17 recovery passed with a nonempty canceled graph and
  data, catalog, RLS, constraints and effective-grant comparisons. The restored
  Admission gate stays closed and rejects the source operation because the
  target has a different PostgreSQL system identifier. The source snapshot uses
  `pg_export_snapshot()` with `pg_dump --snapshot`, binds the complete public
  table/catalog inventory, dump/operator/roles digests and immutable quiescence
  receipt, and is owner-readable only. Success is published after the isolated
  target container and its anonymous volume are removed.
- Recovery's four-test batch passed in 16.767 s after adding the status-role
  check. It covers inflight Admission serialization, active authority drain,
  idempotent result replay while closed, wrong generation/operation rejection,
  receipt mutation/TRUNCATE denial and migration rollback guards. The restored
  fixture contains one canceled Job and three integration Stages; this is not
  the four-Stage remote lab graph. Scope is `DATABASE_ONLY`, with no object-store,
  WAL/PITR, external replay, RPO/RTO, or Production Gate claim.
- CI discovery now compares actual build-selected Go test files with and without
  `integration` across all packages, then assigns `(package, test)` pairs to
  shards. `go test ./hack` covers multiple integration packages, duplicate test
  names in different packages, invalid shards and failure propagation.
- Scientific checks use only the public simulator/planner APIs and handwritten
  oracles. Before repair: two warm Workers finished in 25 ns instead of 13 ns;
  0.5 GPU-second was reported as idle; 100 ns transfer also became 100 ns queue;
  10 GB / 10 GB/s took 0 ns instead of 1 second; 10 GB stored for 2 seconds cost
  9 instead of 20 micro-units; a 20-second cost overflow produced a negative
  direct total; one-entry cache pressure erased bytes still held by a live Job.
- The 18 oracle tests additionally verify 10,000-completion throughput, fractional
  bandwidth sensitivity, failed-cohort service, queued starvation, same-time
  completion/arrival, expired queue credit, missing prediction status, long-window
  utilization, exact proposal/Scenario binding and numeric bounds. Full simulator
  and CLI packages passed in 2.114 s and 0.999 s; the earlier arithmetic/cache
  revision also passed race checks. Final whole-tree checks remain a separate step.
- `capacity-sim-v2` requires `reserved-service-v1`; it does not replay production
  hierarchical deficits, protected lanes, score/locality policy, correlated node
  failures, object-operation cost, or warm-up cost. Nonzero ignored cost rates
  and non-unit resource multipliers now fail validation. The checked-in synthetic
  input revisions were updated explicitly. See the simulator runbook for exact
  accounting and event-order conventions.
- Runtime cost summary overflow now fails explicitly instead of reporting a
  negative aggregate.
- Mixed acquisition clients initially failed all four migration/rollback cases:
  the optional idle hint changed schema-77 request digests. The client now sends
  the hint only when the schema exposes the internal idle capability. Both old
  and new command initiators on schema 77/78 preserve the exact assignment wire
  through 78 -> 77 -> 78 and cross-client replay. The six idle/upgrade cases
  passed together in 15.468 s (`/tmp/vela-idle-upgrade-green.log`).
- The compact [Scientific Evidence](capacity-sim-scientific-evidence-2026-09-05.json)
  records input/source hashes, revisions, seeds, explicit expected/actual model
  values, the 18-test oracle result and the real example CLI output digest. Full
  local outputs remain under `/tmp/vela-capacity-v2-YRxLTz`.

The first complete four-shard integration run passed shards 2/3 in 304.564 s and
291.108 s. Shard 0 ran 295.804 s and exposed a historical schema-16 retention
fixture that applied the current binary's role contract to an unsupported old
schema. Its historical ACL and current fail-closed/upgrade checks were corrected;
the focused role/contract batch passed in 12.799 s. Shard 1 ran 397.843 s with one
Docker readiness timeout; that exact test passed independently with the snapshot
lock regression batch. The subsequent final schema-87 four-shard run passes;
its durations and evidence are recorded below.

The local [Member Campaign](cpu-member-campaign-evidence-2026-09-05.json) passed
normal execution, follower loss after prepare, and recovery on separate k3d nodes.
The temporary cluster and private credential directory were removed. Its immutable
image digest binds a dirty local build, not a canonical release. This is a
member-transport exercise; the separate concurrent four-Stage runtime campaign
is responsible for Admission, completion, customer Charge and resource growth.

The later sections record the bounded local load and final verification results.
Terminal scratch writer exclusion, complete physical cost telemetry, lifetime
history bounds and release-specific external evidence remain outstanding.

### Terminal stream recovery follow-up

Independent review found two interfaces that prevented database replay from being
reached: the handler rejected already committed or expired materialization
authority, and the Worker omitted COMMIT/SOURCE_LOST command IDs so transport
generated a new random ID after restart. A third mismatch expected `READY` from
Worker FAIL and SOURCE_LOST even though durable outcomes are `RETRY_WAIT` or
`FAILED`. Coordinator-only and permissive fake-control tests did not cover these
cross-module contracts.

The new `TestStageStreamMaterializationJournalReplaysLostResponseAfterTTL`
passed in 12.075 s. It uses a real file journal, StreamAgent, filesystem source,
object publisher, scratch retirer, Handler, ProductionExecutor and role-scoped
PostgreSQL repository. Its transport adapter follows the real client's random-ID
behavior for omitted IDs, then loses one accepted result. After natural authority
expiry, an actual Fleet reconnect and reopening the journal, COMMIT and
SOURCE_LOST return `REPLAYED` with the same command identity and unchanged database
state/receipts/Charges. Inputs survive the lost response; confirmed COMMIT clears
its input/output namespace, while SOURCE_LOST preserves retry inputs. The adapter
does not prove real transport reconnection; that requires the separate transport
regression. Log: `/tmp/vela-stage-stream-materialization-replay-red2.log` (the
filename predates the passing run). The extended race run passed in 15.427 s,
including an explicit check that the sealed output survives the lost COMMIT
response (`/tmp/vela-stage-stream-materialization-replay-race-green.log`). A Go
overlay removing only the two wire command IDs makes both real database recovery
paths fail in 11.926 s, without modifying the shared source
(`/tmp/vela-stage-stream-materialization-identity-mutation-red.log`).

The combined real handler, materialization/FAIL and migration batch passed in
46.971 s. See [Terminal Replay Evidence](materialization-terminal-replay-evidence-2026-09-05.md)
and [Scratch Recovery](runbooks/stage-worker-scratch-retirement.md) for exact
identity, payload, clock and compatibility boundaries. The remaining input
retirement work is specified in [Terminal Scratch Design](terminal-scratch-retirement-design-2026-09-05.md)
and is not implemented by those passing replay tests.

### Control stream generation follow-up

[Control Stream Generation Evidence](control-stream-generation-evidence-2026-09-05.md)
records the independently reproduced queued-Stop, receiver cancellation,
superseded authority and pending Reattach failures. The internal Go control
consumer now uses `NextCommand(ctx)` with generation admission. Old Send cleanup
is bound to the original waiter. Runtime authority adoption and Stop handling
share a mutex, while control ACK waits remain outside it. START, heartbeat and
Reattach can be canceled while their control response is blocked, including a
renewed Reattach with a previous active authority.

The final transport, agent and production-agent race packages passed in 1.687,
2.835 and 3.468 seconds. Full lint reports 0 issues. `make generate` preserved all
23 generated file SHA-256 values. No wire contract or schema change was needed.
These results prove the stated command behavior, not physical STOPPED evidence
or terminal scratch retirement.

### Claim finalization lock-order follow-up

The schema-86 load campaign completed 14 waves / 112 Jobs, then failed on
Artifact COMMIT with PostgreSQL `40P01`. The preserved
[Diagnostic Evidence](cpu-mock-load-diagnostic-2026-09-05.json) contains the
same-source short/cache successes and the complete deadlock excerpt. That
excerpt identifies Artifact COMMIT waiting for a capacity counter and the peer
running claim finalization; it does not identify the peer's waiting tuple.

A separate role-scoped test then held the parent CapacityPool, let a real
CommitClaim block, and failed to obtain the counter with NOWAIT (`55P03`,
4.381 s). This establishes the reversed pool/counter order independently of the
load trace. READY admission holds the parent pool before refreshing its counter;
claim finalization still took the counter before its later parent reference.
Migration 80 had repaired snapshot/new-claim paths but not these terminal paths.

Migration 87 adds the existing parent KEY SHARE lock before the counter in
Commit, Abandon, expired-claim reconciliation and the fairness helper. The four
dynamic paths, natural expiry, resulting claim states and exact replay pass in
15.518 s. Two 86/87 roundtrips preserve function OID, owner, ACL, security mode,
search path and exact rollback definitions in 3.595 s. No runtime privilege was
added. The repeated 512-Job campaign passes against this schema, with zero
Acquire retries.

Logs: `/tmp/vela-scheduler-commit-pool-lock-red.log`,
`/tmp/vela-scheduler-finalization-pool-lock-green.log`, and
`/tmp/vela-stage-claim-finalization-migration87.log`.

### Final CPU load checkpoint

[Final Load Evidence](cpu-mock-load-evidence-2026-09-05.json) binds all 622 Go/SQL,
module and media source files to
`8e1a59e2226b2cbb620336aa999702a477d2adb26d4f04a65bf1e640cb864d70`,
and verifies identical native binary digests across short, exact-cache and
extended runs. Package durations are 14.540, 8.925 and 118.438 seconds.

The extended run completes 512 Jobs in 64 waves of 8 arrivals, with Project running
limit 2, four resident native CPU backends, 2048 direct Stage allocations,
1536 transfers and 512 independent Charges/Visible Completions. The measured
load interval is 108.994097 seconds (4.697502 Jobs/s); this is a CPU mock result,
not a GPU throughput prediction. Posted credit equals the initial
`512 * 1250 = 640000` minor-unit budget; reserved credit ends at zero.

Every completed wave has zero local input/output scratch and Runtime watchdogs,
93 test-process goroutines and 11 file descriptors per native backend. Each raw
wave has 24 terminal execution pins; a real retention batch removes them before
the next wave, retaining all unexpired Artifacts. Final allocations, leases,
buffers, storage reservations and transient pin counts are zero. Each Worker
performs four real capacity reports, and the extended run has zero Acquire
transaction retries.

Durable history and exact objects are deliberately retained. Final PostgreSQL
size is 94,216,719 bytes; test-process heap is 37,315,248 bytes and RSS 314,048,512
bytes. These observations do not prove constant memory or bounded lifetime
database storage. Native backend RSS ranges from 21,856,256 to 22,740,992 bytes.
The test process hosts ModelRuntime gRPC services; four native subprocesses are
their resident CPU backends. Control services are called through Go APIs.
Real control transport, StreamAgent journal recovery, Linux sandbox and local
cluster faults have separate evidence artifacts.

All ordinary unit tests and `make lint test-cross validate-deployment` pass on
schema 87 source. The four integration shards all exit zero; integration package
durations are 339.876, 395.857, 366.349 and 382.674 seconds. The dynamically
discovered bootstrap package also passes on all shards. These compact logs do
not enumerate environment-gated skips; optional external campaigns retain their
separate evidence boundaries.

`make generate` exits zero. All 23 generated Go files match the pinned campaign
source plus the unchanged HEAD OpenAPI output. Regenerating the 622-file campaign
source inventory gives the identical tree digest. All 33 adjacent path/hash
references in the load evidence match their 30 unique local files. The commands,
log hashes, generation baseline and an additional failing correctness probe are
recorded in [Schema-87 Checkpoint](schema87-validation-checkpoint-2026-09-05.json).

### Terminal Runtime reentry counterexample

After the ordinary suite passed, an additive Go overlay exercised the existing
public ModelRuntime gRPC API without changing repository source. It sealed one
execution, then submitted PrepareStage and StartStage with the same still-valid
authority. Both returned ACCEPTED and the new execution returned RUNNING. The
same sequence after CancelStage and an observed STOPPED Status also returned
ACCEPTED/PREPARED and ACCEPTED/RUNNING. Both race-enabled probe cases fail in
0.693 package seconds. This proves Runtime reentry, not observed filesystem
corruption or a production incident.

In schema 87, `SealOutput` clears active authority without retaining an execution
fence. `installOrRenew` also installs authority over a reusable STOPPED/FAILED
execution without excluding the retired attempt. Consequently a previous Seal
or STOPPED observation cannot prove absence of future input/output writers.
The successful scratch finding remains partial. The separate Runtime reentry
finding is repaired below. The 64-wave convergence measurements remain valid for their
tested schedules; they do not establish adversarial scratch retirement safety.
The [Terminal Scratch Design](terminal-scratch-retirement-design-2026-09-05.md)
must cover successful materialization too. No Production Gate advances.

### Schema-88 ordered execution repair

PostgreSQL now assigns an immutable positive sequence after the Worker lock,
StageAuthority V2 signs it, and Control compares it with the exact durable
allocation. Each Runtime keeps one highest installed sequence per epoch and
consumes it before backend Prepare. Terminal cleanup and sealed-receipt eviction
cannot reopen the physical allocation. Persistent epoch advancement rejects old
envelopes after restart. No unbounded per-execution tombstone map is introduced.
See [Ordered physical execution](runtime-execution-order-2026-09-05.md).

The signature regression first rejected unsupported schema V2, then the full
authority race suite passed in 1.681 s. Public-gRPC behavioral regressions failed
before the Runtime fence (0.798 s) and now pass, covering Seal/STOPPED duplicates,
unseen renewals, failed Prepare, new attempts, a busy higher sequence, receipt
eviction and real FileEpochStore restart. The final four-package Runtime/Agent
race batch passes in 8.680 / 1.710 / 3.069 / 3.767 s respectively.

The PostgreSQL baseline exposed missing durable sequence (3.939 s). A separate
mutation of the Go authorizer accepted a correctly signed but database-mismatched
sequence (4.647 s); the real authorizer's exact assignment/START replay regression
passes under race in 6.174 s. The final six-test PostgreSQL sequence batch passes
under race in 27.820 s. It includes a newly reproduced downgrade gap: a real
rolled-back ASSIGN left zero allocation rows yet allowed sequence removal
(4.119 s). Down now also checks the sequence's retained `is_called` state,
under the allocation-table lock, so historical row retention is not its only
memory of issuance.

Historical tests now create allocations at their intended schema before testing
Down/Up. Current assignment production fails closed on unsequenced schemas;
signed V1 fixtures test stored wire recovery only, with no Runtime execution.
Current Control role verification rejects an old schema lacking the sequence
reader. The first full integration run is retained as compatibility diagnostic
history, not a final passing checkpoint.

The Runtime reentry finding is fixed within the declared persistent-epoch and
database-recovery contract. Successful/terminal scratch retirement remains
partial/open: ordered Prepare does not gate Worker input writers or prove a
process group is quiescent. PITR/data-loss recovery must advance and re-register
Runtime epochs before signing resumes; unseen old high-number authority is not
automatically detected by a Runtime's observed watermark.

On the frozen 628-file schema-88 source, `go test ./...`, `make lint test-cross
validate-deployment` and all four integration shards pass. Integration package
times are 388.353 / 424.509 / 347.599 / 406.914 seconds; every shard also passes
the discovered bootstrap package. All 23 generated Go files remain identical
after another `make generate`.

The current-schema local CNPG failover rerun passes in 108.850 s with the existing
pinned PostgreSQL 16.4 / CloudNativePG 1.30.0 harness. Automatic primary replacement
takes 50.158 s and preserves the full Stage allocation rows, signed V2 assignment
and renewal wire, Job/credit/Charge and scheduler evidence. Exact assignment
replay is unchanged. With both standbys stopped, Admission returns HTTP 500 /
SQLSTATE `55000` and Stage Acquire returns `55000`; after replica recovery the
authority digest remains unchanged. The temporary cluster and containers are
removed and the default kubeconfig hash is unchanged. This validates ordinary
synchronous failover, not PITR/data-loss epoch recovery.

The final [Schema-88 Checkpoint](schema88-validation-checkpoint-2026-09-05.json)
binds all checks to source digest
`54a961e7f3d5c8a8b5479ce9fafce013912f183ee7b4231d3f35061399f711a7`.
Its [CPU evidence](cpu-mock-schema88-evidence-2026-09-05.json) contains the full
628-file source map and both complete receipts. The 512-Job/64-wave load passes
in 121.436 package seconds, with a measured workload interval of 110.824 s and
4.620 Jobs/s. It executes 2,048 physical Stages and 1,536 transfers with zero
Acquire retries. All waves finish with zero scratch/watchdog state; real
maintenance clears their terminal execution pins. Final active authority,
capacity/storage/credit reservations are zero; 512 Charges total 640,000 CNY
minor units. These are instrumented local observations, not a controlled
before/after performance comparison with schema 87.

The separate exact-cache run passes in 9.776 package seconds: source 4 physical
Stages, target 2, five transfers, two independent Charges and four readable
Job-owned public copies. Both admitted cache entries and their carrying
references remain live. The source map still matches after both campaigns.

## Post-checkpoint process teardown

After local commit `a9a1f7a`, public CPU driver regressions reproduced inherited
pipe/child-writer leaks, blocked request cancellation, and lost initialization
cleanup errors. The scoped [ProcessBackend repair](process-backend-teardown-evidence-2026-09-05.md)
passes same-source unit, lint, Linux cross-build, macOS race and actual Linux
CPU tests. It preserves resident models and reports escaped-child/blocked-writer
limits without granting scratch deletion permission. Earlier full integration,
CNPG and 512-Job measurements remain bound to their original checkpoints.

Read review of terminal retirement also identified unseen later allocations:
an old stopped allocation is insufficient to close its StageRun namespace.
The design now requires complete scoped allocation history and a retirement
cutoff installed at both Worker input admission and every relevant Runtime,
followed by independent writer drain. That protocol is not implemented yet.

## Post-checkpoint input and response cancellation

Local commit `3ee8008` fixes Stop arriving during input resolution, including a
late successful resolver return and an exact HTTP body completed after context
cancellation. Commit `80239e5` prevents matching Stop from being overwritten by
late START, HEARTBEAT or REATTACH acceptance, including cancellation RPC failure.
Both changes pass unit, related-module race and lint checks; see the separate
[input cancellation](input-stop-evidence-2026-09-05.md) and
[response ordering](late-control-stop-evidence-2026-09-05.md) receipts. Neither
process-local guard establishes persistent namespace exclusion or writer drain.

The terminal reader design now specifies complete physical-attempt/ASSIGN/budget
checks before its scoped cutoff calculation, retained non-content identity roots,
and a union of historical member Runtime scopes. Runtime enforcement requires a
domain-separated signed disposition and separate FLOOR_INSTALLED/DRAINED states.
At that checkpoint, original authority lookup, the role-scoped reader,
persistent Worker/Runtime barriers and fault/restart campaigns remained open.
Schema 89 subsequently implemented the reader; schema 90 separates delivery
content from its retained authority. The remaining retirement protocol is still
open. This design review does not advance an acceptance gate.

## Schema 90 assignment content lifecycle

The [schema-90 repair](assignment-content-lifecycle-evidence-2026-09-05.md)
closes a second Customer Content copy in durable Acquire results. Active delivery
replay remains exact. Deleted/expired delivery returns deterministic rejection,
while the original signed authority remains independently verifiable. A dedicated
public-key migration tool handles existing data; pending legacy rows block
startup and deletion completion, and explicit unverifiable retirement records
only a digest and reason. Up/Down use NOWAIT before any table changes, and Down
refuses to erase retained authority or retired-delivery evidence.

The public deletion regression first failed with a returned assignment after
COMPLETED. Independent review also found a root/Job lock cycle during backfill;
its two-transaction test failed with `55P03` before the repair and passed in
4.853 s afterward. Both completion and backfill now take the retained root lock
before the live Job. Late initial completion, deadline crossing during lock
wait, and metadata expiry cannot restore delivery content.

Local validation, using disposable PostgreSQL 17 instances and no GPU:

- `go test -tags=integration ./internal/integration -run
  '^(TestDatabasePoolsFailClosedOnRoleConfusion|TestStageAssignmentContent|TestStageTerminalHistory|TestPostgresTerminalHistory|TestStageFailureReplayMigration)'
  -count=1 -timeout=10m`: PASS, 64.298 s.
- Assignment-content integration with `-race`: PASS, 48.606 s. The separate
  `TestPostgresTerminalHistoryRequiresExactSignedOriginal` race run passed in
  6.600 s, including a substituted signed SQL candidate rejected by the Go reader.
- An earlier assignment/retention/migration compatibility batch passed in
  103.276 s and lab-bootstrap integration in 11.799 s. These preceded the
  additional root-order repair; the later lifecycle batch rechecks that boundary.
- `go test ./...`, `make lint` (0 issues), and `make generate`: PASS. Generation
  adds the schema-90 SQL types and acquire-result lifecycle columns only.

No remote rollout, GPU execution, new load/CNPG campaign, or Production Gate
advancement is claimed. At that checkpoint, signed terminal disposition,
persistent input admission, writer drain and the retirement journal remained open.

## Signed terminal disposition

The next [protocol increment](terminal-disposition-evidence-2026-09-05.md), based
on `474cde8`, adds a typed historical query to authenticated Stage Worker Control
and a domain-separated Ed25519 response. The unchanged schema-90 reader supplies
the complete scoped allocation cutoff and historical Runtime membership.
Client verification binds that response to the original authority and current
query session. RETAIN contains no signed facts; expired execution authority can
authenticate history but cannot regain execution time.

Local mTLS/role-scoped PostgreSQL integration passes. The focused race batch
passes in 16.152 s, including allocated but undelivered retry and multiple
historical Runtime epochs before/after Fleet Drain. Signature/domain/shape and
transport unit tests pass with the race detector. `go test ./...`, `make lint`
(0 issues), and `make generate` pass. Compatibility results and the earlier
fixture failure are recorded in the linked evidence report.

Persistent Worker input admission, Supervisor/Runtime floor enforcement,
execution-specific writer drain and the crash-safe retirement journal remain
unimplemented. The new response is not a cleanup action and does not establish
terminal scratch bounds. Production Gates remain **0/9**.
