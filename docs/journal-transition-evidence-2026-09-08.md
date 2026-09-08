# Durable journal transitions validate before publication

Date: 2026-09-08. Base: `11ce026`. PostgreSQL 94, Worker journal 5, Runtime
journal 8. Production Gates remain **0/9**.

The existing file owner now independently validates six typed mutations before
publishing execution journal state: admission, signed floor installation,
accepted/confirmed renewal candidates, sealed receipts, writer drain and
explicit backend health. These operations construct candidate data without
filesystem, backend or admission handles. The production file owner remains
responsible for locking, full candidate validation and atomic publication.

This is a prerequisite for the [Node custody candidate](node-journal-custody-design-2026-09-08.md).
It does not implement a Node service, move files, authorize a process or expose
an arbitrary snapshot replacement API. The existing Service still owns current
runtime routing, execution admission and backend interaction.

## Validation boundary

Admission and floor mutations independently verify signatures, temporal bounds
and trusted member scope before changing restrictions. Admission preserves the
Service's configured clock-skew allowance, the monotonic watermark, installed
floor, health restriction and 32-record history bound. A future IPC adapter
must resolve clock policy from its approved route; it cannot trust client policy.

Renewal/seal/drain/health mutations independently verify retained signed authority
and recompute its digest. A caller-supplied `stageauthority.Verified` wrapper is
not trusted verification evidence. Historical metadata uses signature validation;
recording it after expiry does not renew execution permission. A matching drain
digest establishes identity consistency, not independent proof that a writer
actually stopped. Process/backend evidence and authenticated roles remain
separate obligations for the future owner.

Every file publication now checks current schema, journal UUID, scope and
original root/lock identities, followed by the complete journal semantic
validator and encoded-size bound, before opening a temporary file. This includes
initialization, explicit schema upgrades, non-admission and backend-lifecycle
checkpoints that do not use the six draft operations. The existing file fsync,
rename, directory fsync and post-publication identity checks remain in place.
There is one complete semantic validation per publication, not two.

Draft mutations copy changed slices and replace nested records. They do not
modify shared historical records in place. A rejected candidate or an aborted
transition therefore leaves the live journal unchanged. This is an internal
typed-operation discipline, not a sandbox for arbitrary mutation callbacks.
No caller can use the private file persistence function over IPC.

The new temporal recheck introduces an explicit boundary condition: an envelope
can expire after upper-layer validation but before the owner attempts its write.
Admission and floor installation return STALE in this case without poisoning
the owner as a storage failure. Actual publication uncertainty remains restrictive.

## Focused regression evidence

Four new test groups contain 22 subcases, using signed external-package fixtures
and test-only access below the Service validation layer:

- Admission and floor reject invalid signatures, correctly signed foreign Worker
  scope, expiry and nil inputs; a subsequent legal mutation succeeds.
- Candidates, seal, health and drain reject tampered signed authorities even when
  the caller supplies a `Verified` value. Confirmed candidates are independently
  checked; nil health is rejected before dereference.
- A forged wrapper digest cannot validate a forged drain result. Valid signed
  authority determines identity even when the wrapper digest is wrong.
- Malformed full history, schema, UUID, scope and storage identities are rejected
  at publication. A candidate confirmation followed by full-validation failure
  or explicit abort cannot leak through a shared slice into the live state.
- Rejection checks compare the exact disk bytes, original inode and in-memory
  document; require zero directory-sync calls and no leftover temporary files.
- Exact candidate, seal and health replay adds no sync. Valid historical metadata
  remains recordable after expiry; it grants no backend entry.
- Real Supervisor admission/floor calls exercise expiry specifically at the owner
  recheck, followed by successful legal admission/floor. The expired attempt
  writes no document and never reaches the backend.

Existing full ModelRuntime race coverage also exercises crash, recovery, schema
upgrade, lifecycle, renewal, floor, drain, seal and health behavior. New tests do
not substitute a mocked persistence implementation for the file owner.

## Final-source verification

Source digest:
`262c188bc15b80825f7860808dadf9b0b73c9e761ae2f1c5528b8902a9a08e06`.
Full receipts, log hashes and result lines are retained in
[the evidence JSON](journal-transition-evidence-2026-09-08.json).

| Check | Result |
| --- | --- |
| Focused transition regressions | PASS, 1.551 package seconds |
| Full ModelRuntime race, uncached | PASS, 101.775 package seconds |
| Ordinary whole-repository tests | PASS; unchanged packages may use Go cache |
| Whole-repository `go vet` | PASS |
| Ordinary golangci-lint 2.13.1 | PASS, 0 issues |
| Linux/amd64 static cross compilation | PASS; binaries are compiled, not executed |
| Native production-loop CPU campaign under race | PASS, 32 Jobs / 128 physical Stages |
| Durable-stream and direct exact-cache race campaigns | PASS, 21.388 package seconds combined |

The production-loop regression completes four waves of eight concurrent arrivals,
with 32 visible completions and exactly 32 customer Charges totaling 40,000 minor
CNY units. It records 128 accepted heartbeats, four lost-response commit replays
and final Control session epoch 3 on all four Workers. Nine encoder STALE Acquire
responses recover through the production loop. Native model PIDs remain unchanged.

All 128 PostgreSQL allocations match their Worker/Runtime journals. Final active
allocations, leases, payload scratch, runtime watchdogs and execution pins are
zero. Retained local journals total 1,299,282 bytes, with 32 execution records per
Runtime. The exact-cache compatibility runs independently preserve source/target,
miss/admit/hit/reuse and billing assertions through both existing harness modes;
they are not production-loop exact-cache evidence.

No full integration suite, new deployment render, Registry-bound native startup,
GPU or Production Gate validation is claimed. Ordinary lint's zero findings do
not supersede the separately recorded integration-tag lint baseline.

Reproduce the changed paths with Go 1.26.7, ffprobe 8.0.1 and Docker:

```sh
go test -race ./internal/modelruntime -count=1
VELA_RUN_CPU_MOCK_CAMPAIGN=1 \
VELA_CPU_MOCK_WAVES=4 VELA_CPU_MOCK_WIDTH=8 VELA_CPU_MOCK_TIMEOUT=5m \
go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMockProductionLoopJobCampaign$' -count=1 -timeout=8m -v
VELA_RUN_CPU_MOCK_CAMPAIGN=1 go test -race -tags=integration ./internal/integration \
  -run '^TestCPUMock(DurableStreamExactCache|ExactCacheSourceTarget)Campaign$' \
  -count=1 -timeout=5m -v
```

## Cost and remaining architecture work

Full candidate verification walks retained history and repeats cryptographic
checks. Its cost grows with that history, currently bounded at 32 records. The
first final-source production-loop test took 125.32 seconds (127.477 package
seconds; 114.645783 seconds in measured waves), while other validation processes
ran concurrently. It is a correctness receipt, not a controlled comparison with
the preceding 72.25-second test or a sustained-throughput claim.

A subsequent serial pair used an archive of `11ce026` followed by the unchanged
final source, with identical 32-Job/race settings and no other agent-launched
tests running during either campaign:

| Serial campaign | Baseline `11ce026` | Full validation |
| --- | ---: | ---: |
| Test seconds | 75.00 | 110.33 |
| Measured wave seconds | 65.189508 | 101.803868 |
| Mean Job latency seconds | 9.561940 | 14.809083 |
| Wave Jobs/second | 0.490877 | 0.314330 |
| Final retained journal bytes | 1,299,155 | 1,299,338 |

Both campaigns preserve all correctness assertions and finish 32 Jobs. Measured
wave elapsed time rises 56.2% in this pair. This is one ordered pair on shared
host resources, not a randomized/repeated benchmark, statistical estimate or
production throughput measurement. Nevertheless, it shows a material cost that
must be addressed before accepting the custody path's performance. Full history
checking remains required; repeated validation of identical authority bytes
within one candidate is a specific optimization opportunity. Any reuse must
preserve cryptographic, canonical, scope and complete-history checks and must
never cache execution freshness or carry untrusted verification across owners.

The next custody increment must bind typed requests and exact retries to the
approved original caller, current route/activation and Node-owned storage. It
must preserve distinctions between historical metadata, actual backend evidence
and permission to execute. Existing upstream APIs handle some idempotent drain
and floor replay; exposing private mutations alone would not reproduce those
lost-response semantics.

Startup/lifecycle and non-admission operations still need their explicit typed
owner contract, together with Worker journal custody and Registry/Fleet ordering.
Full process replacement, bounded history reclamation and sustained arrival
tests remain open. A signed floor alone does not discharge drain, receipt replay,
terminal recovery or per-allocation health obligations, so this increment neither
deletes records nor increases the retention limit.
