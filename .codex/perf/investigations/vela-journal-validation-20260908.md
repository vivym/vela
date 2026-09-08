# Resumed bounded journal performance investigation

User quote: 请彻底完善并验证 vela，可以先 mock 验证，暂时不用 gpu 验证。要全面的验证 vela 的正确性、科学性。如果发现更优的架构，也要去优化架构。

Baseline source: 581d832, clean working tree at start. Historical evidence: docs/journal-transition-evidence-2026-09-08.json and docs/journal-proof-reuse-evidence-2026-09-08.json. These contain single ordered runs, not an established repeated median baseline. Profile run is diagnostic, not an unprofiled performance sample.

The 32-record retention limit is already explicit; safe reclamation and sustained-arrival/constraint phases remain open and are not inferred from this narrow investigation. No binary search or resource-limit campaign is claimed here.
## Hypotheses - 2026-09-08

**User Quote:** "请彻底完善并验证 vela，可以先 mock 验证，暂时不用 gpu 验证。要全面的验证 vela 的正确性、科学性。如果发现更优的架构，也要去优化架构。"

**Summary**
- H1: Repeated JSON/protobuf canonicalization and history traversal dominate the added CPU cost. [unmeasured] (evidence: execution_state_file.go validateProofs/persist and execution_drain.go retainedAuthority)
- H2: File identity/content checks and durability syscalls dominate wall time. [unmeasured] (evidence: executionStateFile.transition/check/persist)
- H3: Race instrumentation amplifies host validation cost. [unmeasured] (evidence: ModelRuntime Service and journal owner run in integration.test; native subprocesses are drivers only)

**Evidence**
- Git history: 581d832 per-validation authority reuse | 5794259 complete pre-publication validation

## Code Paths - 2026-09-08

**User Quote:** "请彻底完善并验证 vela，可以先 mock 验证，暂时不用 gpu 验证。要全面的验证 vela 的正确性、科学性。如果发现更优的架构，也要去优化架构。"

**Summary**
- Keywords: journal, publication, validation, production loop
- internal/integration/cpu_mock_load_campaign_test.go [newCPULoadWorker, modelruntime.NewService, NewSupervisorWithExecutionFloor]
- internal/modelruntime/execution_journal_transition.go [transition]
- internal/modelruntime/execution_state_file.go [validateProofs, check, persist]
- internal/modelruntime/execution_drain.go [retainedAuthority]

**Evidence**
- Repo map: direct rg/source inspection
- Paths count: 4
## Profiling - 2026-09-08

**User Quote:** "请彻底完善并验证 vela，可以先 mock 验证，暂时不用 gpu 验证。要全面的验证 vela 的正确性、科学性。如果发现更优的架构，也要去优化架构。"

**Summary**
- Tool: Go pprof CPU profile with race instrumentation
- Command: `VELA_RUN_CPU_MOCK_CAMPAIGN=1 VELA_CPU_MOCK_WAVES=4 VELA_CPU_MOCK_WIDTH=8 VELA_CPU_MOCK_TIMEOUT=5m go test -race -tags=integration ./internal/integration -run ^TestCPUMockProductionLoopJobCampaign$ -count=1 -timeout=8m -cpuprofile=/tmp/vela-startup-validation.syiSMk/journal-profile.cpu -o /tmp/vela-startup-validation.syiSMk/journal-profile.test -v`

**Evidence**
- Artifacts: docs/evidence/journal-profile-2026-09-08/journal-profile.cpu, docs/evidence/journal-profile-2026-09-08/journal-profile-flat.txt, docs/evidence/journal-profile-2026-09-08/journal-profile-cumulative.txt, docs/evidence/journal-profile-2026-09-08/journal-profile-lines.txt
- Hotspots: 147.36 CPU sample seconds over 108.17 wall seconds; percentages are not wall-time partitions, recordCandidates retainedExecutionIndex: 7.43 cumulative sample seconds, complete candidate verification: 9.11 cumulative sample seconds, observeCPULoad: 17.28 cumulative sample seconds; observer perturbs load, runtime._ExternalCode: 45.38 flat seconds; not attributed wholesale to application or race
## Decision after profiling

One production change to test: search validated retained history newest-first while still verifying every examined envelope and the selected exact identity. Full pre-publication history validation stays unchanged. Older-history lookup remains linear. Extend the unprofiled 581d832 baseline to three serial runs, using the already captured same-source run plus two new runs; aggregate median. No performance conclusion from the diagnostic profile alone.
## Repeated baseline

User quote: 请彻底完善并验证 vela，可以先 mock 验证，暂时不用 gpu 验证。要全面的验证 vela 的正确性、科学性。如果发现更优的架构，也要去优化架构。

Three successful sequential 32-Job runs of source af09f3ee607cbfce15d960e3fa4e5bec15cd8ab6094fdcc998454dc3325373c9, median aggregation. Runs: 97.072320, 96.634700, 100.898655 wave seconds. Median 97.07232 seconds. Same binary hashes and workload/toolchain checked by hack/summarize-cpu-mock-perf.py; duplicate/mixed inputs are rejected. Prior profile run excluded. Raw receipts and per-log hashes: .codex/perf/baselines/journal-lookup-baseline.json.

Decision: proceed with newest-first retained-execution lookup only. No other performance changes; require oldest/middle/latest and same-sequence conflict regression plus three unprofiled serial campaigns.
