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
