# Profile-guided retained execution lookup

Date: 2026-09-08. Production baseline: `581d832`; documentation and measurement
checkpoints precede the lookup change. PostgreSQL 94, Worker journal 5, Runtime
journal 8. Production Gates remain **0/9**.

This investigation follows the remaining cost recorded in the
[proof reuse report](journal-proof-reuse-evidence-2026-09-08.md). The production
change searches retained executions from newest to oldest. It authenticates each
examined envelope and still requires the selected signed sequence plus exact
authority or valid renewal identity. Full recovery and pre-publication journal
validation remain unchanged.

## Measured execution path

The CPU campaign creates `modelruntime.Service` and `Supervisor` in the Go test
host. The four native subprocesses are mock drivers. Host pprof therefore covers
the real journal owner and the Worker/Control components, while native driver
execution remains outside its samples and the host race detector.

The diagnostic run uses unchanged Go/SQL/module/media source digest
`af09f3ee607cbfce15d960e3fa4e5bec15cd8ab6094fdcc998454dc3325373c9`.
It passes all 32-Job / 128-Stage campaign assertions. The CPU profile covers
108.17 wall seconds and 147.36 CPU sample seconds across host threads.

| Profile observation | Cumulative sample seconds | Interpretation |
| --- | ---: | --- |
| `recordCandidates` at `retainedExecutionIndex` | 7.43 | Repeated signed history lookup on candidate updates |
| All `retainedExecutionIndex` callers | 8.99 | Current and historical target lookup |
| Complete candidate validation in `persist` | 9.11 | Mandatory whole-history publication checks |
| `observeCPULoad` | 17.28 | Campaign observation itself consumes material CPU |
| `json.Marshal` in `persist` | 0.45 | Journal JSON encoding is not the leading sampled component |

Cumulative rows overlap and cannot be added. CPU samples are not a wall-time
partition and do not measure time blocked in fsync or other waits. The flat
profile also contains 45.38 seconds of `runtime._ExternalCode`; those unattributed
samples are not assigned wholesale to application work or race instrumentation.
Known race-call symbols and observer work further limit extrapolation to normal
production execution.

The profile, [flat summary](evidence/journal-profile-2026-09-08/journal-profile-flat.txt),
[cumulative summary](evidence/journal-profile-2026-09-08/journal-profile-cumulative.txt)
and [line attribution](evidence/journal-profile-2026-09-08/journal-profile-lines.txt)
are committed under `docs/evidence/journal-profile-2026-09-08/`. The raw
[CPU profile](evidence/journal-profile-2026-09-08/journal-profile.cpu) has SHA-256
`3855fd19f28e15642e2472ac81f20f65cfda76aa68b57180eac6642ba6954776`.
The matching 117 MiB test binary remains a local profiling artifact at
`/tmp/vela-startup-validation.syiSMk/journal-profile.test`; it is not a release binary.

## Change and correctness boundary

Retained history is already validated as strictly increasing at recovery and
publication. Normal Status/candidate updates target the most recent execution.
The previous lookup started at the oldest record and authenticated each earlier
record before reaching that target. At history length H, locating the newest
record now examines one envelope instead of H. The subsequent full candidate
validation still traverses the complete history before any publication.

This is a traversal-order change, with no persistent index, proof cache lifetime
change, schema change, snapshot trust or new authority. A matching sequence alone
cannot select another allocation. Signed old records and their exact renewals
remain reachable. The tradeoff is that oldest-record repair now scans more
records; it remains linear within the unchanged 32-record bound. No cold-recovery
latency or service-budget guarantee is inferred from normal-path measurements.

New regressions independently select oldest, middle and latest records across
two resident profiles. Each rejects a correctly signed, same-sequence allocation
collision without changing disk bytes, inode or live state; then confirms the
original and accepts its exact renewal. Inspection verifies that only the selected
record changes and all unrelated accepted/confirmed history remains intact.

Focused lookup/transition regressions pass in 2.449 package seconds. Full uncached
ModelRuntime race passes in 94.835 seconds, including existing crash, renewal,
history-bound, drain, floor, health and recovery coverage. Whole-repository
ordinary tests, vet, ordinary golangci-lint 2.13.1 (0 issues) and static
Linux/amd64 cross compilation pass. Cross compilation does not execute Linux
tests; unchanged ordinary packages may use Go's cache.

The final-source durable-stream and direct exact-cache compatibility campaigns
also pass under race detection (25.325 package seconds combined). Their exact
source/target reuse and billing assertions remain separate from production-loop
cache scheduling. Validation log hashes and cache receipts are in
[the evidence JSON](journal-lookup-evidence-2026-09-08.json).

## Repeated comparison protocol

The baseline contains three successful, unprofiled, serial 32-Job runs of the
same source and binary hashes: wave times 97.072320, 96.634700 and 100.898655
seconds, median 97.072320 seconds. The earlier existing same-source run is
retained as the first sample; the other two were collected before the source
change. The diagnostic profile is excluded.

The final comparison runs the same four waves of eight arrivals, project running
limit two, mock processes, mTLS stream, journals, faults, race setting and
observer policy three times serially, after other agent-launched tests finish.
This is a before/after sequence on shared host resources, not randomized order,
exclusive CPU reservation or a sustained-arrival campaign.

`hack/summarize-cpu-mock-perf.py` checks successful completion, exact source,
workload shape, runtime binary hashes, toolchain and numeric metric consistency;
it rejects duplicate or mixed-source inputs. It emits `PERF_METRICS` JSON markers
and preserves all per-run receipts and log hashes. Aggregates are medians of run
metrics, not pooled Job-latency percentiles. Baselines and the investigation log
live under `.codex/perf/`.

Final source digest:
`f594562668dcf293b5977182f8db61ad0ccaea6f3090f4179bed6a113a7cb992`.
The final race runs take 83.085214, 82.215638 and 80.513639 measured wave seconds,
median 82.215638 seconds, 15.3% below the same-source predecessor's median.
Median per-run mean Job latency falls from 14.211927 to 12.024883 seconds. Each
run independently passes 32 completions/Charges, 128 physical Stages, four lost
commit replays, durable session advance, unchanged native driver PIDs and exact
PostgreSQL/Worker/Runtime allocation matching. Final active allocations, leases,
payload scratch, watchdogs and execution pins are zero; each Runtime retains 32
records. Receipt groups are in
[baseline](../.codex/perf/baselines/journal-lookup-baseline.json) and
[final race](../.codex/perf/baselines/journal-lookup-final-race.json) JSON.

## Uninstrumented comparison and a retained failure

Three runs per version were also planned without `-race`, interleaving the old
publication path (`11ce026`), complete validation with forward lookup (`581d832`)
and the reverse-lookup experiment. These are still CPU mocks, not GPU or deployed
production measurements. The original path's third run failed, interrupting the
sequence; the two remaining current-version runs were then executed separately.
The failed run was not overwritten, rerun into a passing sample, or included in
successful-performance aggregates.

| No-race variant | Successful wave seconds | Median | Outcome |
| --- | --- | ---: | --- |
| Old publication, `11ce026` | 35.398082, 36.191482 | Not a complete three-run baseline | Third run failed |
| Complete validation, forward lookup | 37.362711, 37.860797, 38.008012 | 37.860797 | 3/3 PASS |
| Complete validation, reverse lookup | 35.945555, 36.252448, 37.494259 | 36.252448 | 3/3 PASS |

The reverse lookup's no-race median is 4.2% lower than forward lookup. With only
three ordered samples per variant and shared host resources, this is descriptive
evidence, not a statistical guarantee. The effect is much smaller than in race
mode. The previous approximately 49% race-mode gap to the old publication path
must not be described as a normal production penalty.

Full receipts for the completed current-source groups are in
[forward no-race](../.codex/perf/baselines/journal-lookup-forward-norace.json) and
[reverse no-race](../.codex/perf/baselines/journal-lookup-reverse-norace.json) JSON.
The [command manifest](evidence/journal-profile-2026-09-08/journal-lookup-norace-commands.json)
and [follow-up manifest](evidence/journal-profile-2026-09-08/journal-lookup-norace-followup-commands.json)
record actual order, arguments, checkout paths and exit codes. Recorded source,
Go/ffprobe versions and native driver hashes are checked within each aggregate;
instrumentation policy comes from these command records, not from a guessed log label.

The preserved [failed log](evidence/journal-profile-2026-09-08/journal-lookup-norace-old-3.log)
reports `execute StageAssignment: StageAuthority is stale` after the four injected
lost-response retries. It fails before a complete wave receipt, in 14.65 test
seconds. The log does not identify the exact authority timestamp or Worker
admission boundary, so it cannot establish expiry versus a future-issued envelope
or prove that the lookup change fixes it.

Source inspection confirms a separate configuration discrepancy: the CPU campaign
creates durable Worker admission and ModelRuntime Service with zero default clock
skew, while the production entry points use
`authoritypolicy.ProductionMaxClockSkew` (30 seconds). The campaign Control backend
uses one second. The policy discrepancy is verified; its causal relationship to
this specific failed run remains unproven. Next validation must exercise a
deterministic producer/consumer clock offset and preserve rejection outside the
configured bound and after expiry. Do not handle this by ignoring STALE execution
errors or silently releasing an uncertain allocation.

The lookup change is retained because it removes unnecessary current-record work,
passes historical-identity/renewal checks and both three-run current-version
comparisons. Further micro-optimization is deferred in favor of resolving the
campaign policy/diagnostic gap and the remaining custody/recovery work.

Protected Node journal custody, Registry/Fleet startup ordering, full process
replacement, safe reclamation beyond 32 records, resource-constraint testing and
sustained arrivals remain open. Performance measurements here neither authorize
backend execution nor supply Production Gate or GPU evidence.
