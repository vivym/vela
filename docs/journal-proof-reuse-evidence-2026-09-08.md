# Reuse identical authority proofs within one journal validation

Date: 2026-09-08. Base: `5794259`. PostgreSQL 94, Worker journal 5, Runtime
journal 8. Production Gates remain **0/9**.

The [preceding correctness increment](journal-transition-evidence-2026-09-08.md)
added complete validation before every journal publication. A serial 32-Job
comparison exposed a material cost: measured wave time increased from 65.189508
to 101.803868 seconds. Those results were committed before this optimization.

Many fields in one candidate contain the same signed authority: original,
accepted, confirmed, seal and drain, as well as repeated history lookups. Full
validation now checks each distinct retained-authority byte string once per
validation call and reuses that result for identical bytes within the same call.
Every structural relation, bound, witness, receipt, drain, health and lifecycle
check still runs. The highest-watermark and terminal-disposition checks also
remain unchanged.

## Scope of reuse

`validateProofs` makes a private journal copy with a new local map. Only successful
signature, canonical-V2, digest and trusted-member-scope verification populates
it. Exact encoded bytes are the key; allocation ID, sequence and caller-supplied
digest cannot select a cached result. Validators treat cached protobufs as
read-only; renewal/identity comparison operates on canonical copies.

The file owner and mutation drafts retain no proof map. Every new publication,
recovery or snapshot verification starts a new map under its own configured
verifier and scope. No cache survives the call, and nothing caches execution
freshness, permissions, backend health observations or writer exclusion.
Admission's temporal checks and the full publication checks remain mandatory.

Two additional regressions verify the boundary:

- A corrupted confirmed envelope in the same candidate cannot inherit the proof
  for the valid original/accepted bytes naming the same execution. Refusal leaves
  disk bytes, inode and live state unchanged, performs no sync and permits a
  subsequent legal candidate.
- After successful validation with two signing keys, the owner is revalidated
  with only the highest witness's key. The highest witness remains valid, but the
  older retained record must fail because its key was removed. Restoring the
  original verifier permits validation again. Previous successful history cannot
  bypass the new verifier through cached proofs.

This optimization has no schema, wire contract, IPC, mount or production startup
change. It reduces repeated work inside full validation; it does not implement
Node custody, history reclamation or an execution permission issuer.

## Final-source verification and measurement

Source digest:
`af09f3ee607cbfce15d960e3fa4e5bec15cd8ab6094fdcc998454dc3325373c9`.
The [evidence JSON](journal-proof-reuse-evidence-2026-09-08.json) preserves final
receipts and log hashes. The predecessor's full serial receipts remain in its
[evidence JSON](journal-transition-evidence-2026-09-08.json).

Focused transition tests pass in 1.890 package seconds. Full uncached ModelRuntime
race passes in 100.158 package seconds. Whole-repository ordinary tests and vet,
ordinary golangci-lint 2.13.1 (0 issues), and static Linux/amd64 cross compilation
pass. Cross compilation does not execute Linux binaries; unchanged ordinary test
packages may use Go's cache.

The final-source durable-stream and direct exact-cache compatibility campaigns
also pass under race detection (24.790 package seconds combined), preserving
their source/target cache reuse, exact object version and billing assertions.
These remain separate harness modes, not production-loop cache validation.

The final production-loop campaign ran alone after those tests completed, with
the same four waves of eight arrivals and race settings as the predecessor's
serial pair. It completes 32 Jobs / 128 physical Stages, exactly 32 customer
Charges, four lost-response replays and 128 accepted heartbeats. All Workers
finish at Control session epoch 3; eleven encoder STALE Acquire responses recover
through `ProductionAgent.Run`. The four resident native model PIDs are unchanged.
PostgreSQL and both journals match all allocations, with zero final active
allocations, leases, payload scratch, watchdogs and execution pins. Local journal
bytes are 1,299,117; each Runtime still retains 32 execution records.

| One serial run per source | Before publication validation | Full validation | Full validation with local proof reuse |
| --- | ---: | ---: | ---: |
| Source | `11ce026` | `5794259` | Digest above |
| Test seconds | 75.00 | 110.33 | 107.51 |
| Measured wave seconds | 65.189508 | 101.803868 | 97.072320 |
| Mean Job latency seconds | 9.561940 | 14.809083 | 14.211927 |
| Wave Jobs/second | 0.490877 | 0.314330 | 0.329651 |

The optimized run's measured wave time is 4.6% lower than full validation without
reuse, but remains 48.9% above the earlier publication path. These are individual
ordered runs on a shared host, not repeated randomized samples or a statistically
established speedup. Registration/STALE polling counts and physical resource
scheduling vary between runs. Native subprocesses are not instrumented by the
host race detector. This evidence does not establish sustained throughput,
production tail latency, or that the added cost meets a service budget.

The source eliminates duplicate retained-authority verification within each full
check, while full historical traversal and other validation/filesystem costs
remain. The modest observed end-to-end change does not identify the dominant
remaining cost. Further performance work requires profiling those components
under the same operation mix; it must preserve complete validation and must not
turn a previous accepted snapshot into indefinite authorization.

Reproduction uses the same commands and prerequisites in the
[transition report](journal-transition-evidence-2026-09-08.md#final-source-verification),
running the 32-Job campaign without concurrent validation for comparison.
Protected Node custody, Registry/Fleet activation, full process replacement,
history reclamation beyond 32 records and sustained arrivals remain open. No GPU,
deployment, full integration suite or Production Gate claim is added.
