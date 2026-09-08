# Supervisor and Worker integration with the authenticated journal

Date: 2026-09-08. Baseline: `d61cd70`. PostgreSQL 94, Worker journal 5,
Runtime journal 8. Production Gates remain **0/9**.

## Implemented behavior

`NewSupervisorWithRemoteExecutionJournal` now attaches unused Services to an
independently configured Node owner. Actual Supervisor admission, backend
candidate confirmation, health, sealed receipt, drain and historical reads use
the remote adapter. It holds no workload-owned journal files. The existing local
store implements the same private interface; its full validation remains intact.

Construction requires the expected journal UUID/scope/storage identity, complete
launch and verifier, a matching UNRESOLVED startup incarnation, approved Service
bindings and a bounded transport timeout. Nonempty execution/floor/checkpoint
history is refused by this initial-attachment constructor. It is not a backend
factory, first-use permission, epoch allocator, recovery constructor or production
startup issuer. Already running models and their effective configuration still
require trusted outer assembly and original-process approval.

The Runtime transport authenticates the root Node for every operation. Reads are
16 KiB pages within the unchanged 32 KiB reply bound, carrying one state digest,
offset, total length, journal identity and lock document. The assembler limits
the complete document to the existing 12 MiB journal bound, rejects missing,
malformed or mixed pages, verifies the assembled digest and returns no partial
document on failure. The remote adapter applies the complete existing semantic
snapshot verifier, checks original storage and startup incarnation/launch, and
refuses regressing floor or highest sequence. Request data cannot replace state.

The Supervisor retains its admission mutex, shared AUX slot and in-flight call
tracking. Each successful state check refreshes local watermarks monotonically
from the owner. New backend work still requires durable admission and candidates,
followed by authority freshness checks before backend entry. A durable replay
receipt cannot dispatch a fresh Prepare. Unknown write outcomes, inconsistent
receipts and stale readback prevent backend entry and poison admission until
explicit reconciliation. Neither a later ordinary retry nor reconnect clears
that uncertainty automatically.

Paged reads can race a legitimate Worker floor update. The adapter retries only
these read-version conflicts, at most three times within one configured timeout.
Exhausted reads before any mutation reject the current operation without
permanently poisoning admission. A failed readback after a write remains an
uncertain operation. This distinguishes ordinary read contention from a lost
durable acknowledgement; there are no automatic mutation retries.

`NewJournalWorkerClient` wraps the existing Runtime RPC client. It persists floor,
execution non-admission and terminal non-admission through the Worker's separately
authenticated Node identity before issuing the matching Runtime RPC. The Runtime
adapter can read these proofs and install the corresponding in-memory floor but
cannot send Worker mutations using the Runtime role. A direct unpersisted floor
request is rejected without disabling later legal Worker progress. The wrapper
does not take custody of the separate Worker input/materialization journal.

## Evidence boundary and remaining integration

This increment goes beyond testing an isolated broker: the actual Supervisor
runs Prepare, Start, Seal and Drain in a non-root Linux PID-1 process through
**33 authenticated Node exchanges**, against a real root-private journal. The
fake backend's sealed receipt and drain are persisted before success. The
Runtime cannot open/create the state/lock files, and Runtime shutdown leaves the
Node-owned journal available. Original process/role, sibling denial, lost-reply,
exit and transport regressions remain selected in the native runner.

Portable tests also run real Supervisor and gRPC calls with a deterministic
direct-owner transport. They test all three Worker wrapper operations, exact
sealed receipt replay, unavailable owner, lost admission response, replay-marked
receipt, foreign journal receipt, stale readback and digest-bound pagination.
The fault transport supplies no process authentication; the native test supplies
that separate evidence. An in-flight blocked Prepare survives a Worker floor
installation as a tracked operation; waiting times out until it finishes, and
the next Start is fenced. Read-contention tests prove both bounded retry and
subsequent positive progress.

Production commands and deployment mounts are **not switched** to this adapter.
The existing 16-Job CPU campaign and cache/recovery campaigns remain compatibility
checks on the original local-persistence path. They do not prove a full protected
Node-owned Job path. No real GPU, remote rollout, model equivalence, production
performance, or Production Gate claim is made.

Remaining work, in dependency order:

1. Assemble Registry/Fleet-approved effective launch, current epochs, original
   Runtime/Worker processes and journal pair into the production startup path.
   Preserve first-use/adoption provenance and root-private mounts across the
   actual initializer and workload commands; do not synthesize permission from
   the constructor's matching fields.
2. Integrate Node endpoint serving with bounded concurrent handshakes, and wire
   the Worker wrapper and separate Worker journal custody into the production
   composition. The native test currently supplies fixture CRI/native metadata
   and an explicitly trusted startup incarnation, not an independent issuer.
3. Define and verify same-owner uncertain-write reconciliation, Node restart,
   exact-owner exit, successful replacement, and backend/resolver descendant
   containment. The initial remote constructor deliberately does not implement
   those recovery cases. A protocol that only refuses replacement is incomplete.
4. Measure and reduce read/validation cost under matched multi-member load.
   The initial adapter fetches and validates a complete document on each check;
   33 exchanges for one tiny execution are a functional observation, not a
   performance acceptance. RPC cancellation currently waits for an in-progress
   journal exchange's configured timeout/lifetime context; immediate per-RPC
   interruption requires further context plumbing through the store boundary.
5. Reclaim history under proven terminal ownership, run beyond 32 executions and
   sustained arrivals, and run the full CPU Job/cache/fault campaign through the
   protected path. The original `11ce026` STALE campaign failure and integration
   lint backlog remain separate unresolved items.

## Reproduction

Final source digest for Go/SQL/toolchain/media inputs:
`5f2854faca99f083d1a4576559bb4c19eb4b217a46738533136869baa080c237`.

- Ordinary repository tests, vet, ordinary lint, Linux changed-package lint and
  Linux/amd64 cross compilation pass. Both lint results report zero issues;
  cross compilation is not native execution.
- Native Linux/arm64 race: 44 behavioral ModelRuntime main tests plus one helper
  entrypoint, and seven Node main tests; zero skips, failures or race reports.
- Enabled CPU race campaigns plus PostgreSQL recovery pass in 106.839 package
  seconds: durable cache 24.64 s, direct cache 25.13 s, production-loop offset
  48.10 s, replacement-Runtime PostgreSQL recovery 6.08 s.
- The production-loop receipt records 16 Jobs, 64 Stage artifacts and 16 Charges;
  each of four Workers records 16 future-issued assignments under the shared
  30-second skew and -1-second consumer offset. Idle scratch, active leases,
  allocations, reservations and execution/finalization pins are zero. Journals
  retain 654,770 bytes, so sustained history bounds are still not established.

The final-source parallel full race attempt failed in 161.851 s in the existing
`TestAllocationExecutionDiscoveryRejectsAmbiguousOrUnavailableObservations/invalid`:
it returned `context deadline exceeded` instead of its expected domain rejection.
The test gives all fault modes a 30 ms context, including nontimeout modes. It
exposed no observed authority or inspection, so the failure does not demonstrate
acceptance of invalid evidence. Three isolated repetitions pass in 3.293 s.
The timing explanation is supported by the failure and fixture, but the exact
scheduling cause is not instrumented. Retain this result as test-timeout
sensitivity, not a clean first-pass run or a fixed production defect. The
unchanged final source is also checked by a serial full race rerun below.

The serial full uncached ModelRuntime race rerun **passes in 89.786 s**.
[Machine-readable evidence](journal-remote-integration-evidence-2026-09-08.json)
contains the three full CPU receipts and hashes for all 23 retained artifacts.
[Raw evidence](evidence/journal-remote-integration-2026-09-08/) preserves both the
failed parallel attempt and successful final checks. Source did not change
between these final runs; documentation and evidence were added afterwards.

Raw whitespace is preserved in `campaigns.log`, `native/node-startup.log` and
`native/source.patch` under that directory. Only those three named artifacts
are excluded from whitespace checks; their exact bytes are hash-verified.

```sh
go test -race ./internal/modelruntime -count=1
bash hack/run-journal-owner-native.sh
VELA_RUN_CPU_MOCK_CAMPAIGN=1 go test -race -tags=integration ./internal/integration \
  -run '^(TestCPUMockProductionLoopClockOffsetCampaign|TestCPUMockExactCacheSourceTargetCampaign|TestCPUMockDurableStreamExactCacheCampaign|TestStageTerminalRecoveryThroughReplacementRuntimeRestoresCapacity)$' \
  -count=1 -v -timeout=8m
```

The runner retains the pinned builder, static race binaries and isolated scratch
containers. Its final source patch reconstructs 186 affected-package/runner
files byte-for-byte from `d61cd70`. Earlier successful checks preceded the final
read-contention correction; final checks are recorded separately. A concurrently
started Linux lint attempt exited with the tool's process-lock error, then passed
when rerun after ordinary lint finished. It was not a source diagnostic.
