# Retain the watchdog stop after journal admission serialization

Date: 2026-09-09. Baseline: `43f95a0`.
This extends the [journal-outage stop correction](journal-stop-outage-evidence-2026-09-08.md).
PostgreSQL 94, Worker journal 5, Runtime journal 8 and Production Gates **0/9**
are unchanged. The complete correctness/architecture objective remains open.

## Reproduction and correction

Avoiding a cancellation's own journal read did not eliminate waiting behind
another operation holding `admission.mu`. The watchdog created its one-second
backend stop context before acquiring that lock. A separately accepted journal
read held the lock for 1.5 seconds; once released, watchdog admission saw an
expired context and discarded the one-shot event without calling backend Cancel.
The baseline regression records two backend calls before and after the wait
(Prepare and Start), proving that no stop was dispatched.

The watchdog now acquires cancellation admission before creating its backend
stop deadline, matching its existing treatment of backend-operation serialization.
Expiry remains a pending internal stop while it waits. Closed admission and
invalid/changed targets still reject it; backend target inspection and Cancel
retain a bounded stop context. Explicit caller cancellation deadlines retain
their existing semantics. This changes no journal format or authority protocol.

This is eventual stop after a released lock, not a fixed stop-latency guarantee.
The admission mutex, operation mutex, noninterruptible fsync and uncooperative
backend operations can still delay dispatch. Actual process/descendant containment
and a strict outage stop bound require a separate design and fault campaign.

## Native boundary

The `watchdog-queued` case of
`TestJournalServerCancellationSurvivesUnresponsiveNode` uses the actual Worker
Agent, Runtime gRPC and separate non-root Linux PID-1 processes. Node stops
serving while retaining its durable owner. Worker requests drain with a
1.5-second deadline. The parent accepts the Runtime's Unix connection without
replying to its handshake, establishing that the journal read is in progress
before the installed watchdog timer fires.

After the drain request times out and releases admission, the pending watchdog
calls the installed backend exactly once with MONOTONIC_DEADLINE. Prepare and
Start counts remain one each. A subsequent stopped observation still cannot
produce drain evidence while Node is unresponsive. Root-owned journal bytes
remain unchanged and the execution stays pending until the same owner serves
again and persists a validated exact checkpoint.

The queued case completes in 1.86 seconds. Its initial Node server joins 18
authenticated/replied exchanges. The restored server joins seven accepts:
six authenticated replies and one abandoned connection. The parent already
accepted the earlier held connection. No background server work remains.
Immediate explicit cancellation and standalone watchdog outage cases also pass.
These are fixture backend observations, not real workload termination receipts.

## Verification

Final-source evidence is stored under
`docs/evidence/watchdog-lock-wait-2026-09-09/` and pinned by the adjacent JSON
receipt, including the failing baseline and its test-only patch.

- Focused cancellation/watchdog/floor/remote-journal race regressions pass.
- Complete uncached ModelRuntime race tests pass in 80.333 seconds.
- Native Linux/arm64 race passes 53 ModelRuntime behavioral main tests plus one
  helper and 14 Node main tests, with no skips or race reports.
- Repository tests, vet, ordinary/Linux lint and Linux/amd64 Node cross compile pass.
- Four opt-in CPU integration/race campaigns cover direct exact cache,
  durable-stream exact cache, production-loop clock offset and PostgreSQL
  replacement-Runtime recovery on this source.

The four CPU campaigns pass in 61.450 seconds overall (12.08, 7.20, 35.73 and
4.39 seconds respectively). The production-loop campaign completes 16 Jobs,
64 Stage artifacts and 16 Charges totaling 20,000 minor units; payload scratch
is zero after both waves and idle. Its retained journal metadata is 654,755 bytes
after idle. These observations do not imply that journal growth is bounded.

The CPU campaigns still use local persistence. Their success does not establish
complete remote-owner Job/cache execution, protected production startup or
sustained service throughput. The [remaining validation plan](remaining-validation-2026-09-09.md)
keeps these obligations distinct from the stop regressions closed here.
