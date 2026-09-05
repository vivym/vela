# Terminal materialization reconciliation

Local CPU/mock increment over `d0b7c6b`. Worker admission and Runtime journals
remain **4**, materialization journal **2**, database **90**, and launch/Fleet
schemas **2**. Production Gates remain **0/9**. No GPU or remote deployment.

## Behavior

`DurableStreamConfig.TerminalRetirement` explicitly connects the existing
`TerminalScratchRetirement` to the Stream's materialization lifecycle. The
constructor requires the same admission gate and Runtime Agent, configured
materialization, and `attempt-owned-filesystem-scratch/v1`. The default command
still uses `RetainScratchRetirer`; it does not enable this configuration.

`StreamAgent.RetireTerminalScratch` uses fresh signed terminal history and the
existing complete retirement coordinator. It serializes with sealing, source
reads and publication through the existing `materializationMu`. Before allowing
scratch deletion it checks every matching retained materialization record:

- The original StageAuthority signature and complete terminal allocation scope.
- The receipt digest, exact output lineage, attempt-owned locator and size.
- Seal time within that execution envelope's validity. A seal can finish after
  Control observed terminal state; the subsequent complete drain checkpoint
  excludes the in-flight writer, not timestamp ordering alone.
- Any retained MaterializationAuthority signature and its exact relationship to
  the StageAuthority, receipt and output. Expiry does not invalidate old facts.
- A local confirmed COMMIT cannot coexist with a FAILED/CANCELED terminal state.

The issuer and reconciler share `LocalOutputManifestV1.MatchesStageAuthority` for
lineage comparison. That method alone does not validate signatures or a manifest.

Only after durable `RETIRED` does the Stream clear a matching active authority
and delete the selected materialization journal records. The retirement proof,
Worker floor and allocation/input history remain. Other StageRun records are
not rewritten or removed by terminal reconciliation.

## Recovery and result semantics

For an explicitly configured Stream, `ResumeMaterializations` first enumerates
durable retirement entries and validates all affected materialization records.
It resumes READY cleanup before ordinary materialization replay or output reads.
It also processes RETIRED entries whose journal cleanup previously failed. No
Runtime RPC or fresh Control query is needed for those completed proofs, even
after expiry and restart. INTENT retains scratch/records and blocks ordinary
replay until fresh history and complete drain can finish it. The existing
ProductionAgent recovery loop therefore cannot discover/acquire new work through
an incomplete retirement in this configured path.

Deleting an obsolete local recovery record is not acknowledgement of its old
COMMIT or SOURCE_LOST command. A StageRun may have been canceled before the
command arrived, or a different allocation may have succeeded. The returned
`MaterializationResult.TerminalRecordsRetired` counts local records removed in
this call; it does not set `Committed`, `SourceLostReported` or `L2Published` and
is not an exactly-once billing event. No command ID is reconstructed, no new
materialization mutation is sent, and no Control receipt is manufactured.

This permits a terminal record lacking its original command ID to be discarded
using independent terminal/retirement evidence. Ordinary nonterminal replay
still requires its original exact command ID and durable Control receipt.
RETRY_WAIT cannot obtain terminal history and cannot use this cleanup path.

The two journals need no atomic cross-file transaction: durable RETIRED precedes
deleting a materialization record, and its retained proof makes recovery
idempotent if deletion fails or an older record reappears after restart. A
partial result never erases the proof needed to retry.

## Validation

Passed:

```sh
go test ./...
go test -race ./internal/stageworkeragent
make lint
go vet -tags=integration ./internal/integration
go test -tags=integration ./internal/integration -run '^(TestStageMaterializationCannotStartAfterTerminalCancellation|TestStageMaterializationTerminalReplayRequiresDurableReceipt|TestStageMaterializationHandlerReplaysTerminalCommandsAfterTTLAndReconnect|TestStageTerminalHistoryCoversAllocatedUndeliveredRetry)$' -count=1 -timeout=5m
git diff --check
```

New Stream tests cover sealed, late-sealed, issued, published, unconfirmed COMMIT,
confirmed COMMIT, missing legacy command ID, and pending/confirmed SOURCE_LOST
records. They create durable Worker proof through private Runtime UDS, interrupt
cleanup, reopen the Worker/materialization journals, close the Runtime sockets
and expire the query before automatic recovery. These tests construct the local
materialization records as fixtures; they do not represent a new end-to-end
Control-to-backend materialization campaign. The existing PostgreSQL tests above
separately verify terminal command rejection, exact durable replay, and the
RETRY_WAIT/terminal-history distinction supporting the output lifetime premise.

Other new regressions cover malformed signature/lineage/locator/size/seal time,
wrong allocation, conflicting confirmed COMMIT, mismatched Stream owners,
unknown input INTENT blocking Production discovery, journal deletion failure,
preservation of another StageRun's record, and exclusion of concurrent output
reads. All are CPU/mock checks.

Linux arm64 also passes
`^(TestTerminalMaterialization|TestTerminalScratchRetirement|TestAssignmentAdmission|TestAssignmentInputDrain|TestAssignmentFloor)`
under UID/GID 65534, no network, read-only root/binary mounts, all capabilities
dropped, no-new-privileges and private `/tmp` tmpfs. Image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

## Remaining scope

Fresh Control history acquisition and automatic INTENT collection, default
command/bootstrap assembly, recovery of unknown historical input/Runtime
writers and lost renewal envelopes, sealed receipt recovery, and bounded
Worker/Runtime checkpoint reclamation remain open. Journal/root protection and
single-process ownership remain required. External asynchronous driver tasks,
descendants and writable handles need their own containment/drain implementation.
This increment does not claim steady-state Worker throughput or production
readiness, and does not clear the retained admission/retirement checkpoints.
