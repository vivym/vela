# Durable terminal scratch retirement

Local CPU/mock increment over `c0e25bd`. Worker admission journal advances from
3 to **4**. Runtime journal remains **4**, database schema **90**, and launch/Fleet
schemas **2**. Production Gates remain **0/9**. No GPU, push or remote deployment.

## Combined retirement boundary

`NewTerminalScratchRetirement` explicitly combines a bound
`FileAssignmentAdmission`, configured Runtime `Agent`, and the existing
`attempt-owned-filesystem-scratch/v1` contract. Default Worker command assembly
continues to use `RetainScratchRetirer`.

`Retire` implements the following sequence:

1. Preflight the fresh signed terminal history, supplied execution envelopes,
   complete trusted membership and current Runtime routes before RPC dispatch.
2. Atomically persist `INTENT`, the full signed disposition and the Worker input
   floor in the existing locked journal. The StageRun namespace is closed to new
   admission, including a higher-sequence assignment naming that same StageRun.
3. Require durable completion of every retained input invocation through the
   cutoff. Live input work may finish; unknown historical input work rejects.
4. Reconfirm every Runtime's durable floor and collect complete execution
   exclusion proof for every allocation/member pair. Backend drain, envelope
   non-admission and terminal-allocation non-admission remain different types.
5. Persist `READY` with the actual floor acknowledgements, original query
   envelopes and proof responses, plus exact directory component identities.
   Validate the full proof and its relation to retained input admission records.
6. Bind and validate every target before deleting any target. Remove only the
   input StageRun namespace and attempt directories derived from signed history,
   sync the directories, and persist `RETIRED`. Only then close that StageRun's
   latest admission record. Preserve its watermark, floors and input evidence.

The journal is the existing process-locked, root/marker/inode-bound atomic file,
not a separate store with a second admission lock. A new proof cannot become
usable before persistence completes. All collection happens outside the input
admission mutex; final proof validation and deletion serialize with admission.

## Output lifetime premise

`INPUTS_UNUSED` by itself is not an output receipt. Output retirement additionally
uses the authenticated irreversible terminal state and the existing PostgreSQL
materialization contract. New COMMIT requires `MATERIALIZING` with the matching
version/fence; successful COMMIT creates durable L2 before changing the StageRun
to `SUCCEEDED`. `FAILED`/`CANCELED` cannot admit a new COMMIT or SOURCE_LOST.
Previously committed commands replay only their exact durable receipt and need
no local payload. A retrying `RETRY_WAIT` StageRun cannot obtain terminal history.

These rules allow attempt-owned output directories for all terminal allocations
to be retired without interpreting a cancellation ACK as output evidence or
rewriting manifests. Generic reusable output locators are unsupported. This
coordinator does not delete materialization journals, manufacture their command
IDs or recover sealed receipts. Future changes to terminal/materialization
semantics must preserve this premise or change the retirement contract.

## Crash recovery and filesystem ownership

An incomplete collection retains `INTENT` and every file. A fresh signed retry
can complete it without lowering the persisted floor. `Resume` accepts only an
already durable `READY` or `RETIRED` record, identified by StageRun UUID. It
revalidates retained signatures, scope, topology, every proof and directory plan.
It needs no Runtime RPC or fresh Control request after `READY`, including after
query expiry and Runtime profile/epoch changes.

Missing original directory components can represent prior partial deletion.
Replacement components and namespaces that appeared after the proof checkpoint
reject. All targets are checked before any deletion; symlinks, special files
and directories/files writable by another principal reject. Operations stay
relative to bound `os.Root` handles. The shared `stage-runs` parent and unrelated
input/output namespaces remain. A deletion failure leaves `READY`; a final
journal sync failure requires reopen before further action.

The retained record bound is `MaxRecords`, with no eviction, plus the existing
16 MiB whole-journal bound. Each encoded proof is separately bounded. Completed
records are retained. This increment does not establish sustained reclamation
of journal capacity. Empty runtime state or missing process-local input handles
still cannot substitute for historical proof.

Schema 3 requires explicit `AssignmentAdmissionConfig.UpgradeV3`. Schema 2 uses
`UpgradeV2`, now migrating to 4 without inventing input completion. Both preserve
all earlier restrictions and evidence. Bootstrap and upgrades are mutually
exclusive; schemas 2/3 containing retirement evidence and schema 1 reject.

## Validation

Passed:

```sh
go test ./...
go test -race ./internal/stageworkeragent
make lint
go vet -tags=integration ./internal/integration
go test -tags=integration ./internal/integration -run '^TestStageTerminalHistoryCoversAllocatedUndeliveredRetry$' -count=1 -timeout=5m
go test -tags=integration ./internal/integration -run '^(TestStageMaterializationTerminalReplayRequiresDurableReceipt|TestStageMaterializationHandlerReplaysTerminalCommandsAfterTTLAndReconnect)$' -count=1 -timeout=5m
go test -tags=integration ./internal/integration -run '^TestStageMaterializationCannotStartAfterTerminalCancellation$' -count=1 -timeout=5m
git diff --check
```

The two-member UDS test combines an actual FakeRuntime drain, envelope absence
and an unsigned retry's terminal absence with Worker input completion and real
filesystem cleanup. Other tests cover live/unknown input work, persisted Runtime
intent, lost floor replies, READY sync/reply failures, concurrent replay, bounded
history, explicit schema migration, corrupted proof/signature/member/contract,
directory replacement and permissions. A subprocess exits directly after input
deletion; its parent reopens `READY` and finishes only the remaining owned files.

The PostgreSQL regression allocates a retry and cancels before its execution
envelope is ever signed. It now follows authenticated Control history through
private UDS, the Worker journal and actual scratch deletion, preserving unrelated
files. This integration Runtime never executes either allocation; the separate
two-member test covers mixed actual drain and non-admission. Additional PostgreSQL
tests verify the output lifetime premise above.

Linux arm64 passes the selection
`^(TestTerminalScratchRetirement|TestAssignmentAdmission|TestAssignmentInputDrain|TestAssignmentFloor)`
under UID/GID 65534, `--network none`, read-only root/binary mounts, dropped
capabilities, no-new-privileges and private `/tmp` tmpfs. Image:
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

## Remaining work

Default command/Stream recovery assembly, authoritative reconciliation of
materialization records, missing old input/Runtime writer recovery, lost renewal
envelopes, sealed receipt recovery, schema-1 migration and bounded checkpoint
reclamation remain open. External asynchronous driver tasks, descendants and
writable handles still require their own containment/drain implementation.
The coordinator relies on the existing trusted resolver/backend contracts,
authenticated transports and protected journal; it does not defend against a
hostile journal owner. No steady-state Worker throughput or Production Gate is
claimed. The default retention policy remains active.
