# Execute Accepted Jobs As Durable Stage Graphs

Date: 2026-08-29

Status: Accepted and implemented in the repository; production acceptance remains `0/9 PASS`.

## Context

The current repository binds one end-to-end Attempt to one machine-level
Worker. MiniMax H3 actually executes Encoder, DiT, and VAE Decoder as separate,
long-running model processes. Each of the seven DiT processes is single-GPU and
independent. The components have materially different service times, so one
8-GPU scheduling unit prevents independent capacity sizing and makes an
unrelated downstream failure discard expensive upstream work.

Dynamo and llm-d demonstrate that heterogeneous inference phases benefit from
separate pools, topology-aware transfer, bounded flow control, and explicit
routing decisions. Their request and router state cannot replace Vela's durable
Job, credit, cancellation, Artifact, Charge, or Visible Completion authority.

## Decision

Vela will execute an Accepted Job from an immutable
`ExecutionGraphSnapshot`, instantiated from a Catalog
`ExecutionGraphRevision`.

- An end-to-end `Attempt` owns one durable `StageRun` per static graph node.
- A `StageRun` is a logical execution. Each physical try is a `StageAttempt`
  with its own `StageAllocation` and `StageLease`.
- Every StageLease binds the immutable Attempt fence and the StageRun fence.
  Attempt authority remains valid only while it equals `Job.current_fence`.
- The first implementation permits at most one active StageAttempt per
  StageRun. Speculative duplicates are not enabled.
- Stage failure normally retries in the same parent Attempt and reuses durable
  upstream StageArtifacts.
- A new parent Attempt is required only when the frozen graph authority,
  compatibility set, required inputs, or attempt fence cannot support recovery.
- The first graph executor supports a versioned static DAG, AND dependencies,
  and bounded fan-out/fan-in. It rejects dynamic node creation, cycles,
  arbitrary conditions, and user code.
- Artifact Finalization remains a special Vela authority protocol referenced
  by the graph output contract. It is not an ordinary StageRun.
- Internal model-parallel ranks, DiT denoise steps, and LLM token loops remain
  backend implementation details rather than Vela graph nodes.

The `AttemptCoordinator` Module owns graph instantiation, advancement, fencing,
retry, cancellation, and reconciliation behind one expected-version command
Interface. No other caller may update StageRun, StageAttempt, StageLease, input
pin, or winner authority directly.

## Billing and completion

Billable Start is the first effective graph progress: a StageAttempt starts
under a valid StageLease, or an exact cache hit is atomically pinned and
advances a StageRun. Assignment is not Billable Start. The fixed
Admission-time quote, one Charge, indivisible ArtifactSet, and exactly-once
Visible Completion contracts remain unchanged.

## Consequences

- Encoder, DiT, VAE, and CPU capacity can scale and fail independently.
- State-machine and reconciliation complexity moves into AttemptCoordinator.
- Queues, retry budgets, ETA, fairness, observability, and admission must become
  stage-aware.
- Durable intermediate storage becomes part of Accepted Job capacity.
- Future multi-GPU or multi-node LLM execution is represented by one coarse
  StageRun assigned to one multi-member WorkerInstance, not by exposing ranks.

## Rejected alternatives

- Keep one 8-GPU H3 Worker and only improve node-local scheduling: rejected
  because it preserves the wrong schedulable unit and failure scope.
- Let a per-Job in-memory coordinator own graph progress: rejected because an
  Accepted Job must survive process and node loss.
- Use JetStream messages as graph authority: rejected because delivery is
  at-least-once and PostgreSQL already owns fenced execution state.
- Adopt a general workflow engine: rejected because Vela needs a small,
  versioned inference graph contract, not arbitrary workflows.

## Evidence boundary

This ADR accepts a target. It does not prove implementation, H3 output
compatibility, stage transfer performance, cache correctness, or any Production
Gate. Required migration and conformance evidence is defined in specs 0049 and
0050.
