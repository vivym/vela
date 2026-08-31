# H3 Stage Campaign

Owner: Vela Runtime On-call (24x7)

This runbook governs the real H3 stage campaign for the release and
configuration identities named by the approved campaign window. Repository
tests, simulator output, fake-runtime composition, and candidate artifacts are
not Production Gate evidence. Production Gate status remains unchanged until
the complete versioned Launch Receipt manifest passes independent verification.

## Stop conditions

Pause new campaign Admission when any Stage authority exporter scrape fails,
StageScheduler shadow replay diverges, a READY StageRun is older than 15
minutes, an ACTIVE TransferTicket is older than 5 minutes, or READY residency
coverage is missing for ENCODER, DIT, or VAE_DECODER. Preserve the current
database, event, object-version, Worker epoch, and Kubernetes evidence before
recovery.

Do not unload a healthy resident model to create scheduling capacity or to
clear an alert. Normal drain stops new StageAssignments and waits for accepted
work while the ModelRuntime stays warm. ModelRuntime termination requires the
separate approved maintenance exercise bound to that exact target and window.

## Campaign order

1. Capture the release bundle, ResidencyPlan, GPU/DRA, Pod, WorkerInstance,
   WorkerMember, DeviceSet, ModelResidency, and capacity authority twice and
   reject drift.
2. Complete one same-node ENCODER -> DIT -> VAE_DECODER Job and retain the two
   adjacent TransferTicket and StageArtifact lineages.
3. Complete the equivalent cross-node Job with the same root input, graph,
   profiles, interfaces, connectors, output digest, ArtifactSet, Charge, and
   Visible Completion.
4. Complete the exact cache Job with ENCODER and DIT hits, a physical
   VAE_DECODER, active pins, exact object versions, and unchanged customer
   Charge.
5. Run the approved state/event fault matrix. A process-kill action targets a
   stateless control-plane or Worker Agent process unless the exact
   ModelRuntime maintenance approval is present.
6. Run N/N-1 drain and rollback-before-contraction exercises without converting
   in-flight authority or releasing healthy resident models.
7. Run the 72-hour mixed-load soak and retain dashboards, alerts, page events,
   raw events, ledgers, storage versions, and ownership acknowledgements.

## Triage

- `VelaStageAuthorityExporterFailed`: treat Stage state as unknown. Stop new
  campaign Admission and restore read-only database visibility before using
  any other dashboard panel.
- `VelaStageReadyQueueStalled`: compare READY StageRun age with READY
  ModelResidency, fresh capacity observations, StageScheduler outcomes, and the
  active cutover revision. Do not bypass eligibility or fairness.
- `VelaStageSchedulerReplayDiverged`: stop Assignment immediately. Preserve the
  persisted decision evidence and both algorithm revisions; do not select a
  Worker by hand.
- `VelaStageTransferTicketStuck`: preserve the source Artifact version, pin,
  connector, destination Worker/model epochs, and ticket expiry. An L2 outage
  may block downstream READY but must not invent a new authority.
- `VelaStageModelResidencyCoverageMissing`: determine whether Fleet evidence is
  stale, a Worker is fenced, or an approved drain is active. Recover the fixed
  residency; do not repurpose the GPU or unload another healthy model.

## Evidence closure

Record alert fired and resolved timestamps, incident owner, exact release and
configuration digests, affected campaign Job IDs, root cause, recovery action,
and the before/after authority snapshots. A green dashboard after a gap is not
evidence for the missing interval. Any stop condition, unapproved action,
identity drift, missing raw event, or incomplete 72-hour window makes the
campaign insufficient and requires a new bounded exercise.
