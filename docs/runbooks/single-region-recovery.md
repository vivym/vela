# Single-Region Recovery Runbook

This runbook is the operational contract for ADR 0012 and the data/recovery
Production Gate. It is a controlled exercise in an isolated validation
environment. A successful backup job is not a recovery result, and no step may
be declared complete without raw evidence and a versioned receipt.

## Targets

| Failure scope | Metadata RPO | Control-plane RTO | Artifact scope |
| --- | ---: | ---: | --- |
| One Control/Storage Node, disk, or control-plane instance | 0 | <= 5 minutes | Existing durable Artifact Store remains authoritative |
| Entire regional control cluster or primary site | <= 15 minutes | <= 4 hours | Sampled committed Artifact backup must restore; full GPU capacity is separately disclosed |

## Preconditions

Record these values before the drill starts:

```text
release_digest=sha256:<64 hex characters>
configuration_revision=<immutable configuration id>
validation_environment=<isolated cluster and namespace>
operator_owner=<on-call identity>
backup_set_id=<base backup and WAL archive set>
artifact_backup_set_id=<versioned Artifact backup set>
fault_injected_at=<UTC timestamp>
last_known_committed_marker=<PostgreSQL transaction or event marker>
```

The operator must verify that the backup credentials are loaded from an
independent secret manager, the Artifact Store is private and versioned, the
CNPG and JetStream manifests render from the same configuration revision, and
the validation namespace cannot receive customer traffic.

## Exercise A: single-node failure

1. Capture PostgreSQL health, synchronous replica state, JetStream replica and
   consumer state, Outbox age, and the committed marker.
2. Stop or isolate exactly one Control/Storage Node. Do not delete PVCs.
3. Confirm CNPG retains synchronous quorum and elects a primary; confirm the
   JetStream stream retains quorum. During any uncertainty, Admission and new
   Assignment must fail closed.
4. Restore node connectivity, wait for PostgreSQL and JetStream replicas to
   catch up, and verify the committed marker and Outbox rows are unchanged.
5. Measure `recovered_at - fault_injected_at`. The gate fails if metadata is
   lost or control-plane recovery exceeds five minutes.

## Exercise B: whole-cluster/site metadata recovery

Use a clean, isolated three-node cluster. Preserve the source cluster and
backup set until all evidence is sealed.

1. Stop control-plane writes and record the final source marker and archive
   timestamp. Do not accept new Admission or Assignment requests.
2. Restore PostgreSQL from the selected base backup and WAL archive. Verify the
   restored marker, schema migration version, RLS/role inventory, and immutable
   Job/Attempt/Charge constraints before starting application replicas.
3. Restore a representative sample of committed Artifact object versions and
   verify object version, size, checksum, content type, and ArtifactSet
   manifest. An incomplete or losing Attempt must not become visible.
4. Create the three-replica JetStream stream and durable consumers from the
   release configuration. Rebuild only delivery state; JetStream is not the
   business authority.
5. Run Outbox replay with the original `event_id` and aggregate version. Run
   Scheduler, Artifact, Invoice, Webhook, retention, and Worker-loss
   reconciliation scans. Verify no duplicate Visible Completion, Charge,
   Invoice line, or terminal webhook authority is created.
6. Rotate recovery credentials and record the old-credential rejection. Verify
   NATS workload identity and subject authorization before opening internal
   traffic.
7. Run read-only Organization/Project isolation probes and the production-gate
   smoke suite. Re-enable Admission only after all invariants and dashboards
   are green.
8. Measure metadata RPO from the final source marker and metadata RTO from
   fault injection to the first authorized control-plane operation. The gate
   fails if RPO exceeds fifteen minutes or RTO exceeds four hours.

## Stop conditions

Abort the exercise and keep traffic closed if any of these occur:

- backup, WAL, Artifact, or secret identity cannot be proven;
- PostgreSQL has no quorum, restored schema/roles differ, or the source marker
  cannot be reconciled;
- JetStream is treated as the source of Job state, or Outbox replay cannot be
  idempotently bounded;
- any cross-Organization read succeeds, any duplicate Charge/Visible
  Completion is observed, or any incomplete Artifact becomes visible; or
- the RPO/RTO target or evidence timestamps cannot be measured.

## Receipt sealing

The operator must hash the raw command output, PostgreSQL queries, Kubernetes
events, restore logs, Artifact verification report, Outbox/reconciliation
report, and isolation/fault results into one evidence bundle. Then create one
`internal/productiongates.Receipt` for the `data-disaster-recovery` gate with
the release digest, configuration revision, validation environment, owner,
thresholds, observed values, evidence reference, evidence SHA-256, and ordered
timestamps. A verbal sign-off or a successful backup-only job is not a PASS.

The repository currently provides the contract and manifests only; no live
cluster drill or `data-disaster-recovery` receipt exists yet.
