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
last_protected_deletion_authority_marker=<latest deletion authority that must survive restore>
artifact_replication_watermark=<last committed Artifact copy decision included in evidence>
```

The operator must verify that the backup credentials are loaded from an
independent secret manager, the Artifact Store is private and versioned, the
CNPG and JetStream manifests render from the same configuration revision, and
the validation namespace cannot receive customer traffic. Freeze Artifact
replication before selecting a restore point. This release has no external
deletion journal, so the selected PostgreSQL restore point must be at or after
`last_protected_deletion_authority_marker`; otherwise stop rather than risk
reintroducing content whose deletion authority is absent from the restored DB.

For the PostgreSQL path, verify
`deploy/control-storage/barman-cloud-plugin-contract.json` against the exact
recovery release before installing cert-manager, CloudNativePG, or the Barman
Cloud Plugin. Verify each manifest SHA-256, render the verified Barman manifest
through its release-owned install Kustomization before first apply, rewrite
every operator/sidecar image to the recorded digest, and confirm the Barman
operator can read only the exact `vela-backup-s3` Secret while the PostgreSQL
Role can read the same Secret and neither principal can read Artifact
credentials. Confirm the Cluster names
`barman-cloud.cloudnative-pg.io` as its only WAL archiver. Do not fall back to
deprecated `spec.backup.barmanObjectStore`.

## Repository conformance

The local plugin API and recovery path can be exercised before a production
drill:

```sh
HTTP_PROXY=http://127.0.0.1:7897 \
HTTPS_PROXY=http://127.0.0.1:7897 \
NO_PROXY=localhost,127.0.0.1,::1 \
  make test-cnpg-pitr
```

This command creates a fresh disposable four-node kind cluster, verifies and
preloads the pinned images, runs a versioned MinIO bucket, completes a plugin
base backup, archives the target WAL, and restores a second PostgreSQL cluster
to a timestamp between two durable markers. Its output is local conformance
evidence only; it is not a Launch Receipt and cannot replace the production
exercise below.

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
2. Restore PostgreSQL through the pinned Barman Cloud Plugin from the selected
   base backup and WAL archive. Bind the recovery Cluster to the exact
   `ObjectStore` and source server identity, then verify the restored marker,
   schema migration version, RLS/role inventory, and immutable
   Job/Attempt/Charge constraints before starting application replicas.
3. Restore a representative sample of committed Artifact object versions and
   verify object version, size, checksum, content type, and ArtifactSet
   manifest. An incomplete or losing Attempt must not become visible. Keep
   Artifact replication and customer reads disabled.
4. Before JetStream rebuild or application traffic, start only the PRIMARY and
   OFF_CLUSTER_BACKUP retention pools with their separate least-privilege roles
   and stores. Replay all pending Content Deletion and retention targets. Verify
   prompt tombstones, Charge and actor attribution remain present; PRIMARY exact
   versions are absent; every version and delete marker for each backup key is
   absent; and immutable per-target receipts cover both storage tiers. Re-run
   until no eligible target remains. Do not resume replication until its
   watermark is reconciled against deleted Artifacts and a late copy is proven
   impossible.
5. Render `deploy/control-storage` from the exact recovery release and extract
   `jetstream-contract.json` from the generated `vela-jetstream-contract`
   ConfigMap. Using a separately authorized release reconciler, create the
   `VELA_EVENTS` stream and `VELA_SCHEDULER` durable consumer from that document.
   Record the effective stream and consumer info, Raft leader, two current
   followers, file storage, PVC identity, exact subject/filter, explicit ack,
   duplicate window, and limits. Do not lower either replica count or use a
   workload credential for administration. Start `vela-control` with the
   dedicated Scheduler credential and confirm its contract validator binds the
   consumer. Rebuild only delivery state; JetStream is not the business
   authority.
6. Run Outbox replay with the original `event_id` and aggregate version. Confirm
   replayed messages receive a `VELA_EVENTS` quorum `PubAck`, the Scheduler Inbox
   receipt commits before confirmed ack, and periodic PostgreSQL Scheduler scans
   remain active with JetStream stopped. Run
   Scheduler, Artifact, Invoice, Webhook, retention, and Worker-loss
   reconciliation scans. Verify no duplicate Visible Completion, Charge,
   Invoice line, or terminal webhook authority is created.
7. Rotate recovery credentials and record the old-credential rejection. Verify
   NATS workload identity and subject authorization before opening internal
   traffic.
8. Run read-only Organization/Project isolation probes and the production-gate
   smoke suite. Re-enable Admission only after all invariants and dashboards
   are green.
9. Measure metadata RPO from the final source marker and metadata RTO from
   fault injection to the first authorized control-plane operation. The gate
   fails if RPO exceeds fifteen minutes or RTO exceeds four hours.

## Stop conditions

Abort the exercise and keep traffic closed if any of these occur:

- backup, WAL, Artifact, or secret identity cannot be proven;
- the selected restore point predates a protected deletion authority, no
  external deletion journal can replay it, or Artifact replication may race a
  restored/deleted target;
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

The repository includes a PostgreSQL dump/restore plus versioned-MinIO
conformance test for a restore point after Content Deletion authority is durable
and before its targets complete. It also includes committed exact-version
Artifact replication with immutable evidence and copy/delete serialization.
Slice 38 adds a real local Barman Cloud Plugin base backup, WAL archive, and
timestamp PITR in a fresh kind/MinIO environment (`4f4bc2d`). These tests do not
prove production RKE2 or an independent S3 fault domain, provider/network
replication behavior, a restore point before deletion authority, JetStream
rebuild, Outbox replay, secret rotation, the quarterly site exercise, or a
`data-disaster-recovery` Launch Receipt.
