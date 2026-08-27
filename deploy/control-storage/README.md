# Control/Storage Deployment Contract

This Kustomize base is the repository deployment contract for ADR 0025 and the
single-region recovery order in ADR 0012. It describes three non-GPU
Control/Storage Nodes:

- a three-instance CloudNativePG PostgreSQL cluster requiring an acknowledgement
  from any one synchronous replica, with independent WAL PVCs, node
  anti-affinity, and off-cluster S3 WAL/base-backup configuration;
- a three-replica, PVC-backed NATS JetStream StatefulSet with required hostname
  anti-affinity and a two-pod disruption budget; and
- a generated `vela-jetstream-contract` ConfigMap whose
  `jetstream-contract.json` is the release authority for the `VELA_EVENTS`
  stream and `VELA_SCHEDULER` durable consumer; and
- an explicit recovery ConfigMap declaring PostgreSQL, Artifact Store, JetStream,
  Outbox replay, and Reconciler order.

The PostgreSQL Cluster sets `vela.require_synchronous_quorum=on`. Migration
00026 then rejects new Job, Scheduler dispatch, and Attempt commits with
SQLSTATE `55000` when synchronous replication is unconfigured or no streaming
synchronous standby is visible. Single-node development databases leave this
custom GUC unset.

Render it before any cluster action:

```sh
kubectl kustomize deploy/control-storage
```

The manifests intentionally contain no backup credentials. The operator must
create `vela-backup-s3` in `vela-system` from an independent secret manager and
replace the example S3 endpoint and image tags with the approved release
configuration. Production image digests, storage-class capacity, RKE2 node
labels, CNPG operator compatibility, and an external or three-node S3 Artifact
Store are deployment gates, not claims made by this repository render.

The CNPG backup and the committed Artifact backup are separate stores and
credential domains. The application release must provide independent logins
for replication and deletion, independent PRIMARY-read and backup-write
credentials for replication, and a third backup read/delete credential for
retention. The PRIMARY and off-cluster Artifact buckets must both be versioned:

```text
VELA_BACKUP_RETENTION_DATABASE_URL=<login bound only to vela_backup_retention>
VELA_NON_CONTENT_EXPIRY_DATABASE_URL=<login bound only to vela_non_content_expiry>
VELA_NON_CONTENT_EXPIRY_RECONCILER_ID=<stable unique expiry identity>
VELA_NON_CONTENT_EXPIRY_TICK=1m
VELA_NON_CONTENT_EXPIRY_CLAIM_TTL=1m
VELA_NON_CONTENT_EXPIRY_HELD_RETRY=5m
VELA_NON_CONTENT_EXPIRY_BATCH_SIZE=100
VELA_ARTIFACT_REPLICATION_DATABASE_URL=<login bound only to vela_artifact_replication>
VELA_ARTIFACT_REPLICATION_ID=<stable unique replica identity>
VELA_ARTIFACT_BACKUP_S3_ENDPOINT=<off-cluster S3-compatible endpoint>
VELA_ARTIFACT_BACKUP_S3_REGION=<region>
VELA_ARTIFACT_BACKUP_S3_BUCKET=<versioned committed-Artifact backup bucket>
VELA_ARTIFACT_BACKUP_S3_ACCESS_KEY_ID_FILE=<read/delete credential file>
VELA_ARTIFACT_BACKUP_S3_SECRET_ACCESS_KEY_FILE=<secret credential file>
VELA_ARTIFACT_BACKUP_S3_PATH_STYLE=<true|false>
VELA_ARTIFACT_REPLICATION_SOURCE_S3_ACCESS_KEY_ID_FILE=<PRIMARY exact-version read-only credential file>
VELA_ARTIFACT_REPLICATION_SOURCE_S3_SECRET_ACCESS_KEY_FILE=<PRIMARY exact-version read-only secret file>
VELA_ARTIFACT_REPLICATION_BACKUP_S3_ACCESS_KEY_ID_FILE=<backup conditional-write/current-head credential file>
VELA_ARTIFACT_REPLICATION_BACKUP_S3_SECRET_ACCESS_KEY_FILE=<backup conditional-write/current-head secret file>
VELA_ARTIFACT_REPLICATION_TICK=1m
VELA_ARTIFACT_REPLICATION_CLAIM_TTL=20m
VELA_ARTIFACT_REPLICATION_RETRY_DELAY=5m
VELA_ARTIFACT_REPLICATION_TIMEOUT=15m
VELA_ARTIFACT_REPLICATION_BATCH_SIZE=10
```

The non-content expiry login is separate from the Retention, Compliance, and
internal logins. It can execute only the claim and completion functions added
by migration 00031; it has no direct table or Customer Content authority.
`VELA_NON_CONTENT_EXPIRY_CLAIM_TTL` and `VELA_NON_CONTENT_EXPIRY_HELD_RETRY`
must be whole seconds. The release must keep tick in `(0, 1h]`, claim TTL in
`[1s, 1h]`, held retry in `[1s, 24h]`, and batch size in `1..1000`.

Migration 00028 creates backup deletion targets only for `COMMITTED` Artifacts.
The backup Reconciler purges every version and delete marker for the exact key;
the request receipt is not complete until PRIMARY and OFF_CLUSTER_BACKUP targets
both complete. Migration 00029 and the `vela-control` Artifact Replicator create
an immutable intent only when an Artifact becomes `COMMITTED`, read the frozen
PRIMARY version, conditionally write the backup, validate a matching current
backup object after an ambiguous write response, and retain one PostgreSQL row
lock across storage I/O. Content Deletion updates the same row: deletion first
cancels the pending copy; copy first commits its backup version before deletion
can create durable targets and receipts. The operation timeout must remain
strictly below the claim TTL.

Repository integration tests exercise that contract against PostgreSQL and two
versioned MinIO buckets. A deployment still needs live evidence for its exact
network, S3-compatible service, credential policy, process-kill windows, and
failure-domain separation. In particular, an `ALREADY_ABSENT` deletion receipt
proves idempotent cleanup, not that a backup copy previously existed, and the
repository does not claim to eliminate a provider-side write that continues
after the client process and database connection both fail.

The current backup contract uses CNPG 1.30 native
`spec.backup.barmanObjectStore`. CNPG removes that native integration in 1.31.
An operator upgrade to 1.31 or later is blocked until the release replaces this
surface with the Barman Cloud Plugin, pins the plugin/image contract, validates
credential isolation, and produces successful backup plus PITR restore evidence.

Before applying, the operator must verify:

1. exactly three eligible nodes exist and `kubernetes.io/hostname` anti-affinity
   places each replica on a different failure domain;
2. PostgreSQL synchronous replication and WAL archive reach the independent
   backup domain;
3. committed Artifact replication uses an independent failure domain, and a
   replication/deletion race exercise proves no deleted object can be copied
   after deletion authority or restored after retention replay;
4. JetStream storage is on independent durable disks and its replica quorum is
   healthy, and the rendered `vela-jetstream-contract` data matches the release
   artifact byte-for-byte; and
5. the quarterly restore drill produces a versioned Production Gate receipt.

The release reconciler must create or update JetStream only from
`jetstream-contract.json`; neither `vela-control` nor a workload credential has
stream/consumer administration authority. Before and after every publish, the
Outbox validates the actual stream against the same typed contract; the
Scheduler validates both stream and consumer before consuming. Contract drift
leaves Outbox rows unpublished and disables the event wakeup while the periodic
PostgreSQL Scheduler scan continues; it must not be repaired by lowering
replicas, changing storage to memory, broadening the subject filter, or replacing
explicit ack.

The Outbox and Scheduler use separate NKey credentials. In addition to the
existing Outbox variables, the application release must provide:

```text
VELA_SCHEDULER_INBOX_DATABASE_URL=<login bound only to vela_scheduler_inbox>
VELA_NATS_SCHEDULER_CREDENTIALS_FILE=/run/secrets/nats/scheduler.creds
VELA_NATS_SCHEDULER_USER_PUBLIC_KEYS=<current[,overlap] Scheduler user NKey public keys>
```

The Scheduler Inbox database URL must not reuse the `vela_scheduler` or
`vela_internal` login. Its login inherits only the `vela_scheduler_inbox`
NOLOGIN role, whose exact runtime surface is the SECURITY DEFINER receipt
function introduced by migration 00025. Readiness probes both Scheduler pools.

The Outbox credential may publish only `vela.events.>` and the exact
`$JS.API.STREAM.INFO.VELA_EVENTS` request needed to reject contract drift; it may
subscribe only to `_INBOX.>` request replies. Because the added stream-info
permission changes the exact JWT contract, a mixed N/N-1 rollout must use
versioned credential Secrets: the N-1 ReplicaSet retains its old credential and
the current ReplicaSet receives the revised credential. Replacing one shared
credential file underneath both versions is not a compatible rollout.

The Scheduler credential may publish only the exact stream-info,
consumer-info, pull-next, and `VELA_SCHEDULER` ack API subjects declared by the
application contract, and may subscribe only to `_INBOX.>` request replies. It
must not subscribe directly to `vela.events.>`, publish business events, or
administer JetStream.

The current repository has only a rendered contract, not a live-cluster
deployment or restore receipt. `docs/launch-receipts/README.md` remains the
authority for launch evidence.
