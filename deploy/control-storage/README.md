# Control/Storage Deployment Contract

This Kustomize base is the repository deployment contract for ADR 0025 and the
single-region recovery order in ADR 0012. It describes three non-GPU
Control/Storage Nodes:

- a three-instance CloudNativePG PostgreSQL cluster with synchronous replication,
  independent WAL PVCs, node anti-affinity, and off-cluster S3 WAL/base-backup
  configuration;
- a three-replica, PVC-backed NATS JetStream StatefulSet with required hostname
  anti-affinity and a two-pod disruption budget; and
- an explicit recovery ConfigMap declaring PostgreSQL, Artifact Store, JetStream,
  Outbox replay, and Reconciler order.

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

Before applying, the operator must verify:

1. exactly three eligible nodes exist and `kubernetes.io/hostname` anti-affinity
   places each replica on a different failure domain;
2. PostgreSQL synchronous replication and WAL archive reach the independent
   backup domain;
3. JetStream storage is on independent durable disks and its replica quorum is
   healthy; and
4. the quarterly restore drill produces a versioned Production Gate receipt.

The current repository has only a rendered contract, not a live-cluster
deployment or restore receipt. `docs/launch-receipts/README.md` remains the
authority for launch evidence.
