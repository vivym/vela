# Barman Cloud Plugin And PITR Conformance

Date: 2026-08-28

Status: Implementation target. This slice removes the repository's deprecated
CloudNativePG in-tree object-store backup surface and proves a real local base
backup plus point-in-time recovery through the Barman Cloud Plugin. It does not
prove production object-store durability, an independent failure domain, RKE2
operation, credential rotation, quarterly recovery, or a Production Gate.

## Goal

Keep the three-instance synchronous PostgreSQL authority from Slice 26 while
moving continuous WAL archiving, base backups, and recovery to one pinned
plugin contract before CloudNativePG removes `spec.backup.barmanObjectStore`.
The release contract must make a base backup inevitable, retain a bounded
off-cluster recovery window, and identify every third-party manifest and OCI
image by immutable digest.

## Governing Decisions

- ADR 0012 requires off-cluster WAL archiving, metadata RPO at most fifteen
  minutes, RTO at most four hours, and a quarterly PostgreSQL restore exercise.
- ADR 0025 requires three anti-affined PostgreSQL instances on Control/Storage
  Nodes and treats image approval, disk topology, backup credentials, and real
  restoration as production gates.
- ADR 0029 requires a versioned real-environment Launch Receipt before the
  `data-disaster-recovery` gate can pass. Local kind and MinIO evidence cannot
  satisfy that gate.
- The Barman Cloud Plugin replaces CloudNativePG's deprecated in-tree
  `spec.backup.barmanObjectStore` integration. The `ObjectStore` must be in the
  same namespace as the PostgreSQL `Cluster`; the plugin itself must be in the
  CloudNativePG operator namespace.

## Release Contract

`deploy/control-storage` must render all of the following as one release-owned
contract:

1. `vela-postgres` remains a three-instance, synchronously replicated
   `postgresql.cnpg.io/v1` Cluster with required hostname anti-affinity,
   independent data/WAL PVCs, and the no-quorum write guard.
2. The Cluster has no native `spec.backup`. It names exactly one
   `barman-cloud.cloudnative-pg.io` plugin as WAL archiver and references the
   `vela-postgres-backup` ObjectStore.
3. `vela-postgres-backup` is a `barmancloud.cnpg.io/v1` ObjectStore with the
   thirty-day retention policy, off-cluster S3 destination, gzip WAL
   compression, bounded parallelism, bounded sidecar resources, and exact
   selectors into the independently provisioned `vela-backup-s3` Secret.
4. `vela-postgres-daily` is a plugin-method `ScheduledBackup` owned by itself,
   creates one immediate base backup on first installation, and then runs once
   per UTC day. A WAL-only configuration is insufficient for PITR.
5. `barman-cloud-plugin-contract.json` pins CloudNativePG, cert-manager, Barman
   Cloud Plugin, PostgreSQL, and local MinIO manifest/image identities. Tags are
   operator-facing labels only; every accepted OCI identity also has an exact
   multi-architecture digest.
6. Backup credentials remain absent from Git. The ObjectStore may only refer to
   the named Secret keys; the plugin operator and PostgreSQL sidecar must not
   receive Vela Artifact replication or deletion credentials.

The Barman Cloud Plugin release manifest depends on cert-manager and installs
into `cnpg-system`, while the ObjectStore is namespaced with the Cluster in
`vela-system`. The repository does not vendor those cluster-wide third-party
manifests. A production installer must download them by exact version, verify
their SHA-256, and rewrite the operator and sidecar images to the recorded
digests. The local harness pulls the exact digest for its platform, preloads
those bytes into kind, verifies the manifest's release tags, and uses
`IfNotPresent` so a node never resolves a host-local proxy.

## Local PITR Evidence

`make test-cnpg-pitr` must create a fresh four-node kind cluster and must refuse
to reuse a pre-existing cluster. It preloads the exact platform images so a host
proxy is never interpreted as node-local loopback, then:

1. installs pinned cert-manager, CloudNativePG, and Barman Cloud Plugin
   manifests after verifying every downloaded byte digest;
2. starts a test-only MinIO endpoint and creates the PostgreSQL backup bucket;
3. applies the release ObjectStore, Cluster, and ScheduledBackup, with only
   test-only endpoint, storage-size, resource, superuser, and pull-policy
   overlays;
4. verifies the plugin sidecar is injected and continuous WAL archiving becomes
   healthy;
5. writes a durable pre-target marker and completes one plugin base backup;
6. records a PostgreSQL target timestamp, writes a post-target marker, switches
   WAL, and waits until the containing WAL is archived;
7. bootstraps a distinct Cluster from the same ObjectStore and source server at
   the exact target time; and
8. proves the restored authority contains the pre-target marker but excludes
   the post-target marker.

The harness must log pinned identities, source/restore Cluster names, completed
backup name, recovery target, archived WAL, and restored marker counts. It must
clean up only the kind cluster it created.

## Failure And Compatibility Requirements

- A native `spec.backup`, missing ObjectStore, non-plugin ScheduledBackup,
  missing immediate base backup, wrong Secret selector, unbounded sidecar,
  mutable operator/sidecar image-only tag, manifest digest drift, or plugin
  name drift fails the repository deployment-contract test.
- The local drill fails on any manifest checksum mismatch, unexpected image
  platform, failed plugin rollout, failed backup, absent archived WAL, failed
  recovery, or marker mismatch.
- Migration from in-tree backup to plugin configuration is one release change.
  Previously written Barman Cloud backup objects remain compatible, but the
  live rollout and recovery from an existing production archive require a
  separately authorized deployment exercise.
- The plugin migration does not change Vela database schema, public API, event
  schema, billing authority, or Artifact backup/deletion authority.

## Evidence Boundary

Passing rendered-contract and kind/MinIO tests proves configuration/API
compatibility and an actual local plugin backup/PITR path. It does not prove:

- an independent S3 fault domain, provider durability, versioning, object lock,
  lifecycle policy, or network-partition behavior;
- production secret-manager delivery, credential rotation, least-privilege
  provider policy, or encryption-key recovery;
- three physical Control/Storage Nodes, independent disks, production RKE2/CNI,
  or the four-hour site RTO;
- JetStream rebuild, Outbox replay, Artifact sampled restore, retention replay,
  or quarterly operational ownership; or
- a `data-disaster-recovery` Launch Receipt.

Production Gates remain `0/9 PASS` after this slice.

## Verification

Before commit:

- run the focused deployment-contract tests;
- run `make test-cnpg-pitr` with proxy-backed downloads when required;
- run `make generate`, `make test`, `make lint`, `make test-cross`,
  `make validate-deployment`, and the ordinary integration suite;
- review the diff against this specification and repository standards;
- confirm `git diff --check`; and
- stage only the Slice 38 paths.
