# CNPG Single-node Failover And No-quorum Safety

Date: 2026-08-26

Status: Implementation target. This slice closes the repository-verifiable part
of acceptance scenario 26. It does not prove production RKE2 networking, disk
durability, off-cluster WAL restoration, a site recovery, or a Production Gate.

## Goal

Prove that the release-owned three-instance CloudNativePG contract can elect a
new primary after loss of the node hosting the old primary without losing Vela's
committed PostgreSQL authority. Prove separately that a surviving primary cannot
commit a new Admission or Scheduler Assignment after both synchronous standbys
become unavailable. PostgreSQL remains the only business authority throughout
both exercises.

## Governing Decisions

- Architecture sections 8.1, 17.3, and 17.4 require synchronous replication,
  automatic single-node failover, RPO 0, RTO at most five minutes, and fail-closed
  Admission and new Assignment while PostgreSQL quorum cannot be proven.
- ADR 0012 separates single-node automatic recovery from manual whole-site
  restoration and requires live restore receipts for the latter.
- ADR 0025 fixes the release topology at three anti-affined CloudNativePG
  instances with independent data and WAL volumes on Control/Storage Nodes.
- ADR 0029 requires a versioned real-environment fault-injection Launch Receipt
  before the data/disaster-recovery Production Gate may pass.

## Public Test Seams

The architecture already fixes these seams. Tests observe behavior through them
and do not replace CloudNativePG with a single PostgreSQL process restart:

1. The rendered `postgresql.cnpg.io/v1` `Cluster`, PostgreSQL disruption budget,
   and recovery ConfigMap are the release deployment contract.
2. Admission is exercised through the authenticated `POST /v1/projects/{id}/jobs`
   HTTP boundary. A `202 Accepted` is the only successful Admission result.
3. New Assignment is exercised through `scheduler.Service.RunOnce`, including
   its durable Scheduler claim and production `workercontrol.Service.Acquire`
   coordinator call.
4. CloudNativePG status, Kubernetes Pod placement, the read/write Service, and
   PostgreSQL queries are used only to observe election, synchronous health, and
   the committed business authority produced through seams 2 and 3.

## Release Contract

The Control/Storage base must render exactly three CloudNativePG instances with:

- required hostname anti-affinity and a PostgreSQL PodDisruptionBudget retaining
  two available instances;
- unsupervised primary updates and automatic failover;
- required synchronous data durability, with PostgreSQL requiring an
  acknowledgement from any one of the two eligible replicas;
- independent PVC-backed data and WAL storage;
- off-cluster WAL/base-backup configuration and explicit backup credentials; and
- recovery declarations for single-node RPO 0, RTO five minutes, automatic
  failover, and no-quorum Admission/Assignment fail-closed behavior.

The repository conformance exercise pins kind, the Kubernetes node image, and
the CloudNativePG operator version. A test-only `local-path` StorageClass alias
uses kind's bundled local-path provisioner. The exercise applies the release
Cluster manifest itself, then a test-only overlay enables bootstrap superuser
access and removes the intentionally unreachable example backup endpoint.
The release-named operator and PostgreSQL images are preloaded into kind before
the measurement begins. The operator test overlay also uses `IfNotPresent` so a
host proxy is not incorrectly resolved as node-local loopback after preload.
Test-only bootstrap credentials and catalog fixtures may be created after
cluster initialization. The test pins the single operator Pod to the separate kind
control-plane so that stopping a database worker isolates PostgreSQL election
instead of Kubernetes operator rescheduling. These changes are not production
credentials or deployment recommendations. The rendered-contract test, not
this overlay, retains and validates the off-cluster backup configuration.

Migration 00026 owns the no-quorum write fence. Deferred constraint triggers on
`jobs`, `scheduler_dispatch_intents`, and `attempts` check for a streaming
synchronous standby immediately before commit. Their `SECURITY DEFINER`
function is owned by the non-login, non-superuser `vela_quorum_guard_owner`,
whose only elevated membership is `pg_read_all_stats`. The guard is active only
when the explicit custom GUC `vela.require_synchronous_quorum=on` is present;
the release manifest enables it. Once enabled, an empty
`synchronous_standby_names` or the absence of a streaming `sync`/`quorum`
standby fails with SQLSTATE `55000`. Single-node development and test databases
leave the GUC unset. Because the triggers are database-owned, current Admission
and Scheduler transaction paths receive the fence after the expand migration,
independently of process-local health checks.

## Required Failure Evidence

1. Three PostgreSQL instances become healthy on three distinct Kubernetes nodes.
   Both replicas are streaming and current, and at least one is synchronous,
   before business writes begin.
2. Admission accepts two Jobs. Both have an authoritative Outbox intent and
   Credit Reservation. One remains queued and reserved.
3. The second Job receives an Assignment and Lease fence, reaches Billable Start,
   and Customer Cancellation produces exactly one immutable Charge while
   consuming its reservation.
4. The container hosting the current primary node is stopped. CloudNativePG
   automatically elects a different primary from a synchronous replica in at
   most five minutes without editing the Cluster or manually promoting a Pod.
5. After reconnecting through the read/write Service, both Accepted Jobs, their
   Outbox intents and Credit Reservations, the exact Lease fence, and the Charge
   remain unchanged. No duplicate authority row appears.
6. After the old node rejoins and all replicas become current, the two standby
   nodes are stopped while the primary remains reachable. A bounded Admission
   request does not return `202`, and a bounded `scheduler.Service.RunOnce`
   returns SQLSTATE `55000` without producing a Scheduler dispatch or Assignment.
7. After one standby returns and synchronous quorum is restored, no Job,
   Credit Reservation, Outbox intent, Scheduler dispatch, Attempt, Lease, or
   Charge from either rejected no-quorum operation exists. Existing committed
   authority is still unchanged.
8. The test records the old and new primary identities, distinct node placement,
   measured failover duration, no-quorum operation results, and final authority
   counts in the Go test log.

## Compatibility And Non-goals

- This slice adds migration 00026 without changing table shape, public API, or
  event schema. The migration must expand before the binary rollout; contraction
  removes only the two guards and their function after no prior binary remains.
- The kind/local-path environment validates operator behavior and Vela's
  transaction boundaries, but it is not evidence for RKE2, physical disk loss,
  production network partitions, operator high availability/rescheduling, or
  backup durability.
- The exercise does not perform PITR, site restoration, Artifact backup restore,
  JetStream rebuild, Outbox replay, secret rotation, or the quarterly DR drill.
- Repository tests and their logs are not Launch Receipts. Production Gates
  remain `0/9 PASS` until the required real-environment receipts exist.

## Verification

Before commit:

- run the focused rendered PostgreSQL/recovery contract tests;
- run the pinned four-node kind plus CloudNativePG fault test;
- run `make generate`, `make test`, `make lint`, `make test-cross`,
  `make validate-deployment`, and `make test-integration`;
- run a Standards and Spec review against this file;
- confirm `git diff --check`; and
- audit the staged paths so unrelated user changes are not committed.
