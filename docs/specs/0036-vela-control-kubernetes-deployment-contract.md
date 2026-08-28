# Slice 36: `vela-control` Kubernetes Deployment Contract

## Status

Accepted for implementation on 2026-08-28. This slice adds repository-level
deployment evidence only. It does not create a Launch Receipt and does not
advance any Production Gate from the current `0/9 PASS` result.

## Architecture Boundary

`cmd/vela-control` remains the production runtime for the public HTTP API,
Worker control gRPC, Fleet maintenance gRPC, Finance Reconciliation HTTPS, and
Compliance HTTPS interfaces. Health and dependency readiness use an independent
Pod-private management HTTP listener. This slice does not split the runtime. It
supplies the missing Kubernetes contract for the
`vela-control` deployment unit named by `docs/architecture.md` and consumed by
the existing Fleet Controller at `vela-control.vela-system.svc:8444`.

The contract supports ADR 0007 Organization Isolation, ADR 0008 Customer
Content non-reuse, ADR 0013 non-interrupting releases, ADR 0025 three
Control/Storage nodes, and ADR 0029 evidenced Production Gates. A rendered
manifest is implementation evidence, not proof that any of those operational
requirements passed in a real environment.

## Public Seams

The public seams under test are the rendered Kubernetes resource set rooted at
`deploy/vela-control/kustomization.yaml` and the separation between public API
and private management HTTP handlers. `make validate-deployment` is the
repository-level validation entry point. Deployment tests inspect rendered
Kubernetes API objects rather than individual source YAML files.

## Required Contract

1. Run two replicas on distinct `kubernetes.io/hostname` failure domains
   reserved for Control/Storage nodes. Use a rolling strategy with zero
   unavailable replicas, a PodDisruptionBudget with at least one available
   replica, a non-preempting control-plane PriorityClass, and bounded CPU,
   memory, and ephemeral-storage requests and limits. Reserve at least 110 GiB
   per Pod through an ephemeral PVC backed by an I/O-isolated StorageClass for
   Artifact validation state. `storage-contract.json` records the provider-
   independent capacity, topology, reclaim, IOPS, throughput, and live-receipt
   preflight; provider-specific StorageClass values remain an overlay and live
   gate, not repository evidence.
2. Pin the control and secret-materializer images by OCI digest. The control
   zero digest remains an explicit invalid placeholder that a release overlay
   must replace with an approved Vela digest. Slice 47 later pins the shared
   BusyBox secret-materializer to its exact `linux/amd64` manifest.
3. Run the application as UID/GID 10001 with a read-only root filesystem, no
   privilege escalation, all Linux capabilities dropped, RuntimeDefault
   seccomp, no service-account token, and a bounded writable Artifact validation
   workspace. The root init container may add only `CAP_CHOWN` while copying
   projected Secret and ConfigMap data into a memory-backed volume.
4. Materialize every declared file credential by exact key before application
   startup; recursive copying of a projected volume is forbidden. The
   application container must not consume Kubernetes projected symlinks because
   the runtime's secure-file boundary requires regular files, owner-safe modes,
   and no path-level symlink. Database DSNs and the credential pepper come only
   from required Secret references; no Secret value is committed. ConfigMaps
   and Secrets referenced by a Pod template are immutable and release-versioned.
   Rotation creates a newly named Secret and rolling Pod template before expiry;
   in-place Secret updates are not consumed by a running Pod. The machine-
   readable Secret contract must require immutability and enumerate every
   projected source and materialized destination path.
5. Derive Scheduler, Artifact Reconciler, Retention Reconciler, non-content
   expiry, backup replication, Webhook Dispatcher, and Invoice Exporter
   claimant identities from the immutable Pod UID. Two replicas must never
   share a static claimant identity. Remediation instead uses the stable
   `controller/vela-control` actor required by its shared client certificate;
   operation claims remain stable and idempotent across replicas.
6. Publish five single-purpose ClusterIP Services: public API, Worker control,
   Fleet maintenance, Finance Reconciliation, and Compliance. Preserve the
   Fleet endpoint name `vela-control.vela-system.svc:8444`.
7. Default-deny ingress to `vela-control` Pods and admit each of the five ports
   through a distinct NetworkPolicy with an explicit source identity contract.
   Environment-specific egress destinations remain overlay-owned because the
   Kubernetes NetworkPolicy API cannot safely express the configured DNS names
   for PostgreSQL, NATS, OIDC, object storage, Webhooks, Invoice export, and
   Node Agents.
8. Probe `/healthz` for liveness and `/readyz` for startup/readiness on the
   private Pod port. Readiness remains dependency-aware and may not be replaced
   by a static Kubernetes check.
9. Document every required external Secret, release substitution, namespace
   label, and live verification obligation. Repository rendering must continue
   to report Production Gates as `0/9 PASS`.

## Acceptance Evidence

- focused `go test ./internal/deploymentcontract -run TestVelaControl -count=1`;
- focused management-boundary tests in `cmd/vela-control`;
- `make validate-deployment` including `kubectl kustomize deploy/vela-control`;
- `make verify` and the complete integration suite;
- fixed-point Standards and Spec reviews from the pre-slice commit; and
- local commits only unless a separate push is explicitly requested.
