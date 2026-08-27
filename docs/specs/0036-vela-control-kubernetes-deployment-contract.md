# Slice 36: `vela-control` Kubernetes Deployment Contract

## Status

Accepted for implementation on 2026-08-28. This slice adds repository-level
deployment evidence only. It does not create a Launch Receipt and does not
advance any Production Gate from the current `0/9 PASS` result.

## Architecture Boundary

`cmd/vela-control` remains the production runtime for the public HTTP API,
Worker control gRPC, Fleet maintenance gRPC, Finance Reconciliation HTTPS, and
Compliance HTTPS interfaces. This slice does not split that runtime or change
its protocol behavior. It supplies the missing Kubernetes contract for the
`vela-control` deployment unit named by `docs/architecture.md` and consumed by
the existing Fleet Controller at `vela-control.vela-system.svc:8444`.

The contract supports ADR 0007 Organization Isolation, ADR 0008 Customer
Content non-reuse, ADR 0013 non-interrupting releases, ADR 0025 three
Control/Storage nodes, and ADR 0029 evidenced Production Gates. A rendered
manifest is implementation evidence, not proof that any of those operational
requirements passed in a real environment.

## Public Seams

The public seam under test is the rendered Kubernetes resource set rooted at
`deploy/vela-control/kustomization.yaml`. `make validate-deployment` is the
repository-level validation entry point. Tests inspect Kubernetes API objects,
not the internal layout of `cmd/vela-control`.

## Required Contract

1. Run two replicas on distinct `kubernetes.io/hostname` failure domains
   reserved for Control/Storage nodes. Use a rolling strategy with zero
   unavailable replicas, a PodDisruptionBudget with at least one available
   replica, and bounded CPU, memory, and ephemeral-storage requests and limits.
2. Pin the control and secret-materializer images by OCI digest. Repository
   zero-digest values are explicit invalid placeholders that a release overlay
   must replace with approved digests.
3. Run the application as UID/GID 10001 with a read-only root filesystem, no
   privilege escalation, all Linux capabilities dropped, RuntimeDefault
   seccomp, no service-account token, and a bounded writable Artifact validation
   workspace. The root init container may add only `CAP_CHOWN` while copying
   projected Secret and ConfigMap data into a memory-backed volume.
4. Materialize every file credential before application startup. The
   application container must not consume Kubernetes projected symlinks because
   the runtime's secure-file boundary requires regular files, owner-safe modes,
   and no path-level symlink. Database DSNs and the credential pepper come only
   from required Secret references; no Secret value is committed.
5. Derive Scheduler, Artifact Reconciler, Retention Reconciler, non-content
   expiry, backup replication, Webhook Dispatcher, and Invoice Exporter
   identities from the immutable Pod UID. Two replicas must never share a
   static claimant identity.
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
- `make validate-deployment` including `kubectl kustomize deploy/vela-control`;
- `make verify` and the complete integration suite;
- fixed-point Standards and Spec reviews from the pre-slice commit; and
- local commits only unless a separate push is explicitly requested.
