# `vela-control` Deployment Contract

This Kustomize base records the production boundary for the modular
`vela-control` runtime. It runs two identical replicas on distinct
Control/Storage nodes and exposes five single-purpose ClusterIP Services. Render
the base locally with:

```sh
kubectl kustomize deploy/vela-control
```

The repository base is deliberately not deployable as production
configuration. Both container images use an invalid all-zero digest, and
`vela-control-runtime-placeholder` contains `.invalid` endpoints, placeholder
public keys/revisions, and one placeholder Node Agent registration. A release
overlay must replace the images and the complete immutable runtime ConfigMap;
it may not patch only the visibly convenient fields. Rendering this base is not
a deployment receipt or a Launch Receipt and does not advance any Production
Gate from `0/9 PASS`.

## Placement And Availability

Every eligible Control/Storage node must carry
`vela.ai/node-role=control-storage`. At least two distinct
`kubernetes.io/hostname` failure domains are required for the two replicas; the
required anti-affinity intentionally leaves a replica Pending rather than
co-locating both instances. The zero-unavailable rolling strategy and
`minAvailable: 1` disruption budget preserve one serving replica during a
routine release. They do not prove PostgreSQL, NATS, object-store, or ingress
availability and do not replace the N/N-1 rollback and long-running Job drain
receipts required by ADR 0013.

The application service account has no Kubernetes API RBAC and does not receive
an automounted token. The application runs as UID/GID 10001 with a read-only
root filesystem, RuntimeDefault seccomp, no added capability, bounded
CPU/memory/ephemeral-storage, and a bounded Artifact validation `emptyDir`.
Release capacity planning must prove the configured Artifact size and ffprobe
workload fit those bounds on all three Control/Storage nodes.

## Secret Contract

[`secret-contract.json`](secret-contract.json) is the machine-readable preflight
authority for every external Secret name and required key. It contains no
Secret values. Delivery must provision all listed Secrets before creating a
Pod and must source them from the approved secret manager or PKI workflow.

- `vela-control-database-urls` supplies each least-privilege PostgreSQL role DSN
  as a distinct environment key.
- `vela-control-credential-pepper` supplies only the base64 credential pepper.
- `vela-control-transport-tls` keeps Worker and Fleet server identities and
  client CAs separate from public HTTP ingress.
- `vela-control-privileged-http-tls` contains independent Finance and
  Compliance server identities and client CAs.
- `vela-control-nats-client`, `vela-control-artifact-credentials`,
  `vela-control-keyrings`, `vela-control-invoice-export`, and
  `vela-control-remediation-client-tls` carry their named workload materials.

Kubernetes Secret and ConfigMap volume keys are projected as symlinks. The
runtime's secure-file boundary deliberately rejects a path-level symlink for
Node Agent endpoints and TLS private keys. The root init container therefore
copies every file into a 4 MiB memory-backed volume, sets directories to `0700`,
files to `0600`, and transfers ownership to UID/GID 10001. The application
container mounts only those regular files and never mounts the projected source
volumes. The init image digest is part of the release and vulnerability-scan
receipt; `CAP_CHOWN` is its only added capability.

The Scheduler, Artifact Reconciler, Retention Reconciler, non-content expiry,
backup Replicator, Webhook Dispatcher, Invoice Exporter, and remediation actor
identities are derived from the immutable Pod UID. A release overlay must not
replace them with one shared static ID.

## Network Boundary

Each Service selects the same Pod set but publishes exactly one interface:

| Service | Pod port | Purpose |
| --- | ---: | --- |
| `vela-api` | 8080 | Public REST API behind Envoy Gateway TLS termination |
| `vela-worker-control` | 8443 | Worker control mTLS gRPC |
| `vela-control` | 8444 | Fleet maintenance mTLS gRPC; preserves the existing Fleet DNS contract |
| `vela-finance-reconciliation` | 8445 | Finance Reconciliation mTLS HTTPS |
| `vela-compliance` | 8446 | Compliance / Legal Hold mTLS HTTPS |

Ingress is default-denied. The repository policies then admit each port from a
different identity boundary:

- API ingress namespaces require `vela.ai/network-role=api-ingress`;
- Worker and Fleet traffic requires the exact workload label in `vela-system`;
- Finance namespaces and Pods require `vela.ai/network-role=finance` plus
  `vela.ai/client-role=finance-reconciliation`; and
- Compliance namespaces and Pods require `vela.ai/network-role=compliance`
  plus `vela.ai/client-role=legal-hold`.

Namespace labels are privileged cluster configuration and must be part of the
release evidence. Before rollout, verify that the selected CNI enforces both
`namespaceSelector` and `podSelector`, that kubelet probes can reach Pod port
8080, and that no alternate Service exposes a privileged port.

The base does not declare egress policy. PostgreSQL, NATS, OIDC, primary and
backup object storage, Webhook destinations, Invoice export, and host Node Agent
addresses are release inputs whose DNS names and IP ranges vary by environment.
The overlay must add default-deny egress plus explicit DNS and destination
allowlists after resolving that environment; a broad internet egress rule is
not an acceptable substitute.

## Live Verification

`/healthz` is the liveness path. Startup and readiness use dependency-aware
`/readyz`; they must not be changed to the static liveness response. A production
rollout still requires approved image digests, Secret/PKI rotation, real
Control/Storage placement, NetworkPolicy observation, authenticated probes for
all five interfaces, N/N-1 rollout and rollback, long-running Job drain,
database/NATS/object-store failure exercises, and the corresponding immutable
Launch Receipts.
