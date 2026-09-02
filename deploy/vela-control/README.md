# `vela-control` Deployment Contract

This Kustomize base records the production boundary for the modular
`vela-control` runtime. It runs two identical replicas on distinct
Control/Storage nodes and exposes six single-purpose ClusterIP Services. Render
the base locally with:

```sh
kubectl kustomize deploy/vela-control
```

The repository base is deliberately not deployable as production
configuration. Both container images use an invalid all-zero digest.
`vela-control-runtime-r0-placeholder` contains `.invalid` endpoints and
placeholder public keys/revisions, while
`vela-control-node-agents-r0-placeholder` contains one placeholder Node Agent
registration. A release overlay must replace both complete immutable
ConfigMaps, every `r0-placeholder` Secret name, the Pod template release label,
the Artifact scratch StorageClass, and both images as one release revision; it
may not patch only the visibly convenient fields. Rendering this base is not a
deployment receipt or a Launch Receipt and does not advance any Production Gate
from `0/9 PASS`.

The Node Agent registry is shared by outbound Remediation dispatch and inbound
WorkerInstance evidence authorization. The Fleet mTLS client CA establishes
certificate-chain trust only; `vela-control` additionally requires the exact
registered Node identity, legacy Worker UUID, and canonical Node Agent SPIFFE
URI before accepting `ObserveWorkerInstance`. Other Fleet RPCs remain limited
to the configured Fleet Controller identity. The Fleet client CA file must be
an explicit bundle of the approved Fleet Controller client issuer and host Node
Agent client issuer; adding an issuer does not register or authorize any
certificate by itself. The repository base keeps the exact Fleet Controller Pod
selector and adds a separate host-ingress placeholder restricted to the
documentation-only `192.0.2.0/32` address on TCP 8444. That CIDR is deliberately
unusable as a production Node source. A release overlay must replace the entire
`vela-control-allow-node-agent-placeholder` resource with the measured CNI view
of the exact GPU Node source CIDRs; it may not broaden the Fleet Controller
selector, use `0.0.0.0/0`, or enable any other port.

## Release bundle boundary

Production assembly must include the final `kubectl kustomize` output as the
exact `vela-control` render in the canonical Slice 40 release bundle. The bundle
binds every pinned workload/init image plus the exact versioned ConfigMap and
Secret references, required Secret keys, consumer identities, and external
material revisions; it never embeds Secret values. Any overlay, image, Secret
name/revision, or rendered-resource change derives a new configuration revision
and release digest. Bundle verification does not prove real PKI issuance,
credential rotation, storage placement, CNI enforcement, or deployment health.

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
CPU/memory/ephemeral storage, and the non-preempting `vela-control-critical`
PriorityClass. Each Pod requests a 110 GiB generic ephemeral PVC for Artifact
validation. The release must replace
`vela-control-artifact-scratch-placeholder` with a StorageClass that reserves
capacity and enforces I/O isolation from PostgreSQL, etcd, JetStream, and object
storage. [`storage-contract.json`](storage-contract.json) is the machine-readable
preflight for `WaitForFirstConsumer`, delete reclaim, a dedicated capacity pool,
IOPS and throughput limits, and a live verification receipt. Capacity planning
must reserve three active 110 GiB claims for the one-surge rollout plus at least
one additional claim for a terminating Pod and delayed PVC/PV reclamation: four
claims and 440 GiB in aggregate. The release must prove that reclamation stays
within that headroom, or reserve additional capacity, and prove the Artifact
size and ffprobe workload fit on all three Control/Storage nodes.
Provider-specific StorageClass parameters and their live effect remain release
evidence; the repository placeholder does not claim that isolation already
exists.

## Secret Contract

[`secret-contract.json`](secret-contract.json) is the machine-readable preflight
authority for every external Secret name, required key, projected path, and
materialized regular-file path. It contains no Secret values. Its rotation
policy requires immutable Secrets and rejects in-place update as a supported
runtime mechanism. Delivery must provision all listed Secrets before creating a
Pod and must source them from the approved secret manager or PKI workflow.

- `vela-control-database-urls-<release>` supplies each least-privilege
  PostgreSQL role DSN as a distinct environment key.
- `vela-control-credential-pepper-<release>` supplies only the base64
  credential pepper.
- `vela-control-transport-tls-<release>` keeps Worker, Stage Worker, and Fleet
  server identities and client CAs separate from public HTTP ingress.
- `vela-control-stage-worker-identity-<release>` supplies the assignment
  identity HMAC key. Its Secret name rolls with the release, but its key bytes
  must remain stable across N/N-1 replay and rollback windows.
- `vela-control-h3-exact-cache-keyring-<release>` supplies the Project-scoped
  HMAC keys used to derive non-exportable Encoder and DiT exact-cache keys. The
  JSON object is bounded to 4096 canonical Project UUIDs and 1 MiB; every value
  must be base64-encoded key material of at least 32 bytes. Cache enablement is
  fail-closed: a missing, malformed, or incomplete keyring prevents startup.
- `vela-control-privileged-http-tls-<release>` contains independent Finance and
  Compliance server identities and client CAs.
- The remaining names in `secret-contract.json` are also suffixed by the exact
  release revision and carry their named workload materials.

Kubernetes Secret and ConfigMap volume keys are projected as symlinks. The
runtime's secure-file boundary deliberately rejects a path-level symlink for
Node Agent endpoints and TLS private keys. The root init container therefore
copies each declared key, never a projected directory, into a 4 MiB
memory-backed volume, sets directories to `0700`, files to `0600`, and transfers
ownership to UID/GID 10001. The application container mounts only those regular
files and never mounts the projected source volumes. The init image is pinned to
the shared BusyBox `1.37.0` `linux/amd64` OCI manifest. Its exact manifest/config
bytes and required supply-chain evidence remain part of the release;
`CAP_CHOWN` is its only added capability.

The Scheduler, StageScheduler, Artifact Reconciler, Retention Reconciler,
non-content expiry, backup Replicator, Webhook Dispatcher, and Invoice Exporter
claimant identities are derived from the immutable Pod UID. A release overlay
must not replace them with one shared static ID. Remediation is deliberately
different: both replicas use `controller/vela-control`, and the shared client
certificate must contain the exact URI
`spiffe://vela.internal/controller/vela-control`.

Every Secret and ConfigMap reference in the Pod template is release-versioned.
Certificate and credential rotation must provision new immutable Secret names,
wait for cert-manager or the approved issuer to populate them, then roll the
Deployment to the new Pod template. The prior Secrets and ConfigMaps remain
until the prior ReplicaSet is fully retired. Updating a referenced Secret in
place is unsupported because the init container materializes regular files once
at startup.

The release overlay must also replace the H3 exact-cache input-canonicalization
UUID and certified seed/RNG revision. `expected_saved_compute_minor` and
`carry_cost_minor` are separate policy inputs and must be calibrated
independently; the repository placeholder sets both to zero and makes no cost or
benefit claim. Changing any key, revision, or cost input requires a new complete
immutable ConfigMap/Secret revision and a rolling update.

## Network Boundary

Each Service selects the same Pod set but publishes exactly one interface:

| Service | Pod port | Purpose |
| --- | ---: | --- |
| `vela-api` | 8080 | Public REST API behind Envoy Gateway TLS termination |
| `vela-worker-control` | 8443 | Worker control mTLS gRPC |
| `vela-control` | 8444 | Fleet maintenance mTLS gRPC; preserves the existing Fleet DNS contract |
| `vela-finance-reconciliation` | 8445 | Finance Reconciliation mTLS HTTPS |
| `vela-compliance` | 8446 | Compliance / Legal Hold mTLS HTTPS |
| `vela-stage-worker-control` | 8447 | Stage Worker execution control mTLS gRPC |

Ingress is default-denied. The repository policies then admit each port from a
different identity boundary:

- API ingress namespaces require `vela.ai/network-role=api-ingress`, and the
  gateway Pod requires `vela.ai/client-role=api-gateway`;
- Worker, Stage Worker, and Fleet traffic each require their exact workload
  label in `vela-system` on separate ports;
- Finance namespaces and Pods require `vela.ai/network-role=finance` plus
  `vela.ai/client-role=finance-reconciliation`; and
- Compliance namespaces and Pods require `vela.ai/network-role=compliance`
  plus `vela.ai/client-role=legal-hold`; and
- the PodMonitor scraper requires `vela.ai/network-role=observability` plus
  `vela.ai/client-role=otel-collector`, and can reach only management port 8081.

Namespace labels are privileged cluster configuration and must be part of the
release evidence. Before rollout, verify that the selected CNI enforces both
`namespaceSelector` and `podSelector`, that kubelet probes can reach Pod-private
port 8081, and that no Service exposes the management or privileged ports beyond
their declared boundary.

The base does not declare egress policy. PostgreSQL, NATS, OIDC, primary and
backup object storage, Webhook destinations, Invoice export, and host Node Agent
addresses are release inputs whose DNS names and IP ranges vary by environment.
The overlay must add default-deny egress plus explicit DNS and destination
allowlists after resolving that environment; a broad internet egress rule is
not an acceptable substitute.

## Live Verification

`/healthz` is the liveness path on Pod-private management port 8081. Startup and
readiness use dependency-aware `/readyz` on the same listener; neither path is
registered by the public API server or exposed through a Service. A production
scraper reads `/metrics` on that listener through the exact NetworkPolicy
identity above; metrics never use Organization, Project, Job, Attempt or other
customer identifiers as labels. The deployable rule/dashboard contract lives
under `deploy/observability`.

A production rollout still requires approved Vela image digests, complete
BusyBox and Vela supply-chain evidence, Secret/PKI rotation, real Control/Storage
placement, NetworkPolicy observation, authenticated probes for all six
interfaces, N/N-1 rollout and rollback, long-running Job drain,
database/NATS/object-store failure exercises, and the corresponding immutable
Launch Receipts.
