# H3 Worker Deployment Contract

This Kustomize base records the production boundary for one long-running H3
Worker Pod per eligible eight-GPU node. The Fleet Controller owns the live
WorkerPool and `OnDelete` DaemonSet. It must materialize a versioned
`vela-worker-runtime` ConfigMap, the `vela-worker-control-mtls` Secret, and the
`vela-artifact-store-ca` Secret before creating a Pod. Every Pod starts behind
the `fleet.vela.ai/identity-binding` scheduling gate. After the DaemonSet
controller binds the exact target node through required node affinity, Fleet
Controller resolves the durable Worker UUID/epoch, begins the readiness cycle,
writes `vela.ai/worker-id` and `vela.ai/worker-epoch` labels, and only then removes
the gate. Worker Agent reads those labels through the Downward API; there is no
separate mutable identity Secret. The runtime ConfigMap includes an HTTPS
`artifact-store-health-url`; the Worker Agent probes it with the exact CA and a
bounded Lease-scoped request for every execution and finalization Heartbeat. It must also render
the XFS/NVMe-certified `output-cleanup-min-bytes-per-second`; startup rejects a
value for which the full per-Attempt quota plus fixed cleanup overhead cannot be
read within terminal retention. It must also render
the exact certified profile allowlist into `vela-runner-profiles` and the
node-observed UUID role binding into `vela-runner-gpu-roles`; both ConfigMaps are
immutable inputs for the Pod lifetime. Argo CD must not directly prune or patch
the live WorkerPool or DaemonSet.

Render the contract locally with:

```sh
kubectl kustomize deploy/worker-agent
```

The image digests in the base are invalid placeholders. Fleet Controller must
replace all three with approved OCI digests and preserve adjacent-version protocol
compatibility. A rendered manifest is not a deployment receipt.

## Host preflight

Every eligible node must provide all of the following before the Pod is
scheduled:

1. exactly eight certified GPUs and the `vela.ai/worker-profile=h3`,
   `vela.ai/worker-pool=launch`, and `vela.ai/h3=true:NoSchedule` contracts;
2. `/var/lib/vela/worker/scratch` as an exact, non-symlink XFS project root on
   the configured block device, with `PROJINHERIT`, project accounting and
   enforcement enabled, and a positive hard limit;
3. `recovery`, `runner-state`, and `outputs` as private, non-overlapping bounded
   subdirectories of that same project;
4. `/run/vela-node-agent/worker-quota.sock` from the root host Node Agent,
   owned by `root:10001`, mode `0660`, and accepting only Worker UID `10001`;
5. pinned model weights under `/var/lib/vela/models`; and
6. a current centrally resolved Worker UUID/epoch, mTLS identity, backend revision, exact certified
   profile allowlist, exact one-Encoder/VAE plus seven-DiT GPU UUID role map, XFS
   project ID, per-Attempt quota, and high/low/critical watermarks.
   The same certification records a conservative minimum sequential-read
   throughput for terminal output verification; `quota / throughput + 20s`
   must fit the configured terminal-retention window.

The Worker Agent and runner run as UID/GID `10001`, have no host PID/network
namespace, explicitly do not share a process namespace, have no service-account
token, no added capability, no privilege escalation, and a read-only root
filesystem. A root init container is limited to the shared Runner socket and
private scratch-view volumes and adds only `CAP_CHOWN` so it can replace the
kubelet `fsGroup` mode with UID/GID `10001` and mode `0700`; no container
receives `CAP_SYS_ADMIN`. Only the H3 runner container requests `nvidia.com/gpu: 8`; Kubernetes GPU
exclusivity does not prove CPU, host, model, or scratch certification.

The Worker Agent mounts the complete XFS scratch project. The Runner instead
mounts a private memory-backed scratch parent with only the host
`runner-state` and `outputs` directories overlaid through exact `subPath`
mounts. It has no mount of `recovery`, including the Agent-owned
`recovery/clean/quarantine` directory. This separate mount namespace is a
required unlink-safety boundary: once the Agent atomically renames a terminal
Attempt output directory into quarantine, the Runner cannot swap any inode in
that directory before deletion. `runner-state` and `outputs` must exist before
Pod creation because kubelet resolves both `subPath` sources before starting the
init container.

The runner creates its `0600` Unix socket under the owner-only
`/run/vela-runner/private` directory. The Worker Agent validates every directory
ancestor plus the socket inode, owner, and mode before dialing it. Neither
container exposes the Runner service on a TCP listener.

The runner receives `VELA_RUNNER_MAX_OUTPUT_BYTES` from the same immutable
`attempt-quota-bytes` ConfigMap key used by the Worker Agent. It rejects the
aggregate logical size of a successful manifest before hashing or persisting a
success receipt, including sparse files whose logical size exceeds their blocks.

## Lifecycle boundary

Pod readiness is not Worker readiness. Job Coordinator identity, Worker epoch,
Lease state, GPU health, backend warm-up, ModelRevision, and
ExecutionProfileRevision certification remain authoritative. Fleet Controller
must obtain a durable `DrainOperation`, wait for active Leases to end or be
fenced, and then delete the Pod. `OnDelete`, the PDB, SIGTERM handling, and the
120-second Kubernetes grace period are defense in depth, not substitutes for
the Lease protocol.

Production still requires the real H3 soak, fault-injection, GPU remediation,
release rollback, content lifecycle, and on-call Launch Receipts. This base does
not advance any Production Gate.
