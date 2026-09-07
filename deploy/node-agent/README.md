# `vela-node-agent` host deployment contract

`vela-node-agent` runs as a host `systemd` service on the GPU node. It is the
only process in this repository that is allowed to invoke the remediation
command allowlist. It does not connect to PostgreSQL, NATS, Kubernetes, or the
customer API. The control plane calls it over mutually authenticated gRPC and
persists the authoritative operation completion after the response.

## Release bundle boundary

Production assembly must include the `node-agent` package and strict package
contract plus both exact systemd units in the canonical schema 3 release bundle:
`vela-node-agent.service` and `vela-runtime-image-maintenance.service`.
The contract binds `linux/amd64`, revision, absolute entrypoint, package digest,
and size. Bundle verification parses the unit as an exact directive allowlist
with one package-bound `ExecStart`; extra start hooks or conflicting directives
fail closed. Host configuration, capability files, PKI, hardware identity, and
live service enablement remain external and require their own release evidence.

## Runtime image maintenance service

`vela-runtime-image-maintenance.service` runs the same package binary with
`runtime-image-maintenance --config-file /etc/vela/runtime-image-maintenance.json`.
It is independently enabled and supervised. Neither unit requires, orders,
starts or stops the other; maintenance does not load the remediation environment,
contact Fleet or probe GPUs. The remediation daemon has no new containerd
dependency. Maintenance also does not require a particular containerd systemd
unit name: it retries the configured local socket independently.

Provision that JSON file as root-owned mode `0600`, under trusted directories,
with the exact five fields:

```json
{
  "schema_version": 1,
  "containerd_socket": "/run/containerd/containerd.sock",
  "node_identity": "node-1",
  "namespace": "k8s.io",
  "snapshotter": "native"
}
```

The Node identity and namespace must match those used by image observations.
The only currently qualified snapshotter is `native`. The socket must be
root-owned mode `0600` or root-group `0660`, with a root kernel peer and trusted
ancestors. Linux holds an `O_PATH` descriptor to that socket inode throughout
each observer connection; a restarted daemon cannot reuse the held inode and
inherit the old observer's identity. CRI/image observation requires Linux.
Unknown, duplicate, case-aliased, missing or null JSON fields, insecure
files and unsupported snapshotters fail before dialing. Configuration is loaded
once per process; an operator must restart the service to apply a change.

Image observation leases use protocol `v2`: the one-hour recovery deadline is
`vela.ai/runtime-image-expires`, not containerd's automatic GC expiry. A native
view records its allocation phase, Node boot ID and, once known, mount path.
The lease protects this cleanup journal until the kernel mount is absent.
The independent maintenance service is therefore required to reclaim expired
observations. A busy mount retains its view and lease across retries; a missing
activation alone is not proof of successful cleanup. Marked `v1` leases fail
closed for operator reconciliation instead of being silently migrated.

The command immediately attempts one recovery pass, closes its connection, then
waits one minute before the next pass. Every pass re-authenticates the socket,
with a 10-second dial deadline and separate 30-second recovery deadline. At most
32 expired, ownership-validated records are cleaned per pass. Full batches wait
the same interval, bounding sustained work; a large backlog has no fixed cleanup
deadline. A partial failure retains its completed count in the error and exits.
systemd retries after five seconds with start rate limiting disabled, including
when containerd is absent or restarted. This allows recovery after prolonged
runtime downtime. Persistent errors still require operator attention.

Only a fully completed pass emits `runtime_image_maintenance_completed` and its
recovered count. SIGTERM during the idle wait stops cleanly; an interrupted or
failed pass exits with an error after closing its connection. The unit runs as
root to authenticate to the host socket, with an empty capability bounding set,
`NoNewPrivileges=true`, a read-only filesystem and only `AF_UNIX`. These settings
do not restrict what containerd's privileged API can do; the command's exact
ownership checks remain the cleanup boundary.

The release bundle binds both unit files and their package entrypoints. It does
not bind or provision the host-specific JSON values, prove effective systemd
drop-ins, or enable the services. Image maintenance grants no startup, readiness,
incarnation retirement, scratch reset or execution permission. See the
[CPU maintenance evidence](../../docs/runtime-image-maintenance-evidence-2026-09-07.md)
for the tested process path and the remaining deployment boundary.
The subsequent [daemon restart evidence](../../docs/runtime-image-daemon-restart-evidence-2026-09-07.md)
covers both graceful daemon exit and `SIGKILL` with the original root/state
directories retained. It does not establish host reboot or systemd recovery.
The [volatile state reset evidence](../../docs/runtime-image-state-reset-evidence-2026-09-07.md)
also checks a fresh CPU sandbox with only containerd's persistent root retained.
The subsequent [cleanup journal evidence](../../docs/runtime-image-cleanup-journal-evidence-2026-09-07.md)
documents the real `EBUSY` counterexample, lease protocol change, and the
unresolved boundary when allocation state is lost before a mount path is recorded.

## Remediation and quota service

The same host process owns a second gRPC server on a Unix socket for XFS project
quota observations. That service is never registered on the remote mTLS
listener. Linux requires `CAP_SYS_ADMIN` to query an arbitrary project quota, so
the Worker Pod receives no such capability. The host service checks the exact
scratch root inode, block device, project ID, `PROJINHERIT`, project-quota
accounting and enforcement flags, and positive hard limit. The socket is
root-owned, belongs to the configured Worker group, has mode `0660`, and accepts
only the configured Worker UID through `SO_PEERCRED`.

Before enabling the unit, the operator must provision all of the following as
root-owned files with mode `0600` unless the host policy requires a stricter
mode:

- the Node Agent server certificate and private key;
- the controller CA bundle;
- a separate Node Agent `ClientAuth` certificate/private key and Fleet server CA;
- a pinned Fleet address and TLS server name;
- the explicit controller SPIFFE-to-actor map;
- the action allowlist JSON;
- the device/certification capability matrix JSON;
- the receipt directory, owned by the service and mode `0750`;
- the Worker scratch XFS project, exact block device, hard quota, and inherited
  project ID;
- the root-owned Worker quota socket parent directory;
- the non-root Worker UID/GID allowed to use that socket;
- a strict WorkerInstance template file and private persistent observation state
  directory; and
- the exact `nvidia-smi`, PCI sysfs, device sysfs, NVIDIA driver-version, and
  Linux boot-ID paths used to attest resident WorkerInstances.

The Agent rejects group/world-writable configuration, TLS material, endpoint
registries, and state directories. Private keys must not be group or world
accessible. JSON decoders reject unknown fields.

The action allowlist contains absolute executable paths and fixed argument
vectors. It must not contain shell commands, interpolated values, PCI sysfs
paths, or credentials. The fence command must prove that new work is stopped
and the target Worker is safe to modify. The post-check command must return
structured health evidence only after device and inference-backend validation.
An absent or invalid capability, fence, rate-limit, or post-check configuration
causes startup failure or a fail-closed operation result.

The capability matrix is keyed by canonical NVIDIA GPU UUID. Each entry binds
that UUID to one authoritative Device ID and epoch, one lowercase canonical PCI
BDF, one certification revision, an explicit failure-class set, and an explicit
L0-L5 action set. The Device authority must match the current Fleet observation;
a stale epoch fails closed. For example:

```json
{
  "GPU-00000000-0000-0000-0000-000000000001": {
    "device_id": "49440000-0000-0000-0000-000000000003",
    "device_epoch": 1,
    "certification_revision": "gpu-remediation-matrix-v1",
    "pci_bdf": "0000:41:00.0",
    "failure_classes": ["PROCESS_FAILURE"],
    "actions": ["L0_PROCESS_RESTART"]
  }
}
```

This example is structural only and is not a hardware certification. The host
helpers must discover the actual GPU UUID and PCI BDF and return both exact
values. A stale Worker epoch, unknown GPU UUID, changed BDF, unlisted failure
class, unlisted action, or revision mismatch fails before or during execution.

Every action, fence, and post-check helper receives these arguments after its
fixed configured argument vector:

```text
--vela-operation-id=<uuid>
--vela-execution-claim-id=<uuid>
--vela-worker-id=<uuid>
--vela-worker-epoch=<positive integer>
--vela-node-identity=<registered identity>
--vela-device-identity=<canonical GPU UUID>
--vela-gpu-uuid=<canonical GPU UUID>
--vela-pci-bdf=<canonical PCI BDF>
--vela-failure-class=<authoritative failure class>
--vela-action-level=<L0...L5 enum>
--vela-certification-revision=<revision>
--vela-failure-evidence-sha256=<lowercase hex>
--vela-deadline-at=<RFC3339Nano UTC>
```

Fence output is exactly one JSON object containing those identity fields,
including `gpu_uuid` and `pci_bdf`, plus
`new_assignments_stopped=true` and `target_processes_stopped=true`. Post-check
output is exactly one JSON object containing the identity fields plus
`device_healthy=true`, `inference_backend_healthy=true`, and a bounded `detail`.
Unknown, missing, mismatched, false, empty, or oversized evidence fails closed.

The Node Agent certificate URI SAN is canonical:

```text
spiffe://vela.internal/node-agent/<base64url(node_identity)>/<worker_uuid>
```

Controller URI SAN and actor mappings are canonical as
`spiffe://vela.internal/controller/<id>` and `controller/<id>`. The control-plane
endpoint registry is keyed by Node identity and has this shape:

```json
{
  "node-1": {
    "address": "10.0.0.10:9443",
    "server_name": "node-1.vela.internal",
    "worker_id": "10000000-0000-0000-0000-000000000001",
    "worker_epoch": 7,
    "spiffe_identity": "spiffe://vela.internal/node-agent/bm9kZS0x/10000000-0000-0000-0000-000000000001"
  }
}
```

The control plane uses this same registry as the inbound authorization map for
WorkerInstance observations. A certificate chaining to the configured Fleet
client CA is not sufficient: its canonical Node Agent SPIFFE URI, decoded Node
identity, and legacy Worker UUID must exactly match a current registry entry.
Registered Node Agents may call `ObserveWorkerInstance` and the scoped Worker
bootstrap claim, receipt, abandonment and read-only history methods. Direct observations
must contain a complete single-node WorkerInstance whose every Device and
WorkerMember belongs to the authenticated Node. Bootstrap checks the approved
member's Node before consuming first use and binds history to the original Agent
UUID. Fleet Controller identity remains required for ResidencyPlans, Pod
mutation authorization and cross-node aggregate observations.

## Explicit Worker bootstrap command

`vela-node-agent bootstrap --action prepare` is a separate one-shot command.
It loads no daemon configuration, probes no devices, starts no backend and
performs no remediation. `--action history --request-id <original-uuid>` reads
the authenticated Registry history without opening or changing scratch.
`--action reconcile-pair` uses the preparation configuration to restore only
missing local pair metadata when Registry already records both original journals.
`--action abandon --request-id <original-uuid>` permanently rejects completion of
an unrecorded claim and fences its still-unobserved Worker. It preserves local
state and grants no process drain, scratch cleanup or replacement permission.
All actions require explicit Fleet TLS settings and a registered Node Agent
client certificate; node and actor are derived from that same loaded certificate.

Run preparation in the provisioning context as the intended journal-owner UID,
with its preprovisioned owner-only scratch layout and private configuration.
The default root daemon does not provision these journals. Credential delivery
to this one-shot context is a deployment responsibility; ordinary serving does
not need Node Agent credentials. Do not change journal ownership or grant these
credentials to a serving Pod to bypass provisioning failures.

Schema 94 and the Control bootstrap service must be present. See the
[bootstrap runbook](../../docs/runbooks/stage-worker-scratch-retirement.md#authenticated-node-bootstrap)
for arguments and interruption handling. The systemd unit and recurring Fleet
Pod init containers do not invoke this command; activation remains gated by the
remaining containment, replacement and durable serving activation checks.

Outbound observation uses a Node Agent certificate valid for `ClientAuth`, a
pinned Fleet endpoint/TLS server name and server CA, immediate-first periodic
reporting, per-call timeout, and per-WorkerInstance bounded exponential backoff.
One failing WorkerInstance does not stop reports for another. The reporter can
attest inventory and append fresh capacity evidence. It has no model load,
unload, replace, or release authority. ModelResidency evidence is accepted only
as `READY`, and a changed runtime/device/member identity fails closed.

The local WorkerInstance file is a JSON array of static evidence templates.
Runtime fields are intentionally absent from its schema: `observed_at`,
`observed_by`, DeviceSet/member digests, Node/Agent/Device epochs, attestation
digests, health, capacity sequence, and capacity timestamps are generated at
runtime. Unknown or duplicate JSON keys, duplicate WorkerInstances, noncanonical
UUIDs, incomplete Device ownership, cross-Node membership, and non-`READY`
residency fail startup. A structural single-GPU template has this shape:

```json
[
  {
    "schema_version": 1,
    "worker_instance_id": "49440000-0000-0000-0000-000000000001",
    "instance_epoch": 1,
    "control_session_epoch": 1,
    "device_set": {
      "id": "49440000-0000-0000-0000-000000000002",
      "devices": [
        {
          "id": "49440000-0000-0000-0000-000000000003",
          "compute_node_id": "49440000-0000-0000-0000-000000000004",
          "node_identity": "node-1",
          "region": "cn-shanghai",
          "network_domain": "rack-a",
          "fault_domain": "power-a",
          "kind": "GPU",
          "gpu_uuid": "GPU-00000000-0000-0000-0000-000000000001",
          "pci_bdf": "0000:41:00.0",
          "ordinal": 0
        }
      ]
    },
    "members": [
      {
        "id": "49440000-0000-0000-0000-000000000005",
        "member_key": "dit-0",
        "compute_node_id": "49440000-0000-0000-0000-000000000004",
        "member_epoch": 1,
        "device_ids": ["49440000-0000-0000-0000-000000000003"],
        "readiness": "READY"
      }
    ],
    "residencies": [
      {
        "id": "49440000-0000-0000-0000-000000000006",
        "model_component_revision": "h3-dit-v1",
        "runtime_identity": "h3-dit-runtime-v1",
        "runtime_image_digest": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
        "model_runtime_epoch": 1,
        "state": "READY",
        "warmup_evidence_digest": "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
        "canary_evidence_digest": "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"
      }
    ],
    "capacity": {"vector": {"concurrency": 1}}
  }
]
```

The `vela-control` base permits only the non-production `192.0.2.0/32`
placeholder to TCP 8444. Every release overlay must replace that whole policy
with its CNI-observed GPU Node source CIDRs and verify the rendered policy and a
live mTLS observation together.

Rate-limit history survives Agent restarts. A durable execution intent is
published before the action; if the Agent restarts before a terminal receipt is
published, it returns `EXECUTION_OUTCOME_UNKNOWN` and does not repeat the action.

Required environment variables are documented by `cmd/vela-node-agent`:

```text
VELA_NODE_AGENT_ADDRESS
VELA_NODE_AGENT_NODE_IDENTITY
VELA_NODE_AGENT_WORKER_ID
VELA_NODE_AGENT_WORKER_EPOCH
VELA_NODE_AGENT_TLS_CERT_FILE
VELA_NODE_AGENT_TLS_KEY_FILE
VELA_NODE_AGENT_CONTROLLER_CA_FILE
VELA_NODE_AGENT_RECEIPT_DIRECTORY
VELA_NODE_AGENT_CONTROLLERS_FILE
VELA_NODE_AGENT_COMMANDS_FILE
VELA_NODE_AGENT_CAPABILITIES_FILE
VELA_NODE_AGENT_POSTCHECK_PATH
VELA_NODE_AGENT_POSTCHECK_ARGS_JSON
VELA_NODE_AGENT_FENCE_PATH
VELA_NODE_AGENT_FENCE_ARGS_JSON
VELA_NODE_AGENT_WORKER_QUOTA_SOCKET
VELA_NODE_AGENT_WORKER_UID
VELA_NODE_AGENT_WORKER_GID
VELA_NODE_AGENT_WORKER_SCRATCH_ROOT
VELA_NODE_AGENT_WORKER_XFS_DEVICE
VELA_NODE_AGENT_WORKER_XFS_PROJECT_ID
VELA_NODE_AGENT_FLEET_ADDRESS
VELA_NODE_AGENT_FLEET_SERVER_NAME
VELA_NODE_AGENT_FLEET_CA_FILE
VELA_NODE_AGENT_FLEET_CLIENT_CERT_FILE
VELA_NODE_AGENT_FLEET_CLIENT_KEY_FILE
VELA_NODE_AGENT_WORKER_INSTANCES_FILE
VELA_NODE_AGENT_WORKER_INSTANCE_STATE_DIRECTORY
VELA_NODE_AGENT_NVIDIA_SMI_PATH
VELA_NODE_AGENT_PCI_BUS_DEVICES_ROOT
VELA_NODE_AGENT_SYS_DEVICES_ROOT
VELA_NODE_AGENT_NVIDIA_DRIVER_VERSION_PATH
VELA_NODE_AGENT_BOOT_ID_PATH
```

Optional bounded timing overrides are
`VELA_NODE_AGENT_WORKER_INSTANCE_REPORT_INTERVAL`,
`VELA_NODE_AGENT_WORKER_INSTANCE_CALL_TIMEOUT`,
`VELA_NODE_AGENT_WORKER_INSTANCE_BACKOFF_INITIAL`,
`VELA_NODE_AGENT_WORKER_INSTANCE_BACKOFF_MAX`,
`VELA_NODE_AGENT_WORKER_INSTANCE_EVIDENCE_TTL`, and
`VELA_NODE_AGENT_FLEET_DIAL_TIMEOUT`. Defaults are respectively `30s`, `10s`,
`1s`, `30s`, `2m`, and `15s`.

`VELA_NODE_AGENT_WORKER_SCRATCH_ROOT` must already exist on the configured XFS
block device before the service starts. The directory is the project root for
both Worker Local Recovery State and runner outputs. A successful repository
test is not the required capacity receipt; production provisioning must record
the device identity, mount options, project ID, hard limit, observed capacity,
kernel revision, and release/configuration revisions.

The repository provides the unit template but no credentials or hardware
capability claims. A production enablement still requires a versioned GPU
remediation Launch Receipt for every supported GPU/topology/driver tuple.
