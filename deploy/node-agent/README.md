# `vela-node-agent` host deployment contract

`vela-node-agent` runs as a host `systemd` service on the GPU node. It is the
only process in this repository that is allowed to invoke the remediation
command allowlist. It does not connect to PostgreSQL, NATS, Kubernetes, or the
customer API. The control plane calls it over mutually authenticated gRPC and
persists the authoritative operation completion after the response.

## Release bundle boundary

Production assembly must include the `node-agent` package and strict package
contract plus the exact systemd unit in the canonical Slice 40 release bundle.
The contract binds `linux/amd64`, revision, absolute entrypoint, package digest,
and size. Bundle verification parses the unit as an exact directive allowlist
with one package-bound `ExecStart`; extra start hooks or conflicting directives
fail closed. Host configuration, capability files, PKI, hardware identity, and
live service enablement remain external and require their own release evidence.

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
Registered Node Agents may call only `ObserveWorkerInstance`, and direct
reports must contain a complete single-node WorkerInstance whose every Device
and WorkerMember belongs to the authenticated Node. Fleet Controller identity
remains required for every mutation, ResidencyPlan, and cross-node aggregate
operation.

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
