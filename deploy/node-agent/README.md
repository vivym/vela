# `vela-node-agent` host deployment contract

`vela-node-agent` runs as a host `systemd` service on the GPU node. It is the
only process in this repository that is allowed to invoke the remediation
command allowlist. It does not connect to PostgreSQL, NATS, Kubernetes, or the
customer API. The control plane calls it over mutually authenticated gRPC and
persists the authoritative operation completion after the response.

Before enabling the unit, the operator must provision all of the following as
root-owned files with mode `0600` unless the host policy requires a stricter
mode:

- the Node Agent server certificate and private key;
- the controller CA bundle;
- the explicit controller SPIFFE-to-actor map;
- the action allowlist JSON;
- the device/certification capability matrix JSON; and
- the receipt directory, owned by the service and mode `0750`.

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

Every action, fence, and post-check helper receives these arguments after its
fixed configured argument vector:

```text
--vela-operation-id=<uuid>
--vela-execution-claim-id=<uuid>
--vela-worker-id=<uuid>
--vela-worker-epoch=<positive integer>
--vela-node-identity=<registered identity>
--vela-device-identity=<registered identity>
--vela-action-level=<L0...L5 enum>
--vela-certification-revision=<revision>
--vela-failure-evidence-sha256=<lowercase hex>
--vela-deadline-at=<RFC3339Nano UTC>
```

Fence output is exactly one JSON object containing those identity fields plus
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
    "spiffe_identity": "spiffe://vela.internal/node-agent/bm9kZS0x/10000000-0000-0000-0000-000000000001"
  }
}
```

Rate-limit history survives Agent restarts. A durable execution intent is
published before the action; if the Agent restarts before a terminal receipt is
published, it returns `EXECUTION_OUTCOME_UNKNOWN` and does not repeat the action.

Required environment variables are documented by `cmd/vela-node-agent`:

```text
VELA_NODE_AGENT_ADDRESS
VELA_NODE_AGENT_NODE_IDENTITY
VELA_NODE_AGENT_WORKER_ID
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
```

The repository provides the unit template but no credentials or hardware
capability claims. A production enablement still requires a versioned GPU
remediation Launch Receipt for every supported GPU/topology/driver tuple.
