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

The action allowlist contains absolute executable paths and fixed argument
vectors. It must not contain shell commands, interpolated values, PCI sysfs
paths, or credentials. The fence command must prove that new work is stopped
and the target Worker is safe to modify. The post-check command must return
non-empty health evidence only after device and inference-backend validation.
An absent or invalid capability, fence, rate-limit, or post-check configuration
causes startup failure or a fail-closed operation result.

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
