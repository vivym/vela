#!/usr/bin/env bash
set -euo pipefail
# Keep control-plane data on the accepted three-node storage group.
# This requests a safe replica migration before disabling GPU-node scheduling.
KUBECTL="${KUBECTL:-kubectl}"
NAMESPACE="${LONGHORN_NAMESPACE:-longhorn-system}"
ALLOWED="${LONGHORN_ALLOWED_NODES:-llmpool01,llmpool02,marslab-gpu-01}"
export ALLOWED
"$KUBECTL" -n "$NAMESPACE" get nodes.longhorn.io -o yaml > "${LONGHORN_BACKUP:-longhorn-nodes-before.yaml}"
"$KUBECTL" -n "$NAMESPACE" get replicas.longhorn.io -o yaml > "${LONGHORN_REPLICA_BACKUP:-longhorn-replicas-before.yaml}"
nodes_json=$(mktemp)
trap 'rm -f "$nodes_json"' EXIT
"$KUBECTL" -n "$NAMESPACE" get nodes.longhorn.io -o json > "$nodes_json"
python3 - "$nodes_json" "$KUBECTL" "$NAMESPACE" <<'PY'
import json, os, subprocess, sys
nodes_json, kubectl, namespace = sys.argv[1:]
allowed = set(os.environ["ALLOWED"].split(","))
data = json.load(open(nodes_json))
for node in data["items"]:
    name = node["metadata"]["name"]
    disks = node.get("spec", {}).get("disks", {})
    if not disks:
        continue
    enabled = name in allowed
    patch = {"spec": {
        "allowScheduling": enabled,
        "evictionRequested": not enabled,
        "disks": {k: {"allowScheduling": enabled, "evictionRequested": not enabled} for k in disks},
    }}
    subprocess.run([kubectl, "-n", namespace, "patch", "nodes.longhorn.io", name,
                    "--type=merge", "-p", json.dumps(patch)], check=True,
                   stdout=subprocess.DEVNULL)
    print(f"{name}: {'enabled' if enabled else 'eviction requested'}")
PY
