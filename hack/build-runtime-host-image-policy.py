#!/usr/bin/env python3
"""Derive host launch policy from operator-qualified containerd/CDI inventory.

Input is a reviewed static configuration snapshot, never a workload's OCI spec.
The installer must verify the recorded host binary and CDI digests before use.
"""
import argparse
import hashlib
import json
import re
from pathlib import Path, PurePosixPath

DRA_HOOK = '/var/lib/kubelet/plugins/gpu.nvidia.com/nvidia-cdi-hook'
DRA_IMAGE_DIGEST = 'sha256:e7f21f226f90dfc993caba2e851ce652ded6da8c18f11de4a7238f6bde1e4bc8'
DRA_HOOK_DIGEST = 'fc5d41b7b9e63e44e8d9e408629efb244e86ef58ae11219c0669ab69fc453dfc'


def gpu_edits(inventory):
    dra = inventory.get('dra')
    if dra is None:
        return inventory['edits'], '/usr/bin/nvidia-cdi-hook'
    provenance = dra['provenance']
    if (provenance['dra_image'].split('@')[-1] != DRA_IMAGE_DIGEST
            or provenance['hook_sha256'] != DRA_HOOK_DIGEST
            or inventory['binaries'][DRA_HOOK]['sha256'] != DRA_HOOK_DIGEST
            or provenance['node'] != inventory['node_identity']
            or dra['gpu_uuid'] != inventory['gpu_uuid']
            or not re.fullmatch(r'GPU-[0-9a-f-]{36}', dra['gpu_uuid'])
            or not re.fullmatch(r'[0-9a-f]{64}', provenance['helper_sha256'])
            or provenance['method'] != 'DRA v0.5.0 vendored nvcdi static NVML discovery; no workload OCI input'):
        raise ValueError('unqualified DRA inventory provenance or device identity')
    spec = dra['cdi_spec']
    if spec['kind'] != 'k8s.gpu.nvidia.com/claim' or len(spec['devices']) != 1:
        raise ValueError('DRA inventory must select exactly one claim device')
    # Common driver edits precede the independently selected GPU's edits,
    # matching the pinned CDI library. Never derive approval from a live Pod.
    common = spec['containerEdits']
    device = spec['devices'][0]['containerEdits']
    edits = {'hooks': common.get('hooks', []) + device.get('hooks', []),
             'env': common.get('env', []) + device.get('env', [])}
    return edits, DRA_HOOK


def build(inventory, resource_class):
    state = inventory["containerd_state_directory"]
    if not isinstance(state, str) or not state.startswith("/") or state == "/" or str(PurePosixPath(state)) != state or ".." in PurePosixPath(state).parts:
        raise ValueError("containerd state directory must be the qualified absolute daemon path")
    runtime = inventory["runtime"]
    options = runtime["options"]
    if runtime["runtime_type"] != "io.containerd.runc.v2" or options != {
        "BinaryName": inventory["binaries"]["/var/lib/rancher/rke2/bin/runc"]["resolved"],
        "Root": "/run/vela-runc", "SystemdCgroup": True,
    }:
        raise ValueError("runtime configuration differs from the qualified Vela handler")
    hooks = []
    environment = []
    if resource_class == "GPU":
        edits, hook_path = gpu_edits(inventory)
        for hook in edits["hooks"]:
            if hook["hookName"] != "createContainer" or hook["path"] != hook_path:
                raise ValueError("unqualified CDI hook")
            if set(hook) != {"hookName", "path", "args", "env"}:
                raise ValueError("unqualified CDI hook fields")
            # Match runtime-spec Hooks' Go JSON field order and omit absent fields.
            hooks.append({"path": hook["path"], "args": hook["args"], "env": hook["env"]})
        environment = edits["env"]
        if environment != ["NVIDIA_CTK_LIBCUDA_DIR=/usr/lib/x86_64-linux-gnu", "NVIDIA_VISIBLE_DEVICES=void"]:
            raise ValueError("unqualified CDI environment")
    elif resource_class != "CPU":
        raise ValueError("unknown resource class")
    digest = hashlib.sha256(json.dumps({"createContainer": hooks}, separators=(",", ":"), ensure_ascii=False).encode()).digest() if hooks else bytes(32)
    return {"schema_version": 1, "state_directory": state, "runtime_policy": {
        "shim_binary_path": inventory["binaries"]["/var/lib/rancher/rke2/bin/containerd-shim-runc-v2"]["resolved"],
        "runtime_binary_path": options["BinaryName"], "runtime_state_root": options["Root"],
        "systemd_cgroup": True, "hooks_digest": list(digest), "additional_environment": environment,
    }}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("inventory", type=Path)
    parser.add_argument("--resource-class", choices=("GPU", "CPU"), required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    policy = build(json.loads(args.inventory.read_text()), args.resource_class)
    args.output.write_text(json.dumps(policy, indent=2) + "\n")
