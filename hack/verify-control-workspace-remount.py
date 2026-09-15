#!/usr/bin/env python3
"""Reproduce fsGroup widening an Artifact sandbox on a disposable Longhorn PVC.

Run on a management host with kubectl access. Uses only llmpool01, a unique
namespace and 1Gi scratch volume; never mounts application claims. A successful
run observes the unsafe 02770 remount and then proves 0700 survives remount
without fsGroup. All test resources are removed in finally.
"""

import json
import subprocess
import time
import uuid


def kubectl(*args, body=None):
    result = subprocess.run(
        ["kubectl", *args],
        input=json.dumps(body) if body is not None else None,
        text=True, capture_output=True,
    )
    if result.returncode:
        raise RuntimeError(result.stderr.strip())
    return result.stdout


def main():
    namespace = "vela-workspace-drill-" + uuid.uuid4().hex[:10]
    image = "docker.io/library/busybox@sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0"
    receipt = {"namespace": namespace, "node": "llmpool01", "checks": []}

    def run(name, script, fs_group=False, root=False):
        context = {"runAsUser": 0 if root else 10001,
                   "runAsGroup": 0 if root else 10001,
                   "seccompProfile": {"type": "RuntimeDefault"}}
        if fs_group:
            context.update(fsGroup=10001, fsGroupChangePolicy="OnRootMismatch")
        pod = {"apiVersion": "v1", "kind": "Pod",
               "metadata": {"name": name, "namespace": namespace},
               "spec": {"restartPolicy": "Never", "activeDeadlineSeconds": 180,
                        "automountServiceAccountToken": False,
                        "nodeSelector": {"kubernetes.io/hostname": "llmpool01"},
                        "tolerations": [{"key": "node-role.kubernetes.io/control-plane", "operator": "Exists"}],
                        "securityContext": context,
                        "containers": [{"name": "probe", "image": image,
                                        "command": ["sh", "-ec", script],
                                        "resources": {"requests": {"cpu": "10m", "memory": "16Mi"},
                                                      "limits": {"cpu": "100m", "memory": "32Mi"}},
                                        "securityContext": {"allowPrivilegeEscalation": False,
                                                            "readOnlyRootFilesystem": True,
                                                            "capabilities": {"drop": ["ALL"], "add": ["CHOWN"] if root else []}},
                                        "volumeMounts": [{"name": "scratch", "mountPath": "/scratch"}]}],
                        "volumes": [{"name": "scratch", "persistentVolumeClaim": {"claimName": "scratch"}}]}}
        kubectl("apply", "-f", "-", body=pod)
        deadline = time.monotonic() + 180
        while time.monotonic() < deadline:
            current = json.loads(kubectl("-n", namespace, "get", "pod", name, "-o", "json"))
            phase = current["status"].get("phase")
            if phase in ("Succeeded", "Failed"):
                log = kubectl("-n", namespace, "logs", name).strip()
                receipt["checks"].append({"name": name, "phase": phase, "output": log})
                if phase != "Succeeded":
                    raise RuntimeError(name + ": " + log)
                kubectl("-n", namespace, "delete", "pod", name, "--wait=true", "--timeout=60s")
                return
            time.sleep(2)
        raise TimeoutError(name)

    kubectl("create", "namespace", namespace)
    try:
        kubectl("apply", "-f", "-", body={
            "apiVersion": "v1", "kind": "PersistentVolumeClaim",
            "metadata": {"name": "scratch", "namespace": namespace},
            "spec": {"accessModes": ["ReadWriteOnce"], "storageClassName": "longhorn-wffc",
                     "resources": {"requests": {"storage": "1Gi"}}}})
        run("seed", "mkdir -p /scratch/sandboxes; chmod 0700 /scratch /scratch/sandboxes; chown -R 10001:10001 /scratch", root=True)
        # Match the production guard: group/other writable must be rejected.
        run("reproduce-fsgroup", 'mode=$(stat -c %a /scratch/sandboxes); echo sandbox_mode=$mode; test $((0$mode & 0022)) -ne 0', fs_group=True)
        run("owner-restricts", "chmod 0700 /scratch /scratch/sandboxes; test $(stat -c %a /scratch/sandboxes) = 700")
        run("remount-without-fsgroup", 'mode=$(stat -c %a /scratch/sandboxes); echo sandbox_mode=$mode; test "$mode" = 700; touch /scratch/sandboxes/writable; rm /scratch/sandboxes/writable')
        receipt["result"] = "WORKSPACE_REMOUNT_PASS"
    finally:
        kubectl("delete", "namespace", namespace, "--wait=false")
        receipt["cleanup_requested"] = True
        print(json.dumps(receipt, indent=2), flush=True)


if __name__ == "__main__":
    main()
