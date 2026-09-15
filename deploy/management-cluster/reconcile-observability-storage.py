#!/usr/bin/env python3
"""Plan or restore two Loki replicas on the two CPU management disks.

Run with the management kubeconfig. Default is read-only; --apply changes
reservations and placement. --remove-shared-replica additionally uses the
Longhorn replicaRemove API only after both CPU nodes have healthy RW copies.
It never deletes a PVC, volume, snapshot, or any unrelated replica.
"""
import argparse
import json
import os
import subprocess
import urllib.request

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--apply", action="store_true")
parser.add_argument("--remove-shared-replica", action="store_true")
args = parser.parse_args()
if args.remove_shared_replica and not args.apply:
    parser.error("--remove-shared-replica requires --apply")
kube = os.environ.get("KUBECTL", "kubectl")


def get(kind, name=None, namespace="longhorn-system"):
    return json.loads(subprocess.check_output(
        [kube, "-n", namespace, "get", kind, *([name] if name else []), "-o", "json"]))


def patch(kind, name, spec):
    subprocess.run([kube, "-n", "longhorn-system", "patch", kind, name,
                    "--type=merge", "-p", json.dumps({"spec": spec})], check=True)


pvc = get("pvc", "loki-data", "monitoring")
volume_name = pvc["spec"]["volumeName"]
volume = get("volumes.longhorn.io", volume_name)
size = int(volume["spec"]["size"])
plans = []
for name in ("llmpool01", "llmpool02"):
    node = get("nodes.longhorn.io", name)
    if not node["spec"]["allowScheduling"]:
        raise SystemExit(f"{name}: storage scheduling is disabled")
    disks = node["spec"]["disks"]
    if len(disks) != 1:
        raise SystemExit(f"{name}: requires an explicit plan for multiple disks")
    disk, spec = next(iter(disks.items()))
    status = node["status"]["diskStatus"][disk]
    maximum = status["storageMaximum"]
    reserved = maximum // 5  # 20% for OS, registry, and rebuild headroom.
    has_replica = any(r.startswith(volume_name + "-r-")
                      for r in status.get("scheduledReplica", {}))
    projected = status["storageScheduled"] + (0 if has_replica else size)
    if projected > maximum - reserved or status["storageAvailable"] - size < reserved:
        raise SystemExit(f"{name}: insufficient reserved or physical headroom")
    if not spec["allowScheduling"] or spec["evictionRequested"]:
        raise SystemExit(f"{name}: disk is not eligible")
    print(json.dumps({"node": name, "old_reserved": spec["storageReserved"],
                      "new_reserved": reserved, "projected_scheduled": projected,
                      "physical_available": status["storageAvailable"]}))
    plans.append((name, {"tags": sorted(set(node["spec"].get("tags", [])) |
                                      {"observability-cpu"}),
                         "disks": {disk: {"storageReserved": reserved}}}))

if args.apply:
    for name, spec in plans:
        patch("nodes.longhorn.io", name, spec)
    patch("volumes.longhorn.io", volume_name,
          {"numberOfReplicas": 2, "nodeSelector": ["observability-cpu"]})
    print("Requested two replicas; verify engine replicaModeMap has two RW entries.")

if args.remove_shared_replica:
    service = get("service", "longhorn-backend")
    base = "http://" + service["spec"]["clusterIP"] + ":9500/v1/volumes/" + volume_name
    with urllib.request.urlopen(base, timeout=10) as response:
        state = json.load(response)
    copies = state["replicas"]
    cpu_rw = {r["hostId"] for r in copies
              if r["mode"] == "RW" and r["running"] and not r["failedAt"]
              and r["hostId"] in {"llmpool01", "llmpool02"}}
    if cpu_rw != {"llmpool01", "llmpool02"} or state["robustness"] != "healthy":
        raise SystemExit("Both CPU copies must be healthy RW before removing the extra shared copy")
    extra = [r for r in copies if r["hostId"] == "marslab-gpu-01"]
    if len(extra) > 1 or state["numberOfReplicas"] != 2:
        raise SystemExit("Unexpected replica topology; inspect before removal")
    if extra:
        request = urllib.request.Request(
            base + "?action=replicaRemove",
            data=json.dumps({"name": extra[0]["name"]}).encode(),
            headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(request, timeout=30) as response:
            response.read()
        print("Removed extra shared-node copy through Longhorn after verifying both CPU copies")
