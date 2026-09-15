#!/usr/bin/env python3
"""Capacity-checked, online six-member MinIO expansion on the accepted hosts.

Run after cutover. Retain the stopped original MinIO's four two-copy PVCs.
Move one disposable scratch replica and Tempo's GPU-host replica through
Longhorn's replica rebuilding, verify replacement RW, then remove only the
superseded replica and expand six PVCs online. No PVC deletion, filesystem
shrink, host restart or model cleanup.
"""
import datetime
import argparse
import importlib.util
import json
import pathlib
import time

sp = importlib.util.spec_from_file_location("migration", pathlib.Path(__file__).with_name("migrate-minio-ha.py"))
m = importlib.util.module_from_spec(sp)
sp.loader.exec_module(m)
GIB = 2**30
ROOT = m.ROOT / "20260914T091919Z"
CEILINGS = {"llmpool01": 770, "llmpool02": 728, "marslab-gpu-01": 118}


def k(*args):
    return m.kube(*args)


def mutate(*args):
    return m.run(["kubectl", *args])


def patch(kind, name, value, namespace="longhorn-system"):
    mutate("-n", namespace, "patch", kind, name, "--type=merge", "-p", json.dumps(value))


def wait_for(check, description, seconds=300):
    until = time.monotonic() + seconds
    while time.monotonic() < until:
        if check():
            return
        time.sleep(3)
    raise RuntimeError("timed out waiting for " + description)


def replica_rows(volume_name):
    return [r for r in k("-n", "longhorn-system", "get", "replicas.longhorn.io")["items"]
            if r["spec"]["volumeName"] == volume_name]


def evict(volume, source, target, receipt):
    rows = replica_rows(volume["metadata"]["name"])
    selected = [r for r in rows if r["spec"].get("nodeID") == source and r["spec"].get("active")
                and not r["spec"].get("failedAt")]
    if len(selected) != 1:
        raise RuntimeError("eviction source identity is ambiguous")
    name = selected[0]["metadata"]["name"]
    patch("replicas.longhorn.io", name, {"spec": {"evictionRequested": True}})
    desired = volume["spec"]["numberOfReplicas"]
    def replacement_rw():
        replacements = [r for r in replica_rows(volume["metadata"]["name"]) if r["spec"].get("nodeID") == target
                        and r["spec"].get("active") and r["spec"].get("healthyAt") and not r["spec"].get("failedAt")]
        engines = [e for e in k("-n", "longhorn-system", "get", "engines.longhorn.io")["items"]
                   if e["spec"]["volumeName"] == volume["metadata"]["name"] and e["status"].get("currentState") == "running"]
        return len(replacements) == 1 and len(engines) == 1 and \
            engines[0]["status"].get("replicaModeMap", {}).get(replacements[0]["metadata"]["name"]) == "RW"
    def moved():
        current = k("-n", "longhorn-system", "get", "volumes.longhorn.io", volume["metadata"]["name"])
        rows = replica_rows(volume["metadata"]["name"])
        healthy = [r for r in rows if r["spec"].get("active") and r["spec"].get("healthyAt")
                   and not r["spec"].get("failedAt")]
        return (current["status"]["robustness"] == "healthy" and len(healthy) == desired and
                source not in {r["spec"].get("nodeID") for r in healthy} and
                target in {r["spec"].get("nodeID") for r in healthy})
    try:
        wait_for(replacement_rw, "RW replacement of " + volume["status"]["kubernetesStatus"]["pvcName"])
        current = k("-n", "longhorn-system", "get", "volumes.longhorn.io", volume["metadata"]["name"])
        if current["spec"]["numberOfReplicas"] != desired or current["status"]["robustness"] != "healthy":
            raise RuntimeError("volume policy or health changed during replica relocation")
        if any(r["metadata"]["name"] == name for r in replica_rows(volume["metadata"]["name"])):
            # Some Longhorn transitions clear evictionRequested while leaving
            # both copies RW. Remove only this identified old copy after the
            # independent engine check; never remove a volume or unverified copy.
            mutate("-n", "longhorn-system", "delete", "replicas.longhorn.io", name, "--wait=true", "--timeout=60s")
        wait_for(moved, "healthy replacement of " + volume["status"]["kubernetesStatus"]["pvcName"])
    except Exception:
        # Cancellation does not delete either data copy or force an attachment.
        if any(r["metadata"]["name"] == name for r in replica_rows(volume["metadata"]["name"])):
            patch("replicas.longhorn.io", name, {"spec": {"evictionRequested": False}})
        raise
    receipt["moves"].append({"pvc": volume["status"]["kubernetesStatus"]["pvcName"],
                              "source": source, "target": target, "healthy_copies_after": desired})
    m.write_private(ROOT / "expansion-receipt.json", receipt)
    print("Healthy replica relocation completed: " + volume["status"]["kubernetesStatus"]["pvcName"], flush=True)


def main(resume_after_inventory=False):
    if m.run(["hostname"]).strip() != b"llmpool01":
        raise RuntimeError("run on llmpool01")
    previous = None
    if (ROOT / "expansion-receipt.json").exists():
        previous = json.loads((ROOT / "expansion-receipt.json").read_text())
        if not resume_after_inventory or previous["result"] != "PAUSED_FOR_DATA_DISK_INVENTORY" or \
                len(previous["moves"]) != 1 or previous["expanded_claims"]:
            raise RuntimeError("expansion already started; inspect recorded state before retrying")
    if k("-n", "object-store", "get", "service", "minio")["spec"]["selector"] != {"app": "minio-ha"}:
        raise RuntimeError("client cutover is incomplete")
    if k("-n", "object-store", "get", "statefulset", "minio")["spec"]["replicas"] != 0:
        raise RuntimeError("original MinIO is not frozen")
    if k("-n", "object-store", "get", "pods", "-l", "app=minio")["items"]:
        raise RuntimeError("original writers might still be running")
    volumes = {v["metadata"]["name"]: v for v in k("-n", "longhorn-system", "get", "volumes.longhorn.io")["items"]}
    nodes = {n["metadata"]["name"]: n for n in k("-n", "longhorn-system", "get", "nodes.longhorn.io")["items"]
             if n["spec"].get("allowScheduling")}
    if set(nodes) != set(CEILINGS):
        raise RuntimeError("unexpected schedulable storage hosts")
    replicas = k("-n", "longhorn-system", "get", "replicas.longhorn.io")["items"]
    if any(r["spec"].get("evictionRequested") for r in replicas):
        raise RuntimeError("another replica migration is active")
    if any(v["status"]["robustness"] != "healthy" for v in volumes.values() if v["status"]["state"] == "attached"):
        raise RuntimeError("an attached volume is unhealthy")
    gpu_scratch = [v for v in volumes.values() if
                   v["status"].get("kubernetesStatus", {}).get("pvcName", "").startswith("vela-control-") and
                   v["spec"]["numberOfReplicas"] == 1 and int(v["spec"]["size"]) == 20 * GIB and
                   any(r["spec"]["volumeName"] == v["metadata"]["name"] and r["spec"].get("nodeID") == "marslab-gpu-01"
                       and r["spec"].get("active") and not r["spec"].get("failedAt") for r in replicas)]
    if len(gpu_scratch) != (1 if previous else 2):
        raise RuntimeError("scratch topology differs from reviewed capacity plan")
    if previous:
        completed = previous["moves"][0]
        matches = [v for v in volumes.values() if v["status"].get("kubernetesStatus", {}).get("pvcName") == completed["pvc"]]
        if len(matches) != 1 or matches[0]["spec"]["numberOfReplicas"] != 1 or \
                {r["spec"].get("nodeID") for r in replica_rows(matches[0]["metadata"]["name"])} != {"llmpool02"}:
            raise RuntimeError("completed scratch relocation no longer matches its receipt")
    tempo = [v for v in volumes.values() if v["status"].get("kubernetesStatus", {}).get("pvcName") == "tempo-data"]
    if len(tempo) != 1 or int(tempo[0]["spec"]["size"]) != 30 * GIB or tempo[0]["spec"]["numberOfReplicas"] != 2:
        raise RuntimeError("Tempo volume differs from reviewed plan")
    target_claims = sorted([p for p in k("-n", "object-store", "get", "pvc")["items"]
                            if p["metadata"]["name"].startswith("data-minio-ha-")], key=lambda p: p["metadata"]["name"])
    if len(target_claims) != 6 or any(p["status"]["capacity"]["storage"] != "8Gi" for p in target_claims):
        raise RuntimeError("expected six unchanged 8Gi staging claims")
    increases = {node: 0 for node in nodes}
    for p in target_claims:
        rows = [r for r in replicas if r["spec"]["volumeName"] == p["spec"]["volumeName"] and
                r["spec"].get("active") and not r["spec"].get("failedAt")]
        if len(rows) != 2 or len({r["spec"].get("nodeID") for r in rows}) != 2:
            raise RuntimeError("MinIO claim does not have two distinct healthy copies")
        for r in rows:
            increases[r["spec"]["nodeID"]] += 8 * GIB
    deltas = dict(increases)
    deltas["marslab-gpu-01"] -= (30 if previous else 50) * GIB
    deltas["llmpool01"] += 30 * GIB
    deltas["llmpool02"] += (0 if previous else 20) * GIB
    receipt = {"started": datetime.datetime.now(datetime.timezone.utc).isoformat(), "result": "PLANNED", "moves": [],
               "capacity_plan": [], "original_frozen_pvcs_retained": True, "expanded_claims": []}
    if previous:
        receipt["moves"] = previous["moves"]
        receipt["previous_plan"] = previous
        receipt["data_disk_inventory_decision"] = "CPU hosts have only system disks; GPU worker data disks are reserved for local models/cache/temp by user."
    for name, node in nodes.items():
        if len(node["spec"]["disks"]) != 1:
            raise RuntimeError("unexpected storage disk topology")
        disk_id = next(iter(node["spec"]["disks"]))
        disk = node["status"]["diskStatus"][disk_id]
        growth = 0
        for r in replicas:
            if r["spec"].get("nodeID") != name or not r["spec"].get("active") or r["spec"].get("failedAt"):
                continue
            v = volumes[r["spec"]["volumeName"]]
            pvc = v["status"].get("kubernetesStatus", {}).get("pvcName")
            if pvc in {"data-minio-" + str(i) for i in range(4)}:
                continue
            growth += max(0, int(v["spec"]["size"]) - int(v["status"].get("actualSize", 0)))
        scheduled_after = disk["storageScheduled"] + deltas[name]
        cap = CEILINGS[name] * GIB
        # Reserve another 5Gi beyond the existing 10% physical free policy even
        # if all remaining scheduling headroom is consumed. actualSize is the
        # measured Longhorn allocation, not a retention/growth stress test.
        free_at_ceiling = disk["storageAvailable"] - growth - deltas[name] - (cap - scheduled_after)
        if scheduled_after > cap or free_at_ceiling < disk["storageMaximum"] * 0.1 + 5 * GIB:
            raise RuntimeError("reviewed capacity plan no longer fits physical free-space bounds")
        receipt["capacity_plan"].append({"node": name, "disk": disk_id,
                                         "previous_reserved": node["spec"]["disks"][disk_id]["storageReserved"],
                                         "new_reserved": disk["storageMaximum"] - cap,
                                         "scheduled_after_gib": scheduled_after / GIB,
                                         "ceiling_gib": CEILINGS[name],
                                         "projected_free_at_ceiling_gib": round(free_at_ceiling / GIB, 2),
                                         "physical_minimum_gib": round(disk["storageMaximum"] * 0.1 / GIB, 2)})
    m.write_private(ROOT / "expansion-receipt.json", receipt)
    print(json.dumps(receipt["capacity_plan"], indent=2), flush=True)
    # With the ORIGINAL CPU reservation, only llmpool02 can accept this 20Gi
    # replacement: llmpool01 has <20Gi schedulable headroom. Check, then move.
    disk70 = next(iter(nodes["llmpool01"]["status"]["diskStatus"].values()))
    reserve70 = next(iter(nodes["llmpool01"]["spec"]["disks"].values()))["storageReserved"]
    if not previous and disk70["storageMaximum"] - reserve70 - disk70["storageScheduled"] >= 20 * GIB:
        raise RuntimeError("scratch replacement destination is no longer deterministic")
    if not previous:
        evict(gpu_scratch[0], "marslab-gpu-01", "llmpool02", receipt)
    for row in receipt["capacity_plan"]:
        patch("nodes.longhorn.io", row["node"], {"spec": {"disks": {row["disk"]: {"storageReserved": row["new_reserved"]}}}})
    # Tempo's second healthy copy is on llmpool02, so hard anti-affinity selects
    # llmpool01 for the replacement. Both CPU copies must be healthy first.
    if {r["spec"].get("nodeID") for r in replica_rows(tempo[0]["metadata"]["name"])} != {"llmpool02", "marslab-gpu-01"}:
        raise RuntimeError("Tempo replica topology changed")
    evict(tempo[0], "marslab-gpu-01", "llmpool01", receipt)
    for p in target_claims:
        name, vol = p["metadata"]["name"], p["spec"]["volumeName"]
        patch("pvc", name, {"spec": {"resources": {"requests": {"storage": "16Gi"}}}}, "object-store")
        def expanded():
            c = k("-n", "object-store", "get", "pvc", name)
            v = k("-n", "longhorn-system", "get", "volumes.longhorn.io", vol)
            return c["status"].get("capacity", {}).get("storage") == "16Gi" and not c["status"].get("conditions") and \
                int(v["spec"]["size"]) == 16 * GIB and v["status"]["robustness"] == "healthy"
        wait_for(expanded, "online expansion of " + name)
        receipt["expanded_claims"].append(name)
        m.write_private(ROOT / "expansion-receipt.json", receipt)
        print("Expanded and healthy: " + name, flush=True)
    receipt.update({"result": "SIX_PVCS_EXPANDED", "finished": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                    "statefulset_template_reconciliation_pending": True})
    m.write_private(ROOT / "expansion-receipt.json", receipt)
    print(json.dumps(receipt, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resume-after-disk-inventory", action="store_true")
    main(parser.parse_args().resume_after_disk_inventory)
