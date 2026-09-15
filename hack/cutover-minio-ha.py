#!/usr/bin/env python3
"""Quiesce idle MarsLab validation writers and cut over the internal S3 Service.

The original four PVCs are retained. This deliberately pauses the validation
Control/Keycloak/PostgreSQL services, not hosts or GPU workloads. A host-local
timer and finally block resume writers against whichever Service is current;
they never blindly reverse the Service after destination writes may have begun.
"""
import argparse
import datetime
import importlib.util
import json
import os
import pathlib
import time

spec = importlib.util.spec_from_file_location("migration", pathlib.Path(__file__).with_name("migrate-minio-ha.py"))
m = importlib.util.module_from_spec(spec)
spec.loader.exec_module(m)


def command(*args):
    return m.run(["kubectl", *args])


def wait_for(check, description, seconds=240):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(3)
    raise RuntimeError("timeout waiting for " + description)


def ready_pods(namespace, selector, count):
    pods = m.kube("-n", namespace, "get", "pods", "-l", selector)["items"]
    return len(pods) == count and all(
        not p["metadata"].get("deletionTimestamp") and
        any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))
        for p in pods)


def no_active_jobs(primary):
    sql = "SELECT (SELECT count(*) FROM jobs WHERE state::text NOT IN ('SUCCEEDED','FAILED','CANCELED')) + " \
          "(SELECT count(*) FROM stage_runs WHERE state::text NOT IN ('SUCCEEDED','FAILED','CANCELED'));"
    result = command("-n", "vela-system", "exec", primary, "-c", "postgres", "--",
                     "psql", "-U", "postgres", "-d", "app", "-Atc", sql)
    if result.strip() != b"0":
        raise RuntimeError("refusing validation maintenance with active jobs or stages")


def save(directory, state):
    m.write_private(directory / "cutover-state.json", state)


def resume(directory):
    state = json.loads((directory / "cutover-state.json").read_text())
    # Restore availability, not the old data routing. Post-cutover rollback
    # requires accounting for destination-only versions and writes first.
    command("-n", "vela-system", "annotate", "cluster", "vela-postgres",
            "cnpg.io/hibernation=off", "--overwrite")
    for item in state["writers"]:
        current = m.kube("-n", item["namespace"], "get", "deployment", item["name"])
        if current["metadata"]["uid"] != item["uid"]:
            raise RuntimeError("writer Deployment identity changed during maintenance")
        command("-n", item["namespace"], "scale", "deployment", item["name"],
                "--replicas=" + str(item["replicas"]))
    command("-n", "vela-system", "patch", "scheduledbackup", "vela-postgres-daily",
            "--type=merge", "-p", json.dumps({"spec": {"suspend": state["scheduled_backup_suspend"]}}))


def main(directory):
    m.preflight()
    if (directory / "cutover-state.json").exists():
        raise RuntimeError("cutover already started; inspect its state before resuming")
    receipt = json.loads((directory / "verify-receipt.json").read_text())
    if receipt["result"] != "SYNCHRONIZED_SNAPSHOT_VERIFIED":
        raise RuntimeError("initial synchronization has not been verified")
    cluster = m.kube("-n", "vela-system", "get", "cluster", "vela-postgres")
    if cluster["metadata"].get("annotations", {}).get("cnpg.io/hibernation") == "on":
        raise RuntimeError("PostgreSQL was already hibernated")
    no_active_jobs(cluster["status"]["currentPrimary"])
    if any(b.get("status", {}).get("phase") != "completed" for b in
           m.kube("-n", "vela-system", "get", "backups.postgresql.cnpg.io")["items"]):
        raise RuntimeError("PostgreSQL backup is not complete")
    if m.kube("-n", "longhorn-system", "get", "recurringjobs.longhorn.io")["items"]:
        raise RuntimeError("coordinate Longhorn recurring writers before cutover")
    if any(b.get("status", {}).get("state") != "Completed" for b in
           m.kube("-n", "longhorn-system", "get", "backups.longhorn.io")["items"]):
        raise RuntimeError("Longhorn backup is not complete")
    old = m.kube("-n", "object-store", "get", "statefulset", "minio")
    if old["spec"].get("persistentVolumeClaimRetentionPolicy") != {"whenDeleted": "Retain", "whenScaled": "Retain"}:
        raise RuntimeError("original PVC retention is not explicit")
    state = {"started": datetime.datetime.now(datetime.timezone.utc).isoformat(),
             "service_switched": False, "original_pvcs_retained": True, "writers": [],
             "scheduled_backup_suspend": m.kube("-n", "vela-system", "get", "scheduledbackup",
                                                "vela-postgres-daily")["spec"].get("suspend"),
             "timer": "vela-minio-cutover-resume-" + directory.name.lower()}
    for namespace, name in (("vela-system", "vela-control"), ("identity", "keycloak")):
        deployment = m.kube("-n", namespace, "get", "deployment", name)
        state["writers"].append({"namespace": namespace, "name": name,
                                 "uid": deployment["metadata"]["uid"], "replicas": deployment["spec"]["replicas"],
                                 "selector": ",".join(k + "=" + v for k, v in deployment["spec"]["selector"]["matchLabels"].items())})
    save(directory, state)
    m.run(["systemd-run", "--quiet", "--unit=" + state["timer"], "--on-active=900s",
           "--setenv=KUBECONFIG=/etc/rancher/rke2/rke2.yaml",
           "--setenv=PATH=/var/lib/rancher/rke2/bin:/usr/local/bin:/usr/bin:/bin",
           "/usr/bin/python3", str(pathlib.Path(__file__).resolve()), "resume", "--inventory-directory", str(directory)])
    started = time.monotonic()
    try:
        command("-n", "vela-system", "patch", "scheduledbackup", "vela-postgres-daily",
                "--type=merge", "-p", '{"spec":{"suspend":true}}')
        for item in state["writers"]:
            command("-n", item["namespace"], "scale", "deployment", item["name"], "--replicas=0")
        for item in state["writers"]:
            wait_for(lambda i=item: ready_pods(i["namespace"], i["selector"], 0), "writer shutdown")
        no_active_jobs(cluster["status"]["currentPrimary"])
        command("-n", "vela-system", "annotate", "cluster", "vela-postgres", "cnpg.io/hibernation=on", "--overwrite")
        wait_for(lambda: ready_pods("vela-system", "cnpg.io/cluster=vela-postgres", 0), "PostgreSQL hibernation")
        state["writers_paused"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        save(directory, state)
        print("Validation writers paused; source data remains online for final synchronization", flush=True)
        m.sync(directory)
        m.verify(directory)
        verified = json.loads((directory / "verify-receipt.json").read_text())
        if verified["source_additions_since_sync"] != 0 or time.monotonic() - started > 600:
            raise RuntimeError("source is not quiescent or maintenance deadline is too close")
        # Check source bytes/identities once more immediately before routing.
        objects = json.loads((directory / "sync-source-objects.json").read_text())
        if m.snapshot_changes(objects, m.copyable_inventory()) != 0:
            raise RuntimeError("late source writes detected")
        if not ready_pods("object-store", "app=minio-ha", 6):
            raise RuntimeError("destination readiness changed")
        patch = [{"op": "test", "path": "/spec/selector", "value": {"app": "minio"}},
                 {"op": "replace", "path": "/spec/selector", "value": {"app": "minio-ha"}}]
        command("-n", "object-store", "patch", "service", "minio", "--type=json", "-p", json.dumps(patch))
        state.update({"service_switched": True, "switched_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                      "verified_snapshot": verified})
        save(directory, state)
        def destination_endpoints():
            slices = m.kube("-n", "object-store", "get", "endpointslices", "-l", "kubernetes.io/service-name=minio")["items"]
            return {e.get("targetRef", {}).get("name") for s in slices for e in s.get("endpoints", [])
                    if e.get("conditions", {}).get("ready")} == {"minio-ha-" + str(i) for i in range(6)}
        wait_for(destination_endpoints, "destination Service endpoints", seconds=45)
        # Shutdown closes every original S3 connection. The retained PVCs become
        # an immutable rollback snapshot; no source object or PVC is removed.
        command("-n", "object-store", "scale", "statefulset", "minio", "--replicas=0")
        wait_for(lambda: ready_pods("object-store", "app=minio", 0), "original MinIO shutdown")
        state["original_scaled_to_zero"] = True
        save(directory, state)
    finally:
        resume(directory)
    print("S3 Service now selects six members; resuming validation applications", flush=True)
    wait_for(lambda: ready_pods("vela-system", "cnpg.io/cluster=vela-postgres", 3), "PostgreSQL recovery", seconds=420)
    for item in state["writers"]:
        wait_for(lambda i=item: ready_pods(i["namespace"], i["selector"], i["replicas"]), "writer recovery", seconds=420)
    for i in range(4):
        claim = m.kube("-n", "object-store", "get", "pvc", "data-minio-" + str(i))
        if claim["status"]["phase"] != "Bound" or claim["metadata"].get("deletionTimestamp"):
            raise RuntimeError("original PVC retention postcondition failed")
    state.update({"finished": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                  "elapsed_seconds": round(time.monotonic() - started, 2), "result": "CUTOVER_AND_WRITER_RECOVERY_PASSED"})
    save(directory, state)
    m.run(["systemctl", "stop", state["timer"] + ".timer"])
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("cutover", "resume"))
    parser.add_argument("--inventory-directory", required=True)
    args = parser.parse_args()
    directory = m.migration_directory(args.inventory_directory)
    if args.mode == "resume":
        resume(directory)
    else:
        main(directory)
