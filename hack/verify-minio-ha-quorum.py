#!/usr/bin/env python3
"""Restart each host's two NEW minio-ha Pods under network isolation; test S3.

This tests member/network quorum, not physical host or Longhorn failure. It
refuses to run if the production minio Service selects the staging cluster.
No host reboot, driver action or original MinIO member is modified. Temporary
NetworkPolicies have a management-host systemd rollback timer and finally cleanup.
Recreating only the selected staging Pods closes established connections which
Calico can otherwise continue to allow after applying a NetworkPolicy. Thus this
is an unavailable-member/rejoin test, not proof of a live TCP partition or RTO.
Run on llmpool01 with root kubectl access; requires mc in original minio-0.
"""

import collections
import datetime
import hashlib
import json
import pathlib
import shlex
import subprocess
import time
import uuid


def run(args, data=None, timeout=60):
    result = subprocess.run(args, input=data, capture_output=True, timeout=timeout)
    if result.returncode:
        # Never forward CLI errors that might contain credential-bearing URLs.
        raise RuntimeError("command failed: " + " ".join(args[:5]) + "; rc=" + str(result.returncode))
    return result.stdout


def kube(*args):
    return json.loads(run(["kubectl", *args, "-o", "json"]))


def mc(*args, data=None):
    command = '''d=$(mktemp -d /tmp/vela-minio-quorum.XXXXXX)
trap 'rm -rf "$d"' EXIT
mc --config-dir "$d" alias set ha http://minio-ha.object-store.svc.cluster.local:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null
mc --config-dir "$d" ''' + " ".join(map(shlex.quote, args))
    return run(["kubectl", "-n", "object-store", "exec", "-i", "minio-0", "--", "sh", "-ec", command], data=data)


def pods():
    return kube("-n", "object-store", "get", "pods", "-l", "app=minio-ha")["items"]


def ready_names():
    return {p["metadata"]["name"] for p in pods()
            if not p["metadata"].get("deletionTimestamp") and
            any(c["type"] == "Ready" and c["status"] == "True" for c in p["status"].get("conditions", []))}


def wait_ready(expected, seconds=120):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if ready_names() == expected:
            return
        time.sleep(2)
    raise TimeoutError("Ready member set did not converge; expected=" +
                       ",".join(sorted(expected)) + "; actual=" +
                       ",".join(sorted(ready_names())))


def wait_recreated(previous_uids, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        current = {p["metadata"]["name"]: p for p in pods()}
        if all(name in current and current[name]["metadata"]["uid"] != uid and
               not current[name]["metadata"].get("deletionTimestamp")
               for name, uid in previous_uids.items()):
            return
        time.sleep(2)
    raise TimeoutError("selected staging Pod UIDs did not change")


def erasure_state():
    # admin info is not the authority during a two-peer blackhole: this
    # version's getLocalServerProperty probes peers serially (5s each),
    # exceeding ServerInfo's 10s peer-RPC deadline and reporting healthy
    # remote disks as offline. The narrow v3 collector reads objectAPI.Health.
    raw = run(["kubectl", "-n", "object-store", "exec", "minio-0", "--",
               "curl", "-fsS", "--max-time", "10",
               "http://minio-ha.object-store.svc.cluster.local:9000/minio/metrics/v3/cluster/erasure-set"])
    result = {}
    for key in ("online_drives_count", "read_quorum", "write_quorum"):
        prefix = "minio_cluster_erasure_set_" + key + "{"
        lines = [line for line in raw.decode().splitlines() if line.startswith(prefix)]
        if len(lines) != 1 or 'pool_id="0"' not in lines[0] or 'set_id="0"' not in lines[0]:
            raise RuntimeError("missing or unexpected erasure-set telemetry: " + key)
        result[key] = int(float(lines[0].rsplit(" ", 1)[1]))
    if result["read_quorum"] != 3 or result["write_quorum"] != 4:
        raise RuntimeError("erasure-set quorum differs from six-member EC:3")
    return result


def wait_online(expected, seconds=60):
    deadline = time.monotonic() + seconds
    actual = None
    previous = None
    while time.monotonic() < deadline:
        actual = erasure_state()["online_drives_count"]
        if actual != previous:
            print("MinIO erasure-set online drives=" + str(actual), flush=True)
            previous = actual
        if actual == expected:
            return actual
        time.sleep(2)
    raise TimeoutError("online drives did not converge; expected=" + str(expected) +
                       "; actual=" + str(actual))


def checkpoint(receipt):
    directory = pathlib.Path("/opt/vela-cluster/observability-reconcile")
    # Preserve each case before later cleanup can fail. Keep a per-drill file
    # as well as the latest pointer file; never lose earlier successful cases.
    content = json.dumps(receipt, indent=2)
    for name in ["minio-ha-quorum-" + receipt["drill_id"] + ".json",
                 "minio-ha-quorum-receipt.json"]:
        pending = directory / (name + ".tmp")
        pending.write_text(content)
        pending.replace(directory / name)


def remove_test_bucket(bucket):
    # A final rb can race healing after a member rejoins. Retry only this
    # runner's unique synthetic bucket, and verify its absence independently.
    if not bucket.startswith("vela-ha-drill-"):
        raise RuntimeError("refusing cleanup outside the drill namespace")
    for attempt in range(5):
        buckets = [json.loads(line).get("key", "").rstrip("/")
                   for line in mc("ls", "ha", "--json").splitlines()]
        if bucket not in buckets:
            return
        try:
            mc("rb", "--force", "ha/" + bucket)
        except RuntimeError:
            pass
        time.sleep(min(2 ** attempt, 8))
    buckets = [json.loads(line).get("key", "").rstrip("/")
               for line in mc("ls", "ha", "--json").splitlines()]
    if bucket in buckets:
        raise RuntimeError("synthetic bucket cleanup did not converge")


def main():
    if run(["hostname"]).decode().strip() != "llmpool01":
        raise RuntimeError("run the rollback-protected drill on llmpool01")
    original = kube("-n", "object-store", "get", "service", "minio")
    if original["spec"]["selector"] != {"app": "minio"}:
        raise RuntimeError("staging cluster is no longer isolated from production clients")
    groups = collections.defaultdict(list)
    for pod in pods():
        groups[pod["spec"]["nodeName"]].append(pod["metadata"]["name"])
    if set(groups) != {"llmpool01", "llmpool02", "marslab-gpu-01"} or any(len(v) != 2 for v in groups.values()):
        raise RuntimeError("expected exactly two new members per accepted host")
    all_names = {name for names in groups.values() for name in names}
    wait_ready(all_names)
    info = json.loads(mc("admin", "info", "ha", "--json"))["info"]
    backend = info["backend"]
    if backend["onlineDisks"] != 6 or backend["standardSCParity"] != 3 or backend["rrSCParity"] != 3:
        raise RuntimeError("live MinIO erasure policy does not match six-member EC:3")
    # Replica-count evidence is separate from the member-isolation experiment.
    claims = kube("-n", "object-store", "get", "pvc")["items"]
    volumes = []
    for claim in claims:
        if not claim["metadata"]["name"].startswith("data-minio-ha-"):
            continue
        volume = kube("-n", "longhorn-system", "get", "volumes.longhorn.io", claim["spec"]["volumeName"])
        if volume["spec"]["numberOfReplicas"] != 2 or volume["status"]["robustness"] != "healthy":
            raise RuntimeError("staging volume has not reached two-copy durability")
        volumes.append({"claim": claim["metadata"]["name"], "replicas": 2, "robustness": "healthy"})
    if len(volumes) != 6:
        raise RuntimeError("missing staging volume")
    receipt = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "scope": "two staging MinIO Pods recreated under network isolation per host; no host or volume failure",
               "backend": backend, "volumes": volumes, "cases": []}
    bucket = "vela-ha-drill-" + uuid.uuid4().hex[:12]
    receipt.update({"drill_id": bucket, "result": "RUNNING", "test_bucket_removed": False})
    mc("mb", "ha/" + bucket)
    checkpoint(receipt)
    payload = ("VELA_QUORUM_DRILL_" + uuid.uuid4().hex).encode()
    try:
        mc("pipe", "ha/" + bucket + "/before", data=payload)
        for node, names in sorted(groups.items()):
            wait_ready(all_names)
            name = "vela-minio-drill-" + uuid.uuid4().hex[:10]
            policy = {"apiVersion": "networking.k8s.io/v1", "kind": "NetworkPolicy",
                      "metadata": {"name": name, "namespace": "object-store"},
                      "spec": {"podSelector": {"matchLabels": {"app": "minio-ha"},
                                               "matchExpressions": [{"key": "statefulset.kubernetes.io/pod-name",
                                                                     "operator": "In", "values": names}]},
                               "policyTypes": ["Ingress", "Egress"], "ingress": [], "egress": []}}
            run(["systemd-run", "--quiet", "--unit=" + name, "--on-active=300s",
                 "/var/lib/rancher/rke2/bin/kubectl", "--kubeconfig=/etc/rancher/rke2/rke2.yaml",
                 "-n", "object-store", "delete", "networkpolicy", name, "--ignore-not-found=true"])
            start = time.monotonic()
            print("Isolating staging MinIO members on " + node, flush=True)
            try:
                run(["kubectl", "apply", "-f", "-"], data=json.dumps(policy).encode())
                # Do not infer isolation from Ready alone: established MinIO
                # peer connections survived the policy in the original red run.
                # End only these disposable staging processes, keeping their
                # PVCs and the original four-member cluster untouched.
                previous_uids = {}
                for selected in names:
                    current = kube("-n", "object-store", "get", "pod", selected)
                    owners = current["metadata"].get("ownerReferences", [])
                    if current["metadata"]["labels"].get("app") != "minio-ha" or not any(
                            o["kind"] == "StatefulSet" and o["name"] == "minio-ha" and
                            o.get("controller") for o in owners):
                        raise RuntimeError("refusing to recreate a non-staging Pod")
                    previous_uids[selected] = current["metadata"]["uid"]
                run(["kubectl", "-n", "object-store", "delete", "pods", *names, "--wait=false"])
                wait_recreated(previous_uids)
                wait_ready(all_names - set(names), seconds=180)
                convergence = time.monotonic() - start
                online = wait_online(4)
                write_start = time.monotonic()
                for index in range(3):
                    key = "ha/" + bucket + "/during-" + node + "-" + str(index)
                    body = payload + b"-" + str(index).encode()
                    mc("pipe", key, data=body)
                    if mc("cat", key) != body or mc("cat", "ha/" + bucket + "/before") != payload:
                        raise RuntimeError("S3 readback differs during the member outage")
                if time.monotonic() - start >= 290 or ready_names() != all_names - set(names):
                    raise RuntimeError("member outage was not maintained through S3 verification")
                # The automatic rollback must not turn a slow test into a
                # false pass by restoring six members midway through writes.
                kube("-n", "object-store", "get", "networkpolicy", name)
                if erasure_state()["online_drives_count"] != 4:
                    raise RuntimeError("four-drive outage no longer present after S3 verification")
                receipt["cases"].append({"host": node, "isolated_members": names, "online_drives": online,
                                          "readiness_convergence_seconds": round(convergence, 2),
                                          "s3_verification_seconds": round(time.monotonic() - write_start, 2),
                                          "established_connections_closed_by": "staging Pod recreation",
                                          "quorum_telemetry": "v3/cluster/erasure-set, read=3/write=4",
                                          "writes_verified": 3, "existing_object_readbacks": 3,
                                          "payload_sha256": hashlib.sha256(payload).hexdigest()})
                checkpoint(receipt)
                print(node + ": S3 READ_WRITE_PASS with four members", flush=True)
            finally:
                run(["kubectl", "-n", "object-store", "delete", "networkpolicy", name, "--ignore-not-found=true"])
                run(["systemctl", "stop", name + ".timer"])
                wait_ready(all_names)
    except Exception as error:
        receipt.update({"result": "FAILED", "failure_type": type(error).__name__})
        checkpoint(receipt)
        raise
    finally:
        try:
            remove_test_bucket(bucket)
            receipt["test_bucket_removed"] = True
        except Exception as error:
            receipt.update({"result": "CLEANUP_FAILED", "cleanup_failure_type": type(error).__name__})
            raise
        finally:
            checkpoint(receipt)
    receipt["result"] = "MINIO_HA_QUORUM_PASS"
    receipt["test_bucket_removed"] = True
    receipt["temporary_policies_removed"] = True
    checkpoint(receipt)
    print(json.dumps(receipt, indent=2), flush=True)


if __name__ == "__main__":
    main()
