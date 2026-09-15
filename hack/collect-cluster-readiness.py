#!/usr/bin/env python3
"""Read-only, secret-free cluster acceptance snapshot; does not claim release readiness.

Run on a control host with kubectl access and ClusterIP routing. JSON goes to
stdout. This captures measured state, separately from documented drill receipts.
"""

import collections
import base64
import datetime
import http.client
import json
import ssl
import subprocess
import urllib.error
import urllib.parse
import urllib.request


def kube(*args):
    return json.loads(subprocess.check_output(["kubectl", *args, "-o", "json"]))


def api(url):
    with urllib.request.urlopen(url, timeout=15) as response:
        return json.load(response)


def main():
    data = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "scope": "live infrastructure snapshot, not a production release receipt"}
    nodes = kube("get", "nodes")["items"]
    data["nodes"] = {"total": len(nodes), "ready": 0, "allocatable_gpus": 0, "exceptions": [], "pressured": []}
    for node in nodes:
        ready = any(c["type"] == "Ready" and c["status"] == "True" for c in node["status"]["conditions"])
        data["nodes"]["ready"] += int(ready)
        pressures = [c["type"] for c in node["status"]["conditions"]
                     if c["type"] in ("DiskPressure", "MemoryPressure", "PIDPressure") and c["status"] == "True"]
        if pressures:
            data["nodes"]["pressured"].append({"node": node["metadata"]["name"], "conditions": pressures})
        if ready:
            data["nodes"]["allocatable_gpus"] += int(node["status"]["allocatable"].get("nvidia.com/gpu", 0))
        if not ready or node["spec"].get("unschedulable"):
            data["nodes"]["exceptions"].append({"node": node["metadata"]["name"], "ready": ready,
                                                 "cordoned": node["spec"].get("unschedulable", False)})
    data["workloads"] = []
    for item in kube("get", "deployments,statefulsets,daemonsets", "-A")["items"]:
        if item["metadata"]["namespace"] not in ("monitoring", "vela-system", "object-store", "identity", "apisix", "argocd", "kube-system"):
            continue
        status, spec = item["status"], item["spec"]
        if item["kind"] == "DaemonSet":
            desired, ready, updated = status.get("desiredNumberScheduled", 0), status.get("numberReady", 0), status.get("updatedNumberScheduled", 0)
        else:
            desired, ready, updated = spec.get("replicas", 1), status.get("readyReplicas", 0), status.get("updatedReplicas", 0)
        data["workloads"].append({"namespace": item["metadata"]["namespace"], "kind": item["kind"],
                                  "name": item["metadata"]["name"], "desired": desired, "ready": ready, "updated": updated})
    control = kube("-n", "vela-system", "get", "pods", "-l", "app.kubernetes.io/name=vela-control")["items"]
    data["vela_control"] = [{"pod": p["metadata"]["name"], "node": p["spec"]["nodeName"],
                             "fsGroup": p["spec"].get("securityContext", {}).get("fsGroup"),
                             "containers": [{"name": c["name"], "ready": c["ready"],
                                             "restarts": c["restartCount"], "reported_image": c["image"], "imageID": c.get("imageID"),
                                             "requested_image": next(spec["image"] for spec in p["spec"]["containers"] if spec["name"] == c["name"])}
                                            for c in p["status"].get("containerStatuses", [])]}
                            for p in control if not p["metadata"].get("deletionTimestamp")]
    data["postgresql"] = [{"namespace": c["metadata"]["namespace"], "name": c["metadata"]["name"],
                           "desired": c["spec"]["instances"], "ready": c.get("status", {}).get("readyInstances", 0),
                           "phase": c.get("status", {}).get("phase"),
                           "primary": c.get("status", {}).get("currentPrimary")}
                          for c in kube("get", "clusters.postgresql.cnpg.io", "-A")["items"]]
    prom = kube("-n", "monitoring", "get", "service", "monitoring-kube-prometheus-prometheus")["spec"]["clusterIP"]
    queries = [
        'sum by(job) (up{job=~"kube-proxy|kube-scheduler|kube-controller-manager|etcd|node-local-metrics|alloy|apisix-metrics|loki|tempo|longhorn-backend|monitoring/cnpg-postgres|nats-prometheus-exporter|minio|minio-quorum|nvidia-smi|legacy-hardware-federation"})',
        'sum by(endpoint) (up{job="otel-collector"})',
        'count(kube_customresource_longhorn_volume_replicas)',
        'sum(otelcol_exporter_sent_spans_total)',
        'sum(otelcol_exporter_send_failed_spans_total)',
        'sum(otelcol_receiver_refused_spans_total)',
        'sum(loki_write_dropped_entries_total)',
        'sum(loki_write_batch_retries_total)',
        'count(count by(instance) (ipmi_up))',
        'count(count by(instance) (ipmi_up==0))',
        'count by(alertname) (ALERTS{alertstate="firing"})',
    ]
    data["prometheus"] = {query: api("http://" + prom + ":9090/api/v1/query?query=" + urllib.parse.quote(query))["data"]["result"] for query in queries}
    volumes = kube("-n", "longhorn-system", "get", "volumes.longhorn.io")["items"]
    data["longhorn"] = [{"volume": v["metadata"]["name"], "replicas": v["spec"]["numberOfReplicas"],
                         "robustness": v["status"]["robustness"], "state": v["status"]["state"],
                         "namespace": v["status"].get("kubernetesStatus", {}).get("namespace"),
                         "claim": v["status"].get("kubernetesStatus", {}).get("pvcName")}
                        for v in volumes]
    minio_service = kube("-n", "object-store", "get", "service", "minio")["spec"]
    minio = minio_service["clusterIP"]
    selector = ",".join(key + "=" + value for key, value in minio_service["selector"].items())
    members = [{"pod": p["metadata"]["name"], "node": p["spec"].get("nodeName"),
                "phase": p["status"]["phase"],
                "ready": not p["metadata"].get("deletionTimestamp") and
                         any(c["type"] == "Ready" and c["status"] == "True"
                             for c in p["status"].get("conditions", []))}
               for p in kube("-n", "object-store", "get", "pods", "-l", selector)["items"]]
    data["minio"] = {"health": {}, "client_selector": minio_service["selector"], "members": members,
                     "members_per_host": dict(collections.Counter(p["node"] for p in members if p["node"])),
                     "ready_members_per_host": dict(collections.Counter(p["node"] for p in members if p["ready"]))}
    for name, path, header in [("write", "/minio/health/cluster", "X-Minio-Write-Quorum"),
                               ("read", "/minio/health/cluster/read", "X-Minio-Read-Quorum")]:
        try:
            response = urllib.request.urlopen("http://" + minio + ":9000" + path, timeout=10)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            data["minio"]["health"][name] = {"http_status": response.status, "quorum": response.headers.get(header)}
    data["longhorn_metrics_policy_namespaces"] = [p["metadata"]["namespace"] for p in kube("get", "networkpolicies", "-A")["items"]
                                                  if p["metadata"]["name"] == "longhorn-allow-prometheus-metrics"]
    data["gateway_http_status"] = {}
    data["gateway_https_status"] = {}
    ca = subprocess.check_output(["kubectl", "-n", "apisix", "get", "secret", "vela-gateway-tls",
                                  "-o", "jsonpath={.data.ca\\.crt}"], text=True)
    context = ssl.create_default_context(cadata=base64.b64decode(ca).decode())
    for host in ["10.1.201.70", "10.1.201.71"]:
        connection = http.client.HTTPConnection(host, 30080, timeout=15)
        connection.request("GET", "/grafana/login")
        response = connection.getresponse()
        response.read()
        data["gateway_http_status"][host] = {"status": response.status, "location": response.getheader("Location")}
        connection.close()
        connection = http.client.HTTPSConnection(host, 30443, context=context, timeout=15)
        connection.request("GET", "/grafana/login")
        response = connection.getresponse()
        response.read()
        data["gateway_https_status"][host] = response.status
        connection.close()
    data["remaining_test_namespaces"] = [n["metadata"]["name"] for n in kube("get", "namespaces")["items"]
                                         if n["metadata"]["name"].startswith("vela-workspace-drill-")]
    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
