#!/usr/bin/env python3
"""Verify a disposable Prometheus -> Alertmanager -> Grafana silence cycle.

Run on a management node with kubectl and PyYAML. Refuses to fire the drill if
Alertmanager has any receiver integration. Never prints Grafana credentials.
Only the uniquely named drill rule and its exact-match silence are modified.
"""
import base64
import datetime
import json
import subprocess
import time
import urllib.parse
import urllib.request
import uuid

import yaml


def kubectl(*args, obj=None):
    return subprocess.check_output(["kubectl", "-n", "monitoring", *args],
                                   input=None if obj is None else json.dumps(obj).encode())


def service(name):
    return json.loads(kubectl("get", "service", name, "-o", "json"))["spec"]["clusterIP"]


def api(url, payload=None, method=None, auth=None):
    headers = {"Content-Type": "application/json"}
    if auth:
        headers["Authorization"] = auth
    request = urllib.request.Request(url, data=None if payload is None else json.dumps(payload).encode(),
                                     headers=headers, method=method)
    with urllib.request.urlopen(request, timeout=15) as response:
        data = response.read()
        return json.loads(data) if data else None


def wait_for(description, predicate, seconds=180):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            print(description + ": PASS", flush=True)
            return
        time.sleep(3)
    raise RuntimeError(description + ": timed out")


am = "http://" + service("monitoring-kube-prometheus-alertmanager") + ":9093"
prom = "http://" + service("monitoring-kube-prometheus-prometheus") + ":9090"
grafana = "http://" + service("monitoring-grafana")
cfg = yaml.safe_load(api(am + "/api/v2/status")["config"]["original"])
if not all(set(r).issubset({"name", "labels"}) for r in cfg["receivers"]):
    raise SystemExit("Refusing drill: Alertmanager has a receiver integration")
secret = json.loads(kubectl("get", "secret", "grafana-admin", "-o", "json"))["data"]
auth = "Basic " + base64.b64encode(base64.b64decode(secret["admin-user"]) + b":" +
                                  base64.b64decode(secret["admin-password"])).decode()
proxy = grafana + "/api/datasources/proxy/uid/vela-alertmanager"
assert isinstance(api(proxy + "/api/v2/alerts", auth=auth), list)
drill = uuid.uuid4().hex[:12]
name = "vela-observability-drill-" + drill
alert_name = "VelaObservabilityPipelineDrill"
rule = {"apiVersion": "monitoring.coreos.com/v1", "kind": "PrometheusRule",
        "metadata": {"name": name, "namespace": "monitoring", "labels": {"release": "monitoring"}},
        "spec": {"groups": [{"name": name, "interval": "15s", "rules": [
            {"alert": alert_name, "expr": "vector(1)", "for": "0m",
             "labels": {"severity": "info", "validation_only": "true", "drill_id": drill},
             "annotations": {"summary": "Disposable internal observability drill"}}]}]}}
silence_id = None


def alerts():
    return [x for x in api(proxy + "/api/v2/alerts", auth=auth)
            if x["labels"].get("drill_id") == drill]


try:
    kubectl("apply", "-f", "-", obj=rule)
    wait_for("Grafana sees the Prometheus-generated alert", lambda: bool(alerts()))
    now = datetime.datetime.now(datetime.timezone.utc)
    silence = {"matchers": [{"name": "drill_id", "value": drill, "isRegex": False, "isEqual": True}],
               "startsAt": now.isoformat(), "endsAt": (now + datetime.timedelta(minutes=10)).isoformat(),
               "createdBy": "vela-observability-drill", "comment": "Disposable Grafana silence verification"}
    silence_id = api(proxy + "/api/v2/silences", silence, auth=auth)["silenceID"]
    wait_for("Grafana sees the exact-match silence", lambda: any(
        silence_id in x["status"]["silencedBy"] for x in alerts()))
    api(proxy + "/api/v2/silence/" + silence_id, method="DELETE", auth=auth)
    wait_for("Grafana sees the alert unsilenced", lambda: any(
        not x["status"]["silencedBy"] for x in alerts()))
    silence_id = None
    rule["spec"]["groups"][0]["rules"][0]["expr"] = "vector(0) == 1"
    kubectl("apply", "-f", "-", obj=rule)
    wait_for("Prometheus resolves the alert through Grafana", lambda: not alerts())
    print("GRAFANA_ALERT_ROUNDTRIP_PASS " + drill, flush=True)
finally:
    kubectl("delete", "prometheusrule", name, "--ignore-not-found=true")
    if silence_id:
        api(proxy + "/api/v2/silence/" + silence_id, method="DELETE", auth=auth)
