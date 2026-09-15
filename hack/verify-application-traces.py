#!/usr/bin/env python3
"""Verify real application OTLP export through an existing Collector and Tempo.

Run on a Linux management node with the built hack/trace-canary binary. Two
unprivileged processes listen only on loopback. No Kubernetes workloads, disks,
host services, API routes or real application credentials are changed.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import selectors
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", required=True, type=Path)
    parser.add_argument("--sha256", required=True)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    if os.geteuid() != 0:
        raise RuntimeError("Run from the management node's sudo context")
    os.umask(0o077)
    binary_sha = hashlib.sha256(args.binary.read_bytes()).hexdigest()
    if binary_sha != args.sha256:
        raise RuntimeError("Canary binary checksum mismatch")
    args.output.mkdir(mode=0o700, parents=True, exist_ok=False)
    os.environ["PATH"] = "/var/lib/rancher/rke2/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin"
    os.environ.setdefault("KUBECONFIG", "/etc/rancher/rke2/rke2.yaml")

    def service_ip(name):
        service = json.loads(subprocess.check_output(
            ["kubectl", "-n", "monitoring", "get", "svc", name, "-o", "json"], timeout=20))
        return service["spec"]["clusterIP"]

    collector_ip, tempo_ip = service_ip("otel-collector"), service_ip("tempo")
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    receipt = {"at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
               "binary_sha256": binary_sha, "host": socket.gethostname(),
               "scope": "two unprivileged Linux processes; HTTP -> gRPC -> Collector -> Tempo",
               "collector": collector_ip, "tempo": tempo_ip, "checks": [], "cases": []}

    def check(name, passed):
        receipt["checks"].append({"name": name, "passed": bool(passed)})
        if not passed:
            raise AssertionError(name)

    mount = json.loads(subprocess.check_output(
        ["findmnt", "-J", "-T", "/var/tmp", "-o", "TARGET,OPTIONS"], timeout=10))["filesystems"][0]
    check("temporary executable filesystem", "noexec" not in mount["options"].split(","))
    runtime = Path(tempfile.mkdtemp(prefix="vela-trace-canary-", dir="/var/tmp"))
    runtime.chmod(0o755)
    executable = runtime / "canary"
    shutil.copyfile(args.binary, executable)
    executable.chmod(0o555)
    processes, handles, addresses = [], [], []

    def start(mode, backend=""):
        log = (args.output / (mode + ".stderr")).open("w")
        handles.append(log)
        env = {"PATH": "/usr/bin:/bin", "VELA_TRACE_CANARY_MODE": mode,
               "VELA_TRACE_CANARY_BACKEND": backend, "VELA_TRACING_ENABLED": "true",
               "OTEL_EXPORTER_OTLP_ENDPOINT": "http://" + collector_ip + ":4318",
               "OTEL_TRACES_SAMPLER_ARG": "1",
               "OTEL_RESOURCE_ATTRIBUTES": "private.env=private-canary-environment"}
        process = subprocess.Popen([str(executable)], cwd=runtime, env=env,
                                   stdout=subprocess.PIPE, stderr=log, text=True,
                                   user=65534, group=65534, extra_groups=[], start_new_session=True)
        processes.append(process)
        with selectors.DefaultSelector() as selector:
            selector.register(process.stdout, selectors.EVENT_READ)
            if not selector.select(10):
                raise RuntimeError("Canary did not announce its listener")
            address = json.loads(process.stdout.readline())["address"]
        host, port = address.rsplit(":", 1)
        check(mode + " loopback listener", host == "127.0.0.1" and 0 < int(port) < 65536)
        status = Path("/proc") / str(process.pid) / "status"
        uid = next(line.split()[1:] for line in status.read_text().splitlines() if line.startswith("Uid:"))
        check(mode + " unprivileged UID", set(uid) == {"65534"})
        addresses.append((host, int(port)))
        return address

    def request_case(frontend, case, expected, authorized=True):
        trace_id, parent = uuid.uuid4().hex, uuid.uuid4().hex[:16]
        request = urllib.request.Request("http://" + frontend + "/probe/" + case + "-private-path?token=private-query",
                                        data=b"private-canary-body", method="POST", headers={
                                            "Traceparent": "00-" + trace_id + "-" + parent + "-01",
                                            "Tracestate": "customer=private-canary-tracestate",
                                            "Baggage": "customer=private-canary-baggage",
                                            "Authorization": "Bearer private-canary-credential" if authorized else "private-rejected"})
        try:
            with opener.open(request, timeout=10) as response:
                code = response.status
        except urllib.error.HTTPError as error:
            code = error.code
            error.close()
        check(case + " HTTP status", code == expected)
        item = {"case": case, "trace_id": trace_id, "parent_span_id": parent, "status": code,
                "expected_spans": 3 if authorized else 1}
        receipt["cases"].append(item)

    def stop_processes():
        # Stop frontend before backend so outstanding RPCs can finish normally.
        for process in reversed(processes):
            if process.poll() is None:
                process.send_signal(signal.SIGTERM)
                try:
                    process.wait(timeout=12)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)

    def flatten(data):
        result = []
        for batch in data.get("batches", data.get("resourceSpans", [])):
            resource = {a["key"]: a["value"] for a in batch.get("resource", {}).get("attributes", [])}
            service = resource.get("service.name", {}).get("stringValue")
            for scope in batch.get("scopeSpans", batch.get("instrumentationLibrarySpans", [])):
                for span in scope.get("spans", []):
                    result.append({"service": service, "resource": resource, **span})
        return result

    def normalized_id(value):
        if re.fullmatch(r"(?:[0-9a-f]{16}|[0-9a-f]{32})", value):
            return value
        return base64.b64decode(value, validate=True).hex()

    try:
        backend = start("backend")
        frontend = start("frontend", backend)
        request_case(frontend, "ok", 204)
        request_case(frontend, "fail", 503)
        request_case(frontend, "unauthenticated", 401, authorized=False)
        stop_processes()
        check("both processes shut down and flushed successfully", all(p.returncode == 0 for p in processes))
        for case in receipt["cases"]:
            end = time.monotonic() + 45
            spans, data = [], {}
            while time.monotonic() < end:
                try:
                    request = urllib.request.Request("http://" + tempo_ip + ":3200/api/traces/" + case["trace_id"],
                                                     headers={"Accept": "application/json"})
                    with opener.open(request, timeout=10) as response:
                        data = json.load(response)
                    spans = flatten(data)
                    if len(spans) >= case["expected_spans"]:
                        break
                except urllib.error.HTTPError as error:
                    if error.code != 404:
                        raise
                    error.close()
                time.sleep(1)
            prefix = case["case"] + ": "
            check(prefix + "expected spans in Tempo", len(spans) == case["expected_spans"])
            check(prefix + "no synthetic private data", "private-" not in json.dumps(data) and "private.env" not in json.dumps(data))
            check(prefix + "bounded resources", all(set(s["resource"]) <= {"service.name", "deployment.environment"} for s in spans))
            check(prefix + "same trace ID", all(normalized_id(s["traceId"]) == case["trace_id"] for s in spans))
            check(prefix + "empty vendor state", all(not s.get("traceState") for s in spans))
            http_span = next(s for s in spans if s["name"] == "POST /probe/{case}")
            check(prefix + "HTTP incoming parent", normalized_id(http_span["parentSpanId"]) == case["parent_span_id"])
            attributes = {a["key"]: a["value"] for a in http_span.get("attributes", [])}
            check(prefix + "HTTP status attribute", int(attributes["http.response.status_code"]["intValue"]) == case["status"])
            if case["expected_spans"] == 3:
                rpc_client = next(s for s in spans if s["service"] == "vela-trace-canary-frontend" and s["name"] == "grpc.health.v1.Health/Check")
                rpc_server = next(s for s in spans if s["service"] == "vela-trace-canary-backend")
                check(prefix + "cross-process parent chain",
                      normalized_id(rpc_client["parentSpanId"]) == normalized_id(http_span["spanId"]) and
                      normalized_id(rpc_server["parentSpanId"]) == normalized_id(rpc_client["spanId"]))
                check(prefix + "bounded RPC operation", rpc_server["name"] == "grpc.health.v1.Health/Check")
                for kind, span in [("client", rpc_client), ("server", rpc_server)]:
                    attrs = {a["key"]: a["value"] for a in span.get("attributes", [])}
                    check(prefix + kind + " gRPC status", int(attrs["rpc.grpc.status_code"]["intValue"]) == (14 if case["status"] == 503 else 0))
            if case["status"] == 503:
                check(prefix + "error statuses", all(s.get("status", {}).get("code") in [2, "STATUS_CODE_ERROR"] for s in spans))
            (args.output / (case["case"] + "-trace.json")).write_text(json.dumps(data, indent=2) + "\n")
            case["span_count"] = len(spans)
        receipt["passed"] = True
    finally:
        stop_processes()
        for process in processes:
            process.stdout.close()
        for handle in handles:
            handle.close()
        shutil.rmtree(runtime)
        cleanup = all(p.poll() is not None for p in processes) and not runtime.exists()
        for address in addresses:
            with socket.socket() as probe:
                probe.settimeout(1)
                cleanup = cleanup and probe.connect_ex(address) != 0
        receipt["cleanup"] = {"processes_exited": all(p.poll() is not None for p in processes),
                              "runtime_directory_removed": not runtime.exists(), "all_listeners_closed": cleanup}
        receipt["passed"] = receipt.get("passed", False) and cleanup
        (args.output / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print(json.dumps(receipt), flush=True)
    if not receipt["passed"]:
        raise RuntimeError("Canary cleanup failed")


if __name__ == "__main__":
    main()
