#!/usr/bin/env python3
"""Emit synthetic credentials and verify Loki labels, ownership and redaction.

Run on a management node. --expect-unredacted is only the baseline reproduction
mode; normal mode fails if any synthetic credential survives. No real secret is
used. Only a temporary Pod on one of the two CPU management nodes is created.
"""
import argparse
import json
import shlex
import subprocess
import time
import urllib.parse
import urllib.request
import uuid

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--node", choices=["llmpool01", "llmpool02"], default="llmpool01")
parser.add_argument("--expect-unredacted", action="store_true")
args = parser.parse_args()
marker = "vela-log-drill-" + uuid.uuid4().hex[:12]
secret = "VELA_SYNTHETIC_SECRET_" + uuid.uuid4().hex
lines = [marker + " Authorization: Bearer " + secret,
         marker + " password=" + secret,
         json.dumps({"marker": marker, "password": secret}),
         marker + " api_key='" + secret + "'",
         json.dumps({"marker": marker, "secret": secret + 'escaped"tail'}),
         marker + " normal_message=visible"]
k = ["kubectl", "-n", "monitoring"]
service = json.loads(subprocess.check_output(k + ["get", "service", "loki", "-o", "json"]))
base = "http://" + service["spec"]["clusterIP"] + ":3100"
pod = {"apiVersion": "v1", "kind": "Pod", "metadata": {"name": marker, "namespace": "monitoring",
       "labels": {"vela.ai/validation-only": "true"}}, "spec": {"nodeName": args.node,
       "restartPolicy": "Never", "automountServiceAccountToken": False,
       "securityContext": {"runAsNonRoot": True, "runAsUser": 65532},
       "containers": [{"name": "emitter", "image": "10.1.201.70:5000/library/busybox:1.36",
         "command": ["sh", "-c", "sleep 15; printf '%s\\n' " + " ".join(map(shlex.quote, lines)) + "; sleep 180"],
         "securityContext": {"allowPrivilegeEscalation": False, "readOnlyRootFilesystem": True,
                             "capabilities": {"drop": ["ALL"]}},
         "resources": {"requests": {"cpu": "10m", "memory": "16Mi"},
                       "limits": {"cpu": "100m", "memory": "32Mi"}}}]}}
started = time.time()
try:
    subprocess.run(k + ["apply", "-f", "-"], input=json.dumps(pod).encode(), check=True,
                   stdout=subprocess.DEVNULL)
    result = []
    for _ in range(60):
        # Collector diagnostics also mention Pod names. Restrict the stream to
        # the emitter itself, so those diagnostics cannot satisfy the receipt.
        query = '{collector="alloy",instance="monitoring/' + marker + ':emitter"} |= "' + marker + '"'
        params = urllib.parse.urlencode({"query": query, "start": str(int(started * 1e9)),
                                        "end": str(time.time_ns()), "limit": "1000"})
        with urllib.request.urlopen(base + "/loki/api/v1/query_range?" + params, timeout=15) as response:
            result = json.load(response)["data"]["result"]
        observed = {line for stream in result for _, line in stream["values"]}
        if len(observed) >= len(lines):
            break
        time.sleep(3)
    else:
        raise RuntimeError("All synthetic log formats did not arrive")
    raw_secret = any(secret in line for line in observed)
    print(json.dumps({"node": args.node, "marker": marker, "unique_lines": len(observed),
                      "synthetic_secret_found": raw_secret,
                      "streams": [stream["stream"] for stream in result]}), flush=True)
    assert raw_secret == args.expect_unredacted, "Redaction result did not match expected mode"
    if not args.expect_unredacted:
        assert all(stream["stream"].get("namespace") == "monitoring" and
                   stream["stream"].get("pod") == marker and
                   stream["stream"].get("container") == "emitter" and
                   stream["stream"].get("node") == args.node and
                   stream["stream"].get("collector_node") == args.node for stream in result), "Incorrect labels or cross-node duplicate collector"
        assert sum("[REDACTED]" in line for line in observed) == 5, "All five credential formats must be redacted"
        assert any("normal_message=visible" in line for line in observed), "Ordinary content must remain visible"
    print("LOG_PIPELINE_" + ("BASELINE_REPRODUCED" if args.expect_unredacted else "PASS"), flush=True)
finally:
    subprocess.run(k + ["delete", "pod", marker, "--ignore-not-found=true", "--wait=false"],
                   check=True, stdout=subprocess.DEVNULL)
