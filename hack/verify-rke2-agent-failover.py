#!/usr/bin/env python3
"""Run only on the preflighted, cordoned worker server-22 (10.1.201.11).

Temporarily reject this worker's connections to .70 ports 6443/9345, restart
only rke2-agent, and verify local API access through cached alternate servers.
A systemd timer and finally block remove only the uniquely tagged test rule.
The operator must confirm there are no business Pods and restore the original
cordon state afterwards. This script never reboots a host or touches a driver.
"""
import json
import pathlib
import socket
import subprocess
import time
import uuid


def run(*args, timeout=15, check=True):
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout, check=check)


if socket.gethostname() != "server-22":
    raise SystemExit("This reviewed drill is restricted to server-22")
if not pathlib.Path("/etc/rancher/rke2/config.yaml").exists():
    raise SystemExit("RKE2 configuration is missing")
if run("systemctl", "is-active", "rke2-agent").stdout.strip() != "active":
    raise SystemExit("Agent must be active before the drill")
cache = pathlib.Path("/var/lib/rancher/rke2/agent/etc")
for filename, port in [("rke2-agent-load-balancer.json", 9345),
                       ("rke2-api-server-agent-load-balancer.json", 6443)]:
    addresses = json.loads((cache / filename).read_text())["ServerAddresses"]
    if set(addresses) != {f"10.1.201.{last}:{port}" for last in [70, 71, 66]}:
        raise SystemExit("Expected all three cached backends before fault injection")
kube = ["/var/lib/rancher/rke2/bin/kubectl", "--kubeconfig",
        "/var/lib/rancher/rke2/agent/kubeproxy.kubeconfig", "--request-timeout=5s"]
assert run(*kube, "get", "--raw=/readyz").stdout.strip() == "ok"
tag = "vela-failover-" + uuid.uuid4().hex[:10]
rule = ["OUTPUT", "-d", "10.1.201.70/32", "-p", "tcp", "-m", "multiport",
        "--dports", "6443,9345", "-m", "comment", "--comment", tag,
        "-j", "REJECT", "--reject-with", "tcp-reset"]
delete = ["iptables", "-w", "5", "-D", *rule]
unit = tag + "-rollback"
import shlex
rollback = shlex.join(delete) + "; systemctl start rke2-agent"
run("systemd-run", "--unit=" + unit, "--on-active=120s", "/bin/sh", "-c", rollback)
started = time.monotonic()
try:
    run("iptables", "-w", "5", "-I", "OUTPUT", "1", *rule[1:])
    for port in [6443, 9345]:
        try:
            with socket.create_connection(("10.1.201.70", port), timeout=3):
                raise RuntimeError("Fault injection did not block the intended backend")
        except (ConnectionRefusedError, ConnectionResetError, TimeoutError):
            pass
    print("Primary backend blocked; restarting rke2-agent", flush=True)
    run("systemctl", "restart", "rke2-agent", timeout=60)
    run("systemctl", "is-active", "rke2-agent")
    deadline = time.monotonic() + 30
    while time.monotonic() < deadline:
        response = run(*kube, "get", "--raw=/readyz", check=False)
        if response.returncode == 0 and response.stdout.strip() == "ok":
            break
        time.sleep(2)
    else:
        raise RuntimeError("Local API failed while the primary backend was blocked")
    run("iptables", "-w", "5", "-C", *rule)
    sockets = run("ss", "-H", "-tnp").stdout.splitlines()
    alternates = [line for line in sockets if '"rke2"' in line and any(
        address in line for address in ["10.1.201.71:9345", "10.1.201.66:9345",
                                       "10.1.201.71:6443", "10.1.201.66:6443"])]
    if not alternates:
        raise RuntimeError("No live RKE2 connection to an alternate control node was observed")
    print(json.dumps({"result": "AGENT_RESTART_FAILOVER_PASS", "blocked": "10.1.201.70:6443,9345",
                      "duration_seconds": round(time.monotonic() - started, 2),
                      "alternate_connections": alternates}), flush=True)
finally:
    run(*delete, check=False)
    run("systemctl", "stop", unit + ".timer", check=False)
    run("systemctl", "start", "rke2-agent", timeout=60)
    if run("iptables", "-w", "5", "-C", *rule, check=False).returncode == 0:
        raise RuntimeError("Test firewall rule was not removed")
    print("Test rule removed; rke2-agent active", flush=True)
