#!/usr/bin/env python3
"""Run the validation-only Runtime startup helper fault matrix.

This driver exercises the helper wire contract and CRI cleanup boundary. It
does not construct a signed Node plan, Fleet reservation, journal grant, or
ModelRuntime Permit, so its output is validation evidence only.
"""

import argparse
import array
import hashlib
import json
import os
import pathlib
import signal
import socket
import subprocess
import sys
import time
import uuid


SCENARIOS = (
    "normal",
    "caller-replacement",
    "observer-channel-loss",
    "policy-response-loss",
    "helper-timeout",
    "helper-crash",
)


def run_checked(command, *, timeout=15):
    return subprocess.run(
        command,
        check=False,
        text=True,
        capture_output=True,
        timeout=timeout,
    )


def container_ids(cri_socket):
    result = run_checked(["ctr", "-a", cri_socket, "-n", "k8s.io", "containers", "ls", "-q"])
    if result.returncode != 0:
        raise RuntimeError(f"ctr containers ls failed: {result.stderr.strip()}")
    return {line for line in result.stdout.splitlines() if line}


def fd_pid(fd):
    try:
        info = pathlib.Path(f"/proc/self/fdinfo/{fd}").read_text()
    except OSError:
        return None
    for line in info.splitlines():
        if line.startswith("Pid:"):
            try:
                return int(line.split(":", 1)[1].strip())
            except ValueError:
                return None
    return None


def request_payload(name, image, uid, gid):
    pod = {
        "metadata": {
            "name": name,
            "namespace": "default",
            "uid": str(uuid.uuid4()),
        },
        "spec": {
            "containers": [
                {
                    "name": "model-runtime",
                    "image": image,
                    "command": ["/bin/sleep", "3600"],
                    "securityContext": {"runAsUser": uid, "runAsGroup": gid},
                }
            ]
        },
    }
    pod_wire = json.dumps(pod, separators=(",", ":")).encode()
    startup_socket = f"/tmp/vela-validation-startup-{uuid.uuid4().hex}.sock"
    request = {
        "version": 1,
        "manifest": {},
        "expected_pod": pod,
        "expected_pod_digest": list(hashlib.sha256(pod_wire).digest()),
        "startup_socket": startup_socket,
    }
    return request, startup_socket


def receive_frame(connection):
    data, ancillary, flags, _ = connection.recvmsg(64 * 1024, socket.CMSG_SPACE(16 * 4))
    rights = []
    for level, kind, payload in ancillary:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            values = array.array("i")
            values.frombytes(payload[: len(payload) - (len(payload) % values.itemsize)])
            rights.extend(values.tolist())
    return data, rights, flags


def send_json(connection, value):
    connection.send(json.dumps(value, separators=(",", ":")).encode())


def stop_process(process):
    if process.poll() is None:
        process.terminate()
        try:
            process.wait(timeout=20)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait(timeout=10)
    return process.returncode


def run_scenario(args, scenario, output):
    receipt_path = output / f"{scenario}.receipt.json"
    request, startup_socket = request_payload(
        f"vela-validation-{scenario}", args.runtime_image, args.uid, args.gid
    )
    before = container_ids(args.cri_socket)
    parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    child.set_inheritable(True)
    environment = os.environ.copy()
    environment.update(
        {
            "VELA_VALIDATION_ONLY": "1",
            "VELA_VALIDATION_CRI_SOCKET": args.cri_socket,
            "VELA_VALIDATION_OBSERVER_PATH": args.observer,
            "VELA_VALIDATION_SANDBOX_IMAGE": args.sandbox_image,
            "VELA_VALIDATION_WORKER_IMAGE": args.worker_image,
            "VELA_VALIDATION_WORKER_COMMAND": args.worker_command,
            "VELA_VALIDATION_RECEIPT_PATH": str(receipt_path),
            "VELA_VALIDATION_SCENARIO": scenario,
            "VELA_VALIDATION_LAUNCHER_CONTROL_FD": str(child.fileno()),
        }
    )
    process = subprocess.Popen(
        [args.launcher, "--vela-runtime-launcher-control-fd=3"],
        stdin=subprocess.DEVNULL,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        env=environment,
        pass_fds=(child.fileno(),),
    )
    child.close()
    result = {
        "scenario": scenario,
        "receipt_path": str(receipt_path),
        "launcher_pid": process.pid,
        "before_containers": sorted(before),
    }
    rights = []
    try:
        parent.send(json.dumps(request, separators=(",", ":")).encode())
        parent.settimeout(30)
        wire, rights, flags = receive_frame(parent)
        result["reply_flags"] = flags
        result["reply"] = json.loads(wire)
        result["fd_count"] = len(rights)
        result["descriptor_pids"] = [fd_pid(fd) for fd in rights]
        target = result["reply"].get("target", {})
        worker = result["reply"].get("worker_target", {})
        result["created_container_ids"] = sorted(
            set(
                [
                    target.get("container_id"),
                    worker.get("container_id"),
                    target.get("sandbox_id"),
                ]
            )
            - {None}
        )
        after_create = container_ids(args.cri_socket)
        result["created_container_delta"] = sorted(after_create - before)
        if scenario == "normal":
            operation_id = str(uuid.uuid4())
            request_digest = list(hashlib.sha256(b"validation-request").digest())
            send_json(
                parent,
                {
                    "version": 1,
                    "operation_id": operation_id,
                    "request_digest": request_digest,
                },
            )
            policy_wire, policy_rights, _ = receive_frame(parent)
            for fd in policy_rights:
                os.close(fd)
            result["policy_reply"] = json.loads(policy_wire)
        elif scenario == "policy-response-loss":
            send_json(
                parent,
                {
                    "version": 1,
                    "operation_id": str(uuid.uuid4()),
                    "request_digest": list(hashlib.sha256(b"lost-policy").digest()),
                },
            )
        # The remaining scenarios are driven by their helper-side fault hook.
        time.sleep(0.25)
    except (OSError, socket.timeout, json.JSONDecodeError) as error:
        result["driver_error"] = str(error)
    finally:
        for fd in rights:
            try:
                os.close(fd)
            except OSError:
                pass
        result["launcher_exit"] = stop_process(process)
        # Let a normal helper observe SIGTERM and cancel its context before the
        # control socket is closed. Closing the socket first turns an otherwise
        # completed validation run into an artificial EOF failure receipt.
        parent.close()
        # CRI Stop/Remove is synchronous at the API boundary, while the
        # containerd listing can lag briefly. Poll the same namespace before
        # declaring cleanup failure so a transient index delay is not confused
        # with a leaked workload.
        deadline = time.monotonic() + 15
        after = container_ids(args.cri_socket)
        created_ids = set(result.get("created_container_ids", []))
        while created_ids & after and time.monotonic() < deadline:
            time.sleep(0.5)
            after = container_ids(args.cri_socket)
        result["after_containers"] = sorted(after)
        # The host may create unrelated containers while this scenario is
        # running (for example, a Kubernetes controller or a delayed CRI
        # listing).  Cleanup is scoped to the identities returned by this
        # helper, rather than treating every concurrent host change as a leak.
        result["cleanup_remaining_ids"] = sorted(created_ids & after)
        result["cleanup_verified"] = not result["cleanup_remaining_ids"]
        if receipt_path.exists():
            result["receipt"] = json.loads(receipt_path.read_text())
        else:
            result["receipt_missing"] = True
        stderr = process.stderr.read().decode(errors="replace") if process.stderr else ""
        if stderr.strip():
            result["launcher_stderr"] = stderr[-4096:]
    return result


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--launcher", required=True)
    parser.add_argument("--observer", required=True)
    parser.add_argument("--cri-socket", required=True)
    parser.add_argument("--sandbox-image", required=True)
    parser.add_argument("--runtime-image", required=True)
    parser.add_argument("--worker-image", required=True)
    parser.add_argument("--worker-command", default="/bin/sleep 3600")
    parser.add_argument("--output", required=True)
    parser.add_argument("--uid", type=int, default=65532)
    parser.add_argument("--gid", type=int, default=65532)
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    args = parser.parse_args()
    output = pathlib.Path(args.output).resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    scenarios = args.scenario or list(SCENARIOS)
    results = []
    for scenario in scenarios:
        results.append(run_scenario(args, scenario, output))
    matrix = {
        "version": 1,
        "validation_only": True,
        "production_gates": "0/9",
        "node_restart": {"status": "external-driver-required"},
        "results": results,
    }
    matrix_path = output / "matrix.json"
    matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n")
    print(json.dumps(matrix, indent=2, sort_keys=True))
    failures = [
        result
        for result in results
        if not result.get("cleanup_verified")
        or result.get("receipt", {}).get("outcome") not in {"completed", "failed"}
    ]
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
