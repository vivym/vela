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
import re
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


def request_payload(name, image, uid, gid, pidfd_broker_socket):
    worker_journal_socket = f"/run/vela-validation/worker-journal-{uuid.uuid4().hex}/worker.sock"
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
                },
                {
                    "name": "stage-worker-agent",
                    "image": image,
                    "command": ["/bin/sleep", "3600"],
                    "env": [
                        {"name": "VELA_WORKER_JOURNAL_SOCKET", "value": worker_journal_socket},
                        {"name": "VELA_WORKER_JOURNAL_PIDFD_BROKER_SOCKET", "value": pidfd_broker_socket},
                    ],
                    "securityContext": {"runAsUser": uid, "runAsGroup": gid},
                },
            ]
        },
    }
    pod_wire = json.dumps(pod, separators=(",", ":")).encode()
    startup_socket = f"/tmp/vela-validation-startup-{uuid.uuid4().hex}.sock"
    request = {
        # Launcher handoff wire version. The policy channel below remains
        # version 1 and is intentionally checked separately.
        "version": 2,
        "manifest": {},
        "expected_pod": pod,
        "expected_pod_digest": list(hashlib.sha256(pod_wire).digest()),
        "startup_socket": startup_socket,
    }
    return request, startup_socket, worker_journal_socket


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


def validation_errors(result):
    """Evaluate helper/CRI evidence; this never asserts Node authorization."""
    errors = []
    scenario = result.get("scenario")
    reply = result.get("reply", {})
    receipt = result.get("receipt", {})
    if scenario not in SCENARIOS:
        errors.append("unknown scenario")
    if result.get("driver_error"):
        errors.append("driver exchange failed")
    if reply.get("version") != 2 or reply.get("validation_only") is not True:
        errors.append("handoff must identify a validation-only helper")
    if reply.get("fd_count") != 4 or result.get("fd_count") != 4:
        errors.append("handoff must carry exactly four descriptors")
    if result.get("reply_flags", 0) & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
        errors.append("handoff was truncated")
    target, worker = reply.get("target", {}), reply.get("worker_target", {})
    ids = [target.get("container_id"), worker.get("container_id"), target.get("sandbox_id")]
    if any(not isinstance(value, str) or re.fullmatch(r"[a-f0-9]{64}", value) is None for value in ids) or len(set(ids)) != 3:
        errors.append("handoff lacks three exact distinct workload IDs")
    if worker.get("sandbox_id") != target.get("sandbox_id"):
        errors.append("Runtime and Worker do not share the returned sandbox")
    expected = result.get("expected_pod", {})
    for item, name in ((target, "model-runtime"), (worker, "stage-worker-agent")):
        if item.get("container_name") != name or any(item.get(key) != expected.get(key) for key in ("pod_uid", "pod_namespace", "pod_name")):
            errors.append(f"{name} target differs from the requested Pod")
    if result.get("cleanup_verified") is not True or result.get("cleanup_remaining_ids") != []:
        errors.append("exact workload cleanup is unverified")
    if receipt.get("version") != 1 or receipt.get("validation_only") is not True or receipt.get("scenario") != scenario or receipt.get("fd_count") != 4:
        errors.append("receipt identity or handoff count is invalid")
    if receipt.get("target") != target or receipt.get("worker_target") != worker:
        errors.append("receipt changed the handed-off workload")
    expected_outcome = "completed" if scenario == "normal" else "failed"
    if receipt.get("outcome") != expected_outcome:
        errors.append(f"{scenario} must have outcome={expected_outcome}")
    if receipt.get("cleanup_error"):
        errors.append("helper cleanup reported an error")
    if scenario == "normal":
        if result.get("launcher_exit") != 0 or receipt.get("error"):
            errors.append("normal helper did not exit successfully")
        request, policy = result.get("policy_request", {}), result.get("policy_reply", {})
        if not request or policy.get("version") != 1 or any(policy.get(key) != request.get(key) for key in ("operation_id", "request_digest")):
            errors.append("policy response does not bind the current request")
        digest = policy.get("evidence_digest")
        if not isinstance(digest, list) or len(digest) != 32 or not any(digest) or any(type(value) is not int or not 0 <= value <= 255 for value in digest):
            errors.append("policy evidence digest is invalid")
        if result.get("policy_fd_count") != 0 or result.get("policy_flags", 0) & (socket.MSG_TRUNC | socket.MSG_CTRUNC):
            errors.append("policy response transport is invalid")
    elif result.get("launcher_exit") in (None, 0) or not receipt.get("error"):
        errors.append("fault scenario did not record a failed helper exit")
    return errors


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
    request, startup_socket, worker_journal_socket = request_payload(
        f"vela-validation-{scenario}", args.runtime_image, args.uid, args.gid, args.pidfd_broker_socket
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
            "VELA_VALIDATION_PIDFD_BROKER_SOCKET": args.pidfd_broker_socket,
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
        "worker_journal_socket": worker_journal_socket,
        "expected_pod": {
            "pod_uid": request["expected_pod"]["metadata"]["uid"],
            "pod_namespace": request["expected_pod"]["metadata"]["namespace"],
            "pod_name": request["expected_pod"]["metadata"]["name"],
        },
        "node_rejection_verified": False,
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
            result["policy_request"] = {
                "version": 1, "operation_id": operation_id, "request_digest": request_digest,
            }
            send_json(parent, result["policy_request"])
            policy_wire, policy_rights, policy_flags = receive_frame(parent)
            result["policy_fd_count"] = len(policy_rights)
            result["policy_flags"] = policy_flags
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
        result["cleanup_verified"] = len(created_ids) == 3 and not result["cleanup_remaining_ids"]
        if receipt_path.exists():
            result["receipt"] = json.loads(receipt_path.read_text())
        else:
            result["receipt_missing"] = True
        stderr = process.stderr.read().decode(errors="replace") if process.stderr else ""
        if stderr.strip():
            result["launcher_stderr"] = stderr[-4096:]
    result["validation_errors"] = validation_errors(result)
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
    parser.add_argument("--pidfd-broker-socket", default="/run/vela/pidfd-broker.sock")
    parser.add_argument("--output", required=True)
    parser.add_argument("--uid", type=int, default=65532)
    parser.add_argument("--gid", type=int, default=65532)
    parser.add_argument("--scenario", action="append", choices=SCENARIOS)
    args = parser.parse_args()
    output = pathlib.Path(args.output).resolve()
    output.mkdir(mode=0o700, parents=True, exist_ok=True)
    if any(output.iterdir()):
        parser.error("output directory must be empty to preserve prior evidence")
    scenarios = args.scenario or list(SCENARIOS)
    results = []
    for scenario in scenarios:
        results.append(run_scenario(args, scenario, output))
    matrix = {
        "version": 1,
        "validation_only": True,
        "production_gates": "0/9",
        "node_restart": {"status": "external-driver-required"},
        "node_composition_verified": False,
        "all_pass": all(not result["validation_errors"] for result in results),
        "results": results,
    }
    matrix_path = output / "matrix.json"
    matrix_path.write_text(json.dumps(matrix, indent=2, sort_keys=True) + "\n")
    print(json.dumps(matrix, indent=2, sort_keys=True))
    return 0 if matrix["all_pass"] else 1


if __name__ == "__main__":
    sys.exit(main())
