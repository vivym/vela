#!/usr/bin/env python3
"""Exercise the production launcher with synthetic CPU inputs; no Launch Receipt.

Run as root on Linux. All CRI queries use the explicitly selected socket.
The real launcher owns creation and cleanup; this driver never reopens a PID.
"""

import argparse
import array
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import uuid


def wire(value):
    return json.dumps(value, separators=(",", ":")).encode()


def request_for(args):
    worker, member, device, residency, stage = [str(uuid.uuid4()) for _ in range(5)]
    pod = {
        "metadata": {"name": "vela-launcher-smoke-" + worker[:8], "namespace": "default", "uid": worker},
        "spec": {
            "securityContext": {"runAsUser": args.uid, "runAsGroup": args.gid},
            "containers": [
                {
                    "name": "model-runtime",
                    "image": args.runtime_image,
                    # Keep the synthetic Runtime alive with busybox while
                    # preserving the production serve-remote bootstrap
                    # argument contract. /bin/sh consumes the first three
                    # command entries; the four args are positional extras.
                    "command": ["/bin/sh", "-c", "exec /bin/sleep 3600"],
                    "args": ["ignored", "serve-remote", "--bootstrap-file", "/tmp/runtime-bootstrap.json"],
                },
                {"name": "stage-worker-agent", "image": args.worker_image, "command": ["/bin/sleep"], "args": ["3600"]},
            ],
        },
    }
    # Field order matches EncodeLaunchManifest. These synthetic authority values
    # test only the helper transport; they cannot authorize a Node startup.
    manifest = {
        "schema_version": 2, "worker_profile_revision_id": worker, "worker_role": "cpu-encode", "capacity_slots": 1,
        "worker_instance_id": worker, "worker_instance_epoch": 1,
        "worker_member_id": member, "worker_member_epoch": 1,
        "device_set_digest": "0" * 64, "membership_digest": "1" * 64,
        "devices": [{"id": device, "epoch": 1}],
        "members": [{"id": member, "epoch": 1, "identity_digest": "2" * 64, "device_subset_digest": "3" * 64}],
        "local_devices": [{"device_id": device, "device_epoch": 1, "resource_class": "CPU"}],
        "runtimes": [{
            "model_residency_id": residency, "runtime_identity": "smoke", "stage_profile_revision_id": stage,
            "model_runtime_epoch_floor": 0, "component": "CPU_MEDIA", "model_component_revision": "smoke",
            "runtime_image_digest": args.runtime_image.rsplit("@sha256:", 1)[-1], "command": ["/bin/sleep"],
            "scratch_root": "/scratch", "input_root": "/scratch/input", "output_root": "/scratch/output",
            "initialization_timeout": "1s", "shutdown_timeout": "1s",
        }],
    }
    return {"version": 2, "manifest": manifest, "expected_pod": pod,
            # This is the host-side Node socket path. The launcher maps its
            # basename into /run/vela-node inside the Runtime container.
            "expected_pod_digest": list(hashlib.sha256(wire(pod)).digest()), "startup_socket": "/run/vela/startup-smoke.sock"}


def inventory(args):
    base = [args.crictl, "--runtime-endpoint", "unix://" + args.cri_socket,
            "--image-endpoint", "unix://" + args.cri_socket]
    pods = subprocess.run(base + ["pods", "-o", "json"], check=True, capture_output=True, timeout=15)
    containers = subprocess.run(base + ["ps", "-a", "-o", "json"], check=True, capture_output=True, timeout=15)
    return json.loads(pods.stdout).get("items", []), json.loads(containers.stdout).get("containers", [])


def observer_handshake(fd):
    """Exercise the attached observer custody protocol on the handed-off endpoint."""
    endpoint = socket.socket(fileno=fd)
    nonce = os.urandom(32)
    prefix = b"vela-exec-custody-v1"
    endpoint.sendall(prefix + b"C" + nonce)
    packet, ancillary, flags, _ = endpoint.recvmsg(4096, socket.CMSG_SPACE(array.array("i").itemsize))
    if flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC) or packet != prefix + b"O" + nonce:
        raise RuntimeError("observer custody offer is invalid")
    offered = []
    for level, kind, payload in ancillary:
        if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
            descriptors = array.array("i")
            descriptors.frombytes(payload)
            offered.extend(descriptors)
    if len(offered) != 1:
        raise RuntimeError("observer custody target descriptor count is invalid")
    validate_fd(offered[0], "pidfd")
    os.close(offered[0])
    endpoint.sendall(prefix + b"A" + nonce)
    if endpoint.recv(4096) != prefix + b"R" + nonce:
        raise RuntimeError("observer custody acknowledgement is invalid")
    endpoint.sendall(prefix + b"P" + nonce)
    if endpoint.recv(4096) != prefix + b"L" + nonce:
        raise RuntimeError("observer custody liveness response is invalid")
    endpoint.close()


def validate_fd(fd, expected):
    flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    if not flags & fcntl.FD_CLOEXEC:
        # SCM_RIGHTS does not carry descriptor flags on all kernels. Apply the
        # same receiver-side hardening as Node's MSG_CMSG_CLOEXEC path before
        # inspecting the handle.
        fcntl.fcntl(fd, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
        flags = fcntl.fcntl(fd, fcntl.F_GETFD)
    if not flags & fcntl.FD_CLOEXEC:
        raise RuntimeError(f"{expected} descriptor is not close-on-exec")
    link = os.readlink(f"/proc/self/fd/{fd}")
    if expected == "pidfd":
        if "pidfd" not in link:
            raise RuntimeError(f"descriptor is not a pidfd: {link}")
        info = Path(f"/proc/self/fdinfo/{fd}").read_text()
        pid = next((line.split(":", 1)[1].strip() for line in info.splitlines() if line.startswith("Pid:")), "")
        if not pid.isdigit() or int(pid) <= 0:
            raise RuntimeError("pidfd has no live kernel PID identity")
    elif expected == "socket" and not link.startswith("socket:"):
        raise RuntimeError(f"descriptor is not a socket: {link}")


def pid_from_fd(fd):
    info = Path(f"/proc/self/fdinfo/{fd}").read_text()
    value = next((line.split(":", 1)[1].strip() for line in info.splitlines() if line.startswith("Pid:")), "")
    if not value.isdigit() or int(value) <= 0:
        raise RuntimeError("pidfd has no live PID identity")
    return int(value)


def proc_status_field(pid, field):
    prefix = field + ":"
    for line in Path(f"/proc/{pid}/status").read_text().splitlines():
        if line.startswith(prefix):
            return line[len(prefix):].strip()
    raise RuntimeError(f"/proc status field {field} is missing")


def smoke(args):
    request = request_for(args)
    # The production launcher mounts a pre-published Runtime bootstrap
    # directory. Use a unique root-owned directory for this synthetic smoke so
    # the test never depends on (or removes) a daemon's /run/vela state.
    smoke_root = Path("/run") / ("vela-launcher-smoke-" + uuid.uuid4().hex)
    (smoke_root / "runtime-bootstrap").mkdir(mode=0o700, parents=True)
    os.chmod(smoke_root, 0o700)
    request["startup_socket"] = str(smoke_root / "startup.sock")
    started = time.monotonic()
    result = {"scope": "production-launcher-protocol-smoke", "synthetic_inputs": True,
              "production_launch_receipt": False, "cri_socket": args.cri_socket,
              "launcher_sha256": hashlib.sha256(Path(args.launcher).read_bytes()).hexdigest(),
              "observer_sha256": hashlib.sha256(Path(args.observer).read_bytes()).hexdigest(),
              "request": request, "handoff_verified": False, "cleanup_verified": False}
    # Prove the same CRI can be queried before creating anything.
    inventory(args)
    parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_SEQPACKET)
    rights = []
    proc = None
    try:
        if result["launcher_sha256"] != result["observer_sha256"]:
            raise RuntimeError("launcher and observer must be the same source-matched executable")
        env = os.environ.copy()
        env.update({
            "VELA_RUNTIME_LAUNCHER_CONTROL_FD": str(child.fileno()),
            "VELA_RUNTIME_LAUNCHER_CRI_SOCKET": args.cri_socket,
            "VELA_RUNTIME_LAUNCHER_KUBECONFIG": args.kubeconfig,
            "VELA_RUNTIME_LAUNCHER_SANDBOX_IMAGE": args.sandbox_image,
            "VELA_RUNTIME_LAUNCHER_OBSERVER_PATH": args.observer,
            "VELA_RUNTIME_LAUNCHER_CGROUP_PARENT": args.cgroup_parent,
            "VELA_RUNTIME_LAUNCHER_STARTUP_TIMEOUT": str(args.timeout) + "s",
        })
        proc = subprocess.Popen([args.launcher], pass_fds=(child.fileno(),), env=env,
                                stdin=subprocess.DEVNULL, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        # Never retain another copy of the child endpoint: it would hide EOF.
        child.close()
        parent.settimeout(args.timeout + 20)
        parent.sendall(wire(request))
        data, ancillary, flags, _ = parent.recvmsg(65536, socket.CMSG_SPACE(16 * array.array("i").itemsize), socket.MSG_CMSG_CLOEXEC)
        for level, kind, payload in ancillary:
            if level == socket.SOL_SOCKET and kind == socket.SCM_RIGHTS:
                descriptors = array.array("i")
                descriptors.frombytes(payload)
                rights.extend(descriptors)
        if not data:
            raise RuntimeError("launcher exited before handoff")
        reply = json.loads(data)
        result["reply"] = reply
        result["descriptor_count"] = len(rights)
        if flags & (socket.MSG_TRUNC | socket.MSG_CTRUNC) or len(rights) != 4:
            raise RuntimeError("truncated handoff or incorrect descriptor count")
        validate_fd(rights[0], "pidfd")
        validate_fd(rights[1], "pidfd")
        validate_fd(rights[2], "pidfd")
        validate_fd(rights[3], "socket")
        # Production launcher protocol v2 carries four descriptors, including
        # the original launcher pidfd used to bind observer ancestry.
        if reply.get("version") != 2 or reply.get("validation_only") is not False or reply.get("fd_count") != 4:
            raise RuntimeError("unexpected launcher handoff protocol")
        target, worker = reply["target"], reply["worker_target"]
        for item, name in [(target, "model-runtime"), (worker, "stage-worker-agent")]:
            if item["pod_uid"] != request["expected_pod"]["metadata"]["uid"] or item["container_name"] != name:
                raise RuntimeError("handoff targets do not match the requested Pod")
        if target["sandbox_id"] != worker["sandbox_id"] or target["container_id"] == worker["container_id"]:
            raise RuntimeError("Runtime and Worker do not form a distinct pair in one sandbox")
        observer_pid = pid_from_fd(rights[2])
        runtime_pid = pid_from_fd(rights[0])
        # The observer must be the helper's direct child and must still be the
        # ptrace owner of the exact Runtime process at the handoff boundary.
        if int(proc_status_field(observer_pid, "PPid")) != proc.pid:
            raise RuntimeError("observer is not a direct child of launcher helper")
        tracer = proc_status_field(runtime_pid, "TracerPid").split()[0]
        if tracer != str(observer_pid):
            raise RuntimeError("Runtime is not held by the handed-off observer")
        pods, containers = inventory(args)
        if not any(p["id"] == target["sandbox_id"] and p["state"] == "SANDBOX_READY" for p in pods):
            raise RuntimeError("CRI sandbox is not ready")
        for item in (target, worker):
            if not any(c["id"] == item["container_id"] and c["state"] == "CONTAINER_RUNNING" for c in containers):
                raise RuntimeError("CRI container is not running")
        observer_handshake(rights[3])
        rights[3] = -1
        result["observer_handshake_verified"] = True
        result["handoff_verified"] = True
    except Exception as exc:
        result["error"] = str(exc)
    finally:
        for fd in rights:
            if fd >= 0:
                try:
                    os.close(fd)
                except OSError:
                    pass
        child.close()
        parent.close()
        if proc is not None:
            try:
                out, err = proc.communicate(timeout=25)
            except subprocess.TimeoutExpired:
                result["forced_termination"] = True
                proc.terminate()
                try:
                    out, err = proc.communicate(timeout=20)
                except subprocess.TimeoutExpired:
                    proc.kill()
                    out, err = proc.communicate(timeout=5)
            result.update(exit_code=proc.returncode, stdout=out.decode(errors="replace"), stderr=err.decode(errors="replace"))
    try:
        pods, containers = inventory(args)
        uid = request["expected_pod"]["metadata"]["uid"]
        remaining_pods = [p["id"] for p in pods if p.get("metadata", {}).get("uid") == uid]
        known = {result.get("reply", {}).get(key, {}).get("container_id") for key in ("target", "worker_target")}
        remaining_containers = [c["id"] for c in containers if c["id"] in known or c.get("podSandboxId") in remaining_pods]
        result.update(remaining_sandboxes=remaining_pods, remaining_containers=remaining_containers,
                      cleanup_verified=not remaining_pods and not remaining_containers)
    except Exception as exc:
        result["cleanup_error"] = str(exc)
    result["elapsed_seconds"] = round(time.monotonic() - started, 3)
    result["passed"] = result["handoff_verified"] and result.get("observer_handshake_verified") and result["cleanup_verified"] and result.get("exit_code") == 0 and not result.get("forced_termination")
    shutil.rmtree(smoke_root, ignore_errors=True)
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("launcher", "cri-socket", "crictl", "kubeconfig", "observer", "runtime-image", "worker-image", "sandbox-image", "output"):
        parser.add_argument("--" + name, required=True)
    parser.add_argument("--cgroup-parent", default="kubepods.slice")
    parser.add_argument("--uid", type=int, default=65532)
    parser.add_argument("--gid", type=int, default=65532)
    parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    if os.geteuid() != 0 or not 0 < args.timeout <= 900:
        parser.error("root and a timeout between 1 and 900 seconds are required")
    # Refuse to overwrite a prior observation.
    with open(args.output, "x") as output:
        result = smoke(args)
        json.dump(result, output, indent=2)
        output.write("\n")
    print(json.dumps({k: result.get(k) for k in ("passed", "handoff_verified", "observer_handshake_verified", "cleanup_verified", "error", "stderr", "elapsed_seconds")}))
    raise SystemExit(0 if result["passed"] else 1)


if __name__ == "__main__":
    main()
