#!/usr/bin/env python3
"""Fail-closed preflight for the Runtime startup composition harness.

The command deliberately checks inputs only. It never starts Node, changes a
systemd unit, pulls an image, touches CRI state, or rewrites a key. A zero exit
status means that the command-level harness has all locally discoverable
inputs; it does not claim that a composition run or Permit succeeded.
"""

from __future__ import annotations

import argparse
import json
import os
import pathlib
import platform
import stat
import sys


REQUIRED_PATH_ENV = (
    "VELA_NODE_AGENT_RUNTIME_LAUNCH_MANIFEST_FILE",
    "VELA_NODE_AGENT_RUNTIME_BUNDLE_MANIFEST_FILE",
    "VELA_NODE_AGENT_RUNTIME_BINDING_FILE",
    "VELA_NODE_AGENT_RUNTIME_BINDING_VERIFIER_FILE",
    "VELA_NODE_AGENT_RUNTIME_STAGE_VERIFIER_FILE",
    "VELA_NODE_AGENT_RUNTIME_JOURNAL_STATE_DIRECTORY",
    "VELA_NODE_AGENT_RUNTIME_STARTUP_LEDGER_DIRECTORY",
    "VELA_NODE_AGENT_RUNTIME_CRI_SOCKET",
    "VELA_NODE_AGENT_RUNTIME_KUBECONFIG",
    "VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET",
    "VELA_NODE_AGENT_RUNTIME_LAUNCHER_PATH",
    "VELA_NODE_AGENT_RUNTIME_POLICY_ISSUER_SOCKET",
    "VELA_NODE_AGENT_RUNTIME_POLICY_PUBLIC_KEY_FILE",
    "VELA_NODE_AGENT_RUNTIME_POLICY_AUTHORIZATION_PUBLIC_KEY_FILE",
    "VELA_NODE_AGENT_RUNTIME_POLICY_AUTHORIZATION_DIRECTORY",
    "VELA_NODE_AGENT_WORKER_JOURNAL_SOCKET",
    "VELA_NODE_AGENT_WORKER_INPUT_JOURNAL_DIRECTORY",
    "VELA_NODE_AGENT_WORKER_MATERIALIZATION_JOURNAL_DIRECTORY",
    "VELA_NODE_AGENT_WORKER_JOURNAL_PIDFD_BROKER_SOCKET",
)

REQUIRED_FILE_ENV = {
    "VELA_NODE_AGENT_RUNTIME_LAUNCH_MANIFEST_FILE",
    "VELA_NODE_AGENT_RUNTIME_BUNDLE_MANIFEST_FILE",
    "VELA_NODE_AGENT_RUNTIME_BINDING_FILE",
    "VELA_NODE_AGENT_RUNTIME_BINDING_VERIFIER_FILE",
    "VELA_NODE_AGENT_RUNTIME_STAGE_VERIFIER_FILE",
    "VELA_NODE_AGENT_RUNTIME_CRI_SOCKET",
    "VELA_NODE_AGENT_RUNTIME_KUBECONFIG",
    "VELA_NODE_AGENT_RUNTIME_LAUNCHER_PATH",
    "VELA_NODE_AGENT_RUNTIME_POLICY_PUBLIC_KEY_FILE",
    "VELA_NODE_AGENT_RUNTIME_POLICY_AUTHORIZATION_PUBLIC_KEY_FILE",
    "VELA_NODE_AGENT_WORKER_JOURNAL_PIDFD_BROKER_SOCKET",
}


def check_path(name: str, value: str) -> dict[str, object]:
    path = pathlib.Path(value)
    result: dict[str, object] = {"name": name, "value_present": bool(value)}
    if not value:
        result["status"] = "missing"
        return result
    if not path.is_absolute() or path != pathlib.Path(os.path.normpath(value)):
        result["status"] = "noncanonical"
        return result
    result["kind"] = "socket" if name.endswith("SOCKET") else "path"
    if path.is_symlink():
        result["status"] = "symlink"
        return result
    if path.exists():
        mode = path.stat().st_mode
        result["exists"] = True
        result["is_socket"] = stat.S_ISSOCK(mode)
        result["is_directory"] = path.is_dir()
        result["status"] = "present"
        if name.endswith("DIRECTORY") and (path.stat().st_uid != 0 or mode & (stat.S_IWGRP | stat.S_IWOTH)):
            result["status"] = "untrusted"
            result["owner_uid"] = path.stat().st_uid
            result["mode"] = stat.S_IMODE(mode)
        if name in REQUIRED_FILE_ENV and path.is_dir():
            result["status"] = "wrong_type"
        if name.endswith("SOCKET") and not stat.S_ISSOCK(mode):
            result["status"] = "wrong_type"
        return result
    result["exists"] = False
    # Runtime sockets and writable state directories are allowed to be created
    # by the command, but their parents must already exist and be trusted.
    if name.endswith("SOCKET") or name.endswith("DIRECTORY"):
        parent = path.parent
        result["parent"] = str(parent)
        # The command is allowed to create the leaf itself, but its immediate
        # parent must already exist. This also avoids treating platform-level
        # aliases such as macOS /tmp as an application-owned parent.
        if not parent.exists():
            result["status"] = "parent_missing"
            return result
        existing: list[pathlib.Path] = []
        cursor = parent
        while True:
            if cursor.exists():
                existing.append(cursor)
                break
            if cursor.parent == cursor:
                result["status"] = "parent_missing"
                return result
            cursor = cursor.parent
        for component in existing:
            if component.is_symlink() or not component.is_dir():
                result["status"] = "parent_wrong_type"
                return result
            component_stat = component.stat()
            if component_stat.st_uid != 0 or component_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
                result["status"] = "parent_untrusted"
                result["parent_uid"] = component_stat.st_uid
                result["parent_mode"] = stat.S_IMODE(component_stat.st_mode)
                return result
        parent_stat = parent.stat()
        result["parent_uid"] = parent_stat.st_uid
        result["parent_mode"] = stat.S_IMODE(parent_stat.st_mode)
        # Node creates the deferred path as root. Require a root-owned parent
        # with no group/other write bit so an untrusted directory cannot race
        # the socket or durable state path between preflight and startup.
        if parent_stat.st_uid != 0 or parent_stat.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
            result["status"] = "parent_untrusted"
            return result
        result["status"] = "deferred"
    else:
        result["status"] = "missing"
    return result


def kernel_pidfd_checks() -> list[dict[str, object]]:
    """Report the kernel handle primitives used by the startup contract.

    ``pidfs`` is optional: kernels without the filesystem interface use the
    root-owned pidfd broker. ``pidfd_open`` is mandatory because the broker
    and validation launcher must retain the original descriptor rather than
    reconstructing one from a numeric PID.
    """

    if platform.system().lower() != "linux":
        return [
            {"name": "pidfd_open", "status": "unsupported-platform"},
            {"name": "pidfs", "status": "unsupported-platform"},
        ]

    pidfd_open = getattr(os, "pidfd_open", None)
    if not callable(pidfd_open):
        pidfd_status = "missing"
    else:
        try:
            pidfd = pidfd_open(os.getpid())
        except OSError:
            pidfd_status = "unavailable"
        else:
            os.close(pidfd)
            pidfd_status = "present"
    checks = [{"name": "pidfd_open", "status": pidfd_status}]
    try:
        filesystems = pathlib.Path("/proc/filesystems").read_text(encoding="utf-8")
    except OSError:
        checks.append({"name": "pidfs", "status": "unreadable"})
    else:
        checks.append({"name": "pidfs", "status": "present" if "pidfs" in filesystems.split() else "absent-broker-fallback"})
    return checks


def run(environment: dict[str, str]) -> tuple[int, dict[str, object]]:
    checks: list[dict[str, object]] = []
    if platform.system().lower() != "linux":
        checks.append({"name": "platform", "status": "unsupported"})
    else:
        checks.append({"name": "platform", "status": "present"})
    checks.extend(kernel_pidfd_checks())
    if os.geteuid() != 0:
        checks.append({"name": "euid", "status": "requires_root"})
    else:
        checks.append({"name": "euid", "status": "present"})
    for name in REQUIRED_PATH_ENV:
        checks.append(check_path(name, environment.get(name, "")))
    failed = [item for item in checks if item["status"] in {"missing", "unavailable", "noncanonical", "wrong_type", "symlink", "untrusted", "unsupported", "requires_root", "parent_missing", "parent_wrong_type", "parent_untrusted"}]
    report = {
        "schema_version": 1,
        "mode": "preflight-only",
        "ready": not failed,
        "checks": checks,
        "failed_checks": [item["name"] for item in failed],
    }
    return (0 if not failed else 2), report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit machine-readable JSON")
    args = parser.parse_args()
    code, report = run(dict(os.environ))
    if args.json:
        print(json.dumps(report, sort_keys=True, indent=2))
    else:
        print("ready" if report["ready"] else "not-ready")
        for name in report["failed_checks"]:
            print(f"missing-or-invalid: {name}")
    return code


if __name__ == "__main__":
    sys.exit(main())
