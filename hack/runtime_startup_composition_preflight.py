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


def state_machine_checks(environment: dict[str, str]) -> list[dict[str, object]]:
    """Reject known poisoned local state before a rollout consumes authority."""
    checks: list[dict[str, object]] = []
    journal_root = pathlib.Path(environment.get("VELA_NODE_AGENT_RUNTIME_JOURNAL_STATE_DIRECTORY", ""))
    state_file = journal_root / "execution-admission.json"
    if state_file.is_file() and not state_file.is_symlink():
        try:
            state = json.loads(state_file.read_text(encoding="utf-8"))
            if not isinstance(state, dict):
                raise ValueError("journal state is not an object")
            lifecycle = state.get("backend_lifecycle") or {}
            if not isinstance(lifecycle, dict):
                raise ValueError("backend lifecycle is not an object")
            lifecycle_state = lifecycle.get("state")
        except (OSError, ValueError, TypeError):
            lifecycle_state = "invalid"
        status = "ready" if lifecycle_state in {"UNSTARTED", "RETIRED"} else "requires-reprovision"
        checks.append({"name": "runtime_journal_backend_lifecycle", "status": status, "state": lifecycle_state})

    ledger_root = pathlib.Path(environment.get("VELA_NODE_AGENT_RUNTIME_STARTUP_LEDGER_DIRECTORY", ""))
    ledger_file = ledger_root / "runtime-startups.jsonl"
    if ledger_file.is_file() and not ledger_file.is_symlink():
        status, active = validate_startup_ledger(ledger_file)
        checks.append({"name": "runtime_startup_ledger", "status": status, "active_startups": active})

    socket_root = pathlib.Path(environment.get("VELA_NODE_AGENT_RUNTIME_STARTUP_SOCKET", "")).parent
    for name in ("launch", "worker-bootstrap", "runtime-bootstrap"):
        path = socket_root / name
        if path.exists() or path.is_symlink():
            checks.append({"name": f"startup_directory:{name}", "status": "must-be-absent"})
    return checks


def validate_startup_ledger(path: pathlib.Path) -> tuple[str, int]:
    """Validate the durable ledger's minimum association invariants.

    Preflight is not the authoritative Go parser, but it must never turn
    malformed or partially written state into a rollout permit. A header-only
    initialized ledger is valid; every exit must refer to one unique startup.
    """
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        if not lines:
            return "requires-reprovision", -1
        header = json.loads(lines[0])
        if not isinstance(header, dict) or not isinstance(header.get("schema_version"), int) or not header.get("ledger_id") or not header.get("node_identity"):
            return "requires-reprovision", -1
        starts: set[str] = set()
        exits: set[str] = set()
        for line in lines[1:]:
            entry = json.loads(line)
            if not isinstance(entry, dict):
                return "requires-reprovision", -1
            kinds = [name for name in ("startup", "exit", "reservation", "grant_attempt") if name in entry]
            if len(kinds) != 1 or not isinstance(entry[kinds[0]], dict):
                return "requires-reprovision", -1
            kind = kinds[0]
            value = entry[kind]
            if kind == "startup":
                request = value.get("request")
                journal_id = request.get("journal_id") if isinstance(request, dict) else None
                if not isinstance(journal_id, str) or not journal_id or journal_id in starts:
                    return "requires-reprovision", -1
                starts.add(journal_id)
            elif kind == "exit":
                journal_id = value.get("journal_id")
                if not isinstance(journal_id, str) or not journal_id or journal_id not in starts or journal_id in exits:
                    return "requires-reprovision", -1
                exits.add(journal_id)
        active = len(starts - exits)
        return ("ready" if active == 0 else "requires-reprovision"), active
    except (OSError, UnicodeError, ValueError, TypeError):
        return "requires-reprovision", -1


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
    checks.extend(state_machine_checks(environment))
    failed = [item for item in checks if item["status"] in {"missing", "unavailable", "noncanonical", "wrong_type", "symlink", "untrusted", "unsupported", "requires_root", "parent_missing", "parent_wrong_type", "parent_untrusted", "requires-reprovision", "must-be-absent"}]
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
