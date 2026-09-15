#!/usr/bin/env python3
"""Install or roll back the Vela runtime-startup host package.

The default mode is a read-only plan. ``--apply`` is required before any
system path is changed. The package verifier is always run before planning or
installation; this script never reimplements digest or inventory validation.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import pathlib
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid


FILES = {
    "vela-pidfd-broker": ("vela-pidfd-broker", "usr/local/bin/vela-pidfd-broker", 0o755),
    "vela-runtime-launcher": ("vela-runtime-launcher", "usr/local/bin/vela-runtime-launcher", 0o755),
    "vela-runtime-policy-issuer": ("vela-runtime-policy-issuer", "usr/local/bin/vela-runtime-policy-issuer", 0o755),
    "pidfd-broker.service": ("pidfd-broker.service", "etc/systemd/system/vela-pidfd-broker.service", 0o644),
    "runtime-policy-issuer.service": ("runtime-policy-issuer.service", "etc/systemd/system/vela-runtime-policy-issuer.service", 0o644),
}
SYSTEMD_SERVICES = ("vela-pidfd-broker.service", "vela-runtime-policy-issuer.service")


def run_verifier(verifier: str, package_dir: pathlib.Path, revision: str) -> None:
    command = [verifier, "verify-runtime-startup-packages", str(package_dir), revision]
    try:
        result = subprocess.run(command, check=False, text=True, capture_output=True)
    except OSError as exc:
        raise RuntimeError(f"run runtime startup package verifier: {exc}") from exc
    if result.returncode != 0:
        detail = (result.stderr or result.stdout).strip()
        raise RuntimeError(f"runtime startup package verification failed: {detail}")


def target_path(root: pathlib.Path, relative: str) -> pathlib.Path:
    relative_path = pathlib.PurePosixPath(relative)
    if relative_path.is_absolute() or ".." in relative_path.parts:
        raise RuntimeError("installer target path is not relative and confined")
    result = root.joinpath(*relative_path.parts)
    if result.parent != root and root not in result.parents:
        raise RuntimeError("installer target escaped root")
    return result


def validate_parent_chain(root: pathlib.Path, destination: pathlib.Path) -> None:
    """Reject symlink/non-directory parents before any replacement.

    A non-system test root may be owned by the invoking user.  The real system
    root is required to use root-owned, non-writable parent directories.
    """
    parent = destination.parent
    chain: list[pathlib.Path] = []
    while parent != root:
        chain.append(parent)
        if parent.parent == parent or root not in parent.parents:
            raise RuntimeError("installer target parent escaped root")
        parent = parent.parent
    chain.append(root)
    for item in reversed(chain):
        # The installer may create missing directories below the confined
        # root. Existing components are checked before creation and again by
        # atomic_install immediately before replacement.
        if not os.path.lexists(item):
            continue
        if item.is_symlink() or not item.is_dir():
            raise RuntimeError(f"installer target parent is not a directory: {item}")
        mode = stat.S_IMODE(item.stat().st_mode)
        if mode & 0o022:
            raise RuntimeError(f"installer target parent is group/other writable: {item}")
        if root == pathlib.Path("/") and item.stat().st_uid != 0:
            raise RuntimeError(f"installer target parent is not root-owned: {item}")


def sha256_file(path: pathlib.Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def build_plan(package_dir: pathlib.Path, root: pathlib.Path, revision: str, operation_id: str) -> dict[str, object]:
    files: list[dict[str, object]] = []
    for name, (source_name, relative, mode) in FILES.items():
        source = package_dir / source_name
        if not source.is_file() or source.is_symlink():
            raise RuntimeError(f"package artifact is not a regular file: {source_name}")
        destination = target_path(root, relative)
        files.append({
            "source": source_name,
            "target": "/" + relative,
            "target_relative": relative,
            "mode": mode,
            "sha256": sha256_file(source),
            "existed": destination.exists() or destination.is_symlink(),
        })
    return {
        "schema_version": 1,
        "operation_id": operation_id,
        "revision": revision,
        "package_directory": str(package_dir),
        "files": files,
        "systemd": ["daemon-reload", "enable vela-pidfd-broker.service", "enable vela-runtime-policy-issuer.service"],
    }


def atomic_install(source: pathlib.Path, destination: pathlib.Path, mode: int, root: pathlib.Path | None = None) -> None:
    root = root or pathlib.Path("/")
    # The caller has already confined the relative path; this check protects
    # against a symlink appearing in the parent chain between planning and
    # replacement.
    parent = destination.parent
    while parent != root:
        if parent.is_symlink() or not parent.is_dir():
            raise RuntimeError(f"installer target parent changed: {parent}")
        parent = parent.parent
    destination.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(prefix=f".{destination.name}.", dir=destination.parent, delete=False) as stream:
        temporary = pathlib.Path(stream.name)
        try:
            with source.open("rb") as source_stream:
                shutil.copyfileobj(source_stream, stream)
            stream.flush()
            os.fsync(stream.fileno())
            os.chmod(temporary, mode)
            os.replace(temporary, destination)
            directory_fd = os.open(destination.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)


def write_receipt(path: pathlib.Path, receipt: dict[str, object]) -> None:
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    encoded = json.dumps(receipt, sort_keys=True, indent=2).encode() + b"\n"
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    fd = os.open(path, flags, 0o600)
    try:
        offset = 0
        while offset < len(encoded):
            offset += os.write(fd, encoded[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)


def update_receipt(path: pathlib.Path, receipt: dict[str, object]) -> None:
    """Atomically replace an existing receipt after an outcome transition."""
    encoded = json.dumps(receipt, sort_keys=True, indent=2).encode() + b"\n"
    with tempfile.NamedTemporaryFile(prefix=f".{path.name}.", dir=path.parent, delete=False) as stream:
        temporary = pathlib.Path(stream.name)
        try:
            stream.write(encoded)
            stream.flush()
            os.fsync(stream.fileno())
            os.chmod(temporary, 0o600)
            os.replace(temporary, path)
            directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
            try:
                os.fsync(directory_fd)
            finally:
                os.close(directory_fd)
        finally:
            temporary.unlink(missing_ok=True)


def capture_systemd_state() -> dict[str, str]:
    states: dict[str, str] = {}
    for service in SYSTEMD_SERVICES:
        result = subprocess.run(["systemctl", "is-enabled", service], check=False, text=True, capture_output=True)
        state = (result.stdout or result.stderr).strip().splitlines()
        states[service] = state[0] if result.returncode == 0 and state else "disabled"
    return states


def restore_systemd_state(states: object) -> None:
    if not isinstance(states, dict):
        return
    for service in SYSTEMD_SERVICES:
        previous = states.get(service, "disabled")
        if previous in {"enabled", "static", "indirect", "generated", "linked", "linked-runtime", "alias"}:
            subprocess.run(["systemctl", "enable", service], check=True)
        else:
            subprocess.run(["systemctl", "disable", service], check=True)
    subprocess.run(["systemctl", "daemon-reload"], check=True)


def install(args: argparse.Namespace) -> int:
    package_dir = pathlib.Path(args.package_dir).resolve(strict=True)
    root = pathlib.Path(args.root).resolve(strict=True)
    run_verifier(args.verifier, package_dir, args.revision)
    operation_id = str(uuid.uuid4())
    plan = build_plan(package_dir, root, args.revision, operation_id)
    if not args.apply:
        print(json.dumps({**plan, "mode": "plan-only", "applied": False}, sort_keys=True, indent=2))
        return 0
    if os.geteuid() != 0 and root == pathlib.Path("/"):
        raise RuntimeError("--apply to system paths requires root")
    rollback_dir = target_path(root, f"var/lib/vela/runtime-startup/rollback/{operation_id}")
    receipt_path = target_path(root, f"var/lib/vela/runtime-startup/receipts/install-{operation_id}.json")
    backups: list[dict[str, object]] = []
    systemd_before = capture_systemd_state() if args.enable_services else {}
    try:
        for item in plan["files"]:
            assert isinstance(item, dict)
            destination = target_path(root, str(item["target_relative"]))
            validate_parent_chain(root, destination)
            destination.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
            validate_parent_chain(root, destination)
            existed = bool(item["existed"])
            backup = rollback_dir / str(item["target_relative"])
            if existed:
                backup.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
                if destination.is_symlink() or not destination.is_file():
                    raise RuntimeError(f"existing install target is not a regular file: {item['target']}")
                if root == pathlib.Path("/") and destination.stat().st_uid != 0:
                    raise RuntimeError(f"existing install target is not root-owned: {item['target']}")
                shutil.copyfile(destination, backup)
                os.chmod(backup, stat.S_IMODE(destination.stat().st_mode))
            atomic_install(package_dir / str(item["source"]), destination, int(item["mode"]), root)
            backups.append({"target_relative": item["target_relative"], "backup_relative": str(backup.relative_to(root)) if existed else None, "existed": existed})
        receipt = {**plan, "mode": "install", "status": "applying", "applied": False, "backups": backups, "rollback_directory": str(rollback_dir), "receipt": str(receipt_path), "systemd_before": systemd_before}
        write_receipt(receipt_path, receipt)
        try:
            if args.enable_services:
                subprocess.run(["systemctl", "daemon-reload"], check=True)
                subprocess.run(["systemctl", "enable", "vela-pidfd-broker.service", "vela-runtime-policy-issuer.service"], check=True)
            if args.reload_node:
                subprocess.run(["systemctl", "reload", "vela-node-agent.service"], check=True)
        except Exception as exc:
            rollback_files(root, backups)
            try:
                restore_systemd_state(systemd_before)
            except Exception:
                pass
            receipt.update({"status": "failed", "applied": False, "error": str(exc)})
            update_receipt(receipt_path, receipt)
            raise
        receipt.update({"status": "installed", "applied": True})
        update_receipt(receipt_path, receipt)
        print(json.dumps(receipt, sort_keys=True, indent=2))
        return 0
    except Exception:
        rollback_files(root, backups)
        raise


def rollback_files(root: pathlib.Path, backups: list[dict[str, object]]) -> None:
    for item in reversed(backups):
        target = target_path(root, str(item["target_relative"]))
        backup_relative = item.get("backup_relative")
        if backup_relative:
            backup = target_path(root, str(backup_relative))
            atomic_install(backup, target, stat.S_IMODE(backup.stat().st_mode), root)
        else:
            target.unlink(missing_ok=True)


def rollback(args: argparse.Namespace) -> int:
    receipt_path = pathlib.Path(args.receipt).resolve(strict=True)
    receipt = json.loads(receipt_path.read_text())
    if receipt.get("schema_version") != 1 or receipt.get("mode") != "install" or not receipt.get("applied") or receipt.get("status") != "installed":
        raise RuntimeError("rollback receipt is invalid")
    if receipt.get("rolled_back"):
        raise RuntimeError("rollback receipt was already consumed")
    root = pathlib.Path(args.root).resolve(strict=True)
    backups = receipt.get("backups")
    if not isinstance(backups, list):
        raise RuntimeError("rollback receipt has no file inventory")
    if not args.apply:
        print(json.dumps({"mode": "rollback-plan", "receipt": str(receipt_path), "files": len(backups)}, sort_keys=True, indent=2))
        return 0
    rollback_files(root, backups)
    if receipt.get("systemd_before"):
        restore_systemd_state(receipt["systemd_before"])
    receipt["rolled_back"] = True
    receipt["status"] = "rolled-back"
    update_receipt(receipt_path, receipt)
    print(json.dumps({"mode": "rollback", "receipt": str(receipt_path), "applied": True}, sort_keys=True, indent=2))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    install_parser = subparsers.add_parser("install")
    install_parser.add_argument("package_dir")
    install_parser.add_argument("revision")
    install_parser.add_argument("--root", default="/")
    install_parser.add_argument("--verifier", default="vela-release-artifacts")
    install_parser.add_argument("--apply", action="store_true")
    install_parser.add_argument("--enable-services", action="store_true")
    install_parser.add_argument("--reload-node", action="store_true")
    install_parser.set_defaults(handler=install)
    rollback_parser = subparsers.add_parser("rollback")
    rollback_parser.add_argument("receipt")
    rollback_parser.add_argument("--root", default="/")
    rollback_parser.add_argument("--apply", action="store_true")
    rollback_parser.add_argument("--enable-services", action="store_true")
    rollback_parser.set_defaults(handler=rollback)
    args = parser.parse_args()
    try:
        return int(args.handler(args))
    except (OSError, RuntimeError, subprocess.CalledProcessError, json.JSONDecodeError) as exc:
        print(f"runtime startup install: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
