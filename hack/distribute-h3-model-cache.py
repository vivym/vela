#!/usr/bin/env python3
"""Resume a bounded, manifest-verified H3 cache distribution from a management host.

The configuration pins the existing fast-h3 prefetch implementation and its
manifest. This coordinator never loads a model or changes a running Worker.
"""

import argparse
import base64
from concurrent.futures import ThreadPoolExecutor, as_completed
import datetime
import fcntl
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import subprocess
import threading
import time


PROTECTED = {"10.1.201.44", "10.1.201.56", "10.1.201.57", "10.1.201.66"}
REMOTE_FILES = r'''
import fcntl, os, pathlib, signal, stat, subprocess, tempfile

def run_group(command, env, timeout):
    process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                               stderr=subprocess.PIPE, text=True, start_new_session=True)
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        # rsync inherits this process group. Stop descendants before releasing
        # either the cache publication lock or the outer distribution lock.
        os.killpg(process.pid, signal.SIGKILL)
        process.communicate()
        raise
    return subprocess.CompletedProcess(command, process.returncode, stdout, stderr)

def directory(path, mode=0o755):
    if path.parent != path:
        directory(path.parent)
    if not path.exists() and not path.is_symlink():
        path.mkdir(mode=mode)
        path.chmod(mode)
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode) or info.st_uid not in {0, os.geteuid()} or info.st_mode & 0o022:
        raise ValueError('unsafe directory: ' + str(path))
    if mode == 0o700:
        path.chmod(mode)

def cache_directory(path):
    directory(path)
    # Runtime UID 10001 must traverse these shared ancestors. Do not change
    # private Worker directories or any of their contents.
    for ancestor in (path.parent, path):
        directory(ancestor)
        ancestor.chmod(stat.S_IMODE(ancestor.lstat().st_mode) | 0o111)

def atomic_file(path, raw, mode=0o600, immutable=False):
    directory(path.parent)
    if path.exists() or path.is_symlink():
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_uid != os.geteuid() or info.st_mode & 0o022:
            raise ValueError('unsafe existing file: ' + str(path))
        if immutable:
            if path.read_bytes() != raw:
                raise ValueError('existing tool bytes differ: ' + str(path))
            return
    fd, temporary = tempfile.mkstemp(prefix='.' + path.name + '-', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.chmod(temporary, mode)
        os.replace(temporary, path)
        parent = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try: os.fsync(parent)
        finally: os.close(parent)
    finally:
        pathlib.Path(temporary).unlink(missing_ok=True)
'''

REMOTE = REMOTE_FILES + r'''
import base64, datetime, hashlib, json, socket, subprocess, sys
os.umask(0o077)
assert os.geteuid() == 0
d = json.loads(base64.b64decode(PAYLOAD))
assert socket.gethostname() == d['node'], 'target hostname changed'
mem = dict(x.split(':', 1) for x in pathlib.Path('/proc/meminfo').read_text().splitlines())
assert 220 <= int(mem['MemTotal'].split()[0]) / 1048576 <= 280, 'target is not in 256GB tier'
root = pathlib.Path(d['tools_root'])
assert root.is_absolute()
directory(root, mode=0o700)
lock_fd = os.open(root / 'distribution.lock', os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW, 0o600)
fcntl.flock(lock_fd, fcntl.LOCK_EX)
for name, asset in d['assets'].items():
    p = root / name
    raw = base64.b64decode(asset['data'])
    assert hashlib.sha256(raw).hexdigest() == asset['sha256']
    atomic_file(p, raw, immutable=True)
cache = pathlib.Path('/var/lib/vela/models')
cache_directory(cache)
wrapper = root / 'bin'
directory(wrapper, mode=0o700)
atomic_file(wrapper / 'rsync', ('#!/bin/sh\nexec /usr/bin/rsync --bwlimit=' + str(d['bandwidth_kib']) + ' "$@"\n').encode(), 0o700, immutable=True)
env = dict(os.environ, PYTHONPATH=str(root / 'src'), PYTHONDONTWRITEBYTECODE='1',
           PATH=str(wrapper) + ':' + os.environ.get('PATH', '/usr/bin:/bin'),
           RSYNC_PASSWORD=d['password'])
before = pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()
try:
    command = ['/usr/bin/python3', str(root / 'scripts/prefetch_vela_h3_models.py'),
        '--manifest', str(root / 'model-manifest.json'), '--manifest-sha256', d['manifest_sha256'],
        '--source', d['source'], '--cache-root', str(cache)]
    if 'reuse-manifest.json' in d['assets']:
        command += ['--reuse-manifest', str(root / 'reuse-manifest.json')]
    p = run_group(command, env=env, timeout=21600)
    if p.returncode:
        raise RuntimeError((p.stderr or p.stdout)[-3000:].replace(d['password'], '<redacted>'))
    receipt = json.loads(p.stdout)
    assert receipt['verified_bytes'] == d['expected_bytes']
    assert receipt['model_root'] == str(cache / d['content_sha256'])
    after = pathlib.Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    assert before == after, 'host boot changed during prefetch'
    receipt.update(node=d['node'], address=d['address'], boot_id=after,
                   finished_at=datetime.datetime.now(datetime.timezone.utc).isoformat(),
                   manifest_sha256=d['manifest_sha256'], passed=True)
    atomic_file(root / 'receipt.json', (json.dumps(receipt, indent=2) + '\n').encode())
    print(json.dumps(receipt), flush=True)
finally:
    os.close(lock_fd)
'''


def validate_targets(targets):
    addresses, nodes = set(), set()
    for target in targets:
        address = str(ipaddress.ip_address(target["address"]))
        if ipaddress.ip_address(address) not in ipaddress.ip_network("10.1.201.0/24"):
            raise ValueError("target outside authorized subnet")
        if address in PROTECTED or target.get("memory_tier") != "256gb":
            raise ValueError("target outside the 256GB worker scope")
        if address in addresses or target["node"] in nodes:
            raise ValueError("duplicate target")
        addresses.add(address)
        nodes.add(target["node"])
    if not targets:
        raise ValueError("no target nodes")


def load_assets(directory, expected):
    assets = {}
    for name, digest in expected.items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("unsafe asset path")
        raw = (directory / relative).read_bytes()
        if hashlib.sha256(raw).hexdigest() != digest:
            raise ValueError("tool or manifest changed: " + name)
        assets[name] = {"sha256": digest, "data": base64.b64encode(raw).decode()}
    return assets


def atomic_json(path, value):
    temporary = path.with_suffix(".tmp")
    with temporary.open("w") as stream:
        json.dump(value, stream, indent=2)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())
    temporary.replace(path)


def ssh_command(address, password_required=False):
    return ["sudo", "-u", "user", "ssh", "-o", "BatchMode=yes", "-o",
            "StrictHostKeyChecking=yes", "-o", "ConnectTimeout=15", "-o",
            "ServerAliveInterval=20", "-o", "ServerAliveCountMax=6",
            "user@" + address, "sudo -k -S -p '' python3 -" if password_required else "sudo -n python3 -"]


def execute_target(config, target, assets, password):
    payload = dict(target, assets=assets, password=password,
                   tools_root=config["remote_tools_root"], source=config["source"],
                   bandwidth_kib=config["bandwidth_kib"],
                   manifest_sha256=config["manifest_sha256"],
                   content_sha256=config["content_sha256"], expected_bytes=config["expected_bytes"])
    encoded = base64.b64encode(json.dumps(payload).encode()).decode()
    sudo_password = (Path(target["sudo_password_file"]).read_text().strip()
                     if target.get("sudo_password_file") else None)
    remote_input = "PAYLOAD = " + repr(encoded) + "\n" + REMOTE
    if sudo_password is not None:
        remote_input = sudo_password + "\n" + remote_input
    result = subprocess.run(ssh_command(target["address"], sudo_password is not None),
                            input=remote_input,
                            text=True, capture_output=True, timeout=22000)
    if result.returncode:
        message = (result.stderr or result.stdout)[-3500:].replace(password, "<redacted>")
        if sudo_password is not None:
            message = message.replace(sudo_password, "<redacted>")
        raise RuntimeError(message)
    receipt = json.loads(result.stdout)
    if not receipt.get("passed") or receipt["address"] != target["address"]:
        raise ValueError("remote receipt does not match target")
    return receipt


def wait_for_predecessor(predecessor):
    """Append a fixed batch after another systemd batch has fully stopped."""
    while True:
        result = subprocess.run(["systemctl", "show", predecessor["unit"],
                                 "-p", "LoadState", "-p", "ActiveState", "-p", "Result"],
                                check=True, capture_output=True, text=True, timeout=15)
        service = dict(line.split("=", 1) for line in result.stdout.splitlines() if "=" in line)
        if service.get("LoadState") != "loaded" or service.get("ActiveState") == "failed":
            print("predecessor unavailable or failed; inspect its receipts before resuming", flush=True)
            raise SystemExit(2)
        if service.get("ActiveState") == "inactive":
            progress = json.loads(Path(predecessor["progress_file"]).read_text())
            if service.get("Result") != "success" or progress.get("passed") is not True:
                print("predecessor stopped without successful verification", flush=True)
                raise SystemExit(2)
            return
        time.sleep(30)


def source_service(source, action):
    command = ["sudo", "-u", "user", "ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=yes",
               "-o", "ConnectTimeout=15", "user@" + source["address"],
               "sudo -n systemctl " + action + " " + source["unit"]]
    subprocess.run(command, check=True, timeout=45)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    config_bytes = args.config.read_bytes()
    config = json.loads(config_bytes)
    validate_targets(config["targets"])
    if not 1 <= config["concurrency"] <= 4 or not 1 <= config["bandwidth_kib"] <= 131072:
        raise ValueError("unbounded transfer concurrency or bandwidth")
    assets = load_assets(Path(config["tools_directory"]), config["assets_sha256"])
    if assets["model-manifest.json"]["sha256"] != config["manifest_sha256"]:
        raise ValueError("manifest identity mismatch")
    manifest = json.loads(base64.b64decode(assets["model-manifest.json"]["data"]))
    if manifest["content_sha256"] != "sha256:" + config["content_sha256"] or manifest["size_bytes"] != config["expected_bytes"]:
        raise ValueError("release identity or size mismatch")
    if not args.apply:
        print(json.dumps({"targets": len(config["targets"]), "concurrency": config["concurrency"],
                          "bytes_per_node": config["expected_bytes"], "apply": False}))
        return
    if config.get("wait_for"):
        print(json.dumps({"state": "WAITING_FOR_PREDECESSOR", "unit": config["wait_for"]["unit"]}), flush=True)
        wait_for_predecessor(config["wait_for"])
    os.umask(0o077)
    run = Path(config["run_directory"])
    run.mkdir(parents=True, exist_ok=True, mode=0o700)
    with (run / "coordinator.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        digest = hashlib.sha256(config_bytes).hexdigest()
        path = run / "progress.json"
        state = json.loads(path.read_text()) if path.exists() else {"config_sha256": digest, "nodes": {}}
        if state["config_sha256"] != digest:
            raise ValueError("resume configuration changed")
        guard = threading.Lock()

        def update(address, value):
            with guard:
                state["nodes"][address] = value
                state["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
                atomic_json(path, state)

        password = Path(config["password_file"]).read_text().strip()
        if config.get("source_start"):
            source_service(config["source_start"], "start")
        stop = threading.Event()

        def work(target):
            if stop.is_set():
                return False
            address = target["address"]
            previous = state["nodes"].get(address, {})
            if previous.get("state") == "VERIFIED":
                return True
            for attempt in range(1, 4):
                update(address, dict(state="COPYING_OR_VERIFYING", node=target["node"], attempt=attempt))
                try:
                    receipt = execute_target(config, target, assets, password)
                    atomic_json(run / (address + ".json"), receipt)
                    update(address, dict(state="VERIFIED", node=target["node"], attempt=attempt, receipt=receipt))
                    print(json.dumps({"address": address, "state": "VERIFIED", "reused": receipt["reused"]}), flush=True)
                    return True
                except Exception as error:
                    update(address, dict(state="RETRY" if attempt < 3 else "FAILED", node=target["node"], attempt=attempt, error=str(error)))
                    if attempt < 3:
                        time.sleep(30 * attempt)
            stop.set()
            return False

        with ThreadPoolExecutor(max_workers=config["concurrency"]) as pool:
            outcomes = [future.result() for future in as_completed([pool.submit(work, t) for t in config["targets"]])]
        state["passed"] = all(outcomes)
        atomic_json(path, state)
        if not state["passed"]:
            raise SystemExit(2)
        # Source cleanup is scoped to the dedicated distribution service/files.
        cleanup = config.get("source_cleanup")
        if cleanup:
            source_service(cleanup, "disable --now")
        print(json.dumps({"passed": True, "verified_nodes": len(outcomes)}), flush=True)


if __name__ == "__main__":
    main()
