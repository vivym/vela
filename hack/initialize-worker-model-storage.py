#!/usr/bin/env python3
"""Prepare or explicitly initialize the reviewed 14 worker NVMe data disks.

prepare is read-only. apply requires the SHA256 of the reviewed plan and explicit
user authorization to format. It never uses mkfs force, changes partitions,
restarts RKE2, or touches GPU drivers. Existing signatures or changed sampled
content make initialization fail closed, even when the plan was approved.
"""
import argparse
import hashlib
import json
import os
import pathlib
import pwd
import shutil
import socket
import subprocess

ALLOWED_HOSTS = {"10.1.201." + str(i) for i in (58, 59, 60, 62, 63, 64, 65)}
MOUNTS = {"models": "/srv/vela/models", "cache": "/srv/vela/cache"}


def run(args, codes=(0,)):
    p = subprocess.run(args, capture_output=True, timeout=180)
    if p.returncode not in codes:
        raise RuntimeError("command failed: " + args[0] + "; rc=" + str(p.returncode))
    return p


def select_node(plan):
    found = [n for n in plan["nodes"] if n["hostname"] == socket.gethostname()]
    if len(found) != 1 or found[0]["address"] not in ALLOWED_HOSTS:
        raise RuntimeError("this host is outside the reviewed seven-worker scope")
    node = found[0]
    if len(node["devices"]) != 2 or {d["role"] for d in node["devices"]} != set(MOUNTS):
        raise RuntimeError("expected exactly one models and one cache device")
    if len({d["serial"] for d in node["devices"]}) != 2:
        raise RuntimeError("disk identities are not distinct")
    for d in node["devices"]:
        if d["mountpoint"] != MOUNTS[d["role"]] or d["filesystem"] != "xfs":
            raise RuntimeError("unreviewed mount or filesystem")
        if not d["device_by_id"].startswith("/dev/disk/by-id/nvme-"):
            raise RuntimeError("use the reviewed stable NVMe identity")
    return node


def inspect(d):
    device = d["device_by_id"]
    row = json.loads(run(["lsblk", "-b", "-J", "-o", "KNAME,TYPE,SIZE,MODEL,SERIAL,FSTYPE,MOUNTPOINT", device]).stdout)["blockdevices"][0]
    if (row["type"] != "disk" or row.get("children") or row.get("mountpoint") or row.get("fstype") or
            row["serial"] != d["serial"] or row["size"] != d["size_bytes"] or row["model"].strip() != d["model"].strip()):
        raise RuntimeError("disk identity or occupancy differs from the reviewed plan")
    if list((pathlib.Path("/sys/class/block") / row["kname"] / "holders").iterdir()):
        raise RuntimeError("disk has active block-device holders")
    signatures = json.loads(run(["wipefs", "--no-act", "--json", device]).stdout)["signatures"]
    if signatures or run(["blkid", "-p", device], codes=(0, 2)).returncode != 2:
        raise RuntimeError("disk has a filesystem or partition signature")
    if run(["fuser", device], codes=(0, 1)).returncode != 1:
        raise RuntimeError("disk is open by another process")
    with open(device, "rb", buffering=0) as f:
        for sample in d["sample_sha256"]:
            f.seek(sample["offset"])
            data = f.read(sample["bytes"])
            if len(data) != sample["bytes"] or hashlib.sha256(data).hexdigest() != sample["sha256"]:
                raise RuntimeError("sampled raw data changed since review")
    target = pathlib.Path(d["mountpoint"])
    if target.resolve() != target or target.is_symlink() or (target.exists() and (not target.is_dir() or any(target.iterdir()))):
        raise RuntimeError("mount target already contains data or is a symlink")
    for line in pathlib.Path("/etc/fstab").read_text().splitlines():
        fields = line.split()
        if len(fields) >= 2 and not fields[0].startswith("#") and fields[1] == str(target):
            raise RuntimeError("mount target is already configured in fstab")


CHECK_SCRIPT = '''#!/usr/bin/env python3
import json, pathlib, subprocess
config=json.loads(pathlib.Path('/etc/vela/model-storage.json').read_text())
for disk in config['devices']:
    p=subprocess.run(['findmnt','-J','-T',disk['mountpoint'],'-o','TARGET,UUID,FSTYPE'],capture_output=True,check=True)
    rows=json.loads(p.stdout)['filesystems']
    if len(rows)!=1 or rows[0]['target']!=disk['mountpoint'] or rows[0]['uuid']!=disk['uuid'] or rows[0]['fstype']!='xfs':
        raise SystemExit('model data filesystem is not mounted at its reviewed identity')
'''


def apply(node, digest):
    if os.geteuid() != 0 or not shutil.which("mkfs.xfs"):
        raise RuntimeError("root and xfsprogs are required before apply")
    root = pathlib.Path("/var/lib/vela-model-storage")
    if root.exists():
        raise RuntimeError("initialization state exists; inspect it before any retry")
    # Check both devices before starting the first destructive operation.
    for d in node["devices"]:
        inspect(d)
    root.mkdir(mode=0o700)
    state = {"plan_sha256": digest, "hostname": node["hostname"], "devices": [], "complete": False}
    def save():
        (root / "initialization.json").write_text(json.dumps(state, indent=2))
    save()
    original_fstab = pathlib.Path("/etc/fstab").read_bytes()
    (root / "fstab.before").write_bytes(original_fstab)
    agent_pid = run(["systemctl", "show", "rke2-agent", "-p", "ExecMainPID", "--value"]).stdout
    for d in node["devices"]:
        inspect(d)
        run(["mkfs.xfs", "-L", "vela-" + d["role"], d["device_by_id"]])
        uuid = run(["blkid", "-s", "UUID", "-o", "value", d["device_by_id"]]).stdout.decode().strip()
        if not uuid:
            raise RuntimeError("new filesystem UUID is missing")
        state["devices"].append({k: d[k] for k in ("role", "serial", "device_by_id", "mountpoint")})
        state["devices"][-1]["uuid"] = uuid
        save()
        pathlib.Path(d["mountpoint"]).mkdir(parents=True, mode=0o755, exist_ok=True)
        run(["mount", "-t", "xfs", "-o", "noatime,prjquota", "UUID=" + uuid, d["mountpoint"]])
        user = pwd.getpwnam("user")
        for sub in (["weights"] if d["role"] == "models" else ["artifacts", "hf", "tmp"]):
            target = pathlib.Path(d["mountpoint"]) / sub
            target.mkdir(mode=0o770)
            os.chown(target, user.pw_uid, user.pw_gid)
        (pathlib.Path(d["mountpoint"]) / ".vela-volume.json").write_text(json.dumps(state["devices"][-1], indent=2))
    lines = ["\n# Vela worker model data; reviewed plan " + digest]
    for d in state["devices"]:
        lines.append("UUID=" + d["uuid"] + " " + d["mountpoint"] + " xfs defaults,noatime,prjquota,nofail,x-systemd.device-timeout=30s 0 0")
    if pathlib.Path("/etc/fstab").read_bytes() != original_fstab:
        raise RuntimeError("fstab changed during initialization; preserve and reconcile manually")
    pathlib.Path("/etc/fstab").write_bytes(original_fstab + ("\n".join(lines) + "\n").encode())
    pathlib.Path("/etc/vela").mkdir(mode=0o755, exist_ok=True)
    pathlib.Path("/etc/vela/model-storage.json").write_text(json.dumps(state, indent=2))
    pathlib.Path("/usr/local/libexec").mkdir(mode=0o755, exist_ok=True)
    pathlib.Path("/usr/local/libexec/vela-model-storage-check.py").write_text(CHECK_SCRIPT)
    pathlib.Path("/etc/systemd/system/vela-model-storage-check.service").write_text('''[Unit]
Description=Verify Vela local model and cache filesystems
RequiresMountsFor=/srv/vela/models /srv/vela/cache
Before=rke2-agent.service
[Service]
Type=oneshot
ExecStart=/usr/bin/python3 /usr/local/libexec/vela-model-storage-check.py
RemainAfterExit=yes
[Install]
WantedBy=multi-user.target
''')
    dropin = pathlib.Path("/etc/systemd/system/rke2-agent.service.d")
    dropin.mkdir(mode=0o755, exist_ok=True)
    (dropin / "30-vela-model-storage.conf").write_text("[Unit]\nRequires=vela-model-storage-check.service\nAfter=vela-model-storage-check.service\n")
    run(["systemctl", "daemon-reload"])
    run(["systemctl", "enable", "--now", "vela-model-storage-check.service"])
    run(["python3", "/usr/local/libexec/vela-model-storage-check.py"])
    if run(["systemctl", "show", "rke2-agent", "-p", "ExecMainPID", "--value"]).stdout != agent_pid:
        raise RuntimeError("RKE2 process changed during initialization; inspect concurrent changes")
    state["complete"] = True
    save()
    print(json.dumps(state, indent=2))


if __name__ == "__main__":
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("mode", choices=("prepare", "apply"))
    p.add_argument("--plan", required=True)
    p.add_argument("--approved-plan-sha256")
    args = p.parse_args()
    raw = pathlib.Path(args.plan).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    node = select_node(json.loads(raw))
    if args.mode == "apply":
        if args.approved_plan_sha256 != digest:
            p.error("apply requires the exact explicitly approved plan SHA256")
        apply(node, digest)
    else:
        for disk in node["devices"]:
            inspect(disk)
        print(json.dumps({"hostname": node["hostname"], "plan_sha256": digest, "preflight_passed": True,
                          "xfsprogs_installed": bool(shutil.which("mkfs.xfs")), "formatting_performed": False}))
