#!/usr/bin/env python3
"""Read-only host disk/mount inventory, excluding CSI bind mounts and loops.

No mount, filesystem repair, formatting, SMART tests, file-content scans or
driver operations. Filesystem free space is read only for existing local mounts.
"""
import datetime
import json
import os
import pathlib
import socket
import subprocess


def output(args):
    return subprocess.check_output(args, stderr=subprocess.PIPE, timeout=20)


def managed_mount(path):
    return path.startswith(("/var/lib/kubelet/", "/var/lib/rancher/", "/run/", "/snap/", "/var/lib/docker/overlay2/"))


def main():
    cols = "NAME,KNAME,TYPE,SIZE,FSTYPE,UUID,PKNAME,MODEL,ROTA"
    try:
        blocks = json.loads(output(["lsblk", "-b", "-J", "-o", cols + ",MOUNTPOINTS"]))["blockdevices"]
    except subprocess.CalledProcessError:
        blocks = json.loads(output(["lsblk", "-b", "-J", "-o", cols + ",MOUNTPOINT"]))["blockdevices"]
    longhorn = {os.path.realpath(p) for p in pathlib.Path("/dev/longhorn").glob("*")}
    def clean(rows):
        result = []
        for row in rows:
            if row["type"] in ("loop", "rom") or "/dev/" + row["kname"] in longhorn:
                continue
            mounts = row.get("mountpoints", [row.get("mountpoint")])
            if (row.get("model") or "").strip() == "VIRTUAL-DISK" and mounts and all(not p or managed_mount(p) for p in mounts):
                continue
            item = {k: v for k, v in row.items() if k not in ("children", "mountpoints", "mountpoint")}
            item["mountpoints"] = [p for p in mounts if p and not managed_mount(p)]
            if row.get("children"):
                item["children"] = clean(row["children"])
            result.append(item)
        return result
    mounts = json.loads(output(["findmnt", "-J", "--real", "-o", "SOURCE,TARGET,FSTYPE,OPTIONS"]))["filesystems"]
    local = []
    def walk(rows):
        for row in rows:
            if not managed_mount(row["target"]) and row["fstype"] in ("ext4", "ext3", "ext2", "xfs", "btrfs", "zfs", "vfat", "ntfs", "exfat"):
                item = {k: v for k, v in row.items() if k != "children"}
                try:
                    stat = os.statvfs(row["target"])
                    item.update({"total_bytes": stat.f_blocks * stat.f_frsize,
                                 "available_bytes": stat.f_bavail * stat.f_frsize,
                                 "free_bytes": stat.f_bfree * stat.f_frsize,
                                 "available_inodes": stat.f_favail, "total_inodes": stat.f_files})
                except OSError as error:
                    item["stat_error"] = type(error).__name__
                local.append(item)
            walk(row.get("children", []))
    walk(mounts)
    result = {"time": datetime.datetime.now(datetime.timezone.utc).isoformat(), "hostname": socket.gethostname(),
              "blocks": clean(blocks), "local_filesystems": local,
              "fstab": [], "note": "Unmounted or signature-free disks are candidates for inspection, not permission to erase."}
    for line in pathlib.Path("/etc/fstab").read_text().splitlines():
        fields = line.split()
        if len(fields) >= 4 and not fields[0].startswith("#"):
            # Avoid credential-bearing remote share options.
            if fields[2] in ("ext4", "ext3", "ext2", "xfs", "btrfs", "zfs", "vfat", "swap"):
                result["fstab"].append({"source": fields[0], "target": fields[1], "fstype": fields[2],
                                        "options": fields[3]})
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
