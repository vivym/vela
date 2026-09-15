#!/usr/bin/env python3
"""Read-only identification of the two unmounted Intel 4TB NVMe candidates."""
import hashlib
import json
import os
import pathlib
import socket
import subprocess


def run(args):
    p = subprocess.run(args, capture_output=True, timeout=30)
    return p.returncode, p.stdout


def main():
    disks = json.loads(run(["lsblk", "-b", "-J", "-o", "NAME,KNAME,TYPE,SIZE,MODEL,SERIAL,FSTYPE,UUID,MOUNTPOINT"])[1])["blockdevices"]
    results = []
    for disk in disks:
        if disk["type"] != "disk" or (disk.get("model") or "").strip() != "INTEL SSDPE2KX040T8":
            continue
        device = "/dev/" + disk["kname"]
        row = {k: v for k, v in disk.items() if k != "children"}
        row["children"] = disk.get("children", [])
        row["holders"] = [p.name for p in (pathlib.Path("/sys/class/block") / disk["kname"] / "holders").iterdir()]
        row["by_id"] = sorted(str(p) for p in pathlib.Path("/dev/disk/by-id").glob("nvme-*") if os.path.realpath(p) == device)
        code, data = run(["wipefs", "--no-act", "--json", device])
        row["wipefs_rc"], row["signatures"] = code, json.loads(data).get("signatures", []) if code == 0 else None
        code, data = run(["blkid", "-p", "-o", "export", device])
        row["blkid_rc"] = code
        row["blkid_fields"] = dict(line.split("=", 1) for line in data.decode().splitlines() if "=" in line)
        code, data = run(["fuser", device])
        row["fuser_rc"], row["open_process_ids"] = code, data.decode().split()
        row["samples"] = []
        # Sparse samples are only extra evidence; never label the whole disk
        # empty on this basis or silently authorize formatting it.
        with open(device, "rb", buffering=0) as stream:
            for offset in (0, disk["size"] // 2, disk["size"] - 1024**2):
                stream.seek(offset)
                data = stream.read(1024**2)
                row["samples"].append({"offset": offset, "bytes": len(data),
                                       "all_zero": not any(data), "sha256": hashlib.sha256(data).hexdigest()})
        results.append(row)
    print(json.dumps({"hostname": socket.gethostname(), "candidates": results,
                      "formatting_performed": False,
                      "scope": "read-only signatures, identity, holders and sparse samples"}, sort_keys=True))


if __name__ == "__main__":
    main()
