#!/usr/bin/env python3
"""Prefetch a pinned H3 release into a verified, immutable node-local cache.

Run on the destination GPU node after its data filesystem is mounted. Source
may be an rsync SSH source (user@host:/srv/models). Inference never uses it.
"""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import subprocess
import tempfile

from fast_h3.vela.model_cache import local_filesystem, require_local_model_cache


def manifest_entries(manifest):
    if manifest.get("schema") != "h3-production-model-manifest-v1":
        raise ValueError("unsupported model manifest schema")
    entries = {}
    for artifact in manifest["artifacts"].values():
        for item in artifact["files"]:
            name = item["path"]
            relative = Path(name)
            if (
                not isinstance(name, str)
                or relative.is_absolute()
                or ".." in relative.parts
                or name != relative.as_posix()
                or "\n" in name
                or "\x00" in name
            ):
                raise ValueError("unsafe model manifest path")
            if name in entries and entries[name] != item:
                raise ValueError("conflicting duplicate model file")
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", item["sha256"]):
                raise ValueError("invalid model file digest")
            entries[name] = item
    if (
        len(entries) != manifest["file_count"]
        or sum(item["size_bytes"] for item in entries.values())
        != manifest["size_bytes"]
    ):
        raise ValueError("model manifest inventory does not match totals")
    return entries


def required_directories(manifest):
    result = []
    for name in manifest.get("required_runtime_directories", []):
        relative = Path(name)
        if (
            relative.is_absolute()
            or ".." in relative.parts
            or name != relative.as_posix()
        ):
            raise ValueError("unsafe required model directory")
        result.append(relative)
    return result


def verify_files(root, manifest, entries, *, published=False):
    if root.is_symlink() or not root.is_dir():
        raise ValueError("local cache root must be a regular directory")
    require_local_model_cache(root, manifest)
    for path in [root, *root.rglob("*")]:
        if path.is_symlink() or (
            not path.is_dir() and path.relative_to(root).as_posix() not in entries
        ):
            raise ValueError("local cache contains an unmanifested file or symlink")
        if published and stat.S_IMODE(path.stat().st_mode) != (
            0o555 if path.is_dir() else 0o444
        ):
            raise ValueError(
                "published local cache must have immutable read-only permissions"
            )
    if published:
        for relative in required_directories(manifest):
            if not (root / relative).is_dir():
                raise ValueError("local cache is missing a required runtime directory")
    for name, item in entries.items():
        path = root / name
        if (
            path.is_symlink()
            or not path.is_file()
            or path.stat().st_size != item["size_bytes"]
        ):
            raise ValueError(f"local cache file missing or wrong size: {name}")
        with path.open("rb") as handle:
            digest = hashlib.file_digest(handle, "sha256").hexdigest()
        if "sha256:" + digest != item["sha256"]:
            raise ValueError(f"local cache file SHA256 mismatch: {name}")


def additional_download_bytes(candidate, entries):
    growth = 0
    replacement_overlap = 0
    for name, item in entries.items():
        path = candidate / name
        allocated = 0
        if path.exists():
            if not path.is_file():
                raise ValueError("partial model file is not a regular file")
            info = path.stat()
            if info.st_size == item["size_bytes"]:
                with path.open("rb") as handle:
                    if (
                        "sha256:" + hashlib.file_digest(handle, "sha256").hexdigest()
                        == item["sha256"]
                    ):
                        continue
            allocated = info.st_blocks * 512
        size = item["size_bytes"]
        growth += max(0, size - allocated)
        # rsync keeps the old file while writing one replacement at a time.
        replacement_overlap = max(replacement_overlap, min(allocated, size))
    return growth + replacement_overlap


def reuse_verified_files(candidate, entries, reuse_root, reuse_manifest):
    """Link only hash-verified, read-only files from a published local release."""
    if reuse_root.is_symlink() or reuse_root.resolve() != reuse_root or not reuse_root.is_dir():
        raise ValueError("reuse root must be a canonical published directory")
    if reuse_root.name != reuse_manifest["content_sha256"].removeprefix("sha256:"):
        raise ValueError("reuse root does not match its release identity")
    old_entries = manifest_entries(reuse_manifest)
    by_content = {(item["sha256"], item["size_bytes"]): name for name, item in old_entries.items()}
    reused_bytes = 0
    for name, item in entries.items():
        destination = candidate / name
        if destination.exists():
            continue
        old_name = by_content.get((item["sha256"], item["size_bytes"]))
        if old_name is None:
            continue
        path = reuse_root / old_name
        if path.resolve() != path or not path.is_file():
            raise ValueError("reuse file is missing or traverses a symlink")
        info = path.stat()
        if stat.S_IMODE(info.st_mode) != 0o444 or info.st_size != item["size_bytes"]:
            raise ValueError("reuse file is not immutable or has changed size")
        with path.open("rb") as handle:
            if "sha256:" + hashlib.file_digest(handle, "sha256").hexdigest() != item["sha256"]:
                raise ValueError("reuse file digest changed: " + old_name)
        destination.parent.mkdir(parents=True, exist_ok=True)
        os.link(path, destination)
        reused_bytes += item["size_bytes"]
    return reused_bytes


def prefetch(manifest_path, expected_sha256, source, cache_root, reuse_manifest_path=None):
    raw = manifest_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("manifest SHA256 does not match the release")
    manifest = json.loads(raw)
    entries = manifest_entries(manifest)
    directories = required_directories(manifest)
    identity = manifest["content_sha256"]
    if not re.fullmatch(r"sha256:[0-9a-f]{64}", identity):
        raise ValueError("invalid release identity")
    if not cache_root.is_absolute() or cache_root.resolve(strict=True) != cache_root:
        raise ValueError("cache root must be an existing canonical data directory")
    local_filesystem(cache_root, Path("/proc/self/mountinfo").read_text())
    release = cache_root / identity.removeprefix("sha256:")
    with (cache_root / ".prefetch.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX)
        if release.exists() or release.is_symlink():
            verify_files(release, manifest, entries, published=True)
            return {
                "model_root": str(release),
                "reused": True,
                "verified_bytes": manifest["size_bytes"],
            }
        # A private partial release can resume, but it is never a runtime path.
        candidate = cache_root / (".partial-" + release.name)
        candidate.mkdir(mode=0o700, exist_ok=True)
        if candidate.is_symlink() or candidate.resolve() != candidate:
            raise ValueError("partial cache is not an owned directory")
        for path in candidate.rglob("*"):
            if path.is_symlink() or (
                not path.is_dir()
                and path.relative_to(candidate).as_posix() not in entries
            ):
                raise ValueError("partial cache contains an unexpected file or symlink")
        if reuse_manifest_path is not None:
            reuse_manifest = json.loads(reuse_manifest_path.read_bytes())
            reuse_identity = reuse_manifest.get("content_sha256", "")
            if not re.fullmatch(r"sha256:[0-9a-f]{64}", reuse_identity):
                raise ValueError("invalid reuse release identity")
            reuse_verified_files(candidate, entries,
                                cache_root / reuse_identity.removeprefix("sha256:"), reuse_manifest)
        disk = os.statvfs(cache_root)
        reserve = max(10 * 1024**3, disk.f_blocks * disk.f_frsize // 5)
        if (
            disk.f_bavail * disk.f_frsize
            < additional_download_bytes(candidate, entries) + reserve
        ):
            raise ValueError(
                "local data disk lacks download capacity plus 20%/10GiB headroom"
            )
        if not source or source.startswith("-") or "\n" in source or "\x00" in source:
            raise ValueError("invalid rsync model source")
        with tempfile.NamedTemporaryFile(
            mode="w", prefix=".file-list-", dir=cache_root
        ) as listing:
            # Linked files are already verified and must never be handed to
            # rsync: even timestamp updates would change the old release inode.
            downloads = []
            for name, item in entries.items():
                path = candidate / name
                valid = False
                if path.is_file() and path.stat().st_size == item["size_bytes"]:
                    with path.open("rb") as handle:
                        valid = "sha256:" + hashlib.file_digest(handle, "sha256").hexdigest() == item["sha256"]
                if not valid:
                    downloads.append(name)
            listing.write("\n".join(sorted(downloads)) + "\n")
            listing.flush()
            if downloads:
                subprocess.run(
                    [
                        "rsync",
                        "-rt",
                        "--partial",
                        "--checksum",
                        "--protect-args",
                        "--files-from=" + listing.name,
                        "--",
                        source.rstrip("/") + "/",
                        str(candidate) + "/",
                    ],
                    check=True,
                )
        verify_files(candidate, manifest, entries)
        for relative in directories:
            (candidate / relative).mkdir(parents=True, exist_ok=True)
        for path in candidate.rglob("*"):
            if path.is_symlink():
                raise ValueError("symlinks are not allowed in a published cache")
            path.chmod(0o555 if path.is_dir() else 0o444)
        candidate.chmod(0o555)
        verify_files(candidate, manifest, entries, published=True)
        candidate.rename(release)
        fd = os.open(cache_root, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)
        return {
            "model_root": str(release),
            "reused": False,
            "verified_bytes": manifest["size_bytes"],
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", required=True, type=Path)
    parser.add_argument("--manifest-sha256", required=True)
    parser.add_argument("--source", required=True)
    parser.add_argument("--cache-root", required=True, type=Path)
    parser.add_argument("--reuse-manifest", type=Path)
    args = parser.parse_args()
    print(
        json.dumps(
            prefetch(args.manifest, args.manifest_sha256, args.source, args.cache_root,
                     args.reuse_manifest),
            indent=2,
        )
    )
