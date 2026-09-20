#!/usr/bin/env python3
"""Install a node-local H3 encoder warmup fixture; this does not mark it READY.

Run the deployed runtime's _load_warmup_spec against the result, then require
real GPU warmup and an API canary before publishing capacity.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import struct
import tempfile
import zlib


def atomic_write(path, payload):
    descriptor, temporary = tempfile.mkstemp(prefix=".warmup-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as output:
            output.write(payload)
            output.flush()
            os.fsync(output.fileno())
        os.chmod(temporary, 0o644)
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def reference_png():
    """Deterministic synthetic fixture, with no customer content."""
    width, height = 1344, 768

    def chunk(kind, data):
        return (struct.pack(">I", len(data)) + kind + data
                + struct.pack(">I", zlib.crc32(kind + data) & 0xffffffff))

    rows = bytearray()
    for y in range(height):
        rows.append(0)
        for x in range(width):
            rows.extend((x * 255 // width, y * 255 // height, 96))
    return (b"\x89PNG\r\n\x1a\n"
            + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
            + chunk(b"IDAT", zlib.compress(rows)) + chunk(b"IEND", b""))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", required=True, type=Path)
    parser.add_argument("--task", required=True, choices=("t2va", "ref2va"))
    parser.add_argument("--replace-invalid", action="store_true",
                        help="Replace an existing file only if it is not JSON.")
    args = parser.parse_args()
    directory = args.directory
    if not directory.is_absolute():
        parser.error("directory must be an absolute node-local path")
    directory.mkdir(parents=True, exist_ok=True)
    destination = directory / "ENCODER.json"
    if destination.is_symlink():
        parser.error("refusing a symlink destination")
    if destination.exists():
        try:
            json.loads(destination.read_bytes())
        except (ValueError, UnicodeError):
            if not args.replace_invalid:
                parser.error("invalid existing file; specify --replace-invalid")
        else:
            parser.error("existing JSON is preserved; use a new qualification directory")

    conditions, inputs = [], []
    if args.task == "ref2va":
        payload = reference_png()
        digest = hashlib.sha256(payload).hexdigest()
        reference = directory / ("synthetic-reference-" + digest + ".png")
        if reference.exists() or reference.is_symlink():
            if reference.is_symlink() or reference.read_bytes() != payload:
                parser.error("reference fixture path collision")
        else:
            atomic_write(reference, payload)
        conditions.append({"type": "image", "role": "reference", "uri": str(reference)})
        inputs.append({"path": str(reference), "size_bytes": len(payload), "sha256": digest})
    spec = {
        "schema": "fast_h3.warmup/v1", "component": "ENCODER", "inputs": inputs,
        "parameters": {
            "schema_revision": 1,
            "canonical_request": {
                "schema": "minimax_h3.request/v1", "task": args.task,
                "prompt": "A colorful abstract image moves gently with soft ambient sound.",
                "conditions": conditions,
                "target": {"short_edge": 768, "aspect_ratio": "16:9", "duration_seconds": 5.0},
                "seed": 17,
            },
            "sampling": {
                "num_inference_steps": 20, "quality": "lossless",
                "imgvid_cond_noise_aug_for_inference": None,
                "audio_cond_noise_aug_for_inference": None,
            },
        },
    }
    payload = (json.dumps(spec, indent=2) + "\n").encode()
    atomic_write(destination, payload)
    assert json.loads(destination.read_bytes()) == spec
    print(json.dumps({"path": str(destination), "sha256": hashlib.sha256(payload).hexdigest(),
                      "task": args.task, "inputs": len(inputs), "gpu_warmup_verified": False}))


if __name__ == "__main__":
    main()
