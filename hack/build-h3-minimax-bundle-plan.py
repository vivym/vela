#!/usr/bin/env python3
"""Render the independent minimax-h3 worker bundle plan.

This is the deterministic input layer for the release builder.  It deliberately
does not publish Kubernetes objects or certify a catalog revision.
"""
import argparse
import json
import uuid
from pathlib import Path

NAMESPACE = uuid.UUID("a4f821ee-4208-4cff-8882-373ad15d61d4")


def ident(kind, ordinal):
    return str(uuid.uuid5(NAMESPACE, f"minimax-h3/{kind}/{ordinal}"))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("identities", type=Path)
    p.add_argument("output", type=Path)
    a = p.parse_args()
    identities = json.loads(a.identities.read_text())
    workers = []
    for key, value in sorted(identities.items(), key=lambda item: item[1]["ids"]["worker"]):
        role = key.rsplit("-", 1)[0]
        ordinal = int(key.rsplit("-", 1)[1])
        component = {"encoder": "ENCODER", "dit": "DIT", "decoder": "VAE_DECODER"}[role]
        measured = value["measured"][0]
        workers.append({
            "worker_instance_id": value["ids"]["worker"],
            "worker_member_id": value["ids"]["member"],
            "worker_bundle_id": value["ids"]["bundle"],
            "device_set_id": value["ids"]["device-set"],
            "role": role,
            "ordinal": ordinal,
            "node": value["node"],
            "address": value["address"],
            "component": component,
            "gpu_uuid": measured["GPUUUID"],
            "pci_bdf": measured["PCIBDF"],
            "model": "minimax-h3",
            "cache_manifest_sha256": "6eac1f04b601ebfd5d9cf54a130d9c3b7e5a13549fa1a8517bea2730657a28c0",
            "epoch": {"worker": 1, "member": 1, "device": 1},
        })
    expected = {"encoder": 1, "dit": 8, "decoder": 2}
    actual = {role: sum(w["role"] == role for w in workers) for role in expected}
    if actual != expected:
        raise SystemExit(f"invalid topology: expected {expected}, got {actual}")
    result = {"schema": "vela.minimax-h3-bundle-plan/v1", "model": "minimax-h3",
              "topology": expected, "workers": workers}
    a.output.parent.mkdir(parents=True, exist_ok=True)
    a.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
