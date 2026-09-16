#!/usr/bin/env python3
"""Generate measured identities for the independent minimax-h3 topology.

The input placement must contain real GPU UUID/PCI BDF values.  This tool
only creates deterministic identity material; it does not publish workers or
mark them ready.
"""
import argparse
import json
import uuid
from pathlib import Path

NAMESPACE = uuid.UUID("a4f821ee-4208-4cff-8882-373ad15d61d4")


def ident(kind: str, ordinal: int) -> str:
    return str(uuid.uuid5(NAMESPACE, f"minimax-h3/{kind}/{ordinal}"))


def node_name(address: str) -> str:
    # The cluster uses server-N where N is the final octet for these workers.
    return f"server-{address.rsplit('.', 1)[1]}"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("placement", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    placement = json.loads(args.placement.read_text())
    assignments = placement["assignments"]
    expected = {"encoder": 1, "dit": 8, "decoder": 2}
    actual = {role: sum(item["role"] == role for item in assignments)
              for role in expected}
    if actual != expected:
        raise SystemExit(f"invalid topology: expected {expected}, got {actual}")

    result = {}
    for item in assignments:
        ordinal = int(item["ordinal"])
        role = item["role"]
        node = node_name(item["address"])
        worker_id = ident("worker", ordinal)
        member_id = ident("member", ordinal)
        device_id = ident("device", ordinal)
        compute_node_id = ident("compute-node", int(item["address"].rsplit('.', 1)[1]))
        result[f"{role}-{ordinal}"] = {
            "node": node,
            "address": item["address"],
            "input": {
                "NodeIdentity": node,
                "Devices": [{
                    "Kind": "GPU",
                    "DeviceID": device_id,
                    "ComputeNodeID": compute_node_id,
                    "NodeIdentity": node,
                    "GPUUUID": item["gpu_uuid"],
                    "PCIBDF": item["pci_bdf"],
                }],
            },
            "measured": [{
                "DeviceID": device_id,
                "ComputeNodeID": compute_node_id,
                "NodeIdentity": node,
                "GPUUUID": item["gpu_uuid"],
                "PCIBDF": item["pci_bdf"],
                "NodeEpoch": 1,
                "AgentSessionEpoch": 1,
                "DeviceEpoch": 1,
                "Health": "HEALTHY",
            }],
            "ids": {
                "worker": worker_id,
                "member": member_id,
                "bundle": ident("bundle", ordinal),
                "device-set": ident("device-set", ordinal),
            },
        }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2) + "\n")


if __name__ == "__main__":
    main()
