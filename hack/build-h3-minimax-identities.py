#!/usr/bin/env python3
"""Build expected device identities; attach only actual Node probe results.

Node names come from a Kubernetes snapshot, never from IP suffixes. The first
pass emits probe inputs. The second pass accepts attested records from the
host-side NVIDIA probe; it never synthesizes health or epoch observations.
"""
import argparse
import ipaddress
import json
import re
import uuid
from pathlib import Path

NAMESPACE = uuid.UUID('a4f821ee-4208-4cff-8882-373ad15d61d4')
TOPOLOGY = {'encoder': 1, 'dit': 8, 'decoder': 2}


def ident(kind, key):
    return str(uuid.uuid5(NAMESPACE, f'minimax-h3/{kind}/{key}'))


def build(placement, nodes, attestations=None):
    assignments = placement['assignments']
    if len(assignments) != 11 or {r: sum(x['role'] == r for x in assignments) for r in TOPOLOGY} != TOPOLOGY:
        raise ValueError('expected exactly 1 encoder, 8 dit and 2 decoder')
    by_address = {}
    for node in nodes['items']:
        for addr in node['status']['addresses']:
            if addr['type'] == 'InternalIP':
                if addr['address'] in by_address:
                    raise ValueError('duplicate Kubernetes InternalIP')
                by_address[addr['address']] = node['metadata']['name']
    seen_ordinals, seen_gpus, seen_pci = set(), set(), set()
    result = {}
    for item in assignments:
        ordinal, role = item['ordinal'], item['role']
        if type(ordinal) is not int or ordinal <= 0 or ordinal in seen_ordinals:
            raise ValueError('invalid or duplicate ordinal')
        seen_ordinals.add(ordinal)
        address = str(ipaddress.ip_address(item['address']))
        node = by_address[address]
        gpu, pci = item['gpu_uuid'], item['pci_bdf']
        if not re.fullmatch(r'GPU-[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}', gpu):
            raise ValueError('invalid GPU UUID')
        if not re.fullmatch(r'[0-9a-f]{4}:[0-9a-f]{2}:[0-9a-f]{2}\.[0-7]', pci):
            raise ValueError('invalid PCI BDF')
        if gpu in seen_gpus or (node, pci) in seen_pci:
            raise ValueError('physical device reused')
        seen_gpus.add(gpu); seen_pci.add((node, pci))
        key = f'{role}-{ordinal}'
        # These are expected logical identities, not measured health claims.
        device = dict(Kind='GPU', DeviceID=ident('device', gpu),
                      ComputeNodeID=item.get('compute_node_id', ident('compute-node', node)),
                      NodeIdentity=node, GPUUUID=gpu, PCIBDF=pci)
        uuid.UUID(device['ComputeNodeID'])
        result[key] = dict(role=role, ordinal=ordinal, node=node, address=address,
                          input=dict(NodeIdentity=node,
                                     Directory=f'/var/lib/vela/minimax-h3/{key}/device-epochs',
                                     Devices=[device]),
                          ids={k: ident(k, ordinal) for k in ['worker', 'member', 'bundle', 'device-set']})
        if attestations is not None:
            rows = attestations[key]
            if len(rows) != 1:
                raise ValueError('expected one attested device per worker')
            row = rows[0]
            for field in ['DeviceID', 'ComputeNodeID', 'NodeIdentity', 'GPUUUID', 'PCIBDF']:
                if row[field] != device[field]:
                    raise ValueError(f'attested {field} differs for {key}')
            for field in ['NodeEpoch', 'AgentSessionEpoch', 'DeviceEpoch']:
                if type(row[field]) is not int or row[field] <= 0:
                    raise ValueError('invalid measured epoch')
            for field in ['NodeAttestationDigest', 'DeviceAttestationDigest']:
                if not re.fullmatch(r'[0-9a-f]{64}', row[field]) or row[field] == '0'*64:
                    raise ValueError('missing device attestation digest')
            if row['Health'] != 'HEALTHY':
                raise ValueError('probe did not report healthy device')
            result[key]['measured'] = rows
    if attestations is not None and set(attestations) != set(result):
        raise ValueError('attestation worker set mismatch')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('placement', type=Path)
    parser.add_argument('output', type=Path)
    parser.add_argument('--nodes', required=True, type=Path)
    parser.add_argument('--attestations', type=Path)
    args = parser.parse_args()
    result = build(json.loads(args.placement.read_text()), json.loads(args.nodes.read_text()),
                   json.loads(args.attestations.read_text()) if args.attestations else None)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with args.output.open('x') as stream:
        args.output.chmod(0o600)
        json.dump(result, stream, indent=2); stream.write('\n')


if __name__ == '__main__':
    main()
