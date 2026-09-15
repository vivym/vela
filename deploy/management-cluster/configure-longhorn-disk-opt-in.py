#!/usr/bin/env python3
"""Require explicit labels before Longhorn creates disks on future nodes.

Existing disk specifications, scheduling, replicas and files are left intact.
The only allowed opt-in nodes are the three approved management/storage hosts.
"""
import json
import subprocess


def kube(*args):
    return json.loads(subprocess.check_output(['kubectl', *args, '-o', 'json'], text=True))


def main():
    allowed = {'llmpool01', 'llmpool02', 'marslab-gpu-01'}
    key = 'node.longhorn.io/create-default-disk'
    nodes = kube('get', 'nodes')['items']
    assert allowed <= {x['metadata']['name'] for x in nodes}, 'approved nodes missing'
    assert not [x['metadata']['name'] for x in nodes if x['metadata']['name'] not in allowed
                and x['metadata'].get('labels', {}).get(key, '').lower() in ('true', 'config')], 'unapproved disk opt-in'
    before = {x['metadata']['name']: x['spec'] for x in kube('-n', 'longhorn-system', 'get', 'nodes.longhorn.io')['items']}
    assert all(before[x].get('disks') for x in allowed), 'approved storage configuration missing'
    setting = kube('-n', 'longhorn-system', 'get', 'settings.longhorn.io', 'create-default-disk-labeled-nodes')
    kube('-n', 'longhorn-system', 'patch', 'settings.longhorn.io', 'create-default-disk-labeled-nodes',
         '--type=merge', '-p', json.dumps({'metadata': {'resourceVersion': setting['metadata']['resourceVersion']}, 'value': 'true'}))
    for name in sorted(allowed):
        kube('label', 'node', name, key + '=true', '--overwrite')
    after = {x['metadata']['name']: x['spec'] for x in kube('-n', 'longhorn-system', 'get', 'nodes.longhorn.io')['items']}
    assert before == after, 'existing Longhorn node specifications changed'
    assert kube('-n', 'longhorn-system', 'get', 'settings.longhorn.io', 'create-default-disk-labeled-nodes')['value'] == 'true'
    print(json.dumps({'result': 'LONGHORN_DISK_OPT_IN_PASS', 'approved_nodes': sorted(allowed),
                      'existing_node_specs_unchanged': len(before), 'previous_setting': setting['value'],
                      'setting': 'true'}))


if __name__ == '__main__':
    main()
