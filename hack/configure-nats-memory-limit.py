#!/usr/bin/env python3
"""Cap JetStream memory without resizing volumes or changing stream contracts.

NATS 2.10 cannot reload a dynamic memory limit. Create an immutable server
Secret preserving credentials, then lower a StatefulSet partition one Pod at
a time. Client Secret references stay unchanged. Failure keeps the partition
and receipt in place so remaining Pods are untouched.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import time
import urllib.request


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run(args, body=None, timeout=30):
    result = subprocess.run(args, input=body, text=True, capture_output=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError('command failed: ' + args[0] + ' (output retained privately when needed)')
    return result.stdout


def kube(*args):
    return json.loads(run(['kubectl', '-n', 'vela-system', *args, '-o', 'json']))


def snapshot(name):
    pod = kube('get', 'pod', name)
    base = 'http://' + pod['status']['podIP'] + ':8222'
    with urllib.request.urlopen(base + '/varz', timeout=5) as response:
        varz = json.load(response)
    with urllib.request.urlopen(base + '/jsz', timeout=5) as response:
        jsz = json.load(response)
    return {'pod': name, 'uid': pod['metadata']['uid'], 'node': pod['spec']['nodeName'],
            'ready': any(x['type'] == 'Ready' and x['status'] == 'True' for x in pod['status'].get('conditions', [])),
            'claims': [v['persistentVolumeClaim']['claimName'] for v in pod['spec']['volumes'] if 'persistentVolumeClaim' in v],
            'version': varz['version'], 'max_memory': varz['jetstream']['config']['max_memory'],
            'max_storage': varz['jetstream']['config']['max_storage'], 'memory': jsz.get('memory', 0),
            'reserved_memory': jsz.get('reserved_memory', 0), 'streams': jsz.get('streams', 0),
            'storage': jsz.get('storage', 0), 'meta_cluster': jsz.get('meta_cluster', {})}


def quorum(snapshot):
    meta = snapshot['meta_cluster']
    return snapshot['ready'] and bool(meta.get('leader')) and meta.get('cluster_size') == 3 and not meta.get('pending', 0)


def cluster_ready(snapshots):
    if len(snapshots) != 3 or not all(quorum(x) for x in snapshots):
        return False
    leaders = {x['meta_cluster']['leader'] for x in snapshots}
    if len(leaders) != 1:
        return False
    leader = next(iter(leaders))
    report = next((x['meta_cluster'] for x in snapshots if x['pod'] == leader), None)
    # In NATS 2.10 only the leader reports replica currency; followers omit it.
    if not report or {x['name'] for x in report.get('replicas', [])} != {x['pod'] for x in snapshots if x['pod'] != leader}:
        return False
    return all(x.get('current') and not x.get('offline') and not x.get('lag', 0) for x in report['replicas'])


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-directory', required=True)
    parser.add_argument('--limits', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    directory = Path(args.run_directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    receipt = directory / 'memory-limit.json'
    if receipt.exists():
        raise RuntimeError('existing rollout: inspect this receipt, do not start another')
    limit = json.loads(Path(args.limits).read_text())['maxMemoryStoreBytes']
    assert limit == 2 * 1024**3, 'reviewed limit is 2GiB'
    before = [snapshot(name) for name in ['nats-0', 'nats-1', 'nats-2']]
    assert cluster_ready(before) and all(x['version'] == '2.10.22' for x in before), 'initial NATS quorum/version mismatch'
    assert all(x['memory'] == x['reserved_memory'] == x['streams'] == x['storage'] == 0 for x in before), 'this first configuration requires the observed empty validation cluster'
    sts = kube('get', 'sts', 'nats')
    assert sts['status'].get('readyReplicas') == 3
    container = next(x for x in sts['spec']['template']['spec']['containers'] if x['name'] == 'nats')
    assert container['resources']['limits']['memory'] == '8Gi'
    secret = kube('get', 'secret', 'vela-nats-auth')
    assert secret.get('immutable'), 'expected the existing immutable credential Secret'
    assert sts['spec']['updateStrategy']['type'] == 'RollingUpdate'
    assert sts['spec']['updateStrategy'].get('rollingUpdate', {}).get('partition', 0) == 0
    old = base64.b64decode(secret['data']['nats.conf']).decode()
    new, count = re.subn(r'jetstream\s*\{\s*store_dir:\s*/data/jetstream\s*\}',
                        'jetstream {\n  store_dir: /data/jetstream\n  max_memory_store: ' + str(limit) + '\n}', old)
    assert count == 1 and new != old, 'unexpected existing JetStream stanza'
    # Validate against the actual server binary and its mounted TLS material.
    run(['kubectl', '-n', 'vela-system', 'exec', '-i', 'nats-0', '--',
         'nats-server', '--test', '--config', '/dev/stdin'], body=new)
    digest = hashlib.sha256(new.encode()).hexdigest()
    secret_name = 'vela-nats-auth-memory-' + digest[:12]
    config_index = next(i for i, v in enumerate(sts['spec']['template']['spec']['volumes']) if v['name'] == 'config')
    assert sts['spec']['template']['spec']['volumes'][config_index]['secret']['secretName'] == 'vela-nats-auth'
    result = {'phase': 'preflight', 'started_at': now(), 'before': before, 'target_max_memory': limit,
              'pod_memory_limit': '8Gi', 'replacements': [], 'source_secret_uid': secret['metadata']['uid'],
              'server_secret_name': secret_name, 'statefulset_uid': sts['metadata']['uid']}

    def save():
        result['updated_at'] = now()
        receipt.write_text(json.dumps(result, indent=2) + '\n')
        print(result['phase'], flush=True)

    save()
    try:
        expected_data = dict(secret['data'], **{'nats.conf': base64.b64encode(new.encode()).decode()})
        new_secret = {'apiVersion': 'v1', 'kind': 'Secret', 'type': secret.get('type', 'Opaque'),
                      'metadata': {'name': secret_name, 'namespace': 'vela-system',
                                   'labels': {'app.kubernetes.io/part-of': 'vela', 'vela.ai/purpose': 'nats-server-memory-bound'}},
                      'immutable': True, 'data': expected_data}
        run(['kubectl', 'create', '-f', '-'], body=json.dumps(new_secret))
        actual = kube('get', 'secret', secret_name)
        assert actual['data'] == expected_data
        assert kube('get', 'secret', 'vela-nats-auth')['data'] == secret['data']
        patch = [{'op': 'test', 'path': '/metadata/resourceVersion', 'value': sts['metadata']['resourceVersion']},
                 {'op': 'add', 'path': '/spec/updateStrategy', 'value': {'type': 'RollingUpdate', 'rollingUpdate': {'partition': 3}}},
                 {'op': 'replace', 'path': '/spec/template/spec/volumes/' + str(config_index) + '/secret/secretName', 'value': secret_name}]
        run(['kubectl', '-n', 'vela-system', 'patch', 'sts', 'nats', '--type=json', '--patch-file=/dev/stdin'], body=json.dumps(patch))
        result.update(phase='configured', config_sha256=digest, credentials_unchanged=True,
                      source_secret_unchanged=True, server_secret_uid=actual['metadata']['uid'])
        save()
        for original in reversed(before):
            assert kube('get', 'sts', 'nats')['metadata']['uid'] == sts['metadata']['uid']
            assert cluster_ready([snapshot(x['pod']) for x in before]), 'quorum not healthy before replacement'
            name = original['pod']
            ordinal = int(name.rsplit('-', 1)[1])
            result['phase'] = 'replacing-' + name
            save()
            started = time.monotonic()
            assert kube('get', 'pod', name)['metadata']['uid'] == original['uid'], 'Pod changed before its rollout step'
            run(['kubectl', '-n', 'vela-system', 'patch', 'sts', 'nats', '--type=merge', '-p',
                 json.dumps({'spec': {'updateStrategy': {'rollingUpdate': {'partition': ordinal}}}})])
            while time.monotonic() - started < 300:
                try:
                    current = snapshot(name)
                    if (current['uid'] != original['uid'] and quorum(current) and current['max_memory'] == limit
                            and cluster_ready([snapshot(x['pod']) for x in before])):
                        assert current['claims'] == original['claims'], 'PVC binding changed'
                        # Auto file capacity is established at process start; allow a
                        # tiny filesystem-stat difference, never a PVC size change.
                        assert abs(current['max_storage'] - original['max_storage']) < 1024**3
                        result['replacements'].append({'before_uid': original['uid'], 'after': current,
                                                       'seconds': round(time.monotonic() - started, 2)})
                        save()
                        break
                except (RuntimeError, OSError, KeyError, subprocess.TimeoutExpired):
                    pass
                time.sleep(3)
            else:
                raise RuntimeError('replacement did not recover; remaining Pods untouched')
        result.update(phase='complete', result='NATS_MEMORY_LIMIT_PASS', volume_capacity_changed=False,
                      stream_contract_changed=False, host_services_restarted=False)
        (directory / 'nats-statefulset-memory.patch.json').write_text(json.dumps({'spec': {
            'updateStrategy': {'type': 'RollingUpdate', 'rollingUpdate': {'partition': 0}},
            'template': {'spec': {'volumes': [{'name': 'config', 'secret': {'secretName': secret_name}}]}}}}, indent=2) + '\n')
        save()
    except Exception as error:
        result.update(phase='failed', error_type=type(error).__name__)
        save()
        raise


if __name__ == '__main__':
    main()
