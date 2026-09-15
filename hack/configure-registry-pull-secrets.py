#!/usr/bin/env python3
"""Install scoped registry pull Secrets, preserving existing ServiceAccount refs.

Run as root on llmpool01 after registry-access prepare. Credentials never enter
arguments or stdout. This changes the Control Pod template once, using its
existing rolling-update strategy; it does not restart any host service.
"""
import argparse
import base64
import datetime
import json
import os
from pathlib import Path
import subprocess

NAME = 'vela-release-pull-v1'
HOSTS = ['10.1.201.70', '10.1.201.71', '10.1.201.66']
SCOPES = [('vela-system', 'vela-control', 'node-pull'),
          ('llm-api', 'llm-api-runtime', 'llm-api-pull'),
          ('llm-models', 'llm-models-runtime', 'llm-models-pull')]


def kubectl(*args, value=None):
    result = subprocess.run(['kubectl', *args], text=True, capture_output=True,
                            input=None if value is None else json.dumps(value), timeout=60)
    if result.returncode:
        raise RuntimeError('kubectl operation failed: ' + result.stderr[:1200])
    return result.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', default='/opt/vela-cluster/registry-access-20260914/live')
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('run as root')
    os.umask(0o077)
    root = Path(args.directory)
    credentials_path = root / 'credentials.json'
    if credentials_path.stat().st_uid != 0 or credentials_path.stat().st_mode & 0o077:
        raise RuntimeError('private root-owned credentials required')
    if json.loads((root / 'prepare.json').read_text())['phase'] != 'prepared':
        raise RuntimeError('all registry hosts must be prepared first')
    credentials = json.loads(credentials_path.read_text())
    receipt = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'scopes': []}
    deployment = json.loads(kubectl('-n', 'vela-system', 'get', 'deployment', 'vela-control', '-o', 'json'))
    if deployment['spec']['replicas'] != 2 or deployment['status'].get('readyReplicas') != 2:
        raise RuntimeError('Control must have two Ready replicas before the update')
    if deployment['spec']['strategy']['rollingUpdate'] != {'maxSurge': 0, 'maxUnavailable': 1}:
        raise RuntimeError('unexpected Control update strategy')
    backup = root / 'control-before-pull-secret.json'
    if not backup.exists():
        backup.write_text(json.dumps(deployment, indent=2) + '\n')
    for namespace, account, user in SCOPES:
        auth = base64.b64encode((user + ':' + credentials[user]).encode()).decode()
        dockerconfig = json.dumps({'auths': {host + ':5005': {'auth': auth} for host in HOSTS}})
        secret = {'apiVersion': 'v1', 'kind': 'Secret',
                  'metadata': {'name': NAME, 'namespace': namespace,
                               'labels': {'vela.ai/managed-by': 'registry-access'}},
                  'type': 'kubernetes.io/dockerconfigjson', 'immutable': True,
                  'data': {'.dockerconfigjson': base64.b64encode(dockerconfig.encode()).decode()}}
        existing = kubectl('-n', namespace, 'get', 'secret', NAME, '--ignore-not-found', '-o', 'json')
        if existing:
            actual = json.loads(existing)
            if actual.get('type') != secret['type'] or actual.get('data') != secret['data'] or not actual.get('immutable'):
                raise RuntimeError('existing pull Secret differs; use an explicit credential revision')
        else:
            kubectl('create', '-f', '-', value=secret)
        sa = json.loads(kubectl('-n', namespace, 'get', 'sa', account, '-o', 'json'))
        previous = sa.get('imagePullSecrets', [])
        refs = previous if any(x['name'] == NAME for x in previous) else previous + [{'name': NAME}]
        if refs != previous:
            kubectl('-n', namespace, 'patch', 'sa', account, '--type=merge', '--patch-file=/dev/stdin',
                    value={'metadata': {'resourceVersion': sa['metadata']['resourceVersion']}, 'imagePullSecrets': refs})
        receipt['scopes'].append({'namespace': namespace, 'account': account, 'registry_identity': user,
                                  'secret': NAME, 'immutable': True, 'preserved_refs': previous})
    spec = deployment['spec']['template']['spec']
    previous = spec.get('imagePullSecrets', [])
    refs = previous if any(x['name'] == NAME for x in previous) else previous + [{'name': NAME}]
    if refs != previous:
        kubectl('-n', 'vela-system', 'patch', 'deployment', 'vela-control', '--type=merge', '--patch-file=/dev/stdin',
                value={'metadata': {'resourceVersion': deployment['metadata']['resourceVersion']},
                       'spec': {'template': {'spec': {'imagePullSecrets': refs}}}})
    receipt['control_generation'] = json.loads(kubectl('-n', 'vela-system', 'get', 'deployment', 'vela-control', '-o', 'json'))['metadata']['generation']
    receipt['result'] = 'PULL_SECRETS_CONFIGURED_ROLLOUT_PENDING'
    (root / 'pull-secrets.json').write_text(json.dumps(receipt, indent=2) + '\n')
    print(json.dumps(receipt))


if __name__ == '__main__':
    main()
