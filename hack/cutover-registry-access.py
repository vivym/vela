#!/usr/bin/env python3
"""Sequentially activate prepared registry gateways after Control pull migration.

Only the approved registry containers change. Each host installer rolls its own
failure back to the saved container ID. A failure stops further cutovers, leaving
already verified authenticated hosts serving normally.
"""
import argparse
import json
import os
from pathlib import Path
import shlex
import subprocess
import time

HOSTS = ['10.1.201.70', '10.1.201.71', '10.1.201.66']


def run(args, **kwargs):
    p = subprocess.run(args, text=True, capture_output=True, **kwargs)
    if p.returncode:
        raise RuntimeError('command failed: ' + args[0] + ' ' + p.stderr[-1800:])
    return p.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', default='/opt/vela-cluster/registry-access-20260914/live')
    args = parser.parse_args()
    if os.geteuid() != 0 or run(['hostname']).strip() != 'llmpool01':
        parser.error('run as root on llmpool01')
    os.umask(0o077)
    root = Path(args.directory)
    if json.loads((root / 'prepare.json').read_text())['phase'] != 'prepared':
        raise RuntimeError('all hosts must be prepared')
    deployment = json.loads(run(['kubectl', '-n', 'vela-system', 'get', 'deployment', 'vela-control', '-o', 'json']))
    status = deployment['status']
    if status.get('observedGeneration') != deployment['metadata']['generation'] or any(status.get(k) != 2 for k in ['updatedReplicas', 'readyReplicas', 'availableReplicas']):
        raise RuntimeError('Control rollout not complete')
    pods = json.loads(run(['kubectl', '-n', 'vela-system', 'get', 'pod', '-l', 'app.kubernetes.io/name=vela-control', '-o', 'json']))['items']
    if len(pods) != 2 or any(not any(s['name'] == 'vela-release-pull-v1' for s in p['spec'].get('imagePullSecrets', [])) for p in pods):
        raise RuntimeError('all Control Pods must carry the new pull Secret')
    pull_receipt = json.loads((root / 'pull-secrets.json').read_text())
    pull_receipt.update(result='PULL_SECRETS_CONTROL_ROLLOUT_PASS', pods=[{
        'name': p['metadata']['name'], 'node': p['spec']['nodeName'], 'imagePullSecrets': p['spec']['imagePullSecrets'],
        'ready': all(c['ready'] for c in p['status']['containerStatuses'])} for p in pods])
    (root / 'pull-secrets.json').write_text(json.dumps(pull_receipt, indent=2) + '\n')
    inventory = json.loads(run(['ansible-inventory', '-i', '/home/user/fleet-ansible/inventory.ini', '--list']))
    password = next(v['ansible_become_password'] for key, v in inventory['_meta']['hostvars'].items() if v.get('ansible_host', key) == '10.1.201.11')
    env = dict(os.environ, SSHPASS=password)
    options = ['-o', 'StrictHostKeyChecking=yes', '-o', 'UserKnownHostsFile=/home/user/.ssh/known_hosts', '-o', 'ConnectTimeout=8']
    evidence = {'at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()), 'hosts': []}
    for host in HOSTS:
        # Read current receipt; only prepared or already active states are valid.
        code = '''import json,subprocess,pathlib
root=pathlib.Path('/opt/vela-registry-access')
state=json.loads((root/'receipt.json').read_text())
if state['phase'] not in ['prepared','active']:raise RuntimeError('host state requires recovery inspection')
phase='cutover' if state['phase']=='prepared' else 'verify'
p=subprocess.run(['python3',str(root/'install.py'),'--phase',phase],text=True,capture_output=True)
(root/'cutover.log').write_text(p.stdout+p.stderr)
if p.returncode:raise RuntimeError('registry cutover failed; inspect private host log and receipt')
print((root/'receipt.json').read_text())
'''
        if host == HOSTS[0]:
            output = run(['python3', '-c', code], timeout=150)
        else:
            output = run(['sshpass', '-e', 'ssh', *options, 'user@' + host, 'sudo -S -p "" python3 -c ' + shlex.quote(code)],
                         input=password + '\n', env=env, timeout=150)
        current = json.loads(output)
        evidence['hosts'].append(current)
        (root / 'cutover.json').write_text(json.dumps(evidence, indent=2) + '\n')
        print(json.dumps({'host': host, 'phase': current['phase'], 'checks': current['checks']}), flush=True)
    evidence['result'] = 'THREE_REGISTRY_AUTH_CUTOVERS_PASS'
    (root / 'cutover.json').write_text(json.dumps(evidence, indent=2) + '\n')


if __name__ == '__main__':
    main()
