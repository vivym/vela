#!/usr/bin/env python3
"""Evict one NATS Pod at a time and verify a prepared isolated R3 test stream.

Run on a management host after hack/nats-live-recovery has prepared STATE_PATH.
Respects the NATS PodDisruptionBudget and checks Pod UID replacement, PVC name
stability, every replica's currency, message SHA256 and durable redelivery.
No host service, GPU driver, PVC or business stream is deleted. On failure the
current Pod is allowed to recover normally, and remaining members are untouched.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess
import time


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def kube(*args):
    return json.loads(subprocess.check_output(['kubectl', '-n', 'vela-system', *args, '-o', 'json'], timeout=30))


def probe(directory, mode):
    p = subprocess.run([str(directory/'probe'), mode, str(directory/'state.json')], capture_output=True, text=True, timeout=40)
    if p.returncode:
        raise RuntimeError(p.stderr.strip())
    return json.loads(p.stdout)


def all_current(result):
    for name in ('cluster', 'consumer_cluster'):
        cluster = result.get(name, {})
        if not cluster.get('leader') or len(cluster.get('replicas', [])) != 2:
            return False
        if any(not p.get('current') or p.get('offline') or p.get('lag', 0) for p in cluster['replicas']):
            return False
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-directory', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    directory = Path(args.run_directory)
    receipt_path = directory/'recovery.json'
    if receipt_path.exists():
        raise RuntimeError('existing recovery receipt: inspect before retrying')
    result = {'started_at': now(), 'phase': 'preflight', 'replacements': [],
              'scope': 'Three sequential NATS Pod replacements; R3 file-backed message and durable consumer redelivery. No full-cluster outage or PostgreSQL Outbox replay.'}
    def save():
        result['updated_at'] = now()
        receipt_path.write_text(json.dumps(result, indent=2)+'\n')
        print(result['phase'], flush=True)
    save()
    try:
        baseline = probe(directory, 'verify')
        if not all_current(baseline):
            raise RuntimeError('initial JetStream quorum incomplete')
        result['baseline'] = baseline
        source = kube('get', 'statefulsets.apps', 'nats')
        result['statefulset_uid'] = source['metadata']['uid']
        if source['spec']['replicas'] != 3 or source['status'].get('readyReplicas') != 3:
            raise RuntimeError('NATS not fully ready before drill')
        # Start with the stream leader to exercise election. All are Pod
        # replacements only, including the shared .66 member, never host reboot.
        initial_leader = baseline['cluster']['leader']
        targets = [initial_leader] + [p for p in ['nats-0', 'nats-1', 'nats-2'] if p != initial_leader]
        for name in targets:
            if kube('get', 'statefulsets.apps', 'nats')['metadata']['uid'] != result['statefulset_uid']:
                raise RuntimeError('NATS StatefulSet changed')
            old = kube('get', 'pods', name)
            if old['spec']['nodeName'] not in ['llmpool01', 'llmpool02', 'marslab-gpu-01']:
                raise RuntimeError('unexpected NATS host')
            old_claims = [v['persistentVolumeClaim']['claimName'] for v in old['spec']['volumes'] if 'persistentVolumeClaim' in v]
            started = time.monotonic()
            change = {'pod': name, 'node': old['spec']['nodeName'], 'old_uid': old['metadata']['uid'], 'pvc_names': old_claims, 'started_at': now()}
            result['replacements'].append(change)
            result['phase'] = 'evicting-'+name
            save()
            eviction = {'apiVersion':'policy/v1','kind':'Eviction','metadata':{'name':name,'namespace':'vela-system'},'deleteOptions':{'preconditions':{'uid':old['metadata']['uid']}}}
            subprocess.run(['kubectl','create','--raw',f'/api/v1/namespaces/vela-system/pods/{name}/eviction','-f','-'],input=json.dumps(eviction),text=True,check=True,capture_output=True,timeout=30)
            last_error = ''
            while time.monotonic()-started < 240:
                try:
                    new = kube('get', 'pods', name)
                    pod_ready = any(c['type']=='Ready' and c['status']=='True' for c in new['status'].get('conditions', []))
                    if new['metadata']['uid'] == old['metadata']['uid'] or not pod_ready:
                        time.sleep(3)
                        continue
                    claims = [v['persistentVolumeClaim']['claimName'] for v in new['spec']['volumes'] if 'persistentVolumeClaim' in v]
                    if claims != old_claims:
                        raise RuntimeError('PVC binding changed')
                    verified = probe(directory, 'verify')
                    if not all_current(verified):
                        raise RuntimeError('consumer replicas not fully current yet')
                    change.update(new_uid=new['metadata']['uid'], ready_and_verified_seconds=round(time.monotonic()-started, 2), evidence=verified)
                    break
                except (RuntimeError, subprocess.SubprocessError) as error:
                    last_error = str(error)
                    time.sleep(3)
            else:
                raise RuntimeError('member recovery timed out: '+last_error)
            result['phase']='verified-'+name
            save()
        result['redelivery'] = probe(directory, 'redelivery')
        if not all_current(result['redelivery']):
            raise RuntimeError('final consumer quorum incomplete')
        result['cleanup'] = probe(directory, 'cleanup')
        source = kube('get', 'statefulsets.apps', 'nats')
        if source['status'].get('readyReplicas') != 3:
            raise RuntimeError('NATS not fully ready after drill')
        result.update(phase='complete', result='NATS_ROLLING_PERSISTENCE_PASS', final_ready_replicas=3)
        save()
    except Exception as error:
        result.update(phase='failed', result='NOT_PASS', error=str(error))
        save()
        raise


if __name__=='__main__':
    main()
