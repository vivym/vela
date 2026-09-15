#!/usr/bin/env python3
"""Restore the live MarsLab database to a private, disposable CNPG Cluster.

Run on a management host with kubectl and the existing kubeconfig. Only an
unpredictably named schema in database postgres is written on the source.
No business Service, source Cluster spec, Secret or existing PVC is changed.
A root-only run directory receives incremental evidence, never credentials.
Failures retain the disposable restore for inspection; --cleanup removes only
UID-matching resources and the schema created by that particular invocation.
"""
import argparse
import datetime as dt
import fcntl
import json
import os
from pathlib import Path
import signal
import subprocess
import time
import uuid

NS = 'vela-system'
SOURCE = 'vela-postgres'
CLUSTER = 'clusters.postgresql.cnpg.io'
BACKUP = 'backups.postgresql.cnpg.io'
PLUGIN = 'barman-cloud.cloudnative-pg.io'


def now():
    return dt.datetime.now(dt.timezone.utc).isoformat()


def command(args, body=None):
    p = subprocess.run(args, input=body, text=True, capture_output=True, timeout=90)
    if p.returncode:
        # Executed commands contain no Secret values; omit stdout/stderr from receipts.
        raise RuntimeError('command failed: ' + ' '.join(args[:7]) + ': ' + p.stderr[-1000:])
    return p.stdout.strip()


def kube(*args):
    return json.loads(command(['kubectl', '-n', NS, *args, '-o', 'json']))


def sql(pod, statement):
    return command(['kubectl', '-n', NS, 'exec', pod, '-c', 'postgres', '--',
                    'psql', '-X', '-v', 'ON_ERROR_STOP=1', '-U', 'postgres', '-d', 'postgres', '-Atqc', statement])


def ready(c):
    return any(x['type'] == 'Ready' and x['status'] == 'True' for x in c['status'].get('conditions', []))


class Drill:
    def __init__(self, directory):
        self.directory = Path(directory)
        self.receipt_path = self.directory / 'receipt.json'
        self.receipt = {}
        self.started = time.monotonic()

    def save(self, phase, **values):
        self.receipt.update(values, phase=phase, updated_at=now())
        temporary = self.receipt_path.with_suffix('.tmp')
        temporary.write_text(json.dumps(self.receipt, indent=2) + '\n')
        temporary.replace(self.receipt_path)
        print(phase, flush=True)

    def create(self, obj):
        obj['metadata'].update(namespace=NS, labels={'vela.ai/pitr-drill': self.receipt['run_id']})
        value = json.loads(command(['kubectl', 'create', '-f', '-', '-o', 'json'], json.dumps(obj)))
        self.receipt['created_resources'].append({'kind': value['kind'], 'name': value['metadata']['name'], 'uid': value['metadata']['uid']})
        self.save(self.receipt['phase'])
        return value

    def source(self):
        c = kube('get', CLUSTER, SOURCE)
        if c['metadata']['uid'] != self.receipt['source_uid']:
            raise RuntimeError('source Cluster UID changed')
        return c['status']['currentPrimary']

    def wait(self, predicate, seconds, label):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            value = predicate()
            if value:
                return value
            time.sleep(3)
        raise RuntimeError(label + ' deadline exceeded; inspect existing restore before retrying')

    def run(self):
        if self.receipt_path.exists():
            raise RuntimeError('existing receipt: inspect or clean up; never start over implicitly')
        c = kube('get', CLUSTER, SOURCE)
        if not ready(c) or c['status'].get('readyInstances') != 3:
            raise RuntimeError('source must have three healthy instances')
        if not any(p['name'] == PLUGIN and p.get('isWALArchiver') for p in c['spec'].get('plugins', [])):
            raise RuntimeError('source WAL archiver differs from expected plugin')
        node_info = kube('get', 'nodes.longhorn.io', '-n', 'longhorn-system')['items']
        cpu_disks = [v for n in node_info if n['metadata']['name'] in ('llmpool01', 'llmpool02') for v in n['status'].get('diskStatus', {}).values()]
        if len(cpu_disks) != 2 or any(v['storageAvailable'] < 40 * 1024**3 for v in cpu_disks):
            raise RuntimeError('insufficient physical free space for the disposable restore')
        token = uuid.uuid4().hex
        run_id = 'pitr-' + token[:12]
        schema = 'vela_pitr_' + token
        primary = c['status']['currentPrimary']
        total = int(sql(primary, 'SELECT sum(pg_database_size(oid)) FROM pg_database'))
        if total > 512 * 1024**2:
            raise RuntimeError('source exceeds bounded 2Gi restore data plan')
        self.save('preflight', run_id=run_id, started_at=now(), source_cluster=SOURCE,
                  source_uid=c['metadata']['uid'], source_primary=primary, source_database_bytes=total,
                  schema=schema, schema_created=False, created_resources=[], image=c['spec']['imageName'],
                  scope='Live base backup and WAL timestamp restore on CPU management node; not site loss or application replay')
        # CREATE fails on collision. Never adopt or drop another run's schema.
        sql(primary, f'CREATE SCHEMA {schema}; CREATE TABLE {schema}.markers (name text PRIMARY KEY, created_at timestamptz DEFAULT clock_timestamp()); INSERT INTO {schema}.markers(name) VALUES (\'base\');')
        self.save('marker-created', schema_created=True)
        bname = 'vela-' + run_id
        self.create({'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Backup', 'metadata': {'name': bname},
                     'spec': {'method': 'plugin', 'target': 'primary', 'cluster': {'name': SOURCE}, 'pluginConfiguration': {'name': PLUGIN}}})
        def backup_done():
            b = kube('get', BACKUP, bname)
            if b['status'].get('phase') == 'failed':
                raise RuntimeError('CNPG backup failed')
            return b if b['status'].get('phase') == 'completed' else None
        b = self.wait(backup_done, 300, 'backup')
        self.save('backup-complete', backup={k: b['status'].get(k) for k in ['backupId', 'backupName', 'beginWal', 'endWal', 'startedAt', 'stoppedAt', 'phase']})
        primary = self.source()
        # This marker is AFTER the base backup, proving WAL replay, not only base extraction.
        sql(primary, f"INSERT INTO {schema}.markers(name) VALUES ('before-target')")
        target = sql(primary, "SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"')")
        # Separate psql calls ensure COMMIT precedes pg_switch_wal. A single multi-
        # statement -c would commit after the switch and leave COMMIT unarchived.
        sql(primary, f"INSERT INTO {schema}.markers(name) VALUES ('after-target')")
        wal = sql(primary, 'SELECT pg_walfile_name(pg_current_wal_lsn())')
        sql(primary, 'SELECT pg_switch_wal()')
        self.save('waiting-for-archive', recovery_target=target, required_wal=wal)
        def archived():
            value = json.loads(sql(self.source(), "SELECT json_build_object('wal',last_archived_wal,'at',last_archived_time,'failed_count',failed_count) FROM pg_stat_archiver"))
            return value if value['wal'] and value['wal'][:8] == wal[:8] and value['wal'] >= wal else None
        archive = self.wait(archived, 180, 'required WAL archiving')
        self.save('wal-archived', archive=archive)
        rname = 'vela-' + run_id + '-restore'
        # Deny ingress before creating any restored database endpoint.
        self.create({'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy', 'metadata': {'name': rname},
                     'spec': {'podSelector': {'matchLabels': {'cnpg.io/cluster': rname}}, 'policyTypes': ['Ingress'], 'ingress': [{'from': [{'namespaceSelector': {'matchLabels': {'kubernetes.io/metadata.name': 'cnpg-system'}}, 'podSelector': {'matchLabels': {'app.kubernetes.io/name': 'cloudnative-pg'}}}], 'ports': [{'port': 8000, 'protocol': 'TCP'}]}]}})
        restore_start = time.monotonic()
        self.create({'apiVersion': 'postgresql.cnpg.io/v1', 'kind': 'Cluster', 'metadata': {'name': rname}, 'spec': {
            'instances': 1, 'imageName': c['spec']['imageName'], 'imagePullPolicy': 'IfNotPresent',
            'enableSuperuserAccess': False,
            'bootstrap': {'recovery': {'source': 'source', 'recoveryTarget': {'targetTime': target}}},
            'externalClusters': [{'name': 'source', 'plugin': {'name': PLUGIN, 'parameters': {'barmanObjectName': 'vela-postgres-backup', 'serverName': SOURCE}}}],
            'affinity': {'nodeSelector': {'vela.ai/control-plane-tier': 'cpu'}},
            'storage': {'size': '2Gi', 'storageClass': 'longhorn'},
            'walStorage': {'size': '2Gi', 'storageClass': 'longhorn'},
            'resources': {'requests': {'cpu': '100m', 'memory': '256Mi'}, 'limits': {'cpu': '2', 'memory': '2Gi'}}}})
        self.save('restore-running', restore_cluster=rname, restore_started_at=now())
        def restored():
            obj = kube('get', CLUSTER, rname)
            return obj if ready(obj) else None
        restored_cluster = self.wait(restored, 480, 'restore')
        restore_primary = restored_cluster['status']['currentPrimary']
        result = json.loads(sql(restore_primary, f"SELECT json_build_object('base', count(*) FILTER (WHERE name='base'), 'before_target', count(*) FILTER (WHERE name='before-target'), 'after_target', count(*) FILTER (WHERE name='after-target'), 'in_recovery',pg_is_in_recovery()) FROM {schema}.markers"))
        if result != {'base': 1, 'before_target': 1, 'after_target': 0, 'in_recovery': False}:
            raise RuntimeError('PITR marker comparison failed: ' + json.dumps(result))
        self.save('verified', restore_primary=restore_primary, marker_result=result,
                  restore_seconds=round(time.monotonic()-restore_start, 2), total_seconds=round(time.monotonic()-self.started, 2))
        self.cleanup()
        self.save('complete', result='LIVE_PITR_PASS')

    def cleanup(self):
        # Exact names and UIDs only; source Cluster, data and previous backup objects stay intact.
        self.receipt = json.loads(self.receipt_path.read_text())
        names = {'Cluster': CLUSTER, 'Backup': BACKUP, 'NetworkPolicy': 'networkpolicies.networking.k8s.io'}
        for r in reversed(self.receipt['created_resources']):
            items = kube('get', names[r['kind']])['items']
            found = next((i for i in items if i['metadata']['name'] == r['name']), None)
            if not found:
                continue
            if found['metadata']['uid'] != r['uid']:
                raise RuntimeError('cleanup UID mismatch')
            command(['kubectl', '-n', NS, 'delete', names[r['kind']], r['name'], '--wait=true', '--timeout=80s'])
            if r['kind'] == 'Cluster':
                # CNPG owns its PVCs; verify collection has completed, do not force detach.
                self.wait(lambda: not kube('get', 'pods,pvc', '-l', 'cnpg.io/cluster='+r['name'])['items'], 90, 'restore PVC cleanup')
        if self.receipt['schema_created']:
            sql(self.source(), f"DROP SCHEMA IF EXISTS {self.receipt['schema']} CASCADE")
            if sql(self.source(), f"SELECT count(*) FROM pg_namespace WHERE nspname='{self.receipt['schema']}'") != '0':
                raise RuntimeError('schema cleanup not verified')
        source_now = kube('get', CLUSTER, SOURCE)
        if source_now['status'].get('readyInstances') != 3 or not ready(source_now):
            raise RuntimeError('source lost readiness')
        self.save('cleaned', cleanup_verified=True, source_ready_instances=3,
                  backup_objects_note='drill Backup CR removed; backup/WAL objects retained under existing retention policy')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-directory', required=True)
    parser.add_argument('--cleanup', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    Path(args.run_directory).mkdir(mode=0o700, parents=True, exist_ok=True)
    with open('/run/vela-cnpg-live-pitr.lock', 'w') as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        def interrupted(signum, frame):
            raise RuntimeError('signal received; stopped, restore retained for explicit cleanup')
        signal.signal(signal.SIGTERM, interrupted)
        signal.signal(signal.SIGINT, interrupted)
        drill = Drill(args.run_directory)
        try:
            drill.cleanup() if args.cleanup else drill.run()
        except Exception as e:
            if drill.receipt:
                drill.save('failed', error=str(e), result='NOT_PASS')
            raise


if __name__ == '__main__':
    main()
