#!/usr/bin/env python3
"""Launch/inspect/clean one bounded KVM worker with its primary server blocked.

Run only on llmpool01. The VM has a fresh OS/data directory, 4GiB RAM, two CPUs,
a 12GiB sparse root disk, a NoSchedule taint and disabled Longhorn scheduling.
It receives the existing agent token privately and runs the packaged RKE2 unit.
No host RKE2 service, GPU driver, worker data disk or physical host is restarted.
The temporary systemd VM unit is capped at 30 minutes. Inspect that exact unit
and receipt before retrying; a failed or timed-out observation never relaunches it.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import time
import uuid

import yaml


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def run(args, body=None, timeout=45):
    p = subprocess.run(args, input=body, text=True, capture_output=True, timeout=timeout)
    if p.returncode:
        # Some inputs contain credentials. Never echo the command body or output.
        raise RuntimeError('command failed: ' + args[0] + ' (exit ' + str(p.returncode) + ')')
    return p.stdout.strip()


def kube(*args):
    return json.loads(run(['kubectl', *args, '-o', 'json']))


class Drill:
    def __init__(self, directory, inputs):
        if socket.gethostname() != 'llmpool01':
            raise RuntimeError('restricted to the preflighted CPU management host')
        self.directory = Path(directory)
        self.inputs = Path(inputs)
        self.receipt_path = self.directory / 'receipt.json'
        self.receipt = json.loads(self.receipt_path.read_text()) if self.receipt_path.exists() else {}

    def save(self, **values):
        self.receipt.update(values, updated_at=now())
        path = self.receipt_path.with_suffix('.tmp')
        path.write_text(json.dumps(self.receipt, indent=2) + '\n')
        path.replace(self.receipt_path)

    def start(self):
        if self.receipt:
            raise RuntimeError('existing receipt: inspect the same VM')
        if not Path('/dev/kvm').exists() or shutil.disk_usage(self.directory).free < 32 * 1024**3:
            raise RuntimeError('bounded VM resource preflight failed')
        image = self.inputs / 'noble-server-cloudimg-amd64.img'
        checksum = next(line.split()[0] for line in (self.inputs / 'SHA256SUMS').read_text().splitlines()
                        if line.endswith(' *' + image.name))
        run(['gpgv', '--keyring', '/usr/share/keyrings/ubuntu-cloudimage-keyring.gpg',
             str(self.inputs / 'SHA256SUMS.gpg'), str(self.inputs / 'SHA256SUMS')])
        with image.open('rb') as file:
            if hashlib.file_digest(file, 'sha256').hexdigest() != checksum:
                raise RuntimeError('cloud image differs from signed checksum')
        name = 'vela-bootstrap-drill-' + uuid.uuid4().hex[:10]
        unit = name + '.service'
        if any(x['metadata']['name'] == name for x in kube('get', 'nodes')['items']):
            raise RuntimeError('unexpected pre-existing validation node')
        self.save(phase='preparing', started_at=now(), node=name, unit=unit,
                  host_boot_id=Path('/proc/sys/kernel/random/boot_id').read_text().strip(),
                  source_image_sha256=checksum, resources={'memory_mib': 4096, 'cpus': 2, 'root_disk_gib': 12},
                  scope='Fresh RKE2 worker registration with primary 6443/9345 blocked inside a tainted KVM guest; not production cross-node CNI qualification')
        # A pre-created Longhorn Node is unsafe: the controller deletes it while
        # the Kubernetes Node is absent. Require the persistent disk opt-in policy.
        setting = kube('-n', 'longhorn-system', 'get', 'settings.longhorn.io', 'create-default-disk-labeled-nodes')
        if setting['value'] != 'true':
            raise RuntimeError('Longhorn must require an explicit disk-creation label')
        self.save(longhorn_disk_opt_in_verified=True)
        share = self.directory / 'media'
        share.mkdir(mode=0o700)
        source = self.inputs / 'source'
        for name in ['rke2-bootstrap-ha.py', 'rke2-bootstrap-endpoints.json', 'rke2-agent-bootstrap-ha.conf', 'guest.sh']:
            shutil.copy2(source / name, share / name)
        for origin, destination in [('/usr/local/bin/rke2', 'rke2'),
                ('/usr/local/lib/systemd/system/rke2-agent.service', 'rke2-agent.service'),
                ('/var/lib/rancher/rke2/server/tls/server-ca.crt', 'server-ca.crt'),
                ('/etc/rancher/rke2/registry-ca.crt', 'registry-ca.crt'),
                ('/etc/rancher/rke2/registries.yaml', 'registries.yaml'),
                ('/var/lib/rancher/rke2/server/agent-token', 'token')]:
            shutil.copyfile(origin, share / destination)
            (share / destination).chmod(0o600)
        config = {'server': 'https://10.1.201.70:9345', 'token-file': '/etc/rancher/rke2/token',
                  'node-name': self.receipt['node'],
                  'node-label': ['vela.ai/validation=bootstrap', 'vela.ai/node-role=bootstrap-validation'],
                  'node-taint': ['vela.ai/validation=bootstrap:NoSchedule'],
                  'resolv-conf': '/run/systemd/resolve/resolv.conf'}
        (share / 'config.yaml').write_text(yaml.safe_dump(config))
        key = self.directory / 'ssh-key'
        run(['ssh-keygen', '-q', '-t', 'ed25519', '-N', '', '-f', str(key)])
        cloud = {'hostname': self.receipt['node'], 'manage_etc_hosts': True, 'ssh_pwauth': False,
                 'users': [{'name': 'vela-validation', 'sudo': 'ALL=(ALL) NOPASSWD:ALL', 'shell': '/bin/bash',
                            'lock_passwd': True, 'ssh_authorized_keys': [key.with_suffix('.pub').read_text().strip()]}],
                 'runcmd': [['mkdir', '-p', '/mnt/vela-bootstrap'],
                            ['mount', '-o', 'ro', '/dev/disk/by-label/VELA_BOOT', '/mnt/vela-bootstrap'],
                            ['/bin/bash', '/mnt/vela-bootstrap/guest.sh']]}
        (self.directory / 'user-data').write_text('#cloud-config\n' + yaml.safe_dump(cloud))
        (self.directory / 'meta-data').write_text(yaml.safe_dump({'instance-id': self.receipt['node'], 'local-hostname': self.receipt['node']}))
        run(['cloud-localds', str(self.directory / 'seed.iso'), str(self.directory / 'user-data'), str(self.directory / 'meta-data')])
        run(['genisoimage', '-quiet', '-J', '-R', '-V', 'VELA_BOOT', '-o', str(self.directory / 'bootstrap.iso'), str(share)])
        disk = self.directory / 'root.qcow2'
        run(['qemu-img', 'create', '-f', 'qcow2', '-F', 'qcow2', '-b', str(image), str(disk), '12G'])
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        self.save(phase='launching', ssh_port=port)
        run(['systemd-run', '--unit=' + unit, '--property=Type=exec', '--property=MemoryMax=6G',
             '--property=CPUQuota=200%', '--property=RuntimeMaxSec=1800', '--property=KillMode=control-group',
             'qemu-system-x86_64', '-name', self.receipt['node'], '-enable-kvm', '-cpu', 'host', '-smp', '2', '-m', '4096',
             '-display', 'none', '-serial', 'file:' + str(self.directory / 'console.log'), '-monitor', 'none',
             '-drive', 'file=' + str(disk) + ',format=qcow2,if=virtio',
             '-drive', 'file=' + str(self.directory / 'seed.iso') + ',media=cdrom,format=raw,readonly=on',
             '-drive', 'file=' + str(self.directory / 'bootstrap.iso') + ',media=cdrom,format=raw,readonly=on',
             '-netdev', f'user,id=net0,hostfwd=tcp:127.0.0.1:{port}-:22', '-device', 'virtio-net-pci,netdev=net0'])
        pid = int(run(['systemctl', 'show', unit, '-p', 'MainPID', '--value']))
        if pid <= 0:
            raise RuntimeError('VM process not live after launch')
        self.save(phase='booting', vm_pid=pid, started_vm_pid=pid)
        return self.receipt

    def guest(self, command):
        return run(['ssh', '-i', str(self.directory / 'ssh-key'), '-p', str(self.receipt['ssh_port']),
                    '-o', 'IdentitiesOnly=yes', '-o', 'ConnectTimeout=5', '-o', 'StrictHostKeyChecking=accept-new',
                    '-o', 'UserKnownHostsFile=' + str(self.directory / 'known-hosts'),
                    'vela-validation@127.0.0.1', command], timeout=30)

    def inspect(self):
        pid, state = self.vm_state()
        self.save(vm_pid=pid, vm_state=state)
        if not pid or state != 'active':
            self.save(phase='vm-stopped', result='NOT_PASS')
            return self.receipt
        try:
            data = self.guest("sudo python3 -c \"import json,pathlib,subprocess; p=pathlib.Path('/var/lib/vela-bootstrap-validation'); print(json.dumps({'fresh':json.loads((p/'fresh.json').read_text()),'selection':json.loads((p/'selection-before-agent.json').read_text()),'override':pathlib.Path('/etc/rancher/rke2/config.yaml.d/99-vela-bootstrap-ha.yaml').read_text(),'blocked':subprocess.run(['iptables','-w','5','-C','OUTPUT','-d','10.1.201.70/32','-p','tcp','-m','multiport','--dports','6443,9345','-m','comment','--comment','vela-fresh-bootstrap','-j','REJECT','--reject-with','tcp-reset'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0,'enabled':subprocess.check_output(['systemctl','is-enabled','rke2-agent'],text=True).strip()}))\"")
            self.save(guest=json.loads(data))
        except (RuntimeError, subprocess.TimeoutExpired):
            self.save(phase='booting-or-agent-starting')
            return self.receipt
        nodes = kube('get', 'nodes')['items']
        node = next((x for x in nodes if x['metadata']['name'] == self.receipt['node']), None)
        if not node:
            self.save(phase='waiting-for-registration')
            return self.receipt
        self.save(node_uid=node['metadata']['uid'], registered_at=node['metadata']['creationTimestamp'],
                  node_ready=any(x['type'] == 'Ready' and x['status'] == 'True' for x in node.get('status', {}).get('conditions', [])))
        lh = next((x for x in kube('-n', 'longhorn-system', 'get', 'nodes.longhorn.io')['items']
                   if x['metadata']['name'] == self.receipt['node']), None)
        if lh:
            if lh['spec'].get('disks'):
                raise RuntimeError('unexpected disk on disposable VM')
            if any(x['spec'].get('nodeID') == self.receipt['node'] for x in
                   kube('-n', 'longhorn-system', 'get', 'replicas.longhorn.io')['items']):
                raise RuntimeError('unexpected replica on disposable VM')
            run(['kubectl', '-n', 'longhorn-system', 'patch', 'nodes.longhorn.io', self.receipt['node'],
                 '--type=merge', '-p', json.dumps({'metadata': {'resourceVersion': lh['metadata']['resourceVersion']},
                                                'spec': {'allowScheduling': False}})])
            self.save(longhorn_node_uid=lh['metadata']['uid'], longhorn_disks={}, longhorn_replicas=0)
        guest = self.receipt['guest']
        if not guest['fresh']['agent_directory_absent'] or not guest['blocked'] or guest['enabled'] != 'enabled':
            raise RuntimeError('fresh boot, fault injection or boot persistence invariant failed')
        selection = guest['selection']
        if selection['selected'] != 'https://10.1.201.71:9345' or selection['attempts'][0]['available']:
            raise RuntimeError('fresh bootstrap did not select the secondary')
        if 'https://10.1.201.71:9345' not in guest['override']:
            raise RuntimeError('RKE2 config differs from selected supervisor')
        if self.receipt['node_ready']:
            self.save(phase='verified', result='FRESH_BOOTSTRAP_PRIMARY_LOSS_PASS')
        return self.receipt

    def vm_state(self):
        process = subprocess.run(['systemctl', 'show', self.receipt['unit'], '-p', 'LoadState',
                                  '-p', 'MainPID', '-p', 'ActiveState'], text=True, capture_output=True)
        values = dict(line.split('=', 1) for line in process.stdout.splitlines() if '=' in line)
        if process.returncode and values.get('LoadState') != 'not-found':
            raise RuntimeError('cannot inspect the owned VM unit')
        return int(values.get('MainPID', '0')), values.get('ActiveState', 'inactive')

    def cleanup(self):
        if not self.receipt:
            raise RuntimeError('no owned VM receipt')
        if Path('/proc/sys/kernel/random/boot_id').read_text().strip() != self.receipt['host_boot_id']:
            raise RuntimeError('host rebooted; inspect ownership before cleanup')
        if self.receipt.get('unit'):
            pid, state = self.vm_state()
            if pid or state in ('active', 'activating', 'deactivating', 'failed'):
                run(['systemctl', 'stop', self.receipt['unit']])
            if self.vm_state()[0]:
                raise RuntimeError('VM still running')
        name = self.receipt['node']
        for kind, namespace, key in [('node', None, 'node_uid'), ('nodes.longhorn.io', 'longhorn-system', 'longhorn_node_uid')]:
            prefix = ['-n', namespace] if namespace else []
            found = next((x for x in kube(*prefix, 'get', kind)['items'] if x['metadata']['name'] == name), None)
            if not found:
                continue
            if key not in self.receipt or found['metadata']['uid'] != self.receipt[key]:
                raise RuntimeError('resource ownership differs during cleanup')
            if kind == 'nodes.longhorn.io':
                replicas = kube('-n', namespace, 'get', 'replicas.longhorn.io')['items']
                if any(x['spec'].get('nodeID') == name for x in replicas):
                    raise RuntimeError('unexpected Longhorn replica on disposable VM')
            run(['kubectl', *prefix, 'delete', kind, name, '--wait=true', '--timeout=60s'], timeout=70)
        # The node controller owns the node-password Secret; verify it is removed.
        secret = name + '.node-password.rke2'
        for _ in range(30):
            pods = kube('get', 'pods', '-A', '--field-selector', 'spec.nodeName=' + name)['items']
            password_exists = bool(run(['kubectl', '-n', 'kube-system', 'get', 'secret', secret,
                                       '--ignore-not-found', '-o', 'name']))
            if not pods and not password_exists:
                break
            time.sleep(2)
        else:
            raise RuntimeError('guest Pod or node-password cleanup not complete')
        for filename in ['bootstrap.iso', 'root.qcow2', 'ssh-key']:
            (self.directory / filename).unlink(missing_ok=True)
        if (self.directory / 'media').exists():
            shutil.rmtree(self.directory / 'media')
        self.save(phase='complete', cleanup_verified=True, host_rebooted=False,
                  vm_pid=0, vm_state='inactive', guest_credentials_removed=True)
        return self.receipt


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('action', choices=['start', 'inspect', 'cleanup'])
    parser.add_argument('--run-directory', required=True)
    parser.add_argument('--inputs', default='/opt/vela-cluster/bootstrap-ha-20260914')
    args = parser.parse_args()
    os.umask(0o077)
    Path(args.run_directory).mkdir(mode=0o700, parents=True, exist_ok=True)
    drill = Drill(args.run_directory, args.inputs)
    print(json.dumps(getattr(drill, args.action)(), indent=2))


if __name__ == '__main__':
    main()
