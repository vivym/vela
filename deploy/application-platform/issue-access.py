#!/usr/bin/env python3
"""Issue a 10-60 minute scoped kubeconfig; never print a credential.

Install root-owned as /usr/local/sbin/vela-issue-access on a management node.
Run under sudo. The recipient must receive the file through a secure channel.
An existing output, symlink, or writable output directory is refused.
"""
import argparse
import base64
import json
import os
from pathlib import Path
import stat
import subprocess
import yaml

SCOPES = {'cluster-observer': ('vela-access', 'cluster-observer')}
for tenant in ['llm-api', 'llm-models']:
    SCOPES[tenant + '-viewer'] = (tenant, tenant + '-viewer')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('scope', choices=sorted(SCOPES))
    parser.add_argument('--output', required=True)
    parser.add_argument('--seconds', type=int, default=1800)
    args = parser.parse_args()
    if os.geteuid() != 0:
        parser.error('run as the platform operator through sudo')
    if not 600 <= args.seconds <= 3600:
        parser.error('seconds must be between 600 and 3600')
    os.umask(0o077)
    output = Path(args.output).absolute()
    parent = output.parent.stat()
    if parent.st_uid != 0 or stat.S_IMODE(parent.st_mode) & 0o022:
        parser.error('output directory must be owned by root and not group/world writable')
    # O_EXCL prevents overwriting or following a pre-existing output symlink.
    fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        kubeconfig = '/etc/rancher/rke2/rke2.yaml'
        original = yaml.safe_load(Path(kubeconfig).read_text())
        ca = original['clusters'][0]['cluster']['certificate-authority-data']
        base64.b64decode(ca, validate=True)
        namespace, account = SCOPES[args.scope]
        request = {'apiVersion': 'authentication.k8s.io/v1', 'kind': 'TokenRequest',
                   'spec': {'audiences': [], 'expirationSeconds': args.seconds}}
        p = subprocess.run(['/var/lib/rancher/rke2/bin/kubectl', '--kubeconfig', kubeconfig,
                            'create', '--raw', '/api/v1/namespaces/' + namespace + '/serviceaccounts/' + account + '/token', '-f', '-'],
                           input=json.dumps(request), capture_output=True, text=True, timeout=20)
        if p.returncode:
            raise RuntimeError('TokenRequest failed; no credential published')
        result = json.loads(p.stdout)['status']
        config = {'apiVersion': 'v1', 'kind': 'Config', 'clusters': [], 'contexts': [],
                  'current-context': args.scope + '-70',
                  'users': [{'name': args.scope, 'user': {'token': result['token']}}]}
        for suffix in ['70', '71']:
            name = args.scope + '-' + suffix
            config['clusters'].append({'name': name, 'cluster': {'server': 'https://10.1.201.' + suffix + ':6443',
                                                                  'certificate-authority-data': ca}})
            config['contexts'].append({'name': name, 'context': {'cluster': name, 'user': args.scope, 'namespace': namespace}})
        with os.fdopen(fd, 'w') as handle:
            fd = None
            json.dump(config, handle, indent=2)
            handle.write('\n')
            handle.flush()
            os.fsync(handle.fileno())
        print(json.dumps({'scope': args.scope, 'namespace': namespace, 'file': str(output),
                          'expires_at': result['expirationTimestamp'], 'mode': '0600'}))
    except Exception:
        if fd is not None:
            os.close(fd)
        output.unlink(missing_ok=True)
        raise


if __name__ == '__main__':
    main()
