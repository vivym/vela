#!/usr/bin/env python3
"""Select a ready control-plane API, then execute a kubectl command once.

Authentication and CA validation use the caller's KUBECONFIG/current context.
There is deliberately no replay of the user's command after an API error.
"""
import ipaddress
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import urllib.parse


def main(args):
    if not args:
        raise ValueError('usage: vela-kubectl KUBECTL_ARGUMENTS; select credentials with KUBECONFIG')
    forbidden = {'--server', '--kubeconfig', '--context', '--cluster', '--user',
                 '--insecure-skip-tls-verify', '--certificate-authority', '--tls-server-name',
                 '--client-certificate', '--client-key', '--token', '--as', '--as-group', '--as-uid'}
    for arg in args:
        if arg == '--':
            break  # Remaining words belong to a remote exec command, not kubectl.
        if arg.split('=', 1)[0] in forbidden or arg.startswith('-s') and not arg.startswith('--'):
            raise ValueError('connection/authentication overrides are unsupported; use KUBECONFIG')
    path = os.environ.get('VELA_KUBECTL_ENDPOINTS', '/etc/rancher/rke2/vela-api-endpoints.json')
    endpoints = json.loads(Path(path).read_text())
    if not isinstance(endpoints, list) or not 1 <= len(endpoints) <= 8:
        raise ValueError('expected one to eight API endpoints')
    for endpoint in endpoints:
        url = urllib.parse.urlsplit(endpoint)
        if url.scheme != 'https' or url.username or url.password or url.path or url.query or url.fragment or not url.port:
            raise ValueError('API endpoint must be an HTTPS IP origin with explicit port')
        ipaddress.ip_address(url.hostname)
    env = os.environ.copy()
    if 'KUBECONFIG' not in env and os.access('/etc/rancher/rke2/rke2.yaml', os.R_OK):
        env['KUBECONFIG'] = '/etc/rancher/rke2/rke2.yaml'
    binary = shutil.which('kubectl') or '/var/lib/rancher/rke2/bin/kubectl'
    for endpoint in endpoints:
        try:
            probe = subprocess.run([binary, '--server=' + endpoint, '--insecure-skip-tls-verify=false', '--request-timeout=3s',
                                    'get', '--raw=/readyz'], env=env, text=True,
                                   capture_output=True, timeout=8)
        except subprocess.TimeoutExpired:
            continue
        if probe.returncode == 0 and probe.stdout.strip() == 'ok':
            print('vela-kubectl: selected ' + endpoint, file=sys.stderr, flush=True)
            os.execvpe(binary, [binary, '--server=' + endpoint, '--insecure-skip-tls-verify=false', *args], env)
    print('vela-kubectl: no ready API passed authenticated CA validation; command not executed', file=sys.stderr)
    return 1


if __name__ == '__main__':
    try:
        sys.exit(main(sys.argv[1:]))
    except (OSError, ValueError) as error:
        print('vela-kubectl: ' + str(error), file=sys.stderr)
        sys.exit(2)
