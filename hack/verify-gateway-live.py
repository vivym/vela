#!/usr/bin/env python3
"""Verify private APISIX TLS, Grafana subpath and optional per-Pod rate limits.

Run on a management host with the existing kubeconfig. Secrets remain in
memory. TLS uses the current cert-manager gateway CA as its explicit
trust anchor; this does not assert public PKI trust. --exercise-limit sends
125 unauthenticated API reads to each gateway Pod, consuming only this
management host's rate-limit counters. It changes no route or workload.
"""
import argparse
import base64
import datetime
import hashlib
import http.client
import json
import re
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.request
from collections import Counter


def kube(namespace, kind, name=None, selector=None):
    args = ['kubectl', '-n', namespace, 'get', kind]
    if name:
        args.append(name)
    if selector:
        args.extend(['-l', selector])
    return json.loads(subprocess.check_output(args + ['-o', 'json'], timeout=30))


def request(url, headers=None):
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=15) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as response:
        return response.code, response.read()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--exercise-limit', action='store_true')
    args = parser.parse_args()
    admin = kube('apisix', 'secrets', 'apisix-admin-credentials')['data']
    service = kube('apisix', 'services', 'apisix-admin')['spec']
    port = next(x['port'] for x in service['ports'] if x['port'] == 9180)
    endpoint = f"http://{service['clusterIP']}:{port}/apisix/admin/"
    code, body = request(endpoint + 'ssls/vela-gateway-default', {
        'X-API-KEY': base64.b64decode(admin['admin']).decode()})
    if code != 200:
        raise RuntimeError('cannot read current gateway certificate')
    certificate = json.loads(body)['value']['cert']
    material = kube('apisix', 'secrets', 'vela-gateway-tls')['data']
    ca = base64.b64decode(material['ca.crt']).decode()
    leaf = base64.b64decode(material['tls.crt']).decode()
    def fingerprint(pem):
        first = re.search(r'-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----', pem, re.S)
        if not first:
            raise RuntimeError('certificate missing from PEM')
        return hashlib.sha256(ssl.PEM_cert_to_DER_cert(first.group(0))).hexdigest()
    expected = fingerprint(leaf)
    if fingerprint(certificate) != expected:
        raise RuntimeError('APISIX SSL object differs from cert-manager Secret')
    context = ssl.create_default_context(cadata=ca)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    result = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'trust': 'Cert-manager private gateway CA, not public Web PKI',
              'expected_leaf_sha256': expected,
              'tls': [], 'limits': []}
    pods = kube('apisix', 'pods', selector='app.kubernetes.io/name=apisix,app.kubernetes.io/instance=apisix')['items']
    if len(pods) != 2 or any(not any(c['type'] == 'Ready' and c['status'] == 'True'
                                   for c in p['status'].get('conditions', [])) for p in pods):
        raise RuntimeError('expected two ready gateways')
    targets = [(host, 30443, 'nodeport') for host in ['10.1.201.70', '10.1.201.71']]
    targets.extend((p['status']['podIP'], 9443, p['metadata']['name']) for p in pods)
    for host, port, target in targets:
        connection = http.client.HTTPSConnection('apisix-gateway', port,
                                                  context=context, timeout=15)
        connection.sock = context.wrap_socket(
            socket.create_connection((host, port), timeout=15),
            server_hostname='apisix-gateway')
        peer = connection.sock.getpeercert()
        served = hashlib.sha256(connection.sock.getpeercert(binary_form=True)).hexdigest()
        if served != expected:
            raise RuntimeError('gateway serves a stale certificate')
        evidence = {'host': host, 'port': port, 'target': target, 'sni': 'apisix-gateway',
                    'protocol': connection.sock.version(),
                    'leaf_sha256': served,
                    'cipher': connection.sock.cipher()[0],
                    'not_after': peer['notAfter'], 'paths': {}}
        for path, expected_status in [('/grafana/login', 200), ('/api/v1/jobs', 401)]:
            connection.request('GET', path)
            response = connection.getresponse()
            body = response.read()
            evidence['paths'][path] = response.status
            if response.status != expected_status:
                raise RuntimeError('unexpected TLS gateway response')
            if path.startswith('/grafana') and b'<base href="/grafana/"' not in body:
                raise RuntimeError('Grafana external subpath was lost')
        connection.close()
        result['tls'].append(evidence)
    if args.exercise_limit:
        code, body = request(endpoint + 'routes/vela-api', {
            'X-API-KEY': base64.b64decode(admin['admin']).decode()})
        limit = json.loads(body)['value']['plugins']['limit-count']
        if code != 200 or any(limit.get(k) != v for k, v in {
                'count': 120, 'time_window': 60, 'key': 'remote_addr',
                'policy': 'local', 'rejected_code': 429}.items()):
            raise RuntimeError('limit policy differs from reviewed probe')
        for pod in pods:
            counts = Counter()
            start = time.monotonic()
            connection = http.client.HTTPSConnection('apisix-gateway', 9443, context=context, timeout=10)
            connection.sock = context.wrap_socket(
                socket.create_connection((pod['status']['podIP'], 9443), timeout=10),
                server_hostname='apisix-gateway')
            for _ in range(125):
                connection.request('GET', '/api/v1/jobs', headers={'Host': 'apisix-gateway'})
                response = connection.getresponse()
                response.read()
                counts[response.status] += 1
            connection.close()
            elapsed = time.monotonic() - start
            result['limits'].append({'pod': pod['metadata']['name'],
                                     'requests': 125, 'statuses': dict(counts),
                                     'seconds': round(elapsed, 3)})
            if elapsed >= 60 or counts[429] < 1 or not 1 <= counts[401] <= 120 or set(counts) - {401, 429}:
                raise RuntimeError('local rate limit probe failed or crossed a window')
        result['limit_scope'] = 'Per gateway Pod and source IP; not global or tenant-aware'
    result['result'] = 'PRIVATE_GATEWAY_TLS_AND_LIMIT_PASS' if args.exercise_limit else 'PRIVATE_GATEWAY_TLS_PASS'
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
