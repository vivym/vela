#!/usr/bin/env python3
"""Enable and verify HTTPS redirects after the gateway's IP TLS preflight.

Run on a CPU management node. --apply adds one global redirect plugin while
preserving existing telemetry plugins. A failed postcheck restores the saved
rule. Without --apply this script only verifies the existing policy.
"""
import argparse
import base64
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
import time
import urllib.request

REDIRECT = {'uri': 'https://$host:30443$request_uri', 'ret_code': 308,
            '_meta': {'filter': [['scheme', '==', 'http']]}}


def kube(*args):
    return json.loads(subprocess.check_output(['kubectl', '-n', 'apisix', *args, '-o', 'json'], timeout=30))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--run-directory', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    directory = Path(args.run_directory)
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    receipt = directory / 'https-policy.json'
    if args.apply and receipt.exists():
        raise RuntimeError('existing policy receipt: inspect or verify without --apply')
    material = kube('get', 'secret', 'vela-gateway-tls')['data']
    context = ssl.create_default_context(cadata=base64.b64decode(material['ca.crt']).decode())
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    cert = base64.b64decode(material['tls.crt']).decode()
    leaf = re.search(r'-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----', cert, re.S).group(0)
    expected = hashlib.sha256(ssl.PEM_cert_to_DER_cert(leaf)).hexdigest()
    result = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'ip_tls': [], 'redirects': []}

    def verify_tls():
        result['ip_tls'] = []
        for host in ['10.1.201.70', '10.1.201.71']:
            connection = http.client.HTTPSConnection(host, 30443, context=context, timeout=15)
            connection.connect()  # IP literals cause no SNI; SAN verification stays enabled.
            fingerprint = hashlib.sha256(connection.sock.getpeercert(binary_form=True)).hexdigest()
            assert fingerprint == expected, 'served certificate differs from managed certificate'
            evidence = {'host': host, 'sni': None, 'hostname_verified': host,
                        'protocol': connection.sock.version(), 'leaf_sha256': fingerprint, 'paths': {}}
            for path, status in [('/grafana/login', 200), ('/api/v1/jobs', 401)]:
                connection.request('GET', path)
                response = connection.getresponse()
                body = response.read()
                assert response.status == status, 'unexpected HTTPS application response'
                if path.startswith('/grafana'):
                    assert b'<base href="/grafana/"' in body, 'Grafana subpath lost'
                evidence['paths'][path] = response.status
            connection.close()
            result['ip_tls'].append(evidence)

    verify_tls()  # Do not redirect clients until the destination is usable.
    admin = base64.b64decode(kube('get', 'secret', 'apisix-admin-credentials')['data']['admin']).decode()
    endpoint = 'http://' + kube('get', 'svc', 'apisix-admin')['spec']['clusterIP'] + ':9180/apisix/admin/global_rules/1'

    def request(method='GET', value=None):
        req = urllib.request.Request(endpoint, method=method,
                                     data=json.dumps(value).encode() if value else None,
                                     headers={'X-API-KEY': admin, 'Content-Type': 'application/json'})
        with urllib.request.urlopen(req, timeout=15) as response:
            return json.load(response)['value']

    original = request()
    before = original['plugins']
    if before.get('redirect') not in (None, REDIRECT):
        raise RuntimeError('existing redirect differs; review before replacement')
    applied = False
    try:
        if args.apply:
            (directory / 'global-rule-before.json').write_text(json.dumps(original, indent=2) + '\n')
            request('PATCH', {'plugins': {'redirect': REDIRECT}})
            applied = True
        actual = request()['plugins']
        assert actual == dict(before, redirect=REDIRECT), 'global plugins changed unexpectedly'
        for host in ['10.1.201.70', '10.1.201.71']:
            deadline = time.monotonic() + 20
            while True:
                connection = http.client.HTTPConnection(host, 30080, timeout=5)
                connection.request('GET', '/api/v1/jobs')
                response = connection.getresponse()
                response.read()
                connection.close()
                if response.status == 308:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError('redirect did not propagate to gateway')
                time.sleep(1)
            for method in ['GET', 'HEAD', 'POST']:
                for spoof in [False, True]:
                    path = '/api/v1/jobs?next=a%2Fb&x=1'
                    headers = {'X-Forwarded-Proto': 'https'} if spoof else {}
                    connection = http.client.HTTPConnection(host, 30080, timeout=15)
                    connection.request(method, path, body='{}' if method == 'POST' else None, headers=headers)
                    response = connection.getresponse()
                    response.read()
                    location = response.getheader('Location')
                    assert response.status == 308 and location == 'https://' + host + ':30443' + path, 'redirect or method/query preservation failed'
                    result['redirects'].append({'host': host, 'method': method, 'spoofed_forwarded_proto': spoof,
                                                'status': response.status, 'location': location})
                    connection.close()
            connection = http.client.HTTPConnection(host, 30080, timeout=15)
            connection.request('GET', '/grafana/login')
            response = connection.getresponse()
            response.read()
            assert response.status == 308 and response.getheader('Location') == 'https://' + host + ':30443/grafana/login'
            connection.close()
        verify_tls()  # Detect redirect loops or broken upstreams on HTTPS.
        result.update(result='GATEWAY_HTTPS_POLICY_PASS', telemetry_plugins_preserved=True,
                      redirect=REDIRECT, private_ca_required=True)
        receipt.write_text(json.dumps(result, indent=2) + '\n')
    except Exception:
        if applied:
            request('PUT', {'plugins': before})
        raise
    print(json.dumps(result, indent=2))


if __name__ == '__main__':
    main()
