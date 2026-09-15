#!/usr/bin/env python3
"""Verify live registry scope and authenticated Kubernetes mirror fallback.

Creates two unique BusyBox-derived images only on .71 and .66, then addresses
them through .70 from CPU-management and GPU-worker CPU-only Pods. Verifies a
wrong password fails before the good pull. Deletes only this run's Pods, Secret
and manifests. Unreferenced test blobs may remain; no registry GC is performed.
"""
import argparse
import base64
import copy
import datetime
import hashlib
import http.client
import json
import os
from pathlib import Path
import ssl
import subprocess
import time
import urllib.parse
import uuid

HOSTS = ['10.1.201.70', '10.1.201.71', '10.1.201.66']
TYPES = ['application/vnd.oci.image.index.v1+json', 'application/vnd.docker.distribution.manifest.list.v2+json',
         'application/vnd.oci.image.manifest.v1+json', 'application/vnd.docker.distribution.manifest.v2+json']
BASE = 'sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0'


def sha(data):
    return 'sha256:' + hashlib.sha256(data).hexdigest()


def kubectl(*args, value=None):
    p = subprocess.run(['kubectl', *args], text=True, capture_output=True,
                       input=None if value is None else json.dumps(value), timeout=50)
    if p.returncode:
        raise RuntimeError(p.stderr[:1800])
    return p.stdout


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory', default='/opt/vela-cluster/registry-access-20260914/live')
    parser.add_argument('--receipt', required=True)
    args = parser.parse_args()
    os.umask(0o077)
    root = Path(args.directory)
    secret_path = root / 'credentials.json'
    if secret_path.stat().st_mode & 0o077 or secret_path.stat().st_uid != os.geteuid():
        raise RuntimeError('private credential file required')
    credentials = json.loads(secret_path.read_text())
    context = ssl.create_default_context(cafile='/etc/rancher/rke2/registry-ca.crt')
    run = 'registry-pull-' + uuid.uuid4().hex[:10]
    receipt = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'run': run,
               'checks': [], 'pods': [], 'images': [], 'cleanup': [],
               'scope': 'Real Kubernetes/containerd pulls; CPU probes only, no GPU allocation or host service restart'}
    created = []
    manifests = []

    def request(host, method, path, user=None, body=None, headers=None, port=5005, wrong=False):
        h = {'Accept': ', '.join(TYPES)}
        if user:
            password = ('wrong-' + run) if wrong else credentials[user]
            h['Authorization'] = 'Basic ' + base64.b64encode((user + ':' + password).encode()).decode()
        h.update(headers or {})
        c = http.client.HTTPSConnection(host, port, context=context, timeout=40)
        try:
            c.request(method, path, body=body, headers=h)
            r = c.getresponse()
            return r.status, dict(r.headers), r.read()
        finally:
            c.close()

    def record(name, passed, **fields):
        receipt['checks'].append(dict(name=name, passed=bool(passed), **fields))
        if not passed:
            raise AssertionError(name)

    def upload(host, repo, data, user):
        ref = sha(data)
        status, _, _ = request(host, 'HEAD', '/v2/' + repo + '/blobs/' + ref, user)
        if status == 404:
            code, h, _ = request(host, 'POST', '/v2/' + repo + '/blobs/uploads/', user, b'')
            if code != 202:
                raise RuntimeError('blob upload start: ' + str(code))
            loc = urllib.parse.urlsplit(urllib.parse.urljoin('https://' + host + ':5005/', h['Location']))
            if loc.scheme != 'https' or loc.hostname != host or loc.port != 5005 or not loc.path.startswith('/v2/' + repo + '/blobs/uploads/'):
                raise RuntimeError('unexpected upload destination')
            path = loc.path + ('?' + loc.query if loc.query else '')
            path += ('&' if loc.query else '?') + 'digest=' + ref
            status, _, _ = request(host, 'PUT', path, user, data, {'Content-Type': 'application/octet-stream'})
            if status != 201:
                raise RuntimeError('blob commit: ' + str(status))
        elif status != 200:
            raise RuntimeError('blob HEAD: ' + str(status))
        return ref

    def create(obj):
        kubectl('create', '-f', '-', value=obj)
        created.append((obj['metadata']['namespace'], obj['kind'].lower(), obj['metadata']['name']))

    def pod_wait(namespace, name, failure=False):
        deadline = time.monotonic() + (60 if failure else 140)
        while time.monotonic() < deadline:
            pod = json.loads(kubectl('-n', namespace, 'get', 'pod', name, '-o', 'json'))
            states = pod['status'].get('containerStatuses', [])
            if failure and any(s.get('state', {}).get('waiting', {}).get('reason') in ['ErrImagePull', 'ImagePullBackOff'] for s in states):
                return pod
            if any(s.get('ready') for s in states):
                if failure:
                    raise RuntimeError('wrong registry password allowed a pull')
                return pod
            if pod['status']['phase'] == 'Failed':
                raise RuntimeError('probe pod failed')
            time.sleep(2)
        events = json.loads(kubectl('-n', namespace, 'get', 'events', '--field-selector=involvedObject.name=' + name, '-o', 'json'))
        receipt.setdefault('failure_events', []).extend({'reason': e.get('reason'), 'message': e.get('message', '')[:1500]} for e in events['items'])
        raise RuntimeError('probe did not reach expected pull state')

    try:
        for host in HOSTS:
            code, _, _ = request(host, 'GET', '/v2/')
            record('anonymous production access denied', code == 401, host=host, status=code)
            for user in credentials:
                code, _, _ = request(host, 'GET', '/v2/', user)
                record('production identity accepted', code == 200, host=host, identity=user, status=code)
            code, _, _ = request(host, 'GET', '/v2/_catalog', 'node-pull')
            record('node identity cannot read catalog', code == 401, host=host, status=code)
        # Obtain the pinned base from the already established Docker Hub cache.
        code, _, body = request(HOSTS[0], 'GET', '/v2/library/busybox/manifests/' + BASE, port=5000)
        record('pinned base manifest verified', code == 200 and sha(body) == BASE)
        base = json.loads(body)
        if 'manifests' in base:
            descriptor = next(m for m in base['manifests'] if m.get('platform', {}).get('os') == 'linux' and m.get('platform', {}).get('architecture') == 'amd64')
            code, _, body = request(HOSTS[0], 'GET', '/v2/library/busybox/manifests/' + descriptor['digest'], port=5000)
            record('base Linux amd64 manifest verified', code == 200 and sha(body) == descriptor['digest'])
            base = json.loads(body)
        code, _, config = request(HOSTS[0], 'GET', '/v2/library/busybox/blobs/' + base['config']['digest'], port=5000)
        record('base config verified', code == 200 and sha(config) == base['config']['digest'])
        layers = []
        for layer in base['layers']:
            code, _, data = request(HOSTS[0], 'GET', '/v2/library/busybox/blobs/' + layer['digest'], port=5000)
            record('base layer verified', code == 200 and sha(data) == layer['digest'] and len(data) == layer['size'])
            layers.append(data)
        nodes = json.loads(kubectl('get', 'nodes', '-o', 'json'))['items']
        worker = next(n for n in nodes if any(a['type'] == 'InternalIP' and a['address'] == '10.1.201.11' for a in n['status']['addresses']))
        record('worker preflight', not worker['spec'].get('unschedulable') and any(c['type'] == 'Ready' and c['status'] == 'True' for c in worker['status']['conditions']))
        for namespace, host, node in [('llm-api', HOSTS[1], 'llmpool01'), ('llm-models', HOSTS[2], worker['metadata']['name'])]:
            repo = namespace + '/' + run
            publisher, reader = namespace + '-publisher', namespace + '-pull'
            other = ('llm-models' if namespace == 'llm-api' else 'llm-api') + '-pull'
            marker = run + '-' + namespace
            cfg = json.loads(config)
            cfg.setdefault('config', {}).setdefault('Labels', {})['vela.ai/registry-pull-probe'] = marker
            raw_config = json.dumps(cfg, separators=(',', ':')).encode()
            manifest = copy.deepcopy(base)
            manifest['config']['digest'] = upload(host, repo, raw_config, publisher)
            manifest['config']['size'] = len(raw_config)
            for data in layers:
                upload(host, repo, data, publisher)
            body = json.dumps(manifest, separators=(',', ':')).encode()
            ref = sha(body)
            media = manifest.get('mediaType', TYPES[3])
            code, _, _ = request(host, 'PUT', '/v2/' + repo + '/manifests/' + ref, publisher, body, {'Content-Type': media})
            record('tenant publishes its repository', code == 201, host=host, namespace=namespace, digest=ref)
            manifests.append((host, repo, ref))
            for endpoint in HOSTS:
                code, _, received = request(endpoint, 'GET', '/v2/' + repo + '/manifests/' + ref, reader)
                expected = 200 if endpoint == host else 404
                record('image exists on exactly one fallback endpoint', code == expected and (code != 200 or received == body), host=endpoint, expected=expected, status=code)
            code, _, _ = request(host, 'GET', '/v2/' + repo + '/manifests/' + ref, other)
            record('other tenant cannot pull', code == 401, namespace=namespace, status=code)
            code, _, _ = request(host, 'POST', '/v2/' + repo + '/blobs/uploads/', reader)
            record('runtime credential cannot publish', code == 401, namespace=namespace, status=code)
            image = HOSTS[0] + ':5005/' + repo + '@' + ref
            selector = {'kubernetes.io/hostname': node}
            selector.update({'vela.ai/management': 'true', 'vela.ai/control-plane-tier': 'cpu'} if namespace == 'llm-api' else {'vela.ai/node-role': 'gpu-worker'})
            pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': run, 'namespace': namespace}, 'spec': {
                'serviceAccountName': namespace + '-runtime', 'automountServiceAccountToken': False,
                'priorityClassName': 'vela-application', 'restartPolicy': 'Never', 'nodeSelector': selector,
                'securityContext': {'runAsNonRoot': True, 'runAsUser': 10001, 'runAsGroup': 10001, 'seccompProfile': {'type': 'RuntimeDefault'}},
                'containers': [{'name': 'probe', 'image': image, 'imagePullPolicy': 'Always',
                    'command': ['/bin/sh', '-c', 'echo ' + marker + '; sleep 300'],
                    'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}},
                    'resources': {'requests': {'cpu': '10m', 'memory': '16Mi', 'ephemeral-storage': '16Mi'},
                                  'limits': {'cpu': '100m', 'memory': '32Mi', 'ephemeral-storage': '32Mi'}}}]}}
            bad_auth = base64.b64encode((reader + ':wrong-' + run).encode()).decode()
            bad_config = base64.b64encode(json.dumps({'auths': {h + ':5005': {'auth': bad_auth} for h in HOSTS}}).encode()).decode()
            create({'apiVersion': 'v1', 'kind': 'Secret', 'metadata': {'namespace': namespace, 'name': run + '-wrong'},
                    'type': 'kubernetes.io/dockerconfigjson', 'data': {'.dockerconfigjson': bad_config}})
            denied = copy.deepcopy(pod)
            denied['metadata']['name'] += '-wrong'
            denied['spec']['imagePullSecrets'] = [{'name': run + '-wrong'}]
            create(denied)
            failed = pod_wait(namespace, run + '-wrong', failure=True)
            record('Kubernetes rejects incorrect registry credentials', True, namespace=namespace, node=failed['spec']['nodeName'],
                   reason=failed['status']['containerStatuses'][0]['state']['waiting']['reason'])
            kubectl('-n', namespace, 'delete', 'pod', run + '-wrong', '--wait=true', '--timeout=30s')
            create(pod)
            actual = pod_wait(namespace, run)
            logs = kubectl('-n', namespace, 'logs', run).strip()
            record('Kubernetes authenticates and falls back to unique mirror', logs == marker and actual['spec']['nodeName'] == node,
                   namespace=namespace, source=HOSTS[0], only_available_at=host, node=node, image=image)
            receipt['pods'].append({'namespace': namespace, 'name': run, 'node': node, 'image': image,
                                    'image_id': actual['status']['containerStatuses'][0].get('imageID'),
                                    'imagePullSecrets': actual['spec'].get('imagePullSecrets'), 'log': logs})
            receipt['images'].append({'host': host, 'repo': repo, 'digest': ref})
            print(json.dumps({'namespace': namespace, 'node': node, 'fallback': host, 'result': 'AUTHENTICATED_KUBERNETES_PULL_PASS'}), flush=True)
        receipt['result'] = 'LIVE_REGISTRY_AUTH_AND_KUBERNETES_FALLBACK_PASS'
    finally:
        for namespace, kind, name in reversed(created):
            try:
                kubectl('-n', namespace, 'delete', kind, name, '--ignore-not-found', '--wait=true', '--timeout=35s')
                absent = not kubectl('-n', namespace, 'get', kind, name, '--ignore-not-found', '-o', 'name').strip()
                receipt['cleanup'].append({'namespace': namespace, 'kind': kind, 'name': name, 'absent': absent})
            except Exception as error:
                receipt['cleanup'].append({'namespace': namespace, 'kind': kind, 'name': name, 'error': str(error)[:900]})
        for host, repo, ref in manifests:
            try:
                code, _, _ = request(host, 'DELETE', '/v2/' + repo + '/manifests/' + ref, 'platform-publisher')
                subsequent, _, _ = request(host, 'GET', '/v2/' + repo + '/manifests/' + ref, 'platform-publisher')
                receipt['cleanup'].append({'host': host, 'repository': repo, 'manifest_deleted': code == 202, 'subsequent_get': subsequent})
            except Exception as error:
                receipt['cleanup'].append({'host': host, 'repository': repo, 'error': str(error)[:900]})
        receipt['unreferenced_test_blobs_may_remain'] = True
        if any(x.get('error') or x.get('absent') is False or x.get('manifest_deleted') is False or x.get('subsequent_get', 404) != 404 for x in receipt['cleanup']):
            receipt['result'] = 'CLEANUP_INCOMPLETE'
        Path(args.receipt).write_text(json.dumps(receipt, indent=2) + '\n')


if __name__ == '__main__':
    main()
