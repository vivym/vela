#!/usr/bin/env python3
"""Measure worker host sources, then narrowly adopt Control TCP 8444 ingress.

Run on .70 as an operator. discover creates two temporary CPU-only source echo
Pods and cleans up all of its objects. apply requires that exact discovery and
uses optimistic concurrency for a single existing NetworkPolicy. Remote hosts
only read their boot/service state and make bounded network probes. No reboot,
package install, GPU/driver operation or host filesystem write occurs.
"""
import argparse
import ast
import base64
import concurrent.futures
import datetime
import hashlib
import ipaddress
import json
import os
from pathlib import Path
import re
import shlex
import ssl
import subprocess
import time
import uuid

IMAGE = 'docker.io/library/busybox@sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0'
POLICY = 'vela-control-allow-node-agent'
SELECTOR = {'matchLabels': {'app.kubernetes.io/name': 'vela-control'}}
WORKER_IPS = {f'10.1.201.{i}' for i in range(11, 67) if i not in (44, 56, 57, 61)}
CPU = {'10.1.201.70': 'llmpool01', '10.1.201.71': 'llmpool02'}
SOURCE_SCRIPT = '#!/bin/sh\nprintf \'Content-Type: application/json\\r\\n\\r\\n{"source":"%s"}\\n\' "$REMOTE_ADDR"\n'


def kub(*args, value=None):
    result = subprocess.run(['kubectl', *args], input=json.dumps(value) if value is not None else None,
                            text=True, capture_output=True, timeout=50)
    if result.returncode:
        raise RuntimeError('kubectl failed: ' + result.stderr[:1500])
    return result.stdout


def get(*args):
    return json.loads(kub(*args, '-o', 'json'))


def inventory():
    rows = []
    for node in get('get', 'nodes')['items']:
        labels = node['metadata'].get('labels', {})
        if labels.get('vela.ai/gpu-runtime') != 'true':
            continue
        address = next(a['address'] for a in node['status']['addresses'] if a['type'] == 'InternalIP')
        if address not in WORKER_IPS:
            raise ValueError('GPU node is outside the user-authorized inventory')
        cidr = ipaddress.ip_network(node['spec']['podCIDR'])
        if cidr.version != 4 or cidr.prefixlen != 24 or not cidr.subnet_of(ipaddress.ip_network('10.42.0.0/16')):
            raise ValueError('unexpected worker PodCIDR')
        rows.append({'name': node['metadata']['name'], 'uid': node['metadata']['uid'], 'ip': address,
                     'pod_cidr': str(cidr), 'candidate_source': str(cidr.network_address),
                     'ready': any(c['type'] == 'Ready' and c['status'] == 'True' for c in node['status']['conditions'])})
    if {r['ip'] for r in rows} != WORKER_IPS or len(rows) != len(WORKER_IPS):
        raise ValueError('registered worker set no longer matches the authorized fleet')
    if len({r['pod_cidr'] for r in rows}) != len(rows):
        raise ValueError('worker PodCIDRs overlap')
    return sorted(rows, key=lambda r: ipaddress.ip_address(r['ip']))


def password_from_private_file(path):
    tree = ast.parse(Path(path).read_text())
    candidates = [a.value.value for a in ast.walk(tree) if isinstance(a, ast.Assign)
                  and isinstance(a.value, ast.Constant) and isinstance(a.value.value, str)
                  and any(isinstance(t, ast.Name) and t.id == 'password' for t in a.targets)]
    if len(candidates) != 1 or not candidates[0]:
        raise ValueError('private SSH credential source is not unambiguous')
    return candidates[0]


# This code runs as user, without sudo and without writing files.
REMOTE = '''import json,socket,ssl,hashlib,subprocess,urllib.request,concurrent.futures
from pathlib import Path
options=OPTIONS
def state():
 p=subprocess.run(['systemctl','show','rke2-agent.service','rke2-server.service','vela-node-agent.service','vela-pidfd-broker.service','vela-runtime-policy-issuer.service','--property=Id,ActiveState,UnitFileState,InvocationID'],text=True,capture_output=True,timeout=8)
 return {'boot_id':Path('/proc/sys/kernel/random/boot_id').read_text().strip(),'units':p.stdout.strip()}
before=state()
def probe(item):
 out=dict(item)
 try:
  if item['kind']=='source':
   opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
   with opener.open('http://'+item['ip']+':8444/cgi-bin/source',timeout=4) as response:out['observed_source']=json.load(response)['source']
   out['connected']=True
  elif item['kind']=='tls':
   # Authenticate this anonymous test's server by the exact leaf fetched from
   # the mounted Kubernetes Secret. This changes no application's TLS config.
   context=ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
   context.check_hostname=False
   context.verify_mode=ssl.CERT_NONE
   context.minimum_version=ssl.TLSVersion.TLSv1_3
   out['pinned_peer']=False
   with socket.create_connection((item['ip'],8444),timeout=3) as raw:
    with context.wrap_socket(raw,server_hostname='vela-control.vela-system.svc') as tls:
     out['pinned_peer']=hashlib.sha256(tls.getpeercert(binary_form=True)).hexdigest()==options['tls_sha256']
     if not out['pinned_peer']:raise ValueError('Control serving certificate changed')
     tls.sendall(b'PRI * HTTP/2.0\\r\\n\\r\\nSM\\r\\n\\r\\n')
     tls.recv(32)
   out['anonymous_rejected']=False
  else:
   with socket.create_connection((item['ip'],item['port']),timeout=2) as s:out.update(connected=True,local_address=s.getsockname()[0])
 except ssl.SSLError as error:
  out['tls_reason']=error.reason
  out['anonymous_rejected']=out.get('pinned_peer',False) and error.reason=='TLSV13_ALERT_CERTIFICATE_REQUIRED'
 except (OSError,ValueError) as error:
  out.update(connected=False,error_type=type(error).__name__,errno=getattr(error,'errno',None))
 return out
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(probe,options['targets']))
print(json.dumps({'before':before,'after':state(),'results':results}))
'''


def peer(ip, targets, password, tls_sha256=None):
    if ip not in WORKER_IPS | set(CPU):
        raise ValueError('SSH target outside authorized hosts')
    script = REMOTE.replace('OPTIONS', repr({'targets': targets, 'tls_sha256': tls_sha256}))
    env = dict(os.environ, SSHPASS=password)
    command = ['sshpass', '-e', 'ssh', '-T', '-o', 'StrictHostKeyChecking=yes', '-o',
               'UserKnownHostsFile=/home/user/.ssh/known_hosts', '-o', 'ConnectTimeout=6',
               'user@' + ip, 'python3 -c ' + shlex.quote(script)]
    response = subprocess.run(command, env=env, text=True, capture_output=True, timeout=40)
    if response.returncode:
        raise RuntimeError('read-only host probe failed for ' + ip + ': ' + response.stderr[:300])
    data = json.loads(response.stdout)
    if data['before'] != data['after']:
        raise RuntimeError('host boot/service identity changed during probes for ' + ip)
    return data


def parallel_peers(nodes, targets, password, tls_sha256=None):
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=12) as pool:
        pending = {pool.submit(peer, node['ip'], targets, password, tls_sha256): node for node in nodes}
        for future in concurrent.futures.as_completed(pending):
            node = pending[future]
            try:
                results.append({'node': node, **future.result()})
            except Exception as error:
                results.append({'node': node, 'error': str(error)})
            if len(results) % 10 == 0:
                print(json.dumps({'hosts_checked': len(results), 'total': len(nodes)}), flush=True)
    return sorted(results, key=lambda r: ipaddress.ip_address(r['node']['ip']))


def candidate_policy(sources):
    if not sources or len(sources) != len(set(sources)):
        raise ValueError('source list is empty or ambiguous')
    for value in sources:
        address = ipaddress.ip_address(value)
        host_source = (address in ipaddress.ip_network('10.42.0.0/16') and int(address) % 256 == 0
                       and str(address) not in ('10.42.0.0', '10.42.1.0'))
        if address.version != 4 or not (host_source or str(address) in WORKER_IPS):
            raise ValueError('source is outside this fleet')
    return {'podSelector': SELECTOR, 'policyTypes': ['Ingress'], 'ingress': [
        {'from': [{'ipBlock': {'cidr': ip + '/32'}} for ip in sorted(sources, key=ipaddress.ip_address)],
         'ports': [{'protocol': 'TCP', 'port': 8444}]}]}


def validate_discovery(receipt, current):
    if receipt.get('result') != 'WORKER_SOURCES_VERIFIED' or not receipt.get('cleanup_complete'):
        raise ValueError('discovery did not finish successfully')
    captured = datetime.datetime.fromisoformat(receipt['at'])
    if not 0 <= (datetime.datetime.now(datetime.timezone.utc) - captured).total_seconds() <= 3600:
        raise ValueError('source discovery is older than one hour')
    if receipt.get('inventory') != current:
        raise ValueError('full node inventory changed since discovery')
    active = [n for n in current if n['ready']]
    rows = receipt['observations']
    if [r['node'] for r in rows] != active:
        raise ValueError('Node UID/address/PodCIDR/readiness changed since discovery')
    sources = []
    for row in rows:
        if row.get('error') or row['before'] != row['after'] or len(row['results']) != 2:
            raise ValueError('worker source evidence is incomplete')
        observed = [x.get('observed_source') for x in row['results']]
        if len(set(observed)) != 1 or observed[0] not in (row['node']['ip'], row['node']['candidate_source']):
            raise ValueError('observed source is not bound to this worker on both CPU nodes')
        if {x['node'] for x in row['results']} != set(CPU.values()) or not all(x.get('connected') for x in row['results']):
            raise ValueError('both CPU destinations were not verified')
        sources.append(observed[0])
    return candidate_policy(sources)


def control_state():
    pods = get('-n', 'vela-system', 'get', 'pods', '-l', 'app.kubernetes.io/name=vela-control')['items']
    pods = [p for p in pods if not p['metadata'].get('deletionTimestamp')]
    if len(pods) != 2 or {p['spec'].get('nodeName') for p in pods} != set(CPU.values()) or not all(
            any(c['type'] == 'Ready' and c['status'] == 'True' for c in p['status'].get('conditions', [])) for p in pods):
        raise ValueError('two CPU Control Pods are not Ready')
    tls_secrets = set()
    for pod in pods:
        for volume in pod['spec']['volumes']:
            for secret in [volume.get('secret')] + [s.get('secret') for s in volume.get('projected', {}).get('sources', [])]:
                if secret and any(i['key'] == 'fleet-tls.crt' for i in secret.get('items', [])):
                    tls_secrets.add(secret.get('secretName') or secret.get('name'))
    if len(tls_secrets) != 1:
        raise ValueError('Control serving certificate projection is ambiguous')
    encoded = kub('-n', 'vela-system', 'get', 'secret', next(iter(tls_secrets)), '-o', 'jsonpath={.data.fleet-tls\\.crt}')
    certificate = re.search(r'-----BEGIN CERTIFICATE-----.*?-----END CERTIFICATE-----', base64.b64decode(encoded).decode(), re.S).group(0)
    digest = hashlib.sha256(ssl.PEM_cert_to_DER_cert(certificate)).hexdigest()
    service = get('-n', 'vela-system', 'get', 'service', 'vela-control')
    if service['spec'].get('selector') != SELECTOR['matchLabels'] or not any(
            p['port'] == 8444 and p.get('protocol', 'TCP') == 'TCP' and p['targetPort'] == 'fleet-grpc'
            for p in service['spec']['ports']):
        raise ValueError('Control Service no longer targets the Fleet listener')
    targets = [{'kind': 'tcp', 'ip': p['status']['podIP'], 'port': 8444, 'node': p['spec']['nodeName']} for p in pods]
    targets.append({'kind': 'tcp', 'ip': service['spec']['clusterIP'], 'port': 8444, 'node': 'Service'})
    state = {'pods': sorted([{'uid': p['metadata']['uid'], 'ip': p['status']['podIP'], 'node': p['spec']['nodeName']} for p in pods], key=lambda p: p['node']),
             'service_ip': service['spec']['clusterIP'], 'service_uid': service['metadata']['uid'], 'tls_sha256': digest}
    return state, targets


def discover(root, nodes, password, receipt):
    marker = 'vela-worker-source-' + uuid.uuid4().hex[:8]
    active = [n for n in nodes if n['ready']]
    created = []
    def create(kind, name, value):
        value['metadata'].setdefault('labels', {})['vela.ai/validation-only'] = marker
        # Record intent first: a request may succeed at the API server even if
        # its response is lost. Cleanup checks ownership before deleting it.
        created.append((kind, name))
        kub('create', '-f', '-', value=value)
    try:
        create('configmap', marker, {'apiVersion': 'v1', 'kind': 'ConfigMap', 'metadata': {'name': marker, 'namespace': 'vela-system'}, 'data': {'source': SOURCE_SCRIPT}})
        # The temporary echo server only accepts exact host addresses for this
        # fleet, including the observed Flannel host-source candidates.
        temporary = candidate_policy(sorted({n['ip'] for n in active} | {n['candidate_source'] for n in active}))
        temporary['podSelector'] = {'matchLabels': {'vela.ai/validation-only': marker}}
        create('networkpolicy', marker, {'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy', 'metadata': {'name': marker, 'namespace': 'vela-system'}, 'spec': temporary})
        targets = []
        for index, node in enumerate(CPU.values()):
            name = marker + '-' + str(index)
            labels = {'vela.ai/validation-only': marker, 'vela.ai/source-target': str(index)}
            pod = {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': name, 'namespace': 'vela-system', 'labels': labels}, 'spec': {
                'nodeName': node, 'automountServiceAccountToken': False, 'restartPolicy': 'Never', 'terminationGracePeriodSeconds': 1,
                'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532, 'seccompProfile': {'type': 'RuntimeDefault'}},
                'containers': [{'name': 'source', 'image': IMAGE, 'imagePullPolicy': 'IfNotPresent', 'command': ['httpd', '-f', '-p', '0.0.0.0:8444', '-h', '/www'],
                                'volumeMounts': [{'name': 'script', 'mountPath': '/www/cgi-bin', 'readOnly': True}],
                                'readinessProbe': {'tcpSocket': {'port': 8444}, 'periodSeconds': 2},
                                'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True, 'capabilities': {'drop': ['ALL']}},
                                'resources': {'requests': {'cpu': '5m', 'memory': '8Mi'}, 'limits': {'cpu': '50m', 'memory': '32Mi'}}}],
                'volumes': [{'name': 'script', 'configMap': {'name': marker, 'defaultMode': 0o555}}]}}
            create('pod', name, pod)
            create('service', name, {'apiVersion': 'v1', 'kind': 'Service', 'metadata': {'name': name, 'namespace': 'vela-system'}, 'spec': {'selector': labels, 'ports': [{'port': 8444, 'targetPort': 8444}]}})
            targets.append({'kind': 'source', 'ip': get('-n', 'vela-system', 'get', 'service', name)['spec']['clusterIP'], 'node': node})
        kub('-n', 'vela-system', 'wait', 'pod', '-l', 'vela.ai/validation-only=' + marker, '--for=condition=Ready', '--timeout=45s')
        receipt['probe_image'] = IMAGE
        receipt['targets'] = targets
        receipt['observations'] = parallel_peers(active, targets, password)
        receipt['result'] = 'WORKER_SOURCES_VERIFIED'
    finally:
        errors = []
        for kind, name in reversed(created):
            try:
                existing = kub('-n', 'vela-system', 'get', kind, name, '--ignore-not-found=true', '-o', 'json').strip()
                if not existing:
                    continue
                if json.loads(existing)['metadata'].get('labels', {}).get('vela.ai/validation-only') != marker:
                    raise RuntimeError('refused to remove a temporary object with different ownership')
                kub('-n', 'vela-system', 'delete', kind, name, '--wait=true', '--timeout=35s', '--ignore-not-found=true')
                if kub('-n', 'vela-system', 'get', kind, name, '--ignore-not-found=true', '-o', 'name').strip():
                    raise RuntimeError('temporary object still exists')
            except Exception as error:
                errors.append(str(error))
        receipt['cleanup_complete'] = not errors
        receipt['cleanup_errors'] = errors
    policy = validate_discovery(receipt, inventory())
    receipt['candidate_spec'] = policy
    (root / 'candidate-policy.json').write_text(json.dumps({'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy', 'metadata': {'name': POLICY, 'namespace': 'vela-system'}, 'spec': policy}, indent=2) + '\n')


def adopt(root, nodes, password, receipt, discovery_file):
    discovery = json.loads(Path(discovery_file).read_text())
    candidate = validate_discovery(discovery, nodes)
    before, targets = control_state()
    old = get('-n', 'vela-system', 'get', 'networkpolicy', POLICY)
    receipt['control_before'] = before
    receipt['policy_before'] = old
    if old['spec']['podSelector'] != SELECTOR or old['spec']['policyTypes'] != ['Ingress']:
        raise ValueError('existing policy selector/type changed')
    previous = old['spec']['ingress']
    if any(r.get('ports') != [{'port': 8444, 'protocol': 'TCP'}] for r in previous):
        raise ValueError('existing Node Agent policy has unexpected ports')
    permitted = candidate['ingress'][0]['from']
    if any(source not in permitted for rule in previous for source in rule.get('from', [{}])):
        raise ValueError('candidate would revoke an existing unmeasured source')
    value = {'apiVersion': old['apiVersion'], 'kind': old['kind'], 'metadata': {'name': POLICY, 'namespace': 'vela-system'}, 'spec': candidate}
    kub('apply', '--dry-run=server', '-f', '-', value=value)
    # Verify the current mTLS rejection from the already permitted .66 before
    # expanding network reachability. An anonymous peer never receives an RPC.
    tls_targets = [{'kind': 'tls', 'ip': t['ip'], 'node': t['node']} for t in targets[:2]]
    baseline = peer('10.1.201.66', tls_targets, password, before['tls_sha256'])
    receipt['baseline_mtls'] = baseline
    if not all(r.get('anonymous_rejected') for r in baseline['results']):
        raise ValueError('Control anonymous TLS rejection is not proven')
    attempted = False
    try:
        patch = [{'op': 'test', 'path': '/metadata/resourceVersion', 'value': old['metadata']['resourceVersion']},
                 {'op': 'test', 'path': '/metadata/uid', 'value': old['metadata']['uid']},
                 {'op': 'replace', 'path': '/spec', 'value': candidate}]
        attempted = True
        kub('-n', 'vela-system', 'patch', 'networkpolicy', POLICY, '--type=json', '-p', json.dumps(patch))
        # Wait only for CNI convergence; remote socket probes each have deadlines.
        time.sleep(2)
        negatives = [{'kind': 'tcp', 'ip': t['ip'], 'port': port, 'node': t['node'], 'expected_denied': True} for t in targets[:2] for port in [8081, 8445, 8446, 8447]]
        receipt['verification'] = parallel_peers([n for n in nodes if n['ready']], targets + negatives + tls_targets, password, before['tls_sha256'])
        for row in receipt['verification']:
            if row.get('error'):
                raise ValueError('host verification failed: ' + row['node']['ip'])
            original = next(r for r in discovery['observations'] if r['node']['ip'] == row['node']['ip'])
            if row['before'] != original['after']:
                raise ValueError('host boot/service changed since source discovery')
            for result in row['results']:
                if result['kind'] == 'tls':
                    passed = result.get('anonymous_rejected', False)
                else:
                    passed = (not result.get('connected') and result.get('error_type') in ('TimeoutError', 'timeout')) if result.get('expected_denied') else result.get('connected', False)
                if not passed:
                    raise ValueError('network boundary check failed: ' + row['node']['ip'] + ' ' + str(result))
        receipt['cpu_denied'] = []
        for ip, name in CPU.items():
            target = next(t for t in targets[:2] if t['node'] != name)
            result = peer(ip, [target], password)
            receipt['cpu_denied'].append({'host': ip, **result})
            if result['results'][0].get('connected') or result['results'][0].get('error_type') not in ('TimeoutError', 'timeout'):
                raise ValueError('other CPU host network denial is not proven')
        after, _ = control_state()
        if before != after or inventory() != nodes:
            raise ValueError('Control or node inventory changed during cutover')
        if get('-n', 'vela-system', 'get', 'networkpolicy', POLICY)['spec'] != candidate:
            raise ValueError('live policy differs from measured candidate')
        receipt.update(result='WORKER_CONTROL_NETWORK_PASS', control_after=after, adopted_spec=candidate)
    except Exception:
        if attempted:
            live = get('-n', 'vela-system', 'get', 'networkpolicy', POLICY)
            if live['metadata']['uid'] == old['metadata']['uid'] and live['spec'] == candidate:
                kub('-n', 'vela-system', 'patch', 'networkpolicy', POLICY, '--type=json', '-p', json.dumps([
                    {'op': 'test', 'path': '/metadata/resourceVersion', 'value': live['metadata']['resourceVersion']},
                    {'op': 'test', 'path': '/metadata/uid', 'value': old['metadata']['uid']},
                    {'op': 'replace', 'path': '/spec', 'value': old['spec']}]))
                if get('-n', 'vela-system', 'get', 'networkpolicy', POLICY)['spec'] != old['spec']:
                    raise RuntimeError('rollback verification failed')
                receipt['rollback'] = 'previous policy restored'
            elif live['metadata']['uid'] == old['metadata']['uid'] and live['spec'] == old['spec']:
                receipt['rollback'] = 'previous policy unchanged'
            else:
                receipt['rollback'] = 'refused to overwrite a concurrent policy edit'
        raise


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('mode', choices=['discover', 'apply'])
    parser.add_argument('--run-directory', required=True)
    parser.add_argument('--discovery')
    parser.add_argument('--private-credential-source', default='/opt/vela-cluster/disk-pressure-20260915/archive-readonly.private.py')
    args = parser.parse_args()
    os.umask(0o077)
    os.environ.setdefault('KUBECONFIG', '/etc/rancher/rke2/rke2.yaml')
    os.environ['PATH'] = '/var/lib/rancher/rke2/bin:' + os.environ['PATH']
    root = Path(args.run_directory)
    root.mkdir(mode=0o700, parents=True, exist_ok=False)
    receipt = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'mode': args.mode,
               'scope': 'worker host source and Control 8444 network boundary; not Node Agent registration or model release'}
    try:
        nodes = inventory()
        receipt['inventory'] = nodes
        receipt['excluded_offline'] = [n for n in nodes if not n['ready']]
        password = password_from_private_file(args.private_credential_source)
        if args.mode == 'discover':
            discover(root, nodes, password, receipt)
        else:
            if not args.discovery:
                raise ValueError('apply requires an exact completed discovery receipt')
            adopt(root, nodes, password, receipt, args.discovery)
    except Exception as error:
        receipt['result'] = 'FAILED'
        receipt['error'] = str(error)
        raise
    finally:
        (root / 'receipt.json').write_text(json.dumps(receipt, indent=2) + '\n')
        print(json.dumps({'result': receipt.get('result'), 'path': str(root / 'receipt.json'), 'error': receipt.get('error')}), flush=True)


if __name__ == '__main__':
    main()
