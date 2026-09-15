#!/usr/bin/env python3
"""Verify the live application boundary with short-lived identities and real Pods.

Run as a platform operator on a management node. Credentials remain in memory;
only bounded results are written. Temporary workloads are owned by this run and
removed in finally. No host/service restarts, GPU claims or persistent volumes.
"""
import argparse
import base64
import copy
import datetime
import json
import os
from pathlib import Path
import ssl
import subprocess
import time
import urllib.error
import urllib.request
import urllib.parse
import uuid
import yaml

IMAGE = 'docker.io/library/alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce'
NAMESPACES = ['llm-api', 'llm-models']


def kubectl(*args, value=None, check=True):
    p = subprocess.run(['kubectl', *args], input=json.dumps(value) if value is not None else None,
                       text=True, capture_output=True, timeout=40)
    if check and p.returncode:
        raise RuntimeError(p.stderr[:2000])
    return p


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--receipt', required=True)
    parser.add_argument('--api-only', action='store_true')
    parser.add_argument('--network-only', action='store_true')
    args = parser.parse_args()
    os.umask(0o077)
    kc = yaml.safe_load(Path(os.environ['KUBECONFIG']).read_text())
    cluster = kc['clusters'][0]['cluster']
    context = ssl.create_default_context(cadata=base64.b64decode(cluster['certificate-authority-data']).decode())
    endpoint = cluster['server'].rstrip('/')
    run = 'boundary-' + uuid.uuid4().hex[:10]
    receipt = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'run': run,
               'checks': [], 'pods': [], 'cleanup': []}
    created = []

    def record(name, ok, **fields):
        receipt['checks'].append(dict(name=name, passed=bool(ok), **fields))
        if len(receipt['checks']) % 10 == 0:
            print(json.dumps({'checked': len(receipt['checks']), 'latest': name}), flush=True)
        if not ok:
            raise AssertionError(name)

    def request(ns, role, method, path, value=None):
        headers = {'Authorization': 'Bearer ' + tokens[(ns, role)], 'Content-Type': 'application/json'}
        req = urllib.request.Request(endpoint + path, method=method, headers=headers,
                                     data=json.dumps(value).encode() if value is not None else None)
        try:
            with urllib.request.urlopen(req, context=context, timeout=15) as r:
                return r.status, json.load(r)
        except urllib.error.HTTPError as e:
            return e.code, json.load(e)

    tokens = {(ns, role): kubectl('-n', ns if role != 'controller' else 'argocd', 'create', 'token',
               ns + '-' + role if role != 'controller' else 'argocd-application-controller',
               '--duration=10m').stdout.strip()
              for ns in NAMESPACES for role in ['ci', 'viewer', 'runtime', 'controller']}

    def pod(ns):
        return {'apiVersion': 'v1', 'kind': 'Pod', 'metadata': {'name': run, 'namespace': ns,
                'labels': {'app': run, 'vela.ai/metrics': 'enabled'}}, 'spec': {
            'serviceAccountName': ns + '-runtime', 'automountServiceAccountToken': False,
            'preemptionPolicy': 'Never', 'priorityClassName': 'vela-application', 'terminationGracePeriodSeconds': 2,
            'nodeSelector': ({'vela.ai/management': 'true', 'vela.ai/control-plane-tier': 'cpu'} if ns == 'llm-api'
                             else {'vela.ai/node-role': 'gpu-worker'}),
            'securityContext': {'runAsNonRoot': True, 'runAsUser': 65532, 'runAsGroup': 65532,
                                'seccompProfile': {'type': 'RuntimeDefault'}},
            'containers': [{'name': 'probe', 'image': IMAGE, 'imagePullPolicy': 'IfNotPresent',
                'command': ['sh', '-c', "serve() { while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 14\\r\\nContent-Type: text/plain; version=0.0.4\\r\\nConnection: close\\r\\n\\r\\nboundary_ok 1\\n' | nc -l -p \"$1\"; done; }; serve 9090 & serve 8080"],
                'ports': [{'name': 'http', 'containerPort': 8080}, {'name': 'metrics', 'containerPort': 9090}],
                'securityContext': {'allowPrivilegeEscalation': False, 'readOnlyRootFilesystem': True,
                                    'capabilities': {'drop': ['ALL']}},
                'resources': {'requests': {'cpu': '50m', 'memory': '32Mi', 'ephemeral-storage': '64Mi'},
                              'limits': {'cpu': '100m', 'memory': '64Mi', 'ephemeral-storage': '128Mi'}},
                'volumeMounts': [{'name': 'tmp', 'mountPath': '/tmp'}],
                'readinessProbe': {'httpGet': {'path': '/', 'port': 8080}, 'periodSeconds': 2}}],
            'volumes': [{'name': 'tmp', 'emptyDir': {'sizeLimit': '64Mi'}}]}}

    def admin_admission(name, obj, allowed, fragment=None):
        p = kubectl('create', '--dry-run=server', '-f', '-', value=obj, check=False)
        ok = p.returncode == 0 if allowed else p.returncode != 0 and (fragment is None or fragment in p.stderr)
        record(name, ok, allowed=p.returncode == 0, expected_policy=fragment, diagnostic=p.stderr[:1800] if not ok else '')

    try:
        for ns in ([] if args.network_only else NAMESPACES):
            prefix = '/api/v1/namespaces/' + ns
            for method,path,body in [('GET',prefix+'/secrets',None),('POST',prefix+'/configmaps?dryRun=All',{'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':run}}),('POST','/apis/apps/v1/namespaces/'+ns+'/deployments?dryRun=All',{'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':run}})]:
                status,_=request(ns,'ci',method,path,body)
                record(ns+' retired CI authority rejects '+method+' '+path,status==403,status=status)
            for name, method, path, body in [
                ('other-secrets', 'GET', '/api/v1/namespaces/vela-system/secrets', None),
                ('other-tenant', 'GET', '/api/v1/namespaces/' + ('llm-models' if ns == 'llm-api' else 'llm-api') + '/secrets', None),
                ('nodes', 'GET', '/api/v1/nodes', None),
                ('rbac', 'POST', '/apis/rbac.authorization.k8s.io/v1/namespaces/' + ns + '/rolebindings',
                 {'apiVersion':'rbac.authorization.k8s.io/v1','kind':'RoleBinding','metadata':{'name':run},
                  'roleRef':{'apiGroup':'rbac.authorization.k8s.io','kind':'ClusterRole','name':'cluster-admin'},'subjects':[]}),
                ('serviceaccount-create', 'POST', prefix+'/serviceaccounts', {'apiVersion':'v1','kind':'ServiceAccount','metadata':{'name':run}}),
                ('token-mint', 'POST', prefix+'/serviceaccounts/'+ns+'-ci/token',
                 {'apiVersion':'authentication.k8s.io/v1','kind':'TokenRequest','spec':{'audiences':['https://kubernetes.default.svc'],'expirationSeconds':600}}),
                ('network-policy', 'POST', '/apis/networking.k8s.io/v1/namespaces/'+ns+'/networkpolicies',
                 {'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':{'name':run},'spec':{'podSelector':{}}}),
                ('monitoring-config', 'GET', '/apis/monitoring.coreos.com/v1/namespaces/'+ns+'/servicemonitors',None),
                ('raw-pod', 'POST', prefix+'/pods?dryRun=All',pod(ns)),
            ]:
                status, body = request(ns, 'controller', method, path, body)
                record(ns+' Argo controller rejects '+name,status == 403,status=status)
            for role,path,want in [('viewer', '/pods',200),('viewer','/secrets',403),('viewer','/configmaps',403),
                                   ('runtime','/secrets',403),('runtime','/pods',403),('ci','/secrets',403)]:
                status,_=request(ns,role,'GET',prefix+path)
                record(ns+' '+role+' GET '+path,status==want,status=status)
            cm={'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':run},'data':{'probe':'non-secret'}}
            for role,want in [('viewer',403),('runtime',403),('controller',201)]:
                status,_=request(ns,role,'POST',prefix+'/configmaps?dryRun=All',cm)
                record(ns+' '+role+' publish config',status==want,status=status)
            svc={'apiVersion':'v1','kind':'Service','metadata':{'name':run},'spec':{'type':'ClusterIP','selector':{'app':run},'ports':[{'port':8080,'targetPort':8080}]}}
            for case,change,want in [('clusterip',{},201),('nodeport',{'type':'NodePort'},403),
                                    ('loadbalancer',{'type':'LoadBalancer'},403),('externalip',{'externalIPs':['10.1.201.70']},403),
                                    ('externalname',{'type':'ExternalName','externalName':'kubernetes.default.svc'},403),('selectorless',{'selector':{}},403)]:
                obj=copy.deepcopy(svc);obj['spec'].update(change)
                status,body=request(ns,'controller','POST',prefix+'/services?dryRun=All',obj)
                record(ns+' service '+case,status==want,status=status,diagnostic=body.get('message','')[:1000] if status!=want else '')
            for typ,want in [('Opaque',403),('helm.sh/release.v1',403),('kubernetes.io/service-account-token',403)]:
                obj={'apiVersion':'v1','kind':'Secret','metadata':{'name':run,'annotations':{'kubernetes.io/service-account.name':ns+'-ci'}},'type':typ}
                status,_=request(ns,'controller','POST',prefix+'/secrets?dryRun=All',obj)
                record(ns+' secret '+typ,status==want,status=status)
            baseline=pod(ns)
            admin_admission(ns+' valid restricted Pod',baseline,True)
            modifications={
                'publisher-SA':({'serviceAccountName':ns+'-ci'},'runtime ServiceAccount'),
                'default-SA':({'serviceAccountName':'default'},'runtime ServiceAccount'),
                'automount':({'automountServiceAccountToken':True},'runtime ServiceAccount'),
                'nodeName':({'nodeName':'llmpool01'},'bypass the scheduler'),
                'wrong-selector':({'nodeSelector':{'kubernetes.io/os':'linux'}},'select CPU management'),
                'custom-scheduler':({'schedulerName':'bypass'},'default scheduler'),
                'preemption':({'preemptionPolicy':'PreemptLowerPriority'},'PreemptionPolicy'),
                'all-taints':({'tolerations':[{'operator':'Exists'}]},'bounded Kubernetes'),
                'windows':({'os':{'name':'windows'}},None),
                'host-network':({'hostNetwork':True},'PodSecurity'),
                'host-pid':({'hostPID':True},'PodSecurity'),
                'host-ipc':({'hostIPC':True},'PodSecurity'),
                'hostpath':({'volumes':[{'name':'tmp','hostPath':{'path':'/'}}]},None),
                'projected-token':({'volumes':[{'name':'tmp','projected':{'sources':[{'serviceAccountToken':{'path':'token','expirationSeconds':600}}]}}]},'API tokens'),
            }
            for name,(change,fragment) in modifications.items():
                obj=copy.deepcopy(baseline);obj['spec'].update(change)
                admin_admission(ns+' Pod rejects '+name,obj,False,fragment)
            for category in ['containers','initContainers','ephemeralContainers']:
                if category=='ephemeralContainers':
                    # The publisher cannot reach this subresource at all; PSA
                    # and the Pod policy also match it for platform debug use.
                    status,_=request(ns,'controller','PATCH',prefix+'/pods/'+run+'/ephemeralcontainers',{})
                    record(ns+' Argo controller rejects ephemeralcontainers',status==403,status=status)
                    continue
                for name,mutate in [('privileged',lambda c:c['securityContext'].update(privileged=True,allowPrivilegeEscalation=True)),
                                    ('escalation',lambda c:c['securityContext'].update(allowPrivilegeEscalation=True)),
                                    ('capabilities',lambda c:c['securityContext']['capabilities'].update(add=['SYS_ADMIN'])),
                                    ('hostport',lambda c:c.update(ports=[{'containerPort':8080,'hostPort':8080}])),
                                    ('floating-image',lambda c:c.update(image='alpine:latest'))]:
                    obj=copy.deepcopy(baseline)
                    if category=='initContainers':
                        c=copy.deepcopy(obj['spec']['containers'][0]);c['name']='init';c.pop('readinessProbe',None)
                        obj['spec'][category]=[c]
                    mutate(obj['spec'][category][0])
                    admin_admission(ns+' '+category+' rejects '+name,obj,False,'pinned' if name=='floating-image' else 'PodSecurity')
        if not args.api_only:
            # A real publisher creates controllers; the controller-created Pod
            # must satisfy PSA/admission and receive only the runtime identity.
            for ns in NAMESPACES:
                template=pod(ns)
                # Validation CPU probes stay off the GPU and protected .66.
                if ns=='llm-models':template['spec']['nodeSelector']['kubernetes.io/hostname']='server-22'
                deployment={'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':run,'namespace':ns},
                            'spec':{'replicas':1,'selector':{'matchLabels':{'app':run}},'template':{'metadata':{'labels':template['metadata']['labels']},'spec':template['spec']}}}
                status,_=request(ns,'controller','POST','/apis/apps/v1/namespaces/'+ns+'/deployments',deployment)
                record(ns+' Argo controller creates Deployment',status==201,status=status)
                created.append((ns,'deployment',run))
            deadline=time.monotonic()+150
            ready={}
            while time.monotonic()<deadline:
                for ns in NAMESPACES:
                    items=json.loads(kubectl('-n',ns,'get','pods','-l','app='+run,'-o','json').stdout)['items']
                    for p in items:
                        if any(c['type']=='Ready' and c['status']=='True' for c in p['status'].get('conditions',[])):
                            ready[ns]=p
                if len(ready)==2:break
                time.sleep(2)
            record('both publisher deployments become Ready',len(ready)==2)
            outsider=pod('llm-api')
            outsider['metadata']['namespace']='default'
            outsider['spec']['serviceAccountName']='default'
            outsider['spec']['containers'][0]['command']=['sleep','300']
            outsider['spec']['containers'][0].pop('readinessProbe',None)
            kubectl('create','-f','-',value=outsider)
            created.append(('default','pod',run))
            kubectl('-n','default','wait','--for=condition=Ready','pod/'+run,'--timeout=90s')
            for ns,p in ready.items():
                record(ns+' scheduled role',p['spec']['nodeName'] in (['llmpool01','llmpool02'] if ns=='llm-api' else ['server-22']),node=p['spec']['nodeName'])
                receipt['pods'].append({'namespace':ns,'node':p['spec']['nodeName'],'runtime_sa':p['spec']['serviceAccountName'],
                                        'image':p['spec']['containers'][0]['image'],'gpu_requests':0})
                source=p['metadata']['name']
                ephemeral={'name':'debug-check','image':IMAGE,'command':['true'],
                           'securityContext':{'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}}}
                for unsafe in [False,True]:
                    obj=copy.deepcopy(p);obj['spec']['ephemeralContainers']=[copy.deepcopy(ephemeral)]
                    if unsafe:obj['spec']['ephemeralContainers'][0]['securityContext'].update(privileged=True,allowPrivilegeEscalation=True)
                    test=kubectl('replace','--raw','/api/v1/namespaces/'+ns+'/pods/'+source+'/ephemeralcontainers?dryRun=All','-f','-',value=obj,check=False)
                    record(ns+' ephemeral restricted '+str(not unsafe),
                           test.returncode==0 if not unsafe else test.returncode!=0 and 'PodSecurity' in test.stderr,
                           allowed=test.returncode==0)
                test=kubectl('-n','default','exec',run,'--','nc','-z','-w','3',p['status']['podIP'],'8080',check=False)
                record(ns+' ingress rejects unrelated namespace',test.returncode!=0)
                def shell(script):
                    return kubectl('-n',ns,'exec',source,'--','sh','-c',script,check=False)
                ptest=shell('test ! -e /var/run/secrets/kubernetes.io/serviceaccount/token')
                record(ns+' runtime API token absent',ptest.returncode==0)
                ptest=shell('nslookup kubernetes.default.svc.cluster.local >/dev/null')
                record(ns+' DNS permitted',ptest.returncode==0)
                peer=ready['llm-models' if ns=='llm-api' else 'llm-api']['status']['podIP']
                ptest=shell('wget -q -T 5 -O - http://'+peer+':8080/')
                record(ns+' approved peer API permitted',ptest.returncode==0 and 'boundary_ok' in ptest.stdout)
                collector=json.loads(kubectl('-n','monitoring','get','svc','otel-collector','-o','json').stdout)['spec']['clusterIP']
                ptest=shell('nc -z -w 3 '+collector+' 4318')
                record(ns+' OTLP permitted',ptest.returncode==0)
                adminip=json.loads(kubectl('-n','apisix','get','svc','apisix-admin','-o','json').stdout)['spec']['clusterIP']
                for label,host,port in [('gateway-admin',adminip,9180),('kube-api','10.43.0.1',443),('host-ssh','10.1.201.70',22)]:
                    ptest=shell('nc -z -w 3 '+host+' '+str(port))
                    record(ns+' network rejects '+label,ptest.returncode!=0)
                # Use existing authorized sources, without changing them.
                for srcns,selector,port in [('apisix','app.kubernetes.io/name=apisix',8080)]:
                    sources=json.loads(kubectl('-n',srcns,'get','pods','-l',selector,'-o','json').stdout)['items']
                    selected=next(x for x in sources if x['status']['phase']=='Running')
                    url='http://'+p['status']['podIP']+':'+str(port)+('/metrics' if port==9090 else '/')
                    lua='local s=ngx.socket.tcp(); s:settimeout(5000); assert(s:connect('+json.dumps(p['status']['podIP'])+',8080)); assert(s:send(\"GET / HTTP/1.1\\r\\nHost: probe\\r\\nConnection: close\\r\\n\\r\\n\")); local line,e=s:receive(\"*l\"); assert(line==\"HTTP/1.1 200 OK\",e or line); s:close(); io.write(\"gateway-ok\")'
                    command=(['resty','-e',lua] if srcns=='apisix' else ['wget','-q','-T','5','-O','-',url])
                    ptest=kubectl('-n',srcns,'exec',selected['metadata']['name'],'--',*command,check=False)
                    record(ns+' ingress from '+srcns,ptest.returncode==0,allowed=True,diagnostic=(ptest.stderr+ptest.stdout)[:1600] if ptest.returncode else '')
                prometheus=json.loads(kubectl('-n','monitoring','get','svc','monitoring-kube-prometheus-prometheus','-o','json').stdout)['spec']['clusterIP']
                query='boundary_ok{namespace='+json.dumps(ns)+',pod='+json.dumps(source)+'}'
                deadline=time.monotonic()+90
                samples=[]
                print(json.dumps({'waiting_for':'actual Prometheus scrape','namespace':ns}),flush=True)
                while time.monotonic()<deadline:
                    with urllib.request.urlopen('http://'+prometheus+':9090/api/v1/query?'+urllib.parse.urlencode({'query':query}),timeout=8) as response:
                        samples=json.load(response).get('data',{}).get('result',[])
                    if samples and all(x['value'][1]=='1' for x in samples):break
                    time.sleep(2)
                errors=[]
                if not samples:
                    with urllib.request.urlopen('http://'+prometheus+':9090/api/v1/targets?state=active',timeout=8) as response:
                        targets=json.load(response).get('data',{}).get('activeTargets',[])
                    errors=[{'health':t.get('health'),'last_error':t.get('lastError')} for t in targets if t.get('labels',{}).get('pod')==source]
                record(ns+' actual Prometheus scrape',bool(samples) and all(x['value'][1]=='1' for x in samples),sample_count=len(samples),scrape_errors=errors)
        receipt['result']='APPLICATION_BOUNDARY_PASS'
    finally:
        for ns,kind,name in reversed(created):
            p=kubectl('-n',ns,'delete',kind,name,'--cascade=foreground','--wait=true','--timeout=30s',check=False)
            receipt['cleanup'].append({'namespace':ns,'kind':kind,'name':name,'deleted':p.returncode==0})
        Path(args.receipt).write_text(json.dumps(receipt,indent=2)+'\n')
    if any(not x['deleted'] for x in receipt['cleanup']):raise RuntimeError('cleanup incomplete')
    print(json.dumps({'result':receipt.get('result'),'checks':len(receipt['checks']),'receipt':args.receipt}))


if __name__=='__main__':main()
