#!/usr/bin/env python3
"""Render the pinned, namespace-scoped Argo CD installation. Requires PyYAML."""
import hashlib
from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parent
PINNED = {
 'namespace-install.yaml': 'df727dfc83666dbcc78dceef969505cf9c1659af774c64f9c08a204b09bd7dba',
 'application-crd.yaml': '5dde0e229249b6b707beb98674c1deae3949d5c319a6c45b9f5a80c99618e40c',
 'applicationset-crd.yaml': '7d282054f41ca2b71a22bab04a8caf0000e07e53a62432e6ea292dae5ab4f07d',
 'appproject-crd.yaml': 'ab225266944322750136f1198d93786e4a79a0e43c8d149abfba44da30a3eac8',
}
ARGO = 'quay.io/argoproj/argocd@sha256:dd3f47d5a5e4da563a7a398506e892481b358a7cec50abdf320c71aa55904bfa'
REDIS = 'docker.io/library/redis@sha256:08ad0b1d280850169a790dba1393ff7a90aef951fc19632cf4d3ce4f78e679ba'
RESOURCES = [('', x) for x in ['ConfigMap','Service']] + [('apps', x) for x in ['Deployment','StatefulSet','ReplicaSet']] + [('batch', x) for x in ['Job','CronJob']] + [('autoscaling','HorizontalPodAutoscaler'),('policy','PodDisruptionBudget')]

def obj(kind, name, spec=None, api='v1', namespace='argocd', **fields):
    x = {'apiVersion': api, 'kind': kind, 'metadata': {'name': name}}
    if namespace: x['metadata']['namespace'] = namespace
    if spec is not None: x['spec'] = spec
    x.update(fields)
    return x

def render():
    for name, digest in PINNED.items():
        assert hashlib.sha256((ROOT/'upstream'/name).read_bytes()).hexdigest() == digest, name
    ns = obj('Namespace', 'argocd', namespace=None)
    ns['metadata']['labels'] = {'app.kubernetes.io/part-of':'vela', 'pod-security.kubernetes.io/enforce':'restricted', 'pod-security.kubernetes.io/enforce-version':'v1.35'}
    result = [ns]
    for item in yaml.safe_load_all((ROOT/'upstream/namespace-install.yaml').read_text()):
        name = item['metadata']['name']
        if any(x in name for x in ['applicationset','dex','notifications']): continue
        if item['kind']=='NetworkPolicy': continue
        assert item['kind'] not in ['ClusterRole', 'ClusterRoleBinding']
        item['metadata']['namespace']='argocd'
        if item['kind']=='RoleBinding':
            for s in item['subjects']:
                if s['kind']=='ServiceAccount':s['namespace']='argocd'
        if item['kind'] in ['Deployment','StatefulSet']:
            spec=item['spec'];spec['replicas']=2 if name in ['argocd-server','argocd-repo-server'] else 1
            pod=spec['template']['spec']
            pod['nodeSelector']={'vela.ai/control-plane-tier':'cpu'}
            pod['tolerations']=[{'key':'node-role.kubernetes.io/control-plane','operator':'Exists','effect':'NoSchedule'}]
            pod.setdefault('securityContext', {}).update(runAsNonRoot=True, seccompProfile={'type':'RuntimeDefault'})
            if spec['replicas']==2:
                pod['affinity']={'podAntiAffinity':{'requiredDuringSchedulingIgnoredDuringExecution':[{'topologyKey':'kubernetes.io/hostname','labelSelector':spec['selector']}]}}
                # Avoid a surge Pod that cannot satisfy the required anti-affinity.
                spec['strategy']={'type':'RollingUpdate','rollingUpdate':{'maxSurge':0,'maxUnavailable':1}}
            for c in pod.get('containers',[])+pod.get('initContainers',[]):
                c['image']=REDIS if c['name']=='redis' else ARGO
                c['imagePullPolicy']='IfNotPresent'
                c['resources']={'requests':{'cpu':'100m','memory':'128Mi','ephemeral-storage':'128Mi'},'limits':{'cpu':'1','memory':'512Mi','ephemeral-storage':'1Gi'}}
                if name=='argocd-application-controller':c['resources']['limits']['memory']='1Gi'
                if name=='argocd-repo-server':
                    c['resources']['requests']['memory']='256Mi';c['resources']['limits']['memory']='1Gi';c['resources']['limits']['ephemeral-storage']='2Gi'
                if name=='argocd-server':
                    c.setdefault('env', []).append({'name':'SSL_CERT_DIR','value':'/etc/ssl/certs:/app/config/extra-ca'})
                    c.setdefault('volumeMounts', []).append({'name':'gateway-ca','mountPath':'/app/config/extra-ca','readOnly':True})
                    for probe in ['readinessProbe','livenessProbe']:
                        if 'httpGet' in c.get(probe,{}):c[probe]['httpGet']['path']='/argocd/healthz'
            if name=='argocd-server':
                pod.setdefault('volumes', []).append({'name':'gateway-ca','configMap':{'name':'vela-gateway-ca'}})
            if name=='argocd-redis':
                # Password is created by the operator before install. Redis has no API token.
                pod.pop('initContainers',None);pod['automountServiceAccountToken']=False
        if item['kind'] in ['Role','RoleBinding'] and name=='argocd-redis':continue
        if name=='argocd-cmd-params-cm':
            item['data']={'server.insecure':'true','server.rootpath':'/argocd','server.basehref':'/argocd','server.log.format':'json','controller.log.format':'json','reposerver.log.format':'json','reposerver.parallelism.limit':'4'}
        if name=='argocd-cm':
            item['data']={'url':'https://10.1.201.70:30443/argocd','admin.enabled':'false','users.anonymous.enabled':'false','exec.enabled':'false','timeout.reconciliation':'180s','application.resourceTrackingMethod':'annotation', 'resource.respectRBAC':'strict'}
        if name=='argocd-rbac-cm':
            lines=['g, platform-admins, role:admin']
            for team in ['llm-api','llm-models']:
                for role in ['publisher','approver','viewer']:
                    r=f'role:{team}-{role}'
                    lines.extend([f'p, {r}, applications, get, {team}/*, allow',f'p, {r}, logs, get, {team}/*, allow',f'p, {r}, projects, get, {team}, allow',f'g, {team}-{role}s, {r}'])
                    if role=='approver':lines.append(f'p, {r}, applications, sync, {team}/*, allow')
            item['data']={'policy.default':'role:none','policy.matchMode':'glob','scopes':'[groups]','policy.csv':'\n'.join(lines)+'\n'}
        result.append(item)
    # Empty repository lists fail closed until the existing GitLab is explicitly onboarded.
    for team in ['default','llm-api','llm-models']:
        result.append(obj('AppProject',team,{'description':'Platform-managed publishing boundary; GitLab onboarding deferred','sourceRepos':[], 'destinations':([] if team=='default' else [{'server':'https://kubernetes.default.svc','namespace':team}]),'clusterResourceWhitelist':[], 'namespaceResourceWhitelist':([] if team=='default' else [{'group':g,'kind':k} for g,k in RESOURCES]), 'orphanedResources':{'warn':True}},api='argoproj.io/v1alpha1'))
    # No bearerToken: Argo uses its own in-cluster SA; namespace restriction limits cache discovery.
    cluster=obj('Secret','in-cluster-applications',stringData={'name':'in-cluster','server':'https://kubernetes.default.svc','namespaces':'llm-api,llm-models','clusterResources':'false','config':'{}'})
    cluster['metadata']['labels']={'argocd.argoproj.io/secret-type':'cluster'};result.append(cluster)
    for team in ['llm-api','llm-models']:
        for sa,role in [('argocd-application-controller','application-publisher'),('argocd-server','application-viewer')]:
            result.append(obj('RoleBinding',sa,api='rbac.authorization.k8s.io/v1',namespace=team,roleRef={'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':role},subjects=[{'kind':'ServiceAccount','name':sa,'namespace':'argocd'}]))
    for name in ['argocd-server','argocd-repo-server']:
        result.append(obj('PodDisruptionBudget',name,{'minAvailable':1,'selector':{'matchLabels':{'app.kubernetes.io/name':name}}},api='policy/v1'))
    # Strict ingress, and egress only for discovery, own services and the SSO gateway.
    selector=lambda name:{'podSelector':{'matchLabels':{'app.kubernetes.io/name':name}}}
    nspeer=lambda ns, labels=None:dict(namespaceSelector={'matchLabels':{'kubernetes.io/metadata.name':ns}},**({'podSelector':{'matchLabels':labels}} if labels else {}))
    ports=lambda *p:[{'protocol':'TCP','port':n} for n in p]
    dns={'to':[nspeer('kube-system',{'k8s-app':'kube-dns'})],'ports':ports(53)+[{'protocol':'UDP','port':53}]}
    kube={'to':[{'ipBlock':{'cidr':ip+'/32'}} for ip in ['10.43.0.1','10.1.201.70','10.1.201.71','10.1.201.66']],'ports':ports(443,6443)}
    metrics={'from':[nspeer('monitoring',{'app.kubernetes.io/name':'prometheus'})],'ports':ports(8082,8083,8084)}
    for name in ['argocd-server','argocd-repo-server','argocd-application-controller','argocd-redis']:
        ingress=[metrics] if name!='argocd-redis' else []
        egress=[dns]
        if name=='argocd-server':
            ingress.append({'from':[nspeer('apisix',{'app.kubernetes.io/name':'apisix','app.kubernetes.io/instance':'apisix'})],'ports':ports(8080)})
            egress.extend([kube,{'to':[nspeer('apisix',{'app.kubernetes.io/name':'apisix','app.kubernetes.io/instance':'apisix'}),{'ipBlock':{'cidr':'10.1.201.70/32'}},{'ipBlock':{'cidr':'10.1.201.71/32'}}],'ports':ports(9443,30443)}])
        if name=='argocd-application-controller':egress.append(kube)
        if name in ['argocd-server','argocd-application-controller']:
            egress.append({'to':[selector('argocd-repo-server')],'ports':ports(8081)})
        if name!='argocd-redis':egress.append({'to':[selector('argocd-redis')],'ports':ports(6379)})
        if name=='argocd-redis':ingress.append({'from':[selector(n) for n in ['argocd-server','argocd-application-controller','argocd-repo-server']],'ports':ports(6379)})
        if name=='argocd-repo-server':ingress.append({'from':[selector(n) for n in ['argocd-server','argocd-application-controller']],'ports':ports(8081)})
        result.append(obj('NetworkPolicy',name,{'podSelector':selector(name)['podSelector'],'policyTypes':['Ingress','Egress'],'ingress':ingress,'egress':egress},api='networking.k8s.io/v1'))
    return result

if __name__=='__main__':
    print(yaml.safe_dump_all(render(),sort_keys=False))
