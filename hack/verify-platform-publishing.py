#!/usr/bin/env python3
"""Verify real Argo SSO, tenant permissions, sync and rollback with disposable data.

Run on .70 as a platform operator. Uses the production PKCE browser login and
TOTP, temporary Keycloak users, a read-only local Git fixture, and CPU-only Pods.
No GitLab changes. Every temporary identity, route, allow-rule and workload is
removed in finally. Credentials stay in memory. Requires PyYAML.
"""
import argparse
import base64
import copy
import datetime
import hashlib
import hmac
import html
from http.cookiejar import CookieJar
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import ssl
import struct
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import yaml

IMAGE='docker.io/library/alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce'

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--run-directory',required=True)
    parser.add_argument('--sso-helper',required=True)
    args=parser.parse_args();os.umask(0o077)
    root=Path(args.run_directory);root.mkdir(mode=0o700,parents=True,exist_ok=False)
    spec=importlib.util.spec_from_file_location('sso',args.sso_helper);sso=importlib.util.module_from_spec(spec);spec.loader.exec_module(sso)
    k=sso.k;get=sso.get;http=sso.http
    run='publishing-'+secrets.token_hex(4)
    receipt={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'run':run,'checks':[],'cleanup':[]}
    users=[];apps=[];projects={};gitserver=None;np=False
    tokens={};browsers={}
    def record(name,ok,**extra):
        receipt['checks'].append(dict(name=name,passed=bool(ok),**extra))
        print(json.dumps(receipt['checks'][-1]),flush=True)
        if not ok:raise AssertionError(name)
    base='http://'+get('identity','svc','keycloak')['spec']['clusterIP']+':8080'
    credentials={key:base64.b64decode(v).decode() for key,v in get('identity','secret','keycloak-admin')['data'].items()}
    def admin_headers():
        return {'Authorization':'Bearer '+http(base+'/realms/master/protocol/openid-connect/token','POST',form=dict(client_id='admin-cli',grant_type='password',**credentials))['access_token']}
    ep=base+'/admin/realms/'+sso.REALM
    ca=base64.b64decode(get('apisix','secret','vela-gateway-tls')['data']['ca.crt']).decode();ctx=ssl.create_default_context(cadata=ca)
    origin=sso.ORIGIN
    def api(role,path,method='GET',value=None,retried=False):
        data=json.dumps(value).encode() if value is not None else None
        req=urllib.request.Request(origin+'/argocd/api/v1'+path,method=method,data=data,headers={'Content-Type':'application/json'})
        try:
            with browsers[role].open(req,timeout=30) as r:return r.status,json.load(r)
        except urllib.error.HTTPError as e:
            body=json.load(e)
            if e.code==401 and not retried:
                # ID tokens last five minutes; renew through the existing MFA SSO session.
                with browsers[role].open(origin+'/argocd/auth/login',timeout=30) as r:r.read()
                return api(role,path,method,value,retried=True)
            return e.code,body
    def git(*a,cwd):return subprocess.check_output(['git',*a],cwd=cwd,text=True,stderr=subprocess.DEVNULL).strip()
    def apply(value):return k('apply','--server-side','--field-manager=vela-publishing-verification','-f','-',value=value)
    def manifest(ns,name,bad=False):
        labels={'app':name,'vela.ai/verification':run}
        pod={'serviceAccountName':ns+'-runtime','automountServiceAccountToken':False,'preemptionPolicy':'Never','priorityClassName':'vela-application','terminationGracePeriodSeconds':1,'nodeSelector':({'vela.ai/management':'true','vela.ai/control-plane-tier':'cpu'} if ns=='llm-api' else {'vela.ai/node-role':'gpu-worker','kubernetes.io/hostname':'server-22'}),'securityContext':{'runAsNonRoot':True,'runAsUser':65532,'seccompProfile':{'type':'RuntimeDefault'}},'containers':[{'name':'probe','image':IMAGE,'command':['sh','-c','exit 1' if bad else "while true; do printf 'HTTP/1.1 200 OK\\r\\nContent-Length: 3\\r\\nConnection: close\\r\\n\\r\\nv1\\n' | nc -l -p 8080; done"],'ports':[{'containerPort':8080,'name':'http'}],'readinessProbe':{'httpGet':{'path':'/','port':8080},'periodSeconds':2},'securityContext':{'allowPrivilegeEscalation':False,'readOnlyRootFilesystem':True,'capabilities':{'drop':['ALL']}},'resources':{'requests':{'cpu':'50m','memory':'32Mi','ephemeral-storage':'64Mi'},'limits':{'cpu':'100m','memory':'64Mi','ephemeral-storage':'128Mi'}}}]}
        dep={'apiVersion':'apps/v1','kind':'Deployment','metadata':{'name':name,'namespace':ns,'labels':labels},'spec':{'replicas':1,'progressDeadlineSeconds':20,'strategy':{'type':'RollingUpdate','rollingUpdate':{'maxSurge':0,'maxUnavailable':1}},'selector':{'matchLabels':{'app':name}},'template':{'metadata':{'labels':labels},'spec':pod}}}
        return [dep,{'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':name,'namespace':ns,'labels':labels},'data':{'version':'broken' if bad else 'v1'}},{'apiVersion':'v1','kind':'Service','metadata':{'name':name,'namespace':ns,'labels':labels},'spec':{'selector':{'app':name},'ports':[{'name':'http','port':8080,'targetPort':8080}]}}]
    def wait_app(name,predicate,seconds=120):
        end=time.monotonic()+seconds;last=None
        while time.monotonic()<end:
            last=get('argocd','application',name)
            if predicate(last):return last
            time.sleep(2)
        raise RuntimeError('application wait failed '+name+' '+json.dumps(last.get('status',{}))[:2500])
    try:
        groups={g['name']:g['id'] for g in http(ep+'/groups',headers=admin_headers())}
        roles=['platform-admins']+[f'{ns}-{role}s' for ns in ['llm-api','llm-models'] for role in ['publisher','approver','viewer']]
        for role in roles:
            username=run+'-'+str(len(users));password=secrets.token_urlsafe(24)+'aA1';otp=secrets.token_urlsafe(20)
            value={'username':username,'enabled':True,'firstName':'Temporary','lastName':'Validation','email':username+'@invalid.example','emailVerified':True,'requiredActions':[],'credentials':[{'type':'password','value':password,'temporary':False},{'type':'otp','userLabel':'Verification only','secretData':json.dumps({'value':otp}),'credentialData':json.dumps({'subType':'totp','digits':6,'period':30,'algorithm':'HmacSHA1'})}]}
            http(ep+'/users','POST',value,headers=admin_headers())
            uid=http(ep+'/users?exact=true&username='+username,headers=admin_headers())[0]['id'];users.append(uid)
            http(ep+'/users/'+uid,'PUT',{'requiredActions':[]},headers=admin_headers())
            http(ep+'/users/'+uid+'/groups/'+groups[role],'PUT',headers=admin_headers())
            jar=CookieJar();opener=urllib.request.build_opener(urllib.request.HTTPCookieProcessor(jar),urllib.request.HTTPSHandler(context=ctx));browsers[role]=opener
            with opener.open(origin+'/argocd/auth/login',timeout=25) as r:
                record(role+' uses PKCE S256','code_challenge_method=S256' in r.url);body=r.read().decode()
            def submit(body,data):
                action=html.unescape(re.search(r'<form[^>]+action="([^"]+)"',body).group(1))
                with opener.open(urllib.request.Request(action,data=urllib.parse.urlencode(data).encode()),timeout=25) as r:return r.status,r.url,r.read().decode()
            status,url,body=submit(body,{'username':username,'password':password,'credentialId':''})
            record(role+' password requires TOTP','name="otp"' in body and not any(c.name=='argocd.token' for c in jar))
            digest=hmac.new(otp.encode(),struct.pack('>Q',int(time.time())//30),hashlib.sha1).digest();off=digest[-1]&15
            code=str((struct.unpack('>I',digest[off:off+4])[0]&0x7fffffff)%1000000).zfill(6)
            status,url,body=submit(body,{'otp':code,'login':'Sign In'})
            status,who=api(role,'/session/userinfo');record(role+' authenticated after MFA',status==200 and who.get('loggedIn') and role in who.get('groups',[]),status=status)
        # The fixture is deliberately not a production Git service or a GitLab approval test.
        work=root/'work';work.mkdir();public=root/'public';public.mkdir()
        git('init','--initial-branch=main',cwd=work)
        git('config','user.name','Vela Verification',cwd=work);git('config','user.email','verification@invalid.example',cwd=work)
        names={ns:run+('-api' if ns=='llm-api' else '-models') for ns in ['llm-api','llm-models']}
        for ns,name in names.items():
            d=work/ns;d.mkdir();(d/'resources.yaml').write_text(yaml.safe_dump_all(manifest(ns,name),sort_keys=False))
        unsafe=work/'unsafe';unsafe.mkdir();(unsafe/'secret.yaml').write_text(yaml.safe_dump({'apiVersion':'v1','kind':'Secret','metadata':{'name':run+'-forbidden','namespace':'llm-api'},'stringData':{'value':'non-sensitive-validation'}}))
        git('add','.',cwd=work);git('commit','-m','verified fixture v1',cwd=work);v1=git('rev-parse','HEAD',cwd=work)
        (work/'llm-api/resources.yaml').write_text(yaml.safe_dump_all(manifest('llm-api',names['llm-api'],bad=True),sort_keys=False));git('add','.',cwd=work);git('commit','-m','deliberately unhealthy fixture',cwd=work);v2=git('rev-parse','HEAD',cwd=work)
        git('clone','--bare',str(work),str(public/'fixture.git'),cwd=root);git('--git-dir='+str(public/'fixture.git'),'update-server-info',cwd=root)
        with (root/'http.log').open('w') as log:gitserver=subprocess.Popen(['python3','-m','http.server','18765','--bind','10.1.201.70','--directory',str(public)],stdout=log,stderr=log,start_new_session=True)
        repo='http://10.1.201.70:18765/fixture.git'
        apply({'apiVersion':'networking.k8s.io/v1','kind':'NetworkPolicy','metadata':{'name':run,'namespace':'argocd'},'spec':{'podSelector':{'matchLabels':{'app.kubernetes.io/name':'argocd-repo-server'}},'policyTypes':['Egress'],'egress':[{'to':[{'ipBlock':{'cidr':'10.1.201.70/32'}}],'ports':[{'protocol':'TCP','port':18765}]}]}});np=True
        for ns,name in names.items():
            project=get('argocd','appproject',ns);projects[ns]=project['spec']['sourceRepos']
            k('-n','argocd','patch','appproject',ns,'--type=merge','-p',json.dumps({'spec':{'sourceRepos':[repo]}}))
            app={'apiVersion':'argoproj.io/v1alpha1','kind':'Application','metadata':{'name':name,'namespace':'argocd','labels':{'vela.ai/verification':run}},'spec':{'project':ns,'source':{'repoURL':repo,'path':ns,'targetRevision':v1},'destination':{'server':'https://kubernetes.default.svc','namespace':ns},'syncPolicy':{'syncOptions':['FailOnSharedResource=true']}}}
            apply(app);apps.append(name)
        for ns,name in names.items():
            other=names['llm-models' if ns=='llm-api' else 'llm-api']
            for suffix in ['publishers','approvers','viewers']:
                role=ns+'-'+suffix
                status,data=api(role,'/applications');listed=[a['metadata']['name'] for a in (data.get('items') or [])]
                record(role+' sees only own project',status==200 and name in listed and other not in listed)
                status,_=api(role,'/applications/'+other);record(role+' rejects other project read',status==403,status=status)
                status,_=api(role,'/applications/'+other+'/sync','POST',{'revision':v1});record(role+' rejects other project sync',status==403,status=status)
                status,_=api(role,'/applications/'+name+'/spec','PUT',dict(get('argocd','application',name)['spec'],destination={'server':'https://kubernetes.default.svc','namespace':'kube-system'}));record(role+' cannot edit destination or source',status==403,status=status)
                status,_=api(role,'/applications/'+name+'/resource?namespace='+ns+'&resourceName='+name+'&version=v1&kind=ConfigMap','DELETE');record(role+' cannot directly delete resources',status==403,status=status)
                if suffix!='approvers':
                    status,_=api(role,'/applications/'+name+'/sync','POST',{'revision':v1});record(role+' cannot sync',status==403,status=status)
            status,data=api(ns+'-approvers','/applications/'+name+'/sync','POST',{'revision':v1,'prune':True})
            record(ns+' approver starts sync',status==200,status=status,diagnostic=data if status!=200 else None)
            synced=wait_app(name,lambda a:a.get('status',{}).get('operationState',{}).get('phase')=='Succeeded' and a.get('status',{}).get('health',{}).get('status')=='Healthy')
            pod=json.loads(k('-n',ns,'get','pods','-l','app='+name,'-o','json'))['items'][0]
            record(ns+' workload healthy and placed correctly',pod['spec']['nodeName'] in (['llmpool01','llmpool02'] if ns=='llm-api' else ['server-22']),node=pod['spec']['nodeName'],revision=v1)
            record(ns+' runtime has no API token',pod['spec'].get('automountServiceAccountToken') is False)
            # A platform dry run checks ephemeral Secret references without starting a debug process.
            for allowed in [False,True]:
                annotation=get('','namespace',ns)['metadata'].get('annotations',{}).get('vela.ai/runtime-secret-names')
                try:
                    k('patch','ns',ns,'--type=merge','-p',json.dumps({'metadata':{'annotations':{'vela.ai/runtime-secret-names':run if allowed else None}}}))
                    obj=copy.deepcopy(pod);obj['spec']['ephemeralContainers']=[{'name':'secret-check','image':IMAGE,'command':['true'],'env':[{'name':'VALUE','valueFrom':{'secretKeyRef':{'name':run,'key':'value'}}}],'securityContext':{'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}}}]
                    p=subprocess.run(['kubectl','replace','--raw','/api/v1/namespaces/'+ns+'/pods/'+pod['metadata']['name']+'/ephemeralcontainers?dryRun=All','-f','-'],input=json.dumps(obj),text=True,capture_output=True,timeout=30)
                    record(ns+' ephemeral Secret approved='+str(allowed),p.returncode==0 if allowed else p.returncode!=0 and 'Runtime Secret references require' in p.stderr)
                finally:k('patch','ns',ns,'--type=merge','-p',json.dumps({'metadata':{'annotations':{'vela.ai/runtime-secret-names':annotation}}}))
        role='llm-api-approvers';name=names['llm-api']
        status,_=api(role,'/applications/'+name+'/sync','POST',{'revision':v2,'prune':True});record('approver can attempt a new revision',status==200)
        failed=wait_app(name,lambda a:a.get('status',{}).get('health',{}).get('status')=='Degraded',seconds=100)
        record('deliberately broken rollout is visible as Degraded',True,revision=v2)
        history=next(h for h in failed['status']['history'] if h['revision']==v1)
        status,body=api(role,'/applications/'+name+'/rollback','POST',{'id':history['id'],'prune':True});record('approver starts rollback',status==200,status=status,diagnostic=body if status!=200 else None)
        restored=wait_app(name,lambda a:a.get('status',{}).get('operationState',{}).get('phase')=='Succeeded' and a.get('status',{}).get('health',{}).get('status')=='Healthy' and a.get('status',{}).get('sync',{}).get('revision')==v1)
        record('rollback restores healthy revision',True,revision=v1)
        for ns in ['llm-api','llm-models']:
            for who in ['argocd-application-controller','argocd-server']:
                for res in ['secrets','roles','nodes','networkpolicies','serviceaccounts/token']:
                    p=subprocess.run(['kubectl','auth','can-i','create' if res in ['roles','networkpolicies','serviceaccounts/token'] else 'get',res,'-n',ns,'--as=system:serviceaccount:argocd:'+who],capture_output=True,text=True,timeout=20)
                    record(who+' rejects '+ns+'/'+res,p.returncode==1 and p.stdout.strip()=='no')
        # Secret manifests must fail before creation. Kubernetes RBAC may reject the
        # controller's live-state read before the AppProject validator is reached.
        unsafe_name=run+'-unsafe';app=copy.deepcopy(get('argocd','application',names['llm-api']));app={'apiVersion':app['apiVersion'],'kind':app['kind'],'metadata':{'name':unsafe_name,'namespace':'argocd'},'spec':app['spec']};app['spec']['source']['path']='unsafe';apply(app);apps.append(unsafe_name)
        status,_=api(role,'/applications/'+unsafe_name+'/sync','POST',{'revision':v1})
        if status==200:
            unsafe_result=wait_app(unsafe_name,lambda a:a.get('status',{}).get('operationState',{}).get('phase') in ['Failed','Error'])
            detail=json.dumps(unsafe_result['status'])
        else:detail=str(status)
        record('publishing boundary rejects Secret synchronization',status in [400,403] or 'not permitted' in detail or 'not allowed' in detail or ('secrets' in detail and 'is forbidden' in detail),status=status,diagnostic=detail[:2000])
        p=subprocess.run(['kubectl','-n','llm-api','get','secret',run+'-forbidden'],capture_output=True,text=True);record('forbidden Secret never created',p.returncode!=0 and 'NotFound' in p.stderr)
        receipt['result']='PLATFORM_PUBLISHING_PASS'
    except Exception as e:
        receipt['error']=str(e)[:3000];raise
    finally:
        for name in apps:
            try:k('-n','argocd','delete','application',name,'--ignore-not-found=true','--wait=false');receipt['cleanup'].append('application/'+name)
            except Exception as e:receipt['cleanup'].append('FAILED application/'+name+': '+str(e)[:200])
        for ns in ['llm-api','llm-models']:
            try:k('-n',ns,'delete','deploy,svc,cm','-l','vela.ai/verification='+run,'--ignore-not-found=true','--wait=false');receipt['cleanup'].append(ns+' workloads')
            except Exception as e:receipt['cleanup'].append('FAILED '+ns+': '+str(e)[:200])
        for ns,repos in projects.items():
            try:k('-n','argocd','patch','appproject',ns,'--type=merge','-p',json.dumps({'spec':{'sourceRepos':repos}}));receipt['cleanup'].append(ns+' sourceRepos restored')
            except Exception as e:receipt['cleanup'].append('FAILED sourceRepos '+ns+': '+str(e)[:200])
        if np:
            try:k('-n','argocd','delete','networkpolicy',run,'--ignore-not-found=true');receipt['cleanup'].append('fixture network rule')
            except Exception as e:receipt['cleanup'].append('FAILED network rule: '+str(e)[:200])
        if gitserver:gitserver.terminate();gitserver.wait(timeout=10);receipt['cleanup'].append('fixture HTTP server stopped')
        for uid in users:
            try:http(ep+'/users/'+uid,'DELETE',headers=admin_headers());receipt['cleanup'].append('temporary user/'+uid)
            except Exception as e:receipt['cleanup'].append('FAILED temporary user/'+uid+': '+str(e)[:200])
        if any(x.startswith('FAILED') for x in receipt['cleanup']):receipt['result']='CLEANUP_FAILED'
        (root/'receipt.json').write_text(json.dumps(receipt,indent=2)+'\n')
        print(json.dumps({'result':receipt.get('result','FAILED'),'checks':len(receipt['checks']),'cleanup':receipt['cleanup'],'receipt':str(root/'receipt.json')}),flush=True)
if __name__=='__main__':main()
