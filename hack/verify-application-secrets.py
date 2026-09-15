#!/usr/bin/env python3
"""Exercise platform-controlled runtime Secret references with server dry runs.

Temporary namespace annotations are restored in finally. No Secret contents are
read; no disks, host daemons or application workloads are changed.
"""
import argparse
import copy
import datetime
import json
import os
from pathlib import Path
import subprocess
import uuid

KEY='vela.ai/runtime-secret-names'
IMAGE='docker.io/library/alpine@sha256:14358309a308569c32bdc37e2e0e9694be33a9d99e68afb0f5ff33cc1f695dce'
def k(*args,value=None,check=True):
    p=subprocess.run(['kubectl',*args],input=json.dumps(value) if value is not None else None,text=True,capture_output=True,timeout=35)
    if check and p.returncode:raise RuntimeError(p.stderr[:1500])
    return p

def template(ns,name):
    return {'apiVersion':'v1','kind':'Pod','metadata':{'namespace':ns,'name':name},'spec':{'serviceAccountName':ns+'-runtime','automountServiceAccountToken':False,'preemptionPolicy':'Never','priorityClassName':'vela-application','nodeSelector':({'vela.ai/management':'true','vela.ai/control-plane-tier':'cpu'} if ns=='llm-api' else {'vela.ai/node-role':'gpu-worker'}),'securityContext':{'runAsNonRoot':True,'runAsUser':65532,'seccompProfile':{'type':'RuntimeDefault'}},'containers':[{'name':'probe','image':IMAGE,'command':['sleep','300'],'securityContext':{'allowPrivilegeEscalation':False,'capabilities':{'drop':['ALL']}},'resources':{'requests':{'cpu':'50m','memory':'32Mi','ephemeral-storage':'64Mi'},'limits':{'cpu':'100m','memory':'64Mi','ephemeral-storage':'128Mi'}}}]}}

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--receipt',required=True);args=p.parse_args();os.umask(0o077)
    receipt={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checks':[],'annotation_restored':[]}
    name='secret-boundary-'+uuid.uuid4().hex[:8]
    try:
        for ns in ['llm-api','llm-models']:
            original=json.loads(k('get','ns',ns,'-o','json').stdout)['metadata'].get('annotations',{}).get(KEY)
            try:
                for permitted in [False,True]:
                    k('patch','ns',ns,'--type=merge','-p',json.dumps({'metadata':{'annotations':{KEY:name if permitted else None}}}))
                    for case in ['env','envFrom','init-env','init-envFrom','volume','projected','pull-only','pull-mounted','substring']:
                        value=template(ns,name)
                        ref=name if case!='substring' else name[:-1]
                        if case in ['env','envFrom','init-env','init-envFrom','substring']:
                            c=value['spec']['containers'][0]
                            if case.startswith('init-'):
                                c=copy.deepcopy(c);c['name']='init';value['spec']['initContainers']=[c]
                            if case.endswith('envFrom'):c['envFrom']=[{'secretRef':{'name':ref}}]
                            else:c['env']=[{'name':'VALUE','valueFrom':{'secretKeyRef':{'name':ref,'key':'value'}}}]
                        if case in ['volume','projected','pull-mounted']:
                            src={'secret':{'secretName':ref if case!='pull-mounted' else 'vela-release-pull-v1'}}
                            if case=='projected':src={'projected':{'sources':[{'secret':{'name':ref}}]}}
                            value['spec']['volumes']=[dict(name='data',**src)]
                            value['spec']['containers'][0]['volumeMounts']=[{'name':'data','mountPath':'/secret','readOnly':True}]
                        if case=='pull-only':value['spec']['imagePullSecrets']=[{'name':'vela-release-pull-v1'}]
                        expect=case=='pull-only' or permitted and case not in ['pull-mounted','substring']
                        r=k('create','--dry-run=server','-f','-',value=value,check=False)
                        ok=r.returncode==0 if expect else r.returncode!=0 and 'Runtime Secret references require' in r.stderr
                        receipt['checks'].append({'namespace':ns,'case':case,'approved':permitted,'allowed':r.returncode==0,'passed':ok})
                        if not ok:raise RuntimeError(case+' '+r.stderr[:1000])
            finally:
                k('patch','ns',ns,'--type=merge','-p',json.dumps({'metadata':{'annotations':{KEY:original}}}))
                actual=json.loads(k('get','ns',ns,'-o','json').stdout)['metadata'].get('annotations',{}).get(KEY)
                assert actual==original;receipt['annotation_restored'].append(ns)
        receipt['result']='APPLICATION_SECRET_REFERENCES_PASS'
    finally:
        Path(args.receipt).write_text(json.dumps(receipt,indent=2)+'\n')
    print(json.dumps({'result':receipt['result'],'checks':len(receipt['checks']),'annotation_restored':receipt['annotation_restored']}))
if __name__=='__main__':main()
