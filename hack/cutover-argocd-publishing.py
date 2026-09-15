#!/usr/bin/env python3
"""Revoke the two legacy CI writer bindings after verified Argo publishing.

Only known legacy RoleBindings are removed. The Argo controller keeps the
namespace publisher Role; platform administration remains separate.
"""
import argparse
import datetime
import json
import os
from pathlib import Path
import subprocess

def k(*a,check=True):
    r=subprocess.run(['kubectl',*a],capture_output=True,text=True,timeout=35)
    if check and r.returncode:raise RuntimeError(r.stderr[:2000])
    return r

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--validation-receipt',required=True);p.add_argument('--run-directory',required=True);args=p.parse_args()
    os.umask(0o077);prior=json.loads(Path(args.validation_receipt).read_text())
    assert prior.get('result')=='PLATFORM_PUBLISHING_PASS' and all(c['passed'] for c in prior['checks']), 'live Argo validation must pass'
    root=Path(args.run_directory);root.mkdir(parents=True,exist_ok=True,mode=0o700)
    for obj in ['deployment/argocd-server','deployment/argocd-repo-server','statefulset/argocd-application-controller']:
        o=json.loads(k('-n','argocd','get',obj,'-o','json').stdout)
        assert o['status'].get('readyReplicas',0)==o['spec']['replicas'], obj+' is not Ready'
    bindings=[]
    for ns in ['llm-api','llm-models']:
        r=k('-n',ns,'get','rolebinding','application-publisher','-o','json',check=False)
        if r.returncode:
            assert 'NotFound' in r.stderr;continue
        value=json.loads(r.stdout)
        assert value['roleRef']=={'apiGroup':'rbac.authorization.k8s.io','kind':'Role','name':'application-publisher'}
        assert value['subjects']==[{'kind':'ServiceAccount','name':ns+'-ci','namespace':ns}], 'unexpected writer binding requires review'
        bindings.append(value)
    backup=root/'legacy-writer-bindings.json'
    if not backup.exists():backup.write_text(json.dumps(bindings,indent=2)+'\n')
    for value in bindings:k('-n',value['metadata']['namespace'],'delete','rolebinding','application-publisher')
    checks=[]
    for ns in ['llm-api','llm-models']:
        for identity,want in [('system:serviceaccount:'+ns+':'+ns+'-ci',False),('system:serviceaccount:argocd:argocd-application-controller',True),('system:serviceaccount:argocd:argocd-server',False)]:
            r=k('auth','can-i','create','deployments.apps','-n',ns,'--as='+identity,check=False)
            actual=r.returncode==0 and r.stdout.strip()=='yes'
            assert actual==want and r.stdout.strip() in ['yes','no'], 'writer postcheck failed'
            checks.append({'namespace':ns,'identity':identity,'can_create_deployments':actual})
    receipt={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checks':checks,'validation_receipt':args.validation_receipt,'result':'ARGO_ONLY_PUBLISHING_PASS'}
    (root/'cutover.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
if __name__=='__main__':main()
