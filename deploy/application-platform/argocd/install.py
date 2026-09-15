#!/usr/bin/env python3
"""Install the fixed Argo package on an existing Vela CPU management node.

Does not install GitLab or grant CI direct cluster access. Requires an existing
platform-admin kubeconfig, PyYAML, and the upstream files beside this script.
"""
import importlib.util
import json
import os
from pathlib import Path
import secrets
import subprocess
import sys

ROOT=Path(__file__).resolve().parent
spec=importlib.util.spec_from_file_location('render',ROOT/'render.py');render=importlib.util.module_from_spec(spec);spec.loader.exec_module(render)
spec=importlib.util.spec_from_file_location('sso',ROOT/'configure-sso.py');sso=importlib.util.module_from_spec(spec);spec.loader.exec_module(sso)

def main():
    if os.geteuid()!=0:raise RuntimeError('run as platform administrator with sudo')
    os.umask(0o077)
    os.environ.setdefault('KUBECONFIG','/etc/rancher/rke2/rke2.yaml')
    os.environ['PATH']='/var/lib/rancher/rke2/bin:'+os.environ['PATH']
    for ns in ['llm-api','llm-models']:sso.get(ns,'role','application-publisher')
    objects=render.render()
    def apply(value,dry=False):
        args=['apply','--server-side','--field-manager=vela-argocd']
        if dry:args.append('--dry-run=server')
        print(sso.k(*args,'-f','-',value=value).strip())
    apply(objects[0])
    for name in ['application-crd.yaml','applicationset-crd.yaml','appproject-crd.yaml']:
        print(sso.k('apply','--server-side','--field-manager=vela-argocd','-f',str(ROOT/'upstream'/name)).strip())
    exists=subprocess.run(['kubectl','-n','argocd','get','secret','argocd-redis','-o','name'],capture_output=True,text=True)
    if exists.returncode:
        if 'NotFound' not in exists.stderr:raise RuntimeError('Redis Secret lookup failed')
        print(sso.k('create','-f','-',value={'apiVersion':'v1','kind':'Secret','metadata':{'name':'argocd-redis','namespace':'argocd'},'stringData':{'auth':secrets.token_urlsafe(40)}}).strip())
    bundle={'apiVersion':'v1','kind':'List','items':objects[1:]}
    apply(bundle,dry=True);apply(bundle)
    subprocess.run([sys.executable,str(ROOT/'configure-sso.py'),'--run-directory','/opt/vela-cluster/platform-publishing-sso'],check=True)
    print(sso.k('apply','--server-side','--field-manager=vela-argocd','-f',str(ROOT/'monitoring.yaml')).strip())
    apply({'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'vela-platform-publishing-dashboard','namespace':'monitoring','labels':{'grafana_dashboard':'1','app.kubernetes.io/part-of':'vela'}},'data':{'platform-publishing.json':(ROOT/'dashboard.json').read_text()}})
    for kind,name in [('deployment','argocd-server'),('deployment','argocd-repo-server'),('deployment','argocd-redis'),('statefulset','argocd-application-controller')]:
        subprocess.run(['kubectl','-n','argocd','rollout','status',kind+'/'+name,'--timeout=180s'],check=True)
    print(json.dumps({'result':'ARGO_INSTALLED','next':'run live identity/sync verification before revoking any legacy publisher bindings'}))
if __name__=='__main__':main()
