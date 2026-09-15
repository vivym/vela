#!/usr/bin/env python3
"""Create a personal platform identity with a one-time password and required MFA.

Run on a management node as root. The output directory must be root-owned and
private. This utility never overwrites an existing user or prints credentials.
"""
import argparse
import base64
import importlib.util
import json
import os
from pathlib import Path
import re
import secrets
import stat
import urllib.parse

spec=importlib.util.spec_from_file_location('platform_sso',Path(__file__).with_name('configure-sso.py'))
sso=importlib.util.module_from_spec(spec);spec.loader.exec_module(sso)
GROUPS=['platform-admins']+[f'{team}-{role}s' for team in ['llm-api','llm-models'] for role in ['publisher','approver','viewer']]

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--username',required=True);p.add_argument('--group',required=True,choices=GROUPS)
    p.add_argument('--output',required=True);args=p.parse_args()
    if os.geteuid()!=0:p.error('run as platform operator through sudo')
    if not re.fullmatch(r'[a-z][a-z0-9._-]{2,62}',args.username):p.error('username must be a personal lowercase identifier')
    os.umask(0o077);out=Path(args.output).absolute();parent=out.parent.stat()
    if parent.st_uid!=0 or stat.S_IMODE(parent.st_mode)&0o077:p.error('output directory must be owned by root with mode 0700')
    fd=os.open(out,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    user_id=None
    try:
        cred={k:base64.b64decode(v).decode() for k,v in sso.get('identity','secret','keycloak-admin')['data'].items()}
        base='http://'+sso.get('identity','svc','keycloak')['spec']['clusterIP']+':8080'
        token=sso.http(base+'/realms/master/protocol/openid-connect/token','POST',form=dict(client_id='admin-cli',grant_type='password',**cred))['access_token']
        headers={'Authorization':'Bearer '+token};ep=base+'/admin/realms/'+sso.REALM
        path=ep+'/users?exact=true&username='+urllib.parse.quote(args.username)
        if sso.http(path,headers=headers):raise RuntimeError('user already exists; explicit group review is required')
        group=next(g for g in sso.http(ep+'/groups',headers=headers) if g['name']==args.group)
        password=secrets.token_urlsafe(30)+'aA1'
        sso.http(ep+'/users','POST',{'username':args.username,'enabled':True,'requiredActions':['UPDATE_PASSWORD','CONFIGURE_TOTP'],'credentials':[{'type':'password','value':password,'temporary':True}]},headers=headers)
        user_id=sso.http(path,headers=headers)[0]['id']
        sso.http(ep+'/users/'+user_id+'/groups/'+group['id'],'PUT',headers=headers)
        with os.fdopen(fd,'w') as f:
            fd=None;json.dump({'username':args.username,'one_time_password':password,'group':args.group,'login':sso.ORIGIN+'/argocd/','required_actions':['UPDATE_PASSWORD','CONFIGURE_TOTP']},f,indent=2);f.write('\n');f.flush();os.fsync(f.fileno())
        print(json.dumps({'username':args.username,'group':args.group,'credential_file':str(out),'mode':'0600','mfa_required':True}))
    except Exception:
        if user_id:sso.http(ep+'/users/'+user_id,'DELETE',headers=headers)
        if fd is not None:os.close(fd)
        out.unlink(missing_ok=True)
        raise
if __name__=='__main__':main()
