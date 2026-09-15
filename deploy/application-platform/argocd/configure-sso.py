#!/usr/bin/env python3
"""Configure the existing Keycloak/APISIX for Argo SSO from a management node.

Run as a platform administrator. Secrets stay in memory/Kubernetes. This creates
only the dedicated vela-platform realm and named platform routes; the existing
platform/customer realms and Keycloak hostname remain untouched.
"""
import argparse
import base64
import datetime
import json
import os
from pathlib import Path
import ssl
import subprocess
import urllib.error
import urllib.parse
import urllib.request
import yaml

REALM='vela-platform'
ORIGIN='https://10.1.201.70:30443'

def k(*args, value=None):
    p=subprocess.run(['/var/lib/rancher/rke2/bin/kubectl','--kubeconfig',os.environ.get('KUBECONFIG','/etc/rancher/rke2/rke2.yaml'),*args],input=json.dumps(value) if value is not None else None,text=True,capture_output=True,timeout=45)
    if p.returncode:raise RuntimeError(p.stderr[:2000])
    return p.stdout

def get(ns,kind,name):return json.loads(k('-n',ns,'get',kind,name,'-o','json'))

def http(url,method='GET',value=None,headers=None,context=None,form=None):
    hdr=dict(headers or {})
    if form is not None:data=urllib.parse.urlencode(form).encode();hdr['Content-Type']='application/x-www-form-urlencoded'
    elif value is not None:data=json.dumps(value).encode();hdr['Content-Type']='application/json'
    else:data=None
    with urllib.request.urlopen(urllib.request.Request(url,method=method,data=data,headers=hdr),context=context,timeout=25) as r:
        body=r.read()
        return json.loads(body) if body else None

def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--run-directory',required=True);args=p.parse_args()
    if os.geteuid()!=0:raise RuntimeError('run as platform operator with sudo')
    os.umask(0o077);root=Path(args.run_directory);root.mkdir(mode=0o700,parents=True,exist_ok=True)
    material=get('identity','secret','keycloak-admin')['data']
    cred={key:base64.b64decode(value).decode() for key,value in material.items()}
    base='http://'+get('identity','svc','keycloak')['spec']['clusterIP']+':8080'
    token=http(base+'/realms/master/protocol/openid-connect/token',method='POST',form=dict(client_id='admin-cli',grant_type='password',username=cred['username'],password=cred['password']))['access_token']
    auth={'Authorization':'Bearer '+token}
    realms=http(base+'/admin/realms',headers=auth)
    existing=next((r for r in realms if r['realm']==REALM),None)
    if existing:
        assert existing.get('attributes',{}).get('vela.managed-by')=='platform-publishing','unowned realm exists'
    else:
        http(base+'/admin/realms','POST',{'realm':REALM,'enabled':True,'displayName':'Vela Platform','registrationAllowed':False,'resetPasswordAllowed':False,'rememberMe':False,'sslRequired':'external','bruteForceProtected':True,'failureFactor':5,'accessTokenLifespan':300,'ssoSessionIdleTimeout':1800,'ssoSessionMaxLifespan':28800,'passwordPolicy':'length(14) and digits(1) and lowerCase(1) and upperCase(1)','attributes':{'frontendUrl':ORIGIN+'/identity','vela.managed-by':'platform-publishing'}},headers=auth)
    endpoint=base+'/admin/realms/'+REALM
    http(endpoint+'/events/config','PUT',{'eventsEnabled':True,'eventsExpiration':2592000,'adminEventsEnabled':True,'adminEventsDetailsEnabled':False,'enabledEventTypes':['LOGIN','LOGIN_ERROR','LOGOUT','CODE_TO_TOKEN','CODE_TO_TOKEN_ERROR','UPDATE_PASSWORD','UPDATE_TOTP','REMOVE_TOTP']},headers=auth)
    groups=http(endpoint+'/groups',headers=auth)
    names=['platform-admins']+[f'{team}-{role}s' for team in ['llm-api','llm-models'] for role in ['publisher','approver','viewer']]
    for name in names:
        if not any(g['name']==name for g in groups):http(endpoint+'/groups','POST',{'name':name},headers=auth)
    actions=http(endpoint+'/authentication/required-actions',headers=auth)
    for action in actions:
        if action['alias']=='CONFIGURE_TOTP':
            action.update(enabled=True,defaultAction=True)
            http(endpoint+'/authentication/required-actions/CONFIGURE_TOTP','PUT',action,headers=auth)
    clients=http(endpoint+'/clients?clientId=argocd',headers=auth)
    if clients:
        client=http(endpoint+'/clients/'+clients[0]['id'],headers=auth)
        assert client.get('attributes',{}).get('vela.managed-by')=='platform-publishing','unowned client exists'
    else:
        http(endpoint+'/clients','POST',{'clientId':'argocd','name':'Argo CD','enabled':True,'protocol':'openid-connect','publicClient':False,'standardFlowEnabled':True,'implicitFlowEnabled':False,'directAccessGrantsEnabled':False,'serviceAccountsEnabled':False,'redirectUris':[ORIGIN+'/argocd/auth/callback'],'webOrigins':[ORIGIN],'attributes':{'pkce.code.challenge.method':'S256','vela.managed-by':'platform-publishing'},'protocolMappers':[{'name':'groups','protocol':'openid-connect','protocolMapper':'oidc-group-membership-mapper','config':{'full.path':'false','id.token.claim':'true','access.token.claim':'true','userinfo.token.claim':'true','claim.name':'groups'}}]},headers=auth)
        client=http(endpoint+'/clients?clientId=argocd',headers=auth)[0]
    secret=http(endpoint+'/clients/'+client['id']+'/client-secret',headers=auth)['value']
    # Native Argo OIDC configuration references the confidential Secret key.
    k('apply','--server-side','--field-manager=vela-platform-sso','-f','-', value={'apiVersion':'v1','kind':'Secret','metadata':{'name':'argocd-secret','namespace':'argocd'},'stringData':{'oidc.keycloak.clientSecret':secret}})
    ca=base64.b64decode(get('apisix','secret','vela-gateway-tls')['data']['ca.crt']).decode()
    k('apply','--server-side','--field-manager=vela-platform-sso','-f','-', value={'apiVersion':'v1','kind':'ConfigMap','metadata':{'name':'vela-gateway-ca','namespace':'argocd'},'data':{'ca.crt':ca}})
    config={'name':'Keycloak','issuer':ORIGIN+'/identity/realms/'+REALM,'clientID':'argocd','clientSecret':'$oidc.keycloak.clientSecret','requestedScopes':['openid','profile','email','groups'],'rootCA':ca,'enablePKCEAuthentication':True}
    # Group membership mapper is on the client, so no separately named optional groups scope is needed.
    config['requestedScopes']=['openid','profile','email']
    k('-n','argocd','patch','cm','argocd-cm','--type=merge','-p',json.dumps({'data':{'oidc.config':yaml.safe_dump(config,sort_keys=False)}}))
    gateway='http://'+get('apisix','svc','apisix-admin')['spec']['clusterIP']+':9180/apisix/admin'
    key=base64.b64decode(get('apisix','secret','apisix-admin-credentials')['data']['admin']).decode();headers={'X-API-KEY':key}
    route_defs={
      'vela-platform-argocd':{'name':'vela-platform-argocd','uris':['/argocd','/argocd/*'],'priority':50,'plugins':{'prometheus':{},'limit-count':{'count':600,'time_window':60,'rejected_code':429,'key_type':'var','key':'remote_addr','policy':'local'}},'upstream':{'type':'roundrobin','scheme':'http','nodes':{'argocd-server.argocd.svc.cluster.local:80':1},'timeout':{'connect':5,'send':60,'read':120}}},
      'vela-platform-identity':{'name':'vela-platform-identity','uris':['/identity/realms/'+REALM+'/*','/identity/resources/*'],'priority':50,'plugins':{'prometheus':{},'proxy-rewrite':{'regex_uri':['^/identity/(.*)','/$1']},'limit-count':{'count':120,'time_window':60,'rejected_code':429,'key_type':'var','key':'remote_addr','policy':'local'}},'upstream':{'type':'roundrobin','scheme':'http','nodes':{'keycloak.identity.svc.cluster.local:8080':1},'timeout':{'connect':5,'send':30,'read':30}}}}
    for name,route in route_defs.items():
        try:
            old=http(gateway+'/routes/'+name,headers=headers)['value']
            assert old.get('name')==name,'unowned gateway route exists'
            backup=root/(name+'-before.json')
            if not backup.exists():backup.write_text(json.dumps(old,indent=2)+'\n')
        except urllib.error.HTTPError as e:
            if e.code!=404:raise
        http(gateway+'/routes/'+name,'PUT',route,headers=headers)
    context=ssl.create_default_context(cadata=ca)
    discovery=http(ORIGIN+'/identity/realms/'+REALM+'/.well-known/openid-configuration',context=context)
    assert discovery['issuer']==config['issuer'],discovery['issuer']
    assert discovery['authorization_endpoint'].startswith(config['issuer']+'/'),discovery['authorization_endpoint']
    assert all(r['realm'] in [x['realm'] for x in http(base+'/admin/realms',headers=auth)] for r in realms)
    receipt={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'realm':REALM,'issuer':discovery['issuer'],'groups':names,'client':'argocd','pkce':'S256','direct_access_grants':False,'totp_default_action':True,'existing_realms_preserved':[r['realm'] for r in realms], 'routes':list(route_defs),'ui':ORIGIN+'/argocd/','result':'PLATFORM_SSO_CONFIGURED'}
    (root/'sso-config.json').write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))

if __name__=='__main__':main()
