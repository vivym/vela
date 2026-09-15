#!/usr/bin/env python3
"""Validate a temporary authenticated registry proxy on llmpool01:5445.

The live :5005 endpoint is not changed. Credentials stay in a root-only directory.
A tiny unique OCI manifest is uploaded/read/deleted through the candidate. The
proxy container is removed in finally; Distribution may retain its unreferenced
config blob until normal garbage collection. No existing image is removed.
"""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import secrets
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid

IMAGE='nginx@sha256:a8b39bd9cf0f83869a2162827a0caf6137ddf759d50a171451b335cecc87d236'
HOST='10.1.201.70'
USERS=['platform-publisher','node-pull','llm-api-publisher','llm-api-pull','llm-models-publisher','llm-models-pull']


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--template',required=True)
    parser.add_argument('--directory',required=True)
    args=parser.parse_args()
    if os.geteuid()!=0 or socket.gethostname()!='llmpool01':
        parser.error('run as root on the preflighted llmpool01')
    os.umask(0o077)
    d=Path(args.directory);d.mkdir(mode=0o700,parents=True,exist_ok=True)
    name='vela-registry-access-candidate'
    prior=subprocess.run(['docker','inspect',name],capture_output=True)
    if prior.returncode==0:raise RuntimeError('existing candidate: inspect it before another run')
    if (d/'candidate.json').exists():raise RuntimeError('existing receipt: use a fresh validation directory')
    credentials={user:secrets.token_urlsafe(32) for user in USERS}
    (d/'credentials.json').write_text(json.dumps(credentials,indent=2)+'\n')
    hashes={user:subprocess.check_output(['openssl','passwd','-6','-stdin'],input=password+'\n',text=True).strip()
            for user,password in credentials.items()}
    auth=d/'auth';auth.mkdir(mode=0o750);os.chown(auth,0,101);auth.chmod(0o750)
    groups={'handshake':USERS,'platform-read':['platform-publisher','node-pull'],
            'platform-write':['platform-publisher']}
    for tenant in ['llm-api','llm-models']:
        groups[tenant+'-read']=['platform-publisher','node-pull',tenant+'-publisher',tenant+'-pull']
        groups[tenant+'-write']=['platform-publisher',tenant+'-publisher']
    for filename,users in groups.items():
        p=auth/filename;p.write_text(''.join(user+':'+hashes[user]+'\n' for user in users));os.chown(p,0,101);p.chmod(0o640)
    rendered=Path(args.template).read_text().replace('__LISTEN__',HOST+':5445').replace('__UPSTREAM__',HOST+':5005').replace('__CERT_HOST__','registry.vela.local')
    (d/'nginx.conf').write_text(rendered)
    common=['--network=host','--read-only','--cpus=1','--memory=256m','--pids-limit=64',
            '--cap-drop=ALL','--cap-add=SETUID','--cap-add=SETGID','--cap-add=CHOWN',
            '--security-opt=no-new-privileges','--tmpfs=/tmp:rw,noexec,nosuid,size=64m',
            '-v',str(d/'nginx.conf')+':/etc/nginx/nginx.conf:ro',
            '-v',str(auth)+':/auth:ro','-v','/srv/vela-registry/certs:/certs:ro',
            '-v','/etc/rancher/rke2/registry-ca.crt:/trust/registry-ca.crt:ro','--entrypoint=nginx']
    checked=subprocess.run(['docker','run','--rm',*common,IMAGE,'-t'],capture_output=True,text=True)
    if checked.returncode:raise RuntimeError('nginx syntax validation failed: '+checked.stderr[-1500:])
    context=ssl.create_default_context(cafile='/etc/rancher/rke2/registry-ca.crt')
    opener=urllib.request.build_opener(urllib.request.HTTPSHandler(context=context),NoRedirect())
    base='https://'+HOST+':5445'
    receipt={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'image':IMAGE,
             'scope':'Candidate proxy only; production port 5005 remains unchanged',
             'config_sha256':hashlib.sha256(rendered.encode()).hexdigest(),'checks':[],'cleanup':[]}
    manifests=[];uploads=[]

    def req(user,method,path,body=None,ctype=None):
        url=path if path.startswith('https://') else base+path if path.startswith('/') else urllib.parse.urljoin(base+'/',path)
        if urllib.parse.urlsplit(url).netloc!=HOST+':5445' or not url.startswith(base+'/v2/'):
            raise RuntimeError('unexpected registry upload destination')
        headers={'Accept':'application/vnd.oci.image.index.v1+json, application/vnd.docker.distribution.manifest.list.v2+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json'}
        if user:headers['Authorization']='Basic '+base64.b64encode((user+':'+credentials[user]).encode()).decode()
        if ctype:headers['Content-Type']=ctype
        request=urllib.request.Request(url,method=method,data=body,headers=headers)
        try:
            with opener.open(request,timeout=12) as response:return response.status,dict(response.headers),response.read()
        except urllib.error.HTTPError as e:return e.code,dict(e.headers),e.read()

    def check(label,user,method,path,status,body=None,ctype=None):
        code,headers,content=req(user,method,path,body,ctype)
        receipt['checks'].append({'name':label,'identity':user or 'anonymous','method':method,'status':code,'expected':status,'passed':code==status})
        if code!=status:raise AssertionError(label+' status '+str(code))
        return headers,content

    try:
        subprocess.run(['docker','run','-d','--name',name,'--restart=no',*common,IMAGE,'-g','daemon off;'],check=True,capture_output=True)
        deadline=time.monotonic()+15
        while True:
            try:
                code,_,_=req(None,'GET','/v2/')
                if code==401:break
            except (urllib.error.URLError,ConnectionError):pass
            if time.monotonic()>deadline:raise RuntimeError('candidate did not become available')
            time.sleep(0.5)
        check('anonymous handshake denied',None,'GET','/v2/',401)
        check('anonymous write denied',None,'POST','/v2/llm-api/forbidden/blobs/uploads/',401,b'')
        for user in USERS:check('authenticated handshake',user,'GET','/v2/',200)
        for user in ['node-pull','llm-api-publisher','llm-api-pull','llm-models-publisher','llm-models-pull']:
            check('catalog restricted',user,'GET','/v2/_catalog',401)
            check('platform publication restricted',user,'POST','/v2/vela-control/blobs/uploads/',401,b'')
        control='/v2/vela-control/manifests/sha256:7d1acb338a3c8a31cea7393863070ae0e4ef375b414355dc7b03dcfebfcaee51'
        check('node can pull existing control manifest','node-pull','GET',control,200)
        for tenant in ['llm-api','llm-models']:
            user=tenant+'-publisher';reader=tenant+'-pull';other='llm-models' if tenant=='llm-api' else 'llm-api'
            check('tenant cannot read platform images',user,'GET',control,401)
            check('tenant cannot publish other tenant',user,'POST','/v2/'+other+'/forbidden/blobs/uploads/',401,b'')
            check('cross-repository blob mount cannot borrow platform data',user,'POST','/v2/'+tenant+'/forbidden/blobs/uploads/?mount=sha256:'+'0'*64+'&from=vela-control',401,b'')
            for path in ['/v2/'+tenant+'/../vela-control/tags/list','/v2/'+tenant+'/%2e%2e/vela-control/tags/list','/v2//'+tenant+'/tags/list']:
                check('ambiguous path denied',user,'GET',path,400)
            repo=tenant+'/access-validation-'+uuid.uuid4().hex[:12]
            check('pull identity cannot upload',reader,'POST','/v2/'+repo+'/blobs/uploads/',401,b'')
            headers,_=check('publisher begins blob upload',user,'POST','/v2/'+repo+'/blobs/uploads/',202,b'')
            location=headers['Location'];uploads.append(location)
            config=json.dumps({'architecture':'amd64','os':'linux','config':{},'rootfs':{'type':'layers','diff_ids':[]}},separators=(',',':')).encode()
            digest='sha256:'+hashlib.sha256(config).hexdigest()
            check('publisher commits blob',user,'PUT',location+('&' if '?' in location else '?')+'digest='+digest,201,config,'application/octet-stream')
            uploads.remove(location)
            manifest=json.dumps({'schemaVersion':2,'mediaType':'application/vnd.oci.image.manifest.v1+json','config':{'mediaType':'application/vnd.oci.image.config.v1+json','digest':digest,'size':len(config)},'layers':[]},separators=(',',':')).encode()
            manifest_digest='sha256:'+hashlib.sha256(manifest).hexdigest()
            check('publisher publishes exact manifest',user,'PUT','/v2/'+repo+'/manifests/check',201,manifest,'application/vnd.oci.image.manifest.v1+json')
            manifests.append((repo,manifest_digest))
            _,pulled=check('reader fetches exact manifest',reader,'GET','/v2/'+repo+'/manifests/'+manifest_digest,200)
            assert pulled==manifest
            _,pulled=check('reader fetches exact blob',reader,'GET','/v2/'+repo+'/blobs/'+digest,200)
            assert pulled==config
            check('tenant cannot delete published digest',user,'DELETE','/v2/'+repo+'/manifests/'+manifest_digest,401)
        receipt['result']='REGISTRY_ACCESS_CANDIDATE_PASS'
    finally:
        try:
            for repo,digest in manifests:
                try:
                    code,_,_=req('platform-publisher','DELETE','/v2/'+repo+'/manifests/'+digest)
                    after,_,_=req('platform-publisher','GET','/v2/'+repo+'/manifests/'+digest)
                    receipt['cleanup'].append({'repo':repo,'manifest_deleted':code==202,'subsequent_get':after})
                except Exception as error:
                    receipt['cleanup'].append({'repo':repo,'manifest_deleted':False,'error_type':type(error).__name__})
            for location in uploads:
                try:
                    code,_,_=req('platform-publisher','DELETE',location)
                    receipt['cleanup'].append({'cancel_upload_status':code})
                except Exception as error:
                    receipt['cleanup'].append({'cancel_upload_status':None,'error_type':type(error).__name__})
        finally:
            log=subprocess.run(['docker','logs',name],capture_output=True,text=True)
            (d/'candidate.log').write_text(log.stdout+log.stderr)
            stopped=subprocess.run(['docker','rm','-f',name],capture_output=True)
            receipt['candidate_removed']=stopped.returncode==0
            receipt['unreferenced_test_blob_may_remain']=bool(manifests)
            (d/'candidate.json').write_text(json.dumps(receipt,indent=2)+'\n')
    if not receipt['candidate_removed'] or any(not x.get('manifest_deleted',True) or x.get('subsequent_get',404)!=404 for x in receipt['cleanup']):
        raise RuntimeError('candidate cleanup incomplete')
    print(json.dumps({'result':receipt.get('result'),'checks':len(receipt['checks']),'receipt':str(d/'candidate.json')}))


if __name__=='__main__':main()
