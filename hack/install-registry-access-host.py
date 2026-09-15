#!/usr/bin/env python3
"""Prepare, cut over or roll back the release-registry auth proxy on one host.

Requires a root-owned private input.json in the state directory. All original
container configuration is backed up privately. Only the owned registry
containers change; no host reboot, Docker daemon or RKE2 restart is performed.
"""
import argparse
import base64
import copy
import hashlib
import http.client
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import time
import urllib.parse

ACCEPT=', '.join(['application/vnd.oci.image.index.v1+json','application/vnd.docker.distribution.manifest.list.v2+json',
                 'application/vnd.oci.image.manifest.v1+json','application/vnd.docker.distribution.manifest.v2+json'])
BACKEND='vela-registry-release'
GATEWAY='vela-registry-access'


class UnixConnection(http.client.HTTPConnection):
    def connect(self):
        self.sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout);self.sock.connect('/var/run/docker.sock')


class Docker:
    def __init__(self):
        self.version=''
        self.version='/v'+self.call('GET','/version')['ApiVersion']

    def call(self,method,path,body=None,allow=()):
        conn=UnixConnection('localhost',timeout=45)
        try:
            conn.request(method,self.version+path,body=json.dumps(body) if body is not None else None,
                         headers={'Content-Type':'application/json'})
            response=conn.getresponse();data=response.read()
            if response.status>=300 and response.status not in allow:
                raise RuntimeError('Docker API failed: '+method+' '+path.split('?')[0]+' status '+str(response.status))
            return json.loads(data) if data else None
        finally:conn.close()

    def inspect(self,name):return self.call('GET','/containers/'+name+'/json')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--directory',default='/opt/vela-registry-access')
    parser.add_argument('--phase',required=True,choices=['prepare','cutover','verify','rollback'])
    args=parser.parse_args()
    if os.geteuid()!=0:parser.error('run as root')
    os.umask(0o077)
    root=Path(args.directory)
    if root.stat().st_uid!=0 or root.stat().st_mode & 0o077:
        raise RuntimeError('private root-owned state directory required')
    path=root/'input.json'
    if path.stat().st_uid!=0 or path.stat().st_mode & 0o077:raise RuntimeError('private root-owned input required')
    input_hash=hashlib.sha256(path.read_bytes()).hexdigest()
    data=json.loads(path.read_text())
    host=data['host']
    if host not in ['10.1.201.70','10.1.201.71','10.1.201.66']:raise RuntimeError('unapproved host')
    addresses=json.loads(subprocess.check_output(['ip','-j','address','show'],text=True))
    if not any(a.get('local')==host for link in addresses for a in link.get('addr_info',[])):
        raise RuntimeError('input host is not a local address')
    docker=Docker()
    receipt_path=root/'receipt.json'
    receipt=json.loads(receipt_path.read_text()) if receipt_path.exists() else {'host':host}
    def save(**updates):
        receipt.update(updates,updated_at=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()))
        tmp=root/'receipt.tmp';tmp.write_text(json.dumps(receipt,indent=2)+'\n');tmp.replace(receipt_path)
    def boot():return Path('/proc/sys/kernel/random/boot_id').read_text().strip()
    def invocation():return subprocess.check_output(['systemctl','show','rke2-server','-p','InvocationID','--value'],text=True).strip()
    context=ssl.create_default_context(cafile='/etc/rancher/rke2/registry-ca.crt')
    credential='Basic '+base64.b64encode(('node-pull:'+data['node_pull_password']).encode()).decode()
    def request(port=5005,authenticated=False,method='GET',path='/v2/',loopback=False):
        conn=http.client.HTTPSConnection(host,port,context=context,timeout=8)
        if loopback:
            conn._create_connection=lambda address,timeout,source_address=None: socket.create_connection(('127.0.0.1',port),timeout,source_address)
        headers={'Accept':ACCEPT}
        if authenticated:headers['Authorization']=credential
        try:
            conn.request(method,path,headers=headers)
            response=conn.getresponse();body=response.read()
            return response.status,dict(response.headers),body
        finally:conn.close()
    def verify():
        assert request()[0]==401,'anonymous access not denied'
        assert request(authenticated=True)[0]==200,'authenticated handshake failed'
        code,headers,_=request(authenticated=True,method='HEAD',path='/v2/vela-control/manifests/'+data['control_digest'])
        assert code==200 and headers.get('Docker-Content-Digest')==data['control_digest'],'control digest unavailable'
        assert request(authenticated=True,method='POST',path='/v2/vela-control/blobs/uploads/')[0]==401,'pull identity can write'
        assert request(port=5007,loopback=True)[0]==200,'private backend unavailable'
        backend=docker.inspect(BACKEND);gateway=docker.inspect(GATEWAY)
        assert backend['State']['Running'] and gateway['State']['Running']
        assert backend['HostConfig']['NetworkMode']=='host'
        assert 'REGISTRY_HTTP_ADDR=127.0.0.1:5007' in backend['Config']['Env']
        assert gateway['HostConfig']['RestartPolicy']['Name']=='always'
        assert backend['HostConfig']['RestartPolicy']['Name']=='always'
        assert boot()==receipt['boot_id'] and invocation()==receipt['rke2_invocation'],'host or RKE2 changed'
        return {'anonymous':401,'authenticated':200,'control_digest_head':200,'pull_write_denied':401,
                'backend_loopback':200,'backend_id':backend['Id'],'gateway_id':gateway['Id'],'restart_policy':'always',
                'boot_id_unchanged':True,'rke2_invocation_unchanged':True}
    def gateway_config():
        cert=data['certificate_directory']
        return {'Image':data['nginx_image_id'],'Entrypoint':['nginx'],'Cmd':['-g','daemon off;'],
            'Labels':{'vela.ai/managed-by':'registry-access','vela.ai/config-sha256':data['config_sha256']},
            'HostConfig':{'NetworkMode':'host','RestartPolicy':{'Name':'always'},'ReadonlyRootfs':True,
                'Memory':268435456,'NanoCpus':1000000000,'PidsLimit':64,
                'CapDrop':['ALL'],'CapAdd':['SETUID','SETGID','CHOWN'],'SecurityOpt':['no-new-privileges'],
                'Tmpfs':{'/tmp':'rw,noexec,nosuid,size=64m'},
                'LogConfig':{'Type':'json-file','Config':{'max-size':'10m','max-file':'3'}},
                'Binds':[str(root/'nginx.conf')+':/etc/nginx/nginx.conf:ro',str(root/'auth')+':/auth:ro',
                         cert+':/certs:ro','/etc/rancher/rke2/registry-ca.crt:/trust/registry-ca.crt:ro']}}
    def rollback():
        # Ownership is pinned to the saved IDs; never remove a container by an
        # unverified name if another operator has replaced it.
        for field in ['gateway_id','new_backend_id']:
            if receipt.get(field):docker.call('DELETE','/containers/'+receipt[field]+'?force=1',allow=(404,))
        original=json.loads((root/'original-container.json').read_text())
        current=docker.inspect(original['Id'])
        if current['Name']!='/'+BACKEND:
            docker.call('POST','/containers/'+original['Id']+'/rename?name='+BACKEND)
        docker.call('POST','/containers/'+original['Id']+'/update',{'RestartPolicy':original['HostConfig']['RestartPolicy']})
        if not docker.inspect(original['Id'])['State']['Running']:
            docker.call('POST','/containers/'+original['Id']+'/start')
        save(phase='rolled-back',rollback_verified=request()[0]==200)
    if args.phase in ['cutover','verify'] and receipt.get('input_sha256')!=input_hash:
        raise RuntimeError('input changed after preparation')
    if args.phase=='prepare':
        if receipt.get('phase'):
            raise RuntimeError('existing state: inspect/verify it rather than preparing again')
        with socket.socket() as probe:
            probe.bind(('127.0.0.1',5007))
        subprocess.run(['systemctl','is-active','--quiet','rke2-server'],check=True)
        current=docker.inspect(BACKEND)
        if current['Id']!=data['original_id'] or not current['State']['Running']:raise RuntimeError('registry changed since preflight')
        if request()[0]!=200:raise RuntimeError('existing registry is not in the preflighted anonymous mode')
        image=docker.call('GET','/images/'+data['nginx_image_id']+'/json')
        if image['Id']!=data['nginx_image_id']:raise RuntimeError('nginx image differs')
        (root/'original-container.json').write_text(json.dumps(current,indent=2)+'\n')
        config=data['template'].replace('__LISTEN__',host+':5005').replace('__UPSTREAM__','127.0.0.1:5007').replace('__CERT_HOST__','registry.vela.local')
        if hashlib.sha256(config.encode()).hexdigest()!=data['config_sha256']:raise RuntimeError('config hash differs')
        (root/'nginx.conf').write_text(config)
        auth=root/'auth';auth.mkdir(mode=0o750,exist_ok=True);os.chown(auth,0,101);auth.chmod(0o750)
        allowed={'handshake','platform-read','platform-write','llm-api-read','llm-api-write','llm-models-read','llm-models-write'}
        if set(data['auth_files'])!=allowed:raise RuntimeError('unexpected auth files')
        for name,value in data['auth_files'].items():
            path=auth/name;path.write_text(value);os.chown(path,0,101);path.chmod(0o640)
        # Validate the exact production config in a disposable container. It
        # performs no port bind while nginx is running in configuration-test mode.
        configuration=gateway_config();configuration['Cmd']=['-t'];configuration['HostConfig']['RestartPolicy']={'Name':'no'}
        test=docker.call('POST','/containers/create',configuration)['Id']
        try:
            docker.call('POST','/containers/'+test+'/start')
            status=docker.call('POST','/containers/'+test+'/wait')['StatusCode']
            if status!=0:raise RuntimeError('nginx production config test failed')
        finally:docker.call('DELETE','/containers/'+test+'?force=1',allow=(404,))
        save(phase='prepared',input_sha256=input_hash,original_id=current['Id'],boot_id=boot(),rke2_invocation=invocation(),
             config_sha256=data['config_sha256'],nginx_image_id=data['nginx_image_id'],source_oci_digest=data['nginx_oci_digest'])
    elif args.phase=='cutover':
        if receipt.get('phase')!='prepared':raise RuntimeError('cutover requires a prepared host')
        current=docker.inspect(BACKEND)
        if current['Id']!=receipt['original_id'] or not current['State']['Running']:raise RuntimeError('registry changed after preparation')
        if boot()!=receipt['boot_id'] or invocation()!=receipt['rke2_invocation']:raise RuntimeError('host state changed')
        backup=BACKEND+'-before-access-'+current['Id'][:12]
        config=copy.deepcopy(current['Config'])
        config['Hostname']='';config['Domainname']='';config['Image']=current['Image']
        config['Env']=[x for x in config['Env'] if not x.startswith('REGISTRY_HTTP_ADDR=')]+['REGISTRY_HTTP_ADDR=127.0.0.1:5007']
        config['Labels']=dict(config.get('Labels') or {},**{'vela.ai/managed-by':'registry-access-backend'})
        config['HostConfig']=copy.deepcopy(current['HostConfig'])
        config['HostConfig'].update(NetworkMode='host',PortBindings={},PublishAllPorts=False,RestartPolicy={'Name':'always'})
        save(phase='cutting-over',backup_name=backup)
        try:
            docker.call('POST','/containers/'+current['Id']+'/update',{'RestartPolicy':{'Name':'no'}})
            docker.call('POST','/containers/'+current['Id']+'/stop?t=15')
            docker.call('POST','/containers/'+current['Id']+'/rename?name='+backup)
            new=docker.call('POST','/containers/create?name='+BACKEND,config)['Id'];save(new_backend_id=new)
            docker.call('POST','/containers/'+new+'/start')
            gate=docker.call('POST','/containers/create?name='+GATEWAY,gateway_config())['Id'];save(gateway_id=gate)
            docker.call('POST','/containers/'+gate+'/start')
            deadline=time.monotonic()+25
            while True:
                try:
                    checks=verify();break
                except (OSError,AssertionError):
                    if time.monotonic()>deadline:raise
                    time.sleep(1)
            save(phase='active',checks=checks)
        except Exception:
            rollback()
            raise
    elif args.phase=='rollback':
        rollback()
    else:
        if receipt.get('phase')!='active':raise RuntimeError('verify requires an active deployment')
        save(checks=verify())
    print(json.dumps({'host':host,'phase':receipt['phase'],'receipt':str(receipt_path)}))


if __name__=='__main__':main()
