#!/usr/bin/env python3
"""Stage the reviewed registry auth configuration on all three management hosts.

Run on llmpool01. Keeps credentials and Docker configs private, verifies the OCI
image archive before loading it, and prepares each host without changing its
existing registry endpoint. It does not cut over or restart a host service.
"""
import argparse
import base64
import hashlib
import json
import os
from pathlib import Path
import secrets
import shlex
import shutil
import subprocess
import uuid

HOSTS=['10.1.201.70','10.1.201.71','10.1.201.66']
IMAGE='nginx@sha256:a8b39bd9cf0f83869a2162827a0caf6137ddf759d50a171451b335cecc87d236'
CONTROL='sha256:7d1acb338a3c8a31cea7393863070ae0e4ef375b414355dc7b03dcfebfcaee51'
USERS=['platform-publisher','node-pull','llm-api-publisher','llm-api-pull','llm-models-publisher','llm-models-pull']


def run(args,**kw):
    p=subprocess.run(args,text=True,capture_output=True,**kw)
    if p.returncode:raise RuntimeError('command failed: '+args[0]+' exit '+str(p.returncode)+' '+p.stderr[-1800:])
    return p.stdout


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--installer',required=True)
    parser.add_argument('--template',required=True)
    parser.add_argument('--directory',default='/opt/vela-cluster/registry-access-20260914/live')
    args=parser.parse_args()
    if os.geteuid()!=0 or run(['hostname']).strip()!='llmpool01':parser.error('run as root on llmpool01')
    os.umask(0o077)
    root=Path(args.directory);root.mkdir(mode=0o700,parents=True,exist_ok=True)
    if root.stat().st_uid!=0 or root.stat().st_mode & 0o077:raise RuntimeError('state directory is not private')
    state_path=root/'prepare.json'
    if state_path.exists():raise RuntimeError('existing preparation: inspect host receipts before another run')
    creds_path=root/'credentials.json'
    if creds_path.exists():credentials=json.loads(creds_path.read_text())
    else:
        credentials={user:secrets.token_urlsafe(32) for user in USERS}
        creds_path.write_text(json.dumps(credentials,indent=2)+'\n')
    hashes={user:run(['openssl','passwd','-6','-stdin'],input=password+'\n').strip() for user,password in credentials.items()}
    groups={'handshake':USERS,'platform-read':['platform-publisher','node-pull'],'platform-write':['platform-publisher']}
    for tenant in ['llm-api','llm-models']:
        groups[tenant+'-read']=['platform-publisher','node-pull',tenant+'-publisher',tenant+'-pull']
        groups[tenant+'-write']=['platform-publisher',tenant+'-publisher']
    auth_files={key:''.join(user+':'+hashes[user]+'\n' for user in users) for key,users in groups.items()}
    image=json.loads(run(['docker','image','inspect',IMAGE]))[0]
    archive=root/'nginx-image.tar'
    if shutil.disk_usage(root).free < 2*1024**3:raise RuntimeError('not enough staging space')
    run(['docker','image','save','-o',str(archive),IMAGE],timeout=180)
    with archive.open('rb') as f:archive_sha=hashlib.file_digest(f,'sha256').hexdigest()
    inventory=json.loads(run(['ansible-inventory','-i','/home/user/fleet-ansible/inventory.ini','--list']))
    password=next(v['ansible_become_password'] for key,v in inventory['_meta']['hostvars'].items() if v.get('ansible_host',key)=='10.1.201.11')
    ssh_env=dict(os.environ,SSHPASS=password)
    options=['-o','StrictHostKeyChecking=yes','-o','UserKnownHostsFile=/home/user/.ssh/known_hosts','-o','ConnectTimeout=8']
    def ssh(host,code):
        remote='sudo -S -p "" python3 -c '+shlex.quote(code)
        return run(['sshpass','-e','ssh',*options,'user@'+host,remote],env=ssh_env,input=password+'\n',timeout=240)
    def scp(host,local,remote):
        return run(['sshpass','-e','scp',*options,str(local),'user@'+host+':'+remote],env=ssh_env,timeout=180)
    template=Path(args.template).read_text()
    state={'phase':'preparing','nginx_image_id':image['Id'],'nginx_oci_digest':IMAGE,'archive_sha256':archive_sha,
           'archive_size':archive.stat().st_size,'hosts':[]}
    state_path.write_text(json.dumps(state,indent=2)+'\n')
    for host in HOSTS:
        if host==HOSTS[0]:original=json.loads(run(['docker','inspect', 'vela-registry-release']))[0]
        else:original=json.loads(ssh(host,"import subprocess;print(subprocess.check_output(['docker','inspect','vela-registry-release'],text=True))"))[0]
        certificate=next(x['Source'] for x in original['Mounts'] if x['Destination']=='/certs')
        config=template.replace('__LISTEN__',host+':5005').replace('__UPSTREAM__','127.0.0.1:5007').replace('__CERT_HOST__','registry.vela.local')
        payload={'host':host,'original_id':original['Id'],'template':template,'auth_files':auth_files,
                 'node_pull_password':credentials['node-pull'],'certificate_directory':certificate,'control_digest':CONTROL,
                 'config_sha256':hashlib.sha256(config.encode()).hexdigest(),'nginx_image_id':image['Id'],'nginx_oci_digest':IMAGE}
        p=root/('input-'+host+'.json');p.write_text(json.dumps(payload,indent=2)+'\n')
        destination='/opt/vela-registry-access'
        if host==HOSTS[0]:
            out=Path(destination);out.mkdir(mode=0o700,exist_ok=True)
            if (out/'receipt.json').exists():raise RuntimeError('host already prepared')
            shutil.copyfile(p,out/'input.json');(out/'input.json').chmod(0o600)
            shutil.copyfile(args.installer,out/'install.py');(out/'install.py').chmod(0o700)
            result=run(['python3',str(out/'install.py'),'--directory',destination,'--phase','prepare'],timeout=120)
        else:
            temp='/tmp/vela-registry-access-'+uuid.uuid4().hex[:12]
            run(['sshpass','-e','ssh',*options,'user@'+host,'mkdir -m 700 '+shlex.quote(temp)],env=ssh_env,timeout=20)
            try:
                for local,name in [(p,'input.json'),(Path(args.installer),'install.py'),(archive,'nginx-image.tar')]:
                    scp(host,local,temp+'/'+name)
                code='''import pathlib,os,shutil,hashlib,subprocess
os.umask(0o077)
source=pathlib.Path(SOURCE);root=pathlib.Path(DEST)
root.mkdir(mode=0o700,exist_ok=True)
if (root/'receipt.json').exists():raise RuntimeError('host already prepared')
if shutil.disk_usage(root).free < 2*1024**3:raise RuntimeError('staging disk space insufficient')
for name in ['input.json','install.py','nginx-image.tar']:
 shutil.copyfile(source/name,root/name);(root/name).chmod(0o600)
hashed=hashlib.sha256()
with (root/'nginx-image.tar').open('rb') as f:
 for chunk in iter(lambda:f.read(1024*1024),b''):hashed.update(chunk)
assert hashed.hexdigest()==ARCHIVE_SHA
subprocess.run(['docker','load','-i',str(root/'nginx-image.tar')],check=True,capture_output=True)
result=subprocess.run(['python3',str(root/'install.py'),'--directory',str(root),'--phase','prepare'],text=True,capture_output=True)
(root/'prepare.log').write_text(result.stdout+result.stderr)
if result.returncode:raise RuntimeError('host preparation failed; inspect the root-owned prepare.log')
print(result.stdout)
'''.replace('SOURCE',repr(temp)).replace('DEST',repr(destination)).replace('ARCHIVE_SHA',repr(archive_sha))
                result=ssh(host,code)
            finally:
                # Only the unique temporary files created by this run are removed.
                cleanup='import pathlib; p=pathlib.Path('+repr(temp)+'); [(p/n).unlink(missing_ok=True) for n in ["input.json","install.py","nginx-image.tar"]];p.rmdir()'
                ssh(host,cleanup)
        host_result=json.loads(result.strip())
        state['hosts'].append(host_result);state_path.write_text(json.dumps(state,indent=2)+'\n')
        print(json.dumps(host_result),flush=True)
    clients=root/'docker-clients';clients.mkdir(mode=0o700,exist_ok=True)
    for user,password in credentials.items():
        directory=clients/user;directory.mkdir(mode=0o700,exist_ok=True)
        auth=base64.b64encode((user+':'+password).encode()).decode()
        (directory/'config.json').write_text(json.dumps({'auths':{host+':5005':{'auth':auth} for host in HOSTS}},indent=2)+'\n')
    state['phase']='prepared';state_path.write_text(json.dumps(state,indent=2)+'\n')
    print(json.dumps({'result':'REGISTRY_HOSTS_PREPARED','state':str(state_path)}))


if __name__=='__main__':main()
