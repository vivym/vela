import pathlib,tarfile,subprocess,json,hashlib,bz2,os
os.umask(0o077);root=pathlib.Path('/opt/vela-cluster/native-contract-20260917');delta=root/'delta';delta.mkdir(exist_ok=True)
with tarfile.open('/tmp/native-contract-delta.tgz') as t:t.extractall(delta,filter='data')
old=pathlib.Path('/opt/vela-cluster/h3-admission-retry-20260917/vela-control').read_bytes();meta=json.loads((delta/'meta.json').read_text());assert hashlib.sha256(old).hexdigest()==meta['before'];p=(delta/'control.patch').read_bytes();assert p[:8]==b'BSDIFF40'
def signed(b):
 x=int.from_bytes(b,'little');return -(x&((1<<63)-1)) if x>>63 else x
cl,dl,size=[signed(p[i:i+8]) for i in [8,16,24]];ctrl=bz2.decompress(p[32:32+cl]);diff=bz2.decompress(p[32+cl:32+cl+dl]);extra=bz2.decompress(p[32+cl+dl:]);new=bytearray();op=dp=ep=cp=0
while len(new)<size:
 x,y,z=[signed(ctrl[cp+i:cp+i+8]) for i in [0,8,16]];cp+=24;assert x>=0 and y>=0
 new.extend((diff[dp+i]+(old[op+i] if 0<=op+i<len(old) else 0))%256 for i in range(x));dp+=x;op+=x;new.extend(extra[ep:ep+y]);ep+=y;op+=z
assert len(new)==size and hashlib.sha256(new).hexdigest()==meta['after'];(root/'vela-control').write_bytes(new)
base='10.1.201.70:5005/vela-control@sha256:d3be381e273ec2b6dceaec4d5529c3fb6b050ce7a081f7b599d9e0d303c0ba33';tag='10.1.201.70:5005/vela-control:native-contract-0918-'+meta['after'][:12]
(root/'Dockerfile').write_text('FROM '+base+'\nCOPY --chmod=0555 vela-control /usr/local/bin/vela-control\n')
(root/'.dockerignore').write_text('*\n!Dockerfile\n!vela-control\n')
subprocess.run(['docker','build','--network','none','-t',tag,str(root)],check=True);subprocess.run(['docker','push',tag],check=True)
x=json.loads(subprocess.check_output(['docker','inspect',tag]))[0];image=x['RepoDigests'][0];manifest=json.loads(subprocess.check_output(['docker','manifest','inspect','--insecure',image]));index=image
if 'manifests' in manifest:
 matches=[m for m in manifest['manifests'] if m.get('platform',{}).get('architecture')=='amd64' and m.get('platform',{}).get('os')=='linux'];assert len(matches)==1;image=image.split('@')[0]+'@'+matches[0]['digest']
out={'image':image,'index_image':index,'base_image':base,'binary_sha256':meta['after']};(root/'image.json').write_text(json.dumps(out,indent=2));print(json.dumps(out))
