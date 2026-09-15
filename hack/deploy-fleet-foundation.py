#!/usr/bin/env python3
"""Prepare a Fleet foundation rollout; explicit --apply and healthy storage required."""
import argparse,os,pathlib,json,subprocess,copy,datetime,base64,hashlib,time
os.environ['PATH']='/var/lib/rancher/rke2/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin';os.environ['KUBECONFIG']='/etc/rancher/rke2/rke2.yaml';os.umask(0o077)
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--input-directory',type=pathlib.Path,default=pathlib.Path('/opt/vela-cluster/fleet-deployment-20260915'))
parser.add_argument('--run-directory',type=pathlib.Path,required=True)
parser.add_argument('--apply',action='store_true')
args=parser.parse_args();inputs=args.input_directory.resolve();root=args.run_directory
assert root.is_absolute() and root.parent.resolve()==inputs and root.name.startswith('foundation-'), 'fresh foundation-* run directory required beneath inputs'
root.mkdir(mode=0o700,exist_ok=False);out=root/'foundation.json';r={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'checks':[],'stage':'preflight'}
def save():out.write_text(json.dumps(r,indent=2)+'\n')
def check(name,value):r['checks'].append({'name':name,'passed':bool(value)});save();assert value,name
def kub(*a,input=None,timeout=40):
 p=subprocess.run(['kubectl',*a],input=input,capture_output=True,text=True,timeout=timeout)
 if p.returncode:raise RuntimeError(p.stderr[:1000])
 return p.stdout
def k(*a):return json.loads(kub(*a,'-o','json'))
try:
 import yaml
 candidate=list(yaml.safe_load_all((inputs/'candidate.yaml').read_text()));check('12 reviewed Fleet objects',len(candidate)==12)
 before=k('-n','vela-system','get','deployment','vela-control');check('Control has two Ready replicas',before['status'].get('readyReplicas')==2)
 nodes=k('get','nodes')['items'];management=[n for n in nodes if n['metadata']['name'] in ['llmpool01','llmpool02','marslab-gpu-01']]
 check('All three management nodes Ready without resource pressure',len(management)==3 and all(any(c['type']=='Ready' and c['status']=='True' for c in n['status']['conditions']) and not any(c['type'] in ['DiskPressure','MemoryPressure','PIDPressure'] and c['status']=='True' for c in n['status']['conditions']) for n in management))
 check('APISIX etcd has three Ready members',k('-n','apisix','get','statefulset','apisix-etcd')['status'].get('readyReplicas')==3)
 minio=k('-n','object-store','get','pods','-l','app=minio-ha')['items'];members={name:0 for name in ['llmpool01','llmpool02','marslab-gpu-01']}
 for pod in minio:
  if not pod['metadata'].get('deletionTimestamp') and any(c['type']=='Ready' and c['status']=='True' for c in pod['status'].get('conditions',[])):
   name=pod['spec'].get('nodeName');members[name]=members.get(name,0)+1
 check('Two Ready MinIO members on every management host',members=={'llmpool01':2,'llmpool02':2,'marslab-gpu-01':2})
 volumes=k('-n','longhorn-system','get','volumes.longhorn.io')['items'];check('Active Longhorn volumes healthy with no detach in progress',all(v['status']['state'] in ['attached','detached'] and (v['status']['state']!='attached' or v['status']['robustness']=='healthy') for v in volumes))
 check('NATS has three Ready members',k('-n','vela-system','get','statefulset','nats')['status'].get('readyReplicas')==3)
 cluster=k('-n','vela-system','get','cluster','vela-postgres');check('PostgreSQL healthy',cluster['status']['readyInstances']==3)
 database_name=cluster['spec']['bootstrap']['initdb']['database'];check('Database name comes from active CNPG configuration',bool(database_name));r['database_name']=database_name
 primary=cluster['status']['currentPrimary'];count=kub('-n','vela-system','exec',primary,'--','psql','-XAt','-U','postgres','-d',database_name,'-c','SELECT count(*) FROM public.jobs;');check('No business Jobs before trust rollout',count.strip()=='0')
 check('No existing Fleet controller',not kub('-n','vela-system','get','deployment','vela-fleet-controller','--ignore-not-found','-o','name').strip())
 refs=json.loads((inputs/'materials-v2/apply.json').read_text());check('Six immutable materials verified',refs['passed'] and len(refs['resources'])==6)
 target=next(x for x in refs['resources'] if x['source']['name']=='vela-control-transport-tls-fleet-v1');old_name='vela-control-transport-tls-v-ebd9cc4d0cb4-r-e065c3d7fabb';old=k('-n','vela-system','get','secret',old_name);new=k('-n','vela-system','get','secret',target['name'])
 check('Only Fleet client trust bundle changed',set(old['data'])==set(new['data']) and [x for x in old['data'] if old['data'][x]!=new['data'][x]]==['fleet-client-ca.crt'])
 check('Original client trust retained',base64.b64decode(new['data']['fleet-client-ca.crt']).startswith(base64.b64decode(old['data']['fleet-client-ca.crt']).rstrip()+b'\n'))
 idx=next(i for i,v in enumerate(before['spec']['template']['spec']['volumes']) if v.get('secret',{}).get('secretName')==old_name);path='/spec/template/spec/volumes/'+str(idx)+'/secret/secretName';patch=[{'op':'test','path':path,'value':old_name},{'op':'replace','path':path,'value':target['name']}]
 dry=json.loads(kub('-n','vela-system','patch','deployment','vela-control','--type=json','-p',json.dumps(patch),'--dry-run=server','-o','json'));expected=copy.deepcopy(before['spec']);expected['template']['spec']['volumes'][idx]['secret']['secretName']=target['name'];check('Control dry-run only adds the reviewed trust snapshot',dry['spec']==expected)
 (root/'control-before.json').write_text(json.dumps(before,indent=2)+'\n');(root/'control-trust-patch.json').write_text(json.dumps(patch,indent=2)+'\n')
 result=kub('apply','--dry-run=server','-f',str(inputs/'candidate.yaml'));(root/'candidate-dry-run.log').write_text(result);check('Fleet complete candidate server dry-run passed',True)
 if not args.apply:
  r['stage']='preflight-ready';r['passed']=True;raise SystemExit(0)
 r['stage']='control-trust-rollout';save();kub('-n','vela-system','patch','deployment','vela-control','--type=json','-p',json.dumps(patch));kub('-n','vela-system','rollout','status','deployment/vela-control','--timeout=180s',timeout=190)
 control=k('-n','vela-system','get','deployment','vela-control');check('Control two replicas Ready with additive trust',control['status'].get('readyReplicas')==2 and control['spec']==expected)
 (root/'control-after.json').write_text(json.dumps(control,indent=2)+'\n')
 r['stage']='fleet-rollout';save();objects=[x for x in candidate if x['kind']!='ValidatingWebhookConfiguration'];kub('apply','-f','-',input=json.dumps({'apiVersion':'v1','kind':'List','items':objects}));kub('-n','vela-system','rollout','status','deployment/vela-fleet-controller','--timeout=180s',timeout=190)
 fleet=k('-n','vela-system','get','deployment','vela-fleet-controller');check('Fleet two replicas Ready',fleet['status'].get('readyReplicas')==2)
 pods=k('-n','vela-system','get','pods','-l','app.kubernetes.io/name=vela-fleet-controller')['items'];check('Fleet placed on both CPU management nodes',{p['spec']['nodeName'] for p in pods}=={'llmpool01','llmpool02'} and len(pods)==2)
 r['fleet_pods']=[{'name':p['metadata']['name'],'uid':p['metadata']['uid'],'node':p['spec']['nodeName'],'containers':[{'name':c['name'],'ready':c['ready'],'image_id':c['imageID'],'restarts':c['restartCount']} for c in p['status']['containerStatuses']],'init':[{'name':c['name'],'exit_code':c['state'].get('terminated',{}).get('exitCode')} for c in p['status'].get('initContainerStatuses',[])]} for p in pods];check('All Fleet TLS materializers succeeded',all(all(c['exit_code']==0 for c in p['init']) for p in r['fleet_pods']))
 check('NATS remains three Ready',k('-n','vela-system','get','statefulset','nats')['status'].get('readyReplicas')==3)
 check('Protected admission registration held for API server client configuration',not kub('get','validatingwebhookconfiguration','vela-fleet-protection','--ignore-not-found','-o','name').strip())
 r['stage']='foundation-ready';r['passed']=True
except Exception as e:r['error']=str(e);r['passed']=False;raise
finally:r['finished_at']=datetime.datetime.now(datetime.timezone.utc).isoformat();save();print(json.dumps({'stage':r['stage'],'passed':r.get('passed',False),'checks':len(r['checks']),'error':r.get('error')}),flush=True)
