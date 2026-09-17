import pathlib,subprocess,json,datetime
root=pathlib.Path('/opt/vela-cluster/native-contract-20260917');K=['/var/lib/rancher/rke2/bin/kubectl','--kubeconfig','/etc/rancher/rke2/rke2.yaml','-n','vela-system'];image=json.loads((root/'image.json').read_text());before=json.loads((root/'deployment-before.json').read_text());d=json.loads(subprocess.check_output(K+['get','deploy','vela-control','-o','json']));assert d['spec']==before['spec'],'Deployment spec changed since preflight'
i=next(i for i,c in enumerate(d['spec']['template']['spec']['containers']) if c['name']=='vela-control');assert d['spec']['template']['spec']['containers'][i]['image']==image['base_image']
patch=[{'op':'test','path':'/metadata/resourceVersion','value':d['metadata']['resourceVersion']},{'op':'test','path':f'/spec/template/spec/containers/{i}/image','value':image['base_image']},{'op':'replace','path':f'/spec/template/spec/containers/{i}/image','value':image['image']}]
subprocess.run(K+['patch','deploy','vela-control','--type=json','-p',json.dumps(patch)],check=True)
subprocess.run(K+['rollout','status','deploy/vela-control','--timeout=180s'],check=True)
after=json.loads(subprocess.check_output(K+['get','deploy','vela-control','-o','json']));assert after['status']['availableReplicas']==after['spec']['replicas']==2 and after['status']['updatedReplicas']==2
pods=json.loads(subprocess.check_output(K+['get','pod','-l','app.kubernetes.io/name=vela-control','-o','json']))['items']
# Select via Deployment labels when the chart labels differ.
if not pods:
 selector=','.join(k+'='+v for k,v in after['spec']['selector']['matchLabels'].items());pods=json.loads(subprocess.check_output(K+['get','pod','-l',selector,'-o','json']))['items']
ready=[]
for p in pods:
 if p['metadata'].get('deletionTimestamp'):continue
 for c in p['status'].get('containerStatuses',[]):
  if c['name']=='vela-control':
   assert c['ready'] and c['imageID'].endswith(image['image'].split('@')[1]);ready.append({'pod':p['metadata']['name'],'image_id':c['imageID'],'ready':c['ready'],'restart_count':c['restartCount']})
assert len(ready)==2
out={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'passed':True,'image':image,'schema_migration':False,'replicas':2,'pods':ready,'generation':after['metadata']['generation']};(root/'deployment-receipt.json').write_text(json.dumps(out,indent=2));print(json.dumps(out))
