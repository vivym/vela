import pathlib,subprocess,json,datetime,importlib.util,socket,copy,time,uuid,os
os.umask(0o077);os.environ['KUBECONFIG']='/etc/rancher/rke2/rke2.yaml'
root=pathlib.Path('/opt/vela-cluster/native-contract-20260917');K=['/var/lib/rancher/rke2/bin/kubectl','--kubeconfig','/etc/rancher/rke2/rke2.yaml','-n','vela-system'];primary=json.loads(subprocess.check_output(K+['get','cluster','vela-postgres','-o','json']))['status']['currentPrimary']
spec=importlib.util.spec_from_file_location('flow','/tmp/verify-h3-api-flow.py');f=importlib.util.module_from_spec(spec);spec.loader.exec_module(f)
orig=socket.getaddrinfo;target=['10.1.201.70'];socket.getaddrinfo=lambda h,p,*a,**kw:orig(target[0] if h=='vela.marslab.ic' else h,p,*a,**kw)
base=pathlib.Path('/opt/vela-cluster/h3-api-acceptance-20260917-minimax-v8');body=json.loads((base/'request.json').read_text());project='eb8f032a-1228-5dc6-8ea6-bd0c6d75ddb7';path='/v1/projects/'+project+'/jobs';client=f.Client('https://vela.marslab.ic/api',(base/'bearer-token').read_text().strip(),'/opt/vela-cluster/relay-station-20260916/gateway-ca.crt')
def q(sql):return json.loads(subprocess.check_output(K+['exec','-i',primary,'-c','postgres','--','psql','-XAt','-v','ON_ERROR_STOP=1','-U','postgres','-d','app'],input=sql,text=True))
def invariant():return q("SELECT json_build_object('jobs',(select count(*) from jobs where project_id='"+project+"'),'keys',(select count(*) from idempotency_results where project_id='"+project+"'),'reservations',(select count(*) from credit_reservations where project_id='"+project+"'),'charges',(select count(*) from charges where project_id='"+project+"'));")
before=invariant();report={'at':datetime.datetime.now(datetime.timezone.utc).isoformat(),'passed':False,'before':before,'checks':[]}
try:
 for gateway in ['10.1.201.70','10.1.201.71']:
  target[0]=gateway
  for model in ['minimax-h3','minimax-h3-live-validation']:
   for name,field,value,message in [('batch','generation_count',2,'generation_count=1'),('max-batch','generation_count',16,'generation_count=1'),('long','duration_seconds',10,'duration_seconds=5'),('short','duration_seconds',4,'duration_seconds=5'),('near-duration','duration_seconds',5.000001,'duration_seconds=5'),('portrait','aspect_ratio','9:16','aspect_ratio=16:9')]:
    request=copy.deepcopy(body);request['model']=model
    if field=='generation_count':request[field]=value
    else:request.setdefault('h3',{}).setdefault('target',{})[field]=value
    key='native-negative-'+str(uuid.uuid4());status,result=client.api('POST',path,request,key);record={'gateway':gateway,'model':model,'case':name,'status':status,'code':result.get('code'),'message':result.get('message'),'idempotency_key':key};report['checks'].append(record)
    assert status==400 and result.get('code')=='invalid_request' and message in result.get('message',''),record
    time.sleep(1)
 # Replay the actual relay's pre-deployment accepted Job.
 relay=pathlib.Path('/opt/vela-cluster/relay-api-full-20260917');state=json.loads((relay/'run/state.json').read_text());request=json.loads((relay/'request.json').read_text());c=f.Client('https://vela.marslab.ic/api',pathlib.Path('/opt/vela-cluster/relay-station-20260916/api-key').read_text().strip(),'/opt/vela-cluster/relay-station-20260916/gateway-ca.crt')
 for gateway in ['10.1.201.70','10.1.201.71']:
  target[0]=gateway;status,result=c.api('POST','/v1/projects/'+state['project_id']+'/jobs',request,state['idempotency_key']);assert status==202 and result['job_id']==state['job_id'] and result['pricing']==state['pricing'];report['checks'].append({'gateway':gateway,'case':'historical-relay-replay','status':status,'job_id':result['job_id']})
 report['after']=invariant();assert report['after']==before,'rejected requests left durable effects';report['passed']=True
finally:(root/'negative-api.json').write_text(json.dumps(report,indent=2))
print(json.dumps({'passed':True,'checks':len(report['checks']),'no_durable_effects':True}))
