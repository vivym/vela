import pathlib,importlib.util,socket,json,sys,os,concurrent.futures,datetime,urllib.request,urllib.error,urllib.parse,subprocess,hashlib
os.umask(0o077);os.environ['KUBECONFIG']='/etc/rancher/rke2/rke2.yaml'
root=pathlib.Path('/opt/vela-cluster/relay-native-contract-20260918');root.mkdir(mode=0o700,exist_ok=True)
spec=importlib.util.spec_from_file_location('flow','/tmp/verify-h3-api-flow.py');flow=importlib.util.module_from_spec(spec);spec.loader.exec_module(flow)
request=json.loads(pathlib.Path('/opt/vela-cluster/h3-api-acceptance-20260917-minimax-v8/request.json').read_text());request['prompt']='夕阳下的湖面，一只白色水鸟轻轻掠过，镜头平稳，保留自然水声与鸟鸣。';request['client_metadata']={'relay_order_id':'relay-native-contract-20260918','external_user_id':'验收用户','integer_order_id':9007199254740993}
(root/'request.json').write_text(json.dumps(request,ensure_ascii=False,indent=2))
project='62275ddc-ae83-4ca1-b80c-313161264836';jobs='/v1/projects/'+project+'/jobs';ca='/opt/vela-cluster/relay-station-20260916/gateway-ca.crt';credential='/opt/vela-cluster/relay-station-20260916/api-key';orig=socket.getaddrinfo;target=['10.1.201.70'];socket.getaddrinfo=lambda host,port,*a,**kw:orig(target[0] if host=='vela.marslab.ic' else host,port,*a,**kw)
checks=[];OriginalClient=flow.Client
class RelayClient(OriginalClient):
 def api(self,method,path,body=None,key=None,authenticated=True):
  target[0]='10.1.201.71' if target[0]=='10.1.201.70' else '10.1.201.70'
  status,result=super().api(method,path,body,key,authenticated)
  if method=='POST' and path==jobs and status==202 and not checks:
   jobid=result['job_id']
   # Treat the first accepted response as lost to the relay. Persisted key/body recover it.
   target[0]='10.1.201.71' if target[0]=='10.1.201.70' else '10.1.201.70'
   with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
    calls=[ex.submit(OriginalClient.api,self,method,path,body,key,authenticated) for _ in range(4)]
    for future in calls:
     code,replay=future.result();assert code==202 and replay['job_id']==jobid and replay['pricing']==result['pricing']
   code,replay=super().api(method,path,dict(reversed(list(body.items()))),key,authenticated);assert code==202 and replay['job_id']==jobid
   changed=json.loads(json.dumps(body));changed['client_metadata']['integer_order_id']=9007199254740992
   code,_=super().api(method,path,changed,key,authenticated);assert code==409
   checks.extend(['lost_response_simulation_same_key_recovers_original_job','four_concurrent_replays_one_job','json_key_order_does_not_change_identity','large_metadata_integer_change_conflicts'])
   result=replay
  if method=='GET' and path.startswith(jobs+'/') and path.count('/')==5 and status==200:
   code,page=super().api('GET',jobs+'?active=true&limit=100');assert code==200
   found=result['job_id'] in {j['job_id'] for j in page['jobs']}
   if result['state'] not in ['SUCCEEDED','FAILED','CANCELED'] and not found:
    code,current=super().api('GET',path);assert code==200 and current['state'] in ['SUCCEEDED','FAILED','CANCELED']
   elif result['state']=='SUCCEEDED':assert not found
   checks.append('poll_and_active_list_'+target[0]+'_'+result['state'])
  return status,result
flow.Client=RelayClient
sys.argv=['verify-h3-api-flow.py','--api-url','https://vela.marslab.ic/api','--credential-file',credential,'--ca',ca,'--project-id',project,'--request',str(root/'request.json'),'--run-directory',str(root/'run'),'--timeout','2400','--kubectl','/var/lib/rancher/rke2/bin/kubectl']
code=flow.main()
if code:raise SystemExit(code)
receipt=json.loads((root/'run/receipt.json').read_text());state=json.loads((root/'run/state.json').read_text());client=OriginalClient('https://vela.marslab.ic/api',pathlib.Path(credential).read_text().strip(),ca)
for host in ['10.1.201.70','10.1.201.71']:
 target[0]=host
 status,job=client.api('GET',jobs+'/'+state['job_id']);assert status==200 and job['state']=='SUCCEEDED'
 status,page=client.api('GET',jobs+'?state=SUCCEEDED&limit=100');assert status==200 and state['job_id'] in {j['job_id'] for j in page['jobs']}
 status,artifacts=client.api('GET',jobs+'/'+state['job_id']+'/artifacts');assert status==200
 for a in artifacts['artifacts']:
  dst=root/(host+('-video.mp4' if a['kind']=='VIDEO' else '-thumbnail.webp'));client.download(a,dst)
  request=urllib.request.Request(a['download_url'],headers={'Range':'bytes=0-1023'})
  with client.opener.open(request,timeout=30) as response:
   wire=response.read();assert response.status==206 and wire==dst.read_bytes()[:1024];assert response.headers['Content-Range']=='bytes 0-1023/'+str(a['size_bytes'])
  url=urllib.parse.urlsplit(a['download_url']);unsigned=urllib.parse.urlunsplit((url.scheme,url.netloc,url.path,'',''))
  try:
   with client.opener.open(unsigned,timeout=30) as response:raise AssertionError('unsigned artifact access accepted')
  except urllib.error.HTTPError as error:assert error.code==403
  if a['kind']=='VIDEO':
   probe=flow.verify_media('ffprobe',dst,a['media'],124,5175);flow.write_json(root/(host+'-ffprobe.json'),probe)
   subprocess.run(['ffmpeg','-v','error','-xerror','-i',str(dst),'-map','0:v','-map','0:a','-f','null','-'],check=True,capture_output=True,timeout=120)
 checks.extend(['complete_media_and_range_'+host,'unsigned_artifact_denied_'+host,'successful_job_listed_'+host])
bill=flow.billing_snapshot('/var/lib/rancher/rke2/bin/kubectl','vela-system',state['job_id']);flow.validate_billing(bill,job,artifacts['artifact_set_id']);assert bill['charge']==receipt['billing']['charge']
flow.write_json(root/'supplemental.json',{'passed':True,'checks':checks,'billing':bill,'job_id':state['job_id'],'at':datetime.datetime.now(datetime.timezone.utc).isoformat()});print(json.dumps({'passed':True,'job_id':state['job_id'],'checks':len(checks)}))
