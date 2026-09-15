#!/usr/bin/env python3
"""Generate Argo alert failure, partial coverage and recovery scenarios."""
import json
from pathlib import Path
import sys
import yaml

source=next(o['spec'] for o in yaml.safe_load_all(Path(sys.argv[1]).read_text()) if o['kind']=='PrometheusRule')
root=Path(sys.argv[2]);root.mkdir(parents=True,exist_ok=True)
(root/'rules.json').write_text(json.dumps(source))
rules={r['alert']:r for g in source['groups'] for r in g['rules']};tests=[]
def case(name,alert,series,labels):
    r=rules[alert];expected=[]
    for label in labels:
        annotations={key:value.replace('{{ $labels.name }}',label.get('name','')) for key,value in r['annotations'].items()}
        expected.append({'exp_labels':dict(label,**r['labels']),'exp_annotations':annotations})
    tests.append({'name':name,'interval':'30s','input_series':[{'series':s,'values':v} for s,v in series],'alert_rule_test':[{'eval_time':'40m','alertname':alert,'exp_alerts':expected}]})
up=[(f'up{{namespace="argocd",instance="target-{i}"}}','1+0x80') for i in range(5)]
case('all five targets healthy','VelaArgoMetricsMissing',up,[])
case('one target vanished','VelaArgoMetricsMissing',up[:4],[{}])
case('one target failing','VelaArgoMetricsMissing',up[:4]+[(up[4][0],'0+0x80')],[{}])
case('all targets vanished','VelaArgoMetricsMissing',[],[{'namespace':'argocd'}])
case('brief failure recovered','VelaArgoMetricsMissing',up[:4]+[(up[4][0],'0+0x3 1+0x77')],[])
labels={'name':'sample','namespace':'argocd','project':'llm-api'}
def metric(name,extra):return name+'{'+','.join(f'{k}="{v}"' for k,v in dict(labels,**extra).items())+'}'
case('unhealthy application','VelaArgoApplicationUnhealthy',[(metric('argocd_app_info',{'health_status':'Degraded'}),'1+0x80')],[dict(labels,health_status='Degraded')])
case('healthy application','VelaArgoApplicationUnhealthy',[(metric('argocd_app_info',{'health_status':'Healthy'}),'1+0x80')],[])
case('sustained drift','VelaArgoApplicationOutOfSync',[(metric('argocd_app_info',{'sync_status':'OutOfSync'}),'1+0x80')],[dict(labels,sync_status='OutOfSync')])
case('synced application','VelaArgoApplicationOutOfSync',[(metric('argocd_app_info',{'sync_status':'Synced'}),'1+0x80')],[])
case('repeated sync failures','VelaArgoSyncFailed',[(metric('argocd_app_sync_total',{'phase':'Error'}),'0+1x80')],[dict(labels,phase='Error')])
case('successful syncs','VelaArgoSyncFailed',[(metric('argocd_app_sync_total',{'phase':'Succeeded'}),'0+1x80')],[])
case('old failure does not keep firing','VelaArgoSyncFailed',[(metric('argocd_app_sync_total',{'phase':'Error'}),'1+0x80')],[])
(root/'tests.json').write_text(json.dumps({'rule_files':['rules.json'],'evaluation_interval':'30s','tests':tests}))
print(str(len(tests))+' platform publishing alert scenarios')
