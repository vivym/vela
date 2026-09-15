#!/usr/bin/env python3
"""Generate promtool cases for expiry, issuance and missed sync detection."""
import json
from pathlib import Path
import sys

import yaml

source = next(x['spec'] for x in yaml.safe_load_all(Path(sys.argv[1]).read_text())
              if x and x['kind'] == 'PrometheusRule')
out = Path(sys.argv[2])
out.mkdir(parents=True, exist_ok=True)
(out / 'rules.json').write_text(json.dumps(source))
rules = {r['alert']: r for g in source['groups'] for r in g['rules']}
tests = []


def case(name, alert, series, expected):
    rule = rules[alert]
    tests.append({'name': name, 'interval': '30s',
                  'input_series': [{'series': metric, 'values': values}
                                   for metric, values in series],
                  'alert_rule_test': [{'eval_time': '30m', 'alertname': alert,
                                       'exp_alerts': [{'exp_labels': {**x, **rule['labels']},
                                                       'exp_annotations': rule['annotations']}
                                                      for x in expected]}]})


certificate = {'namespace': 'apisix', 'name': 'vela-gateway'}
expiry = 'certmanager_certificate_expiration_timestamp_seconds{namespace="apisix",name="vela-gateway"}'
case('30 days remain', 'VelaGatewayCertificateExpiring', [(expiry, '2593800+0x60')], [])
case('one hour remains', 'VelaGatewayCertificateExpiring', [(expiry, '5400+0x60')], [certificate])
case('already expired', 'VelaGatewayCertificateExpiring', [(expiry, '1+0x60')], [certificate])
ready = 'certmanager_certificate_ready_status{namespace="apisix",name="vela-gateway",condition="True"}'
case('issuance ready', 'VelaGatewayCertificateNotReady', [(ready, '1+0x60')], [])
case('issuance not ready', 'VelaGatewayCertificateNotReady', [(ready, '0+0x60')], [{**certificate, 'condition': 'True'}])
case('certificate telemetry disappeared', 'VelaGatewayCertificateNotReady', [(ready, '1 stale')], [{**certificate, 'condition': 'True'}])
sync = 'kube_cronjob_status_last_successful_time{namespace="apisix",cronjob="vela-gateway-tls-sync"}'
labels = {'namespace': 'apisix', 'cronjob': 'vela-gateway-tls-sync'}
case('synchronizer succeeding', 'VelaGatewayCertificateSyncStale', [(sync, '0+30x60')], [])
case('synchronizer stopped', 'VelaGatewayCertificateSyncStale', [(sync, '1+0x60')], [labels])
case('synchronizer never succeeded or disappeared', 'VelaGatewayCertificateSyncStale', [], [labels])
(out / 'tests.json').write_text(json.dumps({'rule_files': ['rules.json'], 'evaluation_interval': '30s', 'tests': tests}))
print(f'{len(tests)} gateway certificate scenarios')
