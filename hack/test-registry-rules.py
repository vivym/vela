#!/usr/bin/env python3
"""Generate failure, missing-coverage and certificate-expiry promtool scenarios."""
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
                  'input_series': [{'series': metric, 'values': values} for metric, values in series],
                  'alert_rule_test': [{'eval_time': '30m', 'alertname': alert,
                                       'exp_alerts': [{'exp_labels': {**x, **rule['labels']},
                                                       'exp_annotations': rule['annotations']} for x in expected]}]})


labels = {'job': 'vela-registry-authenticated', 'instance': 'https://10.1.201.70:5005/v2/', 'registry_check': 'authenticated'}
series = 'probe_success{' + ','.join(f'{k}="{v}"' for k, v in labels.items()) + '}'
case('authentication working', 'VelaRegistryProbeFailed', [(series, '1+0x60')], [])
case('authentication or registry unavailable', 'VelaRegistryProbeFailed', [(series, '0+0x60')], [labels])
case('brief failure recovered', 'VelaRegistryProbeFailed', [(series, '0+0x3 1+0x57')], [])
coverage = [(f'probe_success{{job="vela-registry-test",instance="target-{i}"}}', '1+0x60') for i in range(9)]
case('all nine targets present', 'VelaRegistryProbeCoverageMissing', coverage, [])
case('one of nine targets lost', 'VelaRegistryProbeCoverageMissing', coverage[:8], [{}])
case('all targets disappeared', 'VelaRegistryProbeCoverageMissing', [], [{}])
expiry = 'probe_ssl_earliest_cert_expiry{job="vela-registry-authenticated",instance="https://10.1.201.70:5005/v2/"}'
cert_labels = {k: v for k, v in labels.items() if k != 'registry_check'}
case('certificate valid for 60 days', 'VelaRegistryCertificateExpiring', [(expiry, '5185800+0x60')], [])
case('certificate expires in 20 days', 'VelaRegistryCertificateExpiring', [(expiry, '1729800+0x60')], [cert_labels])
case('certificate expired', 'VelaRegistryCertificateExpiring', [(expiry, '1+0x60')], [cert_labels])
(out / 'tests.json').write_text(json.dumps({'rule_files': ['rules.json'], 'evaluation_interval': '30s', 'tests': tests}))
print(f'{len(tests)} registry alert scenarios')
