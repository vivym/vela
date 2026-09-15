"""Fail-closed source binding and policy cutover tests; no remote access."""
import copy
import datetime
import importlib.util
import ipaddress
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('worker_control_network', Path(__file__).with_name('worker-control-network.py'))
network = importlib.util.module_from_spec(spec)
spec.loader.exec_module(network)


def discovery():
    nodes = []
    for index, ip in enumerate(sorted(network.WORKER_IPS, key=ipaddress.ip_address), 2):
        nodes.append({'name': 'node-' + ip, 'uid': 'uid-' + ip, 'ip': ip,
                      'pod_cidr': f'10.42.{index}.0/24', 'candidate_source': f'10.42.{index}.0',
                      'ready': ip != '10.1.201.19'})
    state = {'boot_id': 'unchanged', 'units': 'unchanged'}
    receipt = {'at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'inventory': nodes,
               'result': 'WORKER_SOURCES_VERIFIED', 'cleanup_complete': True, 'observations': []}
    for node in nodes:
        if node['ready']:
            receipt['observations'].append({'node': copy.deepcopy(node), 'before': state.copy(), 'after': state.copy(),
                'results': [{'kind': 'source', 'node': name, 'connected': True, 'observed_source': node['candidate_source']}
                            for name in network.CPU.values()]})
    return receipt, copy.deepcopy(nodes)


class SourceBindingTests(unittest.TestCase):
    def setUp(self):
        self.receipt, self.nodes = discovery()

    def validate(self):
        return network.validate_discovery(self.receipt, self.nodes)

    def test_exact_online_host_sources_only(self):
        policy = self.validate()
        rules = policy['ingress']
        self.assertEqual(rules[0]['ports'], [{'protocol': 'TCP', 'port': 8444}])
        self.assertEqual(len(rules[0]['from']), 51)
        self.assertTrue(all(r['ipBlock']['cidr'].endswith('/32') for r in rules[0]['from']))

    def test_alternative_lan_source_must_match_own_host(self):
        row = self.receipt['observations'][0]
        for result in row['results']:
            result['observed_source'] = row['node']['ip']
        self.validate()
        row['results'][1]['observed_source'] = row['node']['candidate_source']
        with self.assertRaisesRegex(ValueError, 'bound to this worker'):
            self.validate()

    def test_another_workers_source_is_not_accepted(self):
        for result in self.receipt['observations'][0]['results']:
            result['observed_source'] = self.receipt['observations'][1]['node']['candidate_source']
        with self.assertRaisesRegex(ValueError, 'bound to this worker'):
            self.validate()

    def test_each_node_identity_field_is_pinned(self):
        for field, value in [('uid', 'new'), ('ip', '10.1.201.100'), ('pod_cidr', '10.42.200.0/24'), ('ready', False)]:
            with self.subTest(field=field):
                nodes = copy.deepcopy(self.nodes)
                nodes[0][field] = value
                with self.assertRaisesRegex(ValueError, 'inventory changed'):
                    network.validate_discovery(self.receipt, nodes)

    def test_offline_identity_is_also_pinned(self):
        next(n for n in self.nodes if not n['ready'])['uid'] = 'replaced'
        with self.assertRaisesRegex(ValueError, 'inventory changed'):
            self.validate()

    def test_stale_future_and_unfinished_discovery(self):
        for seconds in [-3700, 60]:
            self.receipt['at'] = (datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(seconds=seconds)).isoformat()
            with self.assertRaises(ValueError):
                self.validate()
        self.receipt, self.nodes = discovery()
        for field, value in [('result', 'FAILED'), ('cleanup_complete', False)]:
            receipt = copy.deepcopy(self.receipt)
            receipt[field] = value
            with self.assertRaisesRegex(ValueError, 'did not finish'):
                network.validate_discovery(receipt, self.nodes)

    def test_missing_host_or_cpu_destination_is_not_accepted(self):
        self.receipt['observations'].pop()
        with self.assertRaises(ValueError):
            self.validate()
        self.receipt, self.nodes = discovery()
        self.receipt['observations'][0]['results'][1]['node'] = 'llmpool01'
        with self.assertRaisesRegex(ValueError, 'CPU destinations'):
            self.validate()

    def test_host_restart_during_discovery_is_not_accepted(self):
        self.receipt['observations'][0]['after']['boot_id'] = 'restarted'
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            self.validate()

    def test_no_cidr_pod_address_cpu_or_protected_host_rules(self):
        for source in ['10.42.2.0/24', '10.42.2.4', '10.42.0.0', '10.42.1.0',
                       '10.1.201.44', '10.1.201.56', '10.1.201.57', '10.1.201.70', '10.1.201.71', '::1']:
            with self.subTest(source=source), self.assertRaises(ValueError):
                network.candidate_policy([source])
        for sources in [[], ['10.42.2.0', '10.42.2.0']]:
            with self.assertRaises(ValueError):
                network.candidate_policy(sources)

    def test_inventory_must_cover_exact_authorized_hosts(self):
        api_nodes = [{'metadata': {'name': n['name'], 'uid': n['uid'], 'labels': {'vela.ai/gpu-runtime': 'true'}},
                      'spec': {'podCIDR': n['pod_cidr']}, 'status': {'addresses': [{'type': 'InternalIP', 'address': n['ip']}],
                      'conditions': [{'type': 'Ready', 'status': 'True' if n['ready'] else 'False'}]}} for n in self.nodes]
        with patch.object(network, 'get', return_value={'items': api_nodes}):
            self.assertEqual(network.inventory(), self.nodes)
        for invalid in [api_nodes[:-1], api_nodes + [api_nodes[0]], copy.deepcopy(api_nodes)]:
            if len(invalid) == len(api_nodes):
                invalid[0]['status']['addresses'][0]['address'] = '10.1.201.56'
            with patch.object(network, 'get', return_value={'items': invalid}), self.assertRaises(ValueError):
                network.inventory()


class CutoverTests(unittest.TestCase):
    def run_cutover(self, failure, *, concurrent=False, baseline=True):
        discovered, nodes = discovery()
        old_spec = network.candidate_policy([next(n['candidate_source'] for n in nodes if n['ip'] == '10.1.201.66')])
        old = {'apiVersion': 'networking.k8s.io/v1', 'kind': 'NetworkPolicy',
               'metadata': {'name': network.POLICY, 'uid': 'original', 'resourceVersion': '1'}, 'spec': old_spec}
        live = copy.deepcopy(old)
        receipt = {}
        commands = []
        target = [{'kind': 'tcp', 'ip': '10.42.0.10', 'port': 8444, 'node': 'llmpool01'},
                  {'kind': 'tcp', 'ip': '10.42.1.10', 'port': 8444, 'node': 'llmpool02'},
                  {'kind': 'tcp', 'ip': '10.43.0.10', 'port': 8444, 'node': 'Service'}]

        def kub(*args, **kwargs):
            commands.append(args)
            if 'patch' in args:
                patches = json.loads(args[-1])
                self.assertTrue(any(p['path'] == '/metadata/uid' and p['op'] == 'test' for p in patches))
                live['spec'] = copy.deepcopy(patches[-1]['value'])
                live['metadata']['resourceVersion'] = str(int(live['metadata']['resourceVersion']) + 1)
                if failure == 'lost_response' and len([c for c in commands if 'patch' in c]) == 1:
                    raise RuntimeError('lost patch response')
            return ''

        def peers(*args):
            if concurrent:
                live['spec'] = {'concurrent': 'operator edit'}
            if failure == 'timeout':
                raise RuntimeError('verification transport unavailable')
            row = copy.deepcopy(discovered['observations'][0])
            row['results'] = [{'kind': 'tcp', 'expected_denied': True, 'connected': False,
                               'error_type': 'ConnectionRefusedError'}]
            return [row]

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'discovery.json'
            path.write_text(json.dumps(discovered))
            with patch.object(network, 'control_state', return_value=({'tls_sha256': 'pinned'}, target)), \
                 patch.object(network, 'get', side_effect=lambda *a: copy.deepcopy(live)), \
                 patch.object(network, 'kub', side_effect=kub), \
                 patch.object(network, 'peer', return_value={'results': [{'anonymous_rejected': baseline}]}), \
                 patch.object(network, 'parallel_peers', side_effect=peers), \
                 patch.object(network.time, 'sleep'):
                with self.assertRaises((RuntimeError, ValueError)):
                    network.adopt(Path(directory), nodes, 'unused', receipt, path)
        return old, live, receipt, commands

    def test_failed_verification_restores_only_policy(self):
        old, live, receipt, _ = self.run_cutover('timeout')
        self.assertEqual(live['spec'], old['spec'])
        self.assertEqual(receipt['rollback'], 'previous policy restored')

    def test_lost_success_response_also_rolls_back(self):
        old, live, receipt, _ = self.run_cutover('lost_response')
        self.assertEqual(live['spec'], old['spec'])
        self.assertEqual(receipt['rollback'], 'previous policy restored')

    def test_concurrent_edit_is_not_overwritten(self):
        _, live, receipt, _ = self.run_cutover('timeout', concurrent=True)
        self.assertEqual(live['spec'], {'concurrent': 'operator edit'})
        self.assertIn('refused', receipt['rollback'])

    def test_anonymous_mtls_required_before_mutation(self):
        old, live, _, commands = self.run_cutover('timeout', baseline=False)
        self.assertEqual(live, old)
        self.assertFalse(any('patch' in c for c in commands))

    def test_connection_refused_does_not_prove_network_denial(self):
        old, live, receipt, _ = self.run_cutover('refused')
        self.assertEqual(live['spec'], old['spec'])
        self.assertEqual(receipt['rollback'], 'previous policy restored')


if __name__ == '__main__':
    unittest.main()
