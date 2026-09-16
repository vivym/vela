import copy
import hashlib
import importlib.util
import json
from pathlib import Path
import unittest

ROOT = Path(__file__).parent
SPEC = importlib.util.spec_from_file_location('host_policy', ROOT / 'build-runtime-host-image-policy.py')
POLICY = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(POLICY)


class HostPolicyTest(unittest.TestCase):
    def fixture(self, node='server-23'):
        return json.loads((ROOT / 'testdata/runtime-host-policy' / (node + '-dra.json')).read_text())

    def test_exact_device_edits_are_included(self):
        for node, count in [('server-22', 6), ('server-23', 7)]:
            with self.subTest(node=node):
                inv = self.fixture(node)
                result = POLICY.build(inv, 'GPU')['runtime_policy']
                spec = inv['dra']['cdi_spec']
                edits = spec['containerEdits']['hooks'] + spec['devices'][0]['containerEdits'].get('hooks', [])
                self.assertEqual(len(edits), count)
                hooks = {'createContainer': [{k: h[k] for k in ['path', 'args', 'env']} for h in edits]}
                self.assertEqual(result['hooks_digest'], list(hashlib.sha256(json.dumps(hooks, separators=(',', ':')).encode()).digest()))
                if node == 'server-23':
                    changed = copy.deepcopy(inv)
                    changed['dra']['cdi_spec']['devices'][0]['containerEdits']['hooks'] = []
                    self.assertNotEqual(POLICY.build(changed, 'GPU')['runtime_policy']['hooks_digest'], result['hooks_digest'])

    def test_rejects_unqualified_source_device_and_hook(self):
        changes = [
            lambda i: i['dra']['provenance'].update(dra_image='other@sha256:' + '0'*64),
            lambda i: i['dra']['provenance'].update(hook_sha256='0'*64),
            lambda i: i['dra']['provenance'].update(method='copy live OCI'),
            lambda i: i['dra']['provenance'].update(node='other'),
            lambda i: i.update(gpu_uuid='GPU-other'),
            lambda i: i['binaries'][POLICY.DRA_HOOK].update(sha256='0'*64),
            lambda i: i['dra']['cdi_spec']['containerEdits']['hooks'][0].update(path='/tmp/hook'),
            lambda i: i['dra']['cdi_spec']['containerEdits']['hooks'][0].update(timeout=10),
            lambda i: i['dra']['cdi_spec']['containerEdits'].update(env=['LD_PRELOAD=/tmp/evil']),
        ]
        for change in changes:
            with self.subTest(change=changes.index(change)):
                inv = self.fixture()
                change(inv)
                with self.assertRaises(ValueError):
                    POLICY.build(inv, 'GPU')

    def test_cpu_does_not_inherit_gpu_edits(self):
        result = POLICY.build(self.fixture(), 'CPU')['runtime_policy']
        self.assertEqual(result['hooks_digest'], [0]*32)
        self.assertEqual(result['additional_environment'], [])


if __name__ == '__main__':
    unittest.main()
