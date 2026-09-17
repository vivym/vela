"""Regression checks for independent H3 release generation (synthetic fixtures)."""
import copy
import importlib.util
import json
from pathlib import Path
import subprocess
import tempfile
import unittest
import uuid

ROOT = Path(__file__).resolve().parent

def load(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / (name + '.py'))
    module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
    return module

IDENTITIES = load('build-h3-minimax-identities')
BUNDLES = load('build-h3-minimax-bundle-plan')


def fixture():
    assignments, nodes = [], []
    for i, role in enumerate(['encoder'] + ['dit'] * 8 + ['decoder'] * 2, 1):
        address = f'10.0.0.{i}'
        assignments.append(dict(role=role, ordinal=i, address=address,
            gpu_uuid='GPU-' + str(uuid.UUID(int=i)), pci_bdf='0000:01:00.0'))
        nodes.append(dict(metadata=dict(name=f'actual-node-{99-i}'),
                          status=dict(addresses=[dict(type='InternalIP', address=address)])))
    placement, snapshot = dict(assignments=assignments), dict(items=nodes)
    records = IDENTITIES.build(placement, snapshot)
    attestations = {}
    for key, record in records.items():
        row = copy.deepcopy(record['input']['Devices'][0]); row.pop('Kind')
        row.update(NodeEpoch=7, AgentSessionEpoch=9, DeviceEpoch=3,
                   NodeAttestationDigest='a'*64, DeviceAttestationDigest='b'*64, Health='HEALTHY')
        attestations[key] = [row]
    records = IDENTITIES.build(placement, snapshot, attestations)
    source = dict(worker_bundles=[])
    maps = {}
    for role, component in BUNDLES.COMPONENTS.items():
        name = 'source-'+role
        maps[name] = dict(data={'control-address':'controller:8447', 'connector-revision-id':str(uuid.UUID(int=22))})
        source['worker_bundles'].append(dict(schema_version=2, namespace='vela-system',
            init_image='registry.example.com/init@sha256:'+'1'*64,
            stage_worker_agent_image='registry.example.com/worker@sha256:'+'2'*64,
            runtime_image='registry.example.com/runtime@sha256:'+'3'*64,
            runtime_launch_protocol='kubernetes-pidfd-v1', image_pull_secrets=['pull'],
            stage_worker_config_map=name, model_runtime_verifier_config_map='verifier',
            stage_worker_control_tls_secret='tls', stage_worker_authority_secret='authority',
            artifact_store_credentials_secret='artifacts', artifact_store_ca_secret='ca',
            worker_instances=[dict(worker_profile_revision_id=str(uuid.UUID(int=33)), model_runtimes=[dict(
                stage_profile_revision_id=str(uuid.UUID(int=44)), model_runtime_epoch_floor=1,
                component=component, model_component_revision='test-'+role,
                command=['/opt/venv/bin/python','-B','-m','fast_h3.vela.driver'],
                environment=['CUDA_VISIBLE_DEVICES=old','NVIDIA_VISIBLE_DEVICES=old','HF_HUB_OFFLINE=1'],
                initialization_timeout='40m', shutdown_timeout='2m')])]))
    approval = dict(approved_at='2026-09-17T00:00:00Z', approved_by='test-operator')
    return placement, snapshot, attestations, records, source, approval, maps


class IdentityTest(unittest.TestCase):
    def test_node_mapping_and_expected_are_not_observations(self):
        placement, snapshot, *_ = fixture()
        records = IDENTITIES.build(placement, snapshot)
        self.assertEqual(records['encoder-1']['node'], 'actual-node-98')
        self.assertNotIn('measured', records['encoder-1'])

    def test_rejects_duplicates_and_unmeasured_epochs(self):
        placement, snapshot, att, *_ = fixture()
        att['encoder-1'][0]['DeviceEpoch'] = 0
        with self.assertRaises(ValueError): IDENTITIES.build(placement, snapshot, att)
        placement['assignments'][1]['gpu_uuid'] = placement['assignments'][0]['gpu_uuid']
        with self.assertRaises(ValueError): IDENTITIES.build(placement, snapshot)


class BundleTest(unittest.TestCase):
    def test_roles_devices_and_configmaps(self):
        _, _, _, records, source, approval, maps = fixture()
        rollout, evidence, catalog, configs = BUNDLES.build(records, source, approval, maps)
        workers = [b['worker_instances'][0] for b in rollout['worker_bundles']]
        secrets = [b['stage_worker_control_tls_secret'] for b in rollout['worker_bundles']]
        self.assertEqual(len(set(secrets)), 11)
        for bundle in rollout['worker_bundles']:
            member = bundle['worker_instances'][0]['members'][0]
            self.assertEqual(bundle['stage_worker_control_tls_secret'], 'vela-worker-tls-' + member['id'])

        self.assertEqual([w['role'] for w in workers].count('vae'), 2)
        self.assertEqual(len(evidence), 11)
        self.assertEqual(len(configs), 11)
        self.assertEqual(len({c['metadata']['name'] for c in configs}), 11)
        self.assertEqual(len(catalog), 3)
        for worker in workers:
            self.assertEqual(len(worker['model_runtimes']), 1)
            self.assertEqual(worker['members'][0]['device_constraints'][0]['device_epoch'], 3)
            self.assertNotIn('shared_slot_exception', worker)
        self.assertTrue(all(t[0]['residencies'][0]['state'] == 'UNOBSERVED' for t in evidence.values()))

    def test_rejects_path_escape_node_mismatch_and_missing_measurement(self):
        _, _, _, records, source, approval, maps = fixture()
        for change in [lambda r:r['ids'].update(worker='../../outside'),
                       lambda r:r.update(node='other-node'),
                       lambda r:r.pop('measured')]:
            bad = copy.deepcopy(records); change(bad['encoder-1'])
            with self.assertRaises(ValueError): BUNDLES.build(bad, source, approval, maps)

    def test_production_renderer_validates_all_instances_and_preservation(self):
        _, _, _, records, source, approval, maps = fixture()
        rollout, *_ = BUNDLES.build(records, source, approval, maps)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root/'rollout-input.json').write_text(json.dumps(rollout))
            empty = root/'existing.json'; empty.write_text('{"schema_version":1,"rollouts":[]}')
            command = ['go','run','./hack/render-worker-rollout','--directory',str(root)]
            missing = subprocess.run(command, cwd=ROOT.parent, capture_output=True, text=True)
            self.assertNotEqual(missing.returncode, 0)
            self.assertFalse((root/'rollouts.json').exists())
            rendered = subprocess.run(command+['--existing',str(empty)], cwd=ROOT.parent, capture_output=True, text=True)
            self.assertEqual(rendered.returncode, 0, rendered.stderr)
            self.assertEqual(len(list(root.glob('workers/*/members/*/launch.json'))), 11)
            self.assertEqual(len(list(root.glob('bundles/*/pods.json'))), 11)
            for path in root.glob('bundles/*/claims.json'):
                claim = json.loads(path.read_text())[0]
                self.assertEqual(claim['kind'], 'ResourceClaimTemplate')
                self.assertEqual(claim['apiVersion'], 'resource.k8s.io/v1')
            original = (root/'rollouts.json').read_bytes()
            duplicate = subprocess.run(command+['--existing',str(root/'rollouts.json')], cwd=ROOT.parent, capture_output=True, text=True)
            self.assertNotEqual(duplicate.returncode, 0)
            self.assertIn('duplicate plan', duplicate.stderr)
            self.assertEqual((root/'rollouts.json').read_bytes(), original)


if __name__ == '__main__': unittest.main()
