#!/usr/bin/env python3
"""Build actual Fleet rollout candidates for independent H3 stages.

Requires real host attestations and an existing, pinned runtime rollout as the
execution template. Does not grant catalog certification or publish readiness.
Run hack/render-worker-rollout next to validate and render Kubernetes resources.
"""
import argparse
import copy
import hashlib
import json
import uuid
from collections import Counter
from pathlib import Path

NAMESPACE = uuid.UUID('a4f821ee-4208-4cff-8882-373ad15d61d4')
COMPONENTS = {'encoder': 'ENCODER', 'dit': 'DIT', 'decoder': 'VAE_DECODER'}


def ident(kind, key):
    return str(uuid.uuid5(NAMESPACE, f'minimax-h3/{kind}/{key}'))


def digest(value):
    return hashlib.sha256(json.dumps(value, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()


def build(records, source, approval, source_configmaps):
    if Counter(r['role'] for r in records.values()) != Counter(encoder=1, dit=8, decoder=2):
        raise ValueError('expected 1 encoder, 8 dit and 2 decoder')
    templates = {}
    for bundle in source['worker_bundles']:
        for worker in bundle['worker_instances']:
            for runtime in worker['model_runtimes']:
                templates[runtime['component']] = (bundle, worker, runtime)
    plan_id = ident('plan', '20260917')
    plan = dict(schema_version=1, id=plan_id, stable_id='minimax-h3-20260917', revision=1,
                content_digest='', approval_evidence_digest=digest(approval),
                approved_at=approval['approved_at'], approved_by=approval['approved_by'],
                capacity_pools=[], worker_bundles=[], worker_instances=[])
    bundles, evidence, catalog, configmaps = [], {}, {}, []
    devices, workers = set(), set()
    for key, record in sorted(records.items(), key=lambda item: item[1]['ordinal']):
        role = record['role']; component = COMPONENTS[role]
        rows = record.get('measured', [])
        if len(rows) != 1:
            raise ValueError(f'{key}: actual device attestation required')
        measured = rows[0]
        if key != f"{role}-{record['ordinal']}":
            raise ValueError('worker key differs from role and ordinal')
        for value in record['ids'].values():
            parsed = uuid.UUID(value)
            if str(parsed) != value or parsed.int == 0:
                raise ValueError('expected canonical nonzero UUID')
        expected = record['input']['Devices'][0]
        if record['node'] != record['input']['NodeIdentity'] or record['node'] != expected['NodeIdentity']:
            raise ValueError('scheduling node differs from measured node')
        if any(measured.get(f) != expected[f] for f in ['DeviceID', 'ComputeNodeID', 'NodeIdentity', 'GPUUUID', 'PCIBDF']):
            raise ValueError(f'{key}: measured identity differs')
        if measured.get('Health') != 'HEALTHY' or any(type(measured.get(f)) is not int or measured[f] <= 0 for f in ['NodeEpoch', 'AgentSessionEpoch', 'DeviceEpoch']):
            raise ValueError(f'{key}: unhealthy or unmeasured device')
        for field in ['NodeAttestationDigest', 'DeviceAttestationDigest']:
            value = measured.get(field, '')
            if len(value) != 64 or bytes.fromhex(value) == bytes(32):
                raise ValueError(f'{key}: missing attestation digest')
        gpu = measured['GPUUUID']
        ids = record['ids']
        if gpu in devices or ids['worker'] in workers:
            raise ValueError('duplicate GPU or Worker identity')
        devices.add(gpu); workers.add(ids['worker'])
        source_bundle, source_worker, source_runtime = templates[component]
        bundle = copy.deepcopy(source_bundle)
        runtime = copy.deepcopy(source_runtime)
        profile = ident('worker-profile', role)
        stage_profile = ident('stage-profile', role)
        pool = ident('pool', role)
        residency = ident('residency', key)
        runtime.update(model_residency_id=residency, capacity_pool_id=pool,
                       stage_profile_revision_id=stage_profile, runtime_identity='minimax-h3-'+key)
        runtime['environment'] = [
            name+'='+gpu if name in ['CUDA_VISIBLE_DEVICES', 'NVIDIA_VISIBLE_DEVICES'] else env
            for env in runtime['environment'] for name in [env.split('=', 1)[0]]]
        membership = digest([dict(device_id=measured['DeviceID'], node_id=measured['ComputeNodeID'],
                                 gpu_uuid=gpu, pci_bdf=measured['PCIBDF'], node_epoch=measured['NodeEpoch'],
                                 device_epoch=measured['DeviceEpoch'])])
        topology = digest([dict(device_id=measured['DeviceID'], node_identity=record['node'], region='marslab',
                               network='10.1.201.0/24', fault_domain='shared-cluster', ordinal=0)])
        worker = dict(id=ids['worker'], instance_epoch=1, worker_profile_revision_id=profile,
                      capacity_pool_id=pool, role="vae" if role == "decoder" else role, capacity_slots=1,
                      membership_digest=membership,
                      device_set_digest=hashlib.sha256(bytes.fromhex(membership+topology)).hexdigest(),
                      model_runtimes=[runtime], members=[dict(id=ids['member'], member_epoch=1, key='member-0',
                          node_identity=record['node'], resource_class='GPU', device_count=1,
                          identity_digest=hashlib.sha256(('spiffe://vela.internal/stage-worker/'+ids['member']).encode()).hexdigest(),
                          device_subset_digest=digest([measured['DeviceID']]), device_constraints=[dict(
                              device_id=measured['DeviceID'], device_epoch=measured['DeviceEpoch'],
                              resource_class='GPU', gpu_uuid=gpu, pci_bdf=measured['PCIBDF'])])])
        bundle.update(plan_revision_id=plan_id, worker_bundle_id=ids['bundle'], revision_digest='',
                      stage_worker_config_map='vela-minimax-h3-worker-'+key,
                      stage_worker_control_tls_secret='vela-worker-tls-'+ids['member'],
                      worker_instances=[worker])
        configmaps.append(dict(apiVersion='v1', kind='ConfigMap', metadata=dict(
            name=bundle['stage_worker_config_map'], namespace=bundle['namespace']),
            data=copy.deepcopy(source_configmaps[source_bundle['stage_worker_config_map']]['data'])))
        bundles.append(bundle)
        if role not in catalog:
            plan['capacity_pools'].append(dict(id=pool, stable_id='minimax-h3-'+role,
                stage_profile_revision_id=stage_profile, resource_class='GPU', security_class='INTERNAL',
                region='marslab', max_ready_queue_depth=32))
            catalog[role] = dict(worker_profile_revision_id=profile, stage_profile_revision_id=stage_profile,
                source_worker_profile_revision_id=source_worker['worker_profile_revision_id'],
                source_stage_profile_revision_id=source_runtime['stage_profile_revision_id'],
                model_component_revision=runtime['model_component_revision'],
                runtime_image_digest=bundle['runtime_image'].split('@')[1], required_state='CERTIFIED')
        plan['worker_bundles'].append(dict(id=ids['bundle'], stable_id='minimax-h3-'+key, desired_generation=1, layout_digest=''))
        plan['worker_instances'].append(dict(id=ids['worker'], worker_profile_revision_id=profile,
            capacity_pool_id=pool, worker_bundle_id=ids['bundle'], desired_member_count=1, desired_device_count=1,
            model_runtime_routes=[{f: runtime[f] for f in ['model_residency_id', 'capacity_pool_id', 'stage_profile_revision_id']}]))
        evidence[ids['worker']] = [dict(schema_version=1, worker_instance_id=ids['worker'], instance_epoch=1,
            control_session_epoch=1, device_set=dict(id=ids['device-set'], devices=[dict(id=measured['DeviceID'],
                compute_node_id=measured['ComputeNodeID'], node_identity=record['node'], region='marslab',
                network_domain='10.1.201.0/24', fault_domain='shared-cluster', kind='GPU', gpu_uuid=gpu,
                pci_bdf=measured['PCIBDF'], ordinal=0)]),
            members=[dict(id=ids['member'], member_key='member-0', compute_node_id=measured['ComputeNodeID'],
                          member_epoch=1, device_ids=[measured['DeviceID']], readiness='UNOBSERVED')],
            residencies=[dict(id=residency, model_component_revision=runtime['model_component_revision'],
                runtime_identity=runtime['runtime_identity'], runtime_image_digest=catalog[role]['runtime_image_digest'],
                model_runtime_epoch=0, state='UNOBSERVED')], capacity=dict(vector=dict(active_stage_slots=1, gpu_count=1)))]
    return dict(approved_plan=plan, worker_bundles=bundles), evidence, catalog, configmaps


def write(path, value):
    with path.open('x') as stream:
        path.chmod(0o600)
        json.dump(value, stream, indent=2); stream.write('\n')


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('identities', type=Path)
    p.add_argument('output_directory', type=Path)
    p.add_argument('--source-rollout', required=True, type=Path)
    p.add_argument('--approval', required=True, type=Path)
    p.add_argument('--source-configmaps', required=True, type=Path)
    a = p.parse_args()
    rollout, evidence, catalog, configmaps = build(*(json.loads(path.read_text()) for path in [a.identities, a.source_rollout, a.approval, a.source_configmaps]))
    a.output_directory.mkdir(mode=0o700)  # Refuse stale/partially rendered output.
    write(a.output_directory/'rollout-input.json', rollout)
    write(a.output_directory/'catalog-requirements.json', catalog)
    write(a.output_directory/'configmaps.json', dict(apiVersion='v1', kind='List', items=configmaps))
    for worker_id, template in evidence.items():
        directory = a.output_directory/'workers'/worker_id
        directory.mkdir(mode=0o700, parents=True)
        write(directory/'evidence-template.json', template)


if __name__ == '__main__':
    main()
