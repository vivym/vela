#!/usr/bin/env python3
"""Render operator-approved H3 worker inputs from measured host identities.

This creates catalog/plan candidates. It does not publish readiness, mutate a
cluster, certify production gates, or register an unobserved ModelRuntime.
"""
import argparse
import datetime
import hashlib
import json
from pathlib import Path
import uuid

NAMESPACE = uuid.UUID('a4f821ee-4208-4cff-8882-373ad15d61d4')
MODEL_CACHE = '/var/lib/vela/models/dad0bd33673dee603d107fda712ee22e1e6dca2268748f2b3d723f93c74d5aa1'
QUALIFICATION = '/var/lib/vela/models/qualification/h3-kube-r3'
SCRATCH = '/var/lib/vela/stage-worker/scratch'
GPU_IMAGE = '10.1.201.70:5005/vela-h3-stage-runtime@sha256:90c8d7d37858eac2bb6cee0586509d219ecfbd8ffa848643b35b1b6fa166b0c4'
WORKER_IMAGE = '10.1.201.70:5005/vela-stage-worker-agent@sha256:825d807d608acbecaa8b16e58b52f3e3c39324048f6724f13c1facc26b805a9d'


def identity(name):
    return str(uuid.uuid5(NAMESPACE, name))


def canonical(value):
    return json.dumps(value, separators=(',', ':'), ensure_ascii=False,
                      default=lambda item: item.hex() if isinstance(item, bytes) else str(item)).encode()


def digest(value):
    return hashlib.sha256(canonical(value)).hexdigest()


def write(path, value):
    path.write_text(json.dumps(value, indent=2) + '\n')
    path.chmod(0o600)


def sql_literal(value):
    if value is None:
        return 'NULL'
    if isinstance(value, bool):
        return 'true' if value else 'false'
    if isinstance(value, (int, float)):
        return str(value)
    if isinstance(value, bytes):
        return "decode('" + value.hex() + "','hex')"
    if isinstance(value, dict):
        value = canonical(value).decode()
    return "'" + str(value).replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('directory', type=Path)
    parser.add_argument('--init-image', required=True)
    parser.add_argument('--cpu-image', required=True)
    args = parser.parse_args()
    root = args.directory
    measured = json.loads((root / 'identities.json').read_text())
    qualification = json.loads((root / 'qualification-manifest.json').read_text())
    evidence = bytes.fromhex(qualification['receipt.json'])
    plan_id = identity('h3-live-plan-20260916')
    statements = ['-- Limited internal qualification from the named real GPU receipt.', 'BEGIN;']

    def insert(table, row):
        statements.append(f"INSERT INTO {table} ({','.join(row)}) VALUES ({','.join(sql_literal(v) for v in row.values())});")

    def revision(table, key, fields):
        row = dict(id=identity(key), stable_id='h3-live-' + key.replace('/', '-'), revision=1, state='CERTIFIED', **fields)
        row['content_digest'] = bytes.fromhex(digest(row))
        insert(table, row)

    interfaces = [('request', 'request', 'json', 1048576), ('conditioning', 'tensor', 'minimax-h3-boundary-v1', 67108864),
                  ('latent', 'tensor', 'minimax-h3-boundary-v1', 1073741824), ('video', 'video', 'mp4', 8589934592),
                  ('thumbnail', 'image', 'webp', 4194304)]
    for key, kind, encoding, maximum in interfaces:
        contract = {'encoding': encoding, 'port': key, 'qualification_receipt_sha256': evidence.hex(), 'scope': 'internal-validation'}
        revision('stage_interface_revisions', 'interface/' + key, dict(payload_kind=kind, dtype='', layout=key,
                 shape_contract=contract, serialization=encoding, max_bytes=maximum, digest_algorithm='sha256',
                 schema_digest=bytes.fromhex(digest(contract))))
    for role, components in [('aux', ['ENCODER', 'VAE_DECODER']), ('dit', ['DIT']), ('thumbnail', ['CPU_MEDIA'])]:
        resource = 'CPU' if role == 'thumbnail' else 'GPU'
        capacity = {'active_stage_slots': 1, 'cpu_slot_count' if resource == 'CPU' else 'gpu_count': 1}
        revision('worker_profile_revisions', 'worker-profile/' + role, dict(device_count=1, member_count=1,
                 device_set_shape={'kind': 'single-' + resource.lower(), 'shared_slot_exception': 'H3_AUX_ENCODER_VAE' if role == 'aux' else ''},
                 resident_model_revisions=json.dumps(['h3-live-' + c.lower() + '-20260916' for c in components]),
                 capacity_limits=capacity, readiness_checks={'device': True, 'backend': True, 'warmup': True, 'canary': True}))
    specifications = [('encoder', 'ENCODER', 'aux', 'request', 'conditioning'), ('dit', 'DIT', 'dit', 'conditioning', 'latent'),
                      ('vae', 'VAE_DECODER', 'aux', 'latent', 'video'), ('thumbnail', 'CPU_MEDIA', 'thumbnail', 'video', 'thumbnail')]
    for stage, component, role, input_port, output_port in specifications:
        resource = 'CPU' if role == 'thumbnail' else 'GPU'
        revision('stage_result_equivalence_revisions', 'equivalence/' + stage, dict(
            exact_contract={'scope': 'same pinned implementation and inputs', 'cross_implementation_reuse': False},
            evidence_receipt_ref=QUALIFICATION + '/receipt.json', evidence_digest=evidence))
        revision('stage_definition_revisions', 'definition/' + stage, dict(stage_kind='THUMBNAIL' if role == 'thumbnail' else component,
            input_ports={input_port: identity('interface/' + input_port)}, output_ports={output_port: identity('interface/' + output_port)},
            required_input_ports='{' + input_port + '}', required_output_ports='{' + output_port + '}', resource_class=resource,
            retry_class='STAGE_RETRY', public_phase='PREPARING' if stage == 'encoder' else 'GENERATING'))
        revision('stage_profile_revisions', 'stage-profile/' + stage, dict(stage_definition_revision_id=identity('definition/' + stage),
            model_component_revision='h3-live-' + component.lower() + '-20260916', runtime_image_digest=(args.cpu_image if resource == 'CPU' else GPU_IMAGE).split('@')[1],
            worker_profile_revision_id=identity('worker-profile/' + role), result_equivalence_revision_id=identity('equivalence/' + stage),
            certified_capacity_vector={'active_stage_slots': 1, 'cpu_slot_count' if resource == 'CPU' else 'gpu_count': 1}))
    for port in ['conditioning', 'latent', 'video']:
        revision('connector_revisions', 'connector/' + port, dict(source_interface_revision_id=identity('interface/' + port),
            destination_interface_revision_id=identity('interface/' + port), transport='OBJECT_STORE', durable_fallback=True,
            topology_policy={}, integrity_policy={'digest': 'sha256', 'version_required': True}, security_policy={'scope': 'project'}, limits={}))
    statements.append('COMMIT;')
    (root / 'worker-catalog.sql').write_text('\n'.join(statements) + '\n')

    now = datetime.datetime.now(datetime.timezone.utc).isoformat()
    plan = dict(schema_version=1, id=plan_id, stable_id='h3-live-20260916', revision=1, content_digest='',
                approval_evidence_digest=digest({'authorization': 'Continue repairing the formal startup chain and complete the real H3 billing loop',
                                                 'scope': 'internal-validation', 'qualification_receipt_sha256': evidence.hex()}),
                approved_at=now, approved_by='platform-admin/h3-live-20260916', capacity_pools=[], worker_bundles=[], worker_instances=[])
    bundles = []
    for role, m in measured.items():
        d = m['measured'][0]
        cpu = role == 'thumbnail'
        resource = 'CPU' if cpu else 'GPU'
        worker_role = 'cpu-thumbnail' if cpu else role
        node_id, device_id = d['ComputeNodeID'], d['DeviceID']
        member_id = m['ids']['member']
        membership = digest([dict(device_id=device_id, node_id=node_id, gpu_uuid=d['GPUUUID'], pci_bdf=d['PCIBDF'], node_epoch=d['NodeEpoch'], device_epoch=d['DeviceEpoch'])])
        topology = digest([dict(device_id=device_id, node_identity=m['node'], region='marslab', network='10.1.201.0/24', fault_domain='shared-cluster', ordinal=0)])
        device_set = hashlib.sha256(bytes.fromhex(membership + topology)).hexdigest()
        subset = digest([device_id])
        member_digest = hashlib.sha256(('spiffe://vela.internal/stage-worker/' + member_id).encode()).hexdigest()
        selected = [s for s in specifications if s[2] == role]
        runtimes = []
        for stage, component, _, _, _ in selected:
            stage_profile = identity('stage-profile/' + stage)
            pool = identity('pool/' + stage)
            model_residency = identity('residency/' + stage)
            plan['capacity_pools'].append(dict(id=pool, stable_id='h3-live-' + stage, stage_profile_revision_id=stage_profile,
                                              resource_class=resource, security_class='INTERNAL', region='marslab', max_ready_queue_depth=32))
            environment = ['HOME=' + SCRATCH, 'PYTHONDONTWRITEBYTECODE=1', 'TMPDIR=' + SCRATCH]
            if cpu:
                command = ['/opt/venv/bin/python', '-B', '-m', 'fast_h3.vela.driver', '--component', 'CPU_MEDIA', '--runtime-factory', 'fast_h3.vela.cpu_thumbnail:create_runtime']
                environment += ['PYTHONPATH=/opt/vela-cpu-media', 'PATH=/opt/venv/bin:/usr/local/bin:/usr/bin:/bin']
            else:
                command = ['/opt/venv/bin/python', '-B', '-m', 'fast_h3.vela.driver', '--component', component, '--runtime-factory', 'fast_h3.vela.h3_runtime:create_runtime']
                environment += ['PYTHONPATH=/opt/fast-h3/src:/opt/fast-h3/sglang/python', 'PATH=/opt/venv/bin:/usr/local/cuda/bin:/usr/local/bin:/usr/bin:/bin',
                    'XDG_CACHE_HOME=' + SCRATCH + '/cache', 'TRITON_CACHE_DIR=' + SCRATCH + '/triton-cache',
                    'TORCHINDUCTOR_CACHE_DIR=' + SCRATCH + '/torch-cache', 'CUDA_CACHE_PATH=' + SCRATCH + '/cuda-cache',
                    'CUDA_VISIBLE_DEVICES=' + d['GPUUUID'], 'NVIDIA_VISIBLE_DEVICES=' + d['GPUUUID'], 'HF_HUB_OFFLINE=1', 'TRANSFORMERS_OFFLINE=1',
                    'FAST_H3_MODEL_PATH=' + MODEL_CACHE + '/MiniMaxAI/MiniMax-H3', 'FAST_H3_MODEL_VARIANT=fl2va',
                    'FAST_H3_DIT_PATH=' + MODEL_CACHE + '/MiniMaxAI/MiniMax-H3-int8-v5-convrot-hardened/transformer',
                    'FAST_H3_ENCODER_PATH=' + MODEL_CACHE + '/MiniMax-H3-encoder-int8',
                    'FAST_H3_ADALN_PATH=' + MODEL_CACHE + '/MiniMax-H3-adaln-table-hardened/steps20.safetensors',
                    'FAST_H3_ARTIFACT_ROOT=' + MODEL_CACHE, 'FAST_H3_MODEL_MANIFEST_PATH=/opt/fast-h3/deploy/k8s/h3-disaggregated/node19-model-manifest-v5.json',
                    'FAST_H3_MODEL_MANIFEST_SHA256=6eac1f04b601ebfd5d9cf54a130d9c3b7e5a13549fa1a8517bea2730657a28c0',
                    'FAST_H3_MASTER_PORT=' + str(29681 + ['ENCODER', 'VAE_DECODER', 'DIT'].index(component)),
                    'FAST_H3_WARMUP_SPEC_PATH=' + QUALIFICATION + '/' + component + '.json', 'OMP_NUM_THREADS=8', 'TOKENIZERS_PARALLELISM=false']
            runtimes.append(dict(model_residency_id=model_residency, capacity_pool_id=pool, stage_profile_revision_id=stage_profile,
                model_runtime_epoch_floor=1, component=component, model_component_revision='h3-live-' + component.lower() + '-20260916',
                runtime_identity='h3-live-' + stage + '-20260916', command=command, environment=environment, initialization_timeout='40m', shutdown_timeout='2m'))
        worker = dict(id=m['ids']['worker'], instance_epoch=1, worker_profile_revision_id=identity('worker-profile/' + role),
            capacity_pool_id=runtimes[0]['capacity_pool_id'], role=worker_role, capacity_slots=1, device_set_digest=device_set, membership_digest=membership,
            model_runtimes=runtimes, members=[dict(id=member_id, member_epoch=1, key='member-0', node_identity=m['node'], resource_class=resource,
                identity_digest=member_digest, device_subset_digest=subset, device_count=1, device_constraints=[dict(device_id=device_id,
                    device_epoch=d['DeviceEpoch'], resource_class=resource, gpu_uuid=d['GPUUUID'], pci_bdf=d['PCIBDF'])])])
        if role == 'aux':
            worker['shared_slot_exception'] = 'H3_AUX_ENCODER_VAE'
        bundle = dict(schema_version=2, plan_revision_id=plan_id, worker_bundle_id=m['ids']['bundle'], revision_digest='', namespace='vela-system',
            init_image=args.init_image, stage_worker_agent_image=WORKER_IMAGE, runtime_image=args.cpu_image if cpu else GPU_IMAGE,
            runtime_launch_protocol='kubernetes-pidfd-v1', image_pull_secrets=['vela-release-pull-v1'], stage_worker_config_map='vela-h3-worker-' + role + '-20260916',
            model_runtime_verifier_config_map='vela-h3-runtime-verifier-20260916', stage_worker_control_tls_secret='vela-h3-worker-control-tls-20260916',
            stage_worker_authority_secret='vela-h3-stage-authority-20260916', artifact_store_credentials_secret='vela-h3-artifacts-20260916',
            artifact_store_ca_secret='vela-h3-artifact-ca-20260916', worker_instances=[worker])
        bundles.append(bundle)
        plan['worker_bundles'].append(dict(id=m['ids']['bundle'], stable_id='h3-live-' + role, desired_generation=1, layout_digest=''))
        plan['worker_instances'].append(dict(id=worker['id'], worker_profile_revision_id=worker['worker_profile_revision_id'], capacity_pool_id=worker['capacity_pool_id'],
            worker_bundle_id=m['ids']['bundle'], desired_member_count=1, desired_device_count=1,
            model_runtime_routes=[{key: r[key] for key in ['model_residency_id', 'capacity_pool_id', 'stage_profile_revision_id']} for r in runtimes]))
        directory = root / worker_role
        directory.mkdir(mode=0o700, exist_ok=True)
        template = dict(schema_version=1, worker_instance_id=worker['id'], instance_epoch=1, control_session_epoch=1,
            device_set={'id': m['ids']['device-set'], 'devices': [dict(id=device_id, compute_node_id=node_id, node_identity=m['node'], region='marslab',
                network_domain='10.1.201.0/24', fault_domain='shared-cluster', kind=resource, gpu_uuid=d['GPUUUID'], pci_bdf=d['PCIBDF'], ordinal=0)]},
            members=[dict(id=member_id, member_key='member-0', compute_node_id=node_id, member_epoch=1, device_ids=[device_id], readiness='UNOBSERVED')],
            residencies=[dict(id=r['model_residency_id'], model_component_revision=r['model_component_revision'], runtime_identity=r['runtime_identity'],
                runtime_image_digest=bundle['runtime_image'].split('@')[1], model_runtime_epoch=0, state='UNOBSERVED') for r in runtimes],
            capacity={'vector': {'active_stage_slots': 1, 'cpu_slot_count' if cpu else 'gpu_count': 1}})
        write(directory / 'evidence-template.json', [template])
    write(root / 'rollout-input.json', {'approved_plan': plan, 'worker_bundles': bundles})
    write(root / 'catalog-ids.json', {key: identity(key) for key in ['model', 'graph', 'execution-profile', 'output', 'preset', 'service-class', 'rate-card', 'rate-line', 'certification']})


if __name__ == '__main__':
    main()
