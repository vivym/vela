#!/usr/bin/env python3
"""Verify the external H3 Job API through both configured HTTPS gateways.

Uses a dedicated acceptance Project; the optional peer credential is read-only.
State and idempotency keys are saved before POST so interrupted runs can resume.
Only the campaign's cancellation Job may be canceled. No cluster mutations.
Credentials and signed download URLs are excluded from the public receipt.
"""
import argparse
import copy
import concurrent.futures
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--flow-helper', required=True)
    parser.add_argument('--api-url', required=True)
    parser.add_argument('--gateways', nargs='+', required=True)
    parser.add_argument('--ca', required=True)
    parser.add_argument('--credential-file', required=True)
    parser.add_argument('--project-id', required=True)
    parser.add_argument('--peer-credential-file', required=True)
    parser.add_argument('--peer-project-id', required=True)
    parser.add_argument('--request', required=True)
    parser.add_argument('--run-directory', required=True)
    parser.add_argument('--kubectl', default='kubectl')
    parser.add_argument('--kubeconfig', required=True)
    parser.add_argument('--timeout', type=int, default=2400)
    args = parser.parse_args()
    os.umask(0o077)
    os.environ['KUBECONFIG'] = args.kubeconfig
    root = Path(args.run_directory)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    spec = importlib.util.spec_from_file_location('flow', args.flow_helper)
    flow = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(flow)
    hostname = urllib.parse.urlsplit(args.api_url).hostname
    original_resolver = socket.getaddrinfo
    target = [args.gateways[0]]
    dns = {}
    try:
        dns['addresses'] = sorted({x[4][0] for x in original_resolver(hostname, 443)})
    except socket.gaierror as error:
        dns['error'] = str(error)
    socket.getaddrinfo = lambda host, port, *a, **kw: original_resolver(
        target[0] if host == hostname else host, port, *a, **kw)
    token = Path(args.credential_file).read_text().strip()
    peer_token = Path(args.peer_credential_file).read_text().strip()
    client = flow.Client(args.api_url, token, args.ca)
    request = json.loads(Path(args.request).read_text())
    project = str(uuid.UUID(args.project_id))
    peer_project = str(uuid.UUID(args.peer_project_id))
    jobs = '/v1/projects/' + project + '/jobs'
    peer_jobs = '/v1/projects/' + peer_project + '/jobs'
    state_path = root / 'state.json'
    request_digest = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
    if state_path.exists():
        state = json.loads(state_path.read_text())
        assert state['request_sha256'] == request_digest and state['project'] == project
    else:
        state = {'project': project, 'request_sha256': request_digest,
                 'keys': {n: 'api-surface-' + n + '-' + str(uuid.uuid4())
                          for n in ['success', 'cancel']}}
    flow.write_json(state_path, state)
    receipt = {'passed': False, 'started_at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
               'scope': 'external-h3-job-api', 'dns': dns, 'checks': [], 'failures': [],
               'production_gate_evidence': False}
    last_call = [0.0]

    def call(method, path, body=None, key=None, credential=token, raw=None, content_type='application/json'):
        # Stay below the documented shared per-source-IP gateway request budget.
        time.sleep(max(0, 0.9 - (time.monotonic() - last_call[0])))
        last_call[0] = time.monotonic()
        headers = {'Accept': 'application/json'}
        if credential is not None:
            headers['Authorization'] = 'Bearer ' + credential
        if key is not None:
            headers['Idempotency-Key'] = key
        data = raw if raw is not None else (json.dumps(body).encode() if body is not None else None)
        if data is not None:
            headers['Content-Type'] = content_type
        req = urllib.request.Request(args.api_url.rstrip('/') + path, method=method, headers=headers, data=data)
        start = time.monotonic()
        try:
            response = client.opener.open(req, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            wire = response.read(1024 * 1024 + 1)
            assert len(wire) <= 1024 * 1024
            try:
                body = json.loads(wire)
            except ValueError:
                body = {'non_json': True}
            return response.status, body, {'request_id': response.headers.get('X-Request-ID'),
                'content_type': response.headers.get('Content-Type'),
                'elapsed_seconds': round(time.monotonic() - start, 3)}

    def check(name, method, path, expected, **kw):
        status, body, headers = call(method, path, **kw)
        ok = status in ([expected] if isinstance(expected, int) else expected)
        record = {'name': name, 'gateway': target[0], 'status': status, 'passed': ok, **headers}
        if status >= 400:
            record['error_code'] = body.get('code')
            if not body.get('code') or not body.get('message'):
                record['passed'] = False
        receipt['checks'].append(record)
        if not record['passed']:
            receipt['failures'].append(record)
        flow.write_json(root / 'receipt.json', receipt)
        print(name, target[0], status, 'PASS' if record['passed'] else 'FAIL', flush=True)
        return body

    def submit(name):
        body = copy.deepcopy(request)
        body['prompt'] = request['prompt'] + ' Unique campaign ' + state['keys'][name][-8:] + '.'
        body['client_metadata'] = {'campaign': 'api-surface', 'scenario': name}
        job = check(name + '_submit', 'POST', jobs, 202, body=body, key=state['keys'][name])
        assert 'job_id' in job, 'campaign submission rejected'
        flow.validate_job(job, project, state.get(name, {}).get('job_id'))
        state[name] = {'job_id': job['job_id'], 'request': body, 'pricing': job['pricing']}
        flow.write_json(state_path, state)
        return job

    def sql(query):
        k = [args.kubectl, '-n', 'vela-system']
        primary = json.loads(subprocess.check_output(k + ['get', 'cluster', 'vela-postgres', '-o', 'json']))['status']['currentPrimary']
        return subprocess.check_output(k + ['exec', '-i', primary, '-c', 'postgres', '--', 'psql', '-U', 'postgres', '-d', 'app', '-XAt', '-v', 'ON_ERROR_STOP=1'], input=query, text=True, timeout=30).strip()

    def invariant():
        return json.loads(sql("select json_build_object('jobs',(select count(*) from jobs where project_id='" + project + "'),'idempotency',(select count(*) from idempotency_results where project_id='" + project + "'),'reservations',(select count(*) from credit_reservations where project_id='" + project + "'));"))

    try:
        success = submit('success')
        success_id = success['job_id']
        check('pending_artifacts_unavailable', 'GET', jobs + '/' + success_id + '/artifacts', 404)
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(client.api, 'POST', jobs, state['success']['request'], state['keys']['success']) for _ in range(4)]
            for future in futures:
                status, replay = future.result()
                assert status == 202
                flow.validate_job(replay, project, success_id, state['success']['pricing'])
        receipt['concurrent_idempotency_replay'] = {'requests': 4, 'distinct_jobs': 1, 'passed': True}
        for gateway in args.gateways:
            target[0] = gateway
            for path in [jobs, jobs + '/' + success_id, jobs + '/' + success_id + '/artifacts']:
                check('anonymous_denied', 'GET', path, 401, credential=None)
                check('invalid_credential_denied', 'GET', path, 401, credential='invalid')
            check('anonymous_submit_denied', 'POST', jobs, 401, credential=None, body=request, key='anonymous-test')
            check('anonymous_cancel_denied', 'POST', jobs + '/' + success_id + '/cancel', 401, credential=None)
            for method, suffix in [('GET', ''), ('GET', '/' + success_id), ('GET', '/' + success_id + '/artifacts'), ('POST', '/' + success_id + '/cancel')]:
                check('cross_project_denied', method, jobs + suffix,
                      404 if method == 'GET' and suffix else 403, credential=peer_token)
            check('cross_project_submit_denied', 'POST', jobs, 403, credential=peer_token, body=request, key='cross-project-test')
            check('peer_jobs_read', 'GET', peer_jobs + '?active=true', 200, credential=peer_token)
            for query in ['', '?active=true', '?state=QUEUED', '?state=RUNNING', '?state=SUCCEEDED', '?state=CANCELED', '?active=false&limit=100']:
                page = check('list' + query, 'GET', jobs + query, 200)
                assert isinstance(page['jobs'], list)
                assert all(j['project_id'] == project and j.get('model') and 'prompt' not in j and 'client_metadata' not in j for j in page['jobs'])
                if query == '?active=true':
                    assert all(j['state'] not in ['SUCCEEDED', 'FAILED', 'CANCELED'] for j in page['jobs'])
                    current = check('active_detail', 'GET', jobs + '/' + success_id, 200)
                    if current['state'] not in ['SUCCEEDED', 'FAILED', 'CANCELED']:
                        assert success_id in {j['job_id'] for j in page['jobs']}
                if query.startswith('?state='):
                    assert all(j['state'] == query.split('=')[1] for j in page['jobs'])
            for query in ['?limit=0', '?limit=101', '?limit=abc', '?cursor=bad', '?state=INVALID', '?active=maybe', '?active=true&state=SUCCEEDED']:
                check('invalid_list' + query, 'GET', jobs + query, 400)
            missing = str(uuid.uuid4())
            for method, suffix in [('GET', ''), ('GET', '/artifacts'), ('POST', '/cancel')]:
                check('unknown_job', method, jobs + '/' + missing + suffix, 404)
            check('wrong_project_job_hidden', 'GET', peer_jobs + '/' + success_id, 404, credential=peer_token)
            # Page through stable terminal records; active pagination is a live view.
            cursor, seen, ordering = None, [], []
            for _ in range(200):
                path = jobs + '?state=SUCCEEDED&limit=1' + ('&cursor=' + urllib.parse.quote(cursor, safe='') if cursor else '')
                page = check('terminal_pagination', 'GET', path, 200)
                assert len(page['jobs']) <= 1
                for item in page['jobs']:
                    assert item['job_id'] not in seen
                    seen.append(item['job_id']); ordering.append((item['created_at'], item['job_id']))
                    detail = check('terminal_detail_matches_list', 'GET', jobs + '/' + item['job_id'], 200)
                    assert item == detail
                cursor = page.get('next_cursor')
                if not cursor:
                    break
                check('cursor_filter_binding', 'GET', jobs + '?state=CANCELED&cursor=' + urllib.parse.quote(cursor, safe=''), 400)
            else:
                raise AssertionError('pagination did not terminate')
            assert len(seen) >= 2 and ordering == sorted(ordering, reverse=True)
            before = invariant()
            bad = [('unknown_model', dict(request, model='model-does-not-exist')),
                   ('unknown_sku', dict(request, output_spec='not-a-certified-sku')),
                   ('empty_prompt', dict(request, prompt='')),
                   ('invalid_count', dict(request, generation_count=0)),
                   ('unsupported_service', dict(request, service_class='invalid'))]
            no_sampling = copy.deepcopy(request); no_sampling['h3'].pop('sampling', None)
            bad.append(('missing_sampling', no_sampling))
            for name, body in bad:
                check(name, 'POST', jobs, 400, body=body, key='negative-' + str(uuid.uuid4()))
            check('missing_idempotency_key', 'POST', jobs, 400, body=request)
            check('invalid_idempotency_key', 'POST', jobs, 400, body=request, key='bad key')
            check('malformed_json', 'POST', jobs, 400, raw=b'{', key='negative-' + str(uuid.uuid4()))
            assert invariant() == before, 'rejected request created durable side effects'
            replay = check('accepted_replay', 'POST', jobs, 202, body=state['success']['request'], key=state['keys']['success'])
            flow.validate_job(replay, project, success_id, state['success']['pricing'])
            check('changed_request_conflict', 'POST', jobs, 409, body=dict(state['success']['request'], prompt='different'), key=state['keys']['success'])
        cancelled = submit('cancel')
        cancel_id = cancelled['job_id']
        # Cancel only our second Job, during DiT execution.
        deadline = time.monotonic() + 900
        while time.monotonic() < deadline:
            job = check('cancel_job_progress', 'GET', jobs + '/' + cancel_id, 200)
            phase = sql("select count(*) from stage_attempts s join stage_runs r on r.id=s.stage_run_id join attempts a on a.id=r.attempt_id where a.job_id='" + str(uuid.UUID(cancel_id)) + "' and r.stage_key='dit' and s.state='RUNNING';")
            if phase == '1' or job['state'] in ['CANCELED', 'SUCCEEDED', 'FAILED']:
                break
            time.sleep(5)
        assert job['state'] not in ['SUCCEEDED', 'FAILED'], 'cancellation test missed running window'
        canceled_worker = sql("select l.worker_instance_id from stage_allocations l join stage_runs r on r.id=l.stage_run_id join attempts a on a.id=r.attempt_id where a.job_id='" + str(uuid.UUID(cancel_id)) + "' and r.stage_key='dit' and l.state='ALLOCATED';")
        assert canceled_worker and str(uuid.UUID(canceled_worker)) == canceled_worker
        canceled_at = sql('select clock_timestamp();')
        cancellation = check('running_cancel', 'POST', jobs + '/' + cancel_id + '/cancel', 200)
        for gateway in args.gateways:
            target[0] = gateway
            replay = check('cancel_replay', 'POST', jobs + '/' + cancel_id + '/cancel', 200)
            assert replay == cancellation
            check('canceled_artifacts_unavailable', 'GET', jobs + '/' + cancel_id + '/artifacts', 404)
        recovery_deadline = time.monotonic() + 360
        while time.monotonic() < recovery_deadline:
            recovery = sql("select count(*) from worker_instances w join lateral(select * from capacity_observations c where c.worker_instance_id=w.id order by observation_sequence desc limit 1)c on true where w.id='" + canceled_worker + "' and w.lifecycle_state='READY' and w.reachability_state='CONNECTED' and c.observed_at>'" + canceled_at + "'::timestamptz and c.expires_at>clock_timestamp() and coalesce(c.capacity_vector->>'concurrency',c.capacity_vector->>'active_stage_slots')::int>0 and not exists(select 1 from stage_allocations a where a.worker_instance_id=w.id and a.state!='RELEASED');")
            if recovery == '1':
                break
            time.sleep(5)
        assert recovery == '1', 'canceled Worker did not restore fresh capacity'
        receipt['canceled_worker_recovery'] = {'worker': canceled_worker, 'fresh_positive_capacity': True}
        receipt['cancellation'] = cancellation
        deadline = time.monotonic() + args.timeout
        observations = []
        while time.monotonic() < deadline:
            target[0] = args.gateways[len(observations) % len(args.gateways)]
            job = check('poll_success', 'GET', jobs + '/' + success_id, 200)
            flow.validate_job(job, project, success_id, state['success']['pricing'])
            observations.append({k: job[k] for k in ['job_id', 'state', 'phase', 'phase_progress', 'progress_updated_at'] if k in job})
            if job['state'] in ['SUCCEEDED', 'FAILED', 'CANCELED']:
                break
            page = check('active_job_discoverable', 'GET', jobs + '?active=true', 200)
            if success_id not in {j['job_id'] for j in page['jobs']}:
                current = check('active_terminal_race', 'GET', jobs + '/' + success_id, 200)
                assert current['state'] in ['SUCCEEDED', 'FAILED', 'CANCELED']
            time.sleep(8)
        assert job['state'] == 'SUCCEEDED', 'fresh generation failed or timed out'
        receipt['progress_observations'] = observations
        receipt['success_job'] = job
        for gateway in args.gateways:
            target[0] = gateway
            artifact_set = check('artifacts', 'GET', jobs + '/' + success_id + '/artifacts', 200)
            assert {(a['kind'], a['ordinal']) for a in artifact_set['artifacts']} == {('VIDEO', 0), ('THUMBNAIL', 0)}
            for artifact in artifact_set['artifacts']:
                path = root / (gateway + ('-complete-video.mp4' if artifact['kind'] == 'VIDEO' else '-thumbnail.webp'))
                clean = client.download(artifact, path)
                if artifact['kind'] == 'VIDEO':
                    probe = flow.verify_media('ffprobe', path, artifact['media'], 124, 5175)
                    flow.write_json(root / (gateway + '-ffprobe.json'), probe)
                    # Decode both streams completely, not just container metadata.
                    subprocess.run(['ffmpeg', '-v', 'error', '-xerror', '-i', str(path), '-map', '0:v', '-map', '0:a', '-f', 'null', '-'], capture_output=True, check=True, timeout=120)
                receipt.setdefault('downloads', []).append({'gateway': gateway, **clean})
            replay = check('terminal_replay', 'POST', jobs, 202, body=state['success']['request'], key=state['keys']['success'])
            flow.validate_job(replay, project, success_id, state['success']['pricing'])
            terminal_cancel = check('successful_job_cancel_is_noop', 'POST', jobs + '/' + success_id + '/cancel', 200)
            assert terminal_cancel['state'] == 'SUCCEEDED'
            page = check('terminal_not_active', 'GET', jobs + '?active=true', 200)
            assert success_id not in {j['job_id'] for j in page['jobs']} and cancel_id not in {j['job_id'] for j in page['jobs']}
        billing = flow.billing_snapshot(args.kubectl, 'vela-system', success_id)
        flow.validate_billing(billing, job, artifact_set['artifact_set_id'])
        canceled_billing = flow.billing_snapshot(args.kubectl, 'vela-system', cancel_id)
        assert canceled_billing['state'] == 'CANCELED' and canceled_billing['charge_count'] == 1
        assert canceled_billing['charge']['reason'] == 'CUSTOMER_CANCELLATION'
        assert canceled_billing['charge']['amount_minor'] == state['cancel']['pricing']['quoted_amount_minor']
        assert canceled_billing['artifact_set_count'] == 0 and canceled_billing['visible_completion_count'] == 0
        receipt['billing'] = billing
        receipt['canceled_billing'] = canceled_billing
        receipt['passed'] = not receipt['failures']
    except Exception as error:
        receipt['failures'].append({'exception_type': type(error).__name__, 'message': str(error)[:1000]})
        print('FAILED', type(error).__name__, str(error)[:300], flush=True)
    finally:
        receipt['finished_at'] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        flow.write_json(root / 'receipt.json', receipt)
        socket.getaddrinfo = original_resolver
    return 0 if receipt['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
