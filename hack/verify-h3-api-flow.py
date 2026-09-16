#!/usr/bin/env python3
"""Verify a real H3 Job, complete AV download and exactly one durable Charge.

Run from a management host with read-only PostgreSQL inspection via kubectl.
The private run directory persists the idempotency key before the first POST,
so rerunning after a timeout resumes the same Job. No catalog or Worker facts
are invented. Failure writes a failed receipt and exits nonzero.
"""
import argparse
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import re
import ssl
import subprocess
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def read_json(path):
    return json.loads(Path(path).read_text())


def write_json(path, value):
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(value, indent=2) + '\n')
    temporary.replace(path)


def origin(value):
    parsed = urllib.parse.urlsplit(value)
    if (parsed.scheme != 'https' or not parsed.hostname or parsed.username is not None
            or parsed.password is not None or parsed.fragment or parsed.query):
        raise ValueError('API endpoint must use HTTPS without credentials, query or fragment')
    return parsed.scheme + '://' + parsed.netloc


def validate_job(job, project, expected_id=None, expected_pricing=None):
    job_id = str(uuid.UUID(job['job_id']))
    if job_id == str(uuid.UUID(int=0)) or job['project_id'] != project:
        raise ValueError('Job identity mismatch')
    if expected_id and job_id != expected_id:
        raise ValueError('idempotency replay created a different Job')
    if job['state'] not in {'QUEUED', 'ASSIGNED', 'RUNNING', 'FINALIZING', 'RETRY_WAIT',
                            'CANCELING', 'SUCCEEDED', 'FAILED', 'CANCELED'}:
        raise ValueError('unknown Job state')
    price = job['pricing']
    if (price['quantity'] != 1 or price['quoted_amount_minor'] <= 0
            or price['unit_amount_minor'] != price['quoted_amount_minor']):
        raise ValueError('billing acceptance requires a positive single-generation quote')
    if expected_pricing is not None and price != expected_pricing:
        raise ValueError('immutable PricingSnapshot changed')
    return job_id


def validate_billing(snapshot, job, artifact_set):
    if (snapshot['job_id'] != job['job_id'] or snapshot['project_id'] != job['project_id']
            or snapshot['state'] != 'SUCCEEDED' or not snapshot['billable_started_at']
            or snapshot['charge_count'] != 1 or snapshot['visible_completion_count'] != 1
            or snapshot['artifact_set_count'] != 1 or snapshot['artifact_set_id'] != artifact_set
            or snapshot['idempotency_count'] != 1 or snapshot['reservation_state'] != 'CONSUMED'):
        raise ValueError('durable Job/Visible Completion/Charge cardinality or state mismatch')
    quote = job['pricing']
    charge = snapshot['charge']
    if (charge['reason'] != 'VISIBLE_COMPLETION' or charge['state'] != 'POSTED'
            or charge['amount_minor'] != quote['quoted_amount_minor']
            or charge['currency'] != quote['currency']
            or snapshot['reservation_amount_minor'] != quote['quoted_amount_minor']):
        raise ValueError('Charge does not match the frozen quote')
    if (snapshot['account_reserved_minor'] != snapshot['sum_reserved_minor']
            or snapshot['account_reserved_minor'] < 0):
        raise ValueError('credit reservation balance does not reconcile')


def billing_snapshot(kubectl, namespace, job_id):
    # UUID validation precedes interpolation; no caller-supplied SQL is accepted.
    job_id = str(uuid.UUID(job_id))
    def run(arguments):
        result = subprocess.run([kubectl, '--request-timeout=20s', '-n', namespace] + arguments,
                                capture_output=True, text=True, timeout=30)
        if result.returncode:
            raise RuntimeError('read-only PostgreSQL evidence query failed')
        return result.stdout
    cluster = json.loads(run(['get', 'cluster', 'vela-postgres', '-o', 'json']))
    primary = cluster['status']['currentPrimary']
    sql = f"""SELECT json_build_object(
      'job_id', j.id, 'project_id', j.project_id, 'state',j.state,
      'billable_started_at',j.billable_started_at,
      'charge_count',(SELECT count(*) FROM charges WHERE job_id=j.id),
      'charge',(SELECT row_to_json(c) FROM charges c WHERE c.job_id=j.id),
      'visible_completion_count',(SELECT count(*) FROM visible_completions WHERE job_id=j.id),
      'artifact_set_count',(SELECT count(*) FROM artifact_sets WHERE job_id=j.id),
      'artifact_set_id',(SELECT id FROM artifact_sets WHERE job_id=j.id),
      'idempotency_count',(SELECT count(*) FROM idempotency_results WHERE job_id=j.id),
      'reservation_state',r.state,'reservation_amount_minor',r.amount_minor,
      'account_reserved_minor',a.reserved_minor,
      'sum_reserved_minor',(SELECT coalesce(sum(amount_minor),0) FROM credit_reservations WHERE organization_id=j.organization_id AND state='RESERVED')
    ) FROM jobs j JOIN credit_reservations r ON r.job_id=j.id
      JOIN organization_credit_accounts a ON a.organization_id=j.organization_id
      WHERE j.id='{job_id}'::uuid;"""
    return json.loads(run(['exec', primary, '-c', 'postgres', '--', 'psql', '-U', 'postgres',
                          '-d', 'app', '-XAt', '-v', 'ON_ERROR_STOP=1', '-c', sql]))


class Client:
    def __init__(self, endpoint, token, ca, download_origins=()):
        self.endpoint = endpoint.rstrip('/')
        self.allowed_origins = {origin(endpoint)}
        for value in download_origins:
            if urllib.parse.urlsplit(value).path not in {"", "/"}:
                raise ValueError("download origin must not include a path")
            self.allowed_origins.add(origin(value))
        self.token = token
        self.opener = urllib.request.build_opener(
            NoRedirect(), urllib.request.HTTPSHandler(context=ssl.create_default_context(cafile=ca)))

    def api(self, method, path, body=None, key=None, authenticated=True):
        headers = {'Accept': 'application/json'}
        if authenticated:
            headers['Authorization'] = 'Bearer ' + self.token
        if key:
            headers['Idempotency-Key'] = key
        if body is not None:
            headers['Content-Type'] = 'application/json'
        request = urllib.request.Request(self.endpoint + path, method=method, headers=headers,
                                         data=json.dumps(body).encode() if body is not None else None)
        try:
            response = self.opener.open(request, timeout=30)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            wire = response.read(1024 * 1024 + 1)
            if len(wire) > 1024 * 1024:
                raise ValueError('API response exceeds the JSON bound')
            return response.status, json.loads(wire)

    def download(self, artifact, target):
        url = urllib.parse.urlsplit(artifact['download_url'])
        # The API bearer is never forwarded to the object store or a redirect.
        if (url.scheme + '://' + url.netloc not in self.allowed_origins or url.username is not None
                or url.fragment or url.scheme != 'https'):
            raise ValueError('Artifact download bypasses the configured API gateway')
        if urllib.parse.parse_qs(url.query).get('versionId') != [artifact['object_version_id']]:
            raise ValueError('download URL does not bind the committed object version')
        size = artifact['size_bytes']
        if not isinstance(size, int) or not 0 < size <= 1024**3:
            raise ValueError('Artifact size is outside this runner\'s 1GiB bound')
        if not re.fullmatch('[0-9a-f]{64}', artifact['sha256']):
            raise ValueError('invalid Artifact digest')
        digest, count = hashlib.sha256(), 0
        try:
            with self.opener.open(artifact['download_url'], timeout=60) as response, target.open('wb') as output:
                if response.status != 200:
                    raise ValueError('Artifact download did not return HTTP 200')
                while chunk := response.read(min(1024 * 1024, size + 1 - count)):
                    count += len(chunk)
                    if count > size:
                        raise ValueError('Artifact exceeds its committed size')
                    digest.update(chunk)
                    output.write(chunk)
        except urllib.error.URLError:
            raise RuntimeError('Artifact download failed; signed URL omitted') from None
        if count != size or digest.hexdigest() != artifact['sha256']:
            raise ValueError('Artifact size or SHA256 differs from committed metadata')
        return {k: v for k, v in artifact.items() if not k.startswith('download_url')}


def verify_media(ffprobe, path, metadata, frames, audio_ms):
    result = subprocess.run([ffprobe, '-v', 'error', '-count_frames', '-show_streams',
                             '-show_format', '-of', 'json', str(path)],
                            capture_output=True, text=True, timeout=90)
    if result.returncode or result.stderr.strip():
        raise RuntimeError('ffprobe could not decode the downloaded video cleanly')
    media = json.loads(result.stdout)
    validate_media(media, metadata, frames, audio_ms)
    return media


def validate_media(media, metadata, frames=124, audio_ms=5175):
    # This acceptance SKU is frozen to H3 native 1344x768, requested 5s at 24fps.
    # Integer milliseconds match the server receipt; fractions also reject NaN.
    def milliseconds(value):
        seconds = Fraction(value)
        if seconds < 0:
            raise ValueError('negative media duration')
        return int(seconds * 1000 + Fraction(1, 2))

    video = [s for s in media['streams'] if s['codec_type'] == 'video']
    audio = [s for s in media['streams'] if s['codec_type'] == 'audio']
    if len(media['streams']) != 2 or len(video) != 1 or len(audio) != 1:
        raise ValueError('complete H3 output requires exactly one video and one audio stream')
    v, a = video[0], audio[0]
    expected_video_ms = int(Fraction(frames * 1000, 24) + Fraction(1, 2))
    video_ms, actual_audio_ms = milliseconds(v['duration']), milliseconds(a['duration'])
    container_ms = milliseconds(media['format']['duration'])
    if (v['width'] != 1344 or v['height'] != 768 or v['codec_name'] != 'h264'
            or int(v['nb_read_frames']) != frames or metadata['frame_count'] != frames
            or Fraction(v['avg_frame_rate']) != 24 or Fraction(v['r_frame_rate']) != 24
            or metadata['frame_rate_milli'] != 24000 or video_ms != expected_video_ms
            or metadata['duration_milliseconds'] != video_ms
            or metadata['requested_duration_milliseconds'] != 5000
            or Fraction(v['start_time']) != 0 or Fraction(a['start_time']) != 0
            or 'mp4' not in media['format']['format_name'].split(',')):
        raise ValueError('video dimensions, frames, timing or API metadata differ from the H3 SKU')
    if (a['codec_name'] != 'aac' or int(a['sample_rate']) != 32000 or a['channels'] != 2
            or not audio_ms <= actual_audio_ms <= audio_ms + 32
            or metadata['audio'] != {'codec': 'aac', 'sample_rate': 32000, 'channels': 2,
                                     'duration_milliseconds': actual_audio_ms}
            or not max(video_ms, actual_audio_ms) <= container_ms <= max(video_ms, actual_audio_ms) + 1
            or metadata['container_duration_milliseconds'] != container_ms):
        raise ValueError('full audio, terminal AAC padding or API media metadata mismatch')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--api-url', required=True, help='HTTPS gateway base including /api')
    parser.add_argument('--credential-file', required=True)
    parser.add_argument('--ca', required=True)
    parser.add_argument('--project-id', required=True)
    parser.add_argument('--request', required=True)
    parser.add_argument('--run-directory', required=True)
    parser.add_argument('--timeout', type=int, default=2400)
    parser.add_argument('--expected-frames', type=int, choices=[124], default=124)
    parser.add_argument('--expected-audio-ms', type=int, choices=[5175], default=5175)
    parser.add_argument('--download-origin', action='append', default=[],
                        help='explicitly trusted HTTPS APISIX origin, e.g. https://10.1.201.70:30443')
    parser.add_argument('--ffprobe', default='ffprobe')
    parser.add_argument('--kubectl', default='kubectl')
    parser.add_argument('--namespace', default='vela-system')
    args = parser.parse_args()
    os.umask(0o077)
    root = Path(args.run_directory)
    root.mkdir(mode=0o700, parents=True, exist_ok=True)
    receipt = {'schema_version': 1, 'passed': False, 'production_gate_evidence': False,
               'checks': [], 'scope': 'real-job-video-and-durable-charge'}
    try:
        project = str(uuid.UUID(args.project_id))
        request = read_json(args.request)
        request_hash = hashlib.sha256(json.dumps(request, sort_keys=True).encode()).hexdigest()
        if (args.timeout <= 0 or args.timeout > 7200 or args.expected_frames <= 0
                or args.expected_audio_ms <= 0 or request.get('generation_count') != 1
                or request.get('output_spec') != 'h3-native-av-1344x768-5s-24fps'):
            raise ValueError('invalid time, media or single-generation request bounds')
        state_path = root / 'state.json'
        if state_path.exists():
            state = read_json(state_path)
            if state['request_sha256'] != request_hash or state['project_id'] != project or state['api_url'] != args.api_url:
                raise ValueError('run directory belongs to a different request or endpoint')
        else:
            state = {'request_sha256': request_hash, 'project_id': project, 'api_url': args.api_url,
                     'idempotency_key': 'h3-api-acceptance-' + str(uuid.uuid4())}
            write_json(state_path, state)
        credential_path = Path(args.credential_file)
        if credential_path.stat().st_mode & 0o077:
            raise ValueError('credential file must be private')
        token = credential_path.read_text().strip()
        if not re.fullmatch(r'vla_[0-9a-f-]{36}\.[A-Za-z0-9_-]{43}', token):
            raise ValueError('invalid acceptance service credential format')
        client = Client(args.api_url, token, args.ca, args.download_origin)
        jobs = '/v1/projects/' + project + '/jobs'
        status, _ = client.api('GET', jobs + '/' + str(uuid.uuid4()), authenticated=False)
        if status != 401:
            raise ValueError('anonymous API access did not return 401')
        receipt['checks'].append('anonymous_api_denied')
        status, job = client.api('POST', jobs, request, state['idempotency_key'])
        receipt['submission'] = {'http_status': status, 'response': job}
        if status != 202:
            raise RuntimeError('Job submission rejected: HTTP ' + str(status) + ' ' + str(job.get('code')))
        job_id = validate_job(job, project, state.get('job_id'), state.get('pricing'))
        state.update(job_id=job_id, pricing=job['pricing'])
        write_json(state_path, state)
        receipt['job_id'] = job_id
        for _ in range(2):
            status, replay = client.api('POST', jobs, request, state['idempotency_key'])
            if status != 202:
                raise ValueError('same-key replay was rejected')
            validate_job(replay, project, job_id, state['pricing'])
        changed = dict(request, prompt=request['prompt'] + ' Different request.')
        status, _ = client.api('POST', jobs, changed, state['idempotency_key'])
        if status != 409:
            raise ValueError('same-key different payload did not return 409')
        receipt['checks'].append('idempotent_submission')
        deadline = time.monotonic() + args.timeout
        last_state = None
        while True:
            status, job = client.api('GET', jobs + '/' + job_id)
            if status != 200:
                raise ValueError('accepted Job is not readable')
            validate_job(job, project, job_id, state['pricing'])
            write_json(root / 'job.json', job)
            if job['state'] != last_state:
                print('Job', job_id, job['state'], flush=True)
                last_state = job['state']
            if job['state'] in {'SUCCEEDED', 'FAILED', 'CANCELED'}:
                break
            if time.monotonic() >= deadline:
                raise TimeoutError('Job did not complete; rerun with the same directory to resume')
            time.sleep(5)
        if job['state'] != 'SUCCEEDED':
            raise ValueError('Job terminated without successful video delivery')
        status, artifacts = client.api('GET', jobs + '/' + job_id + '/artifacts')
        if status != 200 or artifacts['job_id'] != job_id or not artifacts['artifacts']:
            raise ValueError('completed Job has no accessible ArtifactSet')
        verified, seen, videos = [], set(), 0
        for artifact in artifacts['artifacts']:
            identity = (artifact['kind'], artifact['ordinal'])
            if identity in seen or artifact['ordinal'] != 0 or artifact['kind'] not in {'VIDEO', 'THUMBNAIL'}:
                raise ValueError('unexpected or repeated Artifact')
            seen.add(identity)
            target = root / ('complete-video.mp4' if artifact['kind'] == 'VIDEO' else 'thumbnail.webp')
            verified.append(client.download(artifact, target))
            if artifact['kind'] == 'VIDEO':
                videos += 1
                media = verify_media(args.ffprobe, target, artifact['media'], args.expected_frames, args.expected_audio_ms)
                write_json(root / 'ffprobe.json', media)
        if videos != 1 or seen != {('VIDEO', 0), ('THUMBNAIL', 0)}:
            raise ValueError('expected one full video and its thumbnail')
        receipt['artifacts'] = verified
        before = billing_snapshot(args.kubectl, args.namespace, job_id)
        validate_billing(before, job, artifacts['artifact_set_id'])
        status, replay = client.api('POST', jobs, request, state['idempotency_key'])
        if status != 202:
            raise ValueError('terminal Job idempotency replay was rejected')
        validate_job(replay, project, job_id, state['pricing'])
        after = billing_snapshot(args.kubectl, args.namespace, job_id)
        validate_billing(after, job, artifacts['artifact_set_id'])
        if before['charge'] != after['charge']:
            raise ValueError('replay changed the durable Charge')
        receipt.update(passed=True, billing=after)
        receipt['checks'] += ['complete_video_and_audio_download', 'one_durable_charge', 'no_duplicate_charge_on_replay']
    except Exception as error:
        receipt['error'] = str(error)
    finally:
        write_json(root / 'receipt.json', receipt)
    print(json.dumps(receipt, indent=2))
    return 0 if receipt['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
