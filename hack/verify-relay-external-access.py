#!/usr/bin/env python3
"""Read-only checks from the relay's actual host; never disables TLS verification.

Environment: VELA_BASE_URL, VELA_PROJECT_ID, VELA_API_KEY; VELA_CA_FILE optional.
Pass --job-id for detail/artifact checks and --download-directory for full media
integrity checks. Makes no submissions, cancellations, or configuration changes.
Output omits credentials, prompts and signed URLs; exit nonzero on any failure.
"""
import argparse
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import socket
import ssl
import urllib.error
import urllib.parse
import urllib.request
import uuid


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--job-id', type=uuid.UUID)
    parser.add_argument('--download-directory', type=Path)
    parser.add_argument('--output', type=Path, default=Path('vela-external-preflight.json'))
    args = parser.parse_args()
    os.umask(0o077)
    report = {'passed': False, 'at': datetime.datetime.now(datetime.timezone.utc).isoformat(),
              'python': platform.python_version(), 'openssl': ssl.OPENSSL_VERSION,
              'created_jobs': 0, 'checks': []}
    try:
        endpoint = os.environ['VELA_BASE_URL'].rstrip('/')
        project = str(uuid.UUID(os.environ['VELA_PROJECT_ID']))
        token = os.environ['VELA_API_KEY'].strip()
        parsed = urllib.parse.urlsplit(endpoint)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username is not None
                or parsed.password is not None or parsed.query or parsed.fragment):
            raise ValueError('VELA_BASE_URL must be an HTTPS API root without credentials or query')
        report['origin'] = parsed.scheme + '://' + parsed.netloc
        report['dns_addresses'] = sorted({a[4][0] for a in socket.getaddrinfo(parsed.hostname, parsed.port or 443)})
        ca = os.environ.get('VELA_CA_FILE') or os.environ.get('CURL_CA_BUNDLE')
        context = ssl.create_default_context(cafile=ca)
        report['verify_flags'] = context.verify_flags
        opener = urllib.request.build_opener(NoRedirect(), urllib.request.HTTPSHandler(context=context))
        jobs = '/v1/projects/' + project + '/jobs'

        def get(path, expected, authenticated=True):
            headers = {'Accept': 'application/json'}
            if authenticated:
                headers['Authorization'] = 'Bearer ' + token
            request = urllib.request.Request(endpoint + path, headers=headers)
            try:
                response = opener.open(request, timeout=30)
            except urllib.error.HTTPError as error:
                response = error
            with response:
                raw = response.read(1024 * 1024 + 1)
                if len(raw) > 1024 * 1024:
                    raise ValueError('API JSON exceeded 1 MiB')
                record = {'path': path, 'status': response.status,
                          'request_id': response.headers.get('X-Request-ID'),
                          'passed': response.status == expected}
                report['checks'].append(record)
                if response.status != expected:
                    raise ValueError('API returned HTTP ' + str(response.status) + ', expected ' + str(expected))
                return json.loads(raw)

        get(jobs, 401, authenticated=False)
        page = get(jobs + '?active=true&limit=100', 200)
        if not isinstance(page['jobs'], list) or any(j['project_id'] != project for j in page['jobs']):
            raise ValueError('Job list Project boundary mismatch')
        report['active_job_count_first_page'] = len(page['jobs'])
        if args.job_id:
            path = jobs + '/' + str(args.job_id)
            job = get(path, 200)
            if job['job_id'] != str(args.job_id) or job['project_id'] != project:
                raise ValueError('Job identity mismatch')
            report['job'] = {k: job[k] for k in ['job_id', 'project_id', 'model', 'state', 'pricing']}
            if job['state'] == 'SUCCEEDED':
                artifacts = get(path + '/artifacts', 200)
                if artifacts['job_id'] != str(args.job_id):
                    raise ValueError('ArtifactSet Job mismatch')
                report['artifact_count'] = len(artifacts['artifacts'])
                if args.download_directory:
                    args.download_directory.mkdir(mode=0o700, parents=True, exist_ok=True)
                    for index, artifact in enumerate(artifacts['artifacts']):
                        url = urllib.parse.urlsplit(artifact['download_url'])
                        if (url.scheme + '://' + url.netloc != report['origin'] or url.username is not None
                                or url.password is not None or url.fragment
                                or urllib.parse.parse_qs(url.query).get('versionId') != [artifact['object_version_id']]):
                            raise ValueError('Artifact URL must bind its exact version on the trusted API origin')
                        size = artifact['size_bytes']
                        if not isinstance(size, int) or not 0 < size <= 1024**3 or not re.fullmatch('[0-9a-f]{64}', artifact['sha256']):
                            raise ValueError('Artifact size/digest is outside verifier bounds')
                        target = args.download_directory / ('artifact-' + str(index) + '.bin')
                        digest, count = hashlib.sha256(), 0
                        try:
                            # No Authorization header is forwarded to storage.
                            with opener.open(artifact['download_url'], timeout=60) as response, target.open('wb') as output:
                                if response.status != 200:
                                    raise ValueError('Artifact did not return HTTP 200')
                                while chunk := response.read(min(1024 * 1024, size + 1 - count)):
                                    count += len(chunk)
                                    if count > size:
                                        raise ValueError('Artifact exceeded committed size')
                                    digest.update(chunk)
                                    output.write(chunk)
                        except urllib.error.URLError:
                            raise RuntimeError('Artifact download failed; signed URL omitted') from None
                        if count != size or digest.hexdigest() != artifact['sha256']:
                            raise ValueError('Artifact integrity mismatch')
                        report['checks'].append({'artifact_kind': artifact['kind'], 'bytes': count,
                                                 'sha256': digest.hexdigest(), 'passed': True})
            elif args.download_directory:
                raise ValueError('Requested Job has no completed media to verify')
        elif args.download_directory:
            raise ValueError('--download-directory requires --job-id')
        report['passed'] = True
    except Exception as error:
        report['error'] = {'type': type(error).__name__, 'message': str(error)}
    args.output.write_text(json.dumps(report, indent=2) + '\n')
    print(json.dumps(report, indent=2))
    return 0 if report['passed'] else 1


if __name__ == '__main__':
    raise SystemExit(main())
