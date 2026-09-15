#!/usr/bin/env python3
"""Select a CA-verified RKE2 supervisor before the agent's first connection.

The node join token is never read or sent by this selector. RKE2 performs its
normal authenticated bootstrap after reading the resulting server override.
Failure preserves the last override and exits nonzero so systemd can retry.
"""
import argparse
import ipaddress
import json
import os
from pathlib import Path
import ssl
import stat
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request


def endpoints(path):
    values = json.loads(Path(path).read_text())
    if not isinstance(values, list) or not 1 <= len(values) <= 8:
        raise ValueError('expected one to eight supervisor endpoints')
    result = []
    for value in values:
        if not isinstance(value, str):
            raise ValueError('endpoint must be a string')
        url = urllib.parse.urlsplit(value)
        if (url.scheme != 'https' or url.username or url.password or
                url.path or url.query or url.fragment or not url.port):
            raise ValueError('endpoint must be an HTTPS IP origin with explicit port')
        ipaddress.ip_address(url.hostname)
        if value in result:
            raise ValueError('duplicate endpoint')
        result.append(value)
    return result


def select(values, ca_file, timeout):
    context = ssl.create_default_context(cafile=ca_file)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    # Never send internal bootstrap probes through a process-wide HTTP proxy.
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}),
                                        urllib.request.HTTPSHandler(context=context))
    attempts = []
    for endpoint in values:
        start = time.monotonic()
        try:
            with opener.open(endpoint + '/ping', timeout=timeout) as response:
                if response.status != 200 or response.read(32).strip() != b'pong':
                    raise ValueError('unexpected supervisor ping response')
            # Both requests validate hostname and the distributed cluster CA.
            with opener.open(endpoint + '/cacerts', timeout=timeout) as response:
                data = response.read(65537)
                if response.status != 200 or len(data) > 65536:
                    raise ValueError('invalid supervisor CA response')
                # Validate PEM syntax without changing the trusted client context.
                ssl.create_default_context(cadata=data.decode('ascii'))
            attempts.append({'endpoint': endpoint, 'available': True,
                             'seconds': round(time.monotonic() - start, 3)})
            return endpoint, attempts
        except (OSError, urllib.error.URLError, ValueError) as error:
            attempts.append({'endpoint': endpoint, 'available': False,
                             'error': type(error).__name__,
                             'seconds': round(time.monotonic() - start, 3)})
    return None, attempts


def write_override(path, endpoint):
    path = Path(path)
    if path.is_symlink() or (path.exists() and not stat.S_ISREG(path.stat().st_mode)):
        raise ValueError('server override must be a regular file')
    body = 'server: ' + json.dumps(endpoint) + '\n'
    if path.exists() and path.read_text() == body:
        return False
    path.parent.mkdir(mode=0o755, parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(prefix='.vela-bootstrap-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as file:
            file.write(body)
            file.flush()
            os.fsync(file.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        Path(temporary).unlink(missing_ok=True)
    return True


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--endpoints', default='/etc/rancher/rke2/vela-bootstrap-endpoints.json')
    parser.add_argument('--ca', default='/etc/rancher/rke2/bootstrap-server-ca.crt')
    parser.add_argument('--output', default='/etc/rancher/rke2/config.yaml.d/99-vela-bootstrap-ha.yaml')
    parser.add_argument('--timeout', type=float, default=2.0)
    parser.add_argument('--check-only', action='store_true')
    args = parser.parse_args()
    if not 0 < args.timeout <= 15:
        parser.error('timeout must be between zero and 15 seconds')
    endpoint, attempts = select(endpoints(args.endpoints), args.ca, args.timeout)
    result = {'selected': endpoint, 'attempts': attempts, 'changed': False}
    if endpoint and not args.check_only:
        result['changed'] = write_override(args.output, endpoint)
    print(json.dumps(result), flush=True)
    return 0 if endpoint else 1


if __name__ == '__main__':
    raise SystemExit(main())
