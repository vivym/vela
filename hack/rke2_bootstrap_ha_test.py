#!/usr/bin/env python3
"""Exercise supervisor selection with real HTTPS and atomic config writes."""
import contextlib
import http.server
import importlib.util
import io
import json
import os
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import threading
import unittest
from unittest import mock

spec = importlib.util.spec_from_file_location('bootstrap_ha',
    Path(__file__).resolve().parents[1] / 'deploy/cluster-platform/rke2-bootstrap-ha.py')
ha = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ha)


class SupervisorTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.directory = tempfile.TemporaryDirectory(prefix='vela-bootstrap-tls-')
        root = Path(cls.directory.name)
        cls.ca, cls.key = root / 'ca.crt', root / 'key.pem'
        subprocess.run(['openssl', 'req', '-x509', '-newkey', 'rsa:2048', '-nodes',
                        '-subj', '/CN=bootstrap-test', '-days', '1',
                        '-addext', 'subjectAltName=IP:127.0.0.1',
                        '-keyout', str(cls.key), '-out', str(cls.ca)],
                       check=True, capture_output=True)
        certificate = cls.ca.read_bytes()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_GET(self):
                body = b'pong' if self.path == '/ping' else certificate
                if self.path not in ['/ping', '/cacerts']:
                    self.send_error(404)
                    return
                self.send_response(200)
                self.send_header('Content-Length', str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, *_):
                pass

        cls.server = http.server.ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(cls.ca, cls.key)
        cls.server.socket = context.wrap_socket(cls.server.socket, server_side=True)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.good = 'https://127.0.0.1:' + str(cls.server.server_port)

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()
        cls.directory.cleanup()

    def test_unreachable_primary_selects_authenticated_secondary(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            # Bound but not listening: a stable refused-connect endpoint.
            dead = 'https://127.0.0.1:' + str(listener.getsockname()[1])
            with mock.patch.dict(os.environ, {'https_proxy': 'http://127.0.0.1:1'}):
                selected, attempts = ha.select([dead, self.good], str(self.ca), 0.2)
        self.assertEqual(selected, self.good)
        self.assertEqual([a['available'] for a in attempts], [False, True])

    def test_hostname_verification_is_not_skipped(self):
        wrong_name = self.good.replace('127.0.0.1', 'localhost')
        selected, attempts = ha.select([wrong_name], str(self.ca), 0.5)
        self.assertIsNone(selected)
        self.assertFalse(attempts[0]['available'])

    def test_untrusted_root_is_rejected(self):
        with mock.patch.object(ssl, 'create_default_context', wraps=ssl.create_default_context) as make:
            # An empty root set is not a request to disable certificate checks.
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
            make.return_value = context
            selected, _ = ha.select([self.good], str(self.ca), 0.5)
        self.assertIsNone(selected)

    def test_all_unavailable_preserves_existing_override(self):
        with tempfile.TemporaryDirectory() as directory, socket.socket() as listener:
            root = Path(directory)
            output = root / 'config.yaml'
            output.write_text('server: https://192.0.2.1:9345\n')
            listener.bind(('127.0.0.1', 0))
            endpoints = root / 'endpoints.json'
            endpoints.write_text(json.dumps(['https://127.0.0.1:' + str(listener.getsockname()[1])]))
            argv = ['selector', '--endpoints', str(endpoints), '--ca', str(self.ca), '--output', str(output)]
            with mock.patch('sys.argv', argv), contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(ha.main(), 1)
            self.assertEqual(output.read_text(), 'server: https://192.0.2.1:9345\n')

    def test_override_is_private_atomic_and_idempotent(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / 'config.yaml.d' / '99-vela-bootstrap-ha.yaml'
            self.assertTrue(ha.write_override(output, self.good))
            inode = output.stat().st_ino
            self.assertFalse(ha.write_override(output, self.good))
            self.assertEqual(inode, output.stat().st_ino)
            self.assertEqual(output.stat().st_mode & 0o777, 0o600)
            self.assertTrue(output.read_text().startswith('server: "https://'))
            self.assertEqual(list(output.parent.iterdir()), [output])

    def test_symlink_override_is_rejected(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / 'target'
            target.write_text('preserve')
            link = Path(directory) / 'override'
            link.symlink_to(target)
            with self.assertRaises(ValueError):
                ha.write_override(link, self.good)
            self.assertEqual(target.read_text(), 'preserve')

    def test_endpoint_does_not_accept_credentials_or_paths(self):
        with tempfile.TemporaryDirectory() as directory:
            file = Path(directory) / 'endpoints.json'
            for value in ['http://127.0.0.1:9345', 'https://user:secret@127.0.0.1:9345',
                          'https://127.0.0.1:9345/other', 'https://example.com:9345']:
                file.write_text(json.dumps([value]))
                with self.subTest(value=value), self.assertRaises(ValueError):
                    ha.endpoints(file)


if __name__ == '__main__':
    unittest.main()
