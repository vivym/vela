#!/usr/bin/env python3
"""Process-level tests that mutating commands are executed at most once."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

SCRIPT = Path(__file__).resolve().parents[1] / 'deploy/cluster-platform/vela-kubectl.py'


class KubectlSelectionTest(unittest.TestCase):
    def run_case(self, arguments, ready, command_exit=0):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            fake = root / 'kubectl'
            fake.write_text('''#!/usr/bin/env python3
import json, os, sys
with open(os.environ['CALL_LOG'], 'a') as file:
    file.write(json.dumps(sys.argv[1:]) + '\\n')
if '--raw=/readyz' in sys.argv:
    if sys.argv[1] == '--server=' + os.environ['READY_ENDPOINT']:
        print('ok')
        sys.exit(0)
    sys.exit(1)
print('command executed')
sys.exit(int(os.environ['COMMAND_EXIT']))
''')
            fake.chmod(0o755)
            endpoints = ['https://10.1.201.70:6443', 'https://10.1.201.71:6443', 'https://10.1.201.66:6443']
            (root / 'endpoints.json').write_text(json.dumps(endpoints))
            env = dict(os.environ, PATH=str(root) + os.pathsep + os.environ['PATH'],
                       VELA_KUBECTL_ENDPOINTS=str(root / 'endpoints.json'), CALL_LOG=str(root / 'calls'),
                       READY_ENDPOINT=ready, COMMAND_EXIT=str(command_exit))
            result = subprocess.run(['python3', str(SCRIPT), *arguments], env=env, text=True, capture_output=True)
            calls = [json.loads(x) for x in (root / 'calls').read_text().splitlines()] if (root / 'calls').exists() else []
            return result, calls

    def test_secondary_is_selected_before_one_command(self):
        result, calls = self.run_case(['get', 'nodes'], 'https://10.1.201.71:6443')
        self.assertEqual(result.returncode, 0)
        self.assertEqual(len(calls), 3)
        self.assertEqual(calls[-1], ['--server=https://10.1.201.71:6443', '--insecure-skip-tls-verify=false', 'get', 'nodes'])

    def test_failed_write_is_never_replayed(self):
        result, calls = self.run_case(['create', '-f', 'job.yaml'], 'https://10.1.201.70:6443', 42)
        self.assertEqual(result.returncode, 42)
        self.assertEqual(len(calls), 2)
        self.assertEqual(sum('create' in x for x in calls), 1)

    def test_no_ready_endpoint_runs_no_command(self):
        result, calls = self.run_case(['delete', 'pod', 'example'], '')
        self.assertEqual(result.returncode, 1)
        self.assertEqual(len(calls), 3)
        self.assertTrue(all('--raw=/readyz' in x for x in calls))

    def test_connection_and_auth_overrides_fail_before_probe(self):
        for flag in ['--server=https://10.1.201.66:6443', '-shttps://example.com', '--context=other',
                     '--insecure-skip-tls-verify', '--token=example', '--as=admin']:
            with self.subTest(flag=flag):
                result, calls = self.run_case(['get', 'nodes', flag], 'https://10.1.201.70:6443')
                self.assertEqual(result.returncode, 2)
                self.assertEqual(calls, [])

    def test_remote_exec_arguments_are_preserved(self):
        args = ['exec', 'pod', '--', 'command', '--server=value', '-s']
        result, calls = self.run_case(args, 'https://10.1.201.70:6443')
        self.assertEqual(result.returncode, 0)
        self.assertEqual(calls[-1][2:], args)


if __name__ == '__main__':
    unittest.main()
