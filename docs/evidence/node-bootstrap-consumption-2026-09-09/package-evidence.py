import hashlib
import json
import pathlib
import re
import shutil
import subprocess

repo = pathlib.Path('/Users/viv/projs/vela')
scratch = pathlib.Path('/tmp/vela-startup-validation.syiSMk/bootstrap-consumption')
out = repo / 'docs/evidence/node-bootstrap-consumption-2026-09-09'
out.mkdir(parents=True, exist_ok=True)
sha = lambda data: hashlib.sha256(data).hexdigest()

for label, source in [('cli', 'cli-final'), ('cri', 'cri-final'), ('first-cli', 'cli-native'), ('first-cri', 'cri-native')]:
    target = out / label
    target.mkdir(exist_ok=True)
    for name in ('native.log', 'source.txt', 'source.patch', 'docker-version.txt', 'image.txt', 'binaries.sha256', 'downloads.sha256', 'scope.txt', 'build.log', 'build-nodeagent.log', 'build-runtime-command.log', 'build-vela-model-runtime.log'):
        candidate = scratch / source / name
        if candidate.exists():
            shutil.copyfile(candidate, target / name)
    (target / 'source-patch.sha256').write_text(sha((target / 'source.patch').read_bytes()) + '  source.patch\n')
    shutil.copyfile(scratch / (source + '-runner.log'), target / 'runner.log')
for name in ('repository-test.log', 'repository-test-final.log', 'focused-final.log', 'vet-final.log', 'lint.log', 'linux-lint-pinned.log', 'linux-vet.log', 'amd64-cross.log', 'ledger-native.log', 'regression-runner.log'):
    shutil.copyfile(scratch / name, out / name)
regression = out / 'regression'
regression.mkdir(exist_ok=True)
for name in ('native.log', 'exit-code.txt', 'overlay.json', 'reread-at-gate.go.txt', 'image.txt', 'binaries.sha256', 'build.log', 'Dockerfile'):
    shutil.copyfile(scratch / 'regression' / name, regression / name)
for name in ('prepare_regression.py', 'run-regression.sh'):
    shutil.copyfile(scratch / name, regression / name)
shutil.copyfile(__file__, out / 'package-evidence.py')

names = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=repo).decode().split('\0')
files = [{'path': name, 'sha256': sha((repo / name).read_bytes())} for name in sorted(set(names)) if name and (pathlib.Path(name).suffix in ('.go', '.sql', '.proto') or name in ('go.mod', 'go.sum', 'buf.yaml', 'buf.gen.yaml'))]
aggregate = sha(b''.join((item['path'] + '\0' + item['sha256'] + '\n').encode() for item in files))
(out / 'source-files.json').write_text(json.dumps({'sha256': aggregate, 'file_count': len(files), 'files': files}, indent=2) + '\n')

cli = (out / 'cli/native.log').read_text()
cri = (out / 'cri/native.log').read_text()
ledger = (out / 'ledger-native.log').read_text()
for log in (cli, cri, ledger):
    assert log.endswith('PASS\n') and not any(marker in log for marker in ('--- SKIP:', '--- FAIL:', 'WARNING: DATA RACE'))
cli_tests = dict(re.findall(r'^--- PASS: (\S+) \(([^)]+)\)', cli, re.M))
cli_cases = dict(re.findall(r'--- PASS: TestRuntimeBootstrapPublicationActualCLI/(\S+) \(([^)]+)\)', cli))
cri_cases = dict(re.findall(r'--- PASS: TestRuntimeCallerContainerCRI/startup-image-reservation/(publication-\S+) \(([^)]+)\)', cri))
ledger_tests = dict(re.findall(r'^--- PASS: (\S+) \(([^)]+)\)', ledger, re.M))
assert len(cli_tests) == 10 and len(cli_cases) == 3 and len(cri_cases) == 17 and len(ledger_tests) == 11
assert (out / 'cli/source.patch').read_bytes() == (out / 'cri/source.patch').read_bytes()
for name in ('cli', 'cri'):
    subprocess.run(['git', 'apply', '--reverse', '--check', str(out / name / 'source.patch')], cwd=repo, check=True)
    for line in (out / name / 'binaries.sha256').read_text().splitlines():
        digest, path = line.split(maxsplit=1)
        assert sha(pathlib.Path(path).read_bytes()) == digest
red = (regression / 'native.log').read_text()
assert (regression / 'exit-code.txt').read_text().strip() == '1'
assert '--- FAIL: TestRuntimeBootstrapPublicationActualCLI/changed-after-read ' in red
assert not any(marker in red for marker in ('--- SKIP:', 'WARNING: DATA RACE'))
for case in ('permit', 'alias-path'):
    assert '--- PASS: TestRuntimeBootstrapPublicationActualCLI/' + case + ' ' in red
assert '--- FAIL: TestJournalRemoteReadTimeoutCanRetry ' in (out / 'repository-test.log').read_text()
assert 'FAIL' not in (out / 'repository-test-final.log').read_text()
checks = [
    ('VELA_REMOTE_CLI_EVIDENCE=<scratch>/cli-final bash hack/run-remote-runtime-cli-native.sh', 'cli/runner.log'),
    ('VELA_TASK_LAUNCH_SCOPE=startup-publication VELA_TASK_LAUNCH_EVIDENCE=<scratch>/cri-final bash hack/run-task-launch-native.sh', 'cri/runner.log'),
    ('Native cli image: eleven explicit TestRuntimeStartupLedger/Reservation tests, -test.count=1 -test.v -test.timeout=2m', 'ledger-native.log'),
    ("go test ./internal/modelruntime -run 'TestBackendStartup|TestJournalRemoteReadTimeoutCanRetry' -count=10", 'focused-final.log'),
    ('go test ./...', 'repository-test-final.log'),
    ('go vet ./...', 'vet-final.log'),
    ('golangci-lint v2.13.1 run ./...', 'lint.log'),
    ('GOOS=linux GOARCH=arm64 golangci-lint v2.13.1 run --build-tags=integration ./internal/nodeagent ./internal/modelruntime ./cmd/vela-model-runtime', 'linux-lint-pinned.log'),
    ('GOOS=linux GOARCH=arm64 go vet -tags=integration ./internal/nodeagent ./internal/modelruntime ./cmd/vela-model-runtime', 'linux-vet.log'),
    ('GOOS=linux GOARCH=amd64 go test -tags=integration -exec=true ./internal/nodeagent ./internal/modelruntime ./cmd/vela-model-runtime', 'amd64-cross.log'),
]
report = {
    'schema_version': 1,
    'baseline_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
    'source_tree_sha256': aggregate, 'source_file_count': len(files),
    'source_digest_method': 'SHA256 of sorted Go, SQL, proto, go.mod, go.sum, buf.yaml and buf.gen.yaml path + NUL + hex SHA256(file bytes) + newline',
    'runner_sha256': {name: sha((repo / name).read_bytes()) for name in ('hack/run-remote-runtime-cli-native.sh', 'hack/run-task-launch-native.sh')},
    'native_platform': 'linux/arm64', 'race': True, 'skips': 0, 'races': 0,
    'production_gates': '0/9', 'cli_tests': cli_tests, 'cli_publication_cases': cli_cases,
    'cri_publication_cases': cri_cases, 'ledger_tests': ledger_tests,
    'checks': [{'command': command, 'exit_code': 0, 'artifact': str((out / file).relative_to(repo))} for command, file in checks],
    'regression': {'change': 'Actual CLI gate rereads bootstrap path and replaces frozen digest', 'native_exit_code': 1, 'runner_exit_code': 0, 'expected_failure': 'TestRuntimeBootstrapPublicationActualCLI/changed-after-read', 'passing_controls': ['permit', 'alias-path']},
    'first_failure': {'command': 'go test ./...', 'exit_code': 1, 'test': 'TestJournalRemoteReadTimeoutCanRetry', 'observation': 'Successful retry returned STALE/requires state recovery/context deadline exceeded with 1-second journal I/O budget; host latency source is not established.', 'resolution': 'Scope 50ms caller deadline to intentionally blocked read; retain 30-second owner I/O budget for durable successful retry. Product uncertainty isolation is unchanged. This does not explain historical 11ce026 STALE.'},
    'earlier_native_runs': 'first-cli predates the stronger first-journal-RPC replacement boundary and final runner source snapshot; first-cri predates final test-only changes. Use cli/cri for final source evidence.',
    'claim_boundary': 'Actual CLI snapshot digest/path freezing and independent Node declaration comparison; separately real CRI image/mount/journal single-reservation association with fixture Registry/Pod/Fleet. No arbitrary caller attestation, effective argv/env approval, executable continuity, once-only grant, same-operation actual CLI plus CRI plus real TLS/PostgreSQL/Fleet, production Node wiring, protected full Job, Worker input/materialization ownership, power-loss recovery, sustained capacity, GPU or Production Gate evidence.',
    'artifacts': [{'path': str(path.relative_to(repo)), 'bytes': path.stat().st_size, 'sha256': sha(path.read_bytes())} for path in sorted(out.rglob('*')) if path.is_file()],
}
(repo / 'docs/node-bootstrap-consumption-evidence-2026-09-09.json').write_text(json.dumps(report, indent=2) + '\n')
for artifact in report['artifacts']:
    assert sha((repo / artifact['path']).read_bytes()) == artifact['sha256']
print(json.dumps({'source_files': len(files), 'source_tree_sha256': aggregate, 'artifacts': len(report['artifacts']), 'cli_main_tests': len(cli_tests), 'cli_consumption_cases': len(cli_cases), 'cri_cases': len(cri_cases), 'ledger_tests': len(ledger_tests), 'expected_regression_failures': 1}))
