import hashlib
import json
import pathlib
import re
import shutil
import subprocess

repo = pathlib.Path('/Users/viv/projs/vela')
scratch = pathlib.Path('/tmp/vela-startup-validation.syiSMk/journal-readonly')
out = repo / 'docs/evidence/journal-readonly-startup-2026-09-09'
out.mkdir(parents=True, exist_ok=True)
sha = lambda data: hashlib.sha256(data).hexdigest()
for label, source in [('cli', 'cli-native'), ('cri', 'cri-native')]:
    target = out / label
    target.mkdir(exist_ok=True)
    for name in ('native.log', 'source.txt', 'source.patch', 'docker-version.txt', 'image.txt', 'binaries.sha256', 'downloads.sha256', 'scope.txt', 'build.log', 'build-nodeagent.log', 'build-runtime-command.log', 'build-vela-model-runtime.log'):
        candidate = scratch / source / name
        if candidate.exists():
            shutil.copyfile(candidate, target / name)
    (target / 'source-patch.sha256').write_text(sha((target / 'source.patch').read_bytes()) + '  source.patch\n')
    shutil.copyfile(scratch / (source + '-runner.log'), target / 'runner.log')
for name in ('repository-test.log', 'vet.log', 'lint.log', 'linux-lint.log', 'linux-vet.log', 'amd64-cross.log', 'writable-native.log', 'regression-runner.log'):
    shutil.copyfile(scratch / name, out / name)
regression = out / 'regression'
regression.mkdir(exist_ok=True)
for name in ('endpoint.log', 'cri.log', 'endpoint-exit-code.txt', 'cri-exit-code.txt', 'overlay.json', 'readonly-check-disabled.go.txt', 'image.txt', 'binaries.sha256', 'build.log', 'Dockerfile'):
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
writable = (out / 'writable-native.log').read_text()
for log in (cli, cri, writable):
    assert log.endswith('PASS\n') and not any(marker in log for marker in ('--- SKIP:', '--- FAIL:', 'WARNING: DATA RACE'))
cli_tests = dict(re.findall(r'^--- PASS: (\S+) \(([^)]+)\)', cli, re.M))
cri_cases = dict(re.findall(r'--- PASS: TestRuntimeCallerContainerCRI/remote-cli-reservation/(\S+) \(([^)]+)\)', cri))
readonly_cases = dict(re.findall(r'--- PASS: TestJournalReadOnlyEndpoint/(\S+/\S+) \(([^)]+)\)', cli))
writable_tests = dict(re.findall(r'^--- PASS: (\S+) \(([^)]+)\)', writable, re.M))
assert len(cli_tests) == 11 and len(cri_cases) == 11 and len(readonly_cases) == 16 and len(writable_tests) == 6
assert 'same command accepted only by subsequent independent Node owner call' in cri
assert (out / 'cli/source.patch').read_bytes() == (out / 'cri/source.patch').read_bytes()
for directory in (out / 'cli', out / 'cri'):
    subprocess.run(['git', 'apply', '--reverse', '--check', str(directory / 'source.patch')], cwd=repo, check=True)
for directory in (out / 'cli', out / 'cri', regression):
    for line in (directory / 'binaries.sha256').read_text().splitlines():
        digest, path = line.split(maxsplit=1)
        assert sha(pathlib.Path(path).read_bytes()) == digest
red_endpoint = (regression / 'endpoint.log').read_text()
red_cri = (regression / 'cri.log').read_text()
for label, log in [('endpoint', red_endpoint), ('cri', red_cri)]:
    assert (regression / (label + '-exit-code.txt')).read_text().strip() == '1'
    assert not any(marker in log for marker in ('--- SKIP:', 'WARNING: DATA RACE'))
for role in ('runtime', 'worker'):
    assert '--- FAIL: TestJournalReadOnlyEndpoint/' + role + ' ' in red_endpoint
assert '--- FAIL: TestRuntimeCallerContainerCRI/remote-cli-reservation/pregrant-floor ' in red_cri
assert '--- PASS: TestRuntimeCallerContainerCRI/remote-cli-reservation/valid ' in red_cri
assert 'FAIL' not in (out / 'repository-test.log').read_text()
checks = [
    ('bash hack/run-remote-runtime-cli-native.sh', 'cli/runner.log'),
    ('VELA_TASK_LAUNCH_SCOPE=remote-cli bash hack/run-task-launch-native.sh', 'cri/runner.log'),
    ('Final CRI image, no network, SYS_ADMIN/SYS_PTRACE: TestJournalEndpoint, TestJournalServer, TestJournalServerLiveSupervisorRecoversAfterOverload, TestJournalServerTimeoutAndJoin, TestJournalServerListenerFailure, TestJournalServerConfiguration; -test.count=1 -test.v -test.timeout=2m', 'writable-native.log'),
    ('go test ./...', 'repository-test.log'), ('go vet ./...', 'vet.log'),
    ('golangci-lint v2.13.1 run ./...', 'lint.log'),
    ('GOOS=linux GOARCH=arm64 golangci-lint v2.13.1 run --build-tags=integration ./internal/nodeagent', 'linux-lint.log'),
    ('GOOS=linux GOARCH=arm64 go vet -tags=integration ./internal/nodeagent', 'linux-vet.log'),
    ('GOOS=linux GOARCH=amd64 go test -tags=integration -exec=true ./internal/nodeagent', 'amd64-cross.log'),
]
report = {
    'schema_version': 1,
    'baseline_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
    'source_tree_sha256': aggregate, 'source_file_count': len(files),
    'source_digest_method': 'SHA256 of sorted Go, SQL, proto, go.mod, go.sum, buf.yaml and buf.gen.yaml path + NUL + hex SHA256(file bytes) + newline',
    'runner_sha256': {name: sha((repo / name).read_bytes()) for name in ('hack/run-remote-runtime-cli-native.sh', 'hack/run-task-launch-native.sh')},
    'native_platform': 'linux/arm64', 'race': True, 'skips': 0, 'races': 0, 'production_gates': '0/9',
    'cli_main_tests': cli_tests, 'cri_cases': cri_cases, 'readonly_selector_cases': readonly_cases, 'writable_main_tests': writable_tests,
    'checks': [{'command': command, 'exit_code': 0, 'artifact': str((out / file).relative_to(repo))} for command, file in checks],
    'positive_controls': 'Exact correctly signed admission and floor rejected through read-only RPC are accepted by the independent trusted owner after endpoint/server close. Eight selector coverage is not a claim that all eight business-state preconditions were satisfied.',
    'regression': {'change': 'Disable immutable endpoint read-only mutation guard', 'endpoint_exit_code': 1, 'cri_exit_code': 1, 'runner_exit_code': 0, 'expected_failures': ['TestJournalReadOnlyEndpoint/runtime', 'TestJournalReadOnlyEndpoint/worker', 'TestRuntimeCallerContainerCRI/remote-cli-reservation/pregrant-floor'], 'passing_control': 'TestRuntimeCallerContainerCRI/remote-cli-reservation/valid'},
    'claim_boundary': 'Immutable read-only endpoint and actual CLI/publication/CRI startup fixtures. Correctly signed writes rejected before reservation and after explicit test Permit. No automatic upgrade or production grant transition. Existing writable constructor still requires independent prior authority and does not verify a grant. No production Node enrollment, same-operation real TLS/PostgreSQL/Fleet, executable continuity, once-only grant, protected full Job, Worker business journal, descendants/device containment, sustained capacity, GPU or Production Gate evidence.',
    'artifacts': [{'path': str(path.relative_to(repo)), 'bytes': path.stat().st_size, 'sha256': sha(path.read_bytes())} for path in sorted(out.rglob('*')) if path.is_file()],
}
(repo / 'docs/journal-readonly-startup-evidence-2026-09-09.json').write_text(json.dumps(report, indent=2) + '\n')
for artifact in report['artifacts']:
    assert sha((repo / artifact['path']).read_bytes()) == artifact['sha256']
print(json.dumps({'source_files': len(files), 'source_tree_sha256': aggregate, 'artifacts': len(report['artifacts']), 'cli_main_tests': len(cli_tests), 'cri_cases': len(cri_cases), 'readonly_cases': len(readonly_cases), 'writable_tests': len(writable_tests), 'expected_regression_failures': 3}))
