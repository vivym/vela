import hashlib
import json
import pathlib
import re
import shutil
import subprocess

repo = pathlib.Path('/Users/viv/projs/vela')
scratch = pathlib.Path('/tmp/vela-startup-validation.syiSMk/remote-cli-reservation')
out = repo / 'docs/evidence/node-remote-cli-reservation-2026-09-09'
out.mkdir(parents=True, exist_ok=True)
sha = lambda data: hashlib.sha256(data).hexdigest()
for label, source in [('native', 'native-explicit-home'), ('early-rootfs', 'native-first'), ('early-uts', 'native-rootfs'), ('early-env', 'native-hostname'), ('early-proc-diagnostic', 'native-vectors')]:
    target = out / label
    target.mkdir(exist_ok=True)
    for name in ('native.log', 'source.txt', 'source.patch', 'docker-version.txt', 'image.txt', 'binaries.sha256', 'downloads.sha256', 'scope.txt', 'build.log', 'build-runtime-command.log', 'build-vela-model-runtime.log'):
        candidate = scratch / source / name
        if candidate.exists():
            shutil.copyfile(candidate, target / name)
    (target / 'source-patch.sha256').write_text(sha((target / 'source.patch').read_bytes()) + '  source.patch\n')
    shutil.copyfile(scratch / (source + '-runner.log'), target / 'runner.log')
for name in ('repository-test.log', 'vet.log', 'lint.log', 'linux-lint-final.log', 'linux-vet.log', 'amd64-cross.log', 'ledger-native.log', 'publication-native.log', 'regression-runner.log'):
    shutil.copyfile(scratch / name, out / name)
regression = out / 'regression'
regression.mkdir(exist_ok=True)
for name in ('native.log', 'exit-code.txt', 'overlay.json', 'proc-vector-check-disabled.go.txt', 'image.txt', 'binaries.sha256', 'build.log', 'Dockerfile'):
    shutil.copyfile(scratch / 'regression' / name, regression / name)
for name in ('prepare_regression.py', 'run-regression.sh'):
    shutil.copyfile(scratch / name, regression / name)
shutil.copyfile(__file__, out / 'package-evidence.py')
names = subprocess.check_output(['git', 'ls-files', '-z', '--cached', '--others', '--exclude-standard'], cwd=repo).decode().split('\0')
files = [{'path': name, 'sha256': sha((repo / name).read_bytes())} for name in sorted(set(names)) if name and (pathlib.Path(name).suffix in ('.go', '.sql', '.proto') or name in ('go.mod', 'go.sum', 'buf.yaml', 'buf.gen.yaml'))]
aggregate = sha(b''.join((item['path'] + '\0' + item['sha256'] + '\n').encode() for item in files))
(out / 'source-files.json').write_text(json.dumps({'sha256': aggregate, 'file_count': len(files), 'files': files}, indent=2) + '\n')
native = (out / 'native/native.log').read_text()
publication = (out / 'publication-native.log').read_text()
ledger = (out / 'ledger-native.log').read_text()
for log in (native, publication, ledger):
    assert log.endswith('PASS\n') and not any(marker in log for marker in ('--- SKIP:', '--- FAIL:', 'WARNING: DATA RACE'))
cli_cases = dict(re.findall(r'--- PASS: TestRuntimeCallerContainerCRI/remote-cli-reservation/(\S+) \(([^)]+)\)', native))
publication_cases = dict(re.findall(r'--- PASS: TestRuntimeCallerContainerCRI/startup-image-reservation/(publication-\S+) \(([^)]+)\)', publication))
ledger_tests = dict(re.findall(r'^--- PASS: (TestRuntimeStartup\S+) \(([^)]+)\)', ledger, re.M))
configuration_cases = dict(re.findall(r'--- PASS: TestRuntimeRemoteCLIConfiguration/(\S+) \(([^)]+)\)', ledger))
assert len(cli_cases) == 10 and len(publication_cases) == 17 and len(ledger_tests) == 11 and len(configuration_cases) == 20
subprocess.run(['git', 'apply', '--reverse', '--check', str(out / 'native/source.patch')], cwd=repo, check=True)
for directory in (out / 'native', regression):
    for line in (directory / 'binaries.sha256').read_text().splitlines():
        digest, path = line.split(maxsplit=1)
        assert sha(pathlib.Path(path).read_bytes()) == digest
red = (regression / 'native.log').read_text()
assert (regression / 'exit-code.txt').read_text().strip() == '1'
assert not any(marker in red for marker in ('--- SKIP:', 'WARNING: DATA RACE'))
for case in ('hidden-env', 'hidden-argument'):
    assert '--- FAIL: TestRuntimeCallerContainerCRI/remote-cli-reservation/' + case + ' ' in red
    assert 'scenario=' + case + ' calls=1 intents=1' in red
assert '--- PASS: TestRuntimeCallerContainerCRI/remote-cli-reservation/valid ' in red
assert 'mismatched image rootfs and manifest layers' in (out / 'early-rootfs/native.log').read_text()
assert 'unable to set hostname without a private UTS namespace' in (out / 'early-uts/native.log').read_text()
assert 'HOME=/' in (out / 'early-proc-diagnostic/native.log').read_text()
assert 'FAIL' not in (out / 'repository-test.log').read_text()
checks = [
    ('VELA_TASK_LAUNCH_SCOPE=remote-cli bash hack/run-task-launch-native.sh', 'native/runner.log'),
    ("Final native image: docker run --rm --network none --privileged --cgroupns private --cpus 4 --memory 4g --pids-limit 512 -e VELA_TEST_CONTAINERD_SANDBOX=1 <image> '-test.run=^TestRuntimeCallerContainerCRI$/^startup-image-reservation$/^publication-' -test.count=1 -test.v -test.timeout=2m", 'publication-native.log'),
    ('Final native image: TestRuntimeRemoteCLIConfiguration and eleven explicit TestRuntimeStartupLedger/Reservation tests, -test.count=1 -test.v -test.timeout=2m; --network none --cap-add SYS_ADMIN --cap-add SYS_PTRACE --security-opt seccomp=unconfined --cpus 4 --memory 4g --pids-limit 256', 'ledger-native.log'),
    ('go test ./...', 'repository-test.log'), ('go vet ./...', 'vet.log'),
    ('golangci-lint v2.13.1 run ./...', 'lint.log'),
    ('GOOS=linux GOARCH=arm64 golangci-lint v2.13.1 run --build-tags=integration ./internal/nodeagent', 'linux-lint-final.log'),
    ('GOOS=linux GOARCH=arm64 go vet -tags=integration ./internal/nodeagent', 'linux-vet.log'),
    ('GOOS=linux GOARCH=amd64 go test -tags=integration -exec=true ./internal/nodeagent', 'amd64-cross.log'),
]
report = {
    'schema_version': 1,
    'baseline_commit': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=repo, text=True).strip(),
    'source_tree_sha256': aggregate, 'source_file_count': len(files),
    'source_digest_method': 'SHA256 of sorted Go, SQL, proto, go.mod, go.sum, buf.yaml and buf.gen.yaml path + NUL + hex SHA256(file bytes) + newline',
    'runner_sha256': sha((repo / 'hack/run-task-launch-native.sh').read_bytes()),
    'native_image': (out / 'native/image.txt').read_text().strip(),
    'native_platform': 'linux/arm64', 'race': True, 'skips': 0, 'races': 0, 'production_gates': '0/9',
    'cli_reservation_cases': cli_cases, 'publication_cases': publication_cases, 'ledger_tests': ledger_tests, 'configuration_cases': configuration_cases,
    'checks': [{'command': command, 'exit_code': 0, 'artifact': str((out / file).relative_to(repo))} for command, file in checks],
    'regression': {'change': 'Disable original process cmdline/environ byte comparison against approved task vectors', 'native_exit_code': 1, 'runner_exit_code': 0, 'expected_failing_cases': ['hidden-env', 'hidden-argument'], 'incorrect_side_effects_per_case': {'intents': 1, 'fleet_calls': 1}, 'passing_control': 'valid'},
    'initial_failures': [
        {'artifact': 'early-rootfs/native.log', 'cause': 'Fixture replaced RootFS metadata while constructing image config; preserve base ConfigFile and layer diff IDs.'},
        {'artifact': 'early-uts/native.log', 'cause': 'Network-isolated outer sandbox uses host-network, without private UTS for nested CRI; explicitly inject planned HOSTNAME environment without changing kernel hostname.'},
        {'artifacts': ['early-env/native.log', 'early-proc-diagnostic/native.log'], 'cause': 'runc appended HOME=/ to actual exec environment but task config had PATH/HOSTNAME only; explicitly set HOME=/ in image/task and retain byte-exact process comparison.'},
    ],
    'claim_boundary': 'Same operation actual remote CLI, actual Node journal RPC, real CRI image/task, actual published readonly mount and single Fleet fixture reservation. Positive backend initialization/shutdown uses an explicit test Permit. Registry/Pod/Fleet remain fixtures; production renderer still uses local-journal environment. Procfs sampling is not historical consumption, live Go environment, loaded-code attestation or exec continuity. No real TLS/PostgreSQL in this operation, production pre-grant read-only journal enrollment, once-only grant, production Node wiring, protected complete Job, Worker business journal, descendants/device containment, sustained capacity, GPU or Production Gate evidence.',
    'artifacts': [{'path': str(path.relative_to(repo)), 'bytes': path.stat().st_size, 'sha256': sha(path.read_bytes())} for path in sorted(out.rglob('*')) if path.is_file()],
}
(repo / 'docs/node-remote-cli-reservation-evidence-2026-09-09.json').write_text(json.dumps(report, indent=2) + '\n')
for artifact in report['artifacts']:
    assert sha((repo / artifact['path']).read_bytes()) == artifact['sha256']
print(json.dumps({'source_files': len(files), 'source_tree_sha256': aggregate, 'artifacts': len(report['artifacts']), 'cli_cases': len(cli_cases), 'publication_cases': len(publication_cases), 'ledger_tests': len(ledger_tests), 'configuration_cases': len(configuration_cases), 'expected_regression_failures': 2}))
