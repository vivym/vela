import hashlib
import json
import pathlib
import re
import shutil
import subprocess

repo=pathlib.Path('/Users/viv/projs/vela')
scratch=pathlib.Path('/tmp/vela-startup-validation.syiSMk/startup-publication')
out=repo/'docs/evidence/node-startup-publication-2026-09-09'
out.mkdir(parents=True,exist_ok=True)
sha=lambda b:hashlib.sha256(b).hexdigest()
for name in ('native.log','source.txt','source.patch','scope.txt','docker-version.txt','image.txt','binaries.sha256','downloads.sha256','build.log'):
    shutil.copyfile(scratch/'native-pinned'/name,out/name)
(out/'source-patch.sha256').write_text(sha((out/'source.patch').read_bytes())+'  source.patch\n')
shutil.copyfile(scratch/'native-pinned-runner.log',out/'native-runner.log')
for target,source in {'ledger-native.log':'ledger-native.log','repository-test.log':'repository-test.log','vet.log':'vet.log','lint.log':'lint.log','linux-lint.log':'linux-lint-pinned.log','linux-vet.log':'linux-vet.log','amd64-cross.log':'amd64-cross-pinned.log','initial-compile.log':'compile.log','regression-runner.log':'regression-runner.log','regression-build-failure.log':'regression-build-failure.log'}.items():
    shutil.copyfile(scratch/source,out/target)
for label,source in [('first-full','native-first'),('helper-shared-fs','native-mounted'),('helper-root-path','native-final')]:
    for name in ('native.log','source.txt','source.patch','image.txt','binaries.sha256'):
        shutil.copyfile(scratch/source/name,out/(label+'-'+name))
for name in ('native.log','exit-code.txt','overlay.json','mount-identity-disabled.go.txt','publication-copy-disabled.go.txt','run.sh','image.txt','binaries.sha256','build.log'):
    shutil.copyfile(scratch/'regression'/name,out/('regression-'+name))
shutil.copyfile(__file__,out/'package-evidence.py')

names=subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=repo).decode().split('\0')
files=[]
for name in sorted(set(names)):
    if name and (pathlib.Path(name).suffix in ('.go','.sql','.proto') or name in ('go.mod','go.sum','buf.yaml','buf.gen.yaml')):
        files.append({'path':name,'sha256':sha((repo/name).read_bytes())})
aggregate=sha(b''.join((x['path']+'\0'+x['sha256']+'\n').encode() for x in files))
(out/'source-files.json').write_text(json.dumps({'sha256':aggregate,'file_count':len(files),'files':files},indent=2)+'\n')
native=(out/'native.log').read_text()
ledger=(out/'ledger-native.log').read_text()
for log in (native,ledger):
    assert log.endswith('PASS\n') and '--- SKIP:' not in log and 'WARNING: DATA RACE' not in log and '--- FAIL:' not in log
cases=dict(re.findall(r'--- PASS: TestRuntimeCallerContainerCRI/startup-image-reservation/(publication-\S+) \(([^)]+)\)',native))
ledger_tests=dict(re.findall(r'^--- PASS: (\S+) \(([^)]+)\)',ledger,re.M))
assert len(cases)==12 and len(ledger_tests)==11
red=(out/'regression-native.log').read_text()
assert (out/'regression-exit-code.txt').read_text().strip()=='1'
assert 'WARNING: DATA RACE' not in red and '--- SKIP:' not in red
for case in ('valid','before-fleet-remounted','after-fleet-remounted'):
    assert '--- FAIL: TestRuntimeCallerContainerCRI/startup-image-reservation/publication-'+case+' ' in red
checks=[
    ('VELA_TASK_LAUNCH_SCOPE=startup-publication bash hack/run-task-launch-native.sh','native-runner.log'),
    ('Native pinned image: eleven explicit TestRuntimeStartupLedger/Reservation tests, -test.count=1 -test.v -test.timeout=2m','ledger-native.log'),
    ('go test ./...','repository-test.log'),('go vet ./...','vet.log'),
    ('golangci-lint v2.13.1 run ./...','lint.log'),
    ('GOOS=linux GOARCH=arm64 golangci-lint v2.13.1 run --build-tags=integration ./internal/nodeagent','linux-lint.log'),
    ('GOOS=linux GOARCH=arm64 go vet -tags=integration ./internal/nodeagent','linux-vet.log'),
    ('GOOS=linux GOARCH=amd64 go test -tags=integration -exec=true ./internal/nodeagent','amd64-cross.log')]
report={
    'schema_version':1,'baseline_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
    'source_tree_sha256':aggregate,'source_file_count':len(files),
    'source_digest_method':'SHA256 of sorted Go, SQL, proto, go.mod, go.sum, buf.yaml and buf.gen.yaml path + NUL + hex SHA256(file bytes) + newline',
    'runner_sha256':sha((repo/'hack/run-task-launch-native.sh').read_bytes()),
    'native_platform':'linux/arm64','race':True,'skips':0,'races':0,'production_gates':'0/9',
    'native_scope':'startup-publication','publication_cases':cases,'ledger_main_tests':ledger_tests,
    'checks':[{'command':cmd,'exit_code':0,'artifact':str((out/file).relative_to(repo))} for cmd,file in checks],
    'regression':{'changes':['Disable effective mount ID comparison by returning constant 1','Remove nested bootstrap snapshot copy'],'native_exit_code':1,'runner_exit_code':0,'expected_failing_cases':['publication-valid','publication-before-fleet-remounted','publication-after-fleet-remounted']},
    'earlier_full_campaign':'first-full-native.log is the earlier source.patch snapshot, before later test/clone/runner changes; do not claim it is a full final-source rerun.',
    'initial_failures':['os.Root has no Fd method: use retained root.Open(".") procfs descriptor','Mount namespace helper Setns rejected shared CLONE_FS; unshare within disposable OS thread','Cross-namespace mount helper path could not resolve; use retained source/target FDs with OpenTree/MoveMount','Docker FROM raw local image ID was resolved as remote name; tag the existing local image before overlay build'],
    'claim_boundary':'Same-invocation Node publication, original caller procfs-root effective read-only file/mount, image/task observations, held journal and single Fleet reservation. This is sampled availability, not proof of actual CLI configuration consumption or executable continuity. Registry/Pod/Fleet are fixtures, CRI caller is test probe. No Permit, once-only grant, actual CLI plus real Fleet/TLS/PostgreSQL in this operation, production Node/Fleet mounts, full Job, Worker business journal, power-loss recovery, sustained capacity, GPU or Production Gate evidence.',
    'artifacts':[{'path':str(p.relative_to(repo)),'bytes':p.stat().st_size,'sha256':sha(p.read_bytes())} for p in sorted(out.iterdir()) if p.is_file()]
}
(repo/'docs/node-startup-publication-evidence-2026-09-09.json').write_text(json.dumps(report,indent=2)+'\n')
for artifact in report['artifacts']:
    assert sha((repo/artifact['path']).read_bytes())==artifact['sha256']
for filename in ('binaries.sha256','regression-binaries.sha256'):
    for line in (out/filename).read_text().splitlines():
        digest,path=line.split(maxsplit=1)
        assert sha(pathlib.Path(path).read_bytes())==digest
subprocess.run(['git','apply','--reverse','--check',str(out/'source.patch')],cwd=repo,check=True)
print(json.dumps({'source_files':len(files),'source_tree_sha256':aggregate,'artifacts':len(report['artifacts']),'publication_cases':len(cases),'ledger_tests':len(ledger_tests),'regression_failures':3}))
