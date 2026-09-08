import hashlib
import json
import pathlib
import re
import shutil
import subprocess

repo = pathlib.Path('/Users/viv/projs/vela')
scratch = pathlib.Path('/tmp/vela-startup-validation.syiSMk/publication')
out = repo / 'docs/evidence/node-bootstrap-publication-2026-09-09'
out.mkdir(parents=True, exist_ok=True)
sha = lambda data: hashlib.sha256(data).hexdigest()

for name in ['native.log', 'source.txt', 'source.patch', 'docker-version.txt', 'image.txt', 'binaries.sha256', 'build-nodeagent.log', 'build-runtime-command.log', 'build-vela-model-runtime.log']:
    shutil.copyfile(scratch / 'native-final' / name, out / name)
(out / 'source-patch.sha256').write_text(sha((out/'source.patch').read_bytes()) + '  source.patch\n')
shutil.copyfile(scratch/'native-final-runner.log',out/'native-runner.log')
for name in ['repository-test', 'vet', 'lint', 'linux-lint', 'linux-vet', 'amd64-cross']:
    shutil.copyfile(scratch/(name+'-final.log'),out/(name+'.log'))
for label, source in [('initial', 'native-first'), ('custody-red','native-custody-red')]:
    for name in ['native.log','source.txt','source.patch','binaries.sha256','image.txt']:
        shutil.copyfile(scratch/source/name,out/(label+'-'+name))
    shutil.copyfile(scratch/(source+'-runner.log'),out/(label+'-runner.log'))
shutil.copyfile(__file__, out/'package-evidence.py')

names = subprocess.check_output(['git','ls-files','-z','--cached','--others','--exclude-standard'],cwd=repo).decode().split('\0')
files=[]
for name in sorted(set(names)):
    path=repo/name
    if name and (path.suffix in ('.go','.sql','.proto') or name in ('go.mod','go.sum','buf.yaml','buf.gen.yaml')):
        files.append({'path':name,'sha256':sha(path.read_bytes())})
aggregate=sha(b''.join((x['path']+'\0'+x['sha256']+'\n').encode() for x in files))
(out/'source-files.json').write_text(json.dumps({'sha256':aggregate,'file_count':len(files),'files':files},indent=2)+'\n')
log=(out/'native.log').read_text()
assert log.endswith('PASS\n') and '--- SKIP:' not in log and 'WARNING: DATA RACE' not in log and '--- FAIL:' not in log
main=dict(re.findall(r'^--- PASS: (\S+) \(([^)]+)\)',log,re.M))
assert len(main)==10
subcases=re.findall(r'^    --- PASS: (TestRuntimeBootstrapPublication[^ ]*/[^ ]+) ',log,re.M)
assert len(subcases)==43, len(subcases)
checks=[
    ('bash hack/run-remote-runtime-cli-native.sh','native-runner.log'),
    ('go test ./...','repository-test.log'),
    ('go vet ./...','vet.log'),
    ('golangci-lint v2.13.1 run ./...','lint.log'),
    ('GOOS=linux GOARCH=arm64 golangci-lint v2.13.1 run ./internal/nodeagent/... ./internal/modelruntime/... ./internal/stageauthority/...','linux-lint.log'),
    ('GOOS=linux GOARCH=arm64 go vet ./internal/nodeagent ./internal/modelruntime ./internal/stageauthority','linux-vet.log'),
    ('GOOS=linux GOARCH=amd64 go test -exec=true ./internal/nodeagent ./cmd/vela-model-runtime ./internal/modelruntime ./internal/stageauthority','amd64-cross.log')]
report={
    'schema_version':1,
    'baseline_commit':subprocess.check_output(['git','rev-parse','HEAD'],cwd=repo,text=True).strip(),
    'source_tree_sha256':aggregate,'source_file_count':len(files),
    'source_digest_method':'SHA256 of sorted Go, SQL, proto, go.mod, go.sum, buf.yaml and buf.gen.yaml path + NUL + hex SHA256(file bytes) + newline',
    'runner_sha256':sha((repo/'hack/run-remote-runtime-cli-native.sh').read_bytes()),
    'native_platform':'linux/arm64','race':True,'skips':0,'races':0,
    'production_gates':'0/9','native_main_tests':main,'publication_nested_cases':len(subcases),
    'checks':[{'command':cmd,'exit_code':0,'artifact':str((out/file).relative_to(repo))} for cmd,file in checks],
    'initial_failures':[
        {'artifact':'initial-native.log','cause':'Separate StageAuthority keyring could differ from held journal verifier. Removed duplicate input and derive public keys from the actual owner.'},
        {'artifact':'initial-native.log','cause':'Worker helper expected UID 65532 for actual planned UID 10001; product socket ownership correctly rejected. Test helper now uses its actual expected shared Runtime UID.'},
        {'artifact':'custody-red-native.log','cause':'Journal closure after rename left complete exposed history before final refusal. Recheck held journal after filesystem validation and before exposing directory.'}],
    'claim_boundary':'Per-directory publication and history inspection; actual UID/GID 10001 CLI with real held journal and process backend initialize/shutdown, Worker discovery epoch=2. Registry/CRI and Node Permit are fixtures. Publication is not unique per incarnation and is not a grant. No production Node/Fleet mounts, image/env/writer association in this invocation, once-only grant, full CLI Job, Worker business custody, power-loss recovery, descendant containment, sustained load, GPU or Production Gate evidence.',
    'artifacts':[{'path':str(p.relative_to(repo)),'bytes':p.stat().st_size,'sha256':sha(p.read_bytes())} for p in sorted(out.iterdir()) if p.is_file()]
}
target=repo/'docs/node-bootstrap-publication-evidence-2026-09-09.json'
target.write_text(json.dumps(report,indent=2)+'\n')
for artifact in report['artifacts']:
    assert sha((repo/artifact['path']).read_bytes())==artifact['sha256']
for line in (out/'binaries.sha256').read_text().splitlines():
    digest,path=line.split(maxsplit=1)
    assert sha(pathlib.Path(path).read_bytes())==digest
subprocess.run(['git','apply','--reverse','--check',str(out/'source.patch')],cwd=repo,check=True)
print(json.dumps({'source_files':len(files),'source_tree_sha256':aggregate,'artifacts':len(report['artifacts']),'native_main_tests':len(main),'publication_nested_cases':len(subcases)}))
