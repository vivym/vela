import json
import pathlib

repo = pathlib.Path('/Users/viv/projs/vela')
out = pathlib.Path('/tmp/vela-startup-validation.syiSMk/bootstrap-consumption/regression')
out.mkdir(exist_ok=True)
source = repo / 'internal/modelruntime/remote_bootstrap_linux.go'
wire = source.read_text()
marker = '\t\t\t\treturn gate(ctx, intent)'
assert wire.count(marker) == 1
wire = wire.replace(marker, '''\t\t\t\treread, readErr := ReadRemoteRuntimeBootstrapFile(ctx, bootstrapPath)
\t\t\t\tif readErr != nil {
\t\t\t\t\treturn readErr
\t\t\t\t}
\t\t\t\tintent.BootstrapDigest = sha256.Sum256(reread)
''' + marker)
(out / 'reread-at-gate.go.txt').write_text(wire)
(out / 'overlay.json').write_text(json.dumps({'Replace': {
    '/workspace/internal/modelruntime/remote_bootstrap_linux.go': '/evidence/reread-at-gate.go.txt'
}}, indent=2) + '\n')
