import json
import pathlib

repo = pathlib.Path('/Users/viv/projs/vela')
out = pathlib.Path('/tmp/vela-startup-validation.syiSMk/remote-cli-reservation/regression')
out.mkdir(exist_ok=True)
source = (repo / 'internal/nodeagent/runtime_remote_cli_linux.go').read_text()
marker = '|| !bytes.Equal(wire,'
assert source.count(marker) == 1
(out / 'proc-vector-check-disabled.go.txt').write_text(source.replace(marker, '|| false && !bytes.Equal(wire,'))
(out / 'overlay.json').write_text(json.dumps({'Replace': {
    '/workspace/internal/nodeagent/runtime_remote_cli_linux.go': '/evidence/proc-vector-check-disabled.go.txt'
}}, indent=2) + '\n')
