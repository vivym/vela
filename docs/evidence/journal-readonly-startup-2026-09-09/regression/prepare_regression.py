import json
import pathlib

repo = pathlib.Path('/Users/viv/projs/vela')
out = pathlib.Path('/tmp/vela-startup-validation.syiSMk/journal-readonly/regression')
out.mkdir(exist_ok=True)
source = (repo / 'internal/nodeagent/journal_endpoint_linux.go').read_text()
marker = 'if endpoint.readOnly {'
assert source.count(marker) == 1
(out / 'readonly-check-disabled.go.txt').write_text(source.replace(marker, 'if false && endpoint.readOnly {'))
(out / 'overlay.json').write_text(json.dumps({'Replace': {
    '/workspace/internal/nodeagent/journal_endpoint_linux.go': '/evidence/readonly-check-disabled.go.txt'
}}, indent=2) + '\n')
