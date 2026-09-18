package nodeagent

import "errors"

// ErrRuntimeStartupLedger is exposed on every build target so the command
// boundary can classify unrecoverable startup state consistently. The Linux
// implementation adds the detailed ledger checks and wraps this sentinel.
var ErrRuntimeStartupLedger = errors.New("runtime startup ledger is unavailable, changed or incomplete")
