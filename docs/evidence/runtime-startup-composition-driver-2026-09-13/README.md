# Runtime startup composition driver (2026-09-13)

This validation receipt covers the shared Linux composition driver in
`internal/nodeagent/runtime_startup_composition_matrix_linux_test.go`. The
driver uses the existing signed launch-plan fixture, real subprocess pidfds,
observer custody, the Node startup ledger, the reservation/orchestration
implementation and a disposable Fleet/policy boundary.

The success case performs the authenticated startup exchange, consumes one
operation-bound grant, mutates the paired Worker journal through the retained
endpoint, serializes and independently verifies the composition receipt, then
closes the operation and rejects a duplicate startup. The same driver also
checks policy response loss, caller replacement, observer loss and a simulated
Node restart that reopens the durable ledger without reissuing authority. No
production service, Kubernetes workload or Fleet database is touched.

Remote execution on `marslab` (Ubuntu 24.04, kernel `6.8.0-137-generic`):

```text
go test -run '^(TestRuntimeStartupCompositionDriver|TestRuntimeNamespaceOwnerFromPIDFDRejectsWrongCredentials)$' -count=1
PASS
```

The first replay against the minimal validation image exposed two fixture
assumptions: `/run` was absent and the long `t.TempDir()` path exceeded the
Unix socket limit. The disposable endpoint now uses a short `/tmp` directory;
the corrected source-matched Linux amd64 binary was rerun in the same image
with `SYS_ADMIN`/`SYS_PTRACE` and passed the four composition scenarios plus
the wrong-pidfd-credentials regression. No host service or workload was
changed.

The test binary was cross-compiled for Linux amd64 from the current source;
the temporary binary was removed after execution.

This is validation evidence only. It does not advance `Production Gates` and
does not replace the later command-level run with signed production inputs.
