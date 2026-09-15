# Node composition restart validation (2026-09-13)

Validation-only evidence; `Production Gates` remains `0/9`.

The Linux amd64 `internal/nodeagent` test binary was run as root on `marslab`
with temporary `exec-observer` and `exec-probe` helpers. The test completed a
real fixture reservation and backend startup, closed the orchestration and
ledger, reopened the ledger as a restarted Node, and attempted the same startup
again.

Observed result:

```text
=== RUN   TestRuntimeStartupCompositionNodeRestartDoesNotReissueAuthority
--- PASS: TestRuntimeStartupCompositionNodeRestartDoesNotReissueAuthority (0.61s)
PASS
```

The restarted ledger retained the original reservation and grant attempt,
rejected the duplicate startup with `ErrRuntimeStartupRecorded`, and did not
reissue a Fleet reservation. Temporary helper binaries were removed afterward.
This covers Node-side durable restart semantics; it does not prove the
command-level resource loader, production Fleet PostgreSQL, or a production
ModelRuntime image.

The same source-matched binary also reran the composition failure matrix after
fixing its Fleet fixture to return the required policy-authorization wire. The
`policy-response-loss`, `caller-replacement`, and `observer-loss` cases all
passed and reached their intended failure boundary.
