# Runtime startup composition receipt replay evidence (2026-09-13)

This validation evidence exercises the real Linux `internal/nodeagent`
composition path with root-owned subprocess fixtures. The success path creates
one Fleet reservation, consumes one durable startup grant, receives a Permit,
and then persists the resulting typed composition receipt to a temporary file.
The test reopens that file and verifies both the receipt self-digest and its
`operation_id`/journal/request/reservation/authorization binding.

Command on a Linux amd64 validation host (the test is excluded by the local
macOS build constraints):

```text
go test -c ./internal/nodeagent -o /tmp/vela-nodeagent.test
/tmp/vela-nodeagent.test -test.run '^TestRuntimeStartupAuthorityPrepareComposesReservationAndGrant$' -test.count=1 -test.v
```

The native run requires the disposable `/exec-observer` and `/exec-probe`
fixtures on the validation host; without them the Linux test intentionally
skips before creating any workload.

The exact test state was subsequently rerun as a Linux amd64 root process on
`marslab`, with source-matched temporary `/exec-observer` and `/exec-probe`
fixtures. The selected tests all passed:

```text
TestRuntimeStartupAuthorityPrepareComposesReservationAndGrant: PASS
TestRuntimeStartupCompositionNodeRestartDoesNotReissueAuthority: PASS
TestRuntimeStartupAuthorityCompositionFailureMatrix: PASS
  policy-response-loss: PASS
  caller-replacement: PASS
  observer-loss: PASS
```

The replay assertion computes the backend request digest from the independently
retained startup request and the reservation digest from the durable reservation
record JSON before calling `VerifyBinding`. Temporary binaries and fixtures were
removed after the run.

On a host without those disposable fixtures, the same command intentionally
returns:

```text
SKIP: requires native exec-observer
PASS (no test failures; no workload created)
```

The fixture uses real subprocess pidfds, observer custody and the production
reservation/orchestration methods. It is still validation-only: the Fleet
registry, policy issuer and backend are test fixtures, so this receipt does not
promote `Production Gates` or replace a source-matched production composition.
