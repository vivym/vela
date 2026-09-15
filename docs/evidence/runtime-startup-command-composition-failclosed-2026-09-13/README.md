# Runtime startup command composition fail-closed evidence

Date: 2026-09-13

The source-matched Linux amd64 `cmd/vela-node-agent` test binary was copied to
`marslab` for a focused read-only test run and removed after completion. The
run did not enable or restart any service and did not create a CRI workload.

Command:

```text
/tmp/vela-node-agent.test -test.run "TestComposeRuntimeStartupAuthority(FailsClosedOnPolicyIssuerLoss|PropagatesHelperFaults)|TestRuntimeStartupLifecycleCompositionReceiptRequiresOrchestration" -test.v -test.timeout=30s
```

Result:

```text
PASS
TestComposeRuntimeStartupAuthorityPropagatesHelperFaults/timeout
TestComposeRuntimeStartupAuthorityPropagatesHelperFaults/crash
TestComposeRuntimeStartupAuthorityFailsClosedOnPolicyIssuerLoss
TestRuntimeStartupLifecycleCompositionReceiptRequiresOrchestration
TestRuntimeStartupGateRecordsHelperFailuresAndCleansResources/timeout
TestRuntimeStartupGateRecordsHelperFailuresAndCleansResources/crash
```

The gate cases also verify that each helper failure writes exactly one verified
failed receipt and invokes resource cleanup. This receipt covers command-level
policy/launcher failure boundaries only. It
does not claim a successful production composition, because the target host
still lacks a provisioned signed plan, issuer, Node service and ModelRuntime.
