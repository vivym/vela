# Native runtime startup composition validation v2

This receipt covers the current Linux amd64 internal composition fixture on
`marslab` (Ubuntu 24.04, kernel `6.8.0-137-generic`). The fixture uses real
subprocess pidfds, observer custody, the Node ledger, Fleet reservation adapter,
authorization policy boundary, startup grant and fake backend Permit. It also
replays the typed composition receipt against independently derived backend
request and durable reservation digests.

Selected tests passed:

```text
TestRuntimeStartupAuthorityPrepareComposesReservationAndGrant
TestRuntimeStartupOrchestrationExposesImmutableExpectedRequest
TestRuntimeStartupCompositionNodeRestartDoesNotReissueAuthority
TestRuntimeStartupAuthorityCompositionFailureMatrix
  policy-response-loss
  caller-replacement
  observer-loss
```

The run is validation-only. Fleet, policy issuer and ModelRuntime are fixtures,
so it does not create a production Launch Receipt or change `Production Gates`.
Temporary observer binaries and the Linux test binary were removed after the
run.
