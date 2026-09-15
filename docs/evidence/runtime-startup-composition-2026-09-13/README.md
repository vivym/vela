# Runtime startup composition validation (2026-09-13)

This receipt records the first native Linux success-path composition test for
`RuntimeStartupAuthority.Prepare`. It uses the repository's non-root Runtime
fixture and a temporary observer fixture on `marslab` (Ubuntu 24.04,
kernel `6.8.0-137-generic`). The success path exercises the real Node ledger,
Fleet-reservation adapter, operation-bound authorization policy and startup
grant construction in one operation. Negative cases verify that invalid policy
evidence and missing Fleet authorization fail closed.

The receipt is explicitly `validation_only=true`. The Fleet registry, policy
issuer, journal and authorization bytes are test doubles, and the test does
not issue a ModelRuntime Permit or create a production Launch Receipt. The
temporary observer and test binary were removed from the target host after the
run.
