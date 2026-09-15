# Failure receipt authority snapshot validation (2026-09-13)

Validation-only evidence; `Production Gates` remains `0/9`.

The Linux amd64 command test injected a permitted composition, returned a
runtime wait error, and made `Shutdown` revoke the live grant. The gate now
captures the composition receipt before shutdown and writes both the success
composition receipt and the failed backend receipt.

The test passed and verified that the failure receipt retained
`PermitIssued=true`, `GrantCreated=true`, the request/reservation/authorization
digests, and a valid self digest. This prevents post-shutdown revocation from
erasing the historical fact that a Permit had already been issued.
