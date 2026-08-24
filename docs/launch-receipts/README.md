# Production Gate Receipts

Production traffic remains closed until all nine gates have one valid,
versioned PASS receipt. The receipt validator is implemented in
`internal/productiongates` and deliberately treats a missing, malformed,
duplicate, or failed receipt as non-PASS.

Every receipt must bind:

- the release OCI digest;
- the configuration revision;
- the validation environment;
- `PASS` or `FAIL`;
- the owner and exact acceptance threshold;
- the observed result;
- an evidence reference and SHA-256 evidence digest; and
- ordered start, completion, and recording timestamps.

The nine stable gate IDs are:

```text
preset-certification
real-h3-soak
state-event-fault-injection
gpu-remediation
organization-isolation-content-safety
data-disaster-recovery
release-rollback
commercial-data-lifecycle
observability-on-call
```

The repository currently has no production receipt files. Repository tests and
local Testcontainers runs are not launch receipts; the current implementation
status therefore remains `0/9 PASS`.
