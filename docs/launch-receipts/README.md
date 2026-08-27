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

## Verification and Catalog promotion

Store the nine receipts in one strict `schema_version: 1` manifest with a
`receipts` array. Each `evidence_ref` is relative to the manifest directory and
must resolve to a regular file beneath that directory. Verification recomputes
the SHA-256 digest of every evidence file, rejects duplicate or unknown JSON
keys, and requires all receipts to bind the same release digest and
configuration revision.

Run the read-only release check with:

```text
make verify-launch LAUNCH_RECEIPTS=/absolute/path/to/launch-receipts.json
```

Catalog promotion is a separate, mutating operation. It uses a strict promotion
plan and the dedicated `vela_catalog_promotion` login:

```text
VELA_CATALOG_PROMOTION_DATABASE_URL=<dedicated login DSN> \
  go run ./cmd/vela-catalog-promoter /absolute/path/to/catalog-promotion.json
```

The promotion transaction ingests and seals the manifest, records independent
three-Preset certification evidence, promotes the release RateCard, and enables
the fail-closed `EVIDENCED` Catalog protocol atomically. The repository contains
no production plan or credential. See
`docs/specs/0035-catalog-promotion-and-production-gate-enforcement.md` for the
complete contract.
