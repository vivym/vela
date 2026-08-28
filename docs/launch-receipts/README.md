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

The eight non-observability gates also require
`vela.production-gates/<gate>/v1` typed semantic evidence. Each envelope binds
the receipt owner, environment, exact time window, release, and configuration;
uses a fixed check/measurement/artifact inventory; and replaces free-form
receipt summaries with canonical verifier output. Every referenced artifact is
a strict typed payload whose observations must exactly reproduce the envelope.
Artifacts are limited to 16 MiB each and 128 MiB across the manifest.

`preset-certification` evidence additionally lists the authoritative saleable
groups, exactly `quality`, `balanced`, and `fast` certification claims for each
group, and full RateCard binding pairs. A Catalog promotion plan must exactly
match those verified claims before a database transaction begins. The existing
`observability-on-call` schema remains in `internal/sloevidence`.

Build and independently re-verify the canonical release bundle from a plan and
rooted artifact directory with:

```text
make build-release-bundle \
  RELEASE_BUNDLE_PLAN=/absolute/path/to/release/bundle-plan.json \
  RELEASE_BUNDLE=/absolute/path/to/release/release-bundle.json
make verify-release-bundle \
  RELEASE_BUNDLE=/absolute/path/to/release/release-bundle.json
```

The build plan, output bundle, and every referenced artifact must reside in the
same rooted directory. The bundle derives the release digest and configuration
revision from the exact final renders, host packages, Node Agent unit, Worker
materializations, external resource revisions, and OCI manifest/config bytes.
The command verifies its temporary candidate before atomic replacement.

Run the read-only launch check with both canonical artifacts:

```text
make verify-launch \
  RELEASE_BUNDLE=/absolute/path/to/release/release-bundle.json \
  LAUNCH_RECEIPTS=/absolute/path/to/release/launch-receipts.json
```

Catalog promotion is a separate, mutating operation. Its strict plan includes
both `manifest_ref` and `release_bundle_ref`, rooted beneath the plan directory,
and uses the dedicated `vela_catalog_promotion` login:

```text
VELA_CATALOG_PROMOTION_DATABASE_URL=<dedicated login DSN> \
  go run ./cmd/vela-catalog-promoter /absolute/path/to/catalog-promotion.json
```

Before opening the transaction, the service rebuilds the release bundle and
requires its release digest and configuration revision to match the verified
receipt manifest exactly. The promotion transaction then ingests and seals the
manifest, records independent three-Preset certification evidence, promotes the
release RateCard, and enables the fail-closed `EVIDENCED` Catalog protocol
atomically. The repository contains no production plan, bundle, receipt, or
credential. See
`docs/specs/0035-catalog-promotion-and-production-gate-enforcement.md` for the
database authority and
`docs/specs/0039-production-gate-typed-evidence.md` for the typed evidence
contract. The canonical release contract is documented in
`docs/specs/0040-production-release-bundle-and-configuration-revision-closure.md`.

Registry publication, signatures, SBOMs, vulnerability approval, real Secret
and PKI material, production node materialization, and the nine real exercises
remain external release responsibilities. A repository bundle or fixture is
not a Launch Receipt and does not advance `0/9 PASS`.
