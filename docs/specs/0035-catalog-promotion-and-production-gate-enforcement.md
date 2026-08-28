# Catalog promotion and Production Gate enforcement

Date: 2026-08-28

Status: Repository conformance implemented at `aa0ea62` (initial feature at
`c09de0f`, fixed-point Standards and Spec review closure at `aa0ea62`).

This slice turns the Production Gate receipt contract and three-Preset Catalog
requirements into fail-closed release tooling and database authority. It
advances ADRs 0013, 0017, 0018, and 0029. It does not create a production
Launch Receipt, run a benchmark, authorize production traffic, or change the
current `0/9 PASS` result.

Slice 39 (`7829104`) subsequently hardens the eight non-observability evidence
files into typed semantic envelopes and requires the promotion plan to exactly
match verified certification values and full RateCard binding pairs before the
transaction begins. See
`docs/specs/0039-production-gate-typed-evidence.md`.

## Governing boundary

1. One launch manifest contains exactly the nine stable Production Gate
   receipts for one release digest and one configuration revision. Every receipt
   must independently pass the existing receipt contract.
2. `evidence_ref` is a local path beneath the manifest directory. Verification
   opens it through a rooted filesystem handle, requires a regular file, and
   recomputes SHA-256 from the evidence bytes. A digest-shaped string is not
   evidence.
3. JSON manifests and promotion plans are bounded to 1 MiB, contain exactly one
   document, reject unknown fields, and reject duplicate object keys at every
   nesting level.
4. A sealed manifest and its receipts are immutable. A manifest seals only when
   all nine unique gates are stored as `PASS` with the same release and
   configuration binding.
5. `quality`, `balanced`, and `fast` ProfileCertification revisions are promoted
   independently. Each freezes backend revision, driver baseline, benchmark
   corpus, quality, success rate, p50, p95, cost, confidence, and the exact
   sealed `preset-certification` receipt.
6. A RateCardRevision becomes `ACTIVE` only when every saleable model,
   ServiceClassRevision, OutputSpec, and currency group has exactly all three
   evidenced Presets under the same launch receipt.
7. Once the Catalog evidence protocol switches from `LEGACY` to `EVIDENCED`,
   every new `ACTIVE` Catalog revision and every mutation of an active RateCard
   line is checked against immutable release evidence. The switch is one-way;
   its receipt records the initial transition witness, while each later release
   remains independently bound through its own certification evidence and
   RateCard release bindings.
8. Repository tests and local evidence fixtures never become production
   receipts. Production remains closed until nine real versioned receipts pass.

## Release interfaces

The read-only verifier is an explicit release command:

```text
make verify-launch \
  RELEASE_BUNDLE=/absolute/path/to/release-bundle.json \
  LAUNCH_RECEIPTS=/absolute/path/to/launch-receipts.json
```

Slice 40 requires the canonical release bundle as the source of the expected
release digest and configuration revision. The command exits nonzero for an
invalid bundle, a missing gate, failed gate, mixed or mismatched
release/configuration, malformed or ambiguous JSON, path escape, non-regular
evidence object, or evidence digest mismatch. Success prints only the verified
release, configuration, manifest digest, and `PASS 9/9` result.

Catalog promotion is a separate database-mutating command:

```text
VELA_CATALOG_PROMOTION_DATABASE_URL=<dedicated login DSN> \
  go run ./cmd/vela-catalog-promoter /absolute/path/to/catalog-promotion.json
```

The plan references both the launch manifest and canonical release bundle
relative to its own directory and lists the ProfileCertification evidence and
RateCard bindings for the release. The command verifies all files and requires
exact bundle/manifest identity before opening one PostgreSQL transaction.
Receipt ingest, manifest sealing, certification promotion, RateCard promotion,
and the protocol switch either commit together or leave no durable effect.

Database credentials come only from
`VELA_CATALOG_PROMOTION_DATABASE_URL`. They are not accepted in the plan or
written to output.

## Database authority

Migration 00032 adds:

- `production_gate_manifests` and `production_gate_receipts`;
- `inference_backend_revisions`;
- `profile_certification_evidence`;
- `rate_card_release_bindings`; and
- singleton Catalog protocol state plus immutable transition history.

Evidence and history tables use forced RLS, immutable triggers, exact digest and
text constraints, and owner-controlled security-definer functions. The legacy
Catalog remains readable during expansion. The `EVIDENCED` switch is allowed
only after the exact receipt covers three independent Presets and the active
RateCard. Catalog `ACTIVE` writers and RateCard-line writers serialize with the
one-way switch through the singleton protocol row, so a concurrent legacy write
cannot commit outside either side of the evidence boundary.

The five mutation seams are:

```text
vela_record_production_gate_receipt(...)
vela_seal_production_gate_manifest(bytea)
vela_promote_profile_certification(...)
vela_promote_rate_card(uuid, uuid, uuid)
vela_enable_evidenced_catalog(uuid)
```

All functions pin `search_path`, revoke `PUBLIC EXECUTE`, and are owned by
`vela_catalog_promotion_owner`. Private receipt and RateCard coverage helpers
are also inaccessible to the runtime login.

## Least privilege

`vela_catalog_promotion` is NOLOGIN, non-superuser, non-BYPASSRLS, owns no
object, and can execute exactly the five mutation seams. It has no direct table,
column, sequence, or private-schema privilege.

`vela_catalog_promotion_owner` is NOLOGIN/BYPASSRLS and is inherited by no
login. It owns the evidence tables and functions and receives only the Catalog
read and state-column privileges needed by those functions. Existing runtime
logins do not inherit either role.

`catalogpromotion.New` performs an exact privilege inventory before accepting a
pool. A login with another runtime membership, owner inheritance, cluster
privilege, table DML, private-schema access, or an additional public function
fails startup.

## Compatibility and rollback

Migrations 00001-00031 remain unchanged. Migration 00032 is additive while the
protocol is `LEGACY`. The exact previous release at
`e53c62054d068aea19ba7860417698a203ed5225` starts against schema 32 and ignores
the new Catalog authority. The current promoter fails closed against schema 31
because its exact function privilege boundary is absent.

Empty `32 -> 31 -> 32` removes and restores the complete authority. Structural
Down takes exclusive locks and refuses with SQLSTATE `55000` when any manifest,
receipt, backend revision, certification evidence, RateCard binding, or
`EVIDENCED` protocol state exists. Durable release evidence cannot be silently
discarded.

## Required evidence

- complete same-release/same-configuration `PASS 9/9` manifest acceptance;
- missing, duplicate, failed, mixed-binding, unknown-field, trailing-document,
  duplicate-key, path-escape, symlink-escape, non-file, and digest-mismatch
  rejection;
- exact evidence-byte SHA-256 recomputation before database mutation;
- three independent Preset promotions with quality, success-rate, p95, cost,
  and confidence threshold enforcement;
- RateCard rejection until every saleable group contains exactly `quality`,
  `balanced`, and `fast` evidence from the same receipt;
- active Catalog and RateCard-line mutation guards after the protocol switch;
- exact plan replay without duplicate evidence or transition rows;
- a later release can promote its own complete evidence without rewriting the
  immutable initial protocol transition;
- the protocol switch serializes with concurrent unreceipted `ACTIVE` writes in
  both commit orders;
- RateCard rejection for duplicate stable-Preset revisions, any fourth line in
  a saleable group, or a referenced Preset revision that is not `ACTIVE`;
- failure at any promotion stage rolls back receipt ingest and every later
  mutation;
- runtime exact-function privilege, no direct table access, no owner
  inheritance, fixed function owner/search path, and no `PUBLIC EXECUTE`;
- empty migration Down/Up, durable-evidence Down refusal, exact N-1 startup on
  schema 32, and current promoter fail-closed behavior on schema 31; and
- generated output, focused, race, unit, integration, cross-build, deployment,
  and two-axis fixed-point review with no unresolved P0-P2 finding.

## Evidence boundary

The repository provides validation and promotion machinery, not the evidence
it consumes. Real H3 benchmarks, production configuration and image digests,
all nine raw evidence bundles, independent owners, operational exercises, and
the resulting versioned Launch Receipts remain external. Production Gates
remain `0/9 PASS`, and no Catalog item in this repository is thereby certified
for production traffic.
