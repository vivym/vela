# Release supply-chain evidence binding

Date: 2026-08-29

Status: Repository conformance implemented by Slice 45.

Implementation: `3d384b0`; trust-boundary review closures: `247e6f7`,
`1b56e49`, `6757932`.

This slice makes registry publication, image signatures, SPDX SBOMs,
vulnerability scans, and independent vulnerability approval mandatory inputs
to launch verification and Catalog promotion. It validates evidence produced by
external release systems; it does not create a production key, sign an image,
generate an SBOM, run a scanner, approve a finding, publish to a production
registry, or change the current `0/9 PASS` Production Gate result.

## Rooted evidence graph

One strict `schema_version: 1` supply-chain manifest binds the canonical release
digest, configuration revision, registry publication receipts, and the complete
ordered OCI image set from the verified release bundle. Every manifest artifact
uses a canonical local relative path, is opened beneath the manifest root with
`O_NOFOLLOW` on every path component, is a bounded non-empty regular file, and
has a unique ownership role. The independently configured trust-policy file is
also opened without following its final path and is bound by its expected raw
SHA-256 digest. Metadata is limited to 16 MiB per file and 128 MiB across the
graph, with both pre-read file size and actual bytes read checked before budget
consumption.

The union of strict Slice 44 registry publication receipts must exactly equal
the release bundle image inventory. Each receipt image must reproduce the
bundle's repository digest reference, manifest digest, OCI media type, and byte
size. A missing image, extra image, duplicate publication, receipt digest
mismatch, or independently asserted descriptor fails closed.

## External trust policy

The strict trust policy is supplied out of band from the release artifacts. It
contains bounded, validity-windowed Ed25519 public keys for two distinct roles:

- release image signers; and
- vulnerability approvers.

The same public key cannot occupy both roles, even under different key IDs.
The policy also fixes the accepted scanner name/version pairs, maximum Critical
and High findings, and maximum scanner database age. Trust is therefore not
bootstrapped from a key or threshold embedded in the artifact being verified.
Policy rotation creates a new policy file and digest; an approval must bind the
exact raw policy digest that authorized it.

## Signed image statement

Every image has one standard DSSE envelope with exactly one trusted Ed25519
signature over the DSSE pre-authentication encoding. Its strict Vela payload
binds:

- the exact image digest reference;
- release digest and configuration revision;
- the registry publication receipt digest;
- the raw SPDX SBOM digest;
- the strict vulnerability report digest; and
- a canonical UTC signing time inside the signer's validity window.

The payload type is
`application/vnd.vela.supply-chain.image-statement.v1+json`. Unknown or
duplicate JSON keys, non-canonical base64, an untrusted key, an invalid
signature, or a mismatched binding is rejected.

## SPDX and vulnerability evidence

The SBOM must be an SPDX 2.3 JSON document with `CC0-1.0` data license, a valid
HTTPS document namespace, canonical creation time, and a described package that
exactly names the image repository and binds the manifest digest through both
`versionInfo` and a SHA-256 package checksum. The package must declare
`filesAnalyzed: false`; every package must carry a valid absolute-URI,
`NONE`, or `NOASSERTION` `downloadLocation`. The document may retain additional
standard SPDX fields, but duplicate keys remain forbidden throughout the JSON
graph.

The strict vulnerability report binds the image and SBOM digest, scanner
name/version, scanner database digest and update time, scan time, severity
counts, and a raw scanner-output file plus SHA-256 digest. The scanner must be
trusted, its database must meet the policy age, Critical and High counts must
meet the policy, and `unknown` findings must be zero.

An independent approver signs a second single-signature DSSE envelope with
payload type
`application/vnd.vela.supply-chain.vulnerability-approval.v1+json`. The payload
must say `APPROVED` and exactly bind the image, release/configuration pair, SBOM,
report, scanner/database identity, raw trust-policy digest, thresholds, database
age limit, and an approval time after both scan and image statement. The time
must fall within the approver key validity window.

## Launch and Catalog enforcement

`vela-verify-launch` now requires four inputs: canonical release bundle,
supply-chain manifest, supply-chain trust policy, and Launch Receipt manifest.
It prints `PASS 9/9` only after all three graphs bind the same release and
configuration. Its success output includes the verified supply-chain manifest
and trust-policy digests.

Catalog promotion plans move to strict `schema_version: 2` and require a rooted
`supply_chain_manifest_ref` in addition to `release_bundle_ref` and
`manifest_ref`. The plan cannot select its own trust root. The Catalog process
must instead receive a canonical absolute policy path and expected SHA-256 via
`VELA_CATALOG_PROMOTION_SUPPLY_CHAIN_POLICY` and
`VELA_CATALOG_PROMOTION_SUPPLY_CHAIN_POLICY_SHA256`. The service verifies the
complete supply-chain graph before opening a PostgreSQL transaction. Missing,
legacy, escaped, symlinked, malformed, tampered, expired, untrusted, incomplete,
unpinned, or policy-failing evidence therefore leaves all Catalog state
unchanged.

## Verification evidence

- unit tests cover complete two-image evidence, exact image-set enforcement,
  SBOM and DSSE tampering, vulnerability thresholds, signer/approver separation,
  key validity, strict approval ordering, scanner database age, path escape,
  symlinks, relative launch paths, SPDX download-location compatibility,
  unknown fields, duplicate JSON keys, and trust-policy digest pinning;
- focused race and vet checks cover the validator and both callers; and
- Catalog integration tests prove valid promotion/replay and pre-transaction
  rejection with zero durable rows for missing or tampered supply-chain
  evidence.

## Evidence boundary

Repository tests use generated test-only Ed25519 keys, local SPDX fixtures, a
synthetic scanner output, and local publication receipts. They prove validator
behavior only. The repository still contains no authorized production registry
receipt, external trust policy, production signature, production SBOM, real
scanner result, vulnerability approval, production H3 backend, deployment, or
Launch Receipt. Production Gates remain `0/9 PASS`.
