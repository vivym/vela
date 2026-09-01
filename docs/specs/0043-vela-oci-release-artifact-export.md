# Vela OCI release artifact export

Date: 2026-08-29

Status: Repository conformance implemented by Slice 43.

Implementation: `32718ac`.

This slice captures the four `linux/amd64` OCI platform manifests and config
blobs produced by the Slice 42 build seam as strict, digest-bound inputs for the
canonical release bundle. It does not publish to a registry, sign an image,
produce an SBOM, approve vulnerability findings, or change the current `0/9
PASS` Production Gate result.

## Build interface

`make build-vela-image-artifacts` requires the same revision, image prefix,
H3 runtime base, command context, and three command digests as
`make build-vela-images`, plus a canonical absolute
`RELEASE_ARTIFACT_DIR` whose final component does not exist. A successful build
prints the absolute path to `vela-images.json` and publishes exactly:

- `vela-images.json`;
- one `<target>.manifest.json` for each exact Vela image target; and
- one `<target>.config.json` for each exact Vela image target.

The strict manifest contains the exact four `releasebundle.OCIManifestInput`
values required by a Slice 40 build plan. Every image reference is lowercase,
tag-free, and pinned to the SHA-256 of the published platform manifest.

## OCI export and validation

Buildx exports one OCI image layout per target with provenance disabled for this
platform-artifact boundary. BuildKit receives read access only to the private H3
command context and write access only to the private OCI layout root. The
private command staging and independent Docker-stage verification from Slice 42
remain mandatory. Before publication, repository code requires each layout to
contain one OCI image manifest and recomputes the manifest, config, and layer
descriptor digests and sizes from the layout blobs.

The config must bind `linux/amd64`, numeric user `10001:10001`, the target's
exact absolute entrypoint, `org.opencontainers.image.title`, and the supplied
`org.opencontainers.image.revision`. The H3 stage runtime config must
additionally bind the exact `vela.ai.h3-runtime-base`,
`vela.ai.h3-encoder.sha256`, `vela.ai.h3-dit.sha256`, and
`vela.ai.h3-vae-decoder.sha256` values. The exact manifest and config
bytes are copied to the release artifact directory without JSON re-encoding.

## Publication safety

Construction and OCI layouts remain in a mode-`0700` sibling candidate. The
fixed nine-file inventory is reloaded through the production validation path,
all digests and runtime contracts are rechecked, and every file and directory is
synced. Publication uses the existing atomic no-replace boundary; build or
verification failure leaves no formal output, and an output created concurrently
is never replaced.

## Verification evidence

- focused and race tests cover the exact nine-file output, canonical release
  bundle validation, invalid layer and subject descriptors, missing rootfs,
  non-exact platform data, unexpected default commands, and symbolic-link
  rejection at both the layout root and blob-directory boundaries;
- the full repository generation, lint, test, cross-build, and deployment
  validation suite passes; and
- a real Buildx smoke through the configured Docker mirror built and loaded all
  four images from a temporary non-production three-command context; independent
  image inspection confirmed the final entrypoint, user, labels, and all three
  container-internal command digests. Focused tests cover artifact export and
  canonical reload; this smoke did not publish formal artifact output.

## Evidence boundary

These local artifacts establish the exact platform identities consumed by the
canonical release bundle. They are not registry receipts and do not establish
remote availability, signature identity, SBOM attachment, vulnerability
approval, production H3 provenance, deployment, or any Launch Receipt. Those
remain separate work and Production Gates remain `0/9 PASS`.
