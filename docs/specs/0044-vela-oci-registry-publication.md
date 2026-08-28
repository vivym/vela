# Vela OCI registry publication

Date: 2026-08-29

Status: Repository conformance implemented by Slice 44.

Implementation: `91e883f`.

This slice extends the Slice 43 export boundary from a fully validated local
OCI layout to an immutable registry manifest. It publishes the exact four Vela
`linux/amd64` images by digest and writes a strict local publication receipt. It
does not create a mutable tag, publish to a production registry during
repository verification, sign an image, attach an SBOM, approve vulnerability
findings, or change the current `0/9 PASS` Production Gate result.

## Publication interface

`make publish-vela-images` requires the same revision, image prefix, external
H3 backend, and new canonical absolute `RELEASE_ARTIFACT_DIR` as
`make build-vela-image-artifacts`. The command builds one private OCI layout per
target, produces and validates the unchanged Slice 43 nine-file artifact set,
then publishes the same layout bytes. A successful operation atomically
publishes a ten-file local directory containing those nine files plus
`vela-registry-publication.json`; stdout prints the receipt path.

Registry credentials are resolved only through the Docker default keychain.
They are not command arguments, environment bindings created by this command,
receipt fields, or log fields. Registry HTTP uses the standard proxy-aware Go
transport, so an operator-controlled `HTTP_PROXY`, `HTTPS_PROXY`, and
`NO_PROXY` policy remains effective without changing the release contract.

## Validate before write

No registry request occurs until all four Buildx layouts pass the Slice 43
checks: exact index and platform, manifest/config/layer digest and size,
bounded descriptors, runtime user and entrypoint, release labels, H3 backend
binding, fixed nine-file inventory, and canonical release-bundle validation.
The publisher then reloads each image from that same private layout by the
already verified manifest digest and compares its raw manifest bytes with the
local release artifact before any network operation.

Publication uses only `<repository>@sha256:<digest>` references. It never sends
a tag manifest. Layers and manifests stream through `go-containerregistry`
with one upload job, and an existing digest is reused rather than assigned a
second identity. After each upload, the command performs a registry GET by
digest and requires the returned raw manifest bytes, computed digest, byte
size, and OCI media type to match the local manifest exactly.

## Receipt and failure semantics

The strict schema-version-1 receipt binds the release revision and, for each
ordered image, its complete digest reference, manifest digest, OCI media type,
and manifest size. The receipt and all Slice 43 artifacts remain mode `0600` in
a mode-`0700` sibling candidate. The exact ten-file directory is synced and
published with the existing atomic no-replace boundary.

A local-validation, authentication, upload, or readback failure creates no
formal local output and no receipt. Earlier immutable digests from a partial
remote operation may remain in the registry; they are safe to upload again for
the same byte-identical layout but are not claimed as a completed release.
Slice 42 does not guarantee a byte-reproducible rebuild, so digest idempotency
does not turn a later independent Buildx run into the same artifact identity.

## Verification evidence

- in-process OCI Distribution tests cover exact digest-only PUTs, successful
  raw-manifest readback, byte-identical idempotent retry, partial failure and
  retry, changed remote bytes, and changed local layout bytes before any
  network request;
- a Basic-auth registry test proves keychain resolution while confirming that
  neither username nor password enters the receipt;
- focused tests, race, vet, lint, the full repository verification suite, and
  the `linux/amd64` cross-test pass with `go-containerregistry v0.22.0`; and
- a real Buildx smoke through the configured `docker.1ms.run` mirror rebuilt
  and exported all four layouts from a temporary non-production backend, then
  produced the exact nine mode-`0600` pre-publication artifacts.

## Evidence boundary

The registry tests use only an in-process non-production server. No external
or production registry was mutated, and the repository contains no actual
production publication receipt. Production repository provisioning,
credentials, retention and availability policy, signature identity, SBOM,
vulnerability approval, the proprietary production H3 backend, deployment,
and all versioned Launch Receipts remain separate work. Production Gates remain
`0/9 PASS`.
