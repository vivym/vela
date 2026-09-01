# Vela OCI image build seam

Date: 2026-08-29

Status: Repository conformance implemented by Slice 42.

Implementation: `eeddbaa`.

This slice adds the repository-owned build boundary for four Vela OCI base
images. It builds real `linux/amd64` runtime images from the committed control,
fleet, Stage Worker, and ModelRuntime wrapper sources, while treating the
proprietary H3 drivers as external digest-bound image content.
It does not publish an image, supply a production backend, create registry or
signature evidence, or change the current `0/9 PASS` Production Gate result.

## Build interface

`make print-vela-image-build` prints the resolved Buildx Bake definition and
`make build-vela-images` builds and loads the exact four revision-tagged images:

- `vela-control`;
- `vela-fleet-controller`;
- `vela-model-runtime`;
- `vela-stage-worker-agent`;

Both targets require `RELEASE_REVISION` and `RELEASE_IMAGE_PREFIX`. A deployable
H3 runtime image is produced downstream by adding the certified driver binaries
to the `vela-model-runtime` base while preserving the exact
`/usr/local/bin/vela-model-runtime` entrypoint. The canonical release bundle
binds the final external image by digest and rejects any other entrypoint.

## Pinned build and runtime contract

The Dockerfile frontend and the Go and Debian base images are
digest-pinned. All targets are fixed to `linux/amd64`, carry the supplied OCI
revision label, run as numeric `10001:10001`, and use exact absolute
entrypoints. Go binaries use read-only modules, trimmed paths, disabled VCS
embedding, and an empty build ID. The ModelRuntime base contains no model driver;
the actuation launch manifest names the exact driver command in the final H3
image.

The control image includes `vela-control`, the separate Artifact Validator
sandbox helper, CA roots, and a static FFprobe 8.0.1 at `/usr/bin/ffprobe`.
FFmpeg source is fetched with an exact SHA-256 and built with a restricted
decoder, demuxer, parser, and protocol set. Direct Debian build packages are
version-pinned. Their moving repository metadata and transitive dependencies
are not a hermetic package snapshot, so this slice does not claim byte-for-byte
image reproducibility; the exported OCI manifest digest remains the release
artifact identity that Slice 43 must capture.

## Verification evidence

- deployment-contract tests validate the exact Bake group, platforms, tags,
  base digests, runtime stages, non-root users, entrypoints, FFmpeg identity,
  and ModelRuntime entrypoint;
- BuildKit Dockerfile checks cover all four targets;
- OCI artifact tests capture and reload all four exact manifest/config pairs.

## Evidence boundary

The proprietary production H3 backend has not been provided. The local smoke
images are not registry publication receipts and have not been pushed, signed,
SBOM-attested, vulnerability-approved, or imported into the canonical release
bundle. Third-party operational images also remain outside this build group.
Real PKI and Secret values, RKE2/H3 deployment, node materialization, fault and
rollback exercises, and all nine versioned Launch Receipts remain external
work. Repository tests and smoke images satisfy no Production Gate; the current
result remains `0/9 PASS`.
