# Vela OCI image build seam

Date: 2026-08-29

Status: Repository conformance implemented by Slice 42.

Implementation: `eeddbaa`.

This slice adds the repository-owned build boundary for the four Vela OCI
images required by the canonical release bundle. It builds real `linux/amd64`
runtime images from the committed control, fleet, Worker, and Runner sources,
while treating the proprietary H3 backend as an external digest-bound input.
It does not publish an image, supply a production backend, create registry or
signature evidence, or change the current `0/9 PASS` Production Gate result.

## Build interface

`make print-vela-image-build` prints the resolved Buildx Bake definition and
`make build-vela-images` builds and loads the exact four revision-tagged images:

- `vela-control`;
- `vela-fleet-controller`;
- `vela-worker-agent`;
- `vela-h3-runner`.

Both targets require `RELEASE_REVISION`, `RELEASE_IMAGE_PREFIX`, an absolute
canonical `H3_BACKEND_CONTEXT`, and a lowercase `H3_BACKEND_SHA256`. The backend
context may contain exactly one non-symlink regular file named `h3-backend`.
Before Buildx receives it, repository code streams and verifies its digest and
requires a little-endian `ELF64 x86-64` executable. Buildx receives only that
explicitly allowed external context. The Docker build independently rechecks
the digest and ELF identity before installing the backend.

## Pinned build and runtime contract

The Dockerfile frontend and the Go, Python, Debian, and uv base images are
digest-pinned. All targets are fixed to `linux/amd64`, carry the supplied OCI
revision label, run as numeric `10001:10001`, and use exact absolute
entrypoints. Go binaries use read-only modules, trimmed paths, disabled VCS
embedding, and an empty build ID. The Runner environment is produced from the
locked Python project with the exact console entrypoint and includes the
verified backend at `/opt/vela/bin/h3-backend`.

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
  and backend verification path;
- negative tests reject a missing backend context or file, digest mismatch,
  non-ELF input, and symbolic-link input before Buildx;
- BuildKit Dockerfile checks complete for all four targets without warnings;
- a local `linux/amd64` smoke build assembles all four images, confirms their
  OCI labels and entrypoints, executes FFprobe 8.0.1 and the two embedded helper
  binaries, and uses only a temporary non-production backend for assembly
  validation.

## Evidence boundary

The proprietary production H3 backend has not been provided. The local smoke
images are not registry publication receipts and have not been pushed, signed,
SBOM-attested, vulnerability-approved, or imported into the canonical release
bundle. Third-party operational images also remain outside this build group.
Real PKI and Secret values, RKE2/H3 deployment, node materialization, fault and
rollback exercises, and all nine versioned Launch Receipts remain external
work. Repository tests and smoke images satisfy no Production Gate; the current
result remains `0/9 PASS`.
