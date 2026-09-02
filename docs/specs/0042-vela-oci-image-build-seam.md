# Vela OCI image build seam

Date: 2026-08-29

Status: Repository conformance implemented by Slice 42 and extended for split
H3 ModelRuntime composition.

Implementation: `eeddbaa`.

This slice adds the repository-owned build boundary for four final Vela OCI
images. It builds real `linux/amd64` runtime images from the committed control,
Fleet, Stage Worker, and ModelRuntime wrapper sources, while treating the
proprietary H3 dependency/model base and stage commands as external
digest-bound inputs.
It does not publish an image, supply a production backend, create registry or
signature evidence, or change the current `0/9 PASS` Production Gate result.

## Build interface

`make print-vela-image-build` prints the resolved Buildx Bake definition and
`make build-vela-images` builds and loads the exact four revision-tagged images:

- `vela-control`;
- `vela-fleet-controller`;
- `vela-h3-stage-runtime`;
- `vela-stage-worker-agent`;

Both targets require `RELEASE_REVISION`, `RELEASE_IMAGE_PREFIX`, a tag-free
SHA-256-pinned `H3_RUNTIME_BASE`, an absolute canonical
`H3_RUNTIME_COMMAND_CONTEXT`, and the independent `H3_ENCODER_SHA256`,
`H3_DIT_SHA256`, and `H3_VAE_DECODER_SHA256` values. The command context must
contain exactly three non-symlinked executable ELF64/x86-64 files named
`h3-encoder`, `h3-dit`, and `h3-vae-decoder`. Buildx receives read authority
only for that exact context.

## Pinned build and runtime contract

The Dockerfile frontend and the Go and Debian base images are
digest-pinned. All targets are fixed to `linux/amd64`, carry the supplied OCI
revision label, run as numeric `10001:10001`, and use exact absolute
entrypoints. Go binaries use read-only modules, trimmed paths, disabled VCS
embedding, and an empty build ID. An independent Docker stage repeats command
inventory, digest, mode, ELF architecture, and executable-entry validation
before installing the three commands at `/opt/vela/bin`. The final runtime
inherits the exact H3 base, adds the Vela wrapper, runs as `10001:10001`, clears
the base image command, and preserves the exact
`/usr/local/bin/vela-model-runtime` entrypoint.

The final OCI config binds the H3 base reference and all three command digests
in `vela.ai.h3-*` labels. OCI artifact export validates those values against
the original build request, while canonical release-bundle validation
independently rejects a tagged base, missing label, malformed digest, alternate
entrypoint, or default command.

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
  base and command digests, exact command context entitlement, runtime stages,
  non-root users, entrypoints, FFmpeg identity, and ModelRuntime entrypoint;
- BuildKit Dockerfile checks cover all four targets;
- OCI artifact tests capture and reload all four exact manifest/config pairs.

## Evidence boundary

The proprietary production H3 runtime base, commands, models, and weights have
not been provided. A local BuildKit smoke used the repository mock ELF for all
three commands and a pinned Debian base; it proved only image composition and
container-internal digest identity. The smoke images are not registry
publication receipts and have not been pushed, signed,
SBOM-attested, vulnerability-approved, or imported into the canonical release
bundle. Third-party operational images also remain outside this build group.
Real PKI and Secret values, RKE2/H3 deployment, node materialization, fault and
rollback exercises, and all nine versioned Launch Receipts remain external
work. Repository tests and smoke images satisfy no Production Gate; the current
result remains `0/9 PASS`.

The external fast-h3 launcher, environment, warmup, release-input, and
cross-worktree protocol contracts are recorded in
`docs/fast-h3-runtime-conformance.md`. That conformance does not change the
external provenance or Production Gate boundary above.
