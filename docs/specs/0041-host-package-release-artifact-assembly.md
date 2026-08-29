# Host package release artifact assembly

Date: 2026-08-28

Status: Repository conformance implemented by Slice 41 (`286eb29`).

This slice adds the repository-owned build boundary for the two host packages
required by the canonical release bundle. It turns the Node Agent and H3 Runner
sources into real, verified package artifacts instead of requiring an operator
to supply opaque fixture bytes. It does not build or publish OCI images, install
the packages on a host, publish a registry artifact, or change the current
`0/9 PASS` Production Gate result.

## Build interface

`make build-host-packages` requires a non-placeholder `RELEASE_REVISION` and a
canonical absolute `RELEASE_ARTIFACT_DIR` whose final component does not exist.
The source and output-parent paths may not contain symbolic links. A successful
build prints the absolute path to `host-packages.json` and produces exactly:

- `host-packages.json`;
- `h3-runner-contract.json`;
- `node-agent-contract.json`;
- `vela_h3_runner-0.1.0-py3-none-any.whl`;
- `vela-node-agent`.

The manifest exposes the exact two `releasebundle.PackageInput` values needed
by a Slice 40 build plan. Each strict package contract binds the caller-supplied
release revision, `linux/amd64`, the absolute installation entrypoint, artifact
SHA-256, and artifact size.

## Reproducible package construction

The Node Agent is built from `./cmd/vela-node-agent` with `CGO_ENABLED=0`,
`GOOS=linux`, `GOARCH=amd64`, read-only modules, trimmed paths, disabled VCS
embedding, and an empty Go build ID. Its published mode is `0755`.

The Runner wheel is built from `runner/pyproject.toml` through `uv build
--wheel --no-sources`. `SOURCE_DATE_EPOCH` is fixed at the minimum ZIP epoch,
the output name is exact, and its published mode is `0644`. Repeated builds from
the same source, toolchain, dependency set, and revision must produce the same
five-file inventory byte for byte.

## Verification and publication

Construction occurs in a mode-`0700` sibling candidate directory. Artifacts and
JSON metadata are individually synced. Before publication, the production path
strictly re-loads the exact inventory, rejects duplicate/unknown/trailing JSON,
recomputes every digest and size, and verifies:

- both exact package contracts and entrypoints;
- a mode-`0755` ELF64 x86-64 Node Agent;
- a safe Runner wheel containing the Runner, generated protobuf modules, and
  exact `vela-h3-runner` console entrypoint.

The candidate directory is synced and published atomically without replacement:
Darwin uses `RENAME_EXCL`, Linux uses `RENAME_NOREPLACE`, and unsupported
platforms fail closed. The parent directory is synced after publication. A
build or verification failure removes the candidate and leaves no formal
output; an output created concurrently remains owned by its creator.

## Verification evidence

- deployment-contract tests execute the Make target and inspect both real
  artifacts and strict contracts;
- repeated builds prove the exact inventory is byte-for-byte reproducible;
- negative tests cover placeholder revision, existing output, direct and
  ancestor symbolic links, invalid builder output, failed construction, and a
  concurrently created destination;
- focused race, vet, lint, Darwin and Linux no-replace tests pass;
- `make verify` passes generated-output checks, all Go and Python tests, lint,
  Linux/amd64 cross-build, and deployment rendering.

## Evidence boundary

This slice closes host-package assembly only. The proprietary H3 backend binary,
pinned `ffprobe`, four Vela OCI images, OCI descriptors, registry publication,
signatures, SBOMs, vulnerability approval, real PKI/Secrets, production RKE2/H3
deployment, fault exercises, and nine Launch Receipts remain separate work.
Repository tests and locally assembled artifacts do not satisfy a Production
Gate; the current result remains `0/9 PASS`.
