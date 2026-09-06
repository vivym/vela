# OCI layer content identity verification

This increment follows `206ab39`. Investigation of the release input needed for
effective Runtime executable verification found a separate, reproducible defect
in the existing artifact-export path. It is repaired before deriving any new
process-startup authority from those image artifacts.

## Reproduced failure

`validateOCIImageLayout` recomputed the stored manifest, config and layer blob
hashes. Its config validator required SHA-256-shaped `rootfs.diff_ids` and the
correct layer count, but did not compute decoded layer hashes or compare the
corresponding DiffIDs. A self-consistent manifest/config pair could therefore
declare a rootfs identity unrelated to its actual layer bytes.

The regression runs the existing `make build-vela-image-artifacts` command with
a local fake-Docker layout export. It changes one config DiffID and updates the
config and manifest descriptors so all stored blob digests remain valid.
Before the repair, this command published the formal artifact directory:

```text
go test ./internal/deploymentcontract \
  -run '^TestBuildVelaImageArtifactsRejectsUnboundLayerDiffID$' -count=1 -v

FAIL: build accepted an unrelated DiffID with valid manifest/config/blob digests
```

After the repair, the same test requires the specific uncompressed-layer digest
error and verifies that no formal artifact directory exists. The positive
fixture now contains actual tar bytes and their correct DiffID; it previously
used arbitrary plain bytes and a placeholder DiffID.

## Implemented boundary

Artifact capture validates the exact image config before checking its layers.
For each ordered layer it opens the existing bounded, non-symlink local blob
and hashes both the stored stream and its decoded stream during the same read.
The stored size and SHA-256 must match the manifest descriptor. The decoded
SHA-256 must match `rootfs.diff_ids` at that exact position. No new declared
executable digest, receipt type or independently supplied allowlist is added.

The decoder follows the existing supported OCI media types: uncompressed,
gzip and zstd. Gzip uses Go's standard decoder; zstd uses the already pinned
`github.com/klauspost/compress v1.19.2`, now a direct dependency at the same
version. All compression frames are consumed and hashed, including decoded
bytes after a tar end marker. Invalid headers, checksums and truncated streams
fail validation. This is content hashing, not tar parsing or extraction.

Each image retains the existing aggregate 8 GiB stored-layer limit and gains a
32 GiB decoded-byte limit shared by all its layers. The stream reads at most one
decoded byte past the remaining budget before rejecting. It does not buffer the
whole layer. Zstd uses one decoder worker, low-memory mode and a 64 MiB maximum
window; this is a decoder window bound, not a measured total-process memory cap.
These are explicit supported export limits, not measured production sizing.

The build context reaches layer validation and is checked before and between
reads and before successful return. This provides cooperative cancellation;
it does not interrupt an operating-system file read that is already blocked.

## Validation

The CPU tests cover all three formats at the exact expansion boundary, wrong
stored and decoded digests, short/long descriptor sizes, truncated data,
unsupported media types, forbidden external/embedded descriptor content,
empty output, corrupt trailers, trailing junk, concatenated frames and full
decoded-stream identity. Mixed-format on-disk layers verify ordered DiffIDs,
count agreement and the aggregate decoded budget with small fixtures.
Cancellation before/during a read and injected read errors return no byte-count
success. The command regression verifies rejection before formal publication.

Focused layer and artifact-export tests pass. Full repository unit tests and
full lint (vet and `golangci-lint v2.13.1`) pass. Related-package race tests
pass: `internal/releaseartifacts` in `7.281s` and `internal/deploymentcontract`
in `65.420s`. The deployment tests invoke the production command with a
fake-Docker export; their child `go run` processes are not race-instrumented.
The empty-output cases supply the correct empty-content DiffID so rejection
cannot rely on a digest mismatch. The final-source focused layer race run
passes in `1.837s`, and its package lint reports zero issues.

```sh
go test ./internal/releaseartifacts -run '^TestOCIImageLayer' -count=1 -v
go test ./internal/deploymentcontract -run '^TestBuildVelaImageArtifacts' -count=1 -v
go test ./... -count=1
go test -race ./internal/releaseartifacts ./internal/deploymentcontract -count=1
make lint
```

Full validation logs are retained under `/tmp/vela-oci-diffid-*.log`. No Docker
daemon, registry publication, external H3 input, model weight or GPU is needed
for the new regression tests. Fake Buildx export does not establish that a real
release image was built, unpacked or run in this increment.

## Remaining work

Matching compressed and decoded layer identities is a prerequisite for trusting
image content; it does not establish the materialized rootfs, tar whiteout/link
semantics, the bytes of the Runtime executable, its loaded configuration or the
process-to-message relationship across exec. Those observations still require
a supported, authenticated launch path. Metadata equality and a caller's matching
manifest declaration remain insufficient for backend startup authority.

Mutually authenticated Node/Runtime endpoint assembly, durable process/journal/
startup-nonce binding and independent exact-owner retirement remain open.
PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry binding 1 and
Production Gates `0/9` remain unchanged. Vela's overall correctness and
architecture acceptance are not complete.
