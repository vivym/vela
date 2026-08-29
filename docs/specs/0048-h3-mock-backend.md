# H3 mock backend

Date: 2026-08-29

Status: Repository implementation complete; deployment evidence pending target
server inventory.

Implementation commit: `4304fe9`; review closure: `f6cb45b`.

This development backend implements the exact file protocol used between the
Python H3 Runner and the proprietary H3 process. It exists to exercise image
assembly, Worker lifecycle, progress, cancellation, recovery, Artifact upload
and validation, retry, and deployment behavior while the real backend is still
under development. It is not an H3 implementation, model certification input,
performance simulator, or Production Gate artifact.

## Build contract

Build a fresh `linux/amd64` backend context with:

```text
make build-h3-mock-backend \
  H3_MOCK_BACKEND_CONTEXT=/absolute/new/path/vela-h3-mock-context
```

The output directory must not already exist. Publication is atomic and
no-replace. It contains exactly one mode-`0555` file named `h3-backend`. The
builder uses `CGO_ENABLED=0`, trimmed source paths, no VCS embedding, and an
empty Go build ID, then runs the same ELF64/x86-64/digest validation required by
the Vela image build seam. Standard output prints the exact
`H3_BACKEND_CONTEXT` and `H3_BACKEND_SHA256` values for the existing Slice 42
image commands.

## Runtime contract

The mock supports all four Runner readiness checks:

- `DEVICE` returns the exact one Encoder/VAE plus seven DiT UUIDs from
  `CUDA_VISIBLE_DEVICES`;
- `INFERENCE_BACKEND` binds the exact `VELA_RUNNER_BACKEND_REVISION`;
- `MODEL_WARMUP` binds the requested ExecutionProfileRevision; and
- `CANARY` returns a fixed mock-only output digest.

Execution requires a fixed OutputSpec UUID in the Runner backend arguments:

```json
[
  "--mock-output-spec-id",
  "<mock-output-spec-uuid>",
  "--mock-mode",
  "success",
  "--mock-stage-delay",
  "250ms"
]
```

The only supported media contract is:

| Field | VIDEO | THUMBNAIL |
| --- | --- | --- |
| kind | `VIDEO` | `THUMBNAIL` |
| dimensions | `1920x1080` | `320x180` |
| duration | `5000 ms` | not applicable |
| frame rate | `24000 milli-fps` | not applicable |
| frame count | `120` | `1` |
| codec | `h264` | `webp` |
| container/content type | `mp4` / `video/mp4` | `webp` / `image/webp` |

Use a dedicated Catalog stable ID such as
`mock-video-1080p-5s-24fps`; do not bind the mock to a saleable production
OutputSpec. An execution whose OutputSpec UUID differs from the fixed mock
argument fails before any output is written.

Success mode emits bounded monotonic stages (`mock/prepare`, `mock/encode`,
`mock/package`, `mock/finalize`) and atomically publishes the complete VIDEO and
THUMBNAIL manifest. `--vela-resume true` safely replaces a partial known mock
file from a prior process while a fresh execution refuses to overwrite any
existing output.

## Failure and cancellation modes

`--mock-mode failure` writes the Runner's strict bounded failure receipt and
exits non-zero. These fixed arguments select the intended branch:

```text
--mock-failure-class CUDA_OOM
--mock-failure-fingerprint mock/cuda-oom/dit
--mock-failure-stage mock/encode
--mock-failure-gpu-index 1
--mock-retry-recommended true
--mock-worker-reusable false
```

The GPU index is `-1` for no implicated device or `0..7` for the corresponding
UUID in `CUDA_VISIBLE_DEVICES`. Failure metadata is restricted to safe fixed
identifiers and never includes prompt or `client_metadata` content.

`--mock-mode hang` publishes one bounded running status and waits until
`SIGTERM`, `SIGINT`, or Runner cancellation. It produces neither outputs nor a
failure receipt, allowing Lease deadline, Agent shutdown, process-group stop,
and same-Worker recovery to be exercised.

All request/result files use strict schemas, reject duplicate or unknown JSON
keys and trailing documents, and remain bounded. Output and receipt files are
private mode `0600` direct children of Runner-owned directories.

## Deployment boundary

The production Worker base remains unchanged: it still requests exactly eight
GPUs and uses invalid Vela image placeholders until Fleet materialization. A
target-specific mock overlay may remove the GPU request only for explicitly
non-production CPU staging while retaining eight synthetic role UUIDs. Such an
overlay must never be used as hardware readiness, remediation, soak,
certification, or performance evidence.

The target-specific overlay and mock-only Catalog records are deliberately
deferred until server inventory is supplied; the repository mock must not guess
whether a target retains physical GPU resources or uses CPU-only staging.

Mock images may be built and deployed to staging, but they must use a mock
backend revision and separate Catalog records. They do not satisfy
`preset-certification`, `real-h3-soak`, `gpu-remediation`, or any other Launch
Receipt. Production Gates remain `0/9 PASS`.
