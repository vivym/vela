# H3 mock backend

Date: 2026-08-29

Status: Repository implementation complete; three-host lab registry, image
distribution, ephemeral protocol checks, and persistent two-Worker Runner
deployment verified; RKE2 GPU integration and one/eight-GPU Pod smoke verified;
Vela Worker Agents, application control plane, concurrent mock endurance, and
four fixed fault-scenario rehearsals verified.

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
identifiers and never includes prompt or `client_metadata` content. When no GPU
is implicated, `gpu_uuids` is serialized as an empty JSON array, not `null`.
The Runner treats any other shape as an invalid failure receipt and must fail
closed. The currently deployed lab image predates this fix, so its host-managed
wrapper pins `--mock-failure-gpu-index 0` only in deterministic failure mode
until a future image rebuild. That compatibility wrapper does not change
success or hang behavior.

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

The physical lab inventory is now recorded, but target-specific Vela workload
overlays and mock-only Catalog records remain deliberately deferred. The
repository mock must not turn the observed eight-GPU lab placement into a
production hardware, model, or saleable-profile claim.

The current non-production host inventory, private registry configuration,
security boundary, registry push/pull receipt, and two-Worker ephemeral Runner
protocol checks are recorded in
[`three-host-mock-environment.md`](../lab/three-host-mock-environment.md). The
control host is excluded from GPU workloads; the two target Workers retain all
eight physical GPUs. A non-Kubernetes persistent Runner deployment verifies
restart and same-authority recovery on both Workers; RKE2 GPU Operator and
sequential one/eight-GPU Pods separately verify Kubernetes placement and
runtime access. Two persistent Vela Worker Agents and the application control
plane now run a mock-only Catalog through one success path, a balanced ten-Job
concurrent rehearsal, Worker-control network partition, and retry-budget
exhaustion, exact Runner process kill, and an Outbox post-commit/pre-claim
control crash. These remain non-production synthetic receipts; six of the ten
fixed fault scenarios and every Production Gate remain open.

The retry-budget receipt records zero Artifact rows, including zero committed
Artifacts. It was produced by harness SHA-256
`852a7ff868bb2cb88808bd746c74e42ed0186865f4ca19d0b7848954f2a13222`.
The review-hardened repository harness is
`b39652e15234f37cf9096f3a7268cfd1b2d830594b4ea4863d9eb9aefbdb132b`
and has not been rerun, so it is not the provenance of the retained receipt.

The process-kill rehearsal binds Worker 1 through a root-owned immutable
container-identity file, validates the exact Docker container and main-process
identity, and sends `SIGKILL` through a pidfd. Container-policy restart changed
the PID and `StartedAt` without changing the container ID; the original
Attempt persisted as `LOST` with `WORKER_LOST`, and one higher-fence replacement
on Worker 2 succeeded with exactly one Visible Completion, one Charge, and two
committed Artifacts. Its harness SHA-256 is
`cc6a79dad257ad51933cc31b0f664977f4f22d56beacc2b6ead3b9e2f5ec7d80`.
The fault Pod alone uses container-level `appArmorProfile: Unconfined` while
retaining `RuntimeDefault` seccomp, no privilege escalation, a read-only root
filesystem, and only `CAP_KILL`. The result advances the synthetic fixed
scenario matrix to `3/10`; Production Gates remain `0/9 PASS`.

The Outbox-post-commit rehearsal temporarily sets the bounded
`VELA_PUBLISHER_TICK` to one minute, verifies a committed but unpublished and
unclaimed `job.ready` event, and sends `SIGKILL` only to the exact control
process through a pidfd. After restart, the Publisher records exactly one
publication to `VELA_EVENTS`; the Job completes with one Attempt, one Visible
Completion, one Charge, and two committed Artifacts. The successful receipt is
bound to harness SHA-256
`70dab9231d7f75f49abfc531dfd028a2316ae462ee6dcdb1af865980898034ef`.
Cleanup removes the override and reloads the default `500ms` Publisher interval.
This advances only the synthetic matrix to `4/10`; Production Gates remain
`0/9 PASS`.

Mock images may be built and deployed to staging, but they must use a mock
backend revision and separate Catalog records. They do not satisfy
`preset-certification`, `real-h3-soak`, `gpu-remediation`, or any other Launch
Receipt. Production Gates remain `0/9 PASS`.
