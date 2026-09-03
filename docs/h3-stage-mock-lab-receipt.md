# H3 Stage mock three-host lab receipt

Date: 2026-09-03

Status: the Stage mock runtime from source revision
`756b2b726c2692de7d2ef4a6fe46401e135a0486` was published to the lab's
private Registry and passed both a CPU-only command-protocol startup smoke and
a direct command-protocol lifecycle suite on both Worker nodes. This is not a
complete Stage/Fleet deployment, a real H3 execution, a performance result, or
a Production Gate receipt. Production Gates remain `0/9 PASS`.

## Scope and topology

The three hosts use their `10.1.200.0/24` LAN addresses for RKE2 and image
distribution. The `100.111.0.0/16` addresses are administration endpoints and
are not part of the runtime or Registry data path.

| Host | Administration endpoint | LAN address | Lab role | Vela GPU policy |
| --- | --- | --- | --- | --- |
| `marslab-server` | `marslab@100.111.196.116` | `10.1.200.17` | RKE2 control node and private Registry | excluded; Kubernetes GPU allocatable is `0` |
| Worker 1 (`hostname=ubuntu`) | `viv@100.111.96.193` | `10.1.200.19` | RKE2 H3 Worker | eight GPUs allocatable |
| Worker 2 (`hostname=ubuntu`) | `viv@100.111.226.6` | `10.1.200.16` | RKE2 H3 Worker | eight GPUs allocatable |

At postflight, all nodes ran RKE2 `v1.35.7+rke2r1` and were `Ready`. The
control node remained selected only for control/storage services. The two
Worker nodes retained their `vela.ai/worker-profile=h3` and
`vela.ai/worker-pool=launch` labels and the `vela.ai/h3=true:NoSchedule` taint.

No credential is committed with this receipt. Registry authentication and its
private CA are host-managed inputs.

## Registry and image identity

The existing `vela-registry` container remained available at
`10.1.200.17:5443`; it was not restarted. The Stage mock image was imported
once on the control host and published under a distinct non-production
repository:

```text
10.1.200.17:5443/vela-lab-next/vela-h3-stage-runtime
```

After publication, the Registry data root `/srv/vela-registry` occupied 4.6
GiB and the control root filesystem retained 513 GiB free. No Registry prune
or deletion was performed.

| Property | Observed value |
| --- | --- |
| source revision and tag | `756b2b726c2692de7d2ef4a6fe46401e135a0486` |
| platform | `linux/amd64` |
| Registry/image digest | `sha256:4f3b13f75b4a8fff1f5f96255c933be9a9a467d4d19a941f48925c5da1f4b536` |
| each of the three runtime-command digests | `ce4d072fb0315dee19d3ef616cc17fe8cefbc15fb40ef3ba3dc11029b0e58685` |
| local Docker image size | 45,712,300 bytes |
| one-time SSH archive | 45,418,984 bytes |
| archive SHA-256 | `395143e8cd74de126d109ae872501ebab5473df023e5e157853e0dbe4858695c` |

Both Workers pulled the exact Registry digest through RKE2 containerd in about
5.2 and 5.4 seconds respectively. This validates the lab's LAN Registry TLS,
authentication, and containerd pull path for this image. The image archive was
not copied separately to either Worker.

The three identical commands were built through
`make build-h3-stage-mock-runtime` and installed as:

- `/opt/vela/bin/h3-encoder`
- `/opt/vela/bin/h3-dit`
- `/opt/vela/bin/h3-vae-decoder`

## CPU-only smoke method

The validation used one ephemeral Pod on each Worker. It invoked each command
directly with `VELA_MODEL_DRIVER_PROTOCOL=stdio-json-v1` and required exactly
three successful responses in order:

```text
initialize -> probe(MODEL_RUNTIME_READINESS_CHECK_MODEL_WARMUP) -> shutdown
```

The smoke intentionally requested no `nvidia.com/gpu` resource. Its main
container requested `50m` CPU and `64Mi` memory, ran as UID/GID `10001`, used a
read-only root filesystem, disabled privilege escalation, dropped all Linux
capabilities, used `RuntimeDefault` seccomp, and did not mount a service-account
token. The only writable roots were bounded `emptyDir` volumes for `/work` and
`/tmp`.

The first Pod form set `fsGroup: 10001`. Kubernetes consequently exposed the
`/work` volume root as group/world writable (`2777`), and the runtime correctly
failed closed with:

```text
bind H3 Stage mock runtime root: secure executable ancestor "work" is not trusted
```

The successful form removed `fsGroup`. A root init container, with no host
mount or GPU access, changed only the fresh `/work` `emptyDir` to
`10001:10001,0700`; the main process remained non-root. Both init containers
reported mode `0700`, and the runtime then accepted the scratch, input, and
output ancestry.

## Initial startup smoke results

| Pod | Node | UTC interval | Result | Main exit | GPU request |
| --- | --- | --- | --- | --- | --- |
| `vela-stage-mock-smoke-w1-v3-756b2b7` | `vela-lab-worker-1` | `05:56:11` to `05:56:14` | `Succeeded` | `0` | `0` |
| `vela-stage-mock-smoke-w2-v3-756b2b7` | `vela-lab-worker-2` | `05:57:16` to `05:57:18` | `Succeeded` | `0` | `0` |

For `ENCODER`, `DIT`, and `VAE_DECODER`, both Pods observed all of:

- `initialize`: `acknowledged=true`, `initialized=true`;
- `probe`: `ready=true`, with component-specific bounded evidence; and
- `shutdown`: `acknowledged=true`.

Kubernetes reported the exact digest-pinned `imageID` above on both nodes.
This proves command startup and the minimal resident protocol path in the
current lab image. This initial phase did not exercise `prepare`, `start`,
`status`, `seal`, `cancel`, Stage artifact transfer, Scheduler/Fleet
integration, or GPU work.

## Direct command lifecycle results

After the initial receipt was squash-merged as
`791c1b22b99f22889d8fc656fc3c0f39c59d1a33`, a follow-up used the same
runtime image and source revision. That merge changed documentation only and
did not rebuild or retag the image. One new ephemeral Pod ran on each Worker:

| Pod | Pod UID | Node | UTC interval | Result | Main exit | GPU request |
| --- | --- | --- | --- | --- | --- | --- |
| `vela-stage-mock-lifecycle-w1-791c1b2` | `3bd5bc4b-585c-4472-8d9f-ad037b367c67` | `vela-lab-worker-1` | `07:11:14` to `07:11:16` | `Succeeded` | `0` | `0` |
| `vela-stage-mock-lifecycle-w2-791c1b2` | `708b403f-51dd-46ac-b646-d8d97c6d0494` | `vela-lab-worker-2` | `07:15:52` to `07:15:54` | `Succeeded` | `0` | `0` |

Both Pods ran the main process as UID/GID `10001`, requested `50m` CPU and
`64Mi` memory, restarted zero times, and reported the exact digest-pinned
`imageID` above. For each component they completed:

```text
initialize -> probe -> prepare -> start -> status(OUTPUT_READY) -> seal -> shutdown
```

The sealed mock outputs were byte-for-byte identical on both Workers:

| Component | Output port | Content type | Size | SHA-256 |
| --- | --- | --- | ---: | --- |
| `ENCODER` | `conditioning` | `application/x-minimax-h3-encoder` | 171 bytes | `c48de0389eed3c11855f8b55bb34da59b77c7fc4bc09f027adf00b7fddc7632b` |
| `DIT` | `latent` | `application/x-minimax-h3-latent` | 233 bytes | `6e51946215a2705b60c794dcb480583265e3438990e4e081a3dc32f34eacf170` |
| `VAE_DECODER` | `video` | `video/mp4` | 12,953 bytes | `f4141b2ace373bdbbd89759dada35ddc65b979470d6c47d096e2209bbe3adbe7` |

Each Pod also passed three bounded negative-path checks:

- the `ENCODER` injected-failure case returned `FAILED`,
  `MOCK_INJECTED_FAILURE`, and `worker_reusable=true`; a replacement Stage
  Attempt was then accepted by `prepare`;
- the `ENCODER` injected-hang case returned `RUNNING`, accepted `cancel`,
  reached `STOPPED`, and left no output; and
- the `DIT` one-byte-different input was rejected with
  `H3 Stage mock input digest is mismatched` and left no output.

These checks directly exercised the three runtime commands over
`stdio-json-v1` inside standalone Pods. They did not involve Vela API
admission, Scheduler assignment, Stage Worker or Fleet orchestration, live
Lease/fence authority, cross-Pod artifact transfer, or GPU execution.

The exact execution-spec bytes and digests, mock input bytes and digests,
sanitized request/response observations, Kubernetes Pod metadata,
non-interference observations, cleanup result, and postflight output are
retained in
[`h3-stage-mock-lifecycle-evidence-2026-09-03.json`](h3-stage-mock-lifecycle-evidence-2026-09-03.json).
That sanitized evidence file has SHA-256
`4b6b43182aa272eb46ca93bf8ffad54609570e9e004bbb93f2149158d042120b`.

## Non-interference and cleanup

The smoke Pods did not request a GPU. Before and after the smoke:

- Worker 1 retained the external `sgl_diffusion::scheduler` process using one
  GPU and its existing `vela-h3-mock-runner` remained healthy;
- Worker 2 retained eight external `VLLM::Worker_PP*` processes and its
  existing `vela-h3-mock-runner` remained healthy; and
- no Docker or RKE2 service was restarted, no database schema was migrated,
  and no existing Vela workload or receipt was modified.

Two unrelated Python processes on the shared control host were visible during
an intermediate observation and absent at the final observation. The smoke
Pods could not have consumed the control GPUs: the control node exposes zero
allocatable GPUs and neither Pod was scheduled there. This receipt does not
attribute the external processes' exit to Vela.

After collecting logs, placement, image identity, runtime user, resource
requests, and exit status, all five temporary diagnostic and successful smoke
Pods were deleted by exact name. No Pod with
`app.kubernetes.io/name=vela-stage-mock-smoke` remained.

The two follow-up lifecycle Pods were likewise deleted by exact name after
their evidence was captured. No Pod with
`app.kubernetes.io/name=vela-stage-mock-lifecycle` remained. Before and after
that suite, ordered preflight and post-suite queries returned the same Worker 1
`sgl_diffusion::scheduler` PID/process name and the same eight Worker 2
`VLLM::Worker_PP*` PID/process names. Both existing
`vela-h3-mock-runner` containers remained healthy. Absolute timestamps for
those two process queries were not retained, so this is an ordered
non-interference observation rather than a time-series receipt.

The pinned postflight verifier
`verify-cluster-b575257.sh` (SHA-256
`10b7dae8395f2d1ca1bd9fb4f8d5a73e4a98af64fb105cd24e8981f1599af24a`)
then returned `result=PASS failures=0` at `2026-09-03T06:01:35Z`. It confirmed:

- API readiness and all three nodes `Ready`;
- GPU capacity/allocatable `8/8` on each Worker and `0` on control;
- explicit `NoSwap`, expected labels and taints, and disabled MPS;
- all DaemonSets and Deployments available; and
- no unhealthy non-terminal Pod.

The same pinned verifier ran again after the lifecycle Pods were removed. At
`2026-09-03T07:17:44Z` it again returned `result=PASS failures=0`, including
GPU capacity/allocatable `8/8` on each Worker and `0` on control. This is a
cluster postflight result, not a Production Gate result.

## Evidence boundary and next deployment

The legacy `vela-h3-mock-runner` containers implement the retired monolithic
Runner protocol. This Stage mock image is not a drop-in replacement for those
containers, so neither experiment rolled them.

A complete Stage/Fleet lab deployment still needs a separate authorized change
window and release-bound inputs, including the schema through migration 65,
PKI and workload credentials, Fleet/Stage configuration, Catalog identities,
and GPU availability. Real H3 model behavior, output quality, latency,
throughput, memory pressure, failure distributions, sustained soak, HA/DR,
release provenance, SBOM/attestation, and all Production Gate Launch Receipts
remain unproven.
