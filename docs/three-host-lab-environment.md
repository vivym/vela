# Three-host lab environment

Date: 2026-09-04 Asia/Shanghai

This document records a sanitized, read-only inventory of the current
three-host non-production lab. It is an operational checkpoint, not a canonical
release bundle, Launch Receipt, or Production Gate result. Passwords, SSH
addresses, registry credentials, Secret payloads, and external workload
identities are intentionally excluded.

## Evidence boundary

- Repository revision: `9e64ac9ae41d2528804c93f46a2fbd8df4c2e961`
  (`main == origin/main`, one clean worktree at observation time).
- Live control image:
  `10.1.200.17:5443/vela-lab/vela-control@sha256:e136ecdd6b9b7584f5540d9e293e41d0c71b99df64df879fda35b2e79ba4ca01`.
- The image config binds source revision
  `67b336ac485064b4172ecdf486467021cd469f7e`,
  `vela.ai.build-kind=noncanonical-lab`,
  `vela.ai.lab.dirty-source=true`, and
  `vela.ai.evidence-boundary=NON_PRODUCTION_MOCK_REHEARSAL`.
- Live PostgreSQL is at migration `33`; the repository schema is migration
  `65`.
- The latest digest-bound CPU-only Stage lifecycle and cross-Worker member
  campaigns passed with `failures=0`. Those campaigns requested no GPU and are
  mock transport/lifecycle evidence only.

The live deployment is therefore behind the repository by both source and
schema identity. Healthy Pods and passing mock campaigns do not establish a
production release identity.

## Topology

| Role | Kubernetes identity | Observed placement | GPU authority |
| --- | --- | --- | --- |
| Control | `vela-lab-control-1` | control, PostgreSQL, MinIO, all three NATS Pods, Prometheus, Alertmanager, Grafana | no allocatable GPU |
| Worker 1 | `vela-lab-worker-1` | Worker Agent | 8 allocatable GPUs |
| Worker 2 | `vela-lab-worker-2` | Worker Agent | 8 allocatable GPUs |

All three nodes were `Ready`. The two Worker Agents were `READY/HEALTHY`, and
the lab requested zero Kubernetes GPUs. Kubernetes allocatable counts are not a
claim that the devices are idle: external GPU experiments remain outside Vela
authority and must not be interrupted or oversubscribed.

The deployed control/storage shape is intentionally lab-only:

- PostgreSQL: one replica on the control node;
- MinIO: one replica on the control node;
- NATS: three replicas, all on the control node;
- Vela control: one replica on the control node; and
- Worker Agent: one CPU-only replica on each Worker node.

The cluster serves the `resource.k8s.io` DRA API types, but no `DeviceClass`,
`ResourceSlice`, `ResourceClaimTemplate`, or `ResourceClaim` exists. It also has
no `StorageClass`, persistent volume, or persistent volume claim. The current
cluster cannot prove real H3 actuation, persistent control storage, metadata
failover, Artifact durability, or restore.

## Database authority

The following PostgreSQL state was observed read-only:

| Check | Result |
| --- | --- |
| Applied migration | `33` |
| Production Gate manifests | `0` |
| Production Gate receipts | `0` (`PASS=0`) |
| Catalog evidence protocol | `LEGACY`, version `1`, transitions `0` |
| SLO measurement protocol | `LEGACY`, version `1`, transitions `0` |
| Active Attempt leases | `0` |
| Jobs | `71 SUCCEEDED`, `15 FAILED`, no nonterminal state |
| Workers | `2 READY/HEALTHY` |
| Admission-open Worker pools | `1` |

The absence of nonterminal work makes a disposable-lab rebuild practical, but
it does not satisfy the guarded contraction contract. The database contains
customer, Catalog, Worker-pool, and Job roots, so it is not an empty install.

## Observability

Prometheus, Alertmanager, and Grafana health/readiness endpoints returned HTTP
`200`. Grafana reported database health `ok`. All three components are single
replicas on the control node.

Prometheus had one healthy target, the Vela control `/metrics` endpoint. All 13
loaded rules were healthy, but the alert state was intentionally not green:

| Alert | Instances | Severity |
| --- | ---: | --- |
| `VelaAPISLISeriesMissing` | 1 | `page` |
| `VelaSLOContractCoverageMissing` | 32 | `page` |
| `VelaLabObservabilityHeartbeat` | 1 | `test` |

Alertmanager was `ready`, had one peer, and held the same 34 active alerts. Its
default receiver is `lab-null`; there is no real paging destination. This proves
that rule evaluation and Alertmanager ingestion work in the lab. It does not
prove alert delivery, acknowledgement, escalation, or on-call ownership.

## Release evidence inventory

The deployment tree contained 5,341 regular files. A filename inventory for
release bundle, SBOM, SPDX, attestation, provenance, supply-chain, Launch
Receipt, and Production Gate terms found only three historical verifier/image
provenance text files. It found no canonical `release-bundle.json`, no
`launch-receipts.json`, no supply-chain manifest/policy directory, and no SBOM
or attestation artifact.

The authoritative PostgreSQL tables independently contained zero Gate manifests
and zero Gate receipts. Historical checksum-valid rehearsal directories remain
non-production evidence bound to an older release/schema and do not substitute
for typed evidence or the current Stage Assignment fault contract.

## Production Gate audit

Every Gate remains `NOT PASS`.

| Gate | Current lab evidence | Blocking production evidence |
| --- | --- | --- |
| `preset-certification` | Repository contract and synthetic fixtures only | Canonical saleable-group snapshot, independent `quality`/`balanced`/`fast` benchmarks, RateCard bindings, supply-chain evidence, typed PASS receipt |
| `real-h3-soak` | CPU-only Stage mock campaigns | Real H3 backend and weights, DRA/GPU identity closure, accepted real Jobs, at least 72 hours, zero reconciliation violations, typed PASS receipt |
| `state-event-fault-injection` | Historical lab rehearsals | Current V2 ten-scenario Stage campaign, including Stage Assignment crash and stale member/device/runtime/lease probes, typed PASS receipt |
| `gpu-remediation` | GPU Operator inventory; no Vela GPU use | Certified L0-L7 hardware actions, negative approval tests, canary/quarantine/post-check evidence, typed PASS receipt |
| `organization-isolation-content-safety` | Historical partial lab probes | Real customer/platform IdP, break-glass workflow, content-reuse audit sink, complete negative surface coverage, typed PASS receipt |
| `data-disaster-recovery` | Single volatile PostgreSQL and MinIO instances | Independent failure domains, durable backup/PITR/Artifact restore, required RPO/RTO exercise, typed PASS receipt |
| `release-rollback` | Historical partial lab rehearsal | Canonical N/N-1 release pair, long H3 Job drain, retained backlog, rollback owner and reconciliation evidence, typed PASS receipt |
| `commercial-data-lifecycle` | Historical invoice/retention/webhook rehearsals marked non-PASS | Real Finance and endpoint integrations, retention/deletion and no-resurrection closure, typed PASS receipt |
| `observability-on-call` | Healthy components, 34 active alerts, `lab-null` receiver | Real paging route, delivery/ack/escalation exercise, dashboards and SLO cohort closure, owned typed PASS receipt |

Result: `production_gates=0/9`.

## Deployment decision

Do not run the repository migration set directly against the current migration
`33` database. Migration `58` is irreversible. For a non-empty database it
requires the M5 zero-backlog receipt, M6 readiness archive/freeze, an authorized
canonical contracted release, all nine sealed PASS Launch Receipts, and a fresh
live-zero recheck. The current lab has none of those release/Gate artifacts.
Starting the full migration sequence would risk leaving the existing lab at an
intermediate schema before migration `58` rejects contraction.

The next safe deployment unit is a parallel schema-v2 lab with a fresh empty
database and separate namespace/release identities:

1. Preserve the current `vela-lab` namespace and receipt tree as the historical
   comparison baseline.
2. Reuse the clean-`9e64ac9` Stage runtime and member-campaign image digests
   already present in the private registry. Build the missing control, Fleet,
   Worker, and bootstrap images from the eventual committed `vela-lab-v2`
   revision containing `deploy/lab-v2` and explicit empty-rollout Fleet support;
   do not build those images from `9e64ac9`. Bind every new image to that exact
   source revision, publish by immutable digest, and label it explicitly as a
   non-production lab artifact.
3. Bootstrap migrations `1..65` into a new empty database. The migration `58`
   empty-install exception applies only after verifying every customer,
   Catalog, Worker-pool, compute-node, and Job root is empty.
4. Deploy a separate Stage-only control and CPU mock environment with zero GPU
   requests. Keep the external GPU workloads excluded.
5. Run readiness, Stage lifecycle, cross-Worker member, Artifact-transfer,
   exact-cache, retry/fencing, and bounded observation checks against the new
   database.
6. Retain digest-bound lab receipts and compare them with this baseline. Keep
   all nine Production Gates at `NOT PASS` until real typed evidence exists.

Creating the new namespace/database and publishing/deploying images are remote
writes. They require an explicit maintenance target and rollback/cleanup
authorization before execution.

## Parallel `vela-lab-v2` configuration

The repository now contains the isolated deployment package under
`deploy/lab-v2`. Its intended graph is:

```text
encoder (GPU mock) -> dit (GPU mock) -> vae (GPU mock) -> thumbnail (CPU mock)
                                               |                 |
                                             VIDEO           THUMBNAIL
```

The two GPU-labelled synthetic Workers are scheduled on
`vela-lab-worker-1` and `vela-lab-worker-2`, but request zero Kubernetes GPUs.
The third Worker is a CPU-only `cpu-thumbnail` Worker scheduled on
`vela-lab-control-1`; its Device evidence has resource class `CPU` and contains
no GPU UUID or PCI BDF. The thumbnail profile uses the bounded CPU media vector
`cpu_milli=2000`, `memory_bytes=4294967296`,
`scratch_bytes=34359738368`, and `concurrency=1`.

Two immutable runtime identities are required:

- `RUNTIME_IMAGE` contains `/usr/local/bin/vela-model-runtime` plus only the
  verified H3 Encoder, DiT, and VAE mock commands. It remains the runtime image
  for the two H3 Stage Workers.
- `BOOTSTRAP_IMAGE` contains `vela-lab-bootstrap`, `vela-lab-smoke`,
  `vela-model-runtime`, and `vela-lab-cpu-thumbnail-mock`, plus database
  migrations. It bootstraps the fresh database, runs the smoke client, and is
  the CPU thumbnail Worker's ModelRuntime image.

Generate a new protected asset set only after the two images have been pushed
to the private registry and resolved to immutable digests:

```sh
go run ./cmd/vela-lab-assets \
  --output /absolute/new/path/lab-v2-assets \
  --runtime-image '10.1.200.17:5443/vela-lab-v2/vela-h3-stage-runtime@sha256:<digest>' \
  --thumbnail-runtime-image '10.1.200.17:5443/vela-lab-v2/vela-lab-bootstrap@sha256:<digest>'
sudo chown -R root:root /absolute/new/path/lab-v2-assets
```

The generator creates the directory with mode `0700` and every asset file with
mode `0600`. Because `install-assets.sh` intentionally accepts only root-owned
secret material, transfer ownership immediately after generation and before
installation, as shown above.

The `images.env` used by `render-manifests.sh` must contain exactly pinned
private-registry references for `POSTGRES_IMAGE`, `NATS_IMAGE`, `MINIO_IMAGE`,
`CONTROL_IMAGE`, `FLEET_CONTROLLER_IMAGE`, `STAGE_WORKER_AGENT_IMAGE`,
`RUNTIME_IMAGE`, and `BOOTSTRAP_IMAGE`. The operational order is:

```sh
sudo ./prepare-host.sh control --apply
sudo ./prepare-host.sh worker-1 --apply
sudo ./prepare-host.sh worker-2 --apply
./render-manifests.sh /absolute/images.env /absolute/new/rendered
sudo ./install-assets.sh /absolute/new/lab-v2-assets --apply
sudo ./deploy.sh /absolute/new/rendered all --apply
sudo ./smoke.sh /absolute/new/rendered --apply
```

Every managed Kubernetes resource and Pod template is bound to
`vela.ai/deployment-identity=vela-lab-v2`. The fail-closed rollback deletes
only resources carrying that exact label, preserves the namespace and hostPath
data for diagnosis, and refuses an empty or identity-mismatched target:

```sh
sudo ./rollback.sh --apply
```

An interrupted asset installation deliberately leaves a partial,
identity-bound resource set that a direct retry will reject. Preserve and
inspect the failed resources first, then run `rollback.sh --apply` to remove the
partial `vela-lab-v2` resources before retrying `install-assets.sh` with the
same verified asset directory. The rollback preserves the namespace and
hostPath data.

As of this checkpoint, the repository unit tests, manifest render contract,
ShellCheck, and a PostgreSQL 17 migration/bootstrap replay through schema `65`
pass locally. No `vela-lab-v2` image has been published and no remote host has
been changed by this checkpoint. The live environment facts earlier in this
document therefore remain the current remote baseline.

## Image and transfer plan

The clean repository archive is about `2.7MB` compressed. The already-published
`9e64ac9` Stage runtime and member-campaign images are about `221MB` and
`17.9MB` in the control host's Docker inventory. To minimize SSH traffic:

- transfer only the small, commit-bound source archive and its digest;
- build missing images on the control host with an explicit Go module proxy;
- push once to the existing private registry by immutable digest; and
- let Worker nodes pull registry layers over the lab network.

Do not copy Docker image archives through SSH to each Worker. Retain existing
layers and avoid image pruning while the new environment is being verified.

## Capacity and disk

At observation time, the control host had about `511G` free. The deployment
tree used about `764M`, and RKE2 used about `12G`. No remote cache or image
cleanup is currently justified. A previous local Go build-cache cleanup freed
about `3G`; the Go module cache was retained. Local free space increased from
about `11GiB` to `14GiB` at that checkpoint.
