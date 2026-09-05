# Three-host lab environment

Date: 2026-09-05 Asia/Shanghai

This document records a sanitized inventory of the historical `vela-lab`
baseline and the isolated `vela-lab-v2` deployment in the three-host
non-production lab. It is an operational checkpoint, not a canonical release
bundle, Launch Receipt, or Production Gate result. Passwords, SSH addresses,
registry credentials, Secret payloads, and external workload identities are
intentionally excluded.

## Evidence boundary

- Historical baseline repository revision:
  `9e64ac9ae41d2528804c93f46a2fbd8df4c2e961`
  (`main == origin/main`, one clean worktree at observation time).
- Historical `vela-lab` control image:
  `10.1.200.17:5443/vela-lab/vela-control@sha256:e136ecdd6b9b7584f5540d9e293e41d0c71b99df64df879fda35b2e79ba4ca01`.
- The image config binds source revision
  `67b336ac485064b4172ecdf486467021cd469f7e`,
  `vela.ai.build-kind=noncanonical-lab`,
  `vela.ai.lab.dirty-source=true`, and
  `vela.ai.evidence-boundary=NON_PRODUCTION_MOCK_REHEARSAL`.
- Historical `vela-lab` PostgreSQL remains at migration `33`.
- The isolated `vela-lab-v2` deployment binds source revision
  `a50897d55854bde67ff24292e593b1ccc65ce913` and runs a fresh PostgreSQL schema
  at migration `69`, matching that deployed source revision.
- Two digest-bound CPU-only four-Stage smoke Jobs passed with all eight
  StageRuns succeeded. They requested no GPU and are mock control,
  transport/lifecycle, Artifact-transfer, and exact-cache evidence only.

The historical deployment remains behind the current repository by both source
and schema identity. The isolated deployment closes that source/schema gap for
the mock path, but healthy Pods and passing mock campaigns do not establish a
canonical production release identity.

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

## Historical database authority

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

## Parallel deployment decision

Do not run the repository migration set directly against the current migration
`33` database. Migration `58` is irreversible. For a non-empty database it
requires the M5 zero-backlog receipt, M6 readiness archive/freeze, an authorized
canonical contracted release, all nine sealed PASS Launch Receipts, and a fresh
live-zero recheck. The current lab has none of those release/Gate artifacts.
Starting the full migration sequence would risk leaving the existing lab at an
intermediate schema before migration `58` rejects contraction.

The authorized deployment used a parallel schema-v2 lab with a fresh empty
database and separate namespace/release identities:

1. Preserved the existing `vela-lab` namespace and receipt tree as the historical
   comparison baseline.
2. Built the control, Fleet, Worker, runtime, and bootstrap images from exact
   source revision `a50897d55854bde67ff24292e593b1ccc65ce913`, published them
   once to the private registry, and deployed only immutable digest references.
3. Bootstrapped migrations `1..69` into a fresh database after the empty-install
   checks required by migration `58`.
4. Deployed a separate Stage-only control and CPU mock environment with zero GPU
   requests; external GPU workloads remained excluded.
5. Ran readiness, four-Stage lifecycle, Artifact-transfer, exact-cache, and
   bounded observation checks against the new database.
6. Retained digest-bound success and failure evidence while keeping all nine
   Production Gates at `NOT PASS` until real typed evidence exists.

The remote writes were performed only after explicit authorization. The
`vela-lab-v2` deployment remains running for continued CPU-only mock testing;
the historical namespace and unrelated GPU experiments were not modified.

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

## Deployed `vela-lab-v2` checkpoint

The control host runs Docker Server `29.1.3`, Docker Buildx `0.30.1`, and
BuildKit `v0.26.2`. The five Vela images were built on that host, pushed once to
the existing private registry, and deployed at these immutable identities:

| Component | Immutable image |
| --- | --- |
| Control | `10.1.200.17:5443/vela-lab-v2/vela-control@sha256:32295b8896583444c198fbef680fa6e9571f3c765578160dcf62e6a7498d4bb5` |
| Fleet controller | `10.1.200.17:5443/vela-lab-v2/vela-fleet-controller@sha256:ade5cdab854ea7a7ccc93242b1c5992d02fcdf147a139ad2f65ef37e7168a7ec` |
| Stage Worker Agent | `10.1.200.17:5443/vela-lab-v2/vela-stage-worker-agent@sha256:6ce28fc769b57eee16b96f1e89efbc4dd850cb0bd05f87fbce97f935a63a4f2f` |
| H3 Stage runtime | `10.1.200.17:5443/vela-lab-v2/vela-h3-stage-runtime@sha256:fbd334340dabc10d66c4a3d0f4525bb4c83c53058bc325da684fb9781dba1ac6` |
| Lab bootstrap and CPU thumbnail | `10.1.200.17:5443/vela-lab-v2/vela-lab-bootstrap@sha256:ce1ba60172d16fb6367a08ba51df15bfa0a5a6c79e6f420d2fda86d6054e5656` |

The protected asset manifest SHA-256 is
`d9e7f6bf069160ee9060c2f4999730084b815c5a5f64e2a8bfb767c092b92d99`.
The repository unit tests, manifest render contract, ShellCheck, PostgreSQL 17
migration/bootstrap replay through schema `69`, focused integration tests, and
`make verify` pass for the deployed revision. Migration `68` accepts the
certified input Connector states needed by the Stage path. Migration `69` makes
TransferTicket resolve/consume validity depend on a post-lock PostgreSQL server
clock sample plus live capacity authority.

The two post-deployment smoke Jobs were:

- `311c1246-e478-412e-8740-ce6841e76755`;
- `b881917b-5611-44c5-8684-cc26d9b19d43`.

Both Jobs reached `SUCCEEDED`. All eight StageRuns reached `SUCCEEDED`, with two
each for `encoder`, `dit`, `vae`, and `thumbnail`; the durable result inventory
contained `VIDEO=2` and `THUMBNAIL=2`. Across 138 two-second monitor samples,
`max_restarts=0` and `max_gpu_requests=0`. Node GPU allocatable remained
`0/8/8`: the control node ran only control/storage services and the CPU
thumbnail Worker, while both eight-GPU nodes remained pure Worker nodes. The
database reported schema `69`, zero Production Gate receipts, and
`production_gates=0/9`.

The successful evidence tree is retained at
`/home/marslab/vela-lab-v2-a50897d-success-20260905T000417Z`. Its
`SHA256SUMS` file has SHA-256
`de167b29b5904f2c6044ec8c157a1c57fcfde9cdea61deb77d6bb1acb28b2411`.

The first deployment of revision `96a1dd3` failed because a Worker clock was
about 300 ms behind the control authority, causing a START event timestamp to
precede `Authority.IssuedAt`. The deployment was fully rolled back before the
fixed revision was installed, and its preserved failure evidence is at
`/home/marslab/vela-lab-v2-failure-96a1dd3-start-event-clock-skew-20260904T234509Z`.
That tree's `SHA256SUMS` file has SHA-256
`b0be2c03a9b4280ae62612ab21f1a99ea741017abc3b5768fe42a9ea256b5262`.
Revision `a50897d` clamps execution-event timestamps only within the shared
bounded-skew policy and still rejects expired, excessively future, or
out-of-window events.

## Image and transfer execution

The clean repository archive was about `2.7MB` compressed. To minimize SSH
traffic, the deployment:

- transferred only the small, commit-bound source archive and its digest;
- built the images on the control host with an explicit Go module proxy;
- pushed once to the existing private registry by immutable digest; and
- let Worker nodes pull registry layers over the lab network.

Do not copy Docker image archives through SSH to each Worker. Retain existing
layers and avoid image pruning while the new environment is being verified.

## Capacity and disk

At the historical observation time, the control host had about `511G` free. The
deployment tree used about `764M`, and RKE2 used about `12G`. No remote image
pruning was performed because the running deployment and shared registry layers
must remain available. The local Go build cache was cleaned after verification
and is about `8KiB`; the `2.1G` module cache was retained. Local free space is
about `17GiB` at this checkpoint.
