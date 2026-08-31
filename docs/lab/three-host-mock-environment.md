# Three-host H3 mock experiment environment

Date: 2026-08-31

Status: Private registry, RKE2/Canal, host-lifecycle-off GPU Operator, two
eight-GPU Workers, non-canonical mock Runner distribution, persistent Runner
recovery, Kubernetes GPU smoke, Vela control-plane deployment, two Worker
Agents, an end-to-end mock Job with verified Artifacts, concurrent mock
endurance, and all ten fixed non-production fault rehearsals are operational.
The final independent live postflight restored and verified the idle two-Worker
authority boundary.

This document records the current non-production lab inventory and the private
registry used to stage H3 mock experiments. It is an environment receipt, not
hardware certification, model certification, soak evidence, remediation
evidence, or a Production Gate artifact. Production Gates remain `0/9 PASS`.

## Topology and role boundary

All service traffic uses the `10.1.200.0/24` LAN. The `100.111.0.0/16`
addresses are SSH administration endpoints only and must not be used for image
distribution.

| Host | SSH administration | LAN address | Role | GPU policy |
| --- | --- | --- | --- | --- |
| `marslab-server` | `marslab@100.111.196.116` | `10.1.200.17` | control and private registry | excluded from Vela GPU workloads |
| Worker 1 (`hostname=ubuntu`) | `viv@100.111.96.193` | `10.1.200.19` | H3 mock Worker | eight GPUs available to experiments |
| Worker 2 (`hostname=ubuntu`) | `viv@100.111.226.6` | `10.1.200.16` | H3 mock Worker | eight GPUs available to experiments |

Inventory observed on 2026-08-29:

- all three hosts run Ubuntu 24.04.4 LTS, `linux/amd64`, and Docker 29.1.3;
- each host exposes eight `NVIDIA Graphics Device` GPUs with 65,536 MiB per
  GPU and has the Docker `nvidia` runtime installed;
- each Worker had about 819 GiB free on `/` and 489 GiB available memory;
- `marslab-server` had about 564 GiB free on `/` and 47 GiB available memory;
  and
- no Kubernetes or RKE2 installation existed at the initial inventory point.

## Applied RKE2 and GPU receipt

The approved lab deployment completed on 2026-08-29:

- all three nodes run RKE2 `v1.35.7+rke2r1`; `marslab-server` is the sole
  server/control node and both GPU hosts are agents;
- Canal, CoreDNS, ingress, metrics, and the RKE2 control plane are Ready. The
  persistent CoreDNS HelmChartConfig adds only the Worker taint toleration
  needed for its second anti-affinity replica;
- 17 pinned RKE2 system images and four pinned GPU Operator images are retained
  in `10.1.200.17:5443`; control and Worker publication state files are
  `complete`;
- GPU Operator `v26.3.2` Helm revision `1` is deployed with driver, toolkit,
  MIG manager, vGPU, VFIO, sandbox/Kata, and Confidential Computing lifecycle
  disabled;
- `verify-cluster.sh gpu` returned `PASS failures=0` at
  `2026-08-29T17:21:41Z`: each Worker reports capacity/allocatable `8/8`, the
  control node reports `0`, and all non-disabled DaemonSets, Deployments, and
  Pods are Ready;
- sequential one-GPU and eight-GPU smoke Pods passed on both Workers, after
  which all four Pods and namespace `vela-gpu-smoke` were deleted; and
- root-only post-state receipts were captured under
  `/root/vela-rke2-receipts/20260829T1723Z-*-gpu-post`; all three receipt
  manifests verified successfully.

The applied GPU bundle is immutable at these receipts:

| Artifact | Receipt |
| --- | --- |
| GPU Operator chart | 50,603 bytes, SHA-256 `b6b7b7a6d40bb8420d50e46c1169c097028ad19a0457d32156568db8214af77f` |
| GPU Operator image | `10.1.200.17:5443/nvidia/gpu-operator:v26.3.2@sha256:25ed43b3e4c1d74f70aab71b981c7a200b8ab047dc6f7d127641d9b161c144cb` |
| Device Plugin/GFD image | `10.1.200.17:5443/nvidia/k8s-device-plugin:v0.19.2@sha256:d4a9fdb14cacd97a6ad15ff549a6d9b52cfc908cb0552f65fa16ef034358677e` |
| DCGM Exporter image | `10.1.200.17:5443/nvidia/k8s/dcgm-exporter:4.5.3-4.8.2-distroless@sha256:e9030b4fca0c8f110032f3b151b030e7e9db56604b407cf79631e1a3218b35e0` |
| NFD image | `10.1.200.17:5443/nfd/node-feature-discovery:v0.18.3@sha256:7a9c4d658013e250d166704b81122e53a6b139f83de36be15a96884e8bcf7977` |
| applied values | SHA-256 `7f8b7ed9042667656d930bf3b278be53999cabefb1d502aad047141c9663afdb` |

The control host remains excluded from Vela GPU workloads through absence of
the Worker label plus `nvidia.com/gpu.deploy.operands=false`. No GPU-requesting
Pod was scheduled there. The shared experiment container retained ID
`b0a653da3926e90d88a6d3329fab8a927456e23ddfd6acb7d7d40cf6f9db0c94`
and remained running; the control Docker daemon was not restarted. Both Worker
Runners remained `running/healthy`, had zero GPU compute processes after smoke,
and both ZFS `data` pools remained `ONLINE`.

The private Registry data root now occupies about 3.4 GiB and the control root
filesystem had about 547 GiB free at postflight. These results are lab
deployment evidence only; Production Gates remain `0/9 PASS`.

After a final successful GPU verifier run, Worker 1 removed only the four
public GPU source digest refs and their four private Docker tags; no container
referenced those image IDs. No global Docker prune ran, and Registry blobs,
RKE2 containerd images, the Runner image, build cache, bundle state, and
receipts were preserved.

## Pre-deployment RKE2 and storage preflight

A second read-only preflight on 2026-08-29 confirmed:

- `rke2-server`, `rke2-agent`, K3s, kubelet, `/etc/rancher`,
  `/var/lib/rancher`, and `/var/lib/kubelet` were absent on all three hosts;
- RKE2 control, etcd, kubelet, NATS, and PostgreSQL ports were free; only the
  intended registry listener occupied `10.1.200.17:5443`;
- host UFW policy is inactive, while the existing Docker/NetBird iptables
  forwarding policy defaults to `DROP`; a future CNI installation must be
  checked against those existing chains rather than assuming an empty host;
- both Workers are time synchronized, had no GPU compute processes, about
  489 GiB available memory, and about 819 GiB free on their ext4 root volumes;
- each Worker had an online, almost-empty 7.25 TiB ZFS `data` pool across two
  3.64 TiB NVMe devices, but no XFS filesystem or project-quota root;
- `marslab-server` is time synchronized, had about 48 GiB available memory,
  3.2 GiB swap in use, one GPU compute process, and an 83%-allocated 1.86 TiB
  ZFS pool holding about 1.6 TiB of models; and
- the shared host's Docker store reported about 38.4 GB of images, including
  unrelated experiment content. No image, container, or cache was pruned.

These observations block direct application of the production Worker manifest:
it requires XFS project quota and a host Node Agent. They also make a one-node
control/storage deployment unsuitable for HA, quorum, DR, or Production Gate
evidence. At that point, installing RKE2 was a separate host/network change
with an air-gap image plan and exact rollback boundary. The repeatable staging
and applied deployment plan is recorded under
[`deploy/lab/rke2-airgap`](../../deploy/lab/rke2-airgap/README.md).

The official stable channel observed on 2026-08-29 resolves to
`v1.35.7+rke2r1`. Its complete `linux/amd64` Canal image archive is
822,789,229 bytes and its binary archive is 40,598,468 bytes, for a verified
external bundle of 863,417,533 bytes including metadata. One-byte HTTPS Range
probes from `marslab-server` succeeded through `https://ghfast.top/`, but the
full transfer there degraded to a few KiB/s. Worker 1 instead downloaded the
bundle once through `https://gh-proxy.com/`; the pinned fetcher verified every
byte count and SHA-256 before the files were served over `10.1.200.0/24` to
`marslab-server` and verified there again. The temporary Worker listener and
all seven Worker staging files were then removed. No image bytes crossed the
SSH administration path, and no archive was copied to Worker 2.

All three LAN interfaces reported a currently negotiated link speed of
`100 Mbps`. The approximately 1.10 GiB RKE2 plus GPU base therefore has an
ideal per-node wire-time floor of about 95 seconds before overhead. Initial
Worker pulls must run one at a time with a two-to-three-minute allowance per
Worker so that simultaneous pulls do not divide the same control-host link.

A metadata-only run of the staged fetcher first completed directly on
`marslab-server`. The completed stage is now retained at
`/srv/vela-rke2-airgap/v1.35.7-rke2r1` as `root:root`: both directories and the
fetcher are mode `0700`, while `release.env` and all five verified artifacts are
mode `0600`. It contains no partial files or credentials. The fetcher SHA-256 is
`5157ac2b090380aa25f95c587c90abb9eeb8c33c7e011712598c76c22a321b1d`.
The binary and image archives matched SHA-256
`7a29a6fbf512903a6a4611a289bcb3bdf01ddeea9df1a09b1bd1f210f3d6948a`
and `f4b265078f3b4763ff4ff7387211196323876935a66a4d2bca212142be45921a`.
The official checksum manifest, image list, and installer matched the pinned SHA-256 values
`373b8b0499f2510ac6f4be5cdf4d0e32bd9db9ce5b1ed5b0b31a0cb0cedbd8b7`,
`4233722b84f17e2debdf8a4781acf6600f057e2e1faff6a47a18fb3a3aa44d77`,
and `2d24db2184dd6b1a5e281fa45cc9a8234c889394721746f89b5fe953fdaaf40a`.

The LAN addresses were observed as DHCP-managed, not statically configured;
host state alone cannot prove whether the DHCP server already reserves them.
The Registry certificate, Registry listener, future RKE2 API certificate, and
agent join endpoint all depend on the current `.16`, `.17`, and `.19`
addresses. Proof of an existing DHCP reservation remains preferable, but the
lab may proceed under an explicit dynamic-address risk acceptance:

| Host | Interface | MAC | Required reservation |
| --- | --- | --- | --- |
| `marslab-server` | `enp34s0f0` | `9c:6b:00:4f:76:d5` | `10.1.200.17` |
| Worker 1 | `eno1` | `3c:ec:ef:66:c3:00` | `10.1.200.19` |
| Worker 2 | `eno1` | `3c:ec:ef:68:d9:a4` | `10.1.200.16` |

Client-side state identifies the DHCP server and default gateway as
`10.1.200.1`, but cannot prove its reservation table. The control host also
logged two carrier-loss/DHCP-lease-loss events followed by reacquisition of
`.17` on 2026-08-29. Retaining the same address after reacquisition is useful
observation, not reservation evidence.

On 2026-08-29, and again on 2026-08-30, the operator confirmed that these LAN
addresses normally remain stable and approved temporary use without reservation
proof. Deployment guards record that decision as `--dynamic-ip-risk-approved`;
it must never be represented as `--dhcp-reservation-proven`. A changed control
address requires reissuing Registry/API certificates and updating all pinned
endpoints before the cluster can be considered healthy again.

Both Workers also report the same OS hostname `ubuntu`, so their future RKE2
`node-name` values must be explicitly unique. Non-secret candidate configs are
checked in under `deploy/lab/rke2-airgap/config`; tokens and Registry
credentials are not.

The two Workers and `marslab-server` have matching NVIDIA driver `580.159.03`,
NVIDIA Container Runtime/Toolkit `1.19.1`, and
`/etc/nvidia-container-runtime/config.toml` SHA-256
`65ff485fab3e17754169b066b0a910221b3de4bc8de4089c2c345357316a7982`.
That was initially only host evidence. The applied RKE2 containerd configs on
both Workers now contain the `nvidia` runtime with
`BinaryName = "/usr/bin/nvidia-container-runtime"`; the GPU receipt above adds
RuntimeClass, Device Plugin, allocatable-GPU, placement, and Pod execution
evidence without changing the host-owned driver/toolkit lifecycle.

All three hosts received the expected `401` challenge from `nvcr.io` and
`registry.k8s.io`. Direct Docker Hub access timed out, while
the configured `https://docker.1ms.run/` endpoint returned the expected
challenge. Its manifest service did not resolve the pinned RKE2 runtime tag, so
the lab does not rely on it for the RKE2 system-image bundle. Public GPU
operands were mirrored by exact `linux/amd64` digest and then consumed only
from the private Registry.

The corrected pinned GPU Operator values were rendered with a checksum-verified
Helm `v3.18.6`. Twenty-seven objects rendered, and the complete enabled direct
Pod plus ClusterPolicy operand inventory resolved to exactly the four private
NFD, GPU Operator, Device Plugin/GFD, and DCGM Exporter images recorded in the
air-gap runbook. vGPU, VFIO, sandbox/Kata, and Confidential Computing managers
were disabled. The four images were subsequently published and the applied
ClusterPolicy reconfirmed the same disabled lifecycle set.

The initial control-host Docker probe failed TLS verification because the
private CA trust file was absent. Before one-time publication, the existing CA
was installed at `/etc/docker/certs.d/10.1.200.17:5443/ca.crt` and matched by
SHA-256 without changing `daemon.json` or restarting the shared Docker daemon.

The read-only preflight and postflight helpers are maintained beside the RKE2
staging plan. The preflight records the previously existing empty root-owned
`/etc/cni/net.d` directory on every host. This matters because the upstream
tarball uninstaller removes `/etc/cni`; whole-lab rollback must restore that
baseline instead of treating every item under `/etc/cni` as RKE2-owned.

Guarded helpers covered one-time RKE2/GPU image publication, raw before/post
state receipt capture, and binary-only node installation. They required literal
`--apply`, exact host/role checks, root-only credential inputs, and separate
dynamic-address/swap confirmations. Publication used resumable root-only state
files; all final publication states are `complete`. The RKE2 services were
enabled and started only after installation postconditions and before-state
receipts passed.

The same preflight script, SHA-256
`da11c2593c07d10adcbd52d4022661ccd7ea7c87d4ce327e5f2abf5831dfea4b`,
was then streamed through root on all three hosts without the external DHCP or
swap-exception approval flags. Each run deliberately returned
`FAIL failures=2`; the only failures were
`dhcp-reservation=external-proof-required` and
`swap-policy=lab-exception-approval-required`. Hostname, LAN address and
gateway, cgroup v2, NTP, AppArmor, required kernel modules, proposed
Pod/Service CIDR non-overlap, UFW inactivity, clean RKE2 paths and ports,
ext4/free space, eight physical GPUs, matching NVIDIA runtime/config, existing
managed containers, Registry `401`, idle Worker GPUs, and Worker `data:ONLINE`
all passed. This is a validated fail-closed preflight, not first-start
approval.

The same pinned preflight was rerun on all three hosts at
`2026-08-29T13:17:53Z`. Each host again returned exactly `FAIL failures=2` for
the same missing DHCP-reservation proof and swap-exception approval, with no
new failure. RKE2 binaries, units, services, and owned paths remained absent;
the Registry and shared control-host container retained their expected
identities; both persistent Runners remained running and healthy; and all three
hosts reported zero GPU compute processes at this revalidation.

After the pidfd recovery rehearsal, the current script was staged by its same
SHA-256, rerun as root, and removed again. The server receipt was captured at
`2026-08-29T14:13:39Z`, Worker 1 at `2026-08-29T14:14:24Z`, and Worker 2 at
`2026-08-29T14:14:49Z`. Every host again returned exactly
`FAIL failures=2`; the only failed checks remained `dhcp-reservation` and
`swap-policy`. The control receipt observed zero GPU compute processes, and the
Worker checks reconfirmed each Runner was `healthy` after its recovery exercise.

| Node | Captured UTC | IPv4 addresses | IPv4 routes | iptables | ip6tables | nftables |
| --- | --- | --- | --- | --- | --- | --- |
| `marslab-server` | `2026-08-29T12:41:29Z` | `69cb4319152a4ead396f6bbdb1a4fca61c7f5586fc09d34e0eb9518ac1bed86b` | `c7d489f1add535d2f7ff3ee022086288d8e550b0f4a37726def490a5546d6bb6` | `c9e236dba878b37abe5dad1bc9c4d84005a84f098dd9775ce374814a31527fa4` | `c3ec3b1e388ce37bca7147a72159b5ccf661a4734eb4022d62229a1ebf044004` | `ee92a1f3d2fb9ac0313a7ea6fe28a860c0bc56039469dae9a97712a0aa982066` |
| Worker 1 | `2026-08-29T12:41:29Z` | `c3c53d7f59a64a65855d06d1600167f3dd9017972389210e8ccc1318e82b597c` | `2b66bffa63ddc7c17accceba5f18c5ebece759e0d7196c7aa227eddae21760b4` | `a721ebe807011ef9863a2e069c7334b1dc6118e531ba93ec8add5ff40623af76` | `84c53b6265e2c065b35ca420fba7b5eb57191a82338a0164a7caeadc9be6c979` | `c573d90369c253ddca3eed730bcd62d1c84cc20a9d3f0a1f9c626f3261fcc886` |
| Worker 2 | `2026-08-29T12:41:29Z` | `b566526ce82e024ec3c6cd9a01f5e2c40ad9626c9462e820e9578c146091f90b` | `565c40474e7840f9b6a5984d4047ee3576afdf62cbd097385cb6858795318183` | `ea31cb4944c6ddd01fd7962961b23adcc8b8d995e1e7ed90e8dc50299a8a46b5` | `84c53b6265e2c065b35ca420fba7b5eb57191a82338a0164a7caeadc9be6c979` | `61e7bd93fa0074c2f661c237c2307eece1b61d95fca8e6ab484571bc233d05c8` |

These hashes can detect drift but are not restore material. Immediately before
an approved first start, rerun the preflight with DHCP proof and retain the raw
root-only routes and firewall rules alongside the terminal receipt. The
temporary preflight scripts were removed from all hosts after these runs.

All three nodes have one active 8 GiB `/swap.img`. Both Workers reported zero
bytes used; `marslab-server` reported 3,355,738,112 bytes used and about 51.2 GB
memory available. Kubernetes documents that kubelet does not start with swap by
default and recommends control-plane nodes without swap. Disabling host swap is
not authorized on this shared machine. The lab candidate therefore preserves
host swap and sets `fail-swap-on=false`, with `NoSwap` as the requested Pod
policy. The initial live `configz` responses reported `failSwapOn=false` but
only `memorySwap: {}`. A retained 2026-08-31T02:41:14Z read-only postflight
again reports that exact state on all three nodes; it does not expose an
explicit `memorySwap.swapBehavior=NoSwap`. The verifier intentionally fails
closed on the missing field. Applying a kubelet configuration drop-in and
restarting RKE2 on the shared control host and both Workers remains a separate
operation requiring explicit approval. Active host swap on the shared control
node remains a lab exception and cannot satisfy a production control-plane
gate.

A fresh run of the strict `verify-cluster.sh gpu` at
`2026-08-31T02:41:14Z` passed API, node identity, version, labels, taints,
DaemonSet, Deployment, Canal, and GPU `0/8/8` checks, but returned
`FAIL failures=4`. Three failures are the missing explicit `NoSwap` field; the
fourth covers 17 retained historical failed lab Pods. This current retained
receipt supersedes the earlier unretained claim that the same verifier failed
only on cluster cleanliness. The organization-isolation harness deleted each
of its own temporary Pods by exact UID, so it did not increase that count. The
historical Pods remain available for diagnosis because deleting them was not
authorized. A clean strict postflight therefore remains pending.

The GPU smoke helper also passed its two mutation guards: without literal
`--apply` it failed before privilege or cluster access, and a root test with a
stub reporting an existing `vela-gpu-smoke` namespace failed before any apply
operation. No Kubernetes API was contacted during either negative test.

`marslab-server` is intentionally not a Worker. The registry container has no
GPU device requests or device mounts. The control host's existing container
`fchip-4591d89ff18127a74b8a25a0` remained running with container ID
`b0a653da3926e90d88a6d3329fab8a927456e23ddfd6acb7d7d40cf6f9db0c94`.
The Docker daemon was not restarted on this shared host.

The preflight observed an unrelated Python process using 280 MiB on GPU 3.
That process was absent at the post-deployment check. No Vela or registry
process received a GPU, and this receipt does not attribute the external
process exit to the registry deployment.

## Private registry

The registry endpoint is:

```text
10.1.200.17:5443
```

The deployment uses OCI Distribution `registry:2.8.3` for `linux/amd64`:

```text
docker.io/library/registry@sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373
```

Because the control host's external image pull stalled, the exact pinned image
was exported from Worker 1 and transferred once as a 9.7 MiB compressed
archive. Its transfer SHA-256 was
`6a7f9ec22ca8d0b69d6786e6342c9168d38bddfb0209003769edb984b64fd3d1`.
After import, Docker retained the immutable local content ID
`sha256:a3d8aaa63ed8681a604f1dea0aa03f100d5895b6a58ace528858a7b332415373`.
Future application images must be pushed once to the registry and pulled over
the LAN instead of being copied between hosts over SSH.

Registry state and credentials are separated:

| Purpose | Path on `marslab-server` | Access |
| --- | --- | --- |
| registry blobs | `/srv/vela-registry` | root-owned, mode `0750` |
| CA and server certificate | `/etc/vela-registry/tls` | root-owned directory, mode `0700` |
| bcrypt `htpasswd` database | `/etc/vela-registry/auth/htpasswd` | root-owned, mode `0600` |
| bootstrap credential | `/etc/vela-registry/secrets/vela-lab.password` | root-owned, mode `0600` |
| publisher credential | `/etc/vela-registry/secrets/vela-rke2-publisher.{username,password}` | root-owned, mode `0600` |
| control pull credential | `/etc/vela-registry/secrets/vela-rke2-control-1.{username,password}` | root-owned, mode `0600` |
| Worker 1 pull credential | `/etc/vela-registry/secrets/vela-rke2-worker-1.{username,password}` | root-owned, mode `0600` |
| Worker 2 pull credential | `/etc/vela-registry/secrets/vela-rke2-worker-2.{username,password}` | root-owned, mode `0600` |

Credential values are deliberately absent from the repository. Publication,
control, and both Workers use separate accounts. Distribution `htpasswd` does
not provide repository-level read-only authorization, so this is still a
lab trust boundary rather than production registry policy. Worker validation
used temporary Docker configuration directories; no registry password was left
in a Worker user's `~/.docker/config.json`.

The private CA has SHA-256 fingerprint
`C3:95:84:41:C6:3E:07:E3:EC:84:9C:C2:D7:CB:13:D6:55:D6:BB:23:E3:0D:8A:5B:60:B8:75:00:9B:AA:63:26`
and expires on 2036-08-26. The server certificate has the IP SAN
`10.1.200.17` and expires on 2028-12-01. Both Workers trust the CA through:

```text
/etc/docker/certs.d/10.1.200.17:5443/ca.crt
/usr/local/share/ca-certificates/vela-lab-registry.crt
```

No host uses Docker `insecure-registries`. All three hosts use the existing
Docker Hub mirror `https://docker.1ms.run/`. Worker mirror configuration was
installed without removing the NVIDIA runtime; the pre-change files are:

```text
# Worker 1
/etc/docker/daemon.json.vela-backup-20260829T074346Z

# Worker 2
/etc/docker/daemon.json.vela-backup-20260829T074907Z
```

The `vela-registry` container is bound only to
`10.1.200.17:5443 -> 5000/tcp` and has these limits:

- two CPUs, 2 GiB memory and swap ceiling, and 256 PIDs;
- read-only root filesystem with a 64 MiB `/tmp` tmpfs;
- all Linux capabilities dropped and `no-new-privileges` enabled;
- `unless-stopped` restart policy;
- `json-file` logs capped at three 10 MiB files; and
- registry deletion disabled.

At initial registry validation, the container used about 11 MiB memory, the
pinned image occupied about 9.7 MiB, and `/srv/vela-registry` used about
2.3 MiB. After RKE2, GPU Operator, and mock Runner publication, the same data
root occupied about 3.4 GiB.

## Validation receipt

The following checks passed on 2026-08-29:

1. Port `5443`, `/srv/vela-registry`, `/etc/vela-registry`, and the
   `vela-registry` container name were free before deployment.
2. The server certificate verified against the private CA and contained only
   the intended IP SAN.
3. An unauthenticated `GET /v2/` returned `401`; an authenticated request
   returned `200` after the persistent credential rotation.
4. Worker 1 pushed the probe image to
   `10.1.200.17:5443/vela-lab/busybox:linux-amd64-7a3ebe5bfd1a`.
5. The registry reported the OCI manifest digest
   `sha256:7a3ebe5bfd1a4a19797d20b0c0bb39d44393e9a03fd852c0865b0f540d868df0`.
6. Worker 2 removed its prior BusyBox image, pulled the private image by that
   digest, verified `linux/amd64`, and ran it successfully with output
   `vela-registry-probe-ok`.
7. Both Worker Docker daemons returned to `active`; the Docker Hub mirror and
   `nvidia` runtime remained configured.
8. Before runner publication, the registry catalog contained only the probe
   repository. After publication it contained exactly `vela-lab/busybox` and
   `vela-lab/vela-h3-runner`.
9. The shared control-host container retained its preflight container ID and
   remained running; the registry has no GPU access.

These registry checks prove TLS, authentication, LAN push, digest-addressed LAN
pull, and basic container execution. By themselves they do not prove H3
protocol behavior, eight-GPU placement, cancellation, recovery, Artifact
publication, or performance; the separate mock Runner checks below cover only
the explicitly listed protocol cases.

## Non-canonical mock runner image

Worker 1 built an experiment-only H3 mock runner from release revision
`bc590e20b3e81ee54651ac7766c8ecd82b394097` and mock backend SHA-256
`765077057011f16f852886601235f066dff7a89d3127719a5ae3c38206c7aee6`.
The image carries `vela.ai.build-kind=noncanonical-lab` and was published to the
LAN registry as:

```text
10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3
```

The image is 52,966,426 bytes unpacked according to Docker. A gzip-compressed
`docker save` stream was 52,593,966 bytes. After publication, the complete
registry data root, including the earlier BusyBox probe, occupied 55,183,173
bytes. Worker 2 pulled the runner by digest over the LAN and verified the same
image ID, runtime UID/GID `10001:10001`, Python 3.13.11 entrypoint environment,
and embedded backend SHA-256. Containers on both Workers each observed exactly
eight distinct GPU UUIDs through the NVIDIA runtime.

No runner image bytes were copied between hosts over the SSH administration
path. SSH carried only the previously staged source, release helper, backend,
and a 22,650,722-byte legacy UV archive; the application image was pushed once
from Worker 1 to `10.1.200.17` and pulled by Worker 2 over `10.1.200.0/24`.

This is deliberately not a Slice 44 canonical publication receipt. The Worker
could not reach `proxy.golang.org`, and the legacy-loaded UV image retained the
correct filesystem content but not the original pinned GHCR manifest identity.
The lab build therefore skipped the Go builder, independently rechecked the
backend SHA-256 in the image build, and used the already loaded UV filesystem.
It must not be substituted for canonical image, signature, SBOM, scan, approval,
or Launch Receipt evidence. Production Gates remain `0/9 PASS`.

Both Worker temporary Docker authentication directories were removed.
Dedicated publisher/control/Worker credentials were added by hot-reloading the
root-only `htpasswd` file without restarting Registry or Docker. After one
Worker credential was accidentally exposed by an overly broad runtime config
read, that credential was rotated, propagated to its root-only RKE2 inputs, and
the old secret plus backups were removed. Unauthenticated `/v2/` returned
`401`, each intended credential returned `200`, and pinned manifests returned
`200` by digest. The registry container remained `running`.

Each Worker then ran an ephemeral container through the image's production
`RunnerServer` Unix-socket gRPC interface. Both executions passed the same
checks:

1. `DEVICE`, `INFERENCE_BACKEND`, `MODEL_WARMUP`, and `CANARY` readiness were
   accepted and passed. `DEVICE` bound the first physical GPU UUID to
   Encoder/VAE and the remaining seven unique UUIDs to DiT.
2. An exact mock profile was prepared and started, reached `SUCCEEDED`, and
   returned one verified `THUMBNAIL` plus one verified `VIDEO`; byte counts and
   SHA-256 digests matched the output files.
3. Injected failure reached `FAILED` with bounded class `CUDA_OOM`, fingerprint
   `mock/cuda-oom/dit`, the second bound GPU UUID, retry recommended, Worker not
   reusable, and no collectable output.
4. Hang mode reported `mock/hang`, accepted a control-plane cancellation,
   reached `CANCELED`, and exposed no collectable output.

The harness and its state lived only in the disposable container `/tmp` and
were removed with the container. This verifies the mock Runner/backend protocol
against both eight-GPU Docker runtimes. It still does not deploy a persistent
Worker Agent or Kubernetes Worker Pod, exercise control-plane Assignment and
Lease authority, upload Artifacts, execute a sustained workload, or prove real
H3 behavior or performance.

The repeatable persistent Runner deployment is maintained under
[`deploy/lab/h3-mock`](../../deploy/lab/h3-mock/README.md). It preserves the
same digest and physical eight-GPU role boundary while adding restart policy,
bounded resources, a persistent Unix socket, and a structured smoke client. It
still excludes the Worker Agent, Assignment/Lease authority, Artifact upload,
and all production claims.

## Persistent mock Runner receipt

The digest-pinned Runner is now installed on both Workers as
`vela-h3-mock-runner`. Its state is rooted at
`/var/lib/vela-lab/mock-runner`; no Runner container was installed on
`marslab-server`.

| Worker | Stable lab identity | Node materialization revision |
| --- | --- | --- |
| `10.1.200.19` | `72ff1aeb-fcc6-e75a-989b-f580e2ed6f47` | `sha256:4e8d6f43fd421088b54c57272b1cd6f294fa44e0eda220aa13c9c2bc7488ca0f` |
| `10.1.200.16` | `f0650c9c-e3cd-c7fd-b8e6-03dde18123a8` | `sha256:b9cfbf7069771467c259d09c5eaba2e71984fef7497a83f6be4df1b6d7130ff9` |

The revisions differ because each binds its own physical GPU UUID role map.
Both containers use the same immutable image digest recorded above and enforce:

- `network=none`, a read-only root filesystem, all capabilities dropped, and
  `no-new-privileges`;
- `unless-stopped`, four CPUs, 4 GiB memory and swap ceiling, and 256 PIDs;
- root-owned read-only profile/GPU configuration and UID/GID `10001:10001`
  private run, state, and output directories; and
- all eight physical GPUs visible for DEVICE checks without an idle compute
  process.

Both Workers also expose a root-owned mode-`0444`
`config/container-identity` file containing only its exact 64-character Docker
container ID and immutable Runner image digest. Existing containers received
this identity contract through a guarded, atomic helper upgrade without a
restart: container ID, PID, `StartedAt`, `RestartCount`, and health were
unchanged across that migration. The installed writer has SHA-256
`b63553613181fcdf11b24178b528ec8551bee835fc3033f715cdcbddb9525c0e`.
The corresponding installed start and remove helpers have SHA-256
`266b913e89433a50746b25ac8877cd78293d83dd21d6f82af0ba13c8e53a54da`
and
`5bf409876ae020e2a7f63dceca01e7a5682e81e538bb84f345be2dcab850c40c`.
This is an exact lab control identity, not a general host-container discovery
interface.

The initial and post-restart smoke checks passed on both Workers. Each check:

1. accepted and passed `DEVICE`, `INFERENCE_BACKEND`, `MODEL_WARMUP`, and
   `CANARY` through the persistent owner-only Unix socket;
2. prepared and completed a new mock Attempt as `SUCCEEDED`;
3. re-read the output files and verified a 164-byte `THUMBNAIL` with SHA-256
   `4a5bebd434087d98bcc98e7c7f52a81e842b740800acd5e212de905b12282a58`;
4. verified a 12,865-byte `VIDEO` with SHA-256
   `d69f5e36470074ff2e23fe1538eba01da9abc41c4a2ad71885102e4f79116658`;
5. restarted the managed container, returned to `healthy`, and repeated the
   complete smoke successfully; and
6. left zero GPU compute processes from the Runner. Idle memory was about
   24-25 MiB per Worker.

An active-Attempt abrupt-restart exercise also passed on both Workers. The
original rehearsal resolved the exact host PID and sent host `SIGKILL` while the
Attempt was `RUNNING` with `resume=true`. On 2026-08-29, both installed recovery
scripts were upgraded from SHA-256
`0d09d8682706849e8941d3cfb85b2d20426d08d1c9f3e6abc854347dbb872923` to
`a96a08813b70d1213945c7db5edf9f9dbd278ae304841764d55424756b032f6a` as
`root:root` mode `0550`; each previous script is retained at
`recover-mock-runner.sh.pre-pidfd-20260829` in the same root-only admin
directory.

The upgraded harness opens a Linux pidfd and revalidates the container ID, PID,
start timestamp, running state, and cgroup membership immediately before
signaling. A fresh exercise through that path produced these receipts:

| Worker | Attempt / Job | PID before -> after | `StartedAt` before -> after | Final `RestartCount` |
| --- | --- | --- | --- | ---: |
| `10.1.200.19` | `a9547f89-e679-4b4b-971c-77f55c9b3361` / `e17d252a-eba6-468b-9569-395bf5243e52` | `51252` -> `92711` | `2026-08-29T11:22:28.794944432Z` -> `2026-08-29T14:04:18.074152115Z` | 2 |
| `10.1.200.16` | `bc10b89b-88bb-4076-ae9c-ce8ea83f2b40` / `fec75e7e-3e8b-451e-87b5-5bd333a6f162` | `45429` -> `87097` | `2026-08-29T11:22:28.768136437Z` -> `2026-08-29T14:05:25.524388342Z` | 2 |

After each container returned to `healthy`, the client replayed the exact
Attempt, Job, Worker epoch, and Lease fence from local state. Both Runners
returned `resumed_local_state=true`, reached `SUCCEEDED`, and reproduced the two
output digests above. A separate postflight confirmed each container remained
`healthy` at `RestartCount=2` and that no GPU compute process remained.

The fault harness deliberately does not use `docker kill`: Docker treats that
CLI operation as an operator stop and suppresses restart-policy recovery. A
signal from inside the PID namespace also cannot be used to kill protected PID
1. The maintained harness therefore requires root, validates the exact managed
container before resolving the host PID, and verifies all restart identities
afterward.

This proves repeatable persistent mock Runner startup, terminal-state
replacement, and same-authority local recovery after a Runner process crash. It
does not prove Worker Agent fencing, stale-Lease rejection, control-plane
reconciliation, node loss, or any Production Gate.

## Vela application control-plane receipt

The non-production application control plane is deployed in the `vela-lab`
namespace. `marslab-server` runs PostgreSQL, three NATS replicas, MinIO, the
bootstrap Job, and one `vela-control` replica. Each GPU host runs one
digest-pinned Worker Agent plus its host-managed persistent mock Runner. The
application Pods request zero GPUs; the control node exposes zero allocatable
GPUs and remains excluded from Vela Worker placement.

The current immutable application images are:

| Component | Image |
| --- | --- |
| PostgreSQL | `10.1.200.17:5443/vela-lab/postgres@sha256:45cd22f8d32e189d245403954882f88e7a8714301fda80dab6da90f1265b25a3` |
| NATS | `10.1.200.17:5443/vela-lab/nats@sha256:26b0ee1a95285aedae137aefb953701d9da1dfffcf7818eb3aeb536c4373892f` |
| MinIO | `10.1.200.17:5443/vela-lab/minio@sha256:3f97c5651cb6662b880c787a232b6b34fec8d8922e08d6617b25d241a21164bb` |
| bootstrap and smoke | `10.1.200.17:5443/vela-lab/vela-lab-bootstrap@sha256:3f6a8bc440ee7bd7f9ba263d07435329c0863134217349db565cded2e9df9eac` |
| Worker Agent | `10.1.200.17:5443/vela-lab/vela-worker-agent@sha256:bf6db207e52dbbaad4bcb0f4aaad739854b6cd3c6ff7088beda90705d6fab9e0` |
| control | `10.1.200.17:5443/vela-lab/vela-control@sha256:970aca18ba6709406729257fb91131ec87ab3b5f5d200ef338d593b77dc0b198` |

The current v20 control image is a bounded replacement of v19 digest
`10.1.200.17:5443/vela-lab/vela-control@sha256:b4fd44f2a266522b2cd60e8b62c64efd943b7add487c835802e75eca5d804d0f`,
which remains available as the rollback image. Its tagged Linux binary has
uncompressed SHA-256
`764a4837b2b716c12a108151c4a011ec4b2c7d52d54acc2c0431b9cecf4d9395`
at about 33 MiB; the approximately 11 MiB gzip transfer has SHA-256
`6bb72b76bdd590b49e6b43d44ac309c580f39e08bcbdb668f500589daee714e5`.
The control host assembled and published the scratch image locally. Existing
layers were reused and the Registry added only about 11.36 MiB, so no OCI
archive crossed the SSH path.

The v19 control image was itself a bounded replacement of the preceding
`10.1.200.17:5443/vela-lab/vela-control@sha256:a056d70743a1dfab5ff7fbf01a3c22b789073974ff86ddbc1d781048b666c3b7`
image. To avoid the constrained
SSH path, the local build sent only an approximately 11 MiB gzip-compressed
Linux binary; no OCI archive or image layer crossed that path. The compressed
transfer has SHA-256
`e3f8712467f6eb4f3b04db9a34fe439d9cfbc346f8736bc28ec27507b21d8df0`,
and the uncompressed binary has SHA-256
`0a24d66ece5f517c52bb8e13165ae5483cced55d8e8ddbbd4a66e5bb66eecbec`.
The control host assembled and pushed the small scratch image to the LAN
Registry.

Post-v19 local review additionally made the Outbox marker reader reject
duplicate JSON keys. That hardening was not part of v19 and is not provenance
for its retained live receipts. The v20 binary supersedes v19 and includes the
reviewed marker reader plus the separately gated Consumer fault hook.

The applied v20 manifests are retained at
`/root/vela-lab-deploy-bc590e20/rendered-manifests-v20`. They were copied from
the actually deployed v19 set and changed only the control image digest in
`30-control.yaml` and `images.env`. An API Server dry-run and normalized live
Deployment diff showed only that image field changing. The rollout retained
the management port at `8081`, produced one Ready control Pod on
`vela-lab-control-1`, and the Pod spec plus runtime image ID both matched the
v20 manifest digest. The preflight and patch evidence is retained under
`/root/vela-lab-deploy-bc590e20/control-v20-*`.

The preceding v19 manifests remain retained at
`/root/vela-lab-deploy-bc590e20/rendered-manifests-v19`. They were copied from
the actually deployed v18 set and changed only the control image digest. A
fresh render from the current repository template would also have deleted the
live management-port entries from the Service and NetworkPolicy, so that apply
was rejected and retained only for diagnosis at
`/root/vela-lab-deploy-bc590e20/rendered-manifests-v19-rejected-template-drift`.
The root-level historical `images.env` remains non-authoritative. The root-only
rollout receipt is
`/root/vela-lab-deploy-bc590e20/receipts/control-publisher-fault-rollout-v1`;
its `SHA256SUMS` file has SHA-256
`de8fd06d1f8f57ce4bbf096c7bf399c0defff6869956ea111d0bc4ca375bb338`.

The control Pod remains UID/GID `10001:10001`, drops all outer capabilities,
disallows privilege escalation, and uses a read-only root filesystem. Ubuntu's
RuntimeDefault seccomp and `unprivileged_userns` AppArmor profiles reject the
namespace-bearing inner ffprobe sandbox, so only this lab control container has
Pod-level seccomp and AppArmor set to `Unconfined`. It is not privileged and
does not receive `CAP_SYS_ADMIN` or any other outer capability.

For each Artifact probe, the inner sandbox creates user, network, PID, mount,
IPC, and UTS namespaces with a single non-translating UID/GID mapping
`10001 -> 10001`. It clears all capability sets, applies `no_new_privs` and
resource limits, and uses Landlock to allow only execution/read of a staged,
pinned static ffprobe plus read of the staged Artifact input. The helper is
removed before probing, the sandbox directory is locked to mode `0500`, and the
input is mode `0400`. Descriptor-relative execution and input reads were not
used because Ubuntu AppArmor rejects their disconnected `/dev/fd/*` paths.
Staging adds one bounded local disk copy per Artifact and no network transfer.

The sandbox passed a Linux container test under the same UID/GID, dropped
capabilities, `no-new-privileges`, read-only root filesystem, no network, and
bounded PID/memory/CPU settings used by the lab. The test ran the pinned
ffprobe 8.0.1 against real H.264 MP4 and WebP inputs, and the complete
`internal/artifactvalidator` test binary passed after its read-only test fixture
cleanup was made explicit.

In the preceding sandbox-v17 rollout, an API Server dry-run showed that only
the control image digest changed from the previous rendered manifests. That
control rollout completed with one Ready Pod,
zero restarts, and the expected Registry image ID. Worker 1 was then restored
from `DRAINING/HEALTHY` to `READY/HEALTHY` by an atomic, exact-identity SQL guard
that also required epoch `1` and zero unrevoked leases.

The fresh smoke Job `vela-lab-smoke-6l87r` produced application Job
`f0c19a02-568c-4268-b6ab-a40b828daba9` with final state `SUCCEEDED`. The smoke
client required a non-empty object version ID, downloaded each object, and
matched downloaded size and SHA-256 to the committed metadata:

| Kind | Size | SHA-256 | State | Validator revision |
| --- | ---: | --- | --- | --- |
| `VIDEO` | 12,865 bytes | `d69f5e36470074ff2e23fe1538eba01da9abc41c4a2ad71885102e4f79116658` | `COMMITTED` | `ffprobe-8.0.1-sandbox-v1` |
| `THUMBNAIL` | 164 bytes | `4a5bebd434087d98bcc98e7c7f52a81e842b740800acd5e212de905b12282a58` | `COMMITTED` | `ffprobe-8.0.1-sandbox-v1` |

Postflight found zero unrevoked Attempt Leases, zero Production Gate receipts,
both Worker records `READY/HEALTHY`, all application Deployments and StatefulSets
Ready, both persistent Runners healthy, and zero GPU compute processes on both
Workers. Root-only rollout and smoke receipts are retained on `marslab-server`
under `/root/vela-lab-deploy-bc590e20/receipts/sandbox-v17`. Its `SHA256SUMS`
manifest self-checks, and that manifest has SHA-256
`70445093cf5863c25ea61db9863b8716174872dcb62fa5fe914debfa9678203e`.

## Concurrent mock endurance rehearsal

On 2026-08-30, the root-only harness
`deploy/lab/control-plane/mock-endurance.sh` was installed on
`marslab-server` with SHA-256
`b8090a826c24d9ef0960f9418bb3e3333f18d7d97bbb4b092b659472be8cf8a4`.
Each wave starts two smoke Jobs concurrently and then independently verifies
for each application Job: terminal `SUCCEEDED`, exactly one Visible Completion,
exactly one posted completion Charge, exactly two committed Artifacts, one
`VIDEO`, one `THUMBNAIL`, complete immutable-object metadata, the pinned
`ffprobe-8.0.1-sandbox-v1` validation receipt, and zero active Leases. The
harness also requires both Worker identities to appear and refuses to proceed
unless exactly two Workers are `READY/HEALTHY`, active Leases are zero, and
Production Gate receipts are zero.

The first two-Job run is retained under
`receipts/mock-endurance-2job-v1` as diagnostic evidence only. Its checksum
manifest recorded absolute paths inside the pre-move temporary directory, so a
strict external check after the atomic directory move fails. It must not be
cited as a complete receipt. The corrected harness records relative paths and
its own digest. Its one-wave validation is retained under
`receipts/mock-endurance-2job-v2`; all 12 payload files listed by the manifest
pass `sha256sum --check --strict`, and its `SHA256SUMS` file has SHA-256
`9f1a03d825283615e5f6b6949a11adb212591a47a1f469808a151e7fd15de43b`.

The subsequent five-wave rehearsal ran from `2026-08-29T22:07:27Z` through
`2026-08-29T22:08:14Z` and is retained under
`receipts/mock-endurance-10job-v2`. Its independently checked result was:

| Check | Result |
| --- | ---: |
| Concurrent waves | 5 |
| Jobs in `SUCCEEDED` | 10 |
| Visible Completions | 10 |
| Posted completion Charges | 10 |
| Committed Artifacts | 20 |
| Jobs on `vela-lab-worker-1` | 5 |
| Jobs on `vela-lab-worker-2` | 5 |

All 36 payload files listed by the manifest pass
`sha256sum --check --strict`; the final
`SHA256SUMS` file has SHA-256
`a583b728c81fad4e9b68dd21a8d6670188fea9f6fd11fb8a240c04931981843a`.
Postflight again found zero active Leases, zero Production Gate receipts, and
exactly two `READY/HEALTHY` Worker records. The control node reported GPU
capacity and allocatable capacity `0/0`. Both persistent Runner containers were
`healthy`, their start timestamps predated the rehearsal, and both Workers had
zero GPU compute processes. Available root-disk space was 543 GiB on the
control host, 805 GiB on Worker 1, and 813 GiB on Worker 2, so no broad image or
Docker-cache pruning was performed.

This is a `NON_PRODUCTION_MOCK_REHEARSAL`. It hardens the concurrent smoke and
receipt harness only. Ten synthetic Jobs completed in 47 seconds do not satisfy
the real-H3, 72-hour, mixed-load, reconciliation, SLO, or fixed ten-scenario
fault-injection requirements, and no Launch Receipt was created.

## Worker-control network-partition rehearsal

On 2026-08-30, the first live fixed-scenario rehearsal exercised loss of the
Worker 1 Agent-to-control path while its persistent mock Runner remained in a
non-terminating execution. The harness uses a temporary NetworkPolicy, deletes
only the Worker 1 Agent Pod, and keeps Worker 2 connected so the scheduler can
place a replacement Attempt after the 120-second Lease TTL and 30-second lost
grace. A root-only watchdog and the normal exit trap both restore the mock mode,
baseline NetworkPolicy, Agent Deployments, and exact Worker authority.

The original mock Catalog revision could not support this timing contract:
`balanced` revision 1 had `certified_p95_compute_seconds=30`, so the service
class multiplier produced only 60 seconds of total compute budget. Diagnostic
Job `8d8ba8e3-1c24-463b-b437-127c64abdff8` therefore reached `LOST` but
correctly ended `FAILED` before a replacement could run. It is not a passed
scenario. The lab now has an immutable replacement Catalog revision with these
exact records:

| Record | Revision 1 | Replacement revision |
| --- | --- | --- |
| GenerationPresetRevision | `84000000-0000-0000-0000-000000000005`, `DRAINING`, p95 30 seconds | `84000000-0000-0000-0000-000000000201`, `ACTIVE`, p95 120 seconds |
| ProfileCertification | `84000000-0000-0000-0000-00000000000a`, `DRAINING` | `84000000-0000-0000-0000-000000000202`, `ACTIVE`, mock-only evidence digest |
| RateCardRevision / line | `84000000-0000-0000-0000-00000000000b` / `84000000-0000-0000-0000-00000000000c`, `DRAINING` | `84000000-0000-0000-0000-000000000203` / `84000000-0000-0000-0000-000000000204`, `ACTIVE` |

The replacement snapshot gives newly accepted Jobs a 240-second total compute
budget. Existing Jobs retain their original immutable execution snapshots. The
Catalog evidence protocol remains `LEGACY`, both RateCards charge the same one
minor CNY unit, Production Gate receipts remain zero, and these synthetic
records are not saleable Catalog evidence.

Because the Runner loads its certified profile allowlist at process start, both
Workers were atomically upgraded to retain revision 1 and add replacement
revision 2. The Runner image remained
`10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3`.
The resulting host-specific configuration revisions are
`sha256:7971f5e2e60c09b26c273f5e778a05a8ec201052242ece8357d4248fa479c83c`
on Worker 1 and
`sha256:d860c42c37a705492169cb48e4487d564b179f4d20ecb1b3f16b7b3f4d85e933`
on Worker 2. Before the Worker 1 restart, terminal state
`7a8b10eb-4844-4164-a85c-100b50bf581e` was moved, not deleted, into
`retired-runner-state` after the database proved one accepted completion, one
posted Charge, two committed Artifacts, and zero active Leases. Its retirement
receipt and checksum manifest are retained on Worker 1.

Diagnostic Job `b6ebecfe-2c0b-47c8-aa44-8107fea1cef1` demonstrated the
fail-closed allowlist boundary before the Runner upgrade: its first Attempt was
rejected during `Prepare` and the Job ended `FAILED` with zero compute seconds.
It also is not a passed scenario.

The final harness revision was installed as
`/root/vela-lab-deploy-bc590e20/control-plane/worker-control-network-partition-v3.sh`
with SHA-256
`2905f2bc73fed26ea3d6f848a6be44cdf5b2081fb5b096d06548e39deb343011`.
It adds a structural preflight of both Workers' Runner profile files and
retains those exact files in the receipt. Its passing repeat is retained at
`/root/vela-lab-deploy-bc590e20/receipts/worker-control-network-partition-v4`;
the earlier passing `v3` receipt remains preserved. The final result was:

| Check | Result |
| --- | --- |
| Application Job | `748f2624-cefc-4ebc-8331-13aeb3ad2b5e`, `SUCCEEDED` |
| Original Attempt | `80a65d14-d987-4a94-9f4c-d006d8ab3abe`, Worker 1, fence 1, `LOST` |
| Replacement Attempt | `2e9885d5-6d94-4301-9788-755d1a794f1e`, Worker 2, fence 3, `SUCCEEDED` |
| Compute budget | 121 seconds consumed of 240 seconds |
| Durable result | one Visible Completion, one posted completion Charge, two committed Artifacts |
| Fixed measurements | lost accepted Jobs 0; duplicate completions 0; duplicate Charges 0; stale-authority acceptances 0 |
| Fixed scenario matrix | `1/10` lab rehearsals complete |
| Production Gates | `0/9 PASS` |

All files in the receipt pass `sha256sum --check --strict`; the `SHA256SUMS`
file has SHA-256
`3e123d4c29ddec87e8fb1437096266edfed046cd78a37f853cdcb5489d1f4950`.
The receipt includes the exact Catalog and Runner-profile observations,
pre/post authority, the authority timeline, all NetworkPolicy material, and raw
Outbox protobuf bytes encoded as base64 rather than misclassified as JSON.

Independent postflight found the baseline NetworkPolicy restored, no watchdog
marker, no active Lease or Job, no Production Gate receipt, exactly two
`READY/HEALTHY` Workers, all application Deployments and StatefulSets Ready,
both Runners `healthy` in `success` mode with only the Runner main process, and
zero GPU compute processes. This is a `NON_PRODUCTION_MOCK_REHEARSAL`, not a
Launch Receipt or a pass for the `state-event-fault-injection` Production Gate.

## Retry-budget-exhaustion rehearsal

On 2026-08-30, the second live fixed-scenario rehearsal set both persistent
Runners to a deterministic `TRANSIENT_BACKEND` failure mode and submitted one
Job whose immutable ServiceClassRevision permits exactly two Attempts. The
first Attempt ran on Worker 1 and transitioned `FAILED -> RETRY_WAIT`; the
second ran on Worker 2 and transitioned `FAILED -> Job FAILED`. Both failure
decisions record `retry_recommended=true` and `worker_reusable=true`. The final
Job state released its CreditReservation and produced zero Visible
Completions, zero Charges, and zero Artifact rows, including zero committed
Artifacts:

| Check | Result |
| --- | --- |
| Application Job | `d1a17749-b06b-417a-b3e9-b6763d2ef5ca`, `FAILED` |
| Attempt 1 | Worker 1, `FAILED`, disposition `RETRY_WAIT`, `TRANSIENT_BACKEND` |
| Attempt 2 | Worker 2, `FAILED`, disposition `FAILED`, `TRANSIENT_BACKEND` |
| CreditReservation | `RELEASED` |
| Durable result | zero Visible Completions, zero Charges, zero Artifact rows, including zero committed Artifacts |
| Fixed scenario matrix | `2/10` lab rehearsals complete |
| Production Gates | `0/9 PASS` |

The first diagnostic run, Job
`be886f1e-929a-4578-bdc6-37c6a33b382b`, correctly failed closed and is not a
scenario pass. The already-deployed mock backend encoded an absent
`gpu_uuids` value as JSON `null`; the Runner's strict receipt validator rejected
that shape and conservatively reported `BACKEND_PROCESS_FAILED` with
`worker_reusable=false`. Its incomplete diagnostic receipt is intentionally
retained at
`/root/vela-lab-deploy-bc590e20/receipts/.vela-lab-retry-budget-exhaustion.vraKMP`.

The repository backend now initializes an absent implicated-device list as an
empty JSON array, with regression coverage. To avoid sending the unchanged
Runner image over the constrained SSH path, the two Workers received only a
guarded wrapper update that adds `--mock-failure-gpu-index 0` in failure mode.
The Runner image therefore remained
`10.1.200.17:5443/vela-lab/vela-h3-runner@sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3`;
no image layer was rebuilt or transferred. The installed wrapper SHA-256 is
`7786408cf1219e9c2304cebc5c3d7f772a6c7455be63043d8ea2958c19543433`.
The wrapper-only updater SHA-256 is
`4daeeaab48b9caa429e77d500086b6f99a796401c6862ec2331cc9eb38f67c96`.

The passing receipt is retained at
`/root/vela-lab-deploy-bc590e20/receipts/retry-budget-exhaustion-v1`.
All files listed by its manifest pass `sha256sum --check --strict`; the
`SHA256SUMS` file has SHA-256
`9d47f814b079743345a7af35d66c139800398ef750b8b33439ed61c00ad90b00`,
and the retained harness has SHA-256
`852a7ff868bb2cb88808bd746c74e42ed0186865f4ca19d0b7848954f2a13222`.
The receipt includes both retry decisions, complete authority state, the
scenario matrix, both Runner profiles, and base64-preserved raw protobuf
events.

Review then hardened the repository harness to bind and validate the preceding
scenario receipt, reject both non-committed and committed Artifact rows, and
close additional deployment and upgrade rollback gaps. The current repository
`retry-budget-exhaustion.sh` has SHA-256
`b39652e15234f37cf9096f3a7268cfd1b2d830594b4ea4863d9eb9aefbdb132b`.
That hardened revision has not been rerun against the live lab. It must not be
presented as the harness that produced `retry-budget-exhaustion-v1`; the
retained receipt remains bound to the executed hash above.

Worker 2 still held Runner state for the successful replacement Attempt from
the preceding network-partition scenario even though its already-verified
output files had been cleaned. After the database reconfirmed its accepted
completion, posted Charge, two committed Artifacts, and zero active Lease, that
state was moved rather than deleted to
`/var/lib/vela-lab/mock-runner/retired-runner-state/2e9885d5-6d94-4301-9788-755d1a794f1e`.
The directory contains a `RETIREMENT_RECEIPT` and a self-checking
`SHA256SUMS` manifest.

Postflight again found zero active Leases, zero active Jobs, zero Production
Gate receipts, exactly two `READY/HEALTHY` Workers, both Runners
`running/healthy` in `success` mode with one main process, and zero GPU compute
processes. The result advances only the non-production fixed-scenario matrix to
`2/10`; it does not pass the `state-event-fault-injection` Production Gate.

## Process-kill rehearsal

On 2026-08-30, the third live fixed-scenario rehearsal started a non-terminating
Attempt on Worker 1, then sent `SIGKILL` to the exact Runner main process. The
fault path read the preinstalled container identity, mounted only that
container's Docker metadata directory read-only, and revalidated the Docker
state, container ID, runtime UID, PID, process start ticks, cgroup identity, and
pidfd immediately before signaling. It used `pidfd_send_signal`; the Docker
container ID remained unchanged while PID and `StartedAt` changed and
`RestartCount` advanced to `4`.

The final fault Pod retained `RuntimeDefault` seccomp,
`allowPrivilegeEscalation=false`, a read-only root filesystem, and only
`CAP_KILL`. Its container-level AppArmor profile alone was set to `Unconfined`.
This narrow exception was required because Ubuntu denied signals from the
RKE2 sender profile `cri-containerd.apparmor.d` to the Docker receiver profile
`docker-default`; the host-wide AppArmor policy and every non-fault Pod remain
unchanged. Before the destructive attempt, an otherwise identical Pod proved
the permission path with non-destructive `os.kill(pid, 0)`. Every fault-Pod
deletion path used the observed Kubernetes Pod UID as a server-side deletion
precondition.

The successful application Job was
`36cdaaf0-c8a1-4c71-93eb-4a89bfb55c0b`. Attempt 1 on Worker 1 ended `LOST`
with `failure_class=WORKER_LOST`; Attempt 2 on Worker 2 used a higher fence and
ended `SUCCEEDED`. Durable state contains exactly one Visible Completion, one
posted completion Charge, two Artifact rows, and two committed Artifacts, with
zero active Leases:

| Check | Result |
| --- | --- |
| Runner fault identity | container `efa8e7ffb1efd5b59adc4cada916f6d715a27ea32f05b1e373dbbea8ca1c3cbc`; PID `732569` -> `742572`; `StartedAt` `2026-08-30T03:41:04.950724625Z` -> `2026-08-30T03:51:30.421984086Z`; `RestartCount=4` |
| Original Attempt | Worker 1, fence 1, `LOST`, `WORKER_LOST` |
| Replacement Attempt | Worker 2, fence 3, `SUCCEEDED` |
| Durable result | one Visible Completion, one posted completion Charge, two committed Artifacts |
| Fixed measurements | lost accepted Jobs 0; duplicate completions 0; duplicate Charges 0; stale-authority acceptances 0 |
| Fixed scenario matrix | `3/10` lab rehearsals complete |
| Production Gates | `0/9 PASS` |

The root-only successful receipt is retained at
`/root/vela-lab-deploy-bc590e20/receipts/process-kill-v1`. Its mode is `0700`,
it contains no symlink, all files pass `sha256sum --check --strict`, and the
`SHA256SUMS` file has SHA-256
`9d1ca447fc0c37c16811fc7baf63051d98ea7914f400e01db9b16af25e553ec0`.
The executed harness has SHA-256
`cc6a79dad257ad51933cc31b0f664977f4f22d56beacc2b6ead3b9e2f5ec7d80`.
SSH transferred that 55,962-byte shell harness, not a Runner image or OCI
layer; the immutable Runner image remained in the LAN Registry.

Five hidden failed-run receipts remain as diagnostic evidence. They record the
initial AppArmor denial, fault-permission probing, cleanup result propagation,
guarded two-Worker recovery, and the exact authority-query corrections. None is
counted as a scenario pass. Independent postflight found all three RKE2 Nodes
`Ready`, no active fault/control Pod, watchdog, or Lease, both Runners healthy,
Worker 2 still at `RestartCount=0`, and zero GPU compute processes on all three
hosts. This advances only the non-production fixed-scenario matrix to `3/10`;
Production Gates remain `0/9 PASS`.

## Outbox post-commit control-crash rehearsal

On 2026-08-30, the fourth live fixed-scenario rehearsal delayed only the Outbox
Publisher loop to one minute, submitted one Job, and observed its durable
`job.ready` event after the Admission transaction committed but before any
Publisher claim or broker receipt. The accepted boundary was
`RUNNING|1 Attempt|1 active Lease|job.ready|0 publish attempts|unpublished|unclaimed|no broker receipt`.
The local Scheduler wakeup may start the first Attempt before `job.ready` is
published; that does not weaken the Outbox boundary and the harness still
requires exactly one final Attempt.

The fault Pod targeted the exact control Pod, container, host PID, process start
ticks, and cgroup. It used a pidfd to send `SIGKILL`, retained
`RuntimeDefault` seccomp, no privilege escalation, a read-only root filesystem,
and only `CAP_KILL`. The signal was sent at
`2026-08-30T05:25:49.882699Z`. After the container restarted, the Publisher
published `job.ready` at `2026-08-30T05:25:52.270536+00:00` with exactly one
publish attempt and Broker stream `VELA_EVENTS`.

Application Job `1c36decc-98a4-43c3-a92c-05c653650524` ended `SUCCEEDED` with
one Attempt, one Visible Completion, one posted completion Charge, two Artifact
rows, and two committed Artifacts. The four fixed measurements remained zero,
and final state contained no active Lease or Production Gate receipt. Cleanup
removed the temporary `VELA_PUBLISHER_TICK` ConfigMap entry, reloaded the
control container on its default `500ms` Publisher interval, and removed the
fault and image-warm Pods and watchdog state. Both Runners remained healthy;
Worker 1 retained `RestartCount=4`, Worker 2 retained `RestartCount=0`, and all
16 Worker GPUs reported zero memory use, zero utilization, and no compute
process.

The successful root-only receipt is
`/root/vela-lab-deploy-bc590e20/receipts/outbox-post-commit-crash-v2`. Its
`SHA256SUMS` file has SHA-256
`674eaf8ac922f8fe4a9740435fbe3c08daa42a9ed88d5a519fee9c8703beb812`,
and the executed harness has SHA-256
`70dab9231d7f75f49abfc531dfd028a2316ae462ee6dcdb1af865980898034ef`.
Review moved watchdog arming ahead of every reversible config mutation and made
the ten scenario IDs an exact-set check before this v2 rerun. The earlier v1
success receipt remains preserved but is superseded by v2. Two hidden
failed-run directories remain diagnostic evidence only. The first stopped on a
malformed readiness `jq` expression before any config mutation, Job creation,
or signal. The second exposed the incorrect assumption that an unpublished
Outbox event prevents local scheduling; it sent no signal and cleanup restored
the default config. Neither is counted as a pass.

This advances only the non-production fixed-scenario matrix to `4/10`;
Production Gates remain `0/9 PASS`, and six fixed scenarios remain unexecuted.
The exact nine-gate status and missing external evidence are tracked in
[`production-gate-gap-matrix.md`](production-gate-gap-matrix.md). Every gate
remains `NOT PASS`.

## Publisher post-PubAck/pre-marker control-crash rehearsal

On 2026-08-30, the fifth live fixed-scenario rehearsal deployed the tagged v19
control image and set
`VELA_LAB_OUTBOX_FAULT_PHASE=publisher-post-puback-pre-mark-crash`. The tagged
Publisher delegated one `job.ready` publication to NATS, atomically wrote a
private payload-free marker containing the PubAck, and paused before returning
the receipt to the database-backed Publisher. The harness validated that exact
boundary and sent `SIGKILL` through a pidfd to the exact control process.

After the 30-second claim TTL, the restarted Publisher reclaimed the same
Outbox event and converged the PostgreSQL marker to the original NATS receipt.
Application Job `af71a549-36be-49b3-aaba-e7c299245f92` ended `SUCCEEDED` with
one Attempt, one Visible Completion, one posted completion Charge, two Artifact
rows, and two committed Artifacts. Outbox event
`06d54b4e-9d90-469d-ae82-298682be4a79` retained Broker stream `VELA_EVENTS` and
sequence `268`; its `publish_attempts` advanced from `1` before the crash to `2`
after recovery. This is at-least-once Publisher execution with one visible
business result, not a Production Gate receipt.

The successful hardened root-only receipt is
`/root/vela-lab-deploy-bc590e20/receipts/publisher-post-puback-pre-mark-crash-v2`.
Its `SHA256SUMS` file has SHA-256
`d73cdce02500580a4f1b5961844e7808f5f19f3cb4bc0bb9be25d236a9165bfb`,
and the executed harness has SHA-256
`cc37ee0df5e813e0929f4ea083782d785153b846bd81040d70802f397065f0a0`.
It forces every interrupted uncommitted run to exit nonzero and permits cleanup
to disarm the watchdog only after a UID-bound control Pod replacement proves
the fault environment and marker are absent. The earlier v1 success receipt is
preserved but superseded by this v2 rerun.

The first diagnostic run checked PostgreSQL too early, before the claim TTL
expired, and correctly refused to emit a passing receipt. Its hidden directory
`receipts/.vela-lab-publisher-post-puback-pre-mark-crash.sB7skT` remains
diagnostic evidence only; the same PubAck sequence `256` later converged. The
harness now waits up to 90 seconds for marker convergence and performs a
stronger Outbox drain during cleanup.

Postflight found zero active Jobs, unrevoked Leases, pending Outbox events, and
Production Gate receipts; exactly two Workers remained `READY/HEALTHY`, the
single control Pod was Ready, and no fault Pod or watchdog remained. Both mock
Runners were healthy, all 16 Worker GPUs reported zero memory use and zero
utilization, and the control host had no GPU compute process. The fault
variable and temporary Publisher tick were absent from the ConfigMap. This
advances only the non-production fixed-scenario matrix to `5/10`; Production
Gates remain `0/9 PASS`, and five fixed scenarios remain unexecuted.

## Publisher pre-PubAck control-crash rehearsal

On 2026-08-30, the sixth live fixed-scenario rehearsal set
`VELA_LAB_OUTBOX_FAULT_PHASE=publisher-pre-puback-crash`. The tagged Publisher
claimed one `job.ready` event in PostgreSQL, wrote a private payload-free marker
that required empty Broker stream and sequence zero, and paused before
delegating any publish to NATS. The harness captured the NATS leader before the
Job and before the signal, requiring all three replicas to be current and the
duplicate window to remain `600000000000ns`.

The Scheduler was allowed to advance the Job before the crash; the fault
boundary was the exact committed PostgreSQL claim plus absence of a Broker
receipt and unchanged NATS leader sequence. The harness sent `SIGKILL` through
a pidfd to the exact control process nine seconds after the database start,
within the 90-second signal bound and 120-second hook timeout. Before the crash,
the leader `last_seq` remained `279`. Recovery reclaimed the same Outbox event,
advanced `publish_attempts` from `1` to `2`, and recorded `VELA_EVENTS`
sequence `285`, strictly after the pre-crash sequence.

Application Job `86a1e970-0847-4f8a-b9c1-4577ec2d6915` ended `SUCCEEDED` with
one Attempt, one Visible Completion, one posted completion Charge, two Artifact
rows, and two committed Artifacts. Outbox event
`bb4ed838-7a6c-4f29-816b-b0186341b949` retained the recovered Broker receipt.
The lost-accepted Job, duplicate completion, duplicate Charge, and stale
authority acceptance measurements were all zero.

The successful root-only receipt is
`/root/vela-lab-deploy-bc590e20/receipts/publisher-pre-puback-crash-v1`. Its
`SHA256SUMS` file has SHA-256
`a05cc044f7b2536cc58604aed95fadc5723806aeb814319d4058c3f4a210c3d9`,
and the executed harness has SHA-256
`6483f806b62c747110ff9a159d6e8bbba40a98efe46e599765a336874a21ed88`.
Both this manifest and the hardened fifth-scenario v2 manifest pass
`sha256sum --check --strict` from their receipt directories.

Two hidden failed-run directories remain diagnostic evidence only:
`receipts/.vela-lab-publisher-pre-puback-crash.bOt1sb` records a POSIX shell
variable collision that truncated captured NATS JSON, while
`receipts/.vela-lab-publisher-pre-puback-crash.Oh8ucl` records the invalid
assumption that the Job must remain `QUEUED` before the signal. Both stopped
before `SIGKILL` and are not counted as passes.

Postflight found zero active Jobs, unrevoked Leases, pending Outbox events, and
Production Gate receipts; two Workers remained `READY/HEALTHY`; and replacement
control Pod `vela-lab-control-556bfdc76-ltjf9` was Ready with zero restarts. No
fault ConfigMap value, marker, fault Pod, or watchdog remained. Both Runners
were healthy, all 16 Worker GPUs reported `0 MiB / 0%` with no compute process,
and the control host had no compute process. Four temporary staging harnesses
under `/home/marslab` were removed; the root-only harness and receipts remain.
This advances only the non-production fixed-scenario matrix to `6/10`;
Production Gates remain `0/9 PASS`. The four remaining fixed scenarios are
`node-reboot`, `consumer-post-db-pre-ack-crash`,
`assignment-post-commit-pre-response-crash`, and
`stale-fence-late-completion`.

## Consumer post-DB/pre-Ack control-crash rehearsal

On 2026-08-30, the seventh live fixed-scenario rehearsal deployed the tagged
v20 control image and set
`VELA_LAB_CONSUMER_FAULT_PHASE=consumer-post-db-pre-ack-crash`. The Scheduler
Consumer wrote a private marker only after the `job.ready` handler and its
separate Inbox receipt transaction committed, then paused before `DoubleAck`.
The Scheduler state transaction and Inbox receipt transaction were not merged.
The harness proved one Inbox receipt, one Attempt, `num_ack_pending=1`, and an
AckFloor behind the target stream sequence before sending `SIGKILL` through a
pidfd to the exact control process.

The harness samples stream Raft state from the stream leader and Consumer Raft
state from the Consumer leader. Its first hidden diagnostic run stopped before
creating an application Job or sending `SIGKILL` because the stream leader's
nested Consumer monitor view was stale. The corrected harness retained the same
strict three-current-replica requirement and used the Consumer leader as the
authority. The diagnostic directory
`receipts/.vela-lab-consumer-post-db-pre-ack-crash.1AF9aE` remains preserved and
is not counted as a pass.

Application Job `0831b136-2639-4139-ac1d-d6af9186b09c` ended `SUCCEEDED` with
one Attempt, one Visible Completion, one posted completion Charge, two Artifact
rows, and two committed Artifacts. Event
`fa809191-d2e3-421f-a3d5-c4a30574fe2a` retained stream sequence `286` while its
Consumer sequence advanced from `48` to `49`. The observed delivery count is
bounded below by two; `num_ack_pending` changed from `1` to `0`; the AckFloor
stream sequence advanced from `285` to `291`; and the Inbox receipt and Attempt
counts remained one with `handler_reapply_count=0`. All four fixed measurements
remained zero.

The successful root-only receipt is
`/root/vela-lab-deploy-bc590e20/receipts/consumer-post-db-pre-ack-crash-v1`.
Its files pass `sha256sum --check --strict`; its `SHA256SUMS` file has SHA-256
`817edbf165a151d8a2552aadbfcef907a4651484d720cade36bae59a63f873fe`,
and the executed harness has SHA-256
`75331cb29a07a89c3d69c6a166e81772ea36ce8aef23afa84a93fa1687d9a0e8`.
Postflight found zero active Jobs, unrevoked Leases, pending Outbox events, and
Production Gate receipts; exactly two Workers remained `READY/HEALTHY`; and all
three nodes were Ready with zero allocatable control-node GPUs. No fault
ConfigMap value, marker, fault Pod, warm Pod, watchdog, or NATS port-forward
remained. This advances only the non-production fixed-scenario matrix to
`7/10`; Production Gates remain `0/9 PASS`. The three remaining fixed scenarios
are `node-reboot`, `assignment-post-commit-pre-response-crash`, and
`stale-fence-late-completion`.

## Node-reboot rehearsal

On 2026-08-30, the eighth live fixed-scenario rehearsal kept
`marslab-server` as the uninterrupted control/Registry node and rebooted only
`vela-lab-worker-1`. The root harness pinned the passing `7/10` receipt, required
an idle authority boundary and two `READY/HEALTHY` Workers, drained Worker 2
until Attempt 1 was `RUNNING` on Worker 1, then persisted an action intent that
bound the node UID, InternalIP, boot ID, Job, Attempt, and fence. The separate
operator terminal executed one reboot only after every field matched the
`action_required=NODE_REBOOT` line.

The first run failed closed after the reboot because kubelet returned
`Ready=True` while `nvidia.com/gpu` was still zero; the device plugin restored
eight allocatable GPUs shortly afterward. The diagnostic directory
`receipts/.vela-lab-node-reboot.ygpybI` is preserved and is not counted. Its
cleanup observed the changed boot ID, allowed the replacement Attempt to
finish, and restored zero active Jobs and Leases plus two `READY/HEALTHY`
Workers. The harness was then hardened to require the same node identity, a new
boot ID, eight allocatable GPUs, and five consecutive stable observations
before continuing.

The passing run used Job `827f5a1b-7f85-43d3-b236-adf2ecdae1d1`. Worker 1
kept Kubernetes node UID `4068931d-46f8-48b0-ba03-fd6135ea64cd` and managed
Runner container ID
`efa8e7ffb1efd5b59adc4cada916f6d715a27ea32f05b1e373dbbea8ca1c3cbc`;
its boot ID changed from `45d53683-08e0-4a0b-845b-ddb5f8acafde` to
`1db56fa8-5375-4250-aaf5-0747cd550f01`, with `Ready=Unknown` observed during
the outage. Attempt 1
`bde490b4-18d3-4dec-8e45-5e93e600890f` became `LOST` with `WORKER_LOST` at
fence 1. Attempt 2 `bd500ca8-94cd-42b3-9754-2d3be28fe60b` succeeded on Worker
2 at fence 3. The Job retained one Visible Completion, one posted completion
Charge, two committed Artifacts, and four zero-valued fixed measurements.

The successful root-only receipt is
`/root/vela-lab-deploy-bc590e20/receipts/node-reboot-v1`. Every file passes
`sha256sum --check --strict`; the `SHA256SUMS` file has SHA-256
`e6decc92d15d6bf8933c922ab8f9550ec76129e3a74682c757a4ead01aa69c20`,
and the executed harness has SHA-256
`cf0633080aacfedbf543290b611f354967b6ef8ad8a0991aa25d5d8520768a84`.
Postflight found all three nodes Ready, control allocatable GPUs zero, both
Workers at eight GPUs with healthy persistent Runners and no GPU compute
process, zero active Jobs, unrevoked Leases, pending Outbox events, or
Production Gate receipts, and no live harness/watchdog process. This advances
only the non-production matrix to `8/10`; Production Gates remain `0/9 PASS`.
At this `8/10` checkpoint, the two remaining fixed scenarios were
`assignment-post-commit-pre-response-crash` and
`stale-fence-late-completion`; both are recorded below.

## Assignment post-commit/pre-response control-crash rehearsal

On 2026-08-30, the ninth live fixed-scenario rehearsal paused the Scheduler only
after PostgreSQL committed one Assignment, Attempt, Lease, and dispatch intent,
but before the assignment response returned to the dispatch loop. The harness
then killed the exact control process through a pidfd. The durable pre-crash
marker bound Attempt `68ee0635-e039-4879-b9b1-7e1a7fa14682` on Worker 1 at
epoch 1 and fence 1 to dispatch intent
`226aa342-3896-4ad8-81ad-c2b6852a66c9`, with the Job and Attempt both
`ASSIGNED` and the dispatch intent `COMMITTED`.

After restart, recovery replayed that committed authority instead of creating a
second Attempt. The Attempt count remained one before and after recovery, and
Job `16a9a421-4fce-4416-860e-49e3fa5e3d34` ended `SUCCEEDED` with one Visible
Completion, one posted completion Charge, and two committed Artifacts. The lost
accepted Job, duplicate Attempt, duplicate completion, duplicate Charge, and
stale-authority acceptance measurements were all zero.

The successful root-only receipt is
`/root/vela-lab-deploy-bc590e20/receipts/assignment-post-commit-pre-response-crash-v1`.
Every file passes `sha256sum --check --strict`; the `SHA256SUMS` file has
SHA-256 `f0c1a320b396579359921a51f0a1d9d97d6f305dc331d1bed288767818bf6b30`,
and the executed harness has SHA-256
`333cb27a399fc03da909a187ae1690b06a50292e8e9fbc317533fc6a1ef1f393`.
This advances only the non-production matrix to `9/10`; Production Gates remain
`0/9 PASS`.

## Stale-fence late-completion rehearsal

On 2026-08-30, the tenth live fixed-scenario rehearsal captured the exact
FINALIZATION Completion Candidate for Attempt 1 on Worker 1, then isolated its
control path while the control plane advanced authority. The original Attempt
`937de460-d7b3-4336-9ec2-d4beb2ee8f17` failed at fence 1 with
`FINALIZATION_TIMEOUT`; its durable RetryDecision recorded Job fence 2 and
`RETRY_WAIT`. Replacement Attempt
`e4c6bf87-a43c-46e2-85b6-bd5d43374334` ran on Worker 2 at fence 3 and ended
`SUCCEEDED`. Replaying the exact old candidate over Worker 1's mTLS identity
returned `REJECTED_STALE_LEASE`.

Job `3e4be0cc-fcc0-42fa-a502-6080df76c634` retained exactly one Visible
Completion, one posted completion Charge, one winning ArtifactSet with two
items, and two committed replacement Artifacts. The two original-fence
Artifacts remained `VERIFIED` and were not published. All four fixed
measurements remained zero. The ten-entry scenario matrix now contains ten
`LAB_REHEARSAL_PASS` rows, while its evidence boundary remains
`NON_PRODUCTION_MOCK_REHEARSAL` and Production Gates remain `0/9`.

The run used Worker Agent image
`10.1.200.17:5443/vela-lab/vela-worker-agent@sha256:88dfa73690c3bd78b7d96eae60a4daa1cf1e942497ce276829797e10ad4ee897`
on both Workers and bootstrap/tool image
`10.1.200.17:5443/vela-lab/vela-lab-bootstrap@sha256:93a5be4c1ba6a8b81e3d8367672a6963d5a5b5d1d1c04d6e050efbfd2f93aa42`.
The Worker Agent binary SHA-256 is
`e377e225ae5b7edcd50e91704b9492c442b6a70cab81614a5075b8f77f060b0b`;
the smoke binary SHA-256 is
`3addccdadc0845fcc0f704355fdf43e9ed638bd9f958462cefde91f810cd8392`.

The successful root-only receipt is
`/root/vela-lab-deploy-bc590e20/receipts/stale-fence-late-completion-v2`.
Every file passes `sha256sum --check --strict`; the `SHA256SUMS` file has
SHA-256 `e2bc7e8af1bdfb43ba546d5ef89f9737ff961e812b1be6ddb570f5cc3f18c4cd`,
and the executed harness has SHA-256
`2d6f642a20758ac47ee5e397ef9fcb2191dbb853edb90defc0e8f07cb3022909`.
Two hidden failed runs remain diagnostic only:
`receipts/.vela-lab-stale-completion.wRJ7jq` records the overwritten candidate
diagnosis, and `receipts/.vela-lab-stale-completion.UrCWCX` records the smoke
client's fail-fast behavior during an intentional control restart. Neither is
counted as a pass.

Independent live postflight then verified zero active Jobs, zero active Leases,
zero Production Gate receipts, and exactly two `READY/HEALTHY` Workers. All
three Kubernetes nodes were Ready with allocatable GPUs `0/8/8`; the temporary
probe, canary, ConfigMap, and NetworkPolicy resources were absent; the control
fault variables were absent; the application egress policy was at its baseline;
and the finalization retry ServiceClass was restored to `DRAINING` while the
base ServiceClass was `ACTIVE`. Both Worker Agent Deployment generations were
observed and both runtime `imageID` values matched the fixed digest. Both
persistent Runners were `running/healthy` in `success` mode with the fixed
Runner digest, and no harness or watchdog process remained. The receipt ledger
in `authority-after.json` and its checksum manifest both passed independent
structural validation.

## Organization isolation and content-safety rehearsal

On 2026-08-30, the live non-production isolation harness exercised seven of the
nine fixed `organization-isolation-content-safety` scenarios. It passed
cross-Organization and cross-Project invisibility against two actual synthetic
foreign-scope Jobs, four Artifact rows, two ArtifactSets, and two access grants.
The fixed role evidence now binds all 30 runtime database URLs in the live
control Deployment Secret to 30 distinct login roles and their exact singleton
group memberships. It records all 60 login/group role attributes, rejects
direct login grants, verifies the exact table and routine privileges of the
three public HTTP roles, and observes live connections for their restricted
pools. A catalog inventory independently reports all 72 physical public tables
with an `organization_id` column and proves `relrowsecurity=true` and
`relforcerowsecurity=true` for every one. Private request context, credential
revocation, composite foreign-key rejection, and exact-version signed URL scope
also passed. The HTTP probe read both committed Artifacts for Job
`3e4be0cc-fcc0-42fa-a502-6080df76c634`, verified their sizes and SHA-256
digests, and rejected method, path, and syntactically valid version tampering.
The executed ledgers derive ten database and ten HTTP negative probes, with
zero unexpected allows and zero credential-revocation bypasses.

The two unexecuted scenarios remain explicit `NOT_RUN` entries:
`break-glass-audit` requires the absent real Platform IdP and approval workflow,
and `customer-content-no-reuse` requires an independent audit sink. Their
measurements are `NOT_MEASURED`, not zero. The result is therefore
`LAB_REHEARSAL_PARTIAL`, scenarios `7/9`, evidence boundary
`NON_PRODUCTION_MOCK_REHEARSAL`, and Production Gates `0/9`.

The root-only receipt is
`/root/vela-lab-deploy-bc590e20/receipts/organization-isolation-content-safety-v2`.
All 28 files listed by its manifest pass `sha256sum --check --strict`; the
receipt contains 29 files including `SHA256SUMS`, whose SHA-256 is
`ba0e6784ce2bcc9d477bc3737ef3479a78043bfaff0e90f799d2e9835f812943`.
The executed harness has SHA-256
`69006f29401a3fbc64f3e88b9448fe3b11bde76c77e9b7bae1435304c6822a5e`,
the executed Python probe has SHA-256
`3f812ca892226c7254f7c5d1d6217190df8c8c0af9ed1d85441e02597c925ccc`,
and the Pod runtime used the pinned Runner digest
`sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3`.
The v1 and v2 receipts are retained as historical evidence; the reviewed v3
receipt below supersedes them for the current mock rehearsal.

Independent postflight matched the complete before/after database snapshot:
zero active Jobs, zero active Leases, zero Production Gate receipts, zero active
Break-glass grants, two `READY/HEALTHY` Workers, zero fixture rows, and an
unchanged actor-session-attribution count of two. The temporary Pod and
immutable ConfigMap were absent and no harness process remained. Four hidden
failed runs are retained as diagnostics only: they found a missing Worker taint
toleration, a committed synthetic-Credential audit side effect, invalid-format
version tampering rejected before signature verification, and a receipt-wrapper
`jq` projection error. Each failed closed, cleaned its owned Kubernetes
resources and removable fixture rows, and is not counted as a pass. The
diagnostic actor-session attribution was not deleted; it is retained as part of
the before/after count of two. The successful transaction-scoped Credential
probe added no further attribution.

The v2 run additionally retained
`receipts/.organization-isolation-content-safety.p1N6QF`: its validator rejected
an invalid local `jq` expression before database fixture creation. It contains
no PASS receipt and is not counted as evidence. The corrected rerun returned the
database to the same boundary, removed its temporary Pod and ConfigMap, and a
terminal observation at `2026-08-30T22:43:09Z` was recorded as failing only on
the 22 separately retained historical failed Pods. That observation did not
retain the raw kubelet `configz` files and is not current swap-policy evidence;
the retained postflight below supersedes it for current cluster state.

Post-review, the v2 `7/9` value above is retained only as the result emitted by
those historical bytes. The configured database URLs, exact role memberships,
and connected restricted pools do not request-correlate each public HTTP path
to its database role, so the fixed-role scenario is no longer counted as a
complete rehearsal PASS.

The reviewed v3 rerun records that limitation explicitly and reports
`LAB_REHEARSAL_PARTIAL`, scenarios `6/9`, evidence boundary
`NON_PRODUCTION_MOCK_REHEARSAL`, and Production Gates `0/9`. Its fixed-role
scenario is `LAB_REHEARSAL_PARTIAL_REQUEST_CORRELATION_ABSENT`; Break-glass and
content reuse remain `NOT_RUN`/`NOT_MEASURED`. The other six scenarios passed,
with ten database and ten HTTP negative probes, zero unexpected allows, and
zero credential-revocation bypasses.

The v3 role snapshot binds all 30 configured database URLs to exact singleton
login/group pairs and records 60 roles. It compares direct and effective
privileges rather than only ACL rows: table sets are `22/22`, explicit
column-only sets are `6/6`, and routine sets are `13/13`. Direct login grants,
sequence privileges, unsafe schema privileges, and effective expansion through
`PUBLIC` or inherited roles are absent. The forced-RLS inventory contains all
72 physical public relations carrying `organization_id`, with zero unprotected
relations. The persisted synthetic foreign-resource fixture again contained
two Jobs, four Artifacts, two ArtifactSets, and two access grants.

The root-only receipt is
`/root/vela-lab-deploy-bc590e20/receipts/organization-isolation-content-safety-v3`.
All 28 manifest files pass `sha256sum --check --strict`; the receipt contains 29
files including `SHA256SUMS`, whose SHA-256 is
`5277940ff6fb2f9b82bfa4383014a2823671a13fb4c3653dde46d4b410318b68`.
The executed harness SHA-256 is
`856f759d50c4164735b34cb09edbdf4b9be6cc5cd3ebb66df3dc71fc24774d7d`,
the executed Python probe SHA-256 is
`3025380dffe056f100f4673d4830d5f4924e65f64556e056513edc7b652b16c0`,
and the Pod runtime retained the pinned Runner digest
`sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3`.

Independent postflight matched the receipt's exact before/after database
snapshot and a fresh live boundary: zero active Jobs, Leases, Production Gate
receipts, active Break-glass grants, and fixture rows; two `READY/HEALTHY`
Workers; and two unchanged actor-session attributions. The temporary Pod and
ConfigMap and the harness process were absent. The strict cluster verifier at
`2026-08-30T23:15:27Z` was recorded in terminal output as `FAIL failures=1`
because 21 retained historical failed Pods remained. It did not retain the raw
kubelet `configz` responses and is not used as current evidence for explicit
`NoSwap`. No historical Pod or diagnostic receipt was deleted by the v3 run.

Commit `b5752572fc3e9fe733daaa3aa32831a2253a7a1b` added request-correlated
database-role observations for service authentication, Job submit/read, and
Artifact read. The control image
`10.1.200.17:5443/vela-lab/vela-control@sha256:4870b579c499c5d07b53a1442bb43083dff9ce2c178e610b80a932155c6adbab`
was rolled out from the rollback digest
`sha256:257fefb1207a19e7023171d2cc73773fa2d0e4e03d30f337abeda70c78bc5985`.
The new control Pod UID is `619cf099-2b51-45bc-9e6f-932e046ae6d3`, with the
new digest as its runtime image ID and zero restarts. All three nodes and both
Worker Agent Deployments were Ready, while the database remained at zero active
Jobs, zero active Leases, zero Production Gate receipts, zero active
Break-glass grants, and two `READY/HEALTHY` Workers. The root-only rollout
receipt is
`/root/vela-lab-deploy-bc590e20/receipts/control-request-role-rollout-v1`.
All files in its manifest pass `sha256sum --check --strict`; the SHA-256 of its
`SHA256SUMS` file is
`ebc21396b34b6a9a67430c2c31857bfad1b826b53fb1403b6094f5901b284cd9`.
Its two-minute control-log postflight contained 240 expected retries against
the unavailable non-production Invoice sink, with zero fatal or panic lines.

The v4 organization-isolation rerun used harness SHA-256
`4d2ca2dfe807984a3aeaead365d3e717d6a6296469b9a03ca489f7fcf6bed1b9`,
probe SHA-256
`7b86d79b2f87428bac63e95e2f28fe37feb6e7505679720019c32e6855bf45ac`,
and the unchanged pinned Runner digest
`sha256:71af1330eefdfff2a33d68e5f8c53c66ebe5b402dc28c35b3ff7516357ec4ca3`.
It remains `LAB_REHEARSAL_PARTIAL`, scenarios `6/9`, evidence boundary
`NON_PRODUCTION_MOCK_REHEARSAL`, and Production Gates `0/9`. The six API
requests carried six unique server-generated request IDs and produced eight
exact correlated observations: six `vela_auth_login -> vela_auth` service
authentication observations, one `vela_request_login -> vela_request` Job-read
observation, and one
`vela_artifact_request_login -> vela_artifact_request` Artifact-read
observation. The fixed-role scenario advances from telemetry absent to
`LAB_REHEARSAL_PARTIAL_PUBLIC_PATH_INVENTORY_INCOMPLETE`; it is not a PASS
because the complete public HTTP path inventory was not exercised. The real
Customer and Platform IdPs, Break-glass workflow, and customer-content reuse
audit sink also remain absent.

The root-only v4 receipt is
`/root/vela-lab-deploy-bc590e20/receipts/organization-isolation-content-safety-v4`.
All files in its manifest pass `sha256sum --check --strict`; the SHA-256 of its
`SHA256SUMS` file is
`5efb9895f4fda791088076a82b1bbc229f44b0d42bdcd32f3be2d40738cce460`.
Its before/after database snapshots have the same SHA-256
`5c10d1d50a3a3335f761b8ed7cd0764737d0582c6680f182f788223ea9847039`.
The temporary Pod and ConfigMap are absent, and no Launch Receipt or database
Production Gate receipt was created.

The independent strict postflight is retained at
`/root/vela-lab-deploy-bc590e20/receipts/cluster-strict-postflight-request-role-v1`.
Its `SHA256SUMS` file has SHA-256
`1fa3337cf10e26e22fc50304291c10602f93c73c1395a13b1ed92f6d87d2b1e4`.
The current result is `LAB_VERIFICATION_PARTIAL`, `FAIL failures=4`: explicit
`memorySwap.swapBehavior=NoSwap` is missing from all three raw `configz`
responses, and 17 exact historical Failed Pods remain. No cluster mutation or
historical cleanup was performed by this postflight.

Repository HEAD now contains a guarded
`deploy/lab/rke2-airgap/configure-kubelet-noswap.sh` candidate and a ten-case
CLI/stub test covering missing confirmations, first apply, idempotent recovery,
wrong host identity, a symlink target, a concurrent publication collision, an
inactive service, an unhealthy post-restart service, and an unexpected
post-restart evidence-capture failure. The helper is not deployed and has not
restarted any lab node. A fresh read-only node inventory confirmed that all
three live drop-in
directories are `root:root 0700`, contain only a `root:root 0600`
`00-rke2-defaults.conf`, and have the expected active/enabled role-matched RKE2
service. The raw three-node `configz` boundary remains unchanged at
`memorySwap: {}`.

The retained remote verifier at
`/root/vela-rke2-ops/gpu-operator-v26.3.2/verify-cluster.sh` is not the strict
current repository version: it uses `.memorySwap.swapBehavior // "NoSwap"` and
therefore reported only the 17 historical Pods as one failure in a fresh
read-only run. That is a false default, not evidence that the live kubelets
have `NoSwap` configured. Before any approved rollout, stage and hash the
current fail-closed verifier, then apply Worker 1, Worker 2, and the shared
control host one at a time with node/GPU/workload recovery checks between
restarts. A separate restart approval is still required.

## Experiment operating rules

- Build the mock backend with the contract in
  [`0048-h3-mock-backend.md`](../specs/0048-h3-mock-backend.md). Do not put
  model weights, datasets, caches, or CUDA toolchains into the mock image.
- Build an OCI image on a Worker or another LAN-reachable build host, publish
  it once, record the returned digest, and deploy only by digest.
- Mount large model artifacts from host storage or an artifact store. Do not
  send multi-gigabyte OCI archives through the SSH administration path.
- Do not schedule Vela workloads, mock or real, on `marslab-server`.
- Do not treat the probe repository, mock Catalog records, or synthetic GPU
  role UUIDs as production or hardware-readiness evidence.
- Before changing RKE2, workload networking, or Registry credentials, perform a
  fresh preflight and record a separate approval and rollback boundary.
- Do not use GitHub's `latest` release as a substitute for the RKE2 stable
  channel. Fetch and verify only the versioned assets pinned in
  `deploy/lab/rke2-airgap/release.env`.

## Recovery and rollback

For a registry service restart, restart only the `vela-registry` container and
then repeat the unauthenticated/authenticated `/v2/` checks. Do not restart the
control host's Docker daemon while unrelated containers are running.

For credential rotation, generate a new root-only secret, atomically replace
the bcrypt entry in `/etc/vela-registry/auth/htpasswd`, verify hot reload with
`401` without credentials and `200` with the new credential, and then update
the separately approved pull-secret distribution. Restart Registry only if a
fresh probe proves hot reload did not occur.

A full rollback is destructive and requires a fresh exact-target approval:

1. stop and remove only the `vela-registry` container;
2. back up and then remove only `/srv/vela-registry` and
   `/etc/vela-registry`;
3. remove the two documented CA trust files from each Worker, run
   `update-ca-certificates`, and restart Docker only after confirming the
   Worker is idle; and
4. verify the Docker Hub mirror, NVIDIA runtime, unrelated containers, and GPU
   process baseline after rollback.
