# Three-host RKE2 air-gap staging

Status: applied three-node lab receipt. RKE2 `v1.35.7+rke2r1`, Canal, and GPU
Operator `v26.3.2` are running on the approved one-control/two-Worker topology.
Base and GPU postflight plus four sequential GPU smoke probes passed on
2026-08-29. The stricter current postflight additionally rejects an omitted
`memorySwap.swapBehavior` and the retained historical Failed Pods. Mutating
helpers still require the literal `--apply` argument; this lab result is not
production, HA, DR, or H3 certification evidence.

The lab candidate is the RKE2 stable-channel release observed on 2026-08-29:

```text
v1.35.7+rke2r1
tag commit: 382a8b31a8fd78e376ab6f02c4bb0ec5592aada2
CNI: Canal
system registry: 10.1.200.17:5443
```

The repository does not constrain an RKE2 or Kubernetes version. This candidate
therefore follows the official stable channel rather than GitHub's different
`latest` release line. `release.env` pins each required file's byte count and
SHA-256. The checksum manifest and image list are official release assets. The
installer is pinned to the same immutable RKE2 tag and independently hashed.

## Bandwidth and disk plan

The complete external transfer is 863,417,533 bytes (823.42 MiB):

| File | Bytes | Purpose |
| --- | ---: | --- |
| `rke2-images.linux-amd64.tar.zst` | 822,789,229 | Core, Canal, and bundled system images |
| `rke2.linux-amd64.tar.gz` | 40,598,468 | RKE2 binaries and service payload |
| checksums, image list, pinned installer | 29,836 | Verification and installation metadata |

The low-SSH-bandwidth path downloads one copy through an explicitly selected
HTTPS transport prefix and retains it in a versioned staging directory on
`marslab-server`. The proxy is untrusted transport: every file must still match
the pinned byte count and SHA-256, and the two archives must match the separately
pinned official checksum manifest. Do not copy the 784.7 MiB image archive to
every Worker. Load it once into Docker on `marslab-server`, tag the exact image inventory under
`10.1.200.17:5443`, and push each image to the private Registry. RKE2 nodes then
pull shared layers over `10.1.200.0/24`.

On 2026-08-29, `https://ghfast.top/` returned valid HTTPS `206` responses for
one-byte Range probes of the pinned image archive, checksum manifest, and
installer from `marslab-server`, but the full transfer there degraded to a few
KiB/s. The exact fetcher process was stopped and its partial was removed only
after the final bundle was verified. Worker 1 then fetched the bundle once
through `https://gh-proxy.com/`; the guarded fetcher verified all pinned sizes
and SHA-256 values before a temporary LAN-only HTTP transfer to
`marslab-server`. The control host verified both archives again, the temporary
Worker listener and all seven Worker staging files were removed, and no image
bytes traversed SSH. TLS verification was never disabled.

The resulting root-owned stage is
`/srv/vela-rke2-airgap/v1.35.7-rke2r1`: its two directories are mode `0700`,
its fetcher is mode `0700`, and `release.env` plus all five artifacts are mode
`0600`. There are no partial files or credentials in the stage. The 17 pinned
system images were published once to the private Registry, and the binary-only
bundle was installed on all three nodes; the large image archive was not copied
to either Worker.

The 38.7 MiB binary archive and verified installer can be served temporarily
from `10.1.200.17` to the two Workers after an install is approved. The
temporary server must bind only to the LAN address, contain no credentials,
and be stopped after all three nodes verify the files. This avoids three
administration-path copies without making the Workers stateful distribution
nodes.

The control host currently has about 562 GiB free on `/`. Importing the archive
will temporarily retain the archive, Docker source images, and Registry blobs.
No global Docker prune is allowed: the host contains unrelated experiment
images and containers. Any later cleanup must enumerate only the pinned RKE2
source tags after Registry pull and restart checks succeed.

All three `10.1.200.0/24` interfaces reported a currently negotiated link speed
of `100 Mbps` on 2026-08-29. At that line rate, the approximately 1.10 GiB base
RKE2 plus GPU bundle has a theoretical per-node transfer floor of about 95
seconds before protocol, Registry, decompression, and disk overhead. Pull and
verify one Worker at a time and budget roughly two to three minutes per Worker;
parallel first pulls would only divide the control host's same 100 Mbps LAN
link. Layer reuse makes later restarts and incremental publications smaller.

## Safe local preparation

Metadata-only mode downloads about 30 KiB and is the default:

```text
HTTPS_PROXY=http://127.0.0.1:7897 \
  ./deploy/lab/rke2-airgap/fetch-artifacts.sh \
  /path/to/rke2-artifacts --metadata-only
```

The full download requires the explicit `--all` mode. It supports HTTP resume,
fails closed on any size or digest mismatch, and never replaces an existing
invalid final file:

```text
HTTPS_PROXY=http://127.0.0.1:7897 \
  ./deploy/lab/rke2-airgap/fetch-artifacts.sh \
  /path/to/rke2-artifacts --all
```

Do not place the bundle in the Git worktree or Go caches.

For the preferred direct-to-marslab path, stage this directory's fetcher and
`release.env` on the control host, then set the transport prefix explicitly:

```text
RKE2_DOWNLOAD_PREFIX=https://ghfast.top/ \
  ./fetch-artifacts.sh /srv/vela-rke2-airgap/v1.35.7-rke2r1 --all
```

`RKE2_DOWNLOAD_PREFIX` must be empty or an HTTPS URL ending in `/`, and it must
not contain credentials, a query, or a fragment. The fetcher prints the chosen
transport in its terminal receipt. The host's reachable `docker.1ms.run`
endpoint is not a substitute for this release bundle: its manifest service did
not resolve the pinned RKE2 runtime tag during the same preflight.

The fetcher applies owner-only mode `0700` to its artifact directory and `0600`
to every verified final or partial file. It rejects symlink inputs. A resumed
or pre-existing file is trusted only after its pinned byte count and SHA-256
both match.

## Guarded execution helpers

The helpers separate download, Registry publication, before-state capture, and
binary installation so that no step implicitly starts RKE2 or Canal:

| Helper | Allowed write | Explicit non-action |
| --- | --- | --- |
| `fetch-artifacts.sh` | One selected artifact directory | Does not import or publish images |
| `publish-rke2-images.sh` | One root-only publication state file, Docker source/target tags, and the existing private Registry | Does not prune, restart Docker/Registry, install RKE2, or copy the archive to Workers |
| `capture-node-state.sh` | One new root-owned `0700` receipt directory | Does not change host, Docker, GPU, CNI, firewall, or RKE2 state |
| `install-node.sh` | `/usr/local` RKE2 payload and root-only `/etc/rancher/rke2` inputs | Does not copy an image archive, enable, or start the RKE2 service |
| `configure-kubelet-noswap.sh` | One exact kubelet drop-in, one new root-only receipt, and one approved restart of the role-matched RKE2 service | Does not disable host swap, edit `/etc/fstab`, clean Pods, change another service, or assert cluster recovery |
| `publish-gpu-operator-bundle.sh` | Root-only GPU bundle state plus four private Registry tags | Does not manage host drivers/toolkit, install the chart, or prune source images |
| `mirror-gpu-operator-images-worker.sh` | Worker 1 mirror state, exact public source images, and four private Registry tags | Restricted to the idle selected Worker; does not change RKE2, GPU state, or Docker configuration |

`publish-rke2-images.sh` is restricted to the exact `marslab-server` identity.
It verifies the pinned archive, checksum manifest, and 17-entry
`docker.io/rancher/*` inventory before loading anything. It then refuses any
pre-existing local source/target tag or remote target tag, uses a temporary
Docker authentication directory and root-only netrc for Registry API checks,
publishes each target under
`10.1.200.17:5443/rancher/*`, and verifies the returned manifest digest. Source
images are retained after publication; cleanup is deliberately not automatic.
Before the large archive is loaded, the helper also pulls the existing tiny,
digest-pinned BusyBox probe and verifies `linux/amd64`. This proves daemon-side
TLS and authentication after the trust file is installed. The probe remains as
a bounded receipt; no image prune runs.

The default initial mode refuses any pre-existing publication state. Immediately
before loading the archive it creates the root-owned mode `0600`
`.publish-rke2-images.state` in the artifact directory with the pinned release,
archive digest, Registry, and `in_progress` state. If loading or pushing is
interrupted, inspect that receipt and rerun with the additional literal
`--resume`. Resume is accepted only for an exact `in_progress` receipt; it
reloads the same verified local archive, restores the 17 source tags, and
idempotently pushes each target again. Registry layer deduplication transfers
only missing content. A successful run atomically changes the receipt to
`complete`; neither initial nor resume mode will overwrite a completed receipt.

Before publication, the control host did not trust its private CA as a Docker
Registry client. The approved deployment copied only
`/etc/vela-registry/tls/ca.crt` to
`/etc/docker/certs.d/10.1.200.17:5443/ca.crt`, retained root ownership, and
verified equal SHA-256 values. The shared Docker daemon was not restarted. The
publisher still refuses to run if either path is missing, is a symlink, or
hashes differently.

After the approval boundary is satisfied, publication has this shape:

```text
sudo ./publish-rke2-images.sh \
  /srv/vela-rke2-airgap/v1.35.7-rke2r1 \
  <root-only-registry-username-file> \
  <root-only-registry-password-file> \
  --apply
```

Only after inspecting an interrupted `in_progress` receipt:

```text
sudo ./publish-rke2-images.sh \
  /srv/vela-rke2-airgap/v1.35.7-rke2r1 \
  <root-only-registry-username-file> \
  <root-only-registry-password-file> \
  --apply --resume
```

Before any RKE2 installation or first start, run `capture-node-state.sh` on all
three nodes. Its destination must be a new absolute path beneath an existing,
root-owned, non-writable-by-group-or-world parent. The receipt contains raw
links, addresses, routes, policy rules, listeners, network sysctls, firewall
rules, mounts, selected Docker state, GPU state, CNI/RKE2 path trees, swap, ZFS,
and a `MANIFEST.sha256`. It deliberately omits container environment variables
and credential contents.

```text
sudo ./capture-node-state.sh server \
  /root/vela-rke2-receipts/<timestamp>-server-prestart --apply
sudo ./capture-node-state.sh worker-1 \
  /root/vela-rke2-receipts/<timestamp>-worker-1-prestart --apply
sudo ./capture-node-state.sh worker-2 \
  /root/vela-rke2-receipts/<timestamp>-worker-2-prestart --apply
```

For installation, create a separate binary-only artifact directory containing
exactly the verified installer, `rke2.linux-amd64.tar.gz`, and
`sha256sum-amd64.txt`. Do not place any `rke2-images*.tar*` file there: the
official installer would otherwise make an unnecessary 784.7 MiB copy on every
node. `install-node.sh` reruns the full node preflight, writes a JSON-form YAML
`registries.yaml` via `jq --rawfile`, verifies all sensitive inputs are
root-owned mode `0600`, runs the pinned offline tar installer, and fails unless
the resulting service remains disabled and inactive. The helper requires
separate literal address-policy, swap-exception, and apply confirmations; `--apply`
alone cannot assert either external decision.

```text
sudo ./install-node.sh server <binary-only-artifact-dir> \
  <registry-ca> <registry-username-file> <registry-password-file> \
  --dynamic-ip-risk-approved --swap-exception-approved --apply

sudo ./install-node.sh worker-1 <binary-only-artifact-dir> \
  <registry-ca> <registry-username-file> <registry-password-file> \
  <agent-token-file> \
  --dynamic-ip-risk-approved --swap-exception-approved --apply
```

Repeat the Worker form with `worker-2`. Server first start, token extraction,
Worker installation, Worker first start, and GPU enablement are separate
checkpoints; none is performed by these helpers.

If the upstream installer or a postcondition fails, preserve the partial
`/usr/local` and `/etc/rancher/rke2` state and capture a new receipt. Do not
rerun the initial installer, delete individual files, or start the service. An
approved recovery must either prove and complete the exact partial state or use
the version-matched uninstaller under the rollback boundary below.

## Separate GPU enablement bundle

The two Workers have matching NVIDIA driver `580.159.03`, NVIDIA Container
Runtime/Toolkit `1.19.1`, and matching host runtime configuration hashes. The
applied RKE2 containerd configuration now resolves the `nvidia` runtime to
`/usr/bin/nvidia-container-runtime`; GPU Operator supplies the RuntimeClass,
Device Plugin/GFD, DCGM Exporter, validator, and NFD integration.

The GPU add-on follows the repository architecture: the host owns
driver and toolkit lifecycle, while GPU Operator `v26.3.2` supplies only the
operator/validator, Device Plugin/GFD, DCGM Exporter, and NFD. The proposed
values in `config/gpu-operator-values.yaml` therefore disable driver, toolkit,
DCGM hostengine, MIG manager, vGPU Device Manager, VFIO Manager, sandbox Device
Plugin, Kata sandbox Device Plugin, and Confidential Computing Manager changes.
Those values were applied as Helm release `gpu-operator` revision `1`.
The NFD Worker is restricted to `vela.ai/worker-profile=h3`, while the Operator
Deployment is restricted to `vela.ai/node-role=control-storage`. The control
node also carries `nvidia.com/gpu.deploy.operands=false`; postflight requires
that label and zero allocatable GPUs there. This keeps the Operator control
loop on `marslab-server` without deploying GPU operand DaemonSets to it.

`gpu-operator.env` pins the 50,603-byte chart and the four required
`linux/amd64` image manifests. Their 72 unique compressed layers total
319,572,859 bytes (304.77 MiB); manifests, configs, and HTTP framing add a small
amount. The base RKE2 bundle plus this candidate GPU bundle is approximately
1.10 GiB before application images. Both the Worker mirror state and the
control publication state are `complete`; the control publisher independently
verified every private tag and digest before chart installation.

The pinned chart was rendered locally with verified Helm `v3.18.6` after the
out-of-scope defaults were disabled. The render produced 27 Kubernetes objects.
Its direct Deployment/DaemonSet/Job Pods referenced only the private NFD and
GPU Operator images. Combining those with enabled ClusterPolicy operands
produced exactly these four unique runtime image references:

```text
10.1.200.17:5443/nfd/node-feature-discovery:v0.18.3
10.1.200.17:5443/nvidia/gpu-operator:v26.3.2
10.1.200.17:5443/nvidia/k8s-device-plugin:v0.19.2
10.1.200.17:5443/nvidia/k8s/dcgm-exporter:4.5.3-4.8.2-distroless
```

The guarded publication sequence is:

```text
# Selected idle Worker 1: public source to private Registry
sudo ./mirror-gpu-operator-images-worker.sh \
  /root/vela-rke2-ops/gpu-operator-v26.3.2 \
  <root-only-worker-registry-username-file> \
  <root-only-worker-registry-password-file> \
  --apply [--resume]

# Control: independently verify private tags/digests and complete bundle state
sudo ./publish-gpu-operator-bundle.sh \
  /srv/vela-rke2-airgap/gpu-operator-v26.3.2 \
  <root-only-publisher-username-file> \
  <root-only-publisher-password-file> \
  --apply --resume
```

The applied Helm command used only the staged chart and checked-in values:

```text
KUBECONFIG=/etc/rancher/rke2/rke2.yaml \
  helm upgrade --install gpu-operator \
  /srv/vela-rke2-airgap/gpu-operator-v26.3.2/gpu-operator-v26.3.2.tgz \
  --namespace gpu-operator --create-namespace \
  --values /root/vela-rke2-ops/gpu-operator-v26.3.2/gpu-operator-values.yaml \
  --atomic --wait --timeout 10m
```

The rendered ClusterPolicy had `driver`, `toolkit`, `migManager`,
`vgpuDeviceManager`, `vfioManager`, `kataSandboxDevicePlugin`, and `ccManager`
disabled; `sandboxWorkloads` was disabled and no enabled sandbox Device Plugin
field rendered. Runtime inspection of the applied ClusterPolicy confirmed the
same disabled lifecycle boundaries.

Worker 1's direct NVCR pull made negligible progress for more than ten minutes.
The completed publication used a temporary SSH reverse tunnel bound only to
Worker loopback and the already approved local HTTPS proxy. Docker was
temporarily given that loopback proxy, restarted, and restored immediately
after all four digest checks passed. The drop-in and tunnel were removed,
Docker again reported an empty proxy environment, its `daemon.json` hash was
unchanged, and the persistent Runner returned to `running/healthy`. Image bytes
were pushed to the private Registry over the LAN; no image archive was copied
to a Worker over SSH.

After Registry publication, cluster deployment, smoke, and a fresh GPU
postflight all passed, Worker 1's four exact public digest refs and four private
Docker tags were removed. No container referenced those image IDs, no global
Docker prune ran, and the private Registry plus RKE2 containerd copies remain
available.

The applied GPU checkpoint completed these requirements:

1. prove RKE2 containerd discovered `/usr/bin/nvidia-container-runtime` without
   letting the Operator rewrite the host toolkit;
2. apply the locally staged chart and values only to the two tainted Workers;
3. require each Worker to report exactly eight allocatable `nvidia.com/gpu`;
4. run a digest-pinned one-GPU smoke Pod, then an eight-GPU exclusivity probe;
5. verify `marslab-server` has no Vela Worker label and receives no GPU Pod; and
6. retain before/after runtime config hashes and containerd/RKE2 restart counts.

`verify-cluster.sh gpu` passed at `2026-08-29T17:21:41Z`. It explicitly accepts
only the intentionally disabled MPS control DaemonSet at `0/0/0`; every other
DaemonSet is required to have a positive desired count with all desired Pods
Ready. Worker 1 and Worker 2 each reported GPU capacity/allocatable `8/8`, the
control node reported `0`, and no GPU-requesting Pod ran there. The smoke helper
then passed one-GPU and eight-GPU probes on each Worker and deleted all four
Pods plus its namespace. None of these checks is real H3 certification or
`gpu-remediation` evidence.

## Exact host write and network set

An approved tarball install writes the RKE2 binary, uninstall helpers, and
systemd units under `/usr/local`. Before first start, the staged non-secret
config is materialized as `/etc/rancher/rke2/config.yaml`; the private CA,
Registry auth configuration, and Worker join token are separate root-owned
`0600` inputs under `/etc/rancher/rke2`.

First start additionally owns only the standard RKE2/Kubernetes paths:

```text
/var/lib/rancher/rke2
/var/lib/kubelet
/var/lib/cni
/etc/cni
/opt/cni/bin
/run/k3s
/var/log/pods
/var/log/containers
/var/log/calico
```

It must not write `/srv/vela-registry`, `/var/lib/vela-lab/mock-runner`, either
Worker ZFS pool, or the shared experiment container's mounts. RKE2 uses its
bundled containerd; it does not replace Docker or reuse Docker's image store.

The server accepts RKE2 supervisor TCP `9345` and Kubernetes API TCP `6443`
from the two LAN Workers. Every node needs kubelet TCP `10250`, Canal VXLAN UDP
`8472`, and Canal health TCP `9099` between the three LAN addresses. Etcd TCP
`2379`, `2380`, and `2381` remain server-only in this one-server lab. Do not
expose VXLAN or etcd on the `100.111.0.0/16` administration network or beyond
the three-node LAN set. CNI startup added `cni0`, `flannel.1`, Calico
interfaces, routes, and `KUBE-`/`CNI-`/`cali-`/Flannel rules.

Before service enablement, retain root-only before-state receipts for routes,
links, listeners, sysctls, nftables/iptables/ip6tables, RKE2 path absence,
Docker containers, Registry health, Runner health, GPU processes, and disk
usage. After each start, compare the same inventory and reject any unexpected
change outside the ownership set above.

All three hosts have an active 8 GiB `/swap.img`; the control host had about
3.36 GB in use during preflight. Kubernetes does not start kubelet on a
swap-enabled Linux node by default. Because disabling swap on the shared
control host could disrupt unrelated experiments, the lab candidate sets
`kubelet-arg: fail-swap-on=false` on all nodes and requests kubelet's default
`NoSwap` behavior for Pods. This is an explicit lab exception, not a production
recommendation: Kubernetes recommends control-plane nodes without swap. The
preflight therefore fails until `--swap-exception-approved` is supplied, and
post-start verification must confirm Pods cannot consume swap. Do not run
`swapoff`, edit `/etc/fstab`, or change system cgroups as part of this plan.

On this RKE2 release, all three live kubelet `configz` responses report
`failSwapOn=false` but return `memorySwap: {}`. The strict verifier does not
convert that missing observation into `NoSwap`; it fails closed until an
approved kubelet configuration makes `memorySwap.swapBehavior=NoSwap`
explicit and a fresh post-start receipt observes it. Applying that change and
restarting RKE2 are separate operations on the shared control host.

`configure-kubelet-noswap.sh` prepares that exact change but has not been run
on any lab node. It requires the exact role, hostname, LAN interface/address,
an active and enabled role-matched RKE2 service, the observed root-owned
`0700` kubelet drop-in directory, its sole root-owned `0600`
`00-rke2-defaults.conf`, one active `/swap.img`, and all three literal
confirmations. It refuses a symlink, conflicting target, extra drop-in, unsafe
receipt parent, inactive service, or pre-existing receipt. The installed file
is exactly `99-vela-noswap.conf`, ends in the upstream-required `.conf`
suffix, and contains a partial `KubeletConfiguration` with `apiVersion`,
`kind`, and `memorySwap.swapBehavior: NoSwap`.

Each run records the exact node/service/swap and target-before state, applied
drop-in, resulting target digest, service after-state, rollback instructions,
and a strict SHA-256 manifest. It atomically publishes an absent target, accepts
only an identical root-owned `0600` target for recovery/idempotency, and
restarts only `rke2-agent` for a Worker or `rke2-server` for the control host.
If restart or the post-restart service check fails, it leaves a `FAIL` receipt
and the target in place for diagnosis; rollback is never automatic.

After a separate restart approval, roll one node at a time in this exact order:

```text
sudo ./configure-kubelet-noswap.sh worker-1 \
  /root/vela-rke2-receipts/<timestamp>-worker-1-noswap \
  --swap-exception-approved --restart-approved --apply

sudo ./configure-kubelet-noswap.sh worker-2 \
  /root/vela-rke2-receipts/<timestamp>-worker-2-noswap \
  --swap-exception-approved --restart-approved --apply

sudo ./configure-kubelet-noswap.sh server \
  /root/vela-rke2-receipts/<timestamp>-server-noswap \
  --swap-exception-approved --restart-approved --apply
```

Do not start the next node until the current node is Ready, its expected GPU
capacity/allocatable value has recovered, all non-retained workloads are Ready,
and the current strict verifier has been run from the control node. The shared
control host is last. The copy currently retained at
`/root/vela-rke2-ops/gpu-operator-v26.3.2/verify-cluster.sh` is obsolete: it
uses `// "NoSwap"` and therefore converts a missing field into a false PASS.
Stage and hash the current repository verifier, whose fallback is
`// "__MISSING__"`, before this rollout. The retained strict receipt captured
17 historical Failed Pods, while a later read-only preflight found 16 because
their owning Jobs already have `ttlSecondsAfterFinished=86400`. No manual
delete is part of this rollout. If TTL cleanup has removed all failed Job Pods
by the time all three explicit fields pass, strict cluster cleanliness may pass
without a separate deletion operation; the retained receipts remain the
diagnostic evidence.

`preflight-node.sh` emits that read-only receipt and requires root so firewall
and ownership hashes are authoritative. It deliberately returns `FAIL` until
the operator supplies exactly one of `--dhcp-reservation-proven` or
`--dynamic-ip-risk-approved` and, while host swap is active,
`--swap-exception-approved`. The dynamic-IP flag records acceptance of the
observed address-stability risk; it does not claim a DHCP reservation exists.
These flags record external decisions but do not manufacture their evidence.
The script also protects the pre-existing
empty `/etc/cni/net.d` directories (root-owned mode `0700`) as baseline state
rather than assuming `/etc/cni` belongs to RKE2.

After first start, run `verify-cluster.sh base` on the server. It is read-only
and requires the exact three nodes, names, LAN IPs, RKE2 version, labels,
Worker taints, API readiness, Canal, DaemonSets, Deployments, and Pods. After
the separately approved GPU add-on, `verify-cluster.sh gpu` additionally
requires exactly eight capacity/allocatable GPUs per Worker and zero on the
control node. Both modes read each kubelet's `configz` and require
`failSwapOn=false` with an explicit `memorySwap.swapBehavior=NoSwap`; an omitted
field is a verification failure.

`smoke-gpu.sh` is the only validation helper here that mutates Kubernetes. It
does nothing without the literal `--apply` flag, refuses an existing
`vela-gpu-smoke` namespace or any already-requested GPU, and runs sequential
one-GPU and eight-GPU probes on each Worker with the existing digest-pinned mock
Runner image. On success it verifies its ownership label and removes only its
temporary namespace. On failure it preserves the namespace for diagnosis.

## Rollback boundary

Rollback is intentionally not automatic. The official tarball
`/usr/local/bin/rke2-uninstall.sh` is destructive to all RKE2 cluster state and
also removes standard kubelet/CNI paths. It invokes the version-matched
`rke2-killall.sh`, which stops RKE2, removes RKE2 pod mounts and CNI interfaces,
and filters only Kubernetes/CNI/Calico/Cilium/Flannel rules from the saved
iptables ruleset. It must never be replaced with a global table flush.

For an approved whole-lab rollback:

1. capture a fresh post-failure receipt and confirm there is no workload state
   to preserve;
2. run the pinned tarball uninstaller on Worker 1 and Worker 2, one at a time,
   verifying Docker, NetBird, the persistent mock Runner, and host networking
   after each node;
3. run the same uninstaller on `marslab-server` only after both agents are
   clean, then verify the Registry and shared experiment container retain their
   original identities and health;
4. compare routes and firewall rules against the pre-start receipt and remove
   only proven stale RKE2-owned state; and
5. preserve `/srv/vela-rke2-airgap` and the private Registry blobs by default.
   Their later removal is a separate exact-path approval because Registry
   deletion is disabled and unrelated blobs share the data root.

The rollback must not run Docker prune, restart the shared host's Docker daemon,
remove Registry CA trust, edit NVIDIA driver/toolkit files, touch ZFS, or purge
`/var/lib/vela-lab/mock-runner`.

## Applied first-start contract

The current host routes do not overlap the RKE2 default Pod and Service CIDRs.
The lab-only candidate keeps `10.42.0.0/16` and `10.43.0.0/16` and uses Canal
VXLAN over the physical LAN interfaces:

| Node | RKE2 name | Node IP | Canal interface | Role |
| --- | --- | --- | --- | --- |
| `marslab-server` | `vela-lab-control-1` | `10.1.200.17` | `enp34s0f0` | server/control only |
| Worker 1 | `vela-lab-worker-1` | `10.1.200.19` | `eno1` | agent |
| Worker 2 | `vela-lab-worker-2` | `10.1.200.16` | `eno1` | agent |

The checked-in files under `config/` encode the non-secret first-start input
that was applied on 2026-08-29. Agent tokens and Registry authentication remain
deliberately absent from the repository. Each node received a distinct
root-owned `0600` Registry configuration and CA, and each Worker received a
root-owned `0600` RKE2 token file.

The Worker reservation taint also blocks CoreDNS unless the chart explicitly
tolerates it. `config/rke2-coredns-helmchartconfig.yaml` preserves the chart's
control-plane and etcd tolerations and adds only the `vela.ai/h3=true` lab
toleration so CoreDNS can satisfy its two-node anti-affinity rule. It does not
remove the Worker taint or make general workloads eligible for those nodes.

Explicit RKE2 names are mandatory because both Worker OS hostnames are
`ubuntu`. The server must advertise and issue its API certificate for
`10.1.200.17`; agents must join `https://10.1.200.17:9345`. Only Worker nodes
receive `vela.ai/worker-profile=h3`; only the server receives
`vela.ai/node-role=control-storage`. That label boundary keeps GPU workloads
off `marslab-server` without changing its existing GPU or Docker setup.

Canal is the RKE2 default, supports the NetworkPolicy resources already in this
repository, and is included in the single 784.7 MiB archive. Its first start
added CNI interfaces, routes, VXLAN traffic on UDP `8472`, health traffic on
TCP `9099`, and iptables/nftables-compat rules. The hosts use
`iptables v1.8.10 (nf_tables)`, have IP forwarding enabled, and also have
Docker/NetBird forwarding chains. Those changes require a before/after ruleset
receipt and cannot be treated as a routine file-only install.

## Executed approval boundary

Before importing the archive, publishing RKE2 tags, installing RKE2, writing
cluster credentials, or starting the first service, the following boundaries
were approved and then checked during deployment:

1. Select one address authority: prove or create DHCP reservations for
   `10.1.200.16`, `.17`, and `.19`, or explicitly accept the lab risk of using
   the observed dynamic addresses. The latter was approved on 2026-08-29 and
   is recorded by `--dynamic-ip-risk-approved`; it is not reservation proof.
   Registry TLS and the cluster endpoint depend on `.17`.
2. Approve the lab-only swap exception: preserve host swap, set kubelet
   `fail-swap-on=false`, retain Pod `NoSwap`, and do not treat the shared control
   host as production control-plane evidence.
3. Approve a lab-only single-server RKE2 topology. It cannot prove HA, quorum,
   control/storage fault domains, or disaster recovery.
4. Approve Canal's network changes and capture the exact pre-start routes,
   listeners, sysctls, and iptables rules for rollback comparison.
5. Approve root-ext4 RKE2 state for this lab. Do not repartition, reformat, or
   mount the Workers' ZFS pools, and do not deploy the production Node Agent or
   XFS quota contract.
6. Provision a distinct root-only Registry pull credential on every RKE2 node.
   The current Distribution Registry has authentication but no per-user
   read-only authorization, so the lab credential is not a production trust
   policy.
7. Approve the exact control-host Docker Registry CA trust file described
   above. Do not change `/etc/docker/daemon.json` or restart Docker.
8. Keep the existing `vela-registry`, `vela-h3-mock-runner`, and shared
   `fchip-4591d89ff18127a74b8a25a0` containers out of rollback commands.
9. Apply GPU Operator only after base cluster health passes; keep host driver
   and toolkit lifecycle disabled. The add-on was applied only after a clean
   base receipt and was followed by GPU postflight and smoke.

The three approved preflights returned `PASS failures=0`; root-only before-state
receipts were retained before service start. The final base receipt passed at
`2026-08-29T16:19:12Z`, GPU postflight passed at
`2026-08-29T17:21:41Z`, and root-only post-state receipts were captured under
`/root/vela-rke2-receipts/20260829T1723Z-*-gpu-post` with validated manifests.
This records execution of the lab boundary; it does not convert the exceptions
above into production evidence.
