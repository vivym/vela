# H3 multi-member mock three-host lab receipt

Date: 2026-09-03

Status: the fake-runtime multi-member campaign from source revision
`c77fae043be56f36d414f8675c37b8d6bcb2fcad` passed on the two RKE2 Worker
nodes without requesting a GPU. It exercised cross-Worker TCP+mTLS, SPIFFE peer
identity pinning, signed StageAuthority validation, the complete two-member
start barrier, follower loss between prepare and start, and recovery. This is a
non-production mock result, not a real H3 run, Launch Receipt, or Production
Gate. Production Gates remain `0/9 PASS`.

## Scope and topology

The campaign reused the three-host lab documented in
[`h3-stage-mock-lab-receipt.md`](h3-stage-mock-lab-receipt.md). The control node
continued to host RKE2 control/storage services and the private Registry. It
did not run a campaign Pod and exposes no Kubernetes GPU. The leader ran on
`vela-lab-worker-1`; the follower ran on `vela-lab-worker-2`. All three nodes
were `Ready` on RKE2 `v1.35.7+rke2r1` before and after the exercise.

The live Vela database was inspected read-only before the campaign. Its latest
applied migration was `00033`, while the current repository contract is
`00065`. `production_gate_manifests` and `production_gate_receipts` were empty,
Catalog and SLO remained `LEGACY v1`, and no schema or control workload was
changed. This version gap is why the existing control deployment was not used
for Stage/Fleet orchestration or upgraded in place.

## Build and image identity

The control host's Go `1.22.2` was below the repository's Go `1.26` contract.
The campaign binary was therefore cross-compiled on the development host with
Go `1.26.7`, after the targeted `internal/h3membercampaign` and command tests
passed. A compressed `linux/amd64` binary was transferred over SSH and imported
into a scratch image on the control host. No Go or Docker build image was
transferred to either Worker.

The control host could not reach `proxy.golang.org`. Read-only probes confirmed
that `goproxy.cn`, `goproxy.io`, and the Alibaba Cloud Go proxy could serve the
exact Go `1.26.7` toolchain metadata. None was persisted in host configuration,
and no remote toolchain download was needed for this campaign.

| Property | Observed value |
| --- | --- |
| source revision | `c77fae043be56f36d414f8675c37b8d6bcb2fcad` |
| source archive SHA-256 | `c39ce08da56716ed1a5799a73b119ce2a495b20304132b966516f09b5787aba7` |
| campaign binary SHA-256 | `2bcfc22b369468eed344dc90b1606185e3edd2bda6f994a639c9ad33457c833e` |
| platform | `linux/amd64` |
| scratch image size | 5,014,654 bytes |
| Registry digest | `sha256:e9eee76dfb64b01210db34d80c8a55bc9dc81008ee01591f72673c863c8cb5f1` |

The image was published only under the non-production repository:

```text
10.1.200.17:5443/vela-lab-next/vela-h3-member-campaign
```

Both Workers reported the exact digest-pinned `imageID`. The image traveled
from the control-host Registry over the lab LAN, not over SSH.

## Campaign method

The repository's disposable campaign workloads were adapted to an exact new
`vela-h3-disposable` namespace in the existing RKE2 cluster. Two temporary node
labels pinned the leader and follower to distinct Workers. Every campaign Pod:

- requested `25m` CPU and `32Mi` memory, with limits of one CPU and `128Mi`;
- requested no `nvidia.com/gpu` resource;
- disabled service-account token mounting and service links;
- used `RuntimeDefault` seccomp, a read-only root filesystem, no privilege
  escalation, and no Linux capabilities; and
- mounted only a one-day campaign CA, independent leaf credentials, and a
  short-lived campaign authority key from the temporary namespace Secret.

The campaign used the production `stageworkermembertransport` and
`stageworkeragent` code paths over TCP+mTLS. The ModelRuntime implementations
and StageAuthority inputs were bounded test fixtures.

## Results

| Phase | UTC interval | Prepared | Started | Result |
| --- | --- | ---: | ---: | --- |
| normal | `11:56:47.053Z` to `11:56:47.061Z` | 2 | 2 | `PASS`; both members reported, acknowledged cancellation, and stopped |
| follower loss | `11:57:42.932Z` to `11:57:57.950Z` | 2 | 1 | `FAULT_REJECTED`; local member stopped and the unavailable remote member was not reported stopped |
| recovery | `11:58:40.200Z` to `11:58:40.208Z` | 2 | 2 | `PASS`; complete barrier and shutdown restored |

The fault was injected only after the follower logged its second prepare. The
test then scaled the temporary follower Deployment from one replica to zero.
The leader started locally, could not complete the remote start, canceled its
local runtime, and failed closed. Scaling the same temporary Deployment back to
one produced a new follower Pod and the recovery phase passed.

## Non-interference and cleanup

The campaign namespace's aggregate GPU request remained `0` in every phase.
The existing Vela and system Pod inventory was captured before the campaign and
after cleanup, normalized to namespace, name, UID, node, phase, restart count,
image, and image ID, and compared byte-for-byte. The diff was empty. All Vela
workloads remained ready, all three nodes remained `Ready`, and the Registry
container remained running.

Cleanup deleted the exact `vela-h3-disposable` namespace, removed both temporary
node labels, deleted all short-lived private keys and certificates, logged out
the temporary Docker Registry credential, and removed the local Docker image.
No campaign workload or temporary label remained. The Registry retains the
approximately 5 MB digest-pinned image. The control host retains approximately
4.3 MB of root-only source and raw evidence; it contains no private key.

The sanitized receipt projection is retained in
[`h3-member-mock-lab-evidence-2026-09-03.json`](h3-member-mock-lab-evidence-2026-09-03.json)
with SHA-256
`eadeac7346bcf2154086b1c268212de33b65eebc85ea2622a78c25b918f4ccda`.
No credential, GPU UUID, Secret payload, or customer content is committed.

## Evidence boundary

This result proves fake-runtime multi-member transport and barrier behavior on
two physical Kubernetes Workers. It does not prove Fleet Controller actuation,
database-backed StageLease or StageAttempt authority, StageArtifact transfer,
ModelResidency, GPU/DRA identity, real model behavior, output equivalence,
performance, soak, HA/DR, release provenance, or on-call operation. It cannot
advance any Production Gate and does not reduce the live schema gap from
`00033` to `00065`.
