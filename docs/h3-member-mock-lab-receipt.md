# H3 multi-member mock three-host lab receipt

Date: 2026-09-03

Status: the sanitized projection records an operator-observed fake-runtime
multi-member campaign from source revision
`c77fae043be56f36d414f8675c37b8d6bcb2fcad`. Its retained phase receipts cover
TCP+mTLS, SPIFFE peer identity pinning, signed StageAuthority validation, the
complete two-member start barrier, follower unavailability during start, and
recovery. This is a non-production mock observation, not independently
recomputable cluster evidence, a real H3 run, Launch Receipt, or Production
Gate. Production Gates remain `0/9 PASS`.

## Scope and topology

The operator procedure reused the three-host lab documented in
[`h3-stage-mock-lab-receipt.md`](h3-stage-mock-lab-receipt.md). The control node
continued to host RKE2 control/storage services and the private Registry. The
projection records `vela-lab-worker-1` as leader, `vela-lab-worker-2` as
follower, and RKE2 `v1.35.7+rke2r1`. It does not retain Pod UIDs, scheduling
events, or before/after node-readiness snapshots.

The projection records the live Vela database at migration `00033` with zero
Production Gate receipts, while the current repository contract is `00066`.
This version gap is why the existing control deployment was not used for
Stage/Fleet orchestration or upgraded in place. The projection does not retain
the raw database queries or other Catalog/SLO observations.

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

The projection binds the digest-pinned image selected for the campaign. It does
not retain the per-Pod Kubernetes `imageID` fields or network-transfer logs, so
this commit does not independently validate the image pull path on each Worker.

## Campaign method

The operator procedure adapted the repository's disposable workloads to an
exact new `vela-h3-disposable` namespace and used temporary node labels for the
leader and follower. The workload configuration specified:

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
| recovery | `11:58:40.200Z` to `11:58:40.208Z` | 2 | 2 | `PASS`; complete barrier, cancellation, and stopped-state reporting restored |

The retained fault receipt records two prepared members, one started member,
local cancellation, and an unavailable remote member. That is consistent with
the intended follower-loss scenario and its fail-closed result. The sanitized
projection does not retain the Deployment scale events or follower Pod UIDs, so
the exact injection ordering and replacement Pod are not independently
recomputable from this commit.

## Non-interference and cleanup

The projection records an aggregate GPU request of `0`, no non-campaign Pod
inventory difference, and post-cleanup absence of the namespace, temporary node
labels, and credential directory. It does not include the raw Pod inventories,
their hashes, node-readiness output, Registry postflight, or Docker cleanup
output. Those stronger cluster and storage postconditions therefore cannot be
independently recomputed from this commit.

The sanitized receipt projection is retained in
[`h3-member-mock-lab-evidence-2026-09-03.json`](h3-member-mock-lab-evidence-2026-09-03.json)
with SHA-256
`f017067d0921e3335f0e4b2e6ddc3b0481d811ffd6035b436ee9ac25ac5dc1e9`.
No credential, GPU UUID, Secret payload, or customer content is committed. The
raw operator evidence is not committed or bound by a manifest digest; the JSON
explicitly limits independent recomputation to its phase receipts and bounded
aggregate fields.

## Evidence boundary

This receipt records fake-runtime multi-member transport and barrier behavior
with the operator-observed Worker mapping above. Without the raw cluster bundle,
it does not independently prove physical placement or cluster postconditions.
It also does not prove Fleet Controller actuation, database-backed StageLease or
StageAttempt authority, StageArtifact transfer, ModelResidency, GPU/DRA
identity, real model behavior, output equivalence, performance, soak, HA/DR,
release provenance, or on-call operation. It cannot advance any Production Gate
and does not reduce the recorded live schema gap from `00033` to `00066`.
