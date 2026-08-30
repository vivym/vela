# Three-host lab Production Gate gap matrix

Date: 2026-08-30

Status: `0/9 PASS`. This is a planning and evidence-boundary document, not a
Launch Receipt manifest.

The current lab has one shared control/registry host and two idle eight-GPU
Workers. It has verified private-registry distribution, RKE2/Canal,
host-lifecycle-off GPU Operator, PostgreSQL, a three-replica NATS cluster,
MinIO, one control replica, two Worker Agents, and persistent mock Runners. A
fresh mock Job and a five-wave, ten-Job concurrent mock rehearsal passed the
durable control path. The rehearsal produced exactly ten Visible Completions,
ten posted completion Charges, and twenty committed Artifacts, split five Jobs
per Worker. Two repetitions of the same Worker-control-network-partition
scenario passed with an original `LOST` Attempt on Worker 1 and a higher-fence
successful replacement on Worker 2, while preserving one Visible Completion
and one Charge per run. One retry-budget-exhaustion scenario also passed with
two `TRANSIENT_BACKEND` failures on different Workers, the first entering
`RETRY_WAIT` and the second exhausting the two-Attempt budget, while preserving
zero Visible Completions, zero Charges, zero Artifact rows, and therefore zero
committed Artifacts, with a released CreditReservation. Both
Workers then passed one process-kill scenario: the exact Worker 1 Runner main
process was killed through a pidfd, the original Attempt became `LOST` with
`WORKER_LOST`, and a higher-fence replacement on Worker 2 succeeded with one
Visible Completion, one Charge, and two committed Artifacts. Both
Workers previously passed sequential one-GPU and eight-GPU Kubernetes smoke,
while the control node remained at zero allocatable GPUs. The Runner and
application images are explicitly non-canonical, the backend is synthetic, the
concurrent rehearsal lasted only 47 seconds, only `3/10` fixed fault scenarios
has lab evidence, and no production receipt file is present. Repository tests
and these lab observations cannot substitute for the external facts required
by ADR 0029.

The retained retry-budget receipt was produced by harness SHA-256
`852a7ff868bb2cb88808bd746c74e42ed0186865f4ca19d0b7848954f2a13222`.
The review-hardened repository harness is
`b39652e15234f37cf9096f3a7268cfd1b2d830594b4ea4863d9eb9aefbdb132b`
and has not been rerun. Its stronger checks are code evidence only until a new
live rehearsal produces a separately retained receipt.

The retained process-kill receipt was produced by harness SHA-256
`cc6a79dad257ad51933cc31b0f664977f4f22d56beacc2b6ead3b9e2f5ec7d80`.
Its root-only manifest verifies independently. The fault Pod alone required
container-level `appArmorProfile: Unconfined` for signaling across the RKE2 and
Docker AppArmor profiles; it retained `RuntimeDefault` seccomp, no privilege
escalation, a read-only root filesystem, and only `CAP_KILL`. This is a narrow
lab exception and does not weaken the host-wide AppArmor policy.

## Environment gates before a production exercise

| Requirement | Current evidence | Result and required action |
| --- | --- | --- |
| Canonical release and supply chain | Mock Runner carries `vela.ai.build-kind=noncanonical-lab`; no canonical four-image publication, production signature, SBOM, scanner evidence, approval, or trust policy is provisioned | `NOT READY`; build and publish an authorized release bundle before any launch exercise |
| Kubernetes/RKE2 deployment | Three-node RKE2 `v1.35.7+rke2r1` with Canal is active; approved preflights, before/post root-only receipts, API/node/CNI readiness, and exact names/IPs passed. Live kubelet `configz` reports `failSwapOn=false` but omits `memorySwap.swapBehavior` on all three nodes. A fresh strict postflight also found ten retained historical failed smoke Pods, so the current verifier returns four failed checks | `LAB RUNNING / VERIFICATION GAP / PRODUCTION NOT READY`; explicitly configure and observe `memorySwap.swapBehavior=NoSwap` under a separately approved RKE2 restart, then archive or delete the exact failed test Jobs under separate authorization and rerun strict postflight. The topology also remains one shared control node, an operator-accepted stable-address assumption without DHCP reservation proof, active host swap, root-ext4 state, and no HA/quorum/DR fault domains |
| Kubernetes GPU integration | GPU Operator `v26.3.2` is applied with driver/toolkit and unrelated managers disabled; both RKE2 containerd configs resolve the host NVIDIA runtime; Device Plugin/GFD/DCGM Exporter/NFD/validator are Ready; Workers report `8/8`, control reports `0`; four one/eight-GPU smoke probes passed and cleaned up | `LAB VERIFIED / PRODUCTION NOT READY`; this proves only placement and execution on two lab Workers. It does not prove the real H3 backend, Node Agent remediation, XFS quota, sustained load, production supply chain, SLO, or hardware certification |
| Control/storage fault domains | Only `marslab-server` is assigned control/storage; it is shared and had about 48 GiB available memory at preflight | `NOT READY`; production CNPG and JetStream require three eligible independent control/storage nodes and durable disks |
| Worker scratch | Both Workers have ext4 roots and an almost-empty 7.25 TiB ZFS pool; no XFS project-quota root exists | `NOT READY`; production Worker/Node Agent materialization requires an approved XFS device, project ID, hard limit, and capacity receipt. Repartitioning or replacing the ZFS pool is destructive and is not authorized by this document |
| Identity and external services | No production PKI, OIDC, S3/backup domain, finance endpoint, webhook target, or on-call integration is provisioned | `NOT READY`; replace every `.invalid` and placeholder revision with externally owned, versioned resources |

## Gate-by-gate status

| Production Gate | Current lab evidence | Why it is not PASS | Next useful experiment |
| --- | --- | --- | --- |
| `preset-certification` | One synthetic profile and fixed mock media contract | No real H3 backend, saleable-group snapshot, three independent Preset certifications, quality/performance/cost measurements, or complete RateCard bindings | Keep mock records isolated; wait for the real backend and approved benchmark corpus |
| `real-h3-soak` | Two persistent Workers expose eight GPUs; success, restart, failure, cancel, one accepted durable control-plane smoke Job, and a balanced five-wave/ten-Job concurrent mock rehearsal with verified Artifacts passed | The backend is mock, the concurrent run lasted 47 seconds, and no real-H3 72-hour mixed-load or reconciliation window exists | Use longer mock cycles only for harness regression; repeat the full 72-hour mixed-load contract on real H3 |
| `state-event-fault-injection` | Runner cancellation, active-Attempt same-authority recovery after host `SIGKILL`, repository crash/fence conformance tests, one live PostgreSQL/NATS/Scheduler/Worker/Runner success path, a no-fault concurrent rehearsal, and live evidence for three fixed scenarios exist. The Worker-control-network-partition scenario produced one `LOST` Attempt, one higher-fence successful replacement, one Visible Completion, one Charge, two Artifacts, and four zero-valued fixed measurements. The retry-budget-exhaustion scenario produced two Worker-reported `TRANSIENT_BACKEND` failures, `RETRY_WAIT -> FAILED`, a released CreditReservation, and no completion, Charge, or Artifact row. The process-kill scenario used `pidfd_send_signal` against the exact Worker 1 Runner process, observed container-policy restart, persisted `WORKER_LOST`, and accepted one higher-fence replacement on Worker 2 without duplicate completion, Charge, Artifact, or stale-authority acceptance. All three retain base64-preserved raw protobuf events | This is synthetic non-production evidence for only `3/10` fixed scenarios. The other seven scenarios, real H3 behavior, Fleet reconciliation, broader repeated runs, and Production Gate review remain absent | Execute the remaining seven fixed scenarios with the same fail-closed receipt boundary, then repeat the applicable matrix against the real H3 backend |
| `gpu-remediation` | Physical eight-GPU UUID inventories are available | Node Agent is absent, no XFS quota path exists, and L0-L7 actions, approvals, post-checks, canaries, quarantine, and rate limits were not exercised | Provision a non-destructive mock post-check/fence harness first; real remediation still requires approved hardware actions and owners |
| `organization-isolation-content-safety` | Repository RLS and authorization tests plus one live synthetic tenant path through MinIO signed Artifact download exist | No multi-organization isolation run, real IdP, credential revocation, break-glass workflow, or content-reuse audit exists | Deploy isolated test tenants and external identity dependencies, then run only synthetic non-sensitive probes |
| `data-disaster-recovery` | Repository CNPG failover/PITR conformance exists; the lab runs one PostgreSQL instance, three co-located NATS replicas, and primary/backup buckets on one MinIO service | One control node cannot prove quorum or independent fault domains; there is no off-cluster WAL/Object Store, failover, restore, or credential-rotation exercise | Add two independent control/storage nodes before attempting the production RPO/RTO matrix |
| `release-rollback` | The current mock image is digest pinned | It is non-canonical and there is no N/N-1 deployment, long-Job ledger, retained event backlog, drain rollout, rollback, or reconciliation | Build two canonical compatible releases and rehearse mixed-version rollout on the lab before a production exercise |
| `commercial-data-lifecycle` | Repository Admission, credit, completion, Invoice, Webhook, retention, and deletion tests plus one live non-billable mock completion exist | No live finance/webhook integrations or end-to-end retained business-record lifecycle was exercised | Use non-billable test tenants to rehearse the fixed scenario inventory with controlled external-service fakes |
| `observability-on-call` | No live evidence | Dashboards, alerts, SLO windows, runbooks, paging integration, named 24x7 owner, and P1 response exercise are absent | Deploy the observability overlay, route test alerts, and run a timed rehearsal with retained evidence |

## Evidence decision

All nine rows remain `NOT PASS`. The current lab can improve deployment,
protocol, endurance, and fault-injection tooling, but it cannot legitimately
close `preset-certification`, `real-h3-soak`, `data-disaster-recovery`, or any
other Production Gate with the mock backend and one control node. Do not create
`docs/launch-receipts` JSON from these observations.
