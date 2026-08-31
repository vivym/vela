# Three-host lab Production Gate gap matrix

Date: 2026-08-31

Status: `0/9 PASS`. This is a planning and evidence-boundary document, not a
Launch Receipt manifest.

The current lab has one shared control/registry host and two idle eight-GPU
Workers. It has verified private-registry distribution, RKE2/Canal,
host-lifecycle-off GPU Operator, PostgreSQL, a three-replica NATS cluster,
MinIO, one control replica, two Worker Agents, and persistent mock Runners. A
six-of-nine organization-isolation rehearsal also passed 20 negative probes,
including cross-Organization/project RLS, credential revocation, composite
foreign keys, and exact-version signed URL method/path/version binding. The v4
rerun additionally correlated six unique API request IDs to eight exact
database login/group observations across service authentication, Job read, and
Artifact read. It is explicitly `NON_PRODUCTION_MOCK_REHEARSAL`; the complete
public HTTP path inventory, real IdPs, Break-glass workflow, and content-reuse
audit sink remain absent. Separately, a fresh mock Job and a five-wave, ten-Job
concurrent mock rehearsal passed the
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
Visible Completion, one Charge, and two committed Artifacts. One
Outbox-post-commit control-crash scenario then killed the exact control process
after a committed `job.ready` event remained unpublished and unclaimed. The
restarted Publisher published it once to `VELA_EVENTS`, while the Job completed
with exactly one Attempt, one Visible Completion, one Charge, and two committed
Artifacts. A Publisher post-PubAck/pre-marker scenario then killed the control
process after NATS acknowledged `job.ready` but before PostgreSQL recorded the
Broker receipt. After the claim TTL, the recovered Publisher reclaimed the same
event, preserved the original Broker sequence, and completed with one Attempt,
one Visible Completion, one Charge, and two committed Artifacts. A Publisher
pre-PubAck scenario then killed the control process after PostgreSQL claimed
`job.ready` but before any NATS delegation or Broker marker. Recovery published
a new sequence and again completed with one Attempt, one Visible Completion,
one Charge, and two committed Artifacts. A Consumer post-DB/pre-Ack scenario
then killed the control process after the Scheduler handler and Inbox receipt
completed their separate transactions but before JetStream Ack confirmation.
The same stream sequence was redelivered with a higher Consumer sequence, the
existing Inbox receipt suppressed handler reapplication, and the Job again
completed with one Attempt, one Visible Completion, one Charge, and two
committed Artifacts. A node-reboot scenario then bound an out-of-band Worker 1
reboot to the exact node UID, boot ID, Job, Attempt, and fence. Kubernetes
observed `Ready=Unknown`, the boot ID changed, Attempt 1 became
`LOST/WORKER_LOST`, and Worker 2 completed one higher-fence replacement without
duplicate durable results. An Assignment post-commit/pre-response crash then
recovered one committed dispatch authority without creating another Attempt.
Finally, the exact old FINALIZATION Completion Candidate was replayed after a
higher-fence replacement had succeeded and was rejected as
`REJECTED_STALE_LEASE`. Both Workers previously passed sequential one-GPU and
eight-GPU Kubernetes smoke, while the control node remained at zero allocatable
GPUs. The Runner and application images are explicitly non-canonical, the
backend is synthetic, the concurrent rehearsal lasted only 47 seconds, all
`10/10` fixed fault scenarios have lab evidence, and no production receipt file
is present. Repository tests and these lab observations cannot substitute for
the external facts required by ADR 0029.

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

The retained Outbox-post-commit receipt was produced by harness SHA-256
`70dab9231d7f75f49abfc531dfd028a2316ae462ee6dcdb1af865980898034ef`.
Its root-only `SHA256SUMS` file has SHA-256
`674eaf8ac922f8fe4a9740435fbe3c08daa42a9ed88d5a519fee9c8703beb812`.
The bounded `VELA_PUBLISHER_TICK` override existed only for fault injection and
was removed afterward; the deployed control again uses the default `500ms`
interval. This v2 receipt uses the review-hardened watchdog-before-mutation and
exact ten-scenario-set checks. The earlier v1 success is superseded; two failed
runs remain diagnostic evidence and are not counted.

The retained Publisher post-PubAck/pre-marker receipt was produced by harness
SHA-256
`cc37ee0df5e813e0929f4ea083782d785153b846bd81040d70802f397065f0a0`
and its root-only v2 `SHA256SUMS` file has SHA-256
`d73cdce02500580a4f1b5961844e7808f5f19f3cb4bc0bb9be25d236a9165bfb`.
The successful event kept `VELA_EVENTS` sequence `268` while
`publish_attempts` advanced from `1` to `2`. This hardened rerun requires a
UID-bound control Pod replacement plus proof that the fault environment and
marker are absent before cleanup can disarm the watchdog. The earlier v1
success is superseded; its diagnostic predecessor is not counted.

The retained Publisher pre-PubAck receipt was produced by harness SHA-256
`6483f806b62c747110ff9a159d6e8bbba40a98efe46e599765a336874a21ed88`.
Its root-only `SHA256SUMS` file has SHA-256
`a05cc044f7b2536cc58604aed95fadc5723806aeb814319d4058c3f4a210c3d9`.
Before the crash, the PostgreSQL marker required an empty Broker stream and
sequence zero while the NATS leader `last_seq` remained `279`. Recovery
recorded `VELA_EVENTS` sequence `285` and advanced `publish_attempts` from `1`
to `2`; all four fixed measurements remained zero. Two failed runs stopped
before `SIGKILL` and remain diagnostic only.

The retained Consumer post-DB/pre-Ack receipt was produced by harness SHA-256
`75331cb29a07a89c3d69c6a166e81772ea36ce8aef23afa84a93fa1687d9a0e8`.
Its root-only `SHA256SUMS` file has SHA-256
`817edbf165a151d8a2552aadbfcef907a4651484d720cade36bae59a63f873fe`.
The exact stream sequence `286` was delivered with Consumer sequences `48`
and `49`; `num_ack_pending` changed from `1` to `0`, the Inbox receipt and
Attempt counts remained one, and handler reapplication remained zero. One
pre-Job diagnostic run stopped before `SIGKILL` and is not counted.

The retained node-reboot receipt was produced by harness SHA-256
`cf0633080aacfedbf543290b611f354967b6ef8ad8a0991aa25d5d8520768a84`.
Its root-only `SHA256SUMS` file has SHA-256
`e6decc92d15d6bf8933c922ab8f9550ec76129e3a74682c757a4ead01aa69c20`.
Worker 1 kept node UID `4068931d-46f8-48b0-ba03-fd6135ea64cd` while its boot
ID changed from `45d53683-08e0-4a0b-845b-ddb5f8acafde` to
`1db56fa8-5375-4250-aaf5-0747cd550f01`. Attempt 1 became `LOST` with
`WORKER_LOST`; Worker 2 completed Attempt 2 at fence 3; and completion, Charge,
and committed Artifact counts remained one, one, and two. The first run failed
closed on transient `Ready=True` with zero allocatable GPUs and remains
diagnostic only.

The retained Assignment post-commit/pre-response receipt was produced by
harness SHA-256
`333cb27a399fc03da909a187ae1690b06a50292e8e9fbc317533fc6a1ef1f393`.
Its root-only `SHA256SUMS` file has SHA-256
`f0c1a320b396579359921a51f0a1d9d97d6f305dc331d1bed288767818bf6b30`.
The Attempt count remained one across recovery; the committed dispatch intent
was replayed without a new Assignment, and the Job retained one completion, one
posted Charge, and two committed Artifacts.

The retained stale-fence late-completion receipt was produced by harness SHA-256
`2d6f642a20758ac47ee5e397ef9fcb2191dbb853edb90defc0e8f07cb3022909`.
Its root-only `SHA256SUMS` file has SHA-256
`e2bc7e8af1bdfb43ba546d5ef89f9737ff961e812b1be6ddb570f5cc3f18c4cd`.
The old fence-1 Attempt failed with `FINALIZATION_TIMEOUT`, the RetryDecision
advanced the Job to fence 2, the replacement succeeded at fence 3, and exact
old-candidate replay returned `REJECTED_STALE_LEASE`. The final ledger contains
one completion, one posted Charge, one winning ArtifactSet, and two committed
replacement Artifacts. Independent live postflight restored the idle two-Worker
boundary and verified the Worker Agent runtime digests.

All ten fixed scenarios now have synthetic lab evidence. This closes the mock
harness inventory only; it does not advance the Production Gate result.

The request-role control rollout is retained at
`/root/vela-lab-deploy-bc590e20/receipts/control-request-role-rollout-v1`.
The v4 isolation receipt is retained at
`/root/vela-lab-deploy-bc590e20/receipts/organization-isolation-content-safety-v4`.
Their `SHA256SUMS` files have SHA-256 values
`ebc21396b34b6a9a67430c2c31857bfad1b826b53fb1403b6094f5901b284cd9` and
`5efb9895f4fda791088076a82b1bbc229f44b0d42bdcd32f3be2d40738cce460`,
respectively. The new control image is digest-pinned at
`sha256:4870b579c499c5d07b53a1442bb43083dff9ce2c178e610b80a932155c6adbab`.
The v4 before/after database snapshots are byte-identical, its temporary Pod
and ConfigMap are absent, and neither a filesystem Launch Receipt nor a database
Production Gate receipt was created.

## Environment gates before a production exercise

| Requirement | Current evidence | Result and required action |
| --- | --- | --- |
| Canonical release and supply chain | Mock Runner carries `vela.ai.build-kind=noncanonical-lab`; no canonical four-image publication, production signature, SBOM, scanner evidence, approval, or trust policy is provisioned | `NOT READY`; build and publish an authorized release bundle before any launch exercise |
| Kubernetes/RKE2 deployment | Three-node RKE2 `v1.35.7+rke2r1` with Canal is active; API/node/CNI readiness and exact names/IPs pass. The retained `2026-08-31T02:41:14Z` strict postflight reports `FAIL failures=4`: all three live kubelet `configz` responses have `failSwapOn=false` but only `memorySwap: {}`, and 17 exact historical Failed Pods were live at capture time. A `2026-08-31T03:58:31Z` read-only preflight found 16; their failed Jobs already carry `ttlSecondsAfterFinished=86400`, and no delete was issued by this work. API, node, DaemonSet, Deployment, Canal, and GPU `0/8/8` checks pass. A guarded and tested NoSwap helper is prepared locally but not applied | `LAB RUNNING / VERIFICATION GAP / PRODUCTION NOT READY`; stage the strict verifier and apply the explicit kubelet drop-in only after separate restart approval, Worker 1 then Worker 2 then shared control, with recovery checks between nodes. Do not manually delete historical Pods as part of this rollout; allow the existing TTL policy to expire them and retain the signed receipts as evidence. The topology remains one shared control node, an operator-accepted stable-address assumption without DHCP reservation proof, active host swap, root-ext4 state, and no HA/quorum/DR fault domains |
| Kubernetes GPU integration | GPU Operator `v26.3.2` is applied with driver/toolkit and unrelated managers disabled; both RKE2 containerd configs resolve the host NVIDIA runtime; Device Plugin/GFD/DCGM Exporter/NFD/validator are Ready; Workers report `8/8`, control reports `0`; four one/eight-GPU smoke probes passed and cleaned up | `LAB VERIFIED / PRODUCTION NOT READY`; this proves only placement and execution on two lab Workers. It does not prove the real H3 backend, Node Agent remediation, XFS quota, sustained load, production supply chain, SLO, or hardware certification |
| Control/storage fault domains | Only `marslab-server` is assigned control/storage; it is shared and had about 48 GiB available memory at preflight | `NOT READY`; production CNPG and JetStream require three eligible independent control/storage nodes and durable disks |
| Worker scratch | Both Workers have ext4 roots and an almost-empty 7.25 TiB ZFS pool; no XFS project-quota root exists | `NOT READY`; production Worker/Node Agent materialization requires an approved XFS device, project ID, hard limit, and capacity receipt. Repartitioning or replacing the ZFS pool is destructive and is not authorized by this document |
| Identity and external services | No production PKI, OIDC, S3/backup domain, finance endpoint, webhook target, or on-call integration is provisioned | `NOT READY`; replace every `.invalid` and placeholder revision with externally owned, versioned resources |

## Gate-by-gate status

| Production Gate | Current lab evidence | Why it is not PASS | Next useful experiment |
| --- | --- | --- | --- |
| `preset-certification` | One synthetic profile and fixed mock media contract | No real H3 backend, saleable-group snapshot, three independent Preset certifications, quality/performance/cost measurements, or complete RateCard bindings | Keep mock records isolated; wait for the real backend and approved benchmark corpus |
| `real-h3-soak` | Two persistent Workers expose eight GPUs; success, restart, failure, cancel, one accepted durable control-plane smoke Job, and a balanced five-wave/ten-Job concurrent mock rehearsal with verified Artifacts passed | The backend is mock, the concurrent run lasted 47 seconds, and no real-H3 72-hour mixed-load or reconciliation window exists | Use longer mock cycles only for harness regression; repeat the full 72-hour mixed-load contract on real H3 |
| `state-event-fault-injection` | Repository crash/fence conformance, live PostgreSQL/NATS/Scheduler/Worker/Runner flow, and all `10/10` fixed lab scenarios have evidence. The matrix covers Worker-control partition, bounded retry exhaustion, exact process kill, three Publisher/Outbox boundaries, Consumer post-DB/pre-Ack redelivery, node reboot, Assignment post-commit/pre-response replay, and exact stale FINALIZATION candidate rejection after a higher-fence replacement. Passing receipts retain complete authority ledgers, exact fault markers, checksums, and base64-preserved raw protobuf events; the final live postflight restored the idle `0 active Jobs / 0 active Leases / 2 READY+HEALTHY Workers` boundary | This remains synthetic non-production mock evidence. Real H3 behavior, Fleet reconciliation under production topology, broader repeated runs, independent Production Gate review, and production identities/services remain absent | Repeat the applicable ten-scenario matrix against the real H3 backend and production dependencies, under the versioned Production Gate receipt contract |
| `gpu-remediation` | Physical eight-GPU UUID inventories are available | Node Agent is absent, no XFS quota path exists, and L0-L7 actions, approvals, post-checks, canaries, quarantine, and rate limits were not exercised | Provision a non-destructive mock post-check/fence harness first; real remediation still requires approved hardware actions and owners |
| `organization-isolation-content-safety` | The reviewed v4 `NON_PRODUCTION_MOCK_REHEARSAL` receipt reports `6/9` fixed scenarios and 20 ledger-derived negative probes against two persisted synthetic foreign-scope Jobs. It binds 30 configured database URLs to exact login/group pairs; compares exact direct/effective table `22/22`, column-only `6/6`, and routine `13/13` ACLs; rejects direct login, sequence, unsafe-schema, and effective `PUBLIC`/inherited expansion; and proves forced RLS on all 72 Organization-scoped tables. Six unique API request IDs correlate to eight exact service-authentication, Job-read, and Artifact-read database role observations. Unexpected allows and credential-revocation bypasses were zero | The complete public HTTP request-role path inventory, Customer and Platform IdPs, real Break-glass approval/audit scenario, and independent customer-content reuse audit remain absent. The fixed-role matrix is `LAB_REHEARSAL_PARTIAL_PUBLIC_PATH_INVENTORY_INCOMPLETE`, and this mock receipt cannot produce a Launch Receipt | Extend request-role coverage to the complete public HTTP path inventory; add real identities, the two-person Break-glass workflow and audit projection, and an independent content-reuse audit sink; rerun all nine scenarios with a canonical release and production identities |
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
