# Durable Worker health evidence and remaining validation

The live-status fix is committed as `9ad887c`. A further public-gRPC counterexample
against that revision showed that an acknowledged `WorkerReusable=false` was
forgotten after exact drain and reconstruction of the durable Supervisor at a
new Runtime epoch. This reproducer exercises the journal/admission layer; the
production server already has an independent unresolved-incarnation barrier.
It does not show that the production startup path bypassed that barrier.

## Implemented behavior

Runtime journal **schema 8** retains each allocation's latest explicit backend
failure/health evidence, alongside its exact signed authority. The successful
Status response and live health clearance follow the durable health write.
Identical observations are idempotent. Persistence uncertainty withholds success
and requires state recovery. STOPPED, cancellation and drain do not clear health.

The member's shared admission and readiness boundary checks all retained health
denials. A validated explicit `WorkerReusable=true` updates only its own exact
allocation; one resident profile cannot clear another profile's denial. The
existing live backend assertion remains the clearance contract. This increment
does not introduce an administrative clearance command or attest physical health.

Recovery preserves health restrictions even when every execution has a matching
drain checkpoint. The actual Registry-bound Runtime server exposes discovery and
recovery inspection but starts zero backend factories for denied history. Fresh
Prepare returns REJECTED with the health-recovery reason. Historical sealed
receipt replay remains read-only and available; it grants no execution rights.

Live journal recovery and complete snapshot validation share bounded canonical
protobuf validation, including failure class/fingerprint/resource units,
timestamps, unknown fields and signed authority membership in the confirmed
renewal interval. Health observation metadata is not itself a signed backend or
device attestation. Original file custody remains a separate obligation.

## Migration and operational impact

Ordinary recovery rejects schema 7. Explicit offline
`vela-model-runtime journal --action upgrade-v7` preserves journal/storage
identity, watermarks, execution history, receipts and backend lifecycle. Any
legacy retained execution makes `health_history_unknown=true`: old drains cannot
prove that no unhealthy status was previously acknowledged. Migration fabricates
neither healthy evidence nor a health denial. Empty first-use history acquires no
invented observation, and the independent backend-incarnation gate still applies.

Schema-2 through schema-6 upgrade paths also target 8 with this conservative
health-history rule. Registry-bound normal startup rejects all migration flags.
An upgraded journal with unknown health can be inspected and can replay complete
old receipts; it cannot resume scheduling. Clearing that uncertainty requires
the future independently authorized retirement/replacement protocol. There is
no reset flag that turns unknown history into healthy history.

PostgreSQL remains 94, Worker journal 5, Registry binding 1 and release bundle 3.
Existing deployments have not been migrated. Repeated changing failure evidence
now requires journal persistence; no sustained throughput budget is claimed.

## Verification

- Full final `go test ./...`, `go vet ./...`, golangci-lint 2.13.1 and
  `make test-cross` pass. Integration-tag tests are separate from ordinary tests.
- Host race exercises public gRPC, persistence uncertainty, explicit clearance,
  cross-profile isolation, migration, actual Runtime-server recovery, and the
  existing terminal drain/renewal/sealed-receipt regressions.
- Six actual child-process exits cover denial and clearance at rename, directory
  sync and successful-response boundaries. Recovery preserves the recorded
  outcome. These tests retain the host filesystem and are not power-cut tests.
- Twelve corruption cases fail both snapshot and live recovery: fingerprint,
  class, resource units, failed/retry timestamps, unknown message/timestamp
  fields, authority, missing confirmation, future renewal, legacy injection and
  oversized evidence.
- Final native Linux/arm64 static race tests pass **45 behavioral main tests
  plus four subprocess helper entrypoints**, without skips or race reports.
  They run as UID/GID 10001 with all capabilities dropped and no network.
- Actual root Node/non-root PID-1 Runtime startup exchange passes four scenarios:
  mock permit, denial, lost response and uncertain Node record. These remain mock
  decisions, not a production startup issuer.
- Final PostgreSQL/Registry/protected-provisioning command campaign passes under
  host race instrumentation in **74.073 package seconds**, including original
  pair/claim recovery and interrupted command boundaries. Its command binaries
  are ordinary builds; the separate native run supplies Linux race evidence.

The final set ran after the last production-code change. Earlier intermediate
lint findings and the new startup test's STALE-versus-REJECTED mismatch were
repaired before that set. No remote CI or full integration-shard run is claimed.

Logs are in `/tmp/vela-startup-validation.syiSMk/`:

| Log | SHA-256 |
| --- | --- |
| `durable-health-red.log` | `0d91b9015e2e819811877b611af9ee361d5da690fd725b5331d139ffda214571` |
| `durable-health-final-unit.log` | `023825b0600270dee5a8dd83299e4bad7a65855f1636d512517eb60a92d020ab` |
| `durable-health-final-host.log` | `5de0c7cb599585dcc488d0478961f4ce74413cd4f4a81cffb47efc573862cbcf` |
| `durable-health-final-lint.log` | `e92606b0bf483111dff0a120c315ea165821348f31365020e2468a0059095c47` |
| `durable-health-final-cross.log` | `3945b555da8a0bad9eb316e9b0a2f68b85ab7d1a073b8873a436debe5b55184a` |
| `durable-health-linux-race.log` | `3922ecee598676ef05e01d69a7870549ca88c61573aab5abb720387345ca55de` |
| `durable-health-node-exchange.log` | `36ba7901d4f4d3942ecf3ae19ccd52272c855bbf5f9a85983911d256e39d2003` |
| `durable-health-final-postgres.log` | `1e13d40d5396d613cef92622de7a23600dfe170facf3d7eb09a9754989ac4bff` |

Final Linux image:
`sha256:0245afd958583ce1a38ea65c9224987b33efb5fba5ed42fe3208aa9adbc16985`.
Both `/modelruntime.test` and `/nodeagent.test` were rebuilt from this source.

## Remaining work, in dependency order

| Priority | Required work | Evidence needed to close it |
| --- | --- | --- |
| P0 | Trusted custody for both Worker and Runtime journals | Node-private typed transitions; authenticated roles; positive Job execution/recovery; workload rewrite, lock ABA and sibling-writer exclusion. The existing prototype proves only floor transitions. |
| P0 | Current Registry/Fleet startup and effective launch authority | Original first-use provenance, exact effective config/mounts/initializer, current activation and durable grant outcome; reconcile partial local/PostgreSQL outcomes and lost responses. |
| P0 | Safe replacement and health clearance | Independently proven old-owner/input/backend writer exclusion, lost-pidfd/Node-restart handling, explicit treatment of unhealthy/unknown history, and a successful authorized replacement path. Persistent denial alone is not availability closure. |
| P0 | Exact retirement and bounded operation | Retire the exact scratch incarnation, settle capacity, reclaim eligible history without forgetting watermarks/health/receipts. Current 32-record admission bound prevents unbounded growth but eventually stops new work. |
| P1 | Complete current-source CPU Job campaign | Exercise the production Worker control stream and durable journals through output publication, exact-cache miss/admit/hit/reuse, one-Charge billing, tenant isolation and injected faults. Older wave campaigns do not cover the final composition. |
| P1 | Sustained arrivals and scientific performance validation | Independent open-loop arrivals, measured backpressure/queue depth/resource growth/tails and recovery; compare matched workloads and costs against a fixed baseline. Closed-loop floor IPC and drained 512-Job waves are not sustained-arrival evidence. |

No current result justifies replacing PostgreSQL authority. Node-private custody
remains the measured architecture candidate described in the
[custody design](node-journal-custody-design-2026-09-08.md), with the limitations in
the [prototype report](node-journal-custody-prototype-evidence-2026-09-08.md).
Health persistence closes one missing local state transition; it does not make
those larger contracts complete. Production Gates remain **0/9**. No GPU work,
push, remote deployment or memory edits occurred.
