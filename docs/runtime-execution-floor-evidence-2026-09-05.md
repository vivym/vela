# Runtime execution admission floor evidence

Status: local CPU-only shared-admission checkpoint `c5bd070`, schema 90, plus the
queued-authority freshness repair described below. All Supervisors
now share execution admission across their resident Services. Explicit signed
floor installation is available through `NewSupervisorWithExecutionFloor` and
`InstallExecutionFloor` as an **in-process, non-durable** component. There is no
floor RPC or default production floor configuration. Production Gates remain
**0/9**.

## Implemented behavior

- A Supervisor installs one gate into every resident Service before execution
  begins. Direct Service calls and calls routed by the Supervisor use that same
  gate. Reattaching an already used or supervised Service is rejected, preventing
  a new Supervisor from resetting its watermark. Failed construction leaves the
  remaining Services unattached.
- Prepare atomically checks the shared slot and the member-wide allocation
  watermark, consumes a valid new sequence and records PREPARING before calling
  the backend. A busy or invalid request does not consume the sequence. A backend
  Prepare failure does. A profile switch cannot restart an older allocation.
- The old Supervisor treated any retained active pointer as occupying the shared
  slot, including a STOPPED execution already marked worker-reusable. The gate
  now applies the same reusable STOPPED/FAILED condition as standalone Service
  admission. Completed model profiles can hand the slot to another resident
  profile without unloading their models.
- Installation verifies the domain-separated terminal signature, current time
  window and complete Worker/device/membership scope before changing the floor.
  Trusted member identity and device-subset digests are supplied separately at
  construction and copied; they are never learned from the incoming request.
  Every allocation must name a resident local profile/residency/runtime and its
  exact local epoch. Historical epochs or missing resident routes are rejected;
  no replacement-process drain is inferred. Barrier generation is not compared
  as though it were the local Runtime epoch.
- The floor advances monotonically across all resident profiles. An expired,
  future, tampered, canceled or mismatched installation request cannot change it.
  A lower valid cutoff cannot decrease it. After installation, Prepare, Start,
  Status and Seal at or below the floor are STALE, including an unseen allocation
  and a previously PREPARED execution. Normal Status cannot implicitly renew or
  inspect below the floor. A future historical inspection path must be separate.
- Exact cancellation and watchdog cancellation remain available below the floor,
  but Cancel cannot install an unseen renewal there. This permits stop signaling
  without reopening admission.
- Each admitted execution RPC and watchdog cancellation is registered while it
  owns its Service operation lock. The shared gate lock is released before any
  backend call. Installation snapshots calls already admitted through the floor;
  `WaitAcceptedOperations` waits for those calls to return. A queued call must
  cross the gate after acquiring its Service operation lock and therefore sees
  an intervening floor. Pre-installation calls may finish after installation.
  Admission state is bounded by the resident Service count, without per-attempt
  tombstone growth.

## Validation

- CPU tests cover one member with two resident profiles, direct Service calls,
  Supervisor routing and a public gRPC delayed Prepare. They check unseen retries,
  late Start/Status/Seal and renewal, exact cancellation, profile switching after
  both Seal and STOPPED, watermark preservation, gate replacement rejection and
  trusted configuration copies.
- Signed mismatch tests cover Worker/epoch, devices/digests, member ID/epoch and
  identity/device-subset digests, including the unseen allocation's local Runtime
  epoch, runtime identity, residency and profile. Invalid requests are followed
  by a lower valid Prepare to verify that they consumed no cutoff or watermark.
- Blocking-backend tests cover Prepare, Start, Status, Seal, Cancel and watchdog
  cancellation. Installation returns while those calls are blocked;
  WaitAcceptedOperations times out until they return. A Start queued behind
  Prepare is rejected after an intervening cutoff. None of these operations
  calls backend Close. The initial Seal fixture omitted Start and was corrected
  to reach OUTPUT_READY before testing the blocked Seal.
- `go test ./...` after the freshness repair: PASS; modelruntime 6.372 s,
  stageworkeragent 7.200 s.
- `go test -race ./internal/modelruntime ./internal/stageworkeragent ./internal/stageworkermembertransport ./internal/stageworkertransport ./cmd/vela-model-runtime`:
  PASS after the freshness repair; 12.767 s, 13.002 s, cached, cached and
  3.910 s respectively.
- `make lint`: PASS, 0 issues after correcting error-string capitalization.

## Queued authority freshness

The follow-up CPU test reproduces an additional pre-existing timing error at
`c5bd070`: Service validates an authority before waiting for operationMu, and
uses the pre-wait remaining lifetime after acquiring the lock. A blocked Prepare
allows a queued Start or Status to outlive its signed authority and still return
ACCEPTED. Prepare replays the expired authority; Seal and Cancel with an unseen
renewal reach backend work instead of rejecting it as STALE at admission.

The test observes the queued request's initial validation before advancing the
manual clock. It deliberately delays watchdog timer delivery, representing a
scheduler delay while proving that RPC admission enforces validity independently.
After the fix, all five queued operations return STALE without a second backend
call. Cancellation of an exact installed historical authority retains its
existing signature-only stop path; an expired renewal cannot be installed.

A second test keeps the queued renewal valid. Issued at 08:00:01 UTC with a
30-second monotonic window and wall expiry 08:01:01, it reaches admission at
08:00:11. Its correct remaining lifetime is `min(50, 30 - 10) = 20` seconds,
so its watchdog deadline is 08:00:31. The old code produced 08:00:41, adding the
10-second wait to the deadline. The assertion uses the minimum of both signed
time bounds; wall expiry alone is not the expected monotonic deadline.

The shared admission boundary now revalidates the already canonical envelope
after acquiring the operation and admission locks, then replaces the remaining
lifetime before Prepare, renewal or backend entry. Initial validation remains
outside the operation lock so invalid envelopes can be rejected before waiting.
This adds a second signature verification per admitted RPC; no performance
improvement is claimed or benchmarked. Pre-admitted backend work still needs its
independent cancellation/drain contract.

## Evidence limits and next work

An installation is only a process-lifetime admission checkpoint. It is not the
durable FLOOR_INSTALLED state required by terminal retirement, and it never
claims DRAINED. Waiting for admitted calls proves neither asynchronous backend
task completion nor closure of writable handles, child writers, later accepted
cancellation calls or Worker input resolvers. This increment cannot authorize
filesystem deletion or retirement of pending admission records.

Next work must persist and recover the floor against trusted, bound state roots
without silently treating missing/replaced state as first use. The existing epoch
store initializes a missing epoch file; its current restart tests do not supply
that stronger recovery guarantee. Signed floor delivery, complete current Fleet
bindings, historical inspection, execution-specific backend drain, the terminal
retirement journal and automatic startup reconciliation remain open. Ordinary
Stage retirement must preserve resident models and cannot substitute Shutdown
for execution drain.

No database integration campaign, new source-bound load campaign, Linux
execution, GPU test or terminal filesystem deletion was performed in this
increment. Earlier receipts retain their original source and scope.
