# Runtime backend identity recovery after unacknowledged renewal

This increment follows `df89ec4`. PostgreSQL schema remains 94, Worker journal 5,
Runtime journal 4, Registry binding 1 and Production Gates `0/9`.

## Reproduced failure

Runtime accepted a renewal before backend Status acknowledged it, replacing its
only active envelope. When the Status call failed before backend application,
Cancel sent the new digest to a backend still holding the original. The
counterexample returned `fake ModelRuntime cancellation authority is stale`.
Exact original inspection/drain was also hidden by Runtime's current-digest
check. The second case, application followed by response loss, already canceled
successfully; the fix must handle both without guessing which one occurred.

`TestModelRuntimeRecoversCancellationAfterUnacknowledgedRenewal` reproduced the
not-applied failure on the preceding implementation. Both cases now cancel the
actual backend identity and persist its exact writer-drain checkpoint.

## Implementation

The accepted execution grant and last backend-confirmed envelope are separate
active fields. Successful Prepare/Start and validated Status confirm backend
identity. Renewed Prepare replay, Start and Start replay synchronize through the
existing backend Status operation, using an interruptible execution-call context.
Ordinary same-envelope operations need no additional Status call. Another
distinct renewal rejects until the previous pending grant has been confirmed;
an exact pending-grant Status retry can confirm it and resume normal renewal.

Cancellation resolves ambiguity with two bounded, read-only exact inspections
under the execution mutex. Exactly one valid Known observation is required.
Missing inspection capability, errors, timeout, neither-known, both-known or
malformed observations cannot dispatch Cancel. The observed envelope is copied
defensively, and caller eligibility is checked again after reconciliation so a
superseded candidate cannot authorize a stop. This changes the backend observation
only, never the accepted grant or watchdog lifetime.

The same resolver serves the watchdog. A FAILED execution without proven reuse
remains eligible for cleanup after partial Prepare failure, structured backend
failure or failed watchdog reconciliation. Failed inspection can be retried by
an exact cancellation request. Cancellation acknowledgement still does not free
the shared slot or create a drain checkpoint.

Exact inspection/drain recognizes the retained backend envelope. Confirmation
of the accepted grant drops the superseded candidate. Explicit drain and
allocation checkpoint lookup preserve the signed envelope actually drained;
they never relabel its proof as belonging to a different renewal.

## Validation

- Recovery covers before-application failure and application followed by response
  loss, terminal-floor installation, exact cancellation, inspection, explicit
  drain and allocation lookup.
- Inspection faults cover errors, unknown, both-known, invalid results and
  timeout; each rejects without backend Cancel, then permits a healthy retry.
- Three-grant tests prove the previous backend identity cannot be overwritten
  by another unconfirmed renewal, and exact Status retry resumes renewal.
- Watchdog tests cover observed original identity, failed inspection and later
  cancellation retry, partial preparation failure, and structured FAILED with
  `WorkerReusable=false` retaining deadline cleanup.
- Renewed Prepare replay, Start and Start replay synchronize backend authority.
  The previous interrupted-renewal test now requires real cancellation
  acknowledgement with the original envelope, while still forbidding invented
  drain or capacity release.
- Both failure outcomes pass with an actual compiled `h3-encoder` CPU process,
  ProcessBackend inspection side channel, exact durable drain and resident
  readiness preserved.
- Actual mTLS -> UDS -> durable Runtime recovery closes the Worker journal after
  failed renewal and installs a terminal floor. A nonleader rejects; the leader
  cancels and drains the actual backend identity. Allocation lookup preserves
  the original checkpoint.
- Full `go test ./...` passes (ModelRuntime `51.060s`, Worker Agent `59.598s`).
  Related race suites pass for ModelRuntime (`75.257s`), Runtime transport
  (cached), member transport (`17.195s`), Worker Agent (`76.140s`) and Worker
  command (`17.922s`). The subsequent structured-failure watchdog test passes
  separately under race (`1.763s`); product code did not change afterward.
- Four PostgreSQL integrations pass (`19.727s`): authenticated terminal
  disposition, replacement-Runtime terminal recovery, lease expiry honoring
  latest renewal during cancellation, and compiled Worker bootstrap binding.
- `make lint` passes with zero issues. Linux arm64 Runtime/member tests pass
  with the compiled H3 command under `/tmp/vela-renewal-recovery-*` and image
  `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
  Both run as UID/GID 65534, without network/capabilities, with read-only
  root/binaries, private tmpfs and `no-new-privileges`.

## Evidence limits and next work

The candidate pair is in memory, not a new journal record. There is no new RPC
that discovers or enumerates the backend envelope; callers in these tests
already possess both signed envelopes. Automatic recovery for callers lacking
that envelope, durable renewal discovery and restart recovery beyond existing
historical drain/replacement proof remain open.

The compiled H3 process is CPU mock with synthetic device identity metadata and
opens no GPU. A backend/inspector that ignores context can remain in flight;
these tests do not prove generic physical containment. Cancellation, read-only
inspection, writer-drain proof and device reuse remain separate facts.

Physical replacement, successful terminal retirement and capacity reclamation,
pending input writers, durable sealed receipts and default Fleet durable
activation still need closure. No remote deployment, push, GPU use or Production
Gate acceptance occurred. The full correctness goal remains incomplete.
