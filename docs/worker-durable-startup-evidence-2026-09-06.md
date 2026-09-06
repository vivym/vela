# Explicit durable Worker serving startup

This CPU-only increment follows `44e91df`. Worker journal remains schema 5,
Runtime journal 4, materialization 2, database 90, launch/Fleet 2 and floor RPC
v1/v2. Production Gates remain `0/9 PASS`.

## Configuration and startup

`vela-stage-worker-agent` can now select durable serving with this complete set:

- `VELA_STAGE_WORKER_LAUNCH_MANIFEST_FILE`: approved private launch manifest.
- `VELA_STAGE_WORKER_ASSIGNMENT_STATE_DIRECTORY`: existing private admission
  journal directory, disjoint from other Worker state and input/output roots.
- `VELA_STAGE_WORKER_ASSIGNMENT_MAX_RECORDS`: original history bound, 1-64.

All absent preserves the existing serving path; partial settings reject.
Serving never initializes or upgrades history. The offline journal command
remains the separate first-use or explicit migration path.

Startup compares the serving configuration with approved Worker/member/device
topology, member SPIFFE identity digests and scratch roots. `DEVICES_JSON` is a
local device set and is checked against `LocalDevices`; journal admission scope
still binds the complete `Devices` set. Every local Runtime must share the
approved input/output pair. It validates existing journal history before
creating ordinary Worker directories or configuring the Artifact Store.

Preflight releases its temporary lock. After local UDS discovery and authenticated
peer discovery, startup reopens the journal with actual Runtime bindings and holds
its lock for the serving lifetime. Exact route, residency, profile and topology
must match; observed epochs must be strictly greater than launch floors. Missing,
duplicate, unknown-field, stale and unapproved identity sets reject. A floor is
never substituted for an observed epoch.

Member-server publication still precedes peer dialing, avoiding a cyclic startup
dependency. The deterministic Leader requires every approved member. It supplies
all observed Runtime bindings and one current floor reader per member, selected
by the smallest residency ID. Discovery is observation, not readiness, durable
ownership or writer drain. A subsequent Runtime identity change requires Worker
topology reconstruction.

## Serving composition and recovery

The Leader constructs a Durable Stream with the same admission gate, Runtime
Agent, input resolver, materialization state, terminal retirement coordinator and
authenticated Control terminal-history reader. The existing materialization
`RetainScratchRetirer` stays in place; terminal cleanup requires the combined
coordinator's complete exclusion and filesystem proof.

A follower opens its own durable admission journal but has no Leader terminal
history or retirement coordinator. Remote bindings remain topology-only, with no
invented runtime observations. Both preflight and final open reject retained
Leader assignment, floor or retirement history on a follower. Startup rollback
and ordinary shutdown release the admission lock; a failed close retains the
owner reference for retry.

The actual serving entrypoint is tested after a retained signed assignment and
original Acquire UUID. Its first Control operation confirms zero capacity, then
queries the original history before readiness or Acquire. An injected unavailable
history response keeps capacity unusable and preserves journal bytes. This test
establishes startup ordering and failed recovery, not successful retirement
through the command.

## Verification

Passed during this increment:

- `go test ./...`.
- `go test -race ./cmd/vela-stage-worker-agent ./internal/stageworkeragent`.
- Focused PostgreSQL integration with race checking:
  `TestStageTerminalHistoryCoversAllocatedUndeliveredRetry` and
  `TestStageTerminalRecoveryThroughReplacementRuntimeRestoresCapacity`.
- `make lint`, including `go vet ./...`, with `0 issues` after error-message
  capitalization fixes; focused command tests passed after those fixes.
- Linux arm64 non-root command tests matching `TestDurableWorker`,
  `TestLoadConfigDurable`, `TestProductionRuntime` and `TestMultiMember`.
- `git diff --check`.

Linux execution used `/tmp/vela-non-admission-linux/worker-durable-startup.test`,
built with `CGO_ENABLED=0 GOOS=linux GOARCH=arm64`, and image
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
The container had no network, a read-only root and binary, UID/GID 65534, no
capabilities, `no-new-privileges` and a private `/tmp` tmpfs. The final two changes
after Linux execution only lowercased error messages; ordinary lint and focused
command tests passed on that final source.

Coverage also includes lock lifetime, construction rollback, absent state/input
directories rejecting before external configuration without recreating paths,
configuration drift, incomplete settings, overlapping directories, two-member
mTLS-to-UDS discovery, authenticated but unapproved peer profiles, and follower
composition. Successful terminal cleanup and restored CPU mock capacity remain
covered by the explicitly assembled PostgreSQL library fixture.

## Remaining boundary

Fleet does not yet provide the Worker manifest, initialized journals or serving
activation settings. This is opt-in command assembly, not deployment activation
or a complete first-use lifecycle. Unknown historical writers, pending Runtime
recovery, sealed receipt recovery, bounded record reclamation and containment
after failed backend shutdown remain open. Expanded integration-tag lint still
has 82 previously recorded findings in untouched files; ordinary lint success
does not establish that broader lint result. No GPU, deployment or production
Launch Receipt is claimed.
