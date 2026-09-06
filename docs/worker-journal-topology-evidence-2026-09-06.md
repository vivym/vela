# Worker admission journal topology binding

This CPU-only increment follows `1770bc9`. It changes Worker admission journal
schema from 4 to 5. Runtime journal remains 4, materialization remains 2,
database remains 90, launch/Fleet remain 2 and floor RPC remains v1/v2.
Production Gates remain `0/9 PASS`.

## Reproduced failure

`NewFileAssignmentAdmission` previously bound the Worker ID/epoch, local member,
filesystem identities, lock and record capacity, but no complete topology.
An empty journal, or retained assignment history without a floor, reopened under
changed device epochs, member identity digests or device subset digests while
the Worker ID/epoch stayed unchanged. The initial six-case regression failed
twice with `changed topology recovered under unchanged Worker epoch`:

```sh
go test ./internal/stageworkeragent -run '^TestAssignmentAdmissionRecoveryRejectsChangedTopologyWithoutFloor$' -count=2
```

Existing floor recovery already checked signed topology. Retained assignment
recovery only checked Worker/local-member identity and authenticated envelopes,
so it did not independently close the no-floor case. Reopening with changed
configuration was demonstrated; unauthorized backend execution was not claimed.

## Durable scope

New journals persist a SHA-256 scope over the canonical structured Worker
ID/epoch, local member ID, complete device IDs/epochs and DeviceSet digest,
membership digest, and complete member IDs/epochs, identity digests and subset
digests. Device/member ordering and the number of resident routes do not change
scope. Runtime identity, epoch, residency and profile are excluded so a trusted
replacement owner can recover historical restrictions. Execution still requires
current signed authority matched to trusted Runtime routes.

Construction rejects incomplete membership, conflicting per-route member
identity/subset configuration, malformed/duplicate IDs, invalid epochs, and
inconsistent complete topology before creating any file or ownership marker.
It retains the launch contract's 64-device/64-member bounds. The configuration
and scope are detached copies. Every reopen compares the persisted scope, even
when the journal is empty, and independently matches retained signed execution
topology. A replaced scope digest cannot hide inconsistent signed history.

The fingerprint is a configuration binding within the existing trusted local
filesystem contract, not a signature authenticating arbitrary disk rewrites.
It proves neither writer completion nor backend readiness.

## Compatibility

Ordinary recovery rejects schemas 2/3/4 and missing/mismatched scope. Explicit
`AssignmentAdmissionConfig.UpgradeV2`, `UpgradeV3` or `UpgradeV4` can upgrade only
the corresponding legacy schema. The retained signed floor must independently
establish the complete original topology, including every member subset, and
all history and retirement proof must pass validation before persistence.
Upgrades preserve evidence, journal/root/lock identities and watermark; they
add only schema 5 and its scope. Initialization and upgrade are mutually
exclusive, and only one upgrade flag is permitted.

An old empty journal or one with only assignment envelopes lacks a complete
member-subset witness. It is deliberately not upgraded from replacement
configuration. Keep its state and scratch intact; a separately designed
authoritative reconciliation path is still required. Do not erase it, reset
its watermark or enable first initialization to bypass this boundary.
Schema 1 remains unsupported. Older binaries reject schema 5; preserve the new
reader for forward recovery instead of stripping fields for rollback.

## Verification

Passed:

- `go test ./...`.
- `go test -race ./internal/stageworkeragent`.
- `make lint`, including `go vet ./...`, with `0 issues`.
- `go test -race -tags=integration ./internal/integration -run '^(TestStageTerminalHistoryCoversAllocatedUndeliveredRetry|TestStageTerminalRecoveryThroughReplacementRuntimeRestoresCapacity)$' -count=1`.
- Linux arm64 non-root focused admission, input-drain upgrade, schema-3
  retirement upgrade and floor recovery tests.

Regression coverage includes empty/pending journal topology drift, malformed
configuration rejected before writes, reordered configuration, replacement
Runtime routes with byte-identical retained history, independent signed-history
validation, explicit schema-2/3/4 floor-backed upgrades, refusal without a signed
witness and schema-4 retirement proof preserved byte-for-byte after upgrade.
Rejected recovery and upgrade leave the journal bytes unchanged.

The existing PostgreSQL replacement regression still proves automatic recovery
through current Runtime owners, durable RETIRED, actual CPU mock readiness,
new-epoch registration, restored capacity and durable `NO_WORK` Acquire.

Linux binary: `/tmp/vela-non-admission-linux/worker-topology.test`, compiled with
`CGO_ENABLED=0 GOOS=linux GOARCH=arm64`. The container used image
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`,
no network, read-only root/binary, UID/GID 65534, all capabilities dropped,
`no-new-privileges`, and a private `/tmp` tmpfs.

No protocol or generated contract changed. These checks do not exercise a GPU
or establish any production receipt. Worker offline preparation, approved
launch/peer binding, default durable Stream assembly and first-use provisioning
remain open, along with unknown writer recovery and bounded reclamation.
