# Offline Worker admission journal preparation

This CPU-only increment follows `2c1f2ce`. Worker journal remains schema 5,
Runtime journal 4, materialization 2, database 90, launch/Fleet 2 and floor RPC
v1/v2. Production Gates remain `0/9 PASS`.

## Command and ownership

`vela-stage-worker-agent journal` now supports explicit `initialize`, `recover`,
`upgrade-v2`, `upgrade-v3` and `upgrade-v4` actions. It requires a trusted private
launch manifest, a public StageAuthority verifier keyring, an existing private
admission-state directory and an explicit history bound of 1-64 records.
No arguments still selects the existing serving path; other arguments reject.

The command validates the launch manifest through the existing ModelRuntime
contract. It binds the complete Worker/member/device topology, including remote
identity/subset digests. It takes input/output roots from the manifest and
requires every local resident Runtime to share that pair. This supports the
certified AUX shared slot and rejects ambiguous independent root pairs before
any journal creation. Preparation needs existing private input/output roots;
it does not create missing directories.

Launch manifests describe only local Runtime routes. Offline preparation
therefore uses topology-only member bindings with no runtime identity,
residency, profile or observed epoch. An epoch floor is never promoted to an
observed epoch. These temporary bindings are internal to preparation; no
execution handle is returned. The core Worker package has no new dependency on
ModelRuntime backend implementation; launch adaptation belongs to the command.

`PrepareAssignmentJournal` reuses `NewFileAssignmentAdmission` for exclusive
locking, canonical parsing, scope/signature/proof validation and durable
recovery. It closes the gate before returning. Initialization is explicit and
requires empty state and content roots; recovery never infers initialization
from absent files. Schema-2/3/4 upgrades retain the signed-original-topology
requirements established by the prior increment. A failed or absent response
can follow persisted initialization; the next action is recovery, not another
initialization attempt.

## Result boundary

The JSON result reports the action, journal ID/version/scope, watermark/floor,
retained execution count, unproven input count and INTENT/READY/RETIRED counts.
It contains no customer execution content, raw signed envelopes or keys.
Successful preparation means local history was validated and its lock released.
It does not mean the Worker is ready, input writers stopped, Runtime execution
is permitted or scratch can be deleted. Even a READY retirement remains READY:
this command does not run its deletion coordinator.

The command loads no serving environment or Artifact Store credentials,
contacts neither Control nor peers, allocates no Runtime epoch and starts no
model/backend. Default Worker serving assembly remains unchanged and does not
yet open this admission journal or create a Durable Stream.

## Verification

Passed:

- `go test ./...`.
- `go test -race ./internal/stageworkeragent ./cmd/vela-stage-worker-agent`.
- `make lint`, including `go vet ./...`, with `0 issues` after correcting two
  error-message capitalizations and documenting an intentional nil-context test.
- Focused command tests after the error-message corrections.
- Linux arm64, non-root focused command and preparation tests, including the
  command's actual `main` dispatch in a separate process.

Coverage includes exclusive ownership against a live admission gate, idempotent
recovery, pre-canceled initialization without writes, original floor-backed
schema-2/3/4 upgrades, refusal to invent missing topology, pending input history
with and without completion, and byte-identical recovery using topology-only
bindings. Runtime processes are closed before the retirement inspection test;
INTENT, READY and RETIRED counts are recovered without phase advancement or
scratch deletion. READY is produced by cancellation after its durable publication
and before deletion, so the test separates an incomplete caller response from
actual persisted state.

Command tests use a nonexistent backend and missing ordinary serving identity.
They validate remote-member subset rejection without journal mutation, shared
versus different AUX roots, epoch floors remaining unobserved, and recovery
after initialization output loss.

Linux binaries, built with `CGO_ENABLED=0 GOOS=linux GOARCH=arm64`:

- `/tmp/vela-non-admission-linux/worker-journal-command.test`
- `/tmp/vela-non-admission-linux/worker-journal-preparation.test`

Both ran with image
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`,
no network, read-only root and binary, UID/GID 65534, no capabilities,
`no-new-privileges` and a private `/tmp` tmpfs.

No protocol/generated contract changed. No database behavior changed, and this
increment did not rerun the PostgreSQL integration suite. GPU, deployment,
default durable assembly, Fleet first-use provisioning, unknown writer recovery,
sealed receipt recovery and bounded checkpoint reclamation remain outside this
increment. These checks establish no production Launch Receipt.
