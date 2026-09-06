# Local Worker journal bootstrap coordination

This increment follows `b1765c4` and connects the Registry first-use API to
actual offline Worker and Runtime journal preparation. Database remains **91**,
Worker journal 5, Runtime journal 4, materialization 2, launch/Fleet 2 and floor
RPC v1/v2. Local bootstrap operation format is 1. Production Gates remain `0/9`.

## Implemented boundary

`workerbootstrap.Prepare` takes the complete approved bundle, the local launch
manifest, node/actor labels, a verifier, a history bound and the local scratch
mount. It calls an `Authority` interface implemented by the existing
`fleet.Service`. It creates no directories, allocates no Runtime epoch and
starts no backend. This is an offline library, not a deployed node command.

`fleetcontroller.WorkerMemberLaunchManifest` derives the exact per-member launch
contract using the same builder as the Pod renderer. The bootstrap adapter
compares the complete encoded manifest, including commands, images, device and
member topology, Runtime routes and roots. It also checks the target node. The
Registry independently checks the complete bundle bytes against its approved
layout when consuming the claim. Local labels alone do not authenticate a node.

The small `workerjournal` adapter shares launch-to-Worker configuration with the
existing offline command. Core Worker journal logic still has no dependency on
ModelRuntime backends or Kubernetes. The bootstrap coordinator depends on these
adapters at the provisioning boundary.

The existing scratch mount must contain these five distinct private directories:

```text
bootstrap/
worker-admission/
runtime-admission/
inputs/
outputs/
```

The scratch root and children must have mode 0700 and belong to the effective
user. Trusted ancestry, held directory identities, private regular files,
single-link files, non-following opens and an exclusive operation lock are
checked. First use requires all five children to be empty and no additional
scratch history. Invalid preflight creates no operation and calls no Registry
authority. The local scratch mount maps only the filesystem locations; journal
scope continues to bind the approved topology. Cross-container mount activation
is not established by this test.

## Durable order and interruptions

1. Create and sync immutable `bootstrap/operation.json`, including a random
   request UUID, complete configuration digests and directory/lock identities.
   Hold its exclusive lock through preparation, recovery and reporting.
2. Recheck roots, then consume Registry first-use permission. Only a fresh,
   committed, exactly matching claim in this invocation permits initialization.
3. Initialize the real Worker journal and then the real Runtime journal.
4. Exclusively create and sync immutable `bootstrap/pair.json` containing the
   actual two journal IDs and scope digests.
5. Recover both journals using their ordinary validators and compare the saved
   identities before reporting the immutable pair to Registry.

No `Fresh=true` permission is serialized. A retained operation never calls Claim
again and never initializes or upgrades either journal. Complete recorded pairs
can recover and replay the receipt after response loss, preserving its first
database timestamp. Every retry revalidates the actual journals. Missing,
replaced, corrupt or locked state rejects; a changed actor or history bound also
rejects. Losing the entire local tree cannot bypass Registry member uniqueness.

An interruption before the pair record exists returns `ErrIncomplete` on
restart, even if both journal initializers actually finished. An interruption
after operation publication but before the first RPC is also incomplete; the
ordinary retry does not infer that permission remained unused. This deliberately
preserves uncertainty. There is no automatic partial-initialization repair or
reset operation in this increment. An incomplete claimed operation remains in
the database's pending bootstrap inventory.

Existing journal preparation releases each journal's lock after validation.
The operation lock serializes provisioners, not serving processes. Receipt
reporting is a trusted offline observation; it does not hold both serving locks
or prove writer exclusion. Eventual startup must validate the Registry-recorded
pair again while obtaining its own lifetime locks before readiness.

## Verification

- Full repository `go test ./...` passed. Subsequent coordinator changes passed
  focused race tests; Fleet renderer and Worker command race regressions passed.
- `make lint` and changed-file integration-tag lint passed with `0 issues`.
- Seven interrupted lifecycle boundaries, nine invalid claim responses,
  preflight rejection, same-process concurrency, held Worker/Runtime locks,
  missing/replaced state, full local state loss and immutable receipt retry.
- Three subprocess exits use the actual test executable and exit without
  cleanup after Worker preparation, Runtime preparation or pair publication.
  Recovery preserves file bytes and inode identities and never repeats Claim.
- PostgreSQL 17 integration with actual local journals passed both committed
  claim-response loss and committed receipt-response loss. It compares the
  database's journal IDs, scope digests and timestamp to the recovered files.
  The race-enabled package run completed in 9.763 seconds.
- Linux arm64 coordinator tests ran as UID/GID 65534, with no network or
  capabilities, a read-only root/binary, `no-new-privileges` and private tmpfs.
  Image: `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.
  Binary: `/tmp/vela-worker-bootstrap-linux.test`.
- Pod-rendered launch JSON matches the shared builder for the H3 and CPU media
  bundles. The renderer's digest format and generated API/schema outputs did
  not change. `git diff --check` passed.

These interruption tests do not emulate physical storage power loss. Existing
journal components retain their separately tested file-durability behavior.
The integration fixture has approved GPU-shaped metadata but invokes no GPU,
driver executable, Runtime backend or Pod. Nothing was pushed or deployed.

## Next lifecycle work

Add authenticated node transport and operation-history inspection, then an
independently authorized reconciliation lifecycle for incomplete initialization.
Normal serving must bind both lifetime-locked journals to the recorded pair.
Only after that should Fleet emit and activate the persistent bootstrap and
serving configuration. The current recurring init scripts do not invoke this
library or create its new layout.

Unknown historical writers, pending Runtime recovery, failed-backend containment,
sealed receipt recovery, bounded checkpoint reclamation and command-level
successful terminal retirement remain open. This increment does not complete
the overall architecture or production acceptance review.
