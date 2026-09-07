# Node Runtime startup association ledger

This increment follows `5a93719` on `feature/vela-mock-hardening`. The Linux
`RuntimeStartupLedger` durably associates an authenticated startup declaration
with its independently retained original namespace owner. It does not issue
startup permission. PostgreSQL 94, Worker journal 5, Runtime journal 6, Registry
binding 1, release bundle 3 and Production Gates `0/9` remain unchanged.

## Persistence and process ownership

Initialization requires an existing empty root-owned `0700` directory and
effective UID 0. Recovery opens existing state only. The root-owned `0600`,
single-link JSONL file has a lifetime exclusive lock and a schema-1 header
binding its UUID, Node identity and directory/file device and inode. Canonical
bounded records reject duplicate/unknown fields, partial tails, CRLF, visible
replacement and ownership changes. Live operations recheck the file digest,
size and paths. An observed integrity or uncertain append failure prevents
further use of that handle. Recovery parses every byte and reconfirms file and
directory durability before returning.

`Record` verifies the request against an opaque verified launch plan, including
the signed Registry binding digest, Node, Runtime journal UUID/scope and launch
digest. Trusted Pod lookup and repeated CRI/native-task/process observations
identify the authenticated non-root namespace PID 1. The ledger retains an
independent pidfd and appends/fsyncs the immutable operation, request, Registry
binding, owner observation and Pod revision before returning. Duplicate journal
registration rejects, including after restart or an observed owner exit.

`RecordExit` accepts only the kernel exit event on that original retained pidfd.
Request/CRI connection loss cannot create exit. The durable exit remains
readable after reopening. An unresolved record reopened without its live pidfd
returns `ErrRuntimeNamespaceOwnerLost`; absent process metadata cannot
reconstruct the original handle. No method resets Runtime state, retires a
backend or releases a DeviceSet.

The ledger accepts at most 1024 startup records and 8 MiB of total bytes, with
individual lines below 64 KiB. Append admission reserves bytes for pending
owners' eventual exit entries. These are deliberate bounds without compaction;
capacity exhaustion rejects new registrations. The follow-up saturation
experiment below validates retained exit persistence after admission stops.

## CPU validation

`TestRuntimeStartupLedgerBeforeActualFactory` runs real `StartRuntimeServer`, a
prepared Runtime journal, signed Registry fixture and concrete Node exchange
inside an independent non-root PID namespace. The root parent authenticates
the caller, records the Node association, and independently checks that the
factory marker is absent and the known fixture Runtime journal is locked.
Only the separately issued test mock permit dispatches one factory. Denial,
lost response and record failure dispatch none. All cases reopen Runtime in
recovery-only mode and persist the exact original owner's observed exit.
CRI/native-task and Kubernetes metadata in this exchange are explicit fixtures;
this is not a production Node authorization transaction.

Five further mandatory tests cover concurrent duplicate registration, independent
owner lifetime, durable exit/reopen, lost owner handles, unbound requests,
uncertain append boundaries and missing/corrupt/replaced/untrusted state.

Validation passed on Linux/arm64 in the isolated CPU image
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`,
using Docker `28.3.2`, containerd `v2.3.1` and runc `1.4.2`:

- Complete Node wrapper: 53 mandatory main tests plus both volatile-state-reset
  scenarios, no skips, 158.97 seconds.
- Final static Linux race binary: all seven ledger main tests plus authenticated channel,
  actual CRI caller, planned caller and three namespace-owner regressions;
  13 explicitly selected main tests passed without skips or race reports.
- Full `go test ./...` and `go vet ./...`.
- Linux integration-tag vet and pinned golangci-lint `v2.13.1` for
  `internal/nodeagent` and `cmd/vela-node-agent`, zero lint issues.
- `git diff --check`.

The race binary used Go `1.26.7` builder image
`sha256:6ef6e30f0ea5c384f6d111cf856e024e3086bbdcb1779da3f3b3fbba0aea53d2`
and `-race -ldflags '-linkmode external -extldflags=-static' -p=2`.
Static glibc NSS linker warnings are test-build limitations, not production
linking evidence. Tests used no network or GPU inside the sandbox.

Reproduce the complete CPU wrapper with:

```sh
VELA_TEST_CONTAINERD_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/nodeagent \
  -run '^TestRuntimeContainerdSandbox$' -count=1 -v
```

Logs are retained under `/tmp/vela-startup-validation.syiSMk`. SHA-256:

| File | Digest |
| --- | --- |
| `ledger-full-cpu.log` | `efec447b5f5ce76cd4b6d24473153bb5d494c3e3430ddd6c09b84cf43276edca` |
| `ledger-race-final-selected.log` | `84f69b54f769273ab989a47cabffe3beb222b7fe07d424ef67aed016b014fad4` |
| `ledger-unit.log` | `c8c6f30ace3740f347f19cf54c07d16f5ad4dccf3c6434a15ead08d79b90f4f8` |
| `ledger-linux-lint-final.log` | `e92606b0bf483111dff0a120c315ea165821348f31365020e2468a0059095c47` |

## Capacity follow-up after 0cbca3e

`TestRuntimeStartupLedgerReservesExitCapacity` fills real ledger storage using
synthetic signed history and bounded CRI strings whose JSON encoding expands.
It rejects further append at 504 records and approximately 4.49 MB of persisted
bytes, before reaching either the raw 8 MiB limit or the record-count limit.
That count is fixture-specific, not a production capacity estimate. Rejection
leaves the ledger usable. A real retained owner remains live while paused;
after explicit process termination, its exact exit persists and survives reopen.

The initial race run exceeded the helper's 15-second test timeout while filling
history. The final test pauses that helper during fixture construction and
explicitly confirms that pause cannot produce exit. All seven ledger main tests
pass within the final 13-test race selection without skips or race reports.
The explicit selection excludes subprocess-only helpers. This corrects the test
lifetime; production implementation is unchanged from `0cbca3e`.

The complete wrapper passes 54 mandatory main tests and both state-reset
scenarios without skips in 161.19 seconds. That wrapper ran before the
helper-pause adjustment; the final 13-test race run covers the adjusted test.
Linux integration-tag Node vet and golangci-lint also pass. Follow-up logs are
retained beside the original logs; `ledger-capacity-full-cpu.log` has SHA-256
`02a974c78ee8a1b05834c1afa1bb03ae5c0d0d9ca6c6e473e9468f1d5ef571d8`.
`ledger-race-final-selected.log` records the final race run.

These bounds still require a future retention/compaction policy with an external
durable authority. Deleting history or starting a new ledger does not establish
permission to reuse a prior Runtime journal or release its owner.

## Remaining authority

No production Node socket endpoint or authorization issuer exists. Registration
does not independently authenticate the original held Runtime journal, effective
launch or current Fleet activation. The public planned-caller API still requires
the exact manifest payload; only the ledger's private path accepts a separately
validated startup declaration. A returned record cannot be exchanged for a
production permit through this API.

Root-owned storage is trusted. Complete administrative rollback or truncation
to an older valid record boundary is not independently detectable. Reload checks
the stored protobuf's canonical structure and internal binding consistency;
it does not reverify the signature against a new keyring. Signature verification
occurs when the opaque launch plan is constructed before registration.

Actual journal/launch/current-activation authority, Node restart availability,
exact-owner retirement and execution/device quiescence remain required before
production permission or replacement. CPU mock permits close none of the nine
Production Gates.
