# Durable backend startup restriction

This increment follows `44e48fd`. Runtime journal schema advances from 5 to 6;
PostgreSQL remains 94, Worker journal 5 and Registry binding 1. Production Gates
remain `0/9`. This is a startup safety restriction, not completed restart
availability or physical retirement.

The subsequent [Node CRI observer](node-runtime-container-evidence-2026-09-06.md)
adds bounded container status reads. It does not yet provide the independently
verified containment binding or retirement required by this checkpoint.

## Problem and resulting behavior

The prior CPU experiment showed surviving writers during failed initialization,
idle residency and successful idle `Close()`, all with zero pending Stage
executions. A replacement Runtime could start both AUX drivers beside them.
Execution history alone therefore could not authorize backend replacement.

Schema 6 retains one member-wide lifecycle alongside the original execution
history:

| State | Creation | Subsequent backend startup |
| --- | --- | --- |
| `UNSTARTED` | Authorized first-use journal initialization | Persist intent before entering the first factory |
| `UNRESOLVED` | UUID v4, SHA-256 of canonical launch manifest, UTC time | Process-free recovery only |
| `LEGACY_UNKNOWN` | Explicit validated upgrade of schema 2, 3, 4 or 5 | Process-free recovery only |

`StartRuntimeServer` holds the original journal lock from validation through
epoch allocation and startup. It persists the intent once before the first
configured factory, covering both AUX profiles even when only one initializes.
Directory-sync errors and cancellation observed after persistence dispatch no
factory. Failed startup, ordinary Close and successful per-Stage drain never
clear the record. The original process may continue normal execution under its
startup intent; reopening it cannot recover that permission.

Recovery retains fresh endpoint epochs, authenticated discovery, signed history
and floor installation. It loads no model and denies readiness and new Prepare;
Start, renewal and Seal cannot infer execution from the endpoint. Pending
historical executions retain `ErrExecutionDrainUnproven`; otherwise the refusal
is `ErrBackendIncarnationUnproven`. Prepare maps both to `REJECTED`.

Ordinary serving refuses older schemas. Explicit `journal --action upgrade-v5`
joins the existing schema-2/3/4 operations and preserves validated journal IDs,
scopes, floors, original/renewal candidates and drain/non-admission history.
Even empty legacy history becomes unknown, never fresh. Offline status reports
backend lifecycle independently of retained and pending execution counts.

## Verification

The new tests exercise actual server assembly and owner-checked UDS. Three
startup boundaries retain exactly one intent: first factory failure, second
factory failure and idle Close. Recovery starts zero additional factories,
remains discoverable and rejects execution while preserving journal bytes.

Three persistence interruptions cover rename-before-directory-sync,
sync-then-error and sync-then-cancellation. All dispatch zero factories and
retain the observed intent after reopening. Eight malformed records reject
before factories or history mutation. Explicit legacy upgrades preserve prior
proof and suppress model startup. An actual Prepare/Start/Seal cycle records a
durable Stage drain and still cannot retire backend ownership on restart.

The existing Docker harness now also has an independent fresh-journal positive
control. It starts both drivers; every unresolved replacement starts zero.
The idle-Close case retains the old living owner and writer while a separate
PID 1 namespace attempts replacement. Journal bytes remain unchanged and the
original writer continues growing its file. Thus zero replacement drivers is
not explained by a broken test command or by stopping the old writer.

```sh
go test ./... -count=1
go test -race ./internal/modelruntime ./internal/workerbootstrap \
  ./cmd/vela-model-runtime -count=1 -timeout=8m
go test -race ./internal/stageworkeragent ./internal/stageworkermembertransport \
  ./internal/modelruntimetransport -count=1 -timeout=8m
go test -race ./internal/modelruntime \
  -run '^TestRuntimeServer(DrainDoesNotRetire|RetainsBackendOwnership|BackendStartupPersistence|RejectsMalformedBackendLifecycle)' \
  -count=1 -v
go test -tags=integration ./internal/integration \
  -run '^(TestWorkerBootstrap|TestLocalWorkerBootstrap|TestStageTerminal|TestPostgresTerminalHistory)' \
  -count=1 -timeout=8m
VELA_TEST_RUNTIME_CONTAINER_IMAGE=sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73 \
  go test -tags=integration ./internal/modelruntime \
  -run '^TestRuntimePIDNamespaceContainsBackendWriters$' -count=1 -v -timeout=3m
go vet ./...
```

Full unit and related race suites pass. After those suites, the additional
completed-Stage-drain test passes with the other lifecycle tests under race.
The PostgreSQL selection covers 27 top-level tests: 26 passed in the broad run;
the Registry-binding command test then passed in a focused rerun (`5.471s`
package time) after correcting its old assertion that idle startup must change
no journal bytes. It now permits only the validated lifecycle addition and
continues comparing all other local and Registry history exactly.

All nine actual Linux container scenarios pass (`32.291s` package time).
Environment: Docker `desktop-linux`, Engine `28.3.2`, Linux arm64 using the local
immutable image above. The harness compiles the current test binary, uses
non-root CPU-only containers, verifies persistent endpoint epoch increases and
removes its exact temporary containers and volumes. This does not execute the
production H3 image or use GPU resources.

Pinned `golangci-lint v2.13.1` passes standard `run ./...` and
`run --build-tags=integration` over Runtime, Worker bootstrap, Worker agent,
member transport, Runtime transport and the Runtime command. The full tagged
sweep reports 82 findings (50 errcheck, 4 staticcheck, 28 unused) outside this
increment. `run --build-tags=integration --new-from-rev=HEAD ./...` reports zero
new findings against parent `44e48fd`. Full tagged lint is not claimed clean.
Diff checks pass; generated protocol/database contracts are unchanged.

## Remaining architecture work

The UUID and digest are durable startup intent. They do not establish the actual
container ID, node incarnation, PID 1 ownership, permitted external writers or
device quiescence. At this checkpoint Node Agent assembly had no trusted CRI/container
observation adapter. There is no backend retirement or reset operation.
Consequently every durable restart after a backend startup attempt remains
recovery-only, including after a successful Close or externally observed PID 1
exit. The restriction deliberately leaves that availability gap explicit.

The next implementation must bind startup to an independently observed exact
containment owner before factory dispatch and retain independent retirement
evidence for that same owner before authorizing replacement. Missing container
metadata, a new namespace, PID disappearance, process-group signaling and
journal-lock acquisition cannot stand in for the proof. Namespace inode reuse
was observed in the earlier experiment. Node reboot, lost observation replies,
agent restart and metadata garbage collection require explicit recovery rules.

That protocol remains separate from per-Stage drain, input-writer exclusion,
sealed receipt recovery, durable unhealthy Worker state, bounded history
reclamation, renewal write cost and Fleet durable activation. No GPU acceptance,
remote deployment, push or Launch Receipt is included. The overall correctness
and architecture goal remains active.
