# Frozen Runtime startup manifest

This increment follows `e89bbc1`. `StartRuntimeServer` accepted its configuration
by value but retained caller-owned manifest slices. The first AUX backend factory
could mutate the caller's second runtime or slices shared by both runtime entries.
The second factory then received a different command/environment/device/root
configuration from the canonical manifest already hashed into the startup journal.

`StartRuntimeServer` now uses the existing deep-copy helper immediately after
manifest validation and before any epoch-store or backend callback. Device,
member and runtime slices are copied; each runtime receives independent command
and environment slices even when the caller reused backing arrays. Journal
creation and backend configuration derivation use this one startup snapshot.

The regression performs two synchronous experiments through actual Server
assembly and its durable journal, using CPU fake backends. In the first factory
it changes the caller-owned second runtime's command, environment and scratch
root, plus the shared local device epoch. The separate alias experiment mutates
the first factory's runtime command/environment where the original AUX entries
shared the same slices. Both cases failed before the fix with:

```text
factory 2 configuration differs from the durable launch preimage
```

Both now pass. Every callback checks its complete launch/runtime configuration
against an independently decoded preimage and the recorded SHA-256. Both AUX
factories must actually run; the test does not pass by denying all startup.

This is ownership of configuration inside a trusted process, not authentication
of an arbitrary injected factory or executable. Callers must still avoid
concurrent mutation while the constructor reads its arguments. The startup
digest remains an intent record; it is not proof of actual executable bytes,
loaded memory or current authorization.

## Lifecycle evidence collection

Repeated Linux validation exposed a separate fixture error: whole-directory
`docker cp` could sample a still-publishing
`journal/.execution-admission-<id>-<id>.tmp` and return a truncated tar entry.
The diagnostic observed a declared 636-byte temporary entry with `unexpected EOF`.
Separating stdout from stderr did not resolve that race, as recorded in
`/tmp/vela-runtime-launch-snapshot-lifecycle-stdout.log`.

The test now waits for the existing phase-specific `owner-ready` gate before
collecting the first archive. That gate follows the initializing phase's durable
startup record or the idle/admitted/closed-idle phase's completed setup. It does
not wait for owner exit: the tests still observe live escaped writers, request
the crash, verify exact process/container outcomes and compare journal history.
Tar data is read only from stdout, with separate diagnostics. Truncated archives
remain fatal; no partial journal is accepted and no parser error is retried away.

## Validation

The two new snapshot regressions pass. Full repository unit tests pass, including
ModelRuntime in `66.483s`; ModelRuntime and Runtime command race tests pass with
no race reports (`105.110s` / `9.897s`). Full vet/lint reports `0 issues`.
All nine Linux CPU lifecycle scenarios pass in three consecutive runs after the
collection fix (`10.21s`, `9.69s`, `9.71s`; package `30.333s`), with no skips.
Linux integration-tag ModelRuntime lint also reports `0 issues`. The Linux runs
use the existing local image
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`
with `--pull never`, bounded container resources and no network. Reproduce with:

```sh
go test ./internal/modelruntime \
  -run '^TestRuntimeServerFreezesLaunchBeforeBackendCallbacks$' -count=1 -v
VELA_TEST_RUNTIME_CONTAINER_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/modelruntime \
  -run '^TestRuntimePIDNamespaceContainsBackendWriters$' -count=3 -v
```

Logs use `/tmp/vela-runtime-launch-snapshot-*.log`. Task-created containers,
volumes and test binaries are removed by the harness; the two pre-existing
stopped containers are preserved.

No GPU, external H3 driver or remote deployment is used. PostgreSQL 94, Worker
journal 5, Runtime journal 6, Registry binding 1 and Production Gates `0/9` are
unchanged. Independent retirement and normal durable Runtime restart are still
unimplemented, alongside executable/message binding and current startup grants.
