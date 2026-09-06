# Offline Runtime journal preparation and command recovery

Local CPU/mock increment over `ddcc9a2`. Runtime and Worker journals remain
**4**, materialization journal **2**, database **90**, and launch/Fleet **2**.
Production Gates remain **0/9**. No GPU or remote deployment.

## Behavior

`PrepareExecutionJournal` validates a trusted launch manifest and public
StageAuthority verifier, then opens the journal using the same ownership,
signature, filesystem and exclusive-lock checks as Runtime startup. It closes
the store before returning its journal ID, schema, scope, high-water mark,
installed floor and retained/pending execution counts. It allocates no Runtime
epoch and starts no backend. Its result is a local observation, not an execution,
retirement, readiness or Production Gate receipt; it holds no lock afterward.

The existing `vela-model-runtime` executable now accepts the explicit `journal`
subcommand. It loads only the supplied manifest, verifier keyring and state
directory, independently of ordinary server environment settings. Actions are
`initialize`, `recover`, `upgrade-v2` and `upgrade-v3`. Missing or invalid actions,
unknown flags and positional arguments reject. Running with no arguments retains
the server path; unknown top-level arguments now reject instead of being ignored.

Ordinary server configuration optionally accepts
`VELA_MODEL_RUNTIME_EXECUTION_STATE_DIRECTORY`. When present, it enables durable
execution floors derived from launch topology and recovers existing state only.
There is no recurring initialization or implicit upgrade environment flag. An
unset directory retains the existing configuration; deployment templates and
default Worker retention policy have not been changed.

## Operator sequence

First initialization requires independently authorized first use of the exact
Worker/member/device scope and an already provisioned private directory. An
empty or missing directory, copied manifest, repeated Pod init, or lost command
output does not establish that authorization. The command enforces local
ownership and one-time initialization; it does not obtain new Fleet authority.

For that authorized first initialization:

```sh
vela-model-runtime journal --action initialize \
  --launch-manifest-file /trusted/runtime/launch.json \
  --verifier-keyring-file /trusted/runtime/verifier.json \
  --directory /persistent/runtime/execution-journal
```

For ordinary offline recovery of the same provisioned state:

```sh
vela-model-runtime journal --action recover \
  --launch-manifest-file /trusted/runtime/launch.json \
  --verifier-keyring-file /trusted/runtime/verifier.json \
  --directory /persistent/runtime/execution-journal
```

For serving, set `VELA_MODEL_RUNTIME_EXECUTION_STATE_DIRECTORY` to that directory
alongside the existing required Runtime environment, then run the executable
without arguments. Startup reacquires and revalidates the journal before epoch
allocation and backend startup. Offline recovery rejects while a Runtime owns
the journal. It may sync recovered state and remove unpublished temporary files;
it is not a strictly read-only inspection command.

Use `upgrade-v2` or `upgrade-v3` only for the corresponding validated legacy
journal migration. Existing restrictions/history survive and no writer proof is
invented. Ordinary recovery rejects legacy versions. Current schema recovery
can resolve a lost upgrade result. A lost initialization result also requires
recovery; repeating initialization rejects, and files must not be deleted to
force it. Schema 1 still requires separate reconciliation.

## Validation

Passed full `go test ./...`, `go test -race ./cmd/vela-model-runtime
./internal/modelruntime`, `make lint` and `git diff --check`. The additional
populated-history assertion passed its focused race check after the full run.

Tests cover explicit action selection, absent state, repeated initialization,
changed Worker scope, corrupt state, cancellation, output failure followed by
recovery, exclusive ownership against a running server, explicit schema-2/3
upgrade and unchanged journal identity. Offline preparation succeeds even when
the configured backend executable and model directories do not exist.

The actual command/process lifecycle test first rejects normal startup against
empty state, initializes offline, starts a CPU driver subprocess with epoch 1,
serves its private UDS, shuts down, and recovers offline. No backend initialization
event occurs before serving. A populated standalone Supervisor journal also
passes offline preparation before Server recovery: highest 10, floor 11, one
retained/pending execution, unchanged bytes, unavailable readiness and rejection
of stale authority after the new Runtime epoch.

Linux arm64 passes `^(TestJournalCommand|TestRun|TestLoadCommandConfig)` in the
command binary, and `^(TestExecutionJournalPreparation|TestRuntimeServerRecoversExistingSupervisorJournalBeforeNewEpochs)`
in the Runtime binary, with UID/GID 65534, no network, read-only root/binary
mounts, all capabilities dropped, no-new-privileges and a private `/tmp` tmpfs.
Image: `sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`.

Fleet first-use provisioning, default deployment activation, durable Worker
assembly, historical writer/receipt recovery, failed-backend containment and
bounded checkpoint reclamation remain open. These command tests do not claim a
deployed automatic retirement workflow or sustained Worker throughput.
