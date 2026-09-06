# Explicit ModelRuntime driver environment

This increment follows `423c354`. `NewProcessBackend` previously appended the
declared launch environment to `os.Environ()`. An otherwise identical manifest
therefore launched different effective configurations when the Runtime parent
or image environment changed, and passed undeclared variables to its driver.

## Contract and compatibility

The driver now starts with exactly its declared environment plus these fixed
protocol entries:

```text
VELA_MODEL_DRIVER_PROTOCOL=stdio-json-v1
VELA_MODEL_DRIVER_INSPECTION_FD=3
VELA_MODEL_DRIVER_DRAIN_FD=4
```

An omitted environment gives the driver only those three entries. A declared
empty value remains empty. Fleet bundle validation, launch manifest validation
and the direct backend constructor share `ValidateDriverEnvironment`. It rejects
more than 128 entries, entries larger than 4096 bytes, malformed UTF-8, missing
`=`, empty names, NUL, duplicate names and overrides of any of the three reserved
entries. Valid UTF-8 and additional `=` characters in values are preserved.
Validation errors never include environment values.

Required `PATH`, dynamic library paths, Python configuration, proxy settings,
device-selection variables and other dependencies must be explicitly declared
in the approved launch configuration. This changes process behavior and requires
new release images and validation of their actual backend dependencies. Old
release images do not acquire this behavior because a manifest is unchanged.
The existing Fleet H3 template already declares its Python executable, model
paths and device selection, but actual external H3 conformance is not established
by that declaration.

This is an environment construction contract, not a security sandbox or complete
configuration attestation. Driver code can still read shared files or construct
its own environment. It shares the Runtime UID and filesystem; dynamic loaders,
scripts, mapped code and external writers remain outside this proof. The child
executable must still be authenticated and bound to the correct message,
configuration, journal ownership and startup incarnation.

## Reproduction and verification

The regression launches a real CPU Go driver, performs initialization and a
readiness probe, then shuts it down. Synthetic parent variables include a secret
placeholder, proxy, library path, device selection and `PATH`. Evidence records
environment names and only selected synthetic values, never inherited secret
values. Before the fix all three initial scenarios failed; the omitted case
received 76 names instead of the then-expected four, including the undeclared
synthetic credential. The final helper uses an argv selector so the omitted case
has a truly nil declaration and expects exactly three entries.

The final four scenarios cover omission, explicit values, explicit empty values
and UTF-8 values containing `=` and newline. Eleven constructor rejection cases
use a nonexistent command and require the environment-specific error, proving
rejection precedes spawn. Manifest encoding/loading and Fleet validation reject
reserved channel overrides; encoding rejects malformed in-memory UTF-8 before
JSON replacement and preserves valid bytes on round trip.

Validation results:

- Full `go test ./... -count=1`: PASS; ModelRuntime package `67.224s`.
- `go test -race ./internal/modelruntime ./internal/fleetcontroller
  ./internal/nodeagent ./cmd/vela-model-runtime -count=1`: PASS; ModelRuntime
  package `103.740s`, no race reports.
- `make lint`: PASS, `0 issues`.
- Linux integration-tag ModelRuntime lint: PASS, `0 issues`.
- Five selected Linux tests, including the real process environment and
  residency-across-assignments tests: PASS, no skips. They run as UID/GID 65534
  in a read-only, network-disabled, capability-free CPU container with bounded
  memory/process count and private tmpfs.
- `TestRuntimePIDNamespaceContainsBackendWriters`: all nine scenarios PASS in
  `9.98s`, package `10.675s`. The first run exposed a fixture that implicitly
  inherited `VELA_TEST_LIFECYCLE_PHASE`; it is now explicitly declared in the
  manifest. Initializing/idle/admitted/closed-idle crash assertions are retained.
- External `TestProcessBackendConformsToFastH3PythonDriver`: not exercised; the
  required source and Python paths are not configured in this environment.

Both Linux experiments use the existing local image
`sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5`
with `--pull never`. No GPU or remote deployment is involved. The focused test
selection is:

```sh
go test ./internal/modelruntime ./internal/fleetcontroller \
  -run 'Test(ProcessBackendUsesOnlyDeclaredEnvironment|ProcessBackendRejectsInvalidEnvironmentBeforeSpawn|LaunchManifestRejectsReservedDriverEnvironment|LaunchManifestPreservesDriverEnvironmentBytes|WorkerBundleRejectsInvalidDriverEnvironment)$' \
  -count=1 -v
```

Lifecycle reproduction:

```sh
VELA_TEST_RUNTIME_CONTAINER_IMAGE=sha256:3489c7674813ba5d8b1a9977baea8a6e553784dab7b84759d1014dbd78f7ebd5 \
  go test -tags=integration ./internal/modelruntime \
  -run '^TestRuntimePIDNamespaceContainsBackendWriters$' -count=1 -v
```

Local logs are `/tmp/vela-driver-environment-*.log`. Task-created test binaries,
containers and volumes are removed after validation; the two pre-existing
stopped containers are preserved. PostgreSQL 94, Worker journal 5, Runtime
journal 6, Registry binding 1 and Production Gates `0/9` remain unchanged.
Startup authorization, executable/message binding, endpoint assembly and
independent retirement remain open.
