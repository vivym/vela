# Resident process inspection channel

Local CPU-only increment over `ea177a5`, database schema 90. Production Gates
remain **0/9**. No GPU or remote deployment was used.

## Problem And Boundary

The normal ProcessBackend command path is deliberately destructive on timeout:
it interrupts blocked stdin writes and uncertain protocol state by terminating
the resident driver group. H3 mock `status` can also install a renewed identity
through `requireActive`. Neither behavior satisfies read-only historical
inspection. A separate local channel now reads state without entering that
command gate or touching process supervision.

`internal/driverinspection` contains the small shared datagram protocol and
descriptor lifecycle. ProcessBackend creates a private connected Unix datagram
socket pair, passes only the child endpoint as fd 3, and declares
`VELA_MODEL_DRIVER_INSPECTION_FD=3`. There is no filesystem socket pathname or
additional network listener. Parent endpoints are close-on-exec; the child marks
its inherited descriptor close-on-exec when consuming it. Startup failure and
normal/forced driver teardown close the parent's inspection endpoint.

The initialize response must advertise exactly
`inspection_protocol: "vela-driver-inspection-v1"`. ProcessBackend implements
`BackendExecutionInspector` only through this negotiated channel; drivers with
no declaration or an unrecognized protocol return an unsupported error. Existing
execution commands remain on `stdio-json-v1` with their established semantics.

## Protocol

Each request and response is one bounded JSON datagram. Requests carry
`schema_version=1`, a nonzero monotonically increasing `request_id`, and the
canonical hexadecimal SHA-256 digest of the exact signed execution envelope.
Responses echo all three and contain `observation{known,state,sequence}`.
Unknown has an empty state and zero sequence. Known states use the existing
execution states; there is no DRAINED state, writer proof, path or command.

Packets are at most 1024 bytes; invalid JSON, duplicate/unknown fields, invalid
states/sequences and mismatched identities reject. Client query lifetime,
including gate wait, is bounded to one second or the shorter caller deadline.
There is one in-flight query per channel, independent of the execution gate.
Socket deadlines interrupt reads and writes without process signals. Up to 32
older valid replies may be discarded before failing a new query. Datagram
boundaries allow retry after a late or malformed reply without desynchronizing
the command stream. Cancellation callbacks finish before releasing the query
gate, so a previous cancellation cannot change a later query's deadline.

The driver serves inspection in a separate goroutine. Its reply writes also
have a one-second bound. Protocol failure closes only the inspection channel;
socket backpressure never holds execution state or stops the resident driver.
Driver exit cancels and joins the inspection server before Runtime cleanup.

## Mock State Publication

H3 Stage mock and lab CPU thumbnail mock commands opt into this capability.
Their command handler first clears the published snapshot, then publishes an
immutable copy of the exact active digest/state/sequence after the command
returns. While execution or output cleanup is in progress, inspection is unknown.
The query goroutine reads only an atomic pointer and never calls `requireActive`,
installs renewal, opens files, performs cleanup or advances cancellation.

Snapshot state is process-local observation, not durable execution history.
An unseen or superseded envelope is unknown; matching one envelope cannot
install it. Successful inspection neither changes ModelRuntime Service state
nor releases its execution slot. A sealed output snapshot remains separate from
proof that all execution-specific writers have exited and closed their handles.

## Validation

- Full `go test ./...`: PASS.
- Related-module/command race suite: PASS, including driverinspection,
  ModelRuntime, H3 mock, both transports, Worker Agent and both mock commands.
- `make lint`: PASS, 0 issues.
- `make verify-generated`: PASS; no protobuf, API or SQL schema change.
- `make test-cross`: PASS, Linux amd64 compilation only.
- Linux arm64 non-root channel and real subprocess tests: PASS with network
  disabled, read-only root, all capabilities dropped and only private tmpfs.

Channel tests inject timeout, late STOPPED replies followed by current RUNNING
replies, malformed/oversized/duplicate packets, mismatched request IDs/digests,
unknown state evidence, socket write backpressure, canceled gate wait and server
shutdown. Real ProcessBackend helpers withhold replies, send malformed JSON or
delay replies until cancellation; cancellation and readiness remain responsive,
the resident process stays alive, and normal shutdown succeeds afterward.
Legacy-driver tests reject unnegotiated inspection while execution continues.

The real built H3 command verifies PREPARED and OUTPUT_SEALED snapshots, rejects
unseen and superseded renewals as unknown, and preserves the immutable output
payload through repeated inspection. The concurrent H3 mock test blocks command
work and observes only unknown until the final state is published. Existing
driver cancellation, teardown, inherited-output and initialization-fault tests
remain green.

The Linux test binaries are `/tmp/vela-process-inspection-linux.test` and
`/tmp/vela-inspection-channel-linux.test`, built with
`CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go test -c`. They were executed under
`sha256:18cfe3ef5e6815560c98237d6216d1e5119702fb0f3894c8785dd58b8bbe5d73`
using `--network none --read-only --cap-drop ALL --user 65534:65534` and
`--tmpfs /tmp:rw,nosuid,nodev,mode=1777`, with each binary mounted read-only.
The process suite selected `^(TestProcessBackendInspection|TestProcessBackendLoadsOnce)`;
the channel suite ran all tests.

## Remaining Work

External production drivers must implement the negotiated read-only snapshot
contract before their inspection is available. The separate CPU-media library
adapter has no optional inspector yet; the lab thumbnail ProcessBackend path
does. Execution-specific writer drain, durable stopped checkpoints, trusted
first bootstrap/default floor assembly, terminal retirement journal,
pending-record reclamation and automatic recovery remain open. No snapshot,
cancel ACK or process shutdown here authorizes scratch deletion or advances a
Production Gate.
