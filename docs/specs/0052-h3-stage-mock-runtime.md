# H3 Stage mock runtime

Date: 2026-09-03

Status: repository implementation and conformance tests complete; target-lab
image publication and deployment receipts remain external.

The H3 Stage mock runtime is a non-production resident driver for the current
Stage-only Worker path. It implements the `stdio-json-v1` ModelRuntime process
contract for `ENCODER`, `DIT`, and `VAE_DECODER` while the real H3 runtime is
still under development. It is not a model implementation, performance
simulator, certification input, Launch Receipt, or Production Gate artifact.

## Build contract

Build a fresh `linux/amd64` command context with:

```text
make build-h3-stage-mock-runtime \
  H3_STAGE_MOCK_RUNTIME_CONTEXT=/absolute/new/path/h3-stage-mock-runtime
```

The output path must be canonical, absolute, and absent. The builder compiles
one static, trimmed, build-ID-free ELF and publishes it atomically under three
mode-`0555` names:

- `h3-encoder`
- `h3-dit`
- `h3-vae-decoder`

All three names intentionally have the same SHA-256 digest. Component identity
comes from the invoked executable name, so the command rejects arguments and an
unrecognized executable name. The builder verifies the exact three-file
inventory, ELF64/x86-64 format, mode, and each supplied digest before the
no-replace rename becomes visible. Standard output reports the command context
and the three digest values needed by the OCI image builder.

## Resident process contract

The Worker starts each command with
`VELA_MODEL_DRIVER_PROTOCOL=stdio-json-v1`. A driver accepts exactly one JSON
document per newline and rejects oversized messages, duplicate keys, unknown
fields, trailing documents, invalid operations, and malformed identities. The
lifecycle is:

```text
initialize -> probe -> prepare -> start -> status -> seal -> shutdown
                               \-> cancel
```

Initialization binds the Worker instance/member epochs, DeviceSet and
membership digests, model residency/runtime epoch, Stage profile revision,
component revision, one exact local GPU identity, and canonical private
scratch/input/output directories. The input and output directories must be
distinct descendants of the scratch directory.

`prepare` decodes a bounded protobuf `StageExecutionSpec`, rejects protobuf
unknown fields, validates the parameters and expected-output JSON objects, and
verifies every declared input against an exact regular file, byte count, and
SHA-256 digest. Input paths use the Worker-owned layout:

```text
<input-root>/stage-runs/<stage-run-id>/inputs/<artifact-id>/<sha256>.bin
<input-root>/stage-runs/<stage-run-id>/root-inputs/<condition-index>/<sha256>.bin
```

The component shape is fixed: Encoder accepts root inputs and no Stage input;
DiT accepts one Encoder Stage input and no root input; VAE Decoder accepts one
DiT Stage input and no root input. A specification must declare the component's
expected output port: `conditioning`, `latent`, or `video` respectively.

## Outputs and replay

Success mode produces deterministic Encoder/DiT payloads from component,
ordered input digests, root-input digests, and the parameters digest. VAE
Decoder produces the repository's bounded H.264/MP4 fixture with a deterministic
mock lineage trailer; the exact published bytes pass the pinned FFprobe 8.0.1
media contract.

Each output is written as a mode-`0600` partial file, synced, and renamed into:

```text
<output-root>/<stage-attempt-id>/<output-port>.bin
```

The sealed manifest binds the local locator, content type, size, SHA-256, and
full Attempt/Stage lineage. Repeated calls with the same authority and exact
specification are idempotent. A different active authority is rejected until
the prior execution is sealed, stopped, or reusable-failed. Cancel or shutdown
deletes an unsealed output before reporting the execution stopped; sealed
output cannot be canceled.

## Fault modes

`VELA_H3_STAGE_MOCK_MODE` supports exactly:

- `success` (the default): publish deterministic output and report
  `OUTPUT_READY`;
- `failure`: report bounded `MOCK_INJECTED_FAILURE` evidence with a reusable
  Worker and deterministic fingerprint; and
- `hang`: remain `RUNNING` until cancel or shutdown.

These modes exercise Stage scheduling, cancellation, retry, sealing, and Worker
reuse. They do not reproduce real GPU allocation, model quality, memory
pressure, latency, throughput, or failure distributions.

## Deployment and evidence boundary

The legacy `vela-h3-mock-backend` remains available only for the retired
monolithic Runner file protocol documented in
[`0048-h3-mock-backend.md`](0048-h3-mock-backend.md). Renaming that legacy ELF
does not produce a valid Stage runtime. Current mock images must install this
spec's three verified resident commands and use a distinct mock revision and
non-production Catalog records.

Repository tests include strict protocol tests, deterministic success/failure/
hang/cancel coverage, a real `modelruntime.NewProcessBackend` process round
trip, pinned FFprobe validation of the exact VAE output, cross-compiled command
verification, and atomic no-replace publication. Those results are repository
conformance only. Lab deployment receipts remain synthetic, and Production
Gates remain unchanged until separately authorized real-H3 Launch Receipts are
validated.
