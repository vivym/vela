# fast-h3 ModelRuntime Conformance

Date: 2026-09-02

Status: repository and local cross-worktree conformance implemented; real
Linux/GPU execution and Production Launch Receipts remain pending.

## External Command Contract

The external fast-h3 repository produces exactly three independently bound
Linux/amd64 commands:

```text
h3-encoder     -> ENCODER
h3-dit         -> DIT
h3-vae-decoder -> VAE_DECODER
```

Each command rejects argv, requires absolute `FAST_H3_PYTHON`, preserves signal
semantics with `execve`, and fixes the Python module and production runtime
factory. `releaseartifacts.VerifyH3RuntimeCommands` remains the authoritative
Vela boundary for exact inventory, SHA-256, mode, and ELF64/x86-64 identity.

The default Fleet topology template binds one `/opt/vela/bin/h3-encoder`, seven
independently schedulable `/opt/vela/bin/h3-dit` processes, and one
`/opt/vela/bin/h3-vae-decoder`. Encoder and VAE are separate resident processes
on the single-slot AUX WorkerInstance and have distinct CapacityPool routes;
the seven DiT processes each own one WorkerInstance/GPU. Every process binds
the external runtime-base paths, model variant, role-specific master port and
warmup spec, plus the exact signed GPU UUID. The repository template remains
deliberately unapproved; production replacement changes the canonical
`WorkerBundleActuation` digest and requires a new approved plan revision.

## ProcessBackend Conformance

The opt-in test uses Vela's public `NewProcessBackend` seam, the real fast-h3
`fast_h3.vela.driver`, `H3ComponentRuntime`, strict H3 parameter parser, and
artifact codec. A checked-in deterministic CPU backend replaces only SGLang GPU
math. Vela freezes one exact conditioned H3 request, materializes its verified
root input, and runs two complete Encoder -> DiT -> VAE chains. Each downstream
process reads the actual upstream fast-h3 artifact. The three resident
processes each execute two `prepare -> start -> status -> seal` assignments;
the event ledger proves one initialization and one shutdown per role, and the
two final decoded-media payloads have the same digest.

Run it with explicit sibling authority:

```bash
VELA_FAST_H3_SOURCE_ROOT=/absolute/path/to/fast-h3 \
VELA_FAST_H3_PYTHON=/absolute/path/to/fast-h3/.venv/bin/python \
go test ./internal/modelruntime \
  -run '^TestProcessBackendConformsToFastH3PythonDriver$' \
  -count=1 -v
```

Without both environment variables, ordinary Vela single-repository tests skip
this external conformance rather than silently selecting another checkout or
Python environment.

## Evidence Boundary

The conformance proves JSONL protocol compatibility, immutable component
selection, process residency across assignments, exact frozen parameters,
verified Encoder root material, materialized Encoder/DiT artifact transport,
readiness and lineage transport, output sealing, and orderly shutdown on the
local host. The Linux launcher identity check and native Python protocol test
are deliberately separate because macOS cannot execute the release ELF files.

It does not execute SGLang, load MiniMax H3 weights, allocate a GPU, prove
same-node or cross-node output equivalence, exercise Kubernetes/DRA, measure
throughput, or advance any Production Gate.
