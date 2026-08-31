# H3 Worker Agent And Runner

## Goal

Connect an exact control-plane Assignment to one fenced H3 execution, immutable
Artifact finalization, and bounded same-Worker recovery. The Go Worker Agent owns
Lease and Artifact authority. The Python runner owns only the pinned backend
process and its one-Encoder/VAE plus seven-DiT GPU role binding.

This slice implements repository behavior for ADRs 0003, 0008, 0011, 0013,
0015, 0019, 0020, 0021, 0027, and 0028. It does not certify a model profile,
materialize a live Worker pool, or advance an ADR 0029 Production Gate.

## Authority And Transport

The Worker Agent authenticates to `WorkerControlService.Connect` with its mTLS
Worker identity. `Acquire`, `Start`, `Heartbeat`, `Fail`, finalization, Artifact
upload, verification, and Visible Completion all use the same Attempt ID, Job ID,
Worker ID, Worker epoch, Lease fence, and bounded Lease token authority.

The control plane returns `lease_valid_for` from PostgreSQL time. The Agent binds
it to the monotonic timestamp taken before each request; local network latency
can shorten but cannot extend the executable window. A stop response, stale
authority, local deadline, or canceled Agent context stops the runner before
further execution or finalization is attempted.

The Agent and runner communicate only through the versioned `RunnerService` on
an owner-only Unix socket. The Agent validates the canonical socket path and
every ancestor plus the socket inode, UID, type, and `0600` mode. The Runner is
not registered on the remote mTLS listener. The unprivileged Worker Pod obtains
XFS quota observations from the root Node Agent through the separate
UID-authenticated `WorkerHostService`; it receives no `CAP_SYS_ADMIN`.

## Execution Ordering

For each Assignment, the Agent:

1. rejects scratch pressure before `Acquire` and validates the exact Assignment;
2. opens Local Recovery State for the same Worker, epoch, Attempt, and fence;
3. asks the runner to prepare the exact immutable execution specification;
4. obtains the authoritative control-plane Start decision and Billable Start;
5. only then starts the physical backend process;
6. reports bounded backend-neutral progress, GPU health, local Artifact state,
   scratch capacity, and Artifact-store reachability through Heartbeat;
7. reports a structured backend failure through the existing Retry authority, or
   collects outputs and enters the existing finalization protocol; and
8. terminally removes exact runner outputs and Local Recovery State only after
   the authoritative failure, cancellation, or Visible Completion result.

The Worker Agent, not the runner, owns the Attempt-scoped control-plane
Heartbeat sequence. A runner status sequence is only a lower bound: repeated
runner observations may carry changed scratch, Artifact-store, health, ETA, or
progress fields, so every new control-plane observation still receives a
strictly greater Agent sequence. Before writing the pending Heartbeat request,
the Agent atomically persists the newly reserved high-water sequence in
`upload--execution-heartbeat.json`, bound to the exact Attempt, Job, Worker,
Worker epoch, and Lease fence. A process lost before the pending record skips
that reserved value after restart; a process lost after the pending record
replays the exact request and sequence. A confirmed response may clear the
pending request only after the high-water record is durable. Finalization starts
above the greater of this Agent high water and the terminal runner observation.
Sequence exhaustion fails closed.

On startup the Agent strictly decodes every execution-sequence record before
any control or runner operation. If a pending EXECUTION Heartbeat and a durable
sequence record coexist, their complete authority and sequence must match.
Malformed, unknown, case-folded, duplicate, trailing, cross-authority, or
internally contradictory recovery records require operator reconciliation and
must not trigger an RPC. An exact pending Heartbeat from the immediately prior
Agent version may be replayed without a sequence record during the bounded
adjacent-version rollout.

Terminal output verification uses a fresh cleanup context after control RPCs.
Its bounded duration is derived from the exact receipt byte total and a
Fleet-supplied, XFS/NVMe-certified minimum sequential-read throughput. Worker
startup rejects a per-Attempt quota whose derived cleanup budget, including
bounded control and filesystem overhead, exceeds terminal retention; a replay therefore does
not repeatedly restart a valid large-file hash under one fixed short timeout.

No shell parses backend arguments. The Runner invokes an absolute pinned command
with a fixed argv, a new process group, exact `CUDA_VISIBLE_DEVICES`, bounded
SIGTERM/SIGKILL shutdown, and no inherited file descriptors beyond its standard
streams.

## Runner State And Output Contract

Fleet Controller supplies an exact certified profile allowlist, pinned backend
revision, and an exact role map containing one Encoder/VAE GPU UUID and seven
unique DiT GPU UUIDs. `Prepare` rejects any other revision tuple or malformed
request object. An Attempt ID can never be rebound to different authority or a
different execution specification.

The Runner atomically persists private `request.json` and `state.json` records
under the current scratch project. Reads are bounded, descriptor-based,
`O_NOFOLLOW`, owner/mode checked, and reject duplicate JSON keys. A process
restart may resume `PREPARING`, `READY`, or `RUNNING` work only when the Agent
explicitly supplies the same authority and local recovery remains eligible.
`SUCCEEDED`, `FAILED`, and `CANCELED` recover as immutable terminal states;
terminal `Start` and `Cancel` calls are idempotent replays and never execute the
backend again.

Agent recovery records use exact canonical JSON field names and reject
case-folded aliases, recursive duplicate keys, unknown fields, and trailing
documents before any control or Runner operation.

A successful backend supplies a bounded manifest of direct-child output paths.
The Runner accepts only owned regular files, fixes their private mode, computes
their size and SHA-256, enforces the same Fleet-supplied aggregate logical-byte
quota used by Agent cleanup before hashing, and persists the exact successful
receipt. While the Attempt output directory exists, restart revalidates both the
files and every persisted receipt field. After authoritative terminal cleanup
removes the complete Attempt output directory, restart preserves the immutable
`SUCCEEDED` state without exposing collectable outputs or reexecuting the
backend. An existing but incomplete output directory, invalid manifest,
symlink, malformed failure receipt, changed output, unknown GPU evidence, or
conflicting state still fails closed. The Agent independently reopens each exact
Attempt output, rechecks inode, UID, `0600` mode, size, and SHA-256, and uploads
through signed multipart part intents before requesting Artifact verification
and Visible Completion.

## Deployment Contract

`deploy/worker-agent` renders one `OnDelete` H3 DaemonSet per eligible node. The
Pod requests exactly eight GPUs only for the runner container, uses the H3
label/taint contract, runs as UID/GID 10001 with a read-only root filesystem,
drops all capabilities, disables service-account token mounting, and mounts the
XFS project scratch into only the Worker Agent, plus pinned model weights, mTLS
credentials, exact profile and GPU-role inputs, a private Runner socket, and the
host quota socket. The Runner receives a private empty scratch view with only
the host `runner-state` and `outputs` subdirectories mounted into it. It cannot
mount or traverse Worker Local Recovery State or the Agent-owned terminal-output
quarantine, and `hostPID` and `shareProcessNamespace` are explicitly false. This
mount-namespace exclusion is part of terminal unlink safety: after the Agent
atomically renames an Attempt directory into quarantine, no Runner process can
replace an entry before its exact inode is removed.

A bounded root init container repairs the kubelet `fsGroup` ownership of the
Runner socket and private scratch-view volumes to UID/GID 10001 and mode `0700`;
it adds only `CAP_CHOWN`, while the Worker Agent and runner remain non-root with
all capabilities dropped. The host `runner-state` and `outputs` subdirectories
must already satisfy the host preflight contract before kubelet resolves their
`subPath` mounts.

The Worker Agent and runner image digests in the base are deliberately invalid.
Slice 47 later pins the shared BusyBox socket-init image to its exact
`linux/amd64` manifest. Fleet Controller must materialize node-bound identity,
epoch, configuration, secrets, immutable profile/role inputs, and approved
Worker/runner/backend image digests. It must also enforce the
drain/fence/finalizer lifecycle before replacing a Pod.

## Evidence

Repository evidence includes:

- Go unit and race tests for Worker Agent ordering, monotonic Lease deadlines,
  failure and stop paths, Agent-shutdown recovery, finalization resume, exact
  output validation, multipart replay, cleanup, and transport validation;
- PostgreSQL/NATS/S3 integration tests for Assignment replay, Worker transport,
  failure/finalization authority, Artifact upload, validation, Visible
  Completion, and stale-fence behavior;
- Python `pytest` coverage for exact profile/GPU binding, public Runner command
  semantics, process shutdown, progress, structured failures, output validation,
  terminal replay, immutable recovery receipts, damaged state, and Unix socket
  permissions;
- Linux `amd64` cross-builds for the Worker and Node host boundary; and
- deterministic Go/Python Protobuf generation plus Kustomize rendering and
  deployment-contract validation.

## Remaining Production Evidence

Repository tests do not prove Fleet Controller ownership, live admission policy,
approved OCI digests or SBOMs, real SGLang/H3 model execution, GPU UUID/topology
certification, XFS/NVMe capacity and failure behavior, private object-store
durability, warm-up/canary/drain/rollback, node or NVMe loss, 72-hour mixed-load
soak, Customer Content lifecycle, or on-call response. Those require the exact
versioned Launch Receipts in ADR 0029; Production remains `0/9 PASS`.
