# Node-private journal custody: first measured CPU prototype

The restrictive-floor prototype is viable: a root process can reuse the existing
signed journal state machine, exclude actual non-root filesystem writers and
unapproved same-UID senders, preserve restrictions through process/response loss,
and subsequently advance a legal floor. It also has measurable IPC cost.
This is sufficient to continue the custody investigation, not to accept a new
deployment ADR or claim complete startup/Stage execution/retirement correctness.

The experiment exposed and led to the production
[EINTR repair](runtime-channel-eintr-evidence-2026-09-08.md), committed as `6bb0204`.
Only that validated repair enters the implementation branch. The experimental
broker remains test-only on `feature/node-journal-custody-prototype`, commit
`34aae79`, in worktree `/tmp/vela-journal-custody-prototype`.

## What actually ran

- Actual root broker process and UID/GID 10001 client processes, using the
  existing authenticated Unix seqpacket channel and pidfds.
- An independently selected/pinned client per journal. Clients cannot select
  storage paths, submit complete snapshots or append arbitrary records.
- The existing `Supervisor.InstallRecoveryExecutionFloor` signature, scope,
  monotonicity, file fsync/rename/directory fsync and recovery implementation.
  There is no second floor state machine in the broker.
- Nine actual workload filesystem attempts return EACCES/EPERM: read, rewrite,
  unlink, rename, hard-link, chmod, directory listing, root-process `/proc/root`
  and `/proc/fd` access. A separate same-UID process cannot acquire the approved
  sender role through the same endpoint.
- Five real broker failures: before mutation, after rename, after directory
  sync, committed but response lost, and killed after acknowledged commit.
  A new root process reopens the original volume without initialization, the
  same live client retries, a new signed floor advances to 12, and replay of
  the old floor cannot lower it. Original journal UUID is preserved.
- Typed request rejection covers signature corruption, snapshot replacement,
  client paths, duplicate fields and trailing data. Rejected requests leave
  the private journal bytes unchanged; subsequent legal work still succeeds.

Three complete CPU repetitions pass: 15 crash/recovery subcases, 135 filesystem
denials, 15 same-UID role denials, 15 typed rejection cases and 15,360 timed
floor mutations (7,680 over IPC). The additional native Linux race run passes
all five crash cases and typed rejections without subprocess race reports.
Scoped Linux ModelRuntime lint passes. The independent production channel
regressions cover inherited-FD sender substitution and actual PID namespaces.

## Measurement and interpretation

Go 1.26.7 native Linux/arm64 static binaries; Docker 28.3.2 on the local macOS
host. Each scratch container is limited to 4 CPUs, 4 GiB and 256 processes,
has no network or host/workload mounts, and adds only SYS_PTRACE. These are CPU
limits, not exclusive reserved host cores. The same host/filesystem remains up
across each injected process failure; this is not a power-cut/reboot experiment.

Each repetition performs 512 increasing signed floors per journal, with one
outstanding request per client, for one and four independent fixture journals.
Direct and IPC use exactly the same request bytes and persistence policy.
IPC latency includes fresh socket establishment, challenge, kernel identity
validation, request/response and teardown. Direct latency includes the same
typed decoding, signature validation and persistence without that transport.
Aggregate IPC throughput also includes harness pipe overhead.

The p50 column below is the median of three per-run p50 values, **not** a pooled
latency percentile. Ranges retain all three runs, including the noisy third run.

| Journals | Path | Median per-run p50 (ms) | Per-run p99 range (ms) | Throughput range (updates/s) |
| --- | --- | ---: | ---: | ---: |
| 1 | direct | 0.818 | 1.388–7.220 | 907–1,209 |
| 1 | authenticated IPC | 1.195 | 1.714–5.909 | 533–858 |
| 4 | direct | 1.608 | 2.525–3.536 | 2,247–2,460 |
| 4 | authenticated IPC | 2.132 | 4.055–5.108 | 1,604–1,745 |

Median per-run p50 increases by approximately 0.377 ms (one journal) and
0.525 ms (four journals). Median throughput falls by approximately 33% and 32%
respectively. This cost is material to the measured floor path; it is not a
measured end-to-end Job regression or proof that the architecture misses its
service budget. Tail variation prevents treating the small sample as a stable
production p99. Do not infer that broker and direct persistence have equivalent
performance, or that this result proves a globally superior architecture.

The final journal is 2,397 bytes, or 9,588 bytes across four journals, in all
measurement runs. This establishes the final size for the floor-only operation
mix; it does not establish bounded retained execution/renewal/drain history.
Two syncs per mutation are the source-derived existing policy, not an observed
syscall count. CPU/RSS values in the raw records include the harness and large
test binaries: direct CPU use is 9–10 / 45–50 process ticks for one/four journals,
versus IPC 31–49 / 148–162. RSS endpoints are roughly 31–34 MB for direct and
89–94 / 172 MB for IPC. They are neither production daemon footprints nor
peak/leak measurements.

## Primary evidence and reproduction

In prototype commit `34aae79`:

```sh
bash hack/run-journal-custody-prototype.sh cpu
bash hack/run-journal-custody-prototype.sh race
```

The runner preserves build output, image identity, source revision/diff,
prototype hash and results. It accepts `VELA_CUSTODY_EVIDENCE` and
`VELA_CUSTODY_BUILD_CACHE` as absolute directory overrides. No push/deploy or
image pruning is performed. Raw successful and failing evidence is committed
under `docs/evidence/journal-custody-prototype/` on that branch.

Fixed measurement source is production `6bb0204` plus the prototype file, whose
SHA-256 is `74a86b9ec6433535862f8ff70201e9e5b0ecd8be32ff50538da754f038955116`.
The runner's cache-mount handling was subsequently corrected and exercised by
the race run; it does not change the compiled experiment or operation policy.

| Artifact | Identity |
| --- | --- |
| CPU image | `sha256:f5871f03187e565e33cca0fc1e46e06fc565c846031e326ef150c1bda64c524e` |
| Race image | `sha256:2fe251c53042dad8fbe71177122721c8e88f5c3e021ea0d10c73c3c9703d47ac` |
| `cpu.txt` SHA-256 | `773fe03c852d4aea1cfe97af2be80c5f141c0d3a4271b24333046d26e9e4cfa2` |
| `race.txt` SHA-256 | `e67eeca65d4a78da5754333dfdf459d3a87b9c56e95f8fa16fed7ae332c8e253` |
| `eintr-diagnostic.txt` SHA-256 | `336295a8d68983cc9cf65863c6eec894773d78b502e8598e6c6ba331f89807b8` |

Local source/build receipts remain in
`/tmp/vela-startup-validation.syiSMk/custody-native-cpu/` and
`/tmp/vela-startup-validation.syiSMk/custody-native-race/`.

## Remaining acceptance work

The [custody candidate](node-journal-custody-design-2026-09-08.md) remains a
candidate. Fixtures provide approved PIDs, membership, signing keys and a fixed
clock. The four stores reuse fixture topology shapes. No PostgreSQL activation,
Registry claim, effective executable/configuration, real containment ownership,
Worker journal or production Fleet mount is established by this experiment.
Retrying a restrictive floor does not authorize backend startup or replay work.

The remaining sequence is:

1. Integrate both private journals and authenticated Worker/Runtime roles with
   first-use Registry claims, actual Fleet activation and approved launch
   materialization. Define durable startup intent/outcome and partial-commit
   reconciliation across local storage and PostgreSQL; do not claim one atomic
   distributed transaction.
2. Prove positive normal Stage execution and recovery using that custody path:
   admission, renewal, sealed output publication and reconnect. Independently
   establish exact-owner exit for replacement and handle lost pidfd/Node restart
   without inferring retirement from absence.
3. Close input/backend writer exclusion, exact scratch retirement, unhealthy
   Worker reporting, capacity release and bounded retained history. Current
   restrictive containment and refusal states are not sustained execution.
4. Freeze a new source checkpoint and rerun complete CPU/mock Job, exact-cache,
   one-Charge/isolation and fault campaigns. Prior source-bound receipts remain
   useful regression evidence but do not certify this future architecture.
5. Run sustained open-loop arrivals with backpressure and injected disconnects;
   measure offered/completed work, queue age, timeouts/rejections and resource
   growth. The present closed-loop floor microbenchmark and earlier drained
   512-Job waves do not supply that evidence. Compare costs with the actual
   service budget before accepting or optimizing the IPC design.

PostgreSQL 94, Worker journal 5, Runtime journal 6 and Production Gates **0/9**
remain unchanged. No GPU validation, remote CI, push or deployment occurred.
