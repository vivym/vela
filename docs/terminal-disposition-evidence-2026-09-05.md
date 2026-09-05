# Signed terminal disposition evidence

Status: local CPU-only protocol implementation based on `474cde8`, using schema
90 without a migration. Production Gates remain **0/9**. This increment does not
establish writer drain, local namespace exclusion or permission to delete files.

## Implemented contract

- `ReadStageTerminalDispositionRequest` is operation 13 on the authenticated
  Stage Worker Control Connect stream. Request schema is 1. The original ASSIGN
  requires its Acquire command ID; an exact stored renewal can omit that ID.
- The handler authenticates historical StageAuthority signatures with zero future
  clock allowance and permits their expiry only for this read. Existing active
  execution authorization is unchanged. The role-scoped schema-90 reader
  independently verifies the original stored signature and complete SQL history.
- Incomplete, active or mismatched history returns RETAIN with no signed facts.
  Valid V1 execution authority also returns RETAIN. Invalid queries/signatures
  reject, and storage/signing/validation errors fail closed.
- INPUTS_UNUSED carries a schema-1 `StageTerminalDisposition` signed over
  `vela-stage-terminal-disposition-v1\x00` followed by deterministic protobuf
  bytes with the signature cleared. Devices, allocations and allocation members
  have canonical set order. Unknown fields, unsupported enums/schemas, duplicate
  identities, missing original scope or inconsistent membership reject.
- The signature binds original authority digest; Organization, Project, Job,
  Attempt, StageRun and original physical attempt/allocation/lease; immutable
  terminal state/fence/version; Worker identity/epoch and authenticated member/
  session; topology; all relevant allocation sequences, nonces, profiles,
  residencies, barriers and historical member Runtime identities/epochs/subsets;
  cutoff; observation/expiry; and signing key ID. No caller path, prompt, input
  URL, execution spec or lease token is returned.
- Cutoff equals the maximum of the complete scoped allocation set, including
  allocations never signed or delivered. Sequence gaps are permitted. Runtime
  barrier generation and member-local Runtime epoch remain distinct fields.
- Fresh facts have at most five minutes of validity. A verifier rejects future
  observations or elapsed validity. The wire limit is 3 MiB, within the 4 MiB
  Control receive limit. Existing SQL reader support limits also remain in force;
  histories are never silently truncated.
- `Client.ReadTerminalDisposition` and `ValidateTerminalDispositionResponse`
  check query/result identity, decision consistency, original digest, signature,
  full original binding and session. The client rejects a response if its stream
  generation changed during the query. Successful verification grants no
  execution time and makes no local filesystem changes.

Existing StageAuthority key IDs and public verifier key distribution are reused.
This is protocol domain separation, not Control-exclusive signing against a
compromised Worker holding the current shared signing seed. The broader key
distribution limitation remains documented in the retirement design.

## Local validation

- Dedicated signature tests cover expired original authority, future/expired
  disposition, canonical ordering, public-key-only verification, substitution of
  a validly signed original, execution-signature substitution and signing with
  public bytes. Mutations cover the cutoff, identity, topology, terminal version,
  historical Runtime scopes and signature. Correctly signed but mismatched
  original scope also rejects.
- Shape tests cover nil/duplicate/missing history and members, unknown fields,
  unsupported state/schema and unbounded validity. A 256-allocation/64-member
  protocol envelope fits its wire budget; exceeding allocation count rejects.
  This format test does not relax SQL's smaller JSON history bound.
- Public mTLS Connect uses `PeerAuthenticator`, `Handler`, `ProductionExecutor`,
  `PostgresOperationBackend`, the role-scoped history reader and public verifier.
  Active history returns RETAIN; terminal FAILED returns signed facts. Other
  SPIFFE identity, wrong database session and missing/wrong Acquire IDs retain.
  Malformed schema/Acquire, tampered signature and future-issued authority reject.
  Response mutation tests reject missing facts, changed digest/session/signature,
  wrong request and inconsistent decisions. Queries leave execution state intact.
- The allocation-without-delivery regression now checks the signed cutoff too.
  The Runtime-restart/Fleet-Drain regression checks both historical Runtime epochs
  in the signed response. Both use the full handler and client verifier.
- Focused `-race` integration: PASS, 16.152 s, disposable PostgreSQL 17.
  Signature/control/transport/protocol package race tests: PASS.
- Terminal history plus materialization/failure handler, lost-response journal
  and migration-entrypoint compatibility batch: PASS, 99.810 s. After adding the
  explicit V1 authority retention case, the mTLS query race test passes in
  6.998 s. These are focused integration batches, not a new full-system campaign.
- `go test ./...`, `make lint` (0 issues), and `make generate`: PASS.

The first mTLS test incorrectly expected an original ASSIGN lookup without its
Acquire ID to succeed. It was corrected to require RETAIN. The first broad unit
run found the protocol oneof allowlist needed its new operation/result entries.
An initial combined race run also failed in the existing direct-coordinator
START fixture with stale authority; three diagnostic repeats passed, so that
failure's exact cause was not established. These direct SQL fixture events now
use PostgreSQL time rather than mixing host and container clocks. The final
focused race batch above passes with this change; production time policy was
not relaxed.

Local logs:

- `/tmp/vela-terminal-disposition-integration.log`
- `/tmp/vela-terminal-disposition-race-final.log`
- `/tmp/vela-terminal-disposition-v1-race.log`
- `/tmp/vela-terminal-disposition-compatibility.log`
- `/tmp/vela-terminal-disposition-unit-race.log`
- `/tmp/vela-terminal-disposition-unit-all.log`
- `/tmp/vela-terminal-disposition-lint.log`
- `/tmp/vela-terminal-disposition-generated.log`

## Remaining closure

The Worker has not yet installed the persistent pre-Resolve admission gate or
retirement journal. Runtime/Supervisor has not yet implemented signed floor
installation, the FLOOR_INSTALLED/DRAINED distinction or an execution-specific
drain checkpoint that retains the model. Reused-root corruption, late input
writers, direct delayed Runtime RPCs, restart during deletion and repeated
failure/cancellation/expiry scratch campaigns therefore remain open.

No remote deployment, GPU work, new load/CNPG receipt or production readiness is
claimed. A signed INPUTS_UNUSED result alone must never authorize deletion.
