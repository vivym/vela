# Ordered physical Stage execution

Status: local schema-88 regression, full-suite, synchronous failover and native
CPU load/exact-cache checks pass. No remote rollout or
Production Gate is established by this document.

## Problem and authority

The schema-87 public ModelRuntime gRPC probe reproduced Prepare/Start of the
same still-valid authority after both successful Seal and reusable STOPPED.
Evicting a bounded sealed-receipt cache could also forget an old attempt. Keeping
every retired authority forever would instead grow with the resident runtime's
lifetime, and an expiry based on the last observed envelope would miss unseen
signed renewals.

PostgreSQL now assigns a positive `execution_sequence` to each new
`StageAllocation`. The number is immutable and globally unique. ASSIGN holds the
existing Worker serialization lock before taking the number. The INSERT trigger
also acquires that lock, so numbering cannot run ahead of the Worker ownership
decision. The sequence uses `CACHE 1` and `NO CYCLE`; rollback can leave gaps.
Different Workers can obtain numbers before the earlier transaction commits.
This does not prove that all other locks in their complete ASSIGN transactions
are independent.

`StageAuthority` schema V2 signs this sequence as part of its canonical envelope.
START, Heartbeat and reattachment renewals must retain it. The control authorizer
compares the signed value to the exact lease/allocation pair within its existing
read-only repeatable-read transaction. The narrow scalar reader is executable
only by Stage Worker Control; application roles cannot allocate or reset numbers.

## Runtime invariant

Within one Runtime epoch, let `H` be the largest installed allocation sequence.
A new physical execution may enter Prepare only if `sequence > H`, after normal
signature, identity, deadline and execution-spec checks. Installing it consumes
the number before calling the backend. A failed Prepare therefore cannot reopen
that allocation. Busy and invalid requests do not consume a number.

The existing active authority can replay or renew normally. Seal, STOPPED, FAILED
and active-record cleanup never lower `H`. A later StageAttempt of the same
StageRun receives a new number and remains eligible. An old envelope or an unseen
renewal of a retired allocation always has `sequence <= H` and cannot restart.
Receipt eviction cannot change this invariant. Runtime memory for this fence is
one integer per epoch, independent of the number of completed attempts.

The configured persistent epoch store advances the Runtime identity at restart,
so envelopes from earlier epochs cannot execute even though the in-memory `H`
starts again. Reusing an epoch after losing its local state is unsupported.
This epoch check prevents RPC reentry; it does not prove that a previous backend
process or all descendants have stopped writing files.

## Upgrade, rollback and restore

- Stop new assignment signing and drain/fence V1 work before activating V2
  execution. Apply schema 88 and deploy the matching Control, Agent and Runtime.
  Re-register the new Runtime epochs before admitting new work.
- Historical allocations remain `NULL`; the migration never invents ordering
  for previously signed V1 authority. V1 signatures and stored responses remain
  readable for control recovery. New assignment production requires a positive
  sequence, and new Runtime Prepare rejects V1 execution.
- Current Control startup requires the sequence reader. Older schemas cannot
  satisfy that role contract. Compatibility of stored wire replay does not grant
  permission to create a new legacy execution.
- Down takes an exclusive allocation-table lock and refuses if any sequence
  number was ever consumed, including a rolled-back transaction. Checking the
  sequence's `is_called` also preserves the fence after future history deletion.
  An untouched release can return to schema 87. Post-issuance rollback needs a
  forward-compatible release; resetting or dropping the sequence is unsupported.
- Ordinary synchronous failover requires the committed authority WAL to survive.
  PITR or a failover that loses acknowledged transactions can restore an older
  sequence state. An unseen old high-number authority may then still be valid
  against a surviving Runtime. Stop signing, fence/advance every affected Runtime
  epoch and re-register it before using the restored database for execution.
  The numeric watermark alone cannot detect this situation.

## Verification and remaining boundary

Public gRPC regressions cover sealed/stopped duplicates, unseen renewals, a new
attempt of the same StageRun, failed Prepare, a busy higher number, 300 completed
attempts exceeding receipt retention, V1 rejection and a real FileEpochStore
restart. Signature regressions bind the number and reject a changed renewal.
The control regression rejects an otherwise correctly signed number that differs
from PostgreSQL, and checks exact assignment/START replay.

PostgreSQL regressions cover immutable identity, role permissions, numbering
after the Worker lock, rollback gaps, same-Worker retry order, independent Worker
number acquisition, downgrade refusal and unchanged legacy rows. Migration
roundtrips verify preserved function identity, owner, ACL and security properties.
The [source-bound checkpoint](schema88-validation-checkpoint-2026-09-05.json)
records the final commands and log hashes. The 512-Job load and independent
exact-cache campaign share its unchanged 628-file source digest.

This closes the physical allocation reentry defect only. Successful or terminal
scratch deletion still needs a Worker namespace admission gate, resolver and
download-writer drain, complete process stop proof, a historical terminal
disposition and a durable local cleanup intent. See
[Terminal scratch design](terminal-scratch-retirement-design-2026-09-05.md).
