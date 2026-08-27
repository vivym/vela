# Non-content record expiry conformance

Date: 2026-08-28

Status: Repository conformance implemented, validation and fixed-point reviewed,
and committed in `7c12884`.

This slice implements ordinary expiry for the non-content records governed by
ADR 0015 and consumes the Legal Hold serialization contract added in migration
00030. It does not alter Customer Content retention, shorten a statutory
retention period, or create a Production Gate Launch Receipt.

## Governing boundary

1. `METADATA` consists of the authoritative `jobs` row and every authoritative
   `attempts` row for one terminal Job. They expire 365 days after the canonical
   terminal Job Outbox event. The source rows are physically deleted; a boolean
   flag or an RLS-hidden source row is not expiry.
2. `FINANCIAL` consists of a Job's `credit_reservations`, optional `charges`,
   `invoice_exports`, and `invoice_export_receipts`, plus Organization-scoped
   `finance_reconciliation_records`. Job-scoped records expire 2557 days after
   the canonical terminal Job event. A reconciliation record expires 2557 days
   after its PostgreSQL `posted_at` time.
3. A migration-created root retains only immutable identity, ancestry, expiry
   deadlines, and linkage IDs needed by independent evidence. It contains no
   request content/hash, execution policy, worker/profile assignment, money,
   currency, invoice reference, settlement reference, or reconciliation
   payload. An immutable expiry receipt retains only the target IDs, class,
   scheduled deadline, PostgreSQL expiry time, and deleted-row counts.
4. Existing independent Legal Hold, Content Deletion, Artifact, security,
   reliable-event, and other immutable receipts keep their own contracts. They
   are not copied into the root and are not silently reclassified as an
   unexpired Job, Attempt, Charge, CreditReservation, invoice, or settlement
   source record.
5. Customer Content is excluded. Metadata or financial Legal Hold cannot delay
   Prompt, Artifact, debug dump, multipart, Worker scratch, or Local Recovery
   State deletion, including the 24-hour Content Deletion deadline.

## Expiry clock and candidate authority

Migration 00031 creates one immutable Job root at Admission and one immutable
Attempt root at Assignment. A canonical `job.succeeded`, `job.failed`, or
`job.canceled` Outbox event freezes the Job root's terminal time and creates
separate `JOB_METADATA` and `JOB_FINANCIAL` candidates from the Job's immutable
365/2557-day snapshots. The migration backfills exact current terminal events
and fails if a terminal Job lacks one unambiguous canonical event.

An accepted Finance Reconciliation record creates an
`ORGANIZATION_FINANCIAL` candidate from its PostgreSQL `posted_at` time. This
candidate has Organization ancestry only; Project and Job holds therefore do
not match it.

Candidates are immutable except for the declared `PENDING -> CLAIMED ->
PENDING|EXPIRED` state machine. A claim freezes the instance ID, random token,
expiry, and monotonically increasing attempt count. Claim expiry permits crash
recovery. A stale token cannot complete or release a replacement claim.

## Legal Hold serialization

Expiry performs, in one PostgreSQL transaction:

1. lock the exact candidate row;
2. lock the applicable Organization, Project, and Job ancestry gates in that
   order;
3. call `vela_private.lock_active_non_content_legal_holds(...)` for the exact
   class and ancestry;
4. if a matching ACTIVE hold exists, release the claim back to `PENDING` with a
   bounded next-check delay and delete nothing;
5. otherwise delete the source records, insert one receipt, and mark the
   candidate `EXPIRED`.

Legal Hold placement takes the same ancestry gates before inserting a hold.
This closes the empty-result phantom race: placement first blocks expiry;
expiry first may commit deletion before a later placement, and the later hold
does not restore an expired record. Release remains one-way and can only permit
a future claim.

`lock_active_non_content_legal_holds` accepts exact Organization-only,
Project-only, or Job ancestry. An Organization-only financial record matches
only an Organization hold. A Project record matches Organization and exact
Project holds. A Job record matches Organization, exact Project, and exact Job
holds. Null and cross-ancestor shapes fail closed.

## Physical expiry and retained roots

Before metadata expiry, all existing foreign keys that require a Job or Attempt
identity are rebound to immutable roots with the same composite ownership
keys. This is an expand-first change: current and N-1 writers still insert the
normal rows, while triggers create the roots in the same transaction.

While an Attempt source row remains live, schema-bound trigger functions still
validate every dependent row against its complete production identity: exact
Organization, Project, Attempt, Job, Worker, Worker epoch, and fence fields as
applicable to that table. The immutable roots take over only after physical
expiry; they do not weaken the live writer contract.

Metadata completion deletes every `attempts` source row for the Job and then
the `jobs` source row. Financial records, Legal Holds, and independent receipts
remain valid through root foreign keys. Normal Job/Attempt APIs and control
loops naturally return no source row after expiry.

Financial completion deletes invoice receipt, invoice authority, Charge, and
CreditReservation rows in dependency order. It removes no monetary amount from
the current Organization credit-account aggregate. Organization-financial
completion deletes the exact immutable Finance Reconciliation source row.
Controlled trigger exceptions exist only inside the non-login expiry owner and
only for the exact claimed target; the runtime role has no direct table DML.

## Least privilege and runtime

`vela_non_content_expiry` is a NOLOGIN, non-superuser, non-BYPASSRLS runtime
role. It owns no object and can execute only:

```text
vela_claim_non_content_expiry(text, uuid, integer)
vela_complete_non_content_expiry(non_content_expiry_kind, uuid, uuid, integer)
```

`vela_non_content_expiry_owner` is NOLOGIN/BYPASSRLS, is inherited by no login,
and owns the private implementation. `vela-control` opens a dedicated verified
pool and runs a bounded batch Reconciler. Tick, batch size, claim TTL, and held
retry delay are bounded configuration. There is no object-store or network I/O
inside the expiry transaction.

## Compatibility and rollback

Migration 00031 is additive to public function call shapes used by releases
00001-00030. Existing Job, Attempt, terminal-Outbox, Charge, and Finance
Reconciliation writes gain only same-transaction roots/candidates. The exact
Slice 33 binary at `1968a43` runs on schema 31 without the new role or
configuration; only the current binary starts the expiry Reconciler.

The current binary fails startup on schema 30 because the dedicated role and
functions are absent. Empty `31 -> 30 -> 31` restores the exact source-write
behavior. Down refuses with SQLSTATE `55000` once a terminal candidate, expiry
receipt, or physically expired source exists; durable deletion authority cannot
be silently contracted away.

## Required evidence

- terminal success, failure, and cancellation create exact independent clocks;
- nonterminal Jobs and unposted reconciliation inputs cannot be claimed;
- candidate-row-first locking and active Organization/Project/Job class holds
  block deletion without a partial result;
- concurrent placement, release, claim recovery, stale token, and retry paths
  serialize to one physical expiry and one receipt;
- metadata expiry deletes Job/Attempt source rows while preserving financial,
  Legal Hold, and independent immutable evidence through exact roots;
- every live Attempt dependent retains its exact table-specific composite
  identity check before the source row expires;
- financial expiry deletes Job-scoped and Organization-scoped source records,
  retains no amount or external reference in roots/receipts, and does not alter
  the current credit-account aggregate;
- Customer Content deletion proceeds unchanged under both record classes;
- the runtime role has only the two functions, no direct table access, no
  Customer Content access, and no owner inheritance;
- empty Down/Up, durable-evidence Down refusal, exact N-1 startup/writes, and
  current fail-closed startup all pass;
- focused, race, full unit/integration, cross-build, generated-output,
  deployment, and two-axis fixed-point review complete with no P0-P2 finding.

## Evidence boundary

This slice is repository conformance. Production still requires the deployed
statutory-period configuration, Compliance and expiry credentials, process and
database-failover exercises, observability/SLO receipts, and a versioned Launch
Receipt. Production Gates remain `0/9 PASS`.
