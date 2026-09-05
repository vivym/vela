# Terminal Stage history reader evidence

Status: historical local CPU/PostgreSQL integration evidence for schema 89, based on
`6871a04`. This is a read-only prerequisite for terminal scratch retirement.
It neither signs a disposition nor permits Worker or Runtime deletion.
Production Gates remain **0/9**. Remote deployment is unchanged.

## Implemented boundary

`vela_read_stage_terminal_history(jsonb)` runs as the existing coordinator owner
with a fixed search path. Only Stage Worker Control may call it. The runtime
role receives no direct table privileges. Control's exact startup privilege
contract at this checkpoint required schema 89; code and database must be upgraded together.
The later schema-90 [content lifecycle repair](assignment-content-lifecycle-evidence-2026-09-05.md)
replaces the original assignment candidate with retained signed authority and
requires completed legacy backfill before current control startup.

The reader binds the historical lease token, allocation, physical attempt,
StageRun, retained Job/Attempt roots, Worker epoch, current control session and
historical leader identity. Only SUCCEEDED, FAILED and CANCELED qualify.

Before filtering by Worker, it checks contiguous physical attempt numbers
against the retry budget, one ASSIGN record per physical attempt, and complete
allocation/lease joins. It returns all matching allocations for the StageRun,
Worker and epoch, including unsigned or undelivered retries. Their maximum
immutable execution sequence is the cutoff; global sequence state is irrelevant.
Historical Runtime barriers and member registrations are retained in the result,
including distinct barrier generation and member-local Runtime epoch values.

The bounded reader rejects more than 256 physical attempts, more than 64
members/devices, or an allocation scope above 2 MiB. These are reader support
limits, not scheduler policy limits; unsupported histories yield no candidate
and are never silently truncated.

## Verification

- Live StageRun and RETRY_WAIT reject; terminal FAILED and CANCELED return scoped
  history. Nineteen request mutations reject without returning history or wire.
- A retry committed by the scheduler, without a second signed assignment,
  remains in the cutoff returned for the first authority. Independent global
  sequence advancement does not change that cutoff.
- Fault-injected missing ASSIGN, lease, allocation, physical attempt or Runtime
  registration, and an unnumbered retry, all reject through the restricted SQL
  interface. Fault injection is confined to rolled-back test transactions.
- Actual non-content expiry removes live Job/Attempt metadata; the reader still
  resolves history from retained roots.
- A stored START renewal is returned by full authority digest without an Acquire
  command ID. A different renewal digest rejects.
- Public Runtime registration advances barriers 1 -> 2 and member-local epochs
  1 -> 8; the old authority retains both scopes before and after public Fleet
  Drain. This exercises historical generations, not the SUPERSEDED enum.
- Two Down/Up cycles preserve role isolation and remove only the new function
  and its three added SELECT grants. Schema 88 fails the new startup contract;
  schema 89 passes. Other runtime roles lack EXECUTE and Worker Control lacks
  direct table access.
- Combined terminal history, allocation sequence, assignment renewal, role,
  and failure replay integration: PASS, 50.388 s, disposable PostgreSQL 17.
  Log: `/tmp/vela-terminal-history-final-integration-2026-09-05.log`.
- `go test ./...`, `make lint` (0 issues), `make verify-generated`: PASS.
  Generated outputs remain identical. Existing tests may reuse valid Go cache.

The tests exposed a missing device-set SELECT grant and a startup privilege
contract mismatch; both were repaired before the combined passing run.

## Remaining boundary

SQL `eligible=true` means candidate history only. The original assignment lookup
uses an optional Acquire command ID to locate stored wire. The Go reader described
below now decodes and authenticates that evidence before returning metadata.
Stored wire may contain Customer Content and remains inside Control, without
logging. A typed transport operation and domain-separated signed disposition
remain unimplemented.

Persistent Worker input admission, all relevant Runtime floors and drain proof,
the retirement journal and restart/failure campaign remain open. This evidence
does not update the earlier schema-88 load/CNPG receipts or establish writer
exclusion, bounded terminal scratch usage, or production readiness.

## Authenticated Go reader

The next increment, based on `ad79c99`, adds
`PostgresTerminalHistoryReader.Read`. It validates both the requested historical
authority and the stored original/renewal with `ValidateEnvelopeForReplay(..., 0)`
and requires identical full canonical digests. It then checks the returned
Job/Attempt/StageRun, caller/session, topology, original allocation and all member
identities before returning typed metadata. Missing evidence returns no history;
invalid signatures and future-issued authority reject. Neither outcome changes
the active execution authorizer.

The result contains no original wire, execution spec, prompt, input URL or lease
token. It is not a signed disposition and is not connected to a deletion path.

- Public PostgreSQL Go-reader regression: expired original succeeds; a validly
  signed but changed execution-spec digest and a different same-Worker Acquire
  candidate both reject after SQL deliberately returns candidate history.
  Future-issued and corrupt signatures reject.
- Renewal without Acquire ID, undelivered retry, metadata expiry and historical
  Runtime scopes/DRAINING now also assert through the public Go reader.
- Combined history integration: PASS, 23.385 s.
  Log: `/tmp/vela-terminal-history-reader-integration-2026-09-05.log`.
- Public reader integration under `-race`: PASS, 6.656 s. Stage authority and
  Worker Control package race tests also pass.
- `go test ./...` and `make lint` (0 issues): PASS. No schema or generated
  contract changes are introduced by this increment.
- Independent source review found no concrete defect in the added reader.

A separate source review identified an existing lifecycle gap: complete
`stage_worker_acquire_results.assignment_wire` retains Customer Content and the
Acquire replay path can return it after live content is removed. The schema-89
metadata-expiry fixture does not prove content erasure. Follow-up work must
separate content-free signed authority evidence from delivery replay payloads,
preserving active delivery recovery while fencing replay after content deletion.
This increment does not repair that retention path.
