# Legacy H3 Guarded Contraction Runbook

This procedure prepares the M6 point of no return. It archives terminal
machine-level authority, writes an immutable readiness receipt, and freezes the
archived legacy Job, Attempt, and Lease rows. It deliberately performs no DDL.
The release that removes the monolithic Worker/Runner/protocol/deployment path
must carry the guarded schema contraction as a normal migration in the same
release boundary.

Preparation does not unload healthy `ModelResidency`, delete Stage Worker
processes, create a Launch Receipt, or advance a Production Gate.

## Preconditions

- Complete `docs/runbooks/stage-cutover-zero-backlog.md` and retain its exact
  request, result, external manifests, and sealed receipt ID.
- The sealed receipt still belongs to the current `PRODUCTION`, `STAGE_ONLY`
  cutover revision.
- Confirm that no N-1 process, recovery journal, scheduler, event consumer, or
  Artifact workflow can write legacy machine authority. A zero repository test
  fixture is not this evidence.
- Take and verify the release recovery backup required for forward repair or
  database restore.
- Use the exact release binary and the Catalog Promotion database credential.

Do not stop or unload a healthy resident model for this procedure. Model and
WorkerInstance lifecycle remain under their existing drain and release controls.

## Prepare

Create an owner-only request file. An exact retry must keep the same receipt ID
and operator identity.

```json
{
  "zero_backlog_receipt_id": "52000000-0000-0000-0000-000000000005",
  "prepared_by": "stage-cutover-operator"
}
```

```bash
umask 077
go build -o ./bin/vela-stage-cutover ./cmd/vela-stage-cutover
export VELA_STAGE_CUTOVER_DATABASE_URL='postgres://...'

./bin/vela-stage-cutover prepare-legacy-h3-contraction \
  --request ./evidence/prepare-legacy-h3-contraction-request.json \
  >./evidence/prepare-legacy-h3-contraction-result.json
```

The function serializes by receipt ID, locks the legacy authority relations,
and rechecks live database inventory in the same transaction. It archives each
terminal legacy Job, Attempt, and Lease before writing the readiness receipt.
Receipt-activated triggers then reject mutation or recreation of that archived
authority. Stage graph Admission and execution remain available.

The result contains `archive_digest` and `content_digest` as 64 hexadecimal
characters. Retain it beside the M5 receipt and external evidence. An exact
replay returns the same identities, timestamp, and digests with
`replayed=true`; a changed operator identity fails closed.

## Verify Preparation

Run these checks through migration verification tooling, not an application
role:

```sql
SELECT zero_backlog_receipt_id, prepared_at,
       octet_length(archive_digest), octet_length(content_digest)
FROM legacy_h3_contraction_readiness_receipts
WHERE zero_backlog_receipt_id = :'zero_backlog_receipt_id';

SELECT record_kind, count(*)
FROM legacy_h3_execution_archive
WHERE zero_backlog_receipt_id = :'zero_backlog_receipt_id'
GROUP BY record_kind ORDER BY record_kind;

SELECT to_regclass('public.attempt_leases') IS NOT NULL;
SELECT to_regtype('public.execution_authority_kind') IS NOT NULL;
```

Both digest lengths must be 32 bytes. The last two checks must remain true at
this preparation boundary: schema removal before the matching code release is a
failure, not completion. The archive may be empty only when the cluster never
created legacy authority.

## Release Contraction

The next release must remove the monolithic Worker Assignment, Runner,
scheduler, finalization, recovery, protobuf, deployment, and generated query
surfaces. Its migration must recheck the readiness receipt and live-zero state,
drop an explicit reviewed dependency list with `RESTRICT`, rebuild Stage-only
constraints and triggers, and add permanent schema/protocol/runtime/deployment
reachability tests. Do not issue those drops from a runtime
`SECURITY DEFINER` function.

## Failure Handling

- `legacy_h3_contraction_receipt_required` means the M5 receipt is absent,
  stale, or not bound to the current cutover revision.
- `legacy_h3_contraction_live_inventory_nonzero` means authority changed after
  M5 evidence or during the locked recheck. Investigate every source; never
  delete rows merely to make the count zero.
- `legacy_h3_contraction_preparation_replay_mismatch` means the retry payload
  changed.
- `legacy_h3_contraction_preparation_frozen` means a writer tried to mutate
  prepared legacy authority. Preserve the failed command as incident evidence.
- After a readiness receipt exists, migration Down is forbidden. Use the
  approved forward repair or database restore procedure.

Repository verification proves only the preparation guard and archive. Real
completion still requires versioned external drain, N/N-1, recovery, quality,
performance, observability, and ownership evidence named by the Production Gate
contract.
