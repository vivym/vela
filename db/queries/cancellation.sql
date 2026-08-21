-- name: SetCancellationRequestContext :one
SELECT
    organization_id::uuid,
    project_id::uuid,
    principal_id::uuid,
    transaction_time::timestamptz
FROM vela_set_cancellation_request_context(
    sqlc.arg(credential_id)::uuid,
    sqlc.arg(credential_proof)::bytea
);

-- name: CancelJob :one
SELECT
    coalesce(
        result.cancellation_id,
        '00000000-0000-0000-0000-000000000000'::uuid
    )::uuid AS cancellation_id,
    result.job_id::uuid AS job_id,
    result.decision::cancellation_decision AS decision,
    result.job_state::job_state AS job_state,
    result.previous_job_state::job_state AS previous_job_state,
    result.job_version::bigint AS job_version,
    result.cancellation_fence::bigint AS cancellation_fence,
    coalesce(
        result.attempt_id,
        '00000000-0000-0000-0000-000000000000'::uuid
    )::uuid AS attempt_id,
    coalesce(
        result.worker_id,
        '00000000-0000-0000-0000-000000000000'::uuid
    )::uuid AS worker_id,
    coalesce(result.worker_epoch, 0)::bigint AS worker_epoch,
    coalesce(result.attempt_fence, 0)::bigint AS attempt_fence,
    coalesce(
        result.authority_lease_id,
        '00000000-0000-0000-0000-000000000000'::uuid
    )::uuid AS authority_lease_id,
    coalesce(result.authority_lease_phase::text, '')::text AS authority_lease_phase,
    coalesce(result.authority_lease_owner_kind::text, '')::text AS authority_lease_owner_kind,
    coalesce(result.authority_lease_owner_id, '')::text AS authority_lease_owner_id,
    coalesce(
        result.authority_lease_expires_at,
        'epoch'::timestamptz
    )::timestamptz AS authority_lease_expires_at,
    result.billable::boolean AS billable,
    coalesce(
        result.charge_id,
        '00000000-0000-0000-0000-000000000000'::uuid
    )::uuid AS charge_id,
    coalesce(result.charge_amount_minor, 0)::bigint AS charge_amount_minor,
    coalesce(result.charge_currency, '')::text AS charge_currency,
    coalesce(result.charge_reason::text, '')::text AS charge_reason,
    coalesce(result.charge_posted_at, 'epoch'::timestamptz)::timestamptz AS charge_posted_at,
    result.decided_at::timestamptz AS decided_at,
    result.created::boolean AS created
FROM vela_cancel_job(
    sqlc.arg(job_id)::uuid,
    sqlc.arg(cancellation_id)::uuid,
    sqlc.arg(charge_id)::uuid,
    sqlc.arg(cancel_requested_event_id)::uuid,
    sqlc.arg(canceling_event_id)::uuid,
    sqlc.arg(canceled_event_id)::uuid,
    sqlc.arg(charge_posted_event_id)::uuid,
    sqlc.arg(invoice_export_event_id)::uuid
) AS result;

-- name: ListSucceededArtifactSetForCancellation :many
SELECT
    artifact_set.id::uuid AS artifact_set_id,
    artifact_set.manifest_sha256::bytea AS manifest_sha256,
    artifact_set.retention_expires_at::timestamptz AS retention_expires_at,
    completion.completed_at::timestamptz AS completed_at,
    completion.charge_id::uuid AS charge_id,
    item.artifact_id::uuid AS artifact_id,
    item.kind::text AS kind,
    item.ordinal::integer AS ordinal,
    item.object_key::text AS object_key,
    item.object_version_id::text AS object_version_id,
    item.size_bytes::bigint AS size_bytes,
    item.sha256::bytea AS sha256,
    item.content_type::text AS content_type
FROM jobs AS job
JOIN artifact_sets AS artifact_set
  ON artifact_set.id = job.result_artifact_set_id
 AND artifact_set.job_id = job.id
JOIN visible_completions AS completion
  ON completion.artifact_set_id = artifact_set.id
 AND completion.job_id = job.id
JOIN artifact_set_items AS item
  ON item.artifact_set_id = artifact_set.id
WHERE job.id = sqlc.arg(job_id)::uuid
  AND job.organization_id = sqlc.arg(organization_id)::uuid
  AND job.project_id = sqlc.arg(project_id)::uuid
  AND job.state = 'SUCCEEDED'
ORDER BY CASE item.kind WHEN 'VIDEO' THEN 0 ELSE 1 END, item.ordinal;

-- name: LockCancellationLease :one
SELECT
    id,
    attempt_id,
    worker_id,
    worker_epoch,
    phase,
    owner_kind,
    owner_id,
    fence,
    token_digest,
    expires_at,
    revoked_at
FROM attempt_leases
WHERE id = sqlc.arg(authority_lease_id)
  AND attempt_id = sqlc.arg(attempt_id)
FOR UPDATE;

-- name: GetCancellationStopLeaseID :one
SELECT authority_lease_id::uuid
FROM job_cancellation_decisions
WHERE id = sqlc.arg(cancellation_id)
  AND attempt_id = sqlc.arg(attempt_id)::uuid
  AND authority_lease_id IS NOT NULL;

-- name: FindNextCancellationStopCandidate :one
SELECT
    d.id AS cancellation_id,
    d.attempt_id::uuid AS attempt_id,
    d.worker_id::uuid AS worker_id,
    d.authority_lease_id::uuid AS authority_lease_id
FROM job_cancellation_decisions AS d
JOIN attempt_leases AS l ON l.id = d.authority_lease_id
LEFT JOIN cancellation_stop_receipts AS r ON r.cancellation_id = d.id
WHERE d.attempt_id IS NOT NULL
  AND d.worker_id IS NOT NULL
  AND d.authority_lease_id IS NOT NULL
  AND d.authority_lease_phase IS NOT NULL
  AND d.authority_lease_owner_kind IS NOT NULL
  AND d.authority_lease_expires_at IS NOT NULL
  AND l.revoked_at IS NOT NULL
  AND d.authority_lease_expires_at <= clock_timestamp()
  AND r.id IS NULL
ORDER BY d.authority_lease_expires_at, d.decided_at, d.id
LIMIT 1;

-- name: LockCancellationAttempt :one
SELECT
    id,
    organization_id,
    project_id,
    job_id,
    worker_id,
    worker_epoch,
    fence,
    state,
    ended_at
FROM attempts
WHERE id = sqlc.arg(attempt_id)
FOR UPDATE;

-- name: LockCancellationStopAuthority :one
SELECT
    d.id AS cancellation_id,
    d.organization_id,
    d.project_id,
    d.job_id,
    d.decision,
    d.billable,
    coalesce(d.attempt_id, '00000000-0000-0000-0000-000000000000'::uuid)::uuid
        AS attempt_id,
    coalesce(d.worker_id, '00000000-0000-0000-0000-000000000000'::uuid)::uuid
        AS worker_id,
    coalesce(d.worker_epoch, 0)::bigint AS worker_epoch,
    coalesce(d.attempt_fence, 0)::bigint AS attempt_fence,
    coalesce(
        d.authority_lease_id,
        '00000000-0000-0000-0000-000000000000'::uuid
    )::uuid AS authority_lease_id,
    coalesce(d.authority_lease_phase::text, '')::text AS authority_lease_phase,
    coalesce(d.authority_lease_owner_kind::text, '')::text AS authority_lease_owner_kind,
    coalesce(d.authority_lease_owner_id, '')::text AS authority_lease_owner_id,
    coalesce(d.authority_lease_expires_at, 'epoch'::timestamptz)::timestamptz
        AS authority_lease_expires_at,
    d.cancellation_fence,
    d.job_version AS decision_job_version,
    d.decided_at,
    j.state AS job_state,
    j.version AS job_version,
    coalesce(c.id, '00000000-0000-0000-0000-000000000000'::uuid)::uuid AS charge_id,
    coalesce(r.id, '00000000-0000-0000-0000-000000000000'::uuid)::uuid AS receipt_id,
    coalesce(r.source::text, '')::text AS receipt_source,
    coalesce(r.terminal_job_version, 0)::bigint AS receipt_job_version,
    coalesce(r.stopped_at, 'epoch'::timestamptz)::timestamptz AS receipt_stopped_at
FROM job_cancellation_decisions AS d
JOIN jobs AS j ON j.id = d.job_id
LEFT JOIN charges AS c ON c.cancellation_id = d.id
LEFT JOIN cancellation_stop_receipts AS r ON r.cancellation_id = d.id
WHERE d.id = sqlc.arg(cancellation_id)
FOR UPDATE OF j;

-- name: InsertCancellationStopReceipt :exec
INSERT INTO cancellation_stop_receipts (
    id,
    organization_id,
    project_id,
    job_id,
    cancellation_id,
    attempt_id,
    worker_id,
    worker_epoch,
    attempt_fence,
    cancellation_fence,
    source,
    terminal_job_version,
    stopped_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(cancellation_id),
    sqlc.arg(attempt_id),
    sqlc.arg(worker_id),
    sqlc.arg(worker_epoch),
    sqlc.arg(attempt_fence),
    sqlc.arg(cancellation_fence),
    sqlc.arg(source)::cancellation_stop_source,
    sqlc.arg(terminal_job_version),
    sqlc.arg(stopped_at)
);

-- name: CompleteCancelingJob :one
UPDATE jobs
SET state = 'CANCELED',
    version = version + 1,
    updated_at = sqlc.arg(stopped_at)
WHERE id = sqlc.arg(job_id)
  AND state = 'CANCELING'
  AND version = sqlc.arg(expected_version)
  AND current_fence = sqlc.arg(cancellation_fence)
RETURNING version;

-- name: ReleaseWorkerAfterCancellationStop :execrows
UPDATE workers
SET lifecycle_state = 'READY',
    updated_at = sqlc.arg(stopped_at)
WHERE id = sqlc.arg(worker_id)
  AND epoch = sqlc.arg(worker_epoch)
  AND lifecycle_state = 'DRAINING'
  AND reachability_condition = 'HEALTHY';

-- name: InsertCancellationStoppedOutboxEvent :exec
INSERT INTO outbox_events (
    event_id,
    organization_id,
    project_id,
    aggregate_type,
    aggregate_id,
    aggregate_version,
    event_type,
    schema_version,
    payload,
    occurred_at
) VALUES (
    sqlc.arg(event_id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    'Job',
    sqlc.arg(job_id),
    sqlc.arg(job_version),
    'job.canceled',
    1,
    sqlc.arg(payload),
    sqlc.arg(occurred_at)
);
