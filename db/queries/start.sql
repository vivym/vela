-- name: LockStartAuthority :one
SELECT
    a.id AS attempt_id,
    a.job_id,
    a.attempt_number,
    a.state AS attempt_state,
    a.started_at AS attempt_started_at,
    l.token_digest,
    l.expires_at AS lease_expires_at,
    l.revoked_at AS lease_revoked_at,
    j.organization_id,
    j.project_id,
    j.state AS job_state,
    j.version AS job_version,
    j.current_fence,
    j.billable_started_at,
    j.job_expires_at,
    cr.state AS credit_reservation_state
FROM attempts AS a
JOIN attempt_leases AS l ON l.attempt_id = a.id
JOIN jobs AS j ON j.id = a.job_id
JOIN credit_reservations AS cr ON cr.job_id = j.id
WHERE a.id = sqlc.arg(attempt_id)
  AND a.worker_id = sqlc.arg(worker_id)
  AND a.worker_epoch = sqlc.arg(worker_epoch)
  AND a.fence = sqlc.arg(fence)
  AND l.worker_id = sqlc.arg(worker_id)
  AND l.worker_epoch = sqlc.arg(worker_epoch)
  AND l.fence = sqlc.arg(fence)
  AND l.phase = 'EXECUTION'
  AND l.owner_kind = 'WORKER'
  AND l.owner_id = sqlc.arg(owner_id)
  AND l.revoked_at IS NULL
LIMIT 1
FOR UPDATE OF a, l, j, cr;

-- name: MarkAttemptRunning :execrows
UPDATE attempts
SET state = 'RUNNING',
    started_at = sqlc.arg(started_at),
    updated_at = sqlc.arg(started_at)
WHERE id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND state = 'ASSIGNED'
  AND started_at IS NULL;

-- name: MarkJobRunning :one
UPDATE jobs
SET state = 'RUNNING',
    version = version + 1,
    billable_started_at = COALESCE(billable_started_at, sqlc.arg(started_at)),
    updated_at = sqlc.arg(started_at)
WHERE id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version)
  AND current_fence = sqlc.arg(fence)
  AND state = 'ASSIGNED'
RETURNING version, billable_started_at;

-- name: InsertStartOutboxEvent :exec
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
    'job.started',
    1,
    sqlc.arg(payload),
    sqlc.arg(occurred_at)
);
