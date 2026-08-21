-- name: LockFailureAuthority :one
SELECT
    l.id AS lease_id,
    l.token_digest,
    l.expires_at AS lease_expires_at,
    l.revoked_at AS lease_revoked_at,
	 l.phase AS lease_phase,
	 l.owner_kind AS lease_owner_kind,
	 l.owner_id AS lease_owner_id,
    a.id AS attempt_id,
    a.job_id,
    a.attempt_number,
    a.state AS attempt_state,
    a.started_at AS attempt_started_at,
    a.worker_id,
    a.worker_epoch,
    a.fence AS attempt_fence,
    j.organization_id,
    j.project_id,
    j.worker_pool_id,
    j.state AS job_state,
    j.version AS job_version,
    j.current_fence,
    j.execution_max_attempts,
    j.execution_max_total_compute_seconds,
    j.execution_retry_backoff_policy,
    j.execution_retryable_failure_classes,
    j.job_expires_at,
    rts.attempts_started,
    rts.compute_seconds_consumed,
    ere.excluded_workers,
    ere.failure_fingerprints,
    rts.version AS retry_runtime_version
FROM attempt_leases AS l
JOIN attempts AS a ON a.id = l.attempt_id
JOIN jobs AS j ON j.id = a.job_id
JOIN retry_runtime_states AS rts ON rts.job_id = j.id
JOIN execution_retry_evidence AS ere ON ere.job_id = j.id
WHERE l.attempt_id = sqlc.arg(attempt_id)
LIMIT 1
FOR UPDATE OF l, a, j, rts, ere;

-- name: FindExpiredJobFailureCandidate :one
SELECT
    j.id AS job_id,
    coalesce(active_attempt.id, '00000000-0000-0000-0000-000000000000'::uuid) AS attempt_id,
    coalesce(active_attempt.worker_id, '00000000-0000-0000-0000-000000000000'::uuid) AS worker_id
FROM jobs AS j
LEFT JOIN LATERAL (
    SELECT a.id, a.worker_id
    FROM attempts AS a
    WHERE a.job_id = j.id
      AND a.state IN ('ASSIGNED', 'RUNNING')
    LIMIT 1
) AS active_attempt ON true
WHERE j.state IN ('QUEUED', 'RETRY_WAIT', 'ASSIGNED', 'RUNNING')
  AND j.job_expires_at <= clock_timestamp()
ORDER BY j.job_expires_at, j.id
LIMIT 1;

-- name: FindExpiredExecutionLeaseCandidate :one
SELECT a.job_id, a.id AS attempt_id, a.worker_id
FROM attempt_leases AS l
JOIN attempts AS a ON a.id = l.attempt_id
JOIN jobs AS j ON j.id = a.job_id
WHERE l.phase = 'EXECUTION'
  AND l.owner_kind = 'WORKER'
  AND l.revoked_at IS NULL
  AND a.state IN ('ASSIGNED', 'RUNNING')
  AND j.state IN ('ASSIGNED', 'RUNNING')
  AND j.job_expires_at > clock_timestamp()
  AND l.expires_at
      + sqlc.arg(worker_lost_grace_seconds)::bigint * interval '1 second'
      <= clock_timestamp()
ORDER BY l.expires_at, l.id
LIMIT 1;

-- name: LockJobExpiryWithoutAttempt :one
SELECT
    j.organization_id,
    j.project_id,
    j.worker_pool_id,
    j.state AS job_state,
    j.version AS job_version,
    j.current_fence,
    j.job_expires_at,
    rts.compute_seconds_consumed,
    ere.failure_fingerprints,
    rts.version AS retry_runtime_version
FROM jobs AS j
JOIN retry_runtime_states AS rts ON rts.job_id = j.id
JOIN execution_retry_evidence AS ere ON ere.job_id = j.id
WHERE j.id = sqlc.arg(job_id)
  AND j.state IN ('QUEUED', 'RETRY_WAIT')
  AND j.job_expires_at <= clock_timestamp()
  AND NOT EXISTS (
      SELECT 1
      FROM attempts AS a
      WHERE a.job_id = j.id
        AND a.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
  )
FOR UPDATE OF j, rts, ere;

-- name: GetExecutionFailureDecision :one
SELECT
    id,
    request_hash,
    disposition,
    failure_class,
    attempt_id,
    job_id,
    attempt_state,
    attempt_compute_seconds,
    total_compute_seconds,
    next_retry_at,
    job_fence,
    job_version,
    decided_at
FROM execution_failure_decisions
WHERE attempt_id = sqlc.arg(attempt_id);

-- name: LockFailureProjectCounters :one
SELECT queued_count, retry_wait_count, running_count, running_limit
FROM projects
WHERE organization_id = sqlc.arg(organization_id)
  AND id = sqlc.arg(project_id)
FOR UPDATE;

-- name: LockFailurePoolCounters :one
SELECT queued_count, retry_wait_count
FROM worker_pools
WHERE id = sqlc.arg(worker_pool_id)
FOR UPDATE;

-- name: LockFailureCreditReservation :one
SELECT id, organization_id, project_id, amount_minor, currency, state
FROM credit_reservations
WHERE job_id = sqlc.arg(job_id)
FOR UPDATE;

-- name: LockFailureOrganizationCredit :one
SELECT currency, reserved_minor
FROM organization_credit_accounts
WHERE organization_id = sqlc.arg(organization_id)
FOR UPDATE;

-- name: MarkAttemptFailed :execrows
UPDATE attempts
SET state = 'FAILED',
    ended_at = sqlc.arg(decided_at),
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND state IN ('ASSIGNED', 'RUNNING')
  AND ended_at IS NULL;

-- name: MarkAttemptLost :execrows
UPDATE attempts
SET state = 'LOST',
    ended_at = sqlc.arg(decided_at),
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND state IN ('ASSIGNED', 'RUNNING')
  AND ended_at IS NULL;

-- name: RevokeExecutionLeaseForFailure :execrows
UPDATE attempt_leases
SET revoked_at = sqlc.arg(decided_at),
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(lease_id)
  AND attempt_id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND phase = 'EXECUTION'
  AND owner_kind = 'WORKER'
  AND revoked_at IS NULL;

-- name: MarkWorkerReusableAfterFailure :execrows
UPDATE workers
SET lifecycle_state = CASE
        WHEN lifecycle_state = 'BUSY' THEN 'READY'::worker_lifecycle_state
        ELSE lifecycle_state
    END,
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(worker_id)
  AND epoch = sqlc.arg(worker_epoch)
  AND reachability_condition = 'HEALTHY';

-- name: MarkWorkerDrainingAfterFailure :execrows
UPDATE workers
SET lifecycle_state = CASE
        WHEN lifecycle_state IN ('READY', 'BUSY') THEN 'DRAINING'::worker_lifecycle_state
        ELSE lifecycle_state
    END,
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(worker_id)
  AND epoch = sqlc.arg(worker_epoch);

-- name: MarkWorkerLostAfterFailure :execrows
UPDATE workers
SET lifecycle_state = CASE
        WHEN lifecycle_state IN ('READY', 'BUSY') THEN 'DRAINING'::worker_lifecycle_state
        ELSE lifecycle_state
    END,
    reachability_condition = 'OFFLINE',
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(worker_id)
  AND epoch = sqlc.arg(worker_epoch);

-- name: UpdateRetryRuntimeForFailure :execrows
UPDATE retry_runtime_states
SET compute_seconds_consumed = sqlc.arg(total_compute_seconds),
    next_retry_at = sqlc.narg(next_retry_at),
    last_failure_class = sqlc.arg(failure_class),
    version = version + 1,
    updated_at = sqlc.arg(decided_at)
WHERE job_id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version)
  AND compute_seconds_consumed = sqlc.arg(previous_compute_seconds);

-- name: UpdateExecutionRetryEvidence :execrows
UPDATE execution_retry_evidence
SET excluded_workers = sqlc.arg(excluded_workers),
    failure_fingerprints = sqlc.arg(failure_fingerprints),
    updated_at = sqlc.arg(decided_at)
WHERE job_id = sqlc.arg(job_id);

-- name: MoveProjectRunningToRetryWait :execrows
UPDATE projects
SET running_count = running_count - 1,
    queued_count = queued_count + 1,
    retry_wait_count = retry_wait_count + 1
WHERE organization_id = sqlc.arg(organization_id)
  AND id = sqlc.arg(project_id)
  AND running_count > 0;

-- name: DecrementProjectRunningForFailure :execrows
UPDATE projects
SET running_count = running_count - 1
WHERE organization_id = sqlc.arg(organization_id)
  AND id = sqlc.arg(project_id)
  AND running_count > 0;

-- name: DecrementProjectWaitingForFailure :execrows
UPDATE projects
SET queued_count = queued_count - 1,
    retry_wait_count = retry_wait_count - CASE WHEN sqlc.arg(from_retry)::boolean THEN 1 ELSE 0 END
WHERE organization_id = sqlc.arg(organization_id)
  AND id = sqlc.arg(project_id)
  AND (
      (sqlc.arg(from_retry)::boolean AND retry_wait_count > 0 AND queued_count > 0)
      OR (NOT sqlc.arg(from_retry)::boolean AND queued_count - retry_wait_count > 0)
  );

-- name: DecrementPoolWaitingForFailure :execrows
UPDATE worker_pools
SET queued_count = queued_count - 1,
    retry_wait_count = retry_wait_count - CASE WHEN sqlc.arg(from_retry)::boolean THEN 1 ELSE 0 END
WHERE id = sqlc.arg(worker_pool_id)
  AND (
      (sqlc.arg(from_retry)::boolean AND retry_wait_count > 0 AND queued_count > 0)
      OR (NOT sqlc.arg(from_retry)::boolean AND queued_count - retry_wait_count > 0)
  );

-- name: IncrementPoolRetryWait :execrows
UPDATE worker_pools
SET queued_count = queued_count + 1,
    retry_wait_count = retry_wait_count + 1
WHERE id = sqlc.arg(worker_pool_id);

-- name: MarkJobRetryWait :one
UPDATE jobs
SET state = 'RETRY_WAIT',
    current_fence = sqlc.arg(job_fence),
    version = version + 1,
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version)
  AND current_fence = sqlc.arg(previous_fence)
  AND state IN ('ASSIGNED', 'RUNNING')
RETURNING version;

-- name: MarkJobFailedFromActive :one
UPDATE jobs
SET state = 'FAILED',
    current_fence = sqlc.arg(job_fence),
    version = version + 1,
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version)
  AND current_fence = sqlc.arg(previous_fence)
  AND state IN ('ASSIGNED', 'RUNNING')
RETURNING version;

-- name: MarkJobFailedWithoutAttempt :one
UPDATE jobs
SET state = 'FAILED',
    current_fence = sqlc.arg(job_fence),
    version = version + 1,
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version)
  AND current_fence = sqlc.arg(previous_fence)
  AND state = sqlc.arg(expected_state)
  AND state IN ('QUEUED', 'RETRY_WAIT')
RETURNING version;

-- name: UpdateRetryRuntimeForJobExpiry :execrows
UPDATE retry_runtime_states
SET next_retry_at = NULL,
    last_failure_class = 'JOB_EXPIRED',
    version = version + 1,
    updated_at = sqlc.arg(decided_at)
WHERE job_id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version);

-- name: UpdateExecutionRetryEvidenceForJobExpiry :execrows
UPDATE execution_retry_evidence
SET failure_fingerprints = sqlc.arg(failure_fingerprints),
    updated_at = sqlc.arg(decided_at)
WHERE job_id = sqlc.arg(job_id);

-- name: ReleaseFailureCreditReservation :execrows
UPDATE credit_reservations
SET state = 'RELEASED',
    updated_at = sqlc.arg(decided_at)
WHERE id = sqlc.arg(credit_reservation_id)
  AND job_id = sqlc.arg(job_id)
  AND state = 'RESERVED';

-- name: ReleaseOrganizationCreditForFailure :execrows
UPDATE organization_credit_accounts
SET reserved_minor = reserved_minor - sqlc.arg(amount_minor),
    version = version + 1,
    updated_at = sqlc.arg(decided_at)
WHERE organization_id = sqlc.arg(organization_id)
  AND currency = sqlc.arg(currency)
  AND reserved_minor >= sqlc.arg(amount_minor);

-- name: InsertExecutionFailureDecision :exec
INSERT INTO execution_failure_decisions (
    id,
    organization_id,
    project_id,
    job_id,
    attempt_id,
    worker_id,
    worker_epoch,
    attempt_fence,
    source,
    disposition,
    attempt_state,
    failure_class,
    failure_fingerprint,
    request_hash,
    error_summary,
    backend_stage,
    gpu_uuids,
    inference_backend_revision,
    retry_recommended,
    worker_reusable,
    attempt_compute_seconds,
    total_compute_seconds,
    next_retry_at,
    job_fence,
    job_version,
    decided_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(attempt_id),
    sqlc.arg(worker_id),
    sqlc.arg(worker_epoch),
    sqlc.arg(attempt_fence),
    sqlc.arg(source),
    sqlc.arg(disposition),
    sqlc.arg(attempt_state),
    sqlc.arg(failure_class),
    sqlc.arg(failure_fingerprint),
    sqlc.arg(request_hash),
    sqlc.arg(error_summary),
    sqlc.arg(backend_stage),
    sqlc.arg(gpu_uuids),
    sqlc.arg(inference_backend_revision),
    sqlc.arg(retry_recommended),
    sqlc.arg(worker_reusable),
    sqlc.arg(attempt_compute_seconds),
    sqlc.arg(total_compute_seconds),
    sqlc.narg(next_retry_at),
    sqlc.arg(job_fence),
    sqlc.arg(job_version),
    sqlc.arg(decided_at)
);

-- name: InsertRetryWaitOutboxEvent :exec
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
    'job.retry_wait',
    1,
    sqlc.arg(payload),
    sqlc.arg(occurred_at)
);

-- name: InsertJobFailedOutboxEvent :exec
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
    'job.failed',
    1,
    sqlc.arg(payload),
    sqlc.arg(occurred_at)
);
