-- name: GetPostgresTime :one
SELECT clock_timestamp()::timestamptz;

-- name: ListActiveLeaseSigningKeyIDs :many
SELECT DISTINCT signing_key_id
FROM attempt_leases
WHERE revoked_at IS NULL
  AND expires_at > clock_timestamp()
ORDER BY signing_key_id;

-- name: ResolveWorkerBySPIFFEID :one
SELECT id
FROM workers
WHERE spiffe_id = sqlc.arg(spiffe_id);

-- name: LockWorkerAuthority :one
SELECT id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
FROM workers
WHERE id = sqlc.arg(worker_id)
FOR UPDATE;

-- name: GetActiveWorkerAssignment :one
SELECT
    a.id AS attempt_id,
    a.job_id,
	 j.model_revision_id,
	 j.generation_preset_revision_id,
    a.execution_profile_revision_id,
	 j.output_spec_id,
	 j.request_content::text AS request_content,
    a.attempt_number,
    a.worker_id,
    a.worker_epoch,
    a.fence,
    a.scheduler_dispatch_intent_id,
    l.signing_key_id,
    l.token_digest,
    l.issued_at,
    l.token_claim_expires_at,
    l.expires_at
FROM attempts AS a
JOIN attempt_leases AS l ON l.attempt_id = a.id
JOIN jobs AS j ON j.id = a.job_id
WHERE a.worker_id = sqlc.arg(worker_id)
  AND a.worker_epoch = sqlc.arg(worker_epoch)
  AND a.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
  AND l.phase = 'EXECUTION'
  AND l.owner_kind = 'WORKER'
  AND l.owner_id = sqlc.arg(owner_id)
  AND l.revoked_at IS NULL
ORDER BY a.assigned_at DESC
LIMIT 1
FOR UPDATE OF a, l;

-- name: LockJobForAssignment :one
SELECT
    j.id,
    j.organization_id,
    j.project_id,
    j.state,
    j.version,
    j.model_revision_id,
    j.generation_preset_revision_id,
	j.service_class_revision_id,
    scr.state AS service_class_revision_state,
    j.output_spec_id,
	j.request_content::text AS request_content,
    j.worker_pool_id,
    j.execution_max_attempts,
    j.execution_max_total_compute_seconds,
    j.job_expires_at,
    j.current_fence,
    cr.state AS credit_reservation_state,
    rts.attempts_started,
    rts.compute_seconds_consumed,
    rts.next_retry_at,
	ere.excluded_workers,
    rts.version AS retry_runtime_version
FROM jobs AS j
JOIN credit_reservations AS cr ON cr.job_id = j.id
JOIN retry_runtime_states AS rts ON rts.job_id = j.id
JOIN execution_retry_evidence AS ere ON ere.job_id = j.id
JOIN service_class_revisions AS scr ON scr.id = j.service_class_revision_id
WHERE j.id = sqlc.arg(job_id)
FOR UPDATE OF j, cr, rts, ere
FOR SHARE OF scr;

-- name: LockProjectForAssignment :one
SELECT queued_count, retry_wait_count, running_count, running_limit
FROM projects
WHERE organization_id = sqlc.arg(organization_id)
  AND id = sqlc.arg(project_id)
FOR UPDATE;

-- name: LockSchedulerDispatchForAssignment :one
SELECT
    id,
    worker_pool_id,
    organization_id,
	service_class_revision_id,
    project_id,
    job_id,
    expected_job_version,
    execution_profile_revision_id,
    worker_id,
    worker_epoch,
    lane,
    state,
    claim_expires_at
FROM scheduler_dispatch_intents
WHERE id = sqlc.arg(intent_id)
FOR UPDATE;

-- name: LockOrganizationCapacityForAssignment :one
SELECT capacity.running_limit
FROM organization_capacity_shares AS capacity
WHERE capacity.worker_pool_id = sqlc.arg(worker_pool_id)
  AND capacity.organization_id = sqlc.arg(organization_id)
FOR UPDATE;

-- name: CountActiveOrganizationAssignments :one
SELECT count(*)::bigint
FROM attempts
WHERE worker_pool_id = sqlc.arg(worker_pool_id)
  AND organization_id = sqlc.arg(organization_id)
  AND state IN ('ASSIGNED', 'RUNNING', 'FINALIZING');

-- name: LockAssignmentPoolCapacity :one
SELECT pool.retry_running_limit
FROM worker_pools AS pool
WHERE pool.id = sqlc.arg(worker_pool_id)
FOR UPDATE;

-- name: CountActiveRetryAssignments :one
SELECT count(*)::bigint
FROM attempts
WHERE worker_pool_id = sqlc.arg(worker_pool_id)
  AND attempt_number > 1
  AND state IN ('ASSIGNED', 'RUNNING', 'FINALIZING');

-- name: ValidateScheduledWorkerProfile :one
SELECT readiness.worker_id
FROM worker_profile_readiness AS readiness
WHERE readiness.worker_id = sqlc.arg(worker_id)
  AND readiness.worker_epoch = sqlc.arg(worker_epoch)
  AND readiness.execution_profile_revision_id = sqlc.arg(execution_profile_revision_id)
  AND readiness.readiness IN ('WARM', 'PREWARM_ALLOWED')
LIMIT 1
FOR SHARE;

-- name: ValidateProfileForAssignment :one
SELECT epr.id
FROM execution_profile_revisions AS epr
JOIN profile_certifications AS pc
  ON pc.execution_profile_revision_id = epr.id
WHERE epr.id = sqlc.arg(execution_profile_revision_id)
  AND epr.model_revision_id = sqlc.arg(model_revision_id)
  AND epr.worker_pool_id = sqlc.arg(worker_pool_id)
  AND epr.state = 'ACTIVE'
  AND pc.model_revision_id = sqlc.arg(model_revision_id)
  AND pc.generation_preset_revision_id = sqlc.arg(generation_preset_revision_id)
  AND pc.output_spec_id = sqlc.arg(output_spec_id)
  AND pc.state = 'ACTIVE'
  AND pc.invalidated_at IS NULL
LIMIT 1
FOR SHARE OF epr, pc;

-- name: InsertAttempt :exec
INSERT INTO attempts (
    id,
    organization_id,
    project_id,
    job_id,
    attempt_number,
    execution_profile_revision_id,
    worker_pool_id,
    worker_id,
    worker_epoch,
    scheduler_dispatch_intent_id,
    state,
    fence,
    assigned_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(attempt_number),
    sqlc.arg(execution_profile_revision_id),
    sqlc.arg(worker_pool_id),
    sqlc.arg(worker_id),
    sqlc.arg(worker_epoch),
    sqlc.narg(scheduler_dispatch_intent_id),
    'ASSIGNED',
    sqlc.arg(fence),
    sqlc.arg(assigned_at)
);

-- name: InsertExecutionLease :exec
INSERT INTO attempt_leases (
    id,
    organization_id,
    project_id,
    attempt_id,
    worker_id,
    worker_epoch,
    phase,
    owner_kind,
    owner_id,
    fence,
    token_digest,
    signing_key_id,
    issued_at,
    renewal_protocol_version,
    expires_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(attempt_id),
    sqlc.arg(worker_id),
    sqlc.arg(worker_epoch),
    'EXECUTION',
    'WORKER',
    sqlc.arg(owner_id),
    sqlc.arg(fence),
    sqlc.arg(token_digest),
    sqlc.arg(signing_key_id),
    sqlc.arg(issued_at),
    2,
    sqlc.arg(expires_at)
);

-- name: MarkJobAssigned :one
UPDATE jobs
SET state = 'ASSIGNED',
    version = version + 1,
    current_fence = sqlc.arg(fence),
    updated_at = sqlc.arg(assigned_at)
WHERE id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version)
  AND state IN ('QUEUED', 'RETRY_WAIT')
RETURNING version;

-- name: MarkWorkerBusy :execrows
UPDATE workers
SET lifecycle_state = 'BUSY', updated_at = sqlc.arg(assigned_at)
WHERE id = sqlc.arg(worker_id)
  AND epoch = sqlc.arg(worker_epoch)
  AND lifecycle_state = 'READY'
  AND reachability_condition = 'HEALTHY';

-- name: MoveProjectCountersToRunning :execrows
UPDATE projects
SET queued_count = queued_count - 1,
    running_count = running_count + 1
WHERE organization_id = sqlc.arg(organization_id)
  AND id = sqlc.arg(project_id)
  AND queued_count > 0
  AND running_count < running_limit;

-- name: DecrementPoolQueuedForAssignment :execrows
UPDATE worker_pools
SET queued_count = queued_count - 1
WHERE id = sqlc.arg(worker_pool_id)
  AND queued_count > 0;

-- name: IncrementAttemptsStarted :execrows
UPDATE retry_runtime_states
SET attempts_started = attempts_started + 1,
    next_retry_at = NULL,
    version = version + 1,
    updated_at = sqlc.arg(assigned_at)
WHERE job_id = sqlc.arg(job_id)
  AND attempts_started = sqlc.arg(previous_attempts_started)
  AND version = sqlc.arg(expected_version);

-- name: InsertAssignmentOutboxEvent :exec
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
    'job.assigned',
    1,
    sqlc.arg(payload),
    sqlc.arg(occurred_at)
);
