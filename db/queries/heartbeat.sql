-- name: LockExecutionLeaseRenewalProtocol :one
SELECT vela_lock_execution_lease_renewal_protocol();

-- name: LockExecutionLeaseWrites :exec
LOCK TABLE attempt_leases IN ROW EXCLUSIVE MODE;

-- name: LockHeartbeatAuthority :one
SELECT
    l.id AS lease_id,
    l.phase AS lease_phase,
    l.token_digest,
    l.expires_at AS lease_expires_at,
    l.revoked_at AS lease_revoked_at,
    a.id AS attempt_id,
    a.job_id,
    a.state AS attempt_state,
    a.worker_id,
    a.worker_epoch,
    a.fence,
    j.organization_id,
    j.project_id,
    j.state AS job_state,
    j.execution_phase AS job_execution_phase,
    j.version AS job_version,
    j.current_fence,
    j.job_expires_at,
    a.finalization_deadline_at,
    cr.state AS credit_reservation_state
FROM attempt_leases AS l
JOIN attempts AS a ON a.id = l.attempt_id
JOIN jobs AS j ON j.id = a.job_id
JOIN credit_reservations AS cr ON cr.job_id = j.id
WHERE l.attempt_id = sqlc.arg(attempt_id)
  AND l.worker_id = sqlc.arg(worker_id)
  AND l.worker_epoch = sqlc.arg(worker_epoch)
  AND l.fence = sqlc.arg(fence)
  AND l.phase IN ('EXECUTION', 'FINALIZATION')
  AND l.owner_kind = 'WORKER'
  AND l.owner_id = sqlc.arg(owner_id)
  AND l.revoked_at IS NULL
  AND a.worker_id = sqlc.arg(worker_id)
  AND a.worker_epoch = sqlc.arg(worker_epoch)
  AND a.fence = sqlc.arg(fence)
LIMIT 1
FOR UPDATE OF l, a, j, cr;

-- name: GetAttemptProgressForHeartbeat :one
SELECT
    heartbeat_sequence,
    request_hash,
    execution_phase,
    progress_updated_at
FROM attempt_progress
WHERE attempt_id = sqlc.arg(attempt_id)
FOR UPDATE;

-- name: RenewExecutionLease :execrows
UPDATE attempt_leases
SET expires_at = sqlc.arg(expires_at),
    updated_at = sqlc.arg(updated_at)
WHERE id = sqlc.arg(lease_id)
  AND attempt_id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND phase = sqlc.arg(lease_phase)
  AND owner_kind = 'WORKER'
  AND revoked_at IS NULL
  AND expires_at = sqlc.arg(previous_expires_at);

-- name: UpsertAttemptProgress :execrows
INSERT INTO attempt_progress (
    attempt_id,
    organization_id,
    project_id,
    job_id,
    worker_id,
    worker_epoch,
    fence,
    heartbeat_sequence,
    request_hash,
    backend_stage,
    execution_phase,
    phase_progress,
    estimated_remaining_seconds,
    estimated_finish_at,
    gpu_health_summary,
    local_artifact_state,
    scratch_free_bytes,
    artifact_store_reachable,
    progress_updated_at,
    progress_valid_until,
    updated_at
) VALUES (
    sqlc.arg(attempt_id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(worker_id),
    sqlc.arg(worker_epoch),
    sqlc.arg(fence),
    sqlc.arg(heartbeat_sequence),
    sqlc.arg(request_hash),
    sqlc.arg(backend_stage),
    sqlc.arg(execution_phase),
    sqlc.narg(phase_progress),
    sqlc.narg(estimated_remaining_seconds),
    sqlc.narg(estimated_finish_at),
    sqlc.arg(gpu_health_summary),
    sqlc.arg(local_artifact_state),
    sqlc.arg(scratch_free_bytes),
    sqlc.arg(artifact_store_reachable),
    sqlc.arg(progress_updated_at),
    sqlc.arg(progress_valid_until),
    sqlc.arg(progress_updated_at)
)
ON CONFLICT (attempt_id) DO UPDATE
SET heartbeat_sequence = EXCLUDED.heartbeat_sequence,
    request_hash = EXCLUDED.request_hash,
    backend_stage = EXCLUDED.backend_stage,
    execution_phase = EXCLUDED.execution_phase,
    phase_progress = EXCLUDED.phase_progress,
    estimated_remaining_seconds = EXCLUDED.estimated_remaining_seconds,
    estimated_finish_at = EXCLUDED.estimated_finish_at,
    gpu_health_summary = EXCLUDED.gpu_health_summary,
    local_artifact_state = EXCLUDED.local_artifact_state,
    scratch_free_bytes = EXCLUDED.scratch_free_bytes,
    artifact_store_reachable = EXCLUDED.artifact_store_reachable,
    progress_updated_at = EXCLUDED.progress_updated_at,
    progress_valid_until = EXCLUDED.progress_valid_until,
    updated_at = EXCLUDED.updated_at
WHERE attempt_progress.heartbeat_sequence < EXCLUDED.heartbeat_sequence;

-- name: MarkWorkerHeartbeat :execrows
UPDATE workers
SET last_heartbeat_at = sqlc.arg(heartbeat_at),
    updated_at = sqlc.arg(heartbeat_at)
WHERE id = sqlc.arg(worker_id)
  AND epoch = sqlc.arg(worker_epoch);
