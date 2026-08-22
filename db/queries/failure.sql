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
    a.execution_profile_revision_id,
    a.profile_certification_id,
    a.state AS attempt_state,
    a.started_at AS attempt_started_at,
	 a.finalization_started_at,
	 a.finalization_deadline_at,
    a.worker_id,
    a.worker_epoch,
    a.fence AS attempt_fence,
    j.organization_id,
    j.project_id,
    j.worker_pool_id,
    j.state AS job_state,
    j.version AS job_version,
    j.current_fence,
    j.model_revision_id,
    j.generation_preset_revision_id,
    j.output_spec_id,
    j.execution_max_attempts,
    j.execution_max_total_compute_seconds,
	 j.execution_max_finalization_seconds_per_attempt,
    j.execution_retry_backoff_policy,
    j.execution_retryable_failure_classes,
    j.execution_circuit_fingerprint_window_seconds,
    j.execution_circuit_min_distinct_healthy_workers,
    j.job_expires_at,
    rts.attempts_started,
    rts.compute_seconds_consumed,
	 rts.finalization_seconds_consumed,
	 rts.finalization_retry_count,
    rts.circuit_breaker_state,
    ere.excluded_workers,
    ere.failure_fingerprints,
    rts.version AS retry_runtime_version
FROM attempt_leases AS l
JOIN attempts AS a ON a.id = l.attempt_id
JOIN jobs AS j ON j.id = a.job_id
JOIN retry_runtime_states AS rts ON rts.job_id = j.id
JOIN execution_retry_evidence AS ere ON ere.job_id = j.id
WHERE l.attempt_id = sqlc.arg(attempt_id)
ORDER BY (l.revoked_at IS NULL) DESC, l.issued_at DESC, l.id DESC
LIMIT 1
FOR UPDATE OF l, a, j, rts, ere;

-- name: LockExecutionFailureDecisionWrites :exec
LOCK TABLE execution_failure_decisions IN ROW EXCLUSIVE MODE;

-- name: LockProfileCircuitProtocol :one
SELECT
    protocol.require_circuit_aggregation::boolean AS require_circuit_aggregation,
    CASE
        WHEN protocol.require_circuit_aggregation THEN 2::smallint
        ELSE 1::smallint
    END AS circuit_protocol_version
FROM vela_lock_profile_circuit_protocol() AS protocol(
    require_circuit_aggregation,
    protocol_version
);

-- name: LockProfileCertificationForFailure :one
SELECT id, state, invalidated_at
FROM profile_certifications
WHERE id = sqlc.arg(profile_certification_id)
  AND execution_profile_revision_id = sqlc.arg(execution_profile_revision_id)
  AND model_revision_id = sqlc.arg(model_revision_id)
  AND generation_preset_revision_id = sqlc.arg(generation_preset_revision_id)
  AND output_spec_id = sqlc.arg(output_spec_id)
FOR UPDATE;

-- name: CountProfileCircuitHealthyWorkers :one
SELECT count(DISTINCT evidence.worker_id)::bigint
FROM (
    SELECT decision.worker_id
    FROM execution_failure_decisions AS decision
    JOIN attempts AS attempt ON attempt.id = decision.attempt_id
    WHERE attempt.profile_certification_id = sqlc.arg(profile_certification_id)
      AND decision.source = 'WORKER_REPORTED'
      AND decision.circuit_protocol_version = 2
      AND decision.worker_was_healthy
      AND decision.failure_fingerprint = sqlc.arg(failure_fingerprint)
      AND decision.decided_at >= sqlc.arg(evidence_window_started_at)
      AND decision.decided_at <= sqlc.arg(decided_at)
    UNION ALL
    SELECT sqlc.arg(current_worker_id)::uuid
    WHERE sqlc.arg(current_worker_was_healthy)::boolean
) AS evidence;

-- name: HasAlternateActiveProfileCertification :one
SELECT EXISTS (
    SELECT 1
    FROM profile_certifications AS certification
    JOIN execution_profile_revisions AS profile
      ON profile.id = certification.execution_profile_revision_id
    WHERE certification.model_revision_id = sqlc.arg(model_revision_id)
      AND certification.generation_preset_revision_id = sqlc.arg(generation_preset_revision_id)
      AND certification.output_spec_id = sqlc.arg(output_spec_id)
      AND certification.id <> sqlc.arg(profile_certification_id)
      AND certification.execution_profile_revision_id <>
          sqlc.arg(execution_profile_revision_id)
      AND certification.state = 'ACTIVE'
      AND certification.invalidated_at IS NULL
      AND profile.worker_pool_id = sqlc.arg(worker_pool_id)
      AND profile.state = 'ACTIVE'
);

-- name: InvalidateProfileCertificationForCircuit :execrows
UPDATE profile_certifications
SET state = 'INVALID', invalidated_at = sqlc.arg(opened_at)
WHERE id = sqlc.arg(profile_certification_id)
  AND state = 'ACTIVE'
  AND invalidated_at IS NULL;

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
      AND a.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
    LIMIT 1
) AS active_attempt ON true
WHERE j.state IN ('QUEUED', 'RETRY_WAIT', 'ASSIGNED', 'RUNNING', 'FINALIZING')
  AND j.job_expires_at <= clock_timestamp()
ORDER BY j.job_expires_at, j.id
LIMIT 1;

-- name: FindExpiredFinalizationDeadlineCandidate :one
SELECT a.job_id, a.id AS attempt_id, a.worker_id
FROM attempt_leases AS l
JOIN attempts AS a ON a.id = l.attempt_id
JOIN jobs AS j ON j.id = a.job_id
WHERE l.phase = 'FINALIZATION'
  AND l.owner_kind IN ('WORKER', 'RECONCILER')
  AND l.revoked_at IS NULL
  AND a.state = 'FINALIZING'
  AND j.state = 'FINALIZING'
  AND j.job_expires_at > clock_timestamp()
  AND a.finalization_deadline_at <= clock_timestamp()
ORDER BY a.finalization_deadline_at, a.id
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
	 rts.finalization_seconds_consumed,
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
	 source,
    disposition,
    failure_class,
    attempt_id,
    job_id,
    attempt_state,
    attempt_compute_seconds,
    total_compute_seconds,
	 attempt_finalization_seconds,
	 total_finalization_seconds,
    next_retry_at,
    job_fence,
    job_version,
    decided_at
    , artifact_id
    , artifact_upload_id
    , finalization_failure_code
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
  AND state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
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
  AND (
      (phase = 'EXECUTION' AND owner_kind = 'WORKER')
      OR (phase = 'FINALIZATION' AND owner_kind IN ('WORKER', 'RECONCILER'))
  )
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
	 finalization_seconds_consumed = sqlc.arg(total_finalization_seconds),
	 finalization_retry_count = finalization_retry_count
	     + sqlc.arg(finalization_retry_increment)::integer,
    next_retry_at = sqlc.narg(next_retry_at),
    circuit_breaker_state = sqlc.arg(circuit_breaker_state),
    last_failure_class = sqlc.arg(failure_class),
    version = version + 1,
    updated_at = sqlc.arg(decided_at)
WHERE job_id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version)
  AND compute_seconds_consumed = sqlc.arg(previous_compute_seconds)
  AND finalization_seconds_consumed = sqlc.arg(previous_finalization_seconds);

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
  AND state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
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
  AND state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
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
    circuit_protocol_version,
    worker_was_healthy,
    attempt_compute_seconds,
    total_compute_seconds,
	 attempt_finalization_seconds,
    total_finalization_seconds,
	 artifact_id,
	 artifact_upload_id,
	 finalization_failure_code,
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
    sqlc.arg(circuit_protocol_version),
    sqlc.arg(worker_was_healthy),
    sqlc.arg(attempt_compute_seconds),
    sqlc.arg(total_compute_seconds),
	 sqlc.arg(attempt_finalization_seconds),
	 sqlc.arg(total_finalization_seconds),
	 sqlc.narg(artifact_id),
	 sqlc.narg(artifact_upload_id),
	 sqlc.narg(finalization_failure_code),
    sqlc.narg(next_retry_at),
    sqlc.arg(job_fence),
    sqlc.arg(job_version),
    sqlc.arg(decided_at)
);

-- name: InsertProfileCertificationCircuitOpening :exec
INSERT INTO profile_certification_circuit_openings (
    id,
    organization_id,
    project_id,
    profile_certification_id,
    execution_profile_revision_id,
    triggering_execution_failure_decision_id,
    triggering_job_id,
    triggering_attempt_id,
    triggering_worker_id,
    triggering_worker_epoch,
    triggering_attempt_fence,
    failure_class,
    failure_fingerprint,
    inference_backend_revision,
    policy_fingerprint_window_seconds,
    policy_min_distinct_healthy_workers,
    observed_distinct_healthy_workers,
    evidence_window_started_at,
    opened_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(profile_certification_id),
    sqlc.arg(execution_profile_revision_id),
    sqlc.arg(triggering_execution_failure_decision_id),
    sqlc.arg(triggering_job_id),
    sqlc.arg(triggering_attempt_id),
    sqlc.arg(triggering_worker_id),
    sqlc.arg(triggering_worker_epoch),
    sqlc.arg(triggering_attempt_fence),
    sqlc.arg(failure_class),
    sqlc.arg(failure_fingerprint),
    sqlc.arg(inference_backend_revision),
    sqlc.arg(policy_fingerprint_window_seconds),
    sqlc.arg(policy_min_distinct_healthy_workers),
    sqlc.arg(observed_distinct_healthy_workers),
    sqlc.arg(evidence_window_started_at),
    sqlc.arg(opened_at)
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
