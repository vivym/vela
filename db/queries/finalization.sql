-- name: LockFinalizationAuthority :one
SELECT
    l.id AS lease_id,
    l.phase AS lease_phase,
    l.owner_kind AS lease_owner_kind,
    l.owner_id AS lease_owner_id,
    l.token_digest,
	 l.signing_key_id,
	 l.issued_at AS lease_issued_at,
    l.expires_at AS lease_expires_at,
    l.revoked_at AS lease_revoked_at,
    a.id AS attempt_id,
    a.job_id,
    a.state AS attempt_state,
    a.worker_id,
    a.worker_epoch,
    a.fence AS attempt_fence,
    a.finalization_started_at,
    a.finalization_deadline_at,
    j.organization_id,
    j.project_id,
    j.state AS job_state,
    j.version AS job_version,
    j.current_fence,
    j.job_expires_at,
    j.execution_max_finalization_seconds_per_attempt,
    j.pricing_quantity AS generation_count,
    os.container,
    cr.state AS credit_reservation_state
FROM attempt_leases AS l
JOIN attempts AS a ON a.id = l.attempt_id
JOIN jobs AS j ON j.id = a.job_id
JOIN output_specs AS os ON os.id = j.output_spec_id
JOIN credit_reservations AS cr ON cr.job_id = j.id
WHERE l.attempt_id = sqlc.arg(attempt_id)
  AND l.worker_id = sqlc.arg(worker_id)
  AND l.worker_epoch = sqlc.arg(worker_epoch)
  AND l.fence = sqlc.arg(fence)
  AND l.phase IN ('EXECUTION', 'FINALIZATION')
  AND l.owner_kind = sqlc.arg(owner_kind)
  AND l.owner_id = sqlc.arg(owner_id)
  AND l.revoked_at IS NULL
  AND a.worker_id = sqlc.arg(worker_id)
  AND a.worker_epoch = sqlc.arg(worker_epoch)
  AND a.fence = sqlc.arg(fence)
LIMIT 1
FOR UPDATE OF l, a, j;

-- name: ExpireStageGraphFinalizationClaims :exec
UPDATE stage_graph_finalization_claims
SET state = 'EXPIRED',
    expired_at = sqlc.arg(expired_at),
    updated_at = sqlc.arg(expired_at)
WHERE state = 'ACTIVE'
  AND expires_at <= sqlc.arg(expired_at);

-- name: FindActiveStageGraphFinalizationClaim :one
SELECT
    claim.id AS claim_id,
    claim.organization_id,
    claim.project_id,
    claim.job_id,
    claim.attempt_id,
    claim.attempt_fence,
    claim.final_stage_run_id,
    claim.final_stage_artifact_id,
    claim.exact_object_version,
    claim.owner_id,
    claim.token_digest,
    claim.signing_key_id,
    claim.output_set_digest,
    claim.issued_at,
    claim.expires_at,
    attempt.finalization_started_at,
    attempt.finalization_deadline_at,
    job.version AS job_version,
    artifact.object_key,
    artifact.content_type,
    artifact.sha256,
    artifact.size_bytes,
    artifact.stage_interface_revision_id,
    artifact.expires_at AS artifact_expires_at
FROM stage_graph_finalization_claims AS claim
JOIN attempts AS attempt ON attempt.id = claim.attempt_id
JOIN jobs AS job ON job.id = claim.job_id
JOIN stage_artifacts AS artifact ON artifact.id = claim.final_stage_artifact_id
WHERE claim.owner_id = sqlc.arg(owner_id)
  AND claim.state = 'ACTIVE'
  AND claim.expires_at > sqlc.arg(observed_at)
  AND attempt.state = 'FINALIZING'
  AND attempt.graph_state = 'FINALIZING'
  AND attempt.worker_id IS NULL
  AND attempt.fence = claim.attempt_fence
  AND attempt.finalization_deadline_at > sqlc.arg(observed_at)
  AND job.state = 'FINALIZING'
  AND job.current_fence = claim.attempt_fence
  AND job.job_expires_at > sqlc.arg(observed_at)
  AND artifact.state = 'COMMITTED'
  AND artifact.object_version = claim.exact_object_version
ORDER BY claim.issued_at, claim.id
LIMIT 1
FOR UPDATE OF claim, attempt, job;

-- name: ListStageGraphFinalizationOutputs :many
SELECT
    output.output_key,
    output.interface_revision_id AS stage_interface_revision_id,
    run.id AS stage_run_id,
    artifact.id AS stage_artifact_id,
    artifact.object_key,
    artifact.object_version,
    artifact.content_type,
    artifact.sha256,
    artifact.size_bytes,
    artifact.expires_at
FROM stage_runs AS run
JOIN execution_graph_outputs AS output
  ON output.execution_graph_revision_id = run.execution_graph_revision_id
 AND output.source_stage_key = run.stage_key
JOIN stage_artifacts AS artifact
  ON artifact.id = run.winner_stage_artifact_id
 AND artifact.producer_stage_run_id = run.id
 AND artifact.output_port = output.source_port
 AND artifact.stage_interface_revision_id = output.interface_revision_id
WHERE run.attempt_id = sqlc.arg(attempt_id)
  AND run.state = 'SUCCEEDED'
  AND output.required
  AND artifact.state = 'COMMITTED'
ORDER BY output.output_key;

-- name: InsertStageGraphFinalizationClaimOutput :exec
INSERT INTO stage_graph_finalization_claim_outputs (
    claim_id, organization_id, project_id, attempt_id, output_key,
    artifact_kind, ordinal, stage_run_id, stage_artifact_id,
    stage_interface_revision_id, exact_object_version
) VALUES (
    sqlc.arg(claim_id), sqlc.arg(organization_id), sqlc.arg(project_id),
    sqlc.arg(attempt_id), sqlc.arg(output_key), sqlc.arg(artifact_kind),
    sqlc.arg(ordinal), sqlc.arg(stage_run_id), sqlc.arg(stage_artifact_id),
    sqlc.arg(stage_interface_revision_id), sqlc.arg(exact_object_version)
);

-- name: ListStageGraphFinalizationClaimOutputs :many
SELECT
    output.output_key,
    output.artifact_kind,
    output.ordinal,
    output.stage_run_id,
    output.stage_artifact_id,
    output.stage_interface_revision_id,
    output.exact_object_version,
    artifact.object_key,
    artifact.content_type,
    artifact.sha256,
    artifact.size_bytes,
    artifact.expires_at
FROM stage_graph_finalization_claim_outputs AS output
JOIN stage_artifacts AS artifact
  ON artifact.id = output.stage_artifact_id
 AND artifact.producer_stage_run_id = output.stage_run_id
 AND artifact.stage_interface_revision_id = output.stage_interface_revision_id
 AND artifact.object_version = output.exact_object_version
WHERE output.claim_id = sqlc.arg(claim_id)
ORDER BY output.output_key;

-- name: LockNextStageGraphFinalizationCandidate :one
SELECT
    attempt.organization_id,
    attempt.project_id,
    attempt.job_id,
    attempt.id AS attempt_id,
    attempt.fence AS attempt_fence,
    attempt.finalization_started_at,
    attempt.finalization_deadline_at,
    job.version AS job_version,
    job.job_expires_at,
    run.id AS final_stage_run_id,
    artifact.id AS final_stage_artifact_id,
    artifact.object_key,
    artifact.object_version,
    artifact.content_type,
    artifact.sha256,
    artifact.size_bytes,
    artifact.stage_interface_revision_id,
    artifact.expires_at AS artifact_expires_at
FROM attempts AS attempt
JOIN jobs AS job ON job.id = attempt.job_id
JOIN stage_runs AS run ON run.attempt_id = attempt.id
JOIN stage_artifacts AS artifact ON artifact.id = run.winner_stage_artifact_id
WHERE attempt.state = 'FINALIZING'
  AND attempt.graph_state = 'FINALIZING'
  AND attempt.worker_id IS NULL
  AND attempt.finalization_deadline_at > sqlc.arg(observed_at)
  AND job.state = 'FINALIZING'
  AND job.current_fence = attempt.fence
  AND job.job_expires_at > sqlc.arg(observed_at)
  AND run.state = 'SUCCEEDED'
  AND artifact.state = 'COMMITTED'
  AND artifact.expires_at > sqlc.arg(observed_at)
  AND NOT EXISTS (
      SELECT 1
      FROM stage_dependencies AS outgoing
      WHERE outgoing.attempt_id = attempt.id
        AND outgoing.source_stage_run_id = run.id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM stage_graph_finalization_claims AS active_claim
      WHERE active_claim.attempt_id = attempt.id
        AND active_claim.state = 'ACTIVE'
  )
ORDER BY attempt.finalization_started_at, attempt.id, run.id
LIMIT 1
FOR UPDATE OF attempt, job SKIP LOCKED;

-- name: InsertStageGraphFinalizationClaim :exec
INSERT INTO stage_graph_finalization_claims (
    id, organization_id, project_id, job_id, attempt_id, attempt_fence,
    final_stage_run_id, final_stage_artifact_id, exact_object_version,
    owner_id, token_digest, signing_key_id, output_set_digest, issued_at, expires_at
) VALUES (
    sqlc.arg(claim_id), sqlc.arg(organization_id), sqlc.arg(project_id),
    sqlc.arg(job_id), sqlc.arg(attempt_id), sqlc.arg(attempt_fence),
    sqlc.arg(final_stage_run_id), sqlc.arg(final_stage_artifact_id),
    sqlc.arg(exact_object_version), sqlc.arg(owner_id), sqlc.arg(token_digest),
    sqlc.arg(signing_key_id), sqlc.arg(output_set_digest),
    sqlc.arg(issued_at), sqlc.arg(expires_at)
);

-- name: FindActiveReconcilerFinalizationCandidate :one
SELECT a.id AS attempt_id, a.worker_id, a.worker_epoch, a.fence
FROM attempt_leases AS lease
JOIN attempts AS a ON a.id = lease.attempt_id
JOIN jobs AS job ON job.id = a.job_id
WHERE lease.phase = 'FINALIZATION'
  AND lease.owner_kind = 'RECONCILER'
  AND lease.owner_id = sqlc.arg(owner_id)
  AND lease.revoked_at IS NULL
  AND lease.expires_at > clock_timestamp()
  AND a.state = 'FINALIZING'
  AND a.finalization_deadline_at > clock_timestamp()
  AND job.state = 'FINALIZING'
  AND job.job_expires_at > clock_timestamp()
ORDER BY lease.issued_at, lease.id
LIMIT 1;

-- name: FindRecoverableExpiredFinalizationCandidate :one
SELECT
    a.id AS attempt_id,
    a.worker_id,
    a.worker_epoch,
    a.fence,
    lease.owner_kind,
    lease.owner_id
FROM attempt_leases AS lease
JOIN attempts AS a ON a.id = lease.attempt_id
JOIN jobs AS job ON job.id = a.job_id
WHERE lease.phase = 'FINALIZATION'
  AND lease.owner_kind IN ('WORKER', 'RECONCILER')
  AND lease.revoked_at IS NULL
  AND lease.expires_at <= clock_timestamp()
  AND a.state = 'FINALIZING'
  AND a.finalization_deadline_at > clock_timestamp()
  AND job.state = 'FINALIZING'
  AND job.job_expires_at > clock_timestamp()
  AND NOT EXISTS (
      SELECT 1
      FROM artifact_uploads AS upload
      WHERE upload.attempt_id = a.id
        AND upload.attempt_fence = a.fence
        AND upload.state NOT IN ('UPLOADED', 'VERIFIED')
        AND NOT (
            upload.state = 'UPLOADING'
            AND upload.multipart_upload_id IS NOT NULL
            AND upload.completion_request_hash IS NOT NULL
            AND upload.size_bytes IS NOT NULL
            AND upload.sha256 IS NOT NULL
            AND upload.content_type IS NOT NULL
            AND jsonb_array_length(upload.completed_parts) > 0
        )
  )
  AND EXISTS (
      SELECT 1
      FROM artifact_uploads AS upload
      WHERE upload.attempt_id = a.id
        AND upload.attempt_fence = a.fence
  )
ORDER BY lease.expires_at, lease.id
LIMIT 1;

-- name: RevokeExpiredFinalizationLeaseForTakeover :execrows
UPDATE attempt_leases
SET revoked_at = sqlc.arg(taken_over_at),
    updated_at = sqlc.arg(taken_over_at)
WHERE id = sqlc.arg(lease_id)
  AND attempt_id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND phase = 'FINALIZATION'
  AND owner_kind = sqlc.arg(previous_owner_kind)
  AND owner_id = sqlc.arg(previous_owner_id)
  AND revoked_at IS NULL
  AND expires_at <= sqlc.arg(taken_over_at);

-- name: InsertReconcilerFinalizationLease :exec
INSERT INTO attempt_leases (
    id, organization_id, project_id, attempt_id, worker_id, worker_epoch,
    phase, owner_kind, owner_id, fence, token_digest, signing_key_id,
    issued_at, expires_at
) VALUES (
    sqlc.arg(lease_id), sqlc.arg(organization_id), sqlc.arg(project_id),
    sqlc.arg(attempt_id), sqlc.arg(worker_id), sqlc.arg(worker_epoch),
    'FINALIZATION', 'RECONCILER', sqlc.arg(owner_id), sqlc.arg(fence),
    sqlc.arg(token_digest), sqlc.arg(signing_key_id),
    sqlc.arg(issued_at), sqlc.arg(expires_at)
);

-- name: MarkAttemptFinalizing :execrows
UPDATE attempts
SET state = 'FINALIZING',
    finalization_started_at = sqlc.arg(finalization_started_at),
    finalization_deadline_at = sqlc.arg(finalization_deadline_at),
    updated_at = sqlc.arg(finalization_started_at)
WHERE id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND state = 'RUNNING'
  AND finalization_started_at IS NULL
  AND finalization_deadline_at IS NULL;

-- name: MarkJobFinalizing :one
UPDATE jobs
SET state = 'FINALIZING',
    version = version + 1,
    updated_at = sqlc.arg(finalization_started_at)
WHERE id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version)
  AND current_fence = sqlc.arg(fence)
  AND state = 'RUNNING'
RETURNING version;

-- name: MarkLeaseFinalizing :execrows
UPDATE attempt_leases
SET phase = 'FINALIZATION',
	 expires_at = LEAST(expires_at, sqlc.arg(finalization_deadline_at)),
    updated_at = sqlc.arg(finalization_started_at)
WHERE id = sqlc.arg(lease_id)
  AND attempt_id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND phase = 'EXECUTION'
  AND owner_kind = 'WORKER'
  AND revoked_at IS NULL;

-- name: InsertPlannedArtifact :exec
INSERT INTO artifacts (
    id, organization_id, project_id, job_id, attempt_id, attempt_fence,
    kind, ordinal, object_key, expected_content_type, expires_at
) VALUES (
    sqlc.arg(artifact_id), sqlc.arg(organization_id), sqlc.arg(project_id),
    sqlc.arg(job_id), sqlc.arg(attempt_id), sqlc.arg(attempt_fence),
    sqlc.arg(kind), sqlc.arg(ordinal), sqlc.arg(object_key),
    sqlc.arg(expected_content_type), sqlc.arg(expires_at)
);

-- name: InsertPlannedArtifactUpload :exec
INSERT INTO artifact_uploads (
    id, organization_id, project_id, job_id, attempt_id, attempt_fence,
    artifact_id, expires_at
) VALUES (
    sqlc.arg(upload_id), sqlc.arg(organization_id), sqlc.arg(project_id),
    sqlc.arg(job_id), sqlc.arg(attempt_id), sqlc.arg(attempt_fence),
    sqlc.arg(artifact_id), sqlc.arg(expires_at)
);

-- name: ListFinalizationArtifacts :many
SELECT
    a.id AS artifact_id,
    u.id AS upload_id,
    a.kind,
    a.ordinal,
    a.object_key,
    u.expires_at
FROM artifacts AS a
JOIN artifact_uploads AS u ON u.artifact_id = a.id
WHERE a.attempt_id = sqlc.arg(attempt_id)
  AND a.attempt_fence = sqlc.arg(attempt_fence)
ORDER BY CASE a.kind WHEN 'VIDEO' THEN 0 ELSE 1 END, a.ordinal;

-- name: ClaimArtifactUpload :one
UPDATE artifact_uploads AS upload
SET claim_id = CASE
        WHEN upload.multipart_upload_id IS NOT NULL THEN upload.claim_id
        ELSE sqlc.arg(claim_id)
    END,
    claim_owner_kind = CASE
        WHEN upload.multipart_upload_id IS NOT NULL THEN upload.claim_owner_kind
        ELSE 'WORKER'
    END,
    claim_owner_id = CASE
        WHEN upload.multipart_upload_id IS NOT NULL THEN upload.claim_owner_id
        ELSE sqlc.arg(claim_owner_id)
    END,
    claim_expires_at = CASE
        WHEN upload.multipart_upload_id IS NOT NULL
            OR upload.claim_id = sqlc.arg(claim_id) THEN upload.claim_expires_at
        ELSE sqlc.arg(claim_expires_at)
    END,
    version = CASE
        WHEN upload.multipart_upload_id IS NOT NULL
            OR upload.claim_id = sqlc.arg(claim_id) THEN upload.version
        ELSE upload.version + 1
    END,
    updated_at = CASE
        WHEN upload.multipart_upload_id IS NOT NULL
            OR upload.claim_id = sqlc.arg(claim_id) THEN upload.updated_at
        ELSE sqlc.arg(claimed_at)
    END
FROM artifacts AS artifact
WHERE upload.id = sqlc.arg(upload_id)
  AND upload.artifact_id = artifact.id
  AND upload.attempt_id = sqlc.arg(attempt_id)
  AND upload.attempt_fence = sqlc.arg(attempt_fence)
  AND upload.state IN ('INITIATED', 'UPLOADING')
  AND upload.expires_at > sqlc.arg(claimed_at)
  AND (
      upload.multipart_upload_id IS NOT NULL
      OR upload.claim_id = sqlc.arg(claim_id)
      OR upload.claim_expires_at IS NULL
      OR upload.claim_expires_at <= sqlc.arg(claimed_at)
  )
RETURNING
    upload.id AS upload_id,
    upload.artifact_id,
    artifact.object_key,
    artifact.expected_content_type,
    upload.claim_id,
    upload.claim_expires_at,
    upload.multipart_upload_id,
    upload.expires_at,
    upload.version;

-- name: GetArtifactUploadStatus :one
SELECT
    upload.id AS upload_id,
    upload.artifact_id,
    upload.state,
    artifact.object_key,
    artifact.expected_content_type,
    upload.multipart_upload_id,
    upload.completed_parts,
    upload.object_version_id,
    upload.size_bytes,
    upload.sha256,
    upload.content_type,
    upload.completion_request_hash,
    upload.expires_at,
    upload.version
FROM artifact_uploads AS upload
JOIN artifacts AS artifact ON artifact.id = upload.artifact_id
WHERE upload.id = sqlc.arg(upload_id)
  AND upload.attempt_id = sqlc.arg(attempt_id)
  AND upload.attempt_fence = sqlc.arg(attempt_fence)
FOR UPDATE OF upload, artifact;

-- name: RecordArtifactMultipartSession :one
UPDATE artifact_uploads AS upload
SET multipart_upload_id = CASE
        WHEN upload.multipart_upload_id = sqlc.arg(multipart_upload_id)
            THEN upload.multipart_upload_id
        ELSE sqlc.arg(multipart_upload_id)
    END,
    state = CASE
        WHEN upload.multipart_upload_id = sqlc.arg(multipart_upload_id)
            THEN upload.state
        ELSE 'UPLOADING'
    END,
    claim_id = NULL,
    claim_owner_kind = NULL,
    claim_owner_id = NULL,
    claim_expires_at = NULL,
    version = CASE
        WHEN upload.multipart_upload_id = sqlc.arg(multipart_upload_id)
            THEN upload.version
        ELSE upload.version + 1
    END,
    updated_at = CASE
        WHEN upload.multipart_upload_id = sqlc.arg(multipart_upload_id)
            THEN upload.updated_at
        ELSE sqlc.arg(recorded_at)
    END
FROM artifacts AS artifact
WHERE upload.id = sqlc.arg(upload_id)
  AND upload.artifact_id = artifact.id
  AND upload.attempt_id = sqlc.arg(attempt_id)
  AND upload.attempt_fence = sqlc.arg(attempt_fence)
  AND (
      (
          upload.multipart_upload_id IS NULL
          AND upload.state = 'INITIATED'
          AND upload.claim_id = sqlc.arg(claim_id)
          AND upload.claim_owner_kind = 'WORKER'
          AND upload.claim_owner_id = sqlc.arg(claim_owner_id)
          AND upload.claim_expires_at > sqlc.arg(recorded_at)
      ) OR (
          upload.multipart_upload_id = sqlc.arg(multipart_upload_id)
          AND upload.state IN ('UPLOADING', 'UPLOADED', 'VERIFIED')
      )
  )
RETURNING
    upload.id AS upload_id,
    upload.artifact_id,
    upload.multipart_upload_id,
    upload.version;

-- name: RecordArtifactCompletionIntent :one
UPDATE artifact_uploads AS upload
SET completed_parts = sqlc.arg(completed_parts),
    size_bytes = sqlc.arg(size_bytes),
    sha256 = sqlc.arg(sha256),
    content_type = sqlc.arg(content_type),
    completion_request_hash = sqlc.arg(completion_request_hash),
    version = CASE
        WHEN upload.completion_request_hash IS NULL THEN upload.version + 1
        ELSE upload.version
    END,
    updated_at = CASE
        WHEN upload.completion_request_hash IS NULL THEN sqlc.arg(recorded_at)
        ELSE upload.updated_at
    END
FROM artifacts AS artifact
WHERE upload.id = sqlc.arg(upload_id)
  AND upload.artifact_id = artifact.id
  AND upload.attempt_id = sqlc.arg(attempt_id)
  AND upload.attempt_fence = sqlc.arg(attempt_fence)
  AND upload.state = 'UPLOADING'
  AND upload.multipart_upload_id IS NOT NULL
  AND artifact.expected_content_type = sqlc.arg(content_type)
  AND (
      upload.completion_request_hash IS NULL
      OR (
          upload.completion_request_hash = sqlc.arg(completion_request_hash)
          AND upload.completed_parts = sqlc.arg(completed_parts)
          AND upload.size_bytes = sqlc.arg(size_bytes)
          AND upload.sha256 = sqlc.arg(sha256)
          AND upload.content_type = sqlc.arg(content_type)
      )
  )
RETURNING
    upload.id AS upload_id,
    upload.artifact_id,
    upload.version;

-- name: RecordArtifactUploaded :one
WITH updated_upload AS (
    UPDATE artifact_uploads AS upload
    SET state = CASE
            WHEN upload.state = 'UPLOADING' THEN 'UPLOADED'
            ELSE upload.state
        END,
        completed_parts = sqlc.arg(completed_parts),
        object_version_id = sqlc.arg(object_version_id),
        size_bytes = sqlc.arg(size_bytes),
        sha256 = sqlc.arg(sha256),
        content_type = sqlc.arg(content_type),
        completion_request_hash = sqlc.arg(completion_request_hash),
        uploaded_at = CASE
            WHEN upload.state = 'UPLOADING' THEN sqlc.arg(uploaded_at)
            ELSE upload.uploaded_at
        END,
        version = CASE
            WHEN upload.state = 'UPLOADING' THEN upload.version + 1
            ELSE upload.version
        END,
        updated_at = CASE
            WHEN upload.state = 'UPLOADING' THEN sqlc.arg(uploaded_at)
            ELSE upload.updated_at
        END
    FROM artifacts AS artifact
    WHERE upload.id = sqlc.arg(upload_id)
      AND upload.artifact_id = artifact.id
      AND upload.attempt_id = sqlc.arg(attempt_id)
      AND upload.attempt_fence = sqlc.arg(attempt_fence)
      AND upload.multipart_upload_id IS NOT NULL
      AND artifact.expected_content_type = sqlc.arg(content_type)
      AND (
          (
              upload.state = 'UPLOADING'
              AND (
                  upload.completion_request_hash IS NULL
                  OR (
                      upload.completion_request_hash = sqlc.arg(completion_request_hash)
                      AND upload.completed_parts = sqlc.arg(completed_parts)
                      AND upload.size_bytes = sqlc.arg(size_bytes)
                      AND upload.sha256 = sqlc.arg(sha256)
                      AND upload.content_type = sqlc.arg(content_type)
                  )
              )
          ) OR (
              upload.state IN ('UPLOADED', 'VERIFIED')
              AND upload.completion_request_hash = sqlc.arg(completion_request_hash)
              AND upload.completed_parts = sqlc.arg(completed_parts)
              AND upload.object_version_id = sqlc.arg(object_version_id)
              AND upload.size_bytes = sqlc.arg(size_bytes)
              AND upload.sha256 = sqlc.arg(sha256)
              AND upload.content_type = sqlc.arg(content_type)
          )
      )
    RETURNING
        upload.id,
        upload.artifact_id,
        upload.object_version_id,
        upload.size_bytes,
        upload.sha256,
        upload.content_type,
        upload.version,
        upload.uploaded_at
)
UPDATE artifacts AS artifact
SET state = CASE
        WHEN artifact.state = 'STAGING' THEN 'UPLOADED'
        ELSE artifact.state
    END,
    object_version_id = uploaded.object_version_id,
    size_bytes = uploaded.size_bytes,
    sha256 = uploaded.sha256,
    content_type = uploaded.content_type,
    uploaded_at = uploaded.uploaded_at,
    updated_at = CASE
        WHEN artifact.state = 'STAGING' THEN uploaded.uploaded_at
        ELSE artifact.updated_at
    END
FROM updated_upload AS uploaded
WHERE artifact.id = uploaded.artifact_id
  AND (
      artifact.state = 'STAGING'
      OR (
          artifact.state IN ('UPLOADED', 'VERIFIED')
          AND artifact.object_version_id = uploaded.object_version_id
          AND artifact.size_bytes = uploaded.size_bytes
          AND artifact.sha256 = uploaded.sha256
          AND artifact.content_type = uploaded.content_type
      )
  )
RETURNING
    uploaded.id AS upload_id,
    artifact.id AS artifact_id,
    artifact.object_version_id,
    uploaded.version;

-- name: GetRecordedArtifactVerification :one
SELECT
    upload.id AS upload_id,
    upload.artifact_id,
    upload.verification_id,
    artifact.object_version_id,
    upload.version,
    upload.verified_at
FROM artifact_uploads AS upload
JOIN artifacts AS artifact ON artifact.id = upload.artifact_id
WHERE upload.id = sqlc.arg(upload_id)
  AND upload.attempt_id = sqlc.arg(attempt_id)
  AND upload.attempt_fence = sqlc.arg(attempt_fence)
  AND upload.state = 'VERIFIED'
  AND artifact.state = 'VERIFIED'
LIMIT 1
FOR UPDATE OF upload, artifact;

-- name: ClaimArtifactVerification :one
UPDATE artifact_uploads AS upload
SET claim_id = sqlc.arg(verification_id),
    claim_owner_kind = sqlc.arg(claim_owner_kind),
    claim_owner_id = sqlc.arg(claim_owner_id),
    claim_expires_at = CASE
        WHEN upload.claim_id = sqlc.arg(verification_id) THEN upload.claim_expires_at
        ELSE sqlc.arg(claim_expires_at)
    END,
    version = CASE
        WHEN upload.claim_id = sqlc.arg(verification_id) THEN upload.version
        ELSE upload.version + 1
    END,
    updated_at = CASE
        WHEN upload.claim_id = sqlc.arg(verification_id) THEN upload.updated_at
        ELSE sqlc.arg(claimed_at)
    END
FROM artifacts AS artifact
JOIN jobs AS job ON job.id = artifact.job_id
JOIN output_specs AS output_spec ON output_spec.id = job.output_spec_id
WHERE upload.id = sqlc.arg(upload_id)
  AND upload.artifact_id = artifact.id
  AND upload.attempt_id = sqlc.arg(attempt_id)
  AND upload.attempt_fence = sqlc.arg(attempt_fence)
  AND upload.state = 'UPLOADED'
  AND artifact.state = 'UPLOADED'
  AND upload.expires_at > sqlc.arg(claimed_at)
  AND (
      upload.claim_id = sqlc.arg(verification_id)
      OR upload.claim_expires_at IS NULL
      OR upload.claim_expires_at <= sqlc.arg(claimed_at)
  )
RETURNING
    upload.id AS upload_id,
    upload.artifact_id,
    artifact.kind,
    artifact.ordinal,
    artifact.object_key,
    artifact.object_version_id,
    artifact.size_bytes,
    artifact.sha256,
    artifact.content_type,
    output_spec.width AS expected_width,
    output_spec.height AS expected_height,
    output_spec.duration_milliseconds AS expected_duration_milliseconds,
    output_spec.frame_rate_milli AS expected_frame_rate_milli,
    output_spec.codec AS expected_codec,
    output_spec.container AS expected_container,
    upload.claim_id AS verification_id,
    upload.claim_expires_at,
    upload.version;

-- name: RecordArtifactVerified :one
WITH verified_upload AS (
    UPDATE artifact_uploads AS upload
    SET state = 'VERIFIED',
        verification_id = sqlc.arg(verification_id),
        verification_request_hash = sqlc.arg(verification_request_hash),
        validation_receipt = sqlc.arg(validation_receipt),
        verified_at = sqlc.arg(verified_at),
        claim_id = NULL,
        claim_owner_kind = NULL,
        claim_owner_id = NULL,
        claim_expires_at = NULL,
        version = upload.version + 1,
        updated_at = sqlc.arg(verified_at)
    FROM artifacts AS artifact
    WHERE upload.id = sqlc.arg(upload_id)
      AND upload.artifact_id = artifact.id
      AND upload.attempt_id = sqlc.arg(attempt_id)
      AND upload.attempt_fence = sqlc.arg(attempt_fence)
      AND upload.state = 'UPLOADED'
      AND artifact.state = 'UPLOADED'
      AND upload.claim_id = sqlc.arg(verification_id)
      AND upload.claim_owner_kind = sqlc.arg(claim_owner_kind)
      AND upload.claim_owner_id = sqlc.arg(claim_owner_id)
      AND upload.claim_expires_at > sqlc.arg(verified_at)
      AND artifact.object_version_id = sqlc.arg(object_version_id)
      AND artifact.size_bytes = sqlc.arg(size_bytes)
      AND artifact.sha256 = sqlc.arg(sha256)
      AND artifact.content_type = sqlc.arg(content_type)
    RETURNING
        upload.id,
        upload.artifact_id,
        upload.verification_id,
        upload.version,
        upload.verified_at
)
UPDATE artifacts AS artifact
SET state = 'VERIFIED',
    verification_id = verified.verification_id,
    verification_request_hash = sqlc.arg(verification_request_hash),
    validation_receipt = sqlc.arg(validation_receipt),
    verified_at = verified.verified_at,
    updated_at = verified.verified_at
FROM verified_upload AS verified
WHERE artifact.id = verified.artifact_id
  AND artifact.state = 'UPLOADED'
RETURNING
    verified.id AS upload_id,
    artifact.id AS artifact_id,
    artifact.verification_id,
    artifact.object_version_id,
    verified.version,
    artifact.verified_at;

-- name: ReleaseArtifactVerificationClaim :execrows
UPDATE artifact_uploads
SET claim_id = NULL,
    claim_owner_kind = NULL,
    claim_owner_id = NULL,
    claim_expires_at = NULL,
    version = version + 1,
    updated_at = sqlc.arg(released_at)
WHERE id = sqlc.arg(upload_id)
  AND attempt_id = sqlc.arg(attempt_id)
  AND attempt_fence = sqlc.arg(attempt_fence)
  AND state = 'UPLOADED'
  AND claim_id = sqlc.arg(verification_id)
  AND claim_owner_kind = sqlc.arg(claim_owner_kind)
  AND claim_owner_id = sqlc.arg(claim_owner_id);

-- name: LockVisibleCompletionAuthority :one
SELECT
    lease.id AS lease_id,
    lease.phase AS lease_phase,
    lease.owner_kind AS lease_owner_kind,
    lease.owner_id AS lease_owner_id,
    lease.token_digest,
    lease.expires_at AS lease_expires_at,
    lease.revoked_at AS lease_revoked_at,
    attempt.id AS attempt_id,
    attempt.job_id,
    attempt.state AS attempt_state,
    attempt.worker_id,
    attempt.worker_epoch,
    attempt.fence AS attempt_fence,
    attempt.finalization_started_at,
    attempt.finalization_deadline_at,
    attempt.ended_at AS attempt_ended_at,
    job.organization_id,
    job.project_id,
    job.worker_pool_id,
    job.state AS job_state,
    job.version AS job_version,
    job.current_fence,
    job.job_expires_at,
    job.request_content_deleted_at,
    job.retention_artifact_days,
    retention_policy.stable_id AS retention_policy_revision,
    job.pricing_quantity AS generation_count,
    reservation.id AS credit_reservation_id,
    reservation.state AS credit_reservation_state,
    reservation.amount_minor,
    reservation.currency,
    completion.id AS completion_id,
    completion.authority_lease_id AS completion_authority_lease_id,
    completion.candidate_sha256,
    completion.artifact_set_id,
    completion.charge_id,
    completion.job_version AS completion_job_version,
    completion.completed_at,
    artifact_set.manifest_sha256,
    artifact_set.retention_expires_at
FROM attempt_leases AS lease
JOIN attempts AS attempt ON attempt.id = lease.attempt_id
JOIN jobs AS job ON job.id = attempt.job_id
JOIN retention_policy_revisions AS retention_policy
  ON retention_policy.id = job.retention_policy_revision_id
JOIN credit_reservations AS reservation ON reservation.job_id = job.id
LEFT JOIN visible_completions AS completion ON completion.job_id = job.id
LEFT JOIN artifact_sets AS artifact_set ON artifact_set.id = completion.artifact_set_id
WHERE lease.attempt_id = sqlc.arg(attempt_id)
  AND lease.worker_id = sqlc.arg(worker_id)
  AND lease.worker_epoch = sqlc.arg(worker_epoch)
  AND lease.fence = sqlc.arg(fence)
  AND lease.phase = 'FINALIZATION'
  AND lease.owner_kind = sqlc.arg(owner_kind)
  AND lease.owner_id = sqlc.arg(owner_id)
  AND attempt.worker_id = sqlc.arg(worker_id)
  AND attempt.worker_epoch = sqlc.arg(worker_epoch)
  AND attempt.fence = sqlc.arg(fence)
LIMIT 1
FOR UPDATE OF lease, attempt, job;

-- name: LockVisibleCompletionProject :one
SELECT running_count
FROM projects
WHERE organization_id = sqlc.arg(organization_id)
  AND id = sqlc.arg(project_id)
FOR UPDATE;

-- name: LockVisibleCompletionPool :one
SELECT id
FROM worker_pools
WHERE id = sqlc.arg(worker_pool_id)
FOR UPDATE;

-- name: LockVisibleCompletionCreditReservation :one
SELECT id, state, amount_minor, currency
FROM credit_reservations
WHERE id = sqlc.arg(credit_reservation_id)
  AND job_id = sqlc.arg(job_id)
FOR UPDATE;

-- name: LockVisibleCompletionOrganizationCredit :one
SELECT currency, reserved_minor, unsettled_posted_minor
FROM organization_credit_accounts
WHERE organization_id = sqlc.arg(organization_id)
FOR UPDATE;

-- name: ListCompletionArtifactsForUpdate :many
SELECT
    id,
    organization_id,
    project_id,
    job_id,
    attempt_id,
    attempt_fence,
    kind,
    ordinal,
    object_key,
    object_version_id,
    size_bytes,
    sha256,
    content_type,
    validation_receipt,
    verified_at,
    state
FROM artifacts
WHERE attempt_id = sqlc.arg(attempt_id)
  AND attempt_fence = sqlc.arg(attempt_fence)
ORDER BY CASE kind WHEN 'VIDEO' THEN 0 ELSE 1 END, ordinal
FOR UPDATE;

-- name: ListCommittedArtifactSetItems :many
SELECT
    artifact_id,
    kind,
    ordinal,
    object_key,
    object_version_id,
    size_bytes,
    sha256,
    content_type,
    validation_receipt,
    verified_at
FROM artifact_set_items
WHERE artifact_set_id = sqlc.arg(artifact_set_id)
ORDER BY CASE kind WHEN 'VIDEO' THEN 0 ELSE 1 END, ordinal;

-- name: InsertArtifactSet :exec
INSERT INTO artifact_sets (
    id,
    organization_id,
    project_id,
    job_id,
    attempt_id,
    attempt_fence,
    manifest_sha256,
    retention_policy_revision,
    retention_expires_at,
    committed_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(attempt_id),
    sqlc.arg(attempt_fence),
    sqlc.arg(manifest_sha256),
    sqlc.arg(retention_policy_revision),
    sqlc.arg(retention_expires_at),
    sqlc.arg(committed_at)
);

-- name: InsertArtifactSetItem :exec
INSERT INTO artifact_set_items (
    organization_id,
    project_id,
    job_id,
    attempt_id,
    attempt_fence,
    artifact_set_id,
    artifact_id,
    kind,
    ordinal,
    object_key,
    object_version_id,
    size_bytes,
    sha256,
    content_type,
    validation_receipt,
    verified_at
) VALUES (
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(attempt_id),
    sqlc.arg(attempt_fence),
    sqlc.arg(artifact_set_id),
    sqlc.arg(artifact_id),
    sqlc.arg(kind),
    sqlc.arg(ordinal),
    sqlc.arg(object_key),
    sqlc.arg(object_version_id),
    sqlc.arg(size_bytes),
    sqlc.arg(sha256),
    sqlc.arg(content_type),
    sqlc.arg(validation_receipt),
    sqlc.arg(verified_at)
);

-- name: MarkArtifactCommitted :execrows
UPDATE artifacts
SET state = 'COMMITTED',
    retention_expires_at = sqlc.arg(retention_expires_at),
    updated_at = sqlc.arg(committed_at)
WHERE id = sqlc.arg(artifact_id)
  AND attempt_id = sqlc.arg(attempt_id)
  AND attempt_fence = sqlc.arg(attempt_fence)
  AND state = 'VERIFIED';

-- name: InsertVisibleCompletionCharge :exec
INSERT INTO charges (
    id,
    organization_id,
    project_id,
    job_id,
    credit_reservation_id,
    cancellation_id,
    artifact_set_id,
    reason,
    amount_minor,
    currency,
    posted_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(credit_reservation_id),
    NULL,
    sqlc.arg(artifact_set_id),
    'VISIBLE_COMPLETION',
    sqlc.arg(amount_minor),
    sqlc.arg(currency),
    sqlc.arg(posted_at)
);

-- name: InsertArtifactAccessGrant :exec
INSERT INTO artifact_access_grants (
    id,
    organization_id,
    project_id,
    job_id,
    artifact_set_id,
    eligible_at,
    retention_expires_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(artifact_set_id),
    sqlc.arg(eligible_at),
    sqlc.arg(retention_expires_at)
);

-- name: InsertVisibleCompletion :exec
INSERT INTO visible_completions (
    id,
    organization_id,
    project_id,
    job_id,
    attempt_id,
    attempt_fence,
    authority_lease_id,
    artifact_set_id,
    charge_id,
    candidate_sha256,
    job_version,
    completed_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(attempt_id),
    sqlc.arg(attempt_fence),
    sqlc.arg(authority_lease_id),
    sqlc.arg(artifact_set_id),
    sqlc.arg(charge_id),
    sqlc.arg(candidate_sha256),
    sqlc.arg(job_version),
    sqlc.arg(completed_at)
);

-- name: MarkCompletionAttemptSucceeded :execrows
UPDATE attempts
SET state = 'SUCCEEDED',
    ended_at = sqlc.arg(completed_at),
    updated_at = sqlc.arg(completed_at)
WHERE id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND state = 'FINALIZING'
  AND ended_at IS NULL;

-- name: RevokeCompletionLease :execrows
UPDATE attempt_leases
SET revoked_at = sqlc.arg(completed_at),
    updated_at = sqlc.arg(completed_at)
WHERE id = sqlc.arg(lease_id)
  AND attempt_id = sqlc.arg(attempt_id)
  AND worker_id = sqlc.arg(worker_id)
  AND worker_epoch = sqlc.arg(worker_epoch)
  AND fence = sqlc.arg(fence)
  AND phase = 'FINALIZATION'
  AND owner_kind = sqlc.arg(owner_kind)
  AND revoked_at IS NULL;

-- name: ReleaseWorkerAfterVisibleCompletion :execrows
UPDATE workers
SET lifecycle_state = 'READY',
    updated_at = sqlc.arg(completed_at)
WHERE id = sqlc.arg(worker_id)
  AND epoch = sqlc.arg(worker_epoch)
  AND lifecycle_state = 'BUSY'
  AND reachability_condition = 'HEALTHY';

-- name: DecrementProjectRunningForVisibleCompletion :execrows
UPDATE projects
SET running_count = running_count - 1
WHERE organization_id = sqlc.arg(organization_id)
  AND id = sqlc.arg(project_id)
  AND running_count > 0;

-- name: ConsumeVisibleCompletionCreditReservation :execrows
UPDATE credit_reservations
SET state = 'CONSUMED',
    updated_at = sqlc.arg(completed_at)
WHERE id = sqlc.arg(credit_reservation_id)
  AND job_id = sqlc.arg(job_id)
  AND state = 'RESERVED';

-- name: PostVisibleCompletionOrganizationCredit :execrows
UPDATE organization_credit_accounts
SET reserved_minor = reserved_minor - sqlc.arg(amount_minor),
    unsettled_posted_minor = unsettled_posted_minor + sqlc.arg(amount_minor),
    version = version + 1,
    updated_at = sqlc.arg(completed_at)
WHERE organization_id = sqlc.arg(organization_id)
  AND currency = sqlc.arg(currency)
  AND reserved_minor >= sqlc.arg(amount_minor);

-- name: InsertVisibleCompletionOutboxEvent :exec
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
    sqlc.arg(event_type),
    1,
    sqlc.arg(payload),
    sqlc.arg(occurred_at)
);

-- name: MarkJobSucceeded :one
UPDATE jobs
SET state = 'SUCCEEDED',
    result_artifact_set_id = sqlc.arg(artifact_set_id),
    version = version + 1,
    updated_at = sqlc.arg(completed_at)
WHERE id = sqlc.arg(job_id)
  AND version = sqlc.arg(expected_version)
  AND current_fence = sqlc.arg(fence)
  AND state = 'FINALIZING'
RETURNING version;
