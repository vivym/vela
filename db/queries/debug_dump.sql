-- name: LockDebugDumpUploadAuthority :one
SELECT
    lease.token_digest,
    lease.expires_at AS lease_expires_at,
    lease.revoked_at AS lease_revoked_at,
    lease.phase AS lease_phase,
    lease.owner_kind AS lease_owner_kind,
    lease.owner_id AS lease_owner_id,
    attempt.id AS attempt_id,
    attempt.job_id,
    attempt.state AS attempt_state,
    attempt.worker_id,
    attempt.worker_epoch,
    attempt.fence AS attempt_fence,
    attempt.debug_dump_authorization_id,
    attempt.debug_dump_authorization_expires_at,
    job.organization_id,
    job.project_id,
    job.state AS job_state,
    job.current_fence,
    job.job_expires_at
FROM attempt_leases AS lease
JOIN attempts AS attempt ON attempt.id = lease.attempt_id
JOIN jobs AS job ON job.id = attempt.job_id
WHERE lease.attempt_id = sqlc.arg(attempt_id)
ORDER BY (lease.revoked_at IS NULL) DESC, lease.issued_at DESC, lease.id DESC
LIMIT 1
FOR UPDATE OF lease, attempt, job;

-- name: ConfirmDebugDumpUploadAuthorization :one
SELECT vela_confirm_debug_dump_authorization_for_assignment(
    sqlc.arg(authorization_id), sqlc.arg(confirmed_at)
)::boolean AS active;

-- name: InsertDebugDumpUpload :execrows
INSERT INTO debug_dumps (
    id, organization_id, project_id, job_id, authorization_id,
    attempt_id, attempt_fence, worker_id, worker_epoch, object_key,
    expected_size_bytes, expected_sha256, expected_content_type,
    expires_at, created_at, updated_at
) VALUES (
    sqlc.arg(debug_dump_id), sqlc.arg(organization_id), sqlc.arg(project_id),
    sqlc.arg(job_id), sqlc.arg(authorization_id), sqlc.arg(attempt_id),
    sqlc.arg(attempt_fence), sqlc.arg(worker_id), sqlc.arg(worker_epoch),
    sqlc.arg(object_key), sqlc.arg(expected_size_bytes), sqlc.arg(expected_sha256),
    sqlc.arg(expected_content_type), sqlc.arg(expires_at),
    sqlc.arg(created_at), sqlc.arg(created_at)
) ON CONFLICT (id) DO NOTHING;

-- name: ClaimDebugDumpUpload :one
UPDATE debug_dumps AS dump
SET claim_id = CASE
        WHEN dump.multipart_upload_id IS NOT NULL THEN dump.claim_id
        ELSE sqlc.arg(claim_id)
    END,
    claim_expires_at = CASE
        WHEN dump.multipart_upload_id IS NOT NULL THEN dump.claim_expires_at
        WHEN dump.claim_id = sqlc.arg(claim_id) THEN dump.claim_expires_at
        ELSE sqlc.arg(claim_expires_at)
    END,
    version = CASE
        WHEN dump.multipart_upload_id IS NOT NULL OR dump.claim_id = sqlc.arg(claim_id)
            THEN dump.version
        ELSE dump.version + 1
    END,
    updated_at = CASE
        WHEN dump.multipart_upload_id IS NOT NULL OR dump.claim_id = sqlc.arg(claim_id)
            THEN dump.updated_at
        ELSE sqlc.arg(claimed_at)
    END
WHERE dump.id = sqlc.arg(debug_dump_id)
  AND dump.organization_id = sqlc.arg(organization_id)
  AND dump.project_id = sqlc.arg(project_id)
  AND dump.job_id = sqlc.arg(job_id)
  AND dump.authorization_id = sqlc.arg(authorization_id)
  AND dump.attempt_id = sqlc.arg(attempt_id)
  AND dump.attempt_fence = sqlc.arg(attempt_fence)
  AND dump.worker_id = sqlc.arg(worker_id)
  AND dump.worker_epoch = sqlc.arg(worker_epoch)
  AND dump.object_key = sqlc.arg(object_key)
  AND dump.expected_size_bytes = sqlc.arg(expected_size_bytes)
  AND dump.expected_sha256 = sqlc.arg(expected_sha256)
  AND dump.expected_content_type = sqlc.arg(expected_content_type)
  AND dump.state = 'UPLOADING'
  AND dump.expires_at > sqlc.arg(claimed_at)
  AND (
      dump.multipart_upload_id IS NOT NULL
      OR dump.claim_id = sqlc.arg(claim_id)
      OR dump.claim_expires_at IS NULL
      OR dump.claim_expires_at <= sqlc.arg(claimed_at)
  )
RETURNING
    dump.id AS debug_dump_id,
    dump.authorization_id,
    dump.object_key,
    dump.expected_size_bytes,
    dump.expected_sha256,
    dump.expected_content_type,
    dump.claim_id,
    dump.claim_expires_at,
    dump.multipart_upload_id,
    dump.completed_parts,
    dump.expires_at,
    dump.version;

-- name: RecordDebugDumpMultipartSession :one
UPDATE debug_dumps AS dump
SET multipart_upload_id = CASE
        WHEN dump.multipart_upload_id = sqlc.arg(multipart_upload_id)
            THEN dump.multipart_upload_id
        ELSE sqlc.arg(multipart_upload_id)
    END,
    claim_id = NULL,
    claim_expires_at = NULL,
    version = CASE
        WHEN dump.multipart_upload_id = sqlc.arg(multipart_upload_id)
            THEN dump.version
        ELSE dump.version + 1
    END,
    updated_at = CASE
        WHEN dump.multipart_upload_id = sqlc.arg(multipart_upload_id)
            THEN dump.updated_at
        ELSE sqlc.arg(recorded_at)
    END
WHERE dump.id = sqlc.arg(debug_dump_id)
  AND dump.attempt_id = sqlc.arg(attempt_id)
  AND dump.attempt_fence = sqlc.arg(attempt_fence)
  AND dump.state = 'UPLOADING'
  AND (
      (dump.multipart_upload_id IS NULL
       AND dump.claim_id = sqlc.arg(claim_id)
       AND dump.claim_expires_at > sqlc.arg(recorded_at))
      OR dump.multipart_upload_id = sqlc.arg(multipart_upload_id)
  )
RETURNING dump.id AS debug_dump_id, dump.authorization_id,
    dump.multipart_upload_id, dump.version;

-- name: GetDebugDumpUploadStatus :one
SELECT
    id AS debug_dump_id, authorization_id, state, object_key,
    expected_size_bytes, expected_sha256, expected_content_type,
    multipart_upload_id, completed_parts, completion_request_hash,
    object_version_id, size_bytes, sha256, content_type, expires_at, version
FROM debug_dumps
WHERE id = sqlc.arg(debug_dump_id)
  AND attempt_id = sqlc.arg(attempt_id)
  AND attempt_fence = sqlc.arg(attempt_fence)
FOR UPDATE;

-- name: RecordDebugDumpCompletionIntent :one
UPDATE debug_dumps AS dump
SET completed_parts = sqlc.arg(completed_parts),
    completion_request_hash = sqlc.arg(completion_request_hash),
    version = CASE
        WHEN dump.completion_request_hash IS NULL THEN dump.version + 1
        ELSE dump.version
    END,
    updated_at = CASE
        WHEN dump.completion_request_hash IS NULL THEN sqlc.arg(recorded_at)
        ELSE dump.updated_at
    END
WHERE dump.id = sqlc.arg(debug_dump_id)
  AND dump.attempt_id = sqlc.arg(attempt_id)
  AND dump.attempt_fence = sqlc.arg(attempt_fence)
  AND dump.state = 'UPLOADING'
  AND dump.multipart_upload_id IS NOT NULL
  AND dump.expected_size_bytes = sqlc.arg(size_bytes)
  AND dump.expected_sha256 = sqlc.arg(sha256)
  AND dump.expected_content_type = sqlc.arg(content_type)
  AND (
      dump.completion_request_hash IS NULL
      OR (dump.completion_request_hash = sqlc.arg(completion_request_hash)
          AND dump.completed_parts = sqlc.arg(completed_parts))
  )
RETURNING dump.id AS debug_dump_id, dump.authorization_id, dump.version;

-- name: RecordDebugDumpUploaded :one
SELECT result.id AS debug_dump_id, result.authorization_id,
    result.object_version_id, result.version
FROM vela_record_debug_dump_uploaded(
    sqlc.arg(debug_dump_id), sqlc.arg(attempt_id), sqlc.arg(attempt_fence),
    sqlc.arg(object_version_id), sqlc.arg(size_bytes), sqlc.arg(sha256),
    sqlc.arg(content_type), sqlc.arg(completed_parts),
    sqlc.arg(completion_request_hash), sqlc.arg(uploaded_at)
) AS result;

-- name: InsertDebugDumpClaimedEvent :exec
INSERT INTO debug_dump_events (
    id, organization_id, project_id, job_id, authorization_id, debug_dump_id,
    action, outcome_code, actor_kind, worker_id, worker_epoch, created_at
)
SELECT gen_random_uuid(), dump.organization_id, dump.project_id, dump.job_id,
    dump.authorization_id, dump.id,
    'UPLOAD_CLAIMED', 'UPLOAD_CLAIMED', 'WORKER', dump.worker_id, dump.worker_epoch,
    sqlc.arg(created_at)
FROM debug_dumps AS dump
WHERE dump.id = sqlc.arg(debug_dump_id)
ON CONFLICT DO NOTHING;
