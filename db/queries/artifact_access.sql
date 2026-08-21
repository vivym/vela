-- name: SetArtifactRequestContext :one
SELECT
    organization_id::uuid,
    project_id::uuid,
    principal_id::uuid,
    transaction_time::timestamptz
FROM vela_set_artifact_request_context(
    sqlc.arg(credential_id)::uuid,
    sqlc.arg(credential_proof)::bytea
);

-- name: ListReadableArtifactSet :many
SELECT
    artifact_set.id AS artifact_set_id,
    artifact_set.job_id,
    artifact_set.retention_expires_at,
    artifact_set.committed_at,
    item.artifact_id,
    item.kind,
    item.ordinal,
    item.object_key,
    item.object_version_id,
    item.size_bytes,
    item.sha256,
    item.content_type
FROM jobs AS job
JOIN artifact_sets AS artifact_set
  ON artifact_set.id = job.result_artifact_set_id
 AND artifact_set.job_id = job.id
JOIN artifact_access_grants AS access_grant
  ON access_grant.artifact_set_id = artifact_set.id
 AND access_grant.job_id = job.id
JOIN artifact_set_items AS item
  ON item.artifact_set_id = artifact_set.id
WHERE job.organization_id = sqlc.arg(organization_id)
  AND job.project_id = sqlc.arg(project_id)
  AND job.id = sqlc.arg(job_id)
  AND job.state = 'SUCCEEDED'
  AND access_grant.eligible_at <= sqlc.arg(read_at)
  AND access_grant.retention_expires_at > sqlc.arg(read_at)
  AND access_grant.revoked_at IS NULL
ORDER BY CASE item.kind WHEN 'VIDEO' THEN 0 ELSE 1 END, item.ordinal;
