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
