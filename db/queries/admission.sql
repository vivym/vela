-- name: SetRequestContext :one
SELECT
    context.organization_id::uuid AS organization_id,
    context.project_id::uuid AS project_id,
    context.principal_id::uuid AS principal_id,
    context.transaction_time::timestamptz AS transaction_time
FROM vela_set_request_context(
    sqlc.arg(credential_id)::uuid,
    sqlc.arg(credential_proof)::bytea,
    sqlc.arg(required_scope)::text
) AS context;

-- name: LockIdempotencyKey :one
SELECT pg_advisory_xact_lock(
    hashtext(sqlc.arg(project_id)::text),
    hashtext(sqlc.arg(idempotency_key)::text)
) AS acquired;

-- name: ResolveActiveSKU :one
SELECT
	resolved.model_revision_id::uuid AS model_revision_id,
	resolved.generation_preset_revision_id::uuid AS generation_preset_revision_id,
	resolved.certified_p95_compute_seconds::integer AS certified_p95_compute_seconds,
	resolved.service_class_revision_id::uuid AS service_class_revision_id,
	resolved.queue_retry_allowance_seconds::integer AS queue_retry_allowance_seconds,
	resolved.max_attempts::integer AS max_attempts,
	resolved.max_total_compute_multiplier_milli::integer AS max_total_compute_multiplier_milli,
	resolved.max_finalization_seconds_per_attempt::integer AS max_finalization_seconds_per_attempt,
	resolved.retry_backoff_policy::jsonb AS retry_backoff_policy,
	resolved.retryable_failure_classes::text[] AS retryable_failure_classes,
	resolved.circuit_breaker_policy::jsonb AS circuit_breaker_policy,
	resolved.output_spec_id::uuid AS output_spec_id,
	resolved.rate_card_revision_id::uuid AS rate_card_revision_id,
	resolved.rate_line_id::uuid AS rate_line_id,
	resolved.unit_amount_minor::bigint AS unit_amount_minor,
	resolved.currency::text AS currency,
	resolved.circuit_fingerprint_window_seconds::integer AS circuit_fingerprint_window_seconds,
	resolved.circuit_min_distinct_healthy_workers::integer AS circuit_min_distinct_healthy_workers
FROM vela_resolve_active_sku(
	sqlc.arg(model),
	sqlc.arg(generation_preset),
	sqlc.arg(service_class),
	sqlc.arg(output_spec)
) AS resolved(
	model_revision_id,
	generation_preset_revision_id,
	certified_p95_compute_seconds,
	service_class_revision_id,
	queue_retry_allowance_seconds,
	max_attempts,
	max_total_compute_multiplier_milli,
	max_finalization_seconds_per_attempt,
	retry_backoff_policy,
	retryable_failure_classes,
	circuit_breaker_policy,
	output_spec_id,
	rate_card_revision_id,
	rate_line_id,
	unit_amount_minor,
	currency,
	circuit_fingerprint_window_seconds,
	circuit_min_distinct_healthy_workers
);

-- name: ResolveJobExecutionRoute :one
SELECT
    route.execution_authority_kind::execution_authority_kind
        AS execution_authority_kind,
    route.stage_cutover_revision_id::uuid AS stage_cutover_revision_id,
    COALESCE(
        route.execution_graph_revision_id,
        '00000000-0000-0000-0000-000000000000'::uuid
    )::uuid AS execution_graph_revision_id,
    COALESCE(
        route.execution_profile_revision_id,
        '00000000-0000-0000-0000-000000000000'::uuid
    )::uuid AS execution_profile_revision_id,
    route.reserved_storage_bytes::bigint AS reserved_storage_bytes
FROM vela_resolve_job_execution_route(
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(model_revision_id)
) AS route;

-- name: LockCompatiblePool :one
SELECT
	pool.id::uuid AS id,
	pool.admission_open::boolean AS admission_open,
	pool.queued_count::integer AS queued_count,
	pool.queued_limit::integer AS queued_limit,
	pool.retry_after_seconds::integer AS retry_after_seconds
FROM vela_lock_compatible_pool(
	sqlc.arg(model_revision_id),
	sqlc.arg(generation_preset_revision_id),
	sqlc.arg(output_spec_id)
) AS pool(id, admission_open, queued_count, queued_limit, retry_after_seconds);

-- name: GetIdempotencyResult :one
SELECT request_hash, job_id
FROM idempotency_results
WHERE organization_id = sqlc.arg(organization_id)
  AND project_id = sqlc.arg(project_id)
  AND idempotency_key = sqlc.arg(idempotency_key);

-- name: LockProjectForAdmission :one
SELECT
    p.queued_count - p.retry_wait_count AS queued_count,
    p.queued_limit,
    p.retry_after_seconds,
    p.running_count,
    p.running_limit,
    o.status AS organization_status,
    p.retention_policy_revision_id,
    p.retention_artifact_days AS artifact_retention_days,
    p.retention_request_content_days AS request_content_retention_days,
    p.retention_incomplete_content_hours AS incomplete_content_retention_hours,
    p.retention_scratch_hours AS scratch_retention_hours,
    p.retention_debug_hours AS debug_retention_hours,
    p.retention_metadata_days AS metadata_retention_days,
    p.retention_financial_days AS financial_retention_days
FROM projects AS p
JOIN customer_organizations AS o ON o.id = p.organization_id
WHERE p.organization_id = sqlc.arg(organization_id)
  AND p.id = sqlc.arg(project_id)
FOR UPDATE OF p;

-- name: LockCreditAccount :one
SELECT
    currency,
    contract_credit_limit_minor,
    reserved_minor,
    unsettled_posted_minor
FROM organization_credit_accounts
WHERE organization_id = sqlc.arg(organization_id)
FOR UPDATE;

-- name: IncrementProjectQueued :execrows
UPDATE projects
SET queued_count = queued_count + 1
WHERE organization_id = sqlc.arg(organization_id)
  AND id = sqlc.arg(project_id)
  AND queued_count - retry_wait_count < queued_limit;

-- name: IncrementPoolQueued :execrows
UPDATE worker_pools
SET queued_count = queued_count + 1
WHERE id = sqlc.arg(worker_pool_id)
  AND admission_open
  AND queued_count - retry_wait_count < queued_limit;

-- name: ReserveOrganizationCredit :execrows
UPDATE organization_credit_accounts
SET reserved_minor = reserved_minor + sqlc.arg(amount_minor),
    version = version + 1,
    updated_at = clock_timestamp()
WHERE organization_id = sqlc.arg(organization_id)
  AND currency = sqlc.arg(currency)
  AND contract_credit_limit_minor - unsettled_posted_minor - reserved_minor >= sqlc.arg(amount_minor);

-- name: InsertJob :exec
INSERT INTO jobs (
    id,
    organization_id,
    project_id,
    created_by_principal_id,
    model_revision_id,
    generation_preset_revision_id,
    service_class_revision_id,
    output_spec_id,
    execution_authority_kind,
    stage_cutover_revision_id,
    execution_graph_revision_id,
    stage_execution_profile_revision_id,
    worker_pool_id,
    request_hash,
    request_content,
    request_content_expires_at,
    retention_policy_revision_id,
    retention_artifact_days,
    retention_request_content_days,
    retention_incomplete_content_hours,
    retention_scratch_hours,
    retention_debug_hours,
    retention_metadata_days,
    retention_financial_days,
    pricing_rate_card_revision_id,
    pricing_rate_line_id,
    pricing_unit_amount_minor,
    pricing_quantity,
    pricing_quoted_amount_minor,
    pricing_currency,
    execution_max_attempts,
    execution_max_total_compute_seconds,
    execution_max_finalization_seconds_per_attempt,
    execution_retry_backoff_policy,
    execution_retryable_failure_classes,
    execution_circuit_breaker_policy,
	execution_circuit_fingerprint_window_seconds,
	execution_circuit_min_distinct_healthy_workers,
    job_expires_at
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(created_by_principal_id),
    sqlc.arg(model_revision_id),
    sqlc.arg(generation_preset_revision_id),
    sqlc.arg(service_class_revision_id),
    sqlc.arg(output_spec_id),
    sqlc.arg(execution_authority_kind),
    sqlc.narg(stage_cutover_revision_id),
    sqlc.narg(execution_graph_revision_id),
    sqlc.narg(stage_execution_profile_revision_id),
    sqlc.narg(worker_pool_id),
    sqlc.arg(request_hash),
    sqlc.arg(request_content),
    transaction_timestamp()
        + sqlc.arg(retention_request_content_days)::bigint * interval '1 day',
    sqlc.arg(retention_policy_revision_id),
    sqlc.arg(retention_artifact_days),
    sqlc.arg(retention_request_content_days),
    sqlc.arg(retention_incomplete_content_hours),
    sqlc.arg(retention_scratch_hours),
    sqlc.arg(retention_debug_hours),
    sqlc.arg(retention_metadata_days),
    sqlc.arg(retention_financial_days),
    sqlc.arg(pricing_rate_card_revision_id),
    sqlc.arg(pricing_rate_line_id),
    sqlc.arg(pricing_unit_amount_minor),
    sqlc.arg(pricing_quantity),
    sqlc.arg(pricing_quoted_amount_minor),
    sqlc.arg(pricing_currency),
    sqlc.arg(execution_max_attempts),
    sqlc.arg(execution_max_total_compute_seconds),
    sqlc.arg(execution_max_finalization_seconds_per_attempt),
    sqlc.arg(execution_retry_backoff_policy),
    sqlc.arg(execution_retryable_failure_classes),
    sqlc.arg(execution_circuit_breaker_policy),
	sqlc.arg(execution_circuit_fingerprint_window_seconds),
	sqlc.arg(execution_circuit_min_distinct_healthy_workers),
	transaction_timestamp() + sqlc.arg(job_lifetime_seconds)::bigint * interval '1 second'
);

-- name: InsertRetryRuntimeState :exec
INSERT INTO retry_runtime_states (job_id, organization_id, project_id)
VALUES (sqlc.arg(job_id), sqlc.arg(organization_id), sqlc.arg(project_id));

-- name: InsertCreditReservation :exec
INSERT INTO credit_reservations (
    id, organization_id, project_id, job_id, amount_minor, currency
) VALUES (
    sqlc.arg(id),
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(job_id),
    sqlc.arg(amount_minor),
    sqlc.arg(currency)
);

-- name: InsertIdempotencyResult :exec
INSERT INTO idempotency_results (
    organization_id, project_id, idempotency_key, request_hash, job_id
) VALUES (
    sqlc.arg(organization_id),
    sqlc.arg(project_id),
    sqlc.arg(idempotency_key),
    sqlc.arg(request_hash),
    sqlc.arg(job_id)
);

-- name: InsertOutboxEvent :exec
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
    1,
    'job.ready',
    1,
    sqlc.arg(payload),
    sqlc.arg(occurred_at)
);

-- name: GetJob :one
SELECT
    j.id,
    j.project_id,
    j.state,
    j.execution_phase,
    j.pricing_rate_card_revision_id,
    j.pricing_rate_line_id,
    j.pricing_unit_amount_minor,
    j.pricing_quantity,
    j.pricing_quoted_amount_minor,
    j.pricing_currency,
    rts.attempts_started,
    rts.next_retry_at,
    ap.phase_progress,
    ap.estimated_finish_at,
    ap.progress_updated_at,
    j.job_expires_at,
    j.created_at
FROM jobs AS j
JOIN vela_request_job_runtime AS rts ON rts.job_id = j.id
LEFT JOIN vela_request_job_progress AS ap ON ap.job_id = j.id
WHERE j.organization_id = sqlc.arg(organization_id)
  AND j.project_id = sqlc.arg(project_id)
  AND j.id = sqlc.arg(job_id);
