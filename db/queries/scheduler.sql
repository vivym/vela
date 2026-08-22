-- name: ListSchedulableWorkerPools :many
SELECT pool.worker_pool_id::uuid AS worker_pool_id
FROM vela_list_schedulable_worker_pools() AS pool(worker_pool_id);

-- name: ClaimSchedulerDispatch :one
SELECT
    claim.intent_id::uuid AS intent_id,
    claim.organization_id::uuid AS organization_id,
    claim.service_class_revision_id::uuid AS service_class_revision_id,
    claim.project_id::uuid AS project_id,
    claim.job_id::uuid AS job_id,
    claim.expected_job_version::bigint AS expected_job_version,
    claim.worker_id::uuid AS worker_id,
    claim.worker_epoch::bigint AS worker_epoch,
    claim.execution_profile_revision_id::uuid AS execution_profile_revision_id,
    claim.lane::scheduler_lane AS lane,
    claim.predicted_runtime_seconds::bigint AS predicted_runtime_seconds,
    claim.predicted_start_at::timestamptz AS predicted_start_at,
    claim.predicted_finish_at::timestamptz AS predicted_finish_at,
    claim.job_order_score::bigint AS job_order_score,
    claim.worker_score::bigint AS worker_score,
    claim.claim_expires_at::timestamptz AS claim_expires_at
FROM vela_claim_scheduler_dispatch(
    sqlc.arg(worker_pool_id),
    sqlc.arg(scheduler_id),
    sqlc.arg(claim_ttl_seconds)
) AS claim(
    intent_id,
    organization_id,
    service_class_revision_id,
    project_id,
    job_id,
    expected_job_version,
    worker_id,
    worker_epoch,
    execution_profile_revision_id,
    lane,
    predicted_runtime_seconds,
    predicted_start_at,
    predicted_finish_at,
    job_order_score,
    worker_score,
    claim_expires_at
);

-- name: AbandonSchedulerDispatch :one
SELECT vela_abandon_scheduler_dispatch(
    sqlc.arg(intent_id),
    sqlc.arg(scheduler_id),
    sqlc.arg(reason)
) AS abandoned;

-- name: ReconcileExpiredSchedulerDispatches :one
SELECT vela_reconcile_expired_scheduler_dispatches() AS reconciled_count;

-- name: PredictAdmissionCapacity :one
SELECT
    prediction.predicted_queue_wait_seconds::bigint AS predicted_queue_wait_seconds,
    prediction.predicted_finish_at::timestamptz AS predicted_finish_at
FROM vela_predict_admission_capacity(
    sqlc.arg(worker_pool_id),
    sqlc.arg(model_revision_id),
    sqlc.arg(generation_preset_revision_id),
    sqlc.arg(service_class_revision_id),
    sqlc.arg(output_spec_id),
    sqlc.arg(generation_count)
) AS prediction(predicted_queue_wait_seconds, predicted_finish_at);

-- name: PredictJobDynamicETA :one
SELECT prediction.predicted_finish_at::timestamptz
FROM vela_predict_job_dynamic_eta(sqlc.arg(job_id)) AS prediction(predicted_finish_at);
