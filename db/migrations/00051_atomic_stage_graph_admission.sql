-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_guard_stage_ready_queue_capacity() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_max_ready_queue_depth integer;
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.stage_ready_queue_entries AS ready
        WHERE ready.capacity_pool_id = NEW.capacity_pool_id
          AND ready.stage_run_id = NEW.stage_run_id
    ) THEN
        RETURN NEW;
    END IF;

    SELECT pool.max_ready_queue_depth
    INTO v_max_ready_queue_depth
    FROM public.capacity_pools AS pool
    WHERE pool.id = NEW.capacity_pool_id
      AND pool.state = 'ACTIVE'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_ready_queue_capacity_pool_inactive',
            MESSAGE = 'Stage READY queue requires an active CapacityPool';
    END IF;
    IF (
        SELECT count(*)
        FROM public.stage_ready_queue_entries AS ready
        WHERE ready.capacity_pool_id = NEW.capacity_pool_id
    ) >= v_max_ready_queue_depth THEN
        RAISE EXCEPTION USING
            ERRCODE = '54000',
            CONSTRAINT = 'stage_ready_queue_depth_exceeded',
            MESSAGE = 'Stage READY queue exceeds the CapacityPool bound';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER stage_ready_queue_capacity_guard
BEFORE INSERT ON stage_ready_queue_entries
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_ready_queue_capacity();

CREATE FUNCTION vela_lock_stage_graph_ready_capacity_path(
    p_execution_graph_revision_id uuid,
    p_execution_profile_revision_id uuid
) RETURNS TABLE (
    ready boolean,
    retry_after_seconds integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := statement_timestamp();
    v_ready boolean;
BEGIN
    IF p_execution_graph_revision_id IS NULL
       OR p_execution_profile_revision_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_graph_capacity_path_identity_invalid',
            MESSAGE = 'Stage graph capacity path identity is invalid';
    END IF;
    IF public.vela_current_request_scope() IS DISTINCT FROM 'jobs:submit'
       OR public.vela_current_organization_id() IS NULL
       OR public.vela_current_project_id() IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'stage_graph_capacity_path_context_mismatch',
            MESSAGE = 'Stage graph capacity path requires an authenticated submit context';
    END IF;

    PERFORM pool.id
    FROM public.capacity_pools AS pool
    WHERE pool.state = 'ACTIVE'
      AND EXISTS (
          SELECT 1
          FROM public.execution_profile_stage_options AS option
          WHERE option.execution_graph_revision_id = p_execution_graph_revision_id
            AND option.execution_profile_revision_id = p_execution_profile_revision_id
            AND option.stage_profile_revision_id = pool.stage_profile_revision_id
      )
    ORDER BY pool.id
    FOR UPDATE OF pool;

    v_ready := NOT EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS graph_stage
        WHERE graph_stage.execution_graph_revision_id = p_execution_graph_revision_id
          AND graph_stage.required
          AND NOT EXISTS (
              SELECT 1
              FROM public.execution_profile_stage_options AS option
              JOIN public.stage_profile_revisions AS stage_profile
                ON stage_profile.id = option.stage_profile_revision_id
              JOIN public.capacity_pools AS pool
                ON pool.stage_profile_revision_id = option.stage_profile_revision_id
               AND pool.state = 'ACTIVE'
              JOIN public.worker_instances AS worker
                ON worker.capacity_pool_id = pool.id
               AND worker.lifecycle_state = 'READY'
               AND worker.reachability_state = 'CONNECTED'
              JOIN public.model_residencies AS residency
                ON residency.worker_instance_id = worker.id
               AND residency.worker_instance_epoch = worker.instance_epoch
               AND residency.model_component_revision =
                   stage_profile.model_component_revision
               AND residency.state = 'READY'
              WHERE option.execution_graph_revision_id =
                    graph_stage.execution_graph_revision_id
                AND option.execution_profile_revision_id =
                    p_execution_profile_revision_id
                AND option.stage_key = graph_stage.stage_key
                AND (
                    SELECT count(*)
                    FROM public.stage_ready_queue_entries AS ready
                    WHERE ready.capacity_pool_id = pool.id
                ) < pool.max_ready_queue_depth
                AND EXISTS (
                    SELECT 1
                    FROM public.capacity_observations AS observation
                    WHERE observation.worker_instance_id = worker.id
                      AND observation.worker_instance_epoch = worker.instance_epoch
                      AND observation.expires_at > v_now
                      AND observation.capacity_vector ->> 'concurrency'
                          ~ '^[1-9][0-9]*$'
                      AND NOT EXISTS (
                          SELECT 1
                          FROM public.capacity_observations AS newer_observation
                          WHERE newer_observation.worker_instance_id =
                                observation.worker_instance_id
                            AND newer_observation.worker_instance_epoch =
                                observation.worker_instance_epoch
                            AND newer_observation.observation_sequence >
                                observation.observation_sequence
                      )
                )
                AND public.vela_worker_instance_authority_matches(
                    worker.id,
                    worker.instance_epoch,
                    worker.device_set_digest,
                    worker.membership_digest,
                    residency.id,
                    residency.model_runtime_epoch
                )
          )
    ) AND NOT EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS graph_stage
        JOIN public.execution_profile_stage_options AS option
          ON option.execution_graph_revision_id =
             graph_stage.execution_graph_revision_id
         AND option.execution_profile_revision_id =
             p_execution_profile_revision_id
         AND option.stage_key = graph_stage.stage_key
        JOIN public.capacity_pools AS pool
          ON pool.stage_profile_revision_id = option.stage_profile_revision_id
         AND pool.state = 'ACTIVE'
        WHERE graph_stage.execution_graph_revision_id =
              p_execution_graph_revision_id
          AND NOT EXISTS (
              SELECT 1
              FROM public.execution_graph_edges AS edge
              WHERE edge.execution_graph_revision_id =
                    graph_stage.execution_graph_revision_id
                AND edge.destination_stage_key = graph_stage.stage_key
          )
          AND (
              SELECT count(*)
              FROM public.stage_ready_queue_entries AS ready
              WHERE ready.capacity_pool_id = pool.id
          ) >= pool.max_ready_queue_depth
    );

    RETURN QUERY SELECT v_ready, 30;
END
$$;

CREATE FUNCTION vela_instantiate_admitted_stage_graph(
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid
) RETURNS TABLE (
    aggregate_version bigint,
    current_fence bigint,
    snapshot_id uuid,
    attempt_id uuid,
    attempt_fence bigint,
    stage_run_count integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_work public.stage_graph_instantiation_work%ROWTYPE;
    v_claim_token uuid;
    v_snapshot_id uuid;
    v_attempt_id uuid;
    v_attempt_fence bigint;
    v_stage_run_count integer;
    v_replayed boolean;
    v_completed boolean;
    v_aggregate_version bigint;
    v_current_fence bigint;
    v_capacity_ready boolean;
    v_capacity_retry_after_seconds integer;
BEGIN
    IF p_organization_id IS NULL OR p_project_id IS NULL OR p_job_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_graph_admission_identity_invalid',
            MESSAGE = 'Stage graph Admission identity is invalid';
    END IF;
    IF public.vela_current_request_scope() IS DISTINCT FROM 'jobs:submit'
       OR public.vela_current_organization_id() IS DISTINCT FROM p_organization_id
       OR public.vela_current_project_id() IS DISTINCT FROM p_project_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'stage_graph_admission_context_mismatch',
            MESSAGE = 'Stage graph Admission does not match the authenticated request context';
    END IF;

    SELECT work.* INTO v_work
    FROM public.stage_graph_instantiation_work AS work
    JOIN public.jobs AS job
      ON job.id = work.job_id
     AND job.organization_id = work.organization_id
     AND job.project_id = work.project_id
    WHERE work.job_id = p_job_id
      AND work.organization_id = p_organization_id
      AND work.project_id = p_project_id
    FOR UPDATE OF work;
    IF NOT FOUND OR v_work.source <> 'ADMISSION_TRIGGER'
       OR v_work.state <> 'PENDING'
       OR v_work.claim_owner IS NOT NULL OR v_work.claim_token IS NOT NULL
       OR v_work.claimed_at IS NOT NULL OR v_work.claim_expires_at IS NOT NULL
       OR NOT EXISTS (
            SELECT 1
            FROM public.jobs AS job
            WHERE job.id = v_work.job_id
              AND job.organization_id = v_work.organization_id
              AND job.project_id = v_work.project_id
              AND job.execution_authority_kind = 'STAGE_GRAPH'
              AND job.state = 'QUEUED'
              AND job.version = v_work.expected_job_version
              AND job.current_fence = v_work.expected_job_fence
              AND job.stage_cutover_revision_id = v_work.stage_cutover_revision_id
              AND job.execution_graph_revision_id = v_work.execution_graph_revision_id
              AND job.stage_execution_profile_revision_id =
                  v_work.execution_profile_revision_id
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_admission_work_not_exact',
            MESSAGE = 'Stage graph Admission work is absent, claimed, or does not match the Job';
    END IF;

    SELECT capacity.ready, capacity.retry_after_seconds
    INTO v_capacity_ready, v_capacity_retry_after_seconds
    FROM public.vela_lock_stage_graph_ready_capacity_path(
        v_work.execution_graph_revision_id,
        v_work.execution_profile_revision_id
    ) AS capacity;
    IF NOT v_capacity_ready THEN
        RAISE EXCEPTION USING
            ERRCODE = '54000',
            CONSTRAINT = 'stage_graph_ready_capacity_unavailable',
            MESSAGE = 'Stage graph has no complete READY capacity path',
            HINT = 'Retry after ' || v_capacity_retry_after_seconds::text || ' seconds';
    END IF;

    v_claim_token := gen_random_uuid();
    UPDATE public.stage_graph_instantiation_work AS work
    SET state = 'CLAIMED',
        claim_owner = 'admission:' || pg_catalog.pg_backend_pid()::text,
        claim_token = v_claim_token,
        claimed_at = clock_timestamp(),
        claim_expires_at = clock_timestamp() + interval '5 minutes',
        claim_count = work.claim_count + 1,
        last_error = NULL,
        updated_at = clock_timestamp()
    WHERE work.job_id = v_work.job_id
      AND work.state = 'PENDING';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_graph_admission_claim_lost',
            MESSAGE = 'Stage graph Admission work changed before it was claimed';
    END IF;

    SELECT
        instantiated.snapshot_id,
        instantiated.attempt_id,
        instantiated.attempt_fence,
        instantiated.stage_run_count,
        instantiated.replayed
    INTO
        v_snapshot_id,
        v_attempt_id,
        v_attempt_fence,
        v_stage_run_count,
        v_replayed
    FROM public.vela_instantiate_stage_graph(jsonb_build_object(
        'schema_version', 1,
        'command_id', v_work.command_id,
        'job_id', v_work.job_id,
        'expected_job_version', v_work.expected_job_version,
        'expected_job_fence', v_work.expected_job_fence,
        'execution_graph_snapshot_id', v_work.execution_graph_snapshot_id,
        'execution_graph_revision_id', v_work.execution_graph_revision_id,
        'execution_profile_revision_id', v_work.execution_profile_revision_id,
        'attempt_id', v_work.attempt_id,
        'storage_reservation_id', v_work.storage_reservation_id,
        'reserved_storage_bytes', v_work.reserved_storage_bytes
    )) AS instantiated;
    IF v_replayed OR v_snapshot_id IS DISTINCT FROM v_work.execution_graph_snapshot_id
       OR v_attempt_id IS DISTINCT FROM v_work.attempt_id
       OR v_attempt_fence <> v_work.expected_job_fence + 1
       OR v_stage_run_count <= 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_admission_instantiation_mismatch',
            MESSAGE = 'Stage graph Admission instantiation did not create its exact authority';
    END IF;

    SELECT public.vela_complete_stage_graph_instantiation(
        v_work.job_id,
        v_claim_token,
        v_work.command_id,
        v_work.execution_graph_snapshot_id,
        v_work.attempt_id
    ) INTO v_completed;
    IF NOT v_completed THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_admission_completion_lost',
            MESSAGE = 'Stage graph Admission instantiation could not be completed';
    END IF;
    UPDATE public.stage_graph_instantiation_work AS work
    SET completion_reason = 'ADMISSION_TRANSACTION_INSTANTIATED',
        updated_at = clock_timestamp()
    WHERE work.job_id = v_work.job_id
      AND work.state = 'COMPLETED'
      AND work.completion_reason = 'INSTANTIATION_CONFIRMED';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_admission_completion_mismatch',
            MESSAGE = 'Stage graph Admission completion evidence is not exact';
    END IF;

    SELECT job.version, job.current_fence
    INTO STRICT v_aggregate_version, v_current_fence
    FROM public.jobs AS job
    WHERE job.id = v_work.job_id
      AND job.organization_id = v_work.organization_id
      AND job.project_id = v_work.project_id;
    IF v_aggregate_version <> v_work.expected_job_version + 1
       OR v_current_fence <> v_work.expected_job_fence + 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_admission_job_version_mismatch',
            MESSAGE = 'Stage graph Admission Job version or fence is not exact';
    END IF;
    RETURN QUERY SELECT
        v_aggregate_version,
        v_current_fence,
        v_snapshot_id,
        v_attempt_id,
        v_attempt_fence,
        v_stage_run_count;
END
$$;

CREATE FUNCTION vela_require_atomic_stage_graph_admission() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.stage_graph_instantiation_work AS work
        WHERE work.job_id = NEW.id
          AND work.organization_id = NEW.organization_id
          AND work.project_id = NEW.project_id
          AND work.source = 'ADMISSION_TRIGGER'
          AND work.state = 'COMPLETED'
          AND work.completion_reason = 'ADMISSION_TRANSACTION_INSTANTIATED'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_admission_requires_atomic_instantiation',
            MESSAGE = 'Stage graph Admission cannot commit before exact graph instantiation';
    END IF;
    RETURN NULL;
END
$$;

CREATE CONSTRAINT TRIGGER jobs_require_atomic_stage_graph_admission
AFTER INSERT ON jobs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW
WHEN (NEW.execution_authority_kind = 'STAGE_GRAPH')
EXECUTE FUNCTION vela_require_atomic_stage_graph_admission();

ALTER FUNCTION vela_instantiate_admitted_stage_graph(uuid, uuid, uuid)
    OWNER TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_require_atomic_stage_graph_admission()
    OWNER TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_lock_stage_graph_ready_capacity_path(uuid, uuid)
    OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_guard_stage_ready_queue_capacity()
    OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION
    vela_instantiate_admitted_stage_graph(uuid, uuid, uuid),
    vela_require_atomic_stage_graph_admission(),
    vela_lock_stage_graph_ready_capacity_path(uuid, uuid),
    vela_guard_stage_ready_queue_capacity()
FROM PUBLIC;
REVOKE EXECUTE ON FUNCTION
    vela_reject_stage_cutover_history_mutation(),
    vela_guard_stage_cutover_control_writer(),
    vela_guard_stage_graph_instantiation_work(),
    vela_enqueue_stage_graph_instantiation(),
    vela_guard_stage_graph_instantiate_command()
FROM PUBLIC;
GRANT EXECUTE ON FUNCTION
    vela_current_organization_id(),
    vela_current_project_id(),
    vela_current_request_scope()
TO vela_attempt_coordinator_owner;
GRANT EXECUTE ON FUNCTION
    vela_lock_stage_graph_ready_capacity_path(uuid, uuid)
TO vela_attempt_coordinator_owner;
GRANT EXECUTE ON FUNCTION
    vela_current_organization_id(),
    vela_current_project_id(),
    vela_current_request_scope()
TO vela_fleet_owner;
GRANT SELECT ON
    execution_graph_stages,
    execution_graph_edges,
    execution_profile_stage_options,
    stage_ready_queue_entries
TO vela_fleet_owner;
GRANT EXECUTE ON FUNCTION
    vela_instantiate_admitted_stage_graph(uuid, uuid, uuid),
    vela_lock_stage_graph_ready_capacity_path(uuid, uuid)
TO vela_request;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM stage_graph_instantiation_work
        WHERE completion_reason = 'ADMISSION_TRANSACTION_INSTANTIATED'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'atomic_stage_graph_admission_rollback_is_unsafe',
            MESSAGE = 'Atomic Stage graph Admission evidence prevents rollback';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION
    vela_instantiate_admitted_stage_graph(uuid, uuid, uuid),
    vela_lock_stage_graph_ready_capacity_path(uuid, uuid)
FROM vela_request;
REVOKE SELECT ON
    execution_graph_stages,
    execution_graph_edges,
    execution_profile_stage_options,
    stage_ready_queue_entries
FROM vela_fleet_owner;
REVOKE EXECUTE ON FUNCTION
    vela_current_organization_id(),
    vela_current_project_id(),
    vela_current_request_scope()
FROM vela_fleet_owner;
REVOKE EXECUTE ON FUNCTION
    vela_current_organization_id(),
    vela_current_project_id(),
    vela_current_request_scope()
FROM vela_attempt_coordinator_owner;
REVOKE EXECUTE ON FUNCTION
    vela_lock_stage_graph_ready_capacity_path(uuid, uuid)
FROM vela_attempt_coordinator_owner;
GRANT EXECUTE ON FUNCTION
    vela_reject_stage_cutover_history_mutation(),
    vela_guard_stage_cutover_control_writer(),
    vela_guard_stage_graph_instantiation_work(),
    vela_enqueue_stage_graph_instantiation(),
    vela_guard_stage_graph_instantiate_command()
TO PUBLIC;
DROP TRIGGER jobs_require_atomic_stage_graph_admission ON jobs;
DROP FUNCTION vela_require_atomic_stage_graph_admission();
DROP FUNCTION vela_instantiate_admitted_stage_graph(uuid, uuid, uuid);
DROP FUNCTION vela_lock_stage_graph_ready_capacity_path(uuid, uuid);
DROP TRIGGER stage_ready_queue_capacity_guard ON stage_ready_queue_entries;
DROP FUNCTION vela_guard_stage_ready_queue_capacity();
-- +goose StatementEnd
