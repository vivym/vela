-- +goose Up
-- +goose StatementBegin
-- Legacy Worker compatibility for canonical concurrency and shared H3 AUX slots.

CREATE OR REPLACE FUNCTION vela_capture_stage_scheduler_snapshot(p_authority jsonb)
RETURNS TABLE (snapshot_id uuid, snapshot jsonb)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_snapshot_id uuid;
    v_pool public.capacity_pools%ROWTYPE;
    v_counter public.stage_capacity_pool_counters%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_residency public.model_residencies%ROWTYPE;
    v_observation public.capacity_observations%ROWTYPE;
    v_candidates jsonb;
    v_snapshot jsonb;
    v_active integer;
    v_limit integer;
BEGIN
    IF p_authority IS NULL OR jsonb_typeof(p_authority) <> 'object'
       OR (p_authority ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'StageScheduler authority is invalid';
    END IF;
    SELECT pool.* INTO v_pool
    FROM public.capacity_pools AS pool
    WHERE pool.id = (p_authority ->> 'capacity_pool_id')::uuid
      AND pool.stage_profile_revision_id =
          (p_authority ->> 'stage_profile_revision_id')::uuid
      AND pool.state = 'ACTIVE';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_capacity_pool_stale',
            MESSAGE = 'StageScheduler CapacityPool authority is stale';
    END IF;
    SELECT counter.* INTO v_counter
    FROM public.stage_capacity_pool_counters AS counter
    WHERE counter.capacity_pool_id = v_pool.id
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_capacity_pool_stale',
            MESSAGE = 'StageScheduler CapacityPool counter is missing';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.stage_scheduler_activation_stops AS stop
        WHERE stop.algorithm_revision = 'stage-filter-fairness-score-pick-v1'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_scheduler_activation_stopped',
            MESSAGE = 'StageScheduler activation is stopped after shadow replay divergence';
    END IF;
    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = (p_authority ->> 'worker_instance_id')::uuid
      AND (worker.capacity_pool_id = v_pool.id
           OR EXISTS (
                SELECT 1 FROM public.worker_profile_revisions AS shared_profile
                WHERE shared_profile.id = worker.worker_profile_revision_id
                  AND shared_profile.device_set_shape ->> 'shared_slot_exception' = 'H3_AUX_ENCODER_VAE'
                  AND EXISTS (
                      SELECT 1 FROM public.stage_profile_revisions AS pool_profile
                      WHERE pool_profile.id = v_pool.stage_profile_revision_id
                        AND pool_profile.model_component_revision = 'h3-live-vae_decoder-20260916'
                  )
           ))
      AND worker.instance_epoch = (p_authority ->> 'worker_instance_epoch')::bigint
      AND worker.lifecycle_state = 'READY'
      AND worker.reachability_state = 'CONNECTED'
      AND worker.device_set_digest = decode(p_authority ->> 'device_set_digest', 'hex')
      AND worker.membership_digest = decode(p_authority ->> 'membership_digest', 'hex');
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_worker_authority_stale',
            MESSAGE = 'StageScheduler WorkerInstance authority is stale';
    END IF;
    SELECT residency.* INTO v_residency
    FROM public.model_residencies AS residency
    JOIN public.stage_profile_revisions AS profile
      ON profile.id = v_pool.stage_profile_revision_id
     AND profile.model_component_revision = residency.model_component_revision
    WHERE residency.id = (p_authority ->> 'model_residency_id')::uuid
      AND residency.worker_instance_id = v_worker.id
      AND residency.worker_instance_epoch = v_worker.instance_epoch
      AND residency.model_runtime_epoch = (p_authority ->> 'model_runtime_epoch')::bigint
      AND residency.state = 'READY';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_model_residency_stale',
            MESSAGE = 'StageScheduler ModelResidency authority is stale';
    END IF;
    SELECT observation.* INTO v_observation
    FROM public.capacity_observations AS observation
    WHERE observation.worker_instance_id = v_worker.id
      AND observation.worker_instance_epoch = v_worker.instance_epoch
      AND observation.observation_sequence =
          (p_authority ->> 'observation_sequence')::bigint
      AND observation.expires_at > v_now
      AND observation.capacity_vector = p_authority -> 'capacity_vector';
    IF NOT FOUND OR NOT public.vela_worker_instance_authority_matches(
        v_worker.id, v_worker.instance_epoch, v_worker.device_set_digest,
        v_worker.membership_digest, v_residency.id, v_residency.model_runtime_epoch
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_capacity_observation_stale',
            MESSAGE = 'StageScheduler CapacityObservation authority is stale';
    END IF;

    SELECT count(*)::integer INTO v_active
    FROM public.stage_allocations AS allocation
    WHERE allocation.worker_instance_id = v_worker.id
      AND allocation.worker_instance_epoch = v_worker.instance_epoch
      AND allocation.state = 'ALLOCATED';
    v_limit := COALESCE((v_observation.capacity_vector ->> 'concurrency')::integer, (v_observation.capacity_vector ->> 'active_stage_slots')::integer, 0);
    IF v_limit <= 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_capacity_vector_invalid',
            MESSAGE = 'StageScheduler CapacityObservation has no schedulable concurrency';
    END IF;

    SELECT COALESCE(jsonb_agg(candidate.value ORDER BY candidate.stage_run_id), '[]'::jsonb)
    INTO v_candidates
    FROM (
        SELECT ready.stage_run_id, jsonb_build_object(
            'stage_run_id', run.id,
            'attempt_id', run.attempt_id,
            'stage_profile_revision_id', ready.stage_profile_revision_id,
            'organization_id', ready.organization_id,
            'service_class_revision_id', ready.service_class_revision_id,
            'project_id', ready.project_id,
            'lane', CASE
                WHEN run.retry_count > 0 THEN 'RETRY'
                WHEN EXTRACT(EPOCH FROM (v_now - ready.enqueued_at)) >=
                     service_class.max_queue_wait_before_protection_seconds THEN 'PROTECTED'
                ELSE 'NORMAL'
            END,
            'enqueued_at', ready.enqueued_at,
            'attempt_fence', attempt.fence,
            'stage_fence', run.fence,
            'stage_version', run.version,
            'resource_millis', ready.resource_millis,
            'organization_deficit_millis', organization_deficit.deficit_millis,
            'service_class_deficit_millis', service_deficit.deficit_millis,
            'project_deficit_millis', project_deficit.deficit_millis,
            'score', jsonb_build_object(
                'locality_credit_millis', 0,
                'transfer_penalty_millis', 0,
                'load_penalty_millis', 0,
                'predicted_finish_millis', ready.resource_millis * (v_active + 1),
                'critical_path_credit_millis', 0,
                'age_credit_millis', LEAST(
                    service_class.max_aging_credit_seconds::bigint * 1000,
                    GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (v_now - ready.enqueued_at)) * 1000))::bigint
                )
            ),
            'filter_reasons', CASE
                WHEN run.state <> 'READY' THEN jsonb_build_array('STAGE_NOT_READY')
                WHEN EXISTS (
                    SELECT 1 FROM public.stage_scheduler_claims AS live
                    WHERE live.stage_run_id = run.id AND live.state = 'CLAIMED'
                      AND live.claim_expires_at > v_now
                ) THEN jsonb_build_array('STAGE_NOT_READY')
                WHEN v_active >= v_limit THEN jsonb_build_array('CAPACITY_EXHAUSTED')
                ELSE '[]'::jsonb
            END
        ) AS value
        FROM public.stage_ready_queue_entries AS ready
        JOIN public.stage_runs AS run ON run.id = ready.stage_run_id
        JOIN public.attempts AS attempt ON attempt.id = run.attempt_id
        JOIN public.jobs AS job ON job.id = attempt.job_id
        JOIN public.service_class_revisions AS service_class
          ON service_class.id = ready.service_class_revision_id
        JOIN public.stage_scheduler_organization_deficits AS organization_deficit
          ON organization_deficit.capacity_pool_id = ready.capacity_pool_id
         AND organization_deficit.organization_id = ready.organization_id
        JOIN public.stage_scheduler_service_class_deficits AS service_deficit
          ON service_deficit.capacity_pool_id = ready.capacity_pool_id
         AND service_deficit.organization_id = ready.organization_id
         AND service_deficit.service_class_revision_id = ready.service_class_revision_id
        JOIN public.stage_scheduler_project_deficits AS project_deficit
          ON project_deficit.capacity_pool_id = ready.capacity_pool_id
         AND project_deficit.organization_id = ready.organization_id
         AND project_deficit.service_class_revision_id = ready.service_class_revision_id
         AND project_deficit.project_id = ready.project_id
        WHERE ready.capacity_pool_id = v_pool.id
          AND attempt.execution_authority_kind = 'STAGE_GRAPH'
          AND attempt.graph_state IN ('QUEUED', 'RUNNING')
          AND attempt.fence = job.current_fence
    ) AS candidate;

    v_snapshot_id := gen_random_uuid();
    v_snapshot := jsonb_build_object(
        'algorithm_revision', 'stage-filter-fairness-score-pick-v1',
        'evaluated_at', v_now,
        'valid_until', v_observation.expires_at,
        'capacity_pool_id', v_pool.id,
        'capacity_pool_version', v_counter.version,
        'worker_instance_id', v_worker.id,
        'worker_instance_epoch', v_worker.instance_epoch,
        'observation_sequence', v_observation.observation_sequence,
        'candidates', v_candidates
    );
    INSERT INTO public.stage_scheduler_snapshot_traces (
        id, algorithm_revision, evaluated_at, valid_until, capacity_pool_id,
        capacity_pool_version,
        worker_instance_id, worker_instance_epoch, device_set_digest,
        membership_digest, model_residency_id, model_runtime_epoch,
        observation_sequence, capacity_vector, snapshot
    ) VALUES (
        v_snapshot_id, 'stage-filter-fairness-score-pick-v1', v_now,
        v_observation.expires_at, v_pool.id, v_counter.version,
        v_worker.id, v_worker.instance_epoch,
        v_worker.device_set_digest, v_worker.membership_digest, v_residency.id,
        v_residency.model_runtime_epoch, v_observation.observation_sequence,
        v_observation.capacity_vector, v_snapshot
    );
    RETURN QUERY SELECT v_snapshot_id, v_snapshot;
END
$$;
REVOKE ALL ON FUNCTION vela_capture_stage_scheduler_snapshot(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_capture_stage_scheduler_snapshot(jsonb) OWNER TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_capture_stage_scheduler_snapshot(jsonb) TO vela_stage_scheduler;


-- +goose StatementEnd
