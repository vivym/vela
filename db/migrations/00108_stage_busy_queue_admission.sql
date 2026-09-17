-- +goose Up
-- +goose StatementBegin
-- Idle capacity observations expire while long-running assignments renew their
-- execution leases. Admission may queue behind that authenticated live work;
-- Acquire still requires fresh free-capacity evidence. Preserve explicit newer
-- withdrawals, exact Runtime routes, identity checks and bounded queue depth.
CREATE FUNCTION public.vela_worker_has_live_stage_allocation(
    p_worker_instance_id uuid, p_worker_instance_epoch bigint,
    p_capacity_observation_sequence bigint
) RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM public.stage_allocations AS allocation
        JOIN public.stage_leases AS lease
          ON lease.stage_allocation_id = allocation.id
         AND lease.stage_attempt_id = allocation.stage_attempt_id
         AND lease.state = 'ACTIVE'
        JOIN public.stage_runs AS run ON run.id = allocation.stage_run_id
        JOIN public.stage_attempts AS physical ON physical.id = allocation.stage_attempt_id
        JOIN public.attempts AS attempt ON attempt.id = allocation.attempt_id
        JOIN public.jobs AS job ON job.id = attempt.job_id
        JOIN public.worker_instances AS worker ON worker.id = allocation.worker_instance_id
        WHERE allocation.worker_instance_id = p_worker_instance_id
          AND allocation.worker_instance_epoch = p_worker_instance_epoch
          AND allocation.capacity_observation_sequence = p_capacity_observation_sequence
          AND allocation.state = 'ALLOCATED'
          AND run.state IN ('ASSIGNED', 'RUNNING')
          AND physical.state IN ('ASSIGNED', 'RUNNING')
          AND attempt.graph_state IN ('QUEUED', 'RUNNING')
          AND job.state IN ('QUEUED', 'RUNNING')
          AND lease.attempt_fence = attempt.fence
          AND lease.attempt_fence = job.current_fence
          AND lease.stage_fence = run.fence
          AND public.vela_stage_lease_effective_expires_at(lease.id) > statement_timestamp()
          AND NOT EXISTS (
              SELECT 1 FROM public.stage_authority_renewals AS renewal
              WHERE renewal.stage_lease_id = lease.id
                AND renewal.control_session_epoch <> worker.control_session_epoch
                AND NOT EXISTS (
                    SELECT 1 FROM public.stage_authority_renewals AS newer
                    WHERE newer.stage_lease_id = lease.id
                      AND newer.issued_at > renewal.issued_at
                )
          )
          AND public.vela_worker_instance_execution_identity_matches(
              allocation.worker_instance_id, allocation.worker_instance_epoch,
              allocation.device_set_digest, allocation.membership_digest,
              allocation.model_residency_id, allocation.model_runtime_epoch
          )
    )
$$;
ALTER FUNCTION public.vela_worker_has_live_stage_allocation(uuid,bigint,bigint)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION public.vela_worker_has_live_stage_allocation(uuid,bigint,bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.vela_worker_has_live_stage_allocation(uuid,bigint,bigint)
    TO vela_fleet_owner;

CREATE OR REPLACE FUNCTION vela_lock_stage_graph_ready_capacity_path(
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
                ON worker.lifecycle_state = 'READY'
               AND worker.reachability_state = 'CONNECTED'
              JOIN public.model_residencies AS residency
                ON residency.worker_instance_id = worker.id
               AND residency.worker_instance_epoch = worker.instance_epoch
               AND residency.model_component_revision =
                   stage_profile.model_component_revision
               AND EXISTS (
                   SELECT 1 FROM public.model_runtime_capacity_routes AS route
                   WHERE route.worker_instance_id = worker.id
                     AND route.model_residency_id = residency.id
                     AND route.capacity_pool_id = pool.id
                     AND route.stage_profile_revision_id = option.stage_profile_revision_id
               )
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
                      AND (observation.expires_at > v_now
                           OR public.vela_worker_has_live_stage_allocation(
                               worker.id, worker.instance_epoch,
                               observation.observation_sequence
                           ))
                      AND COALESCE(observation.capacity_vector ->> 'concurrency', observation.capacity_vector ->> 'active_stage_slots')
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
                AND public.vela_worker_instance_execution_identity_matches(
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
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_lock_stage_graph_ready_capacity_path(
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
                ON worker.lifecycle_state = 'READY'
               AND worker.reachability_state = 'CONNECTED'
              JOIN public.model_residencies AS residency
                ON residency.worker_instance_id = worker.id
               AND residency.worker_instance_epoch = worker.instance_epoch
               AND residency.model_component_revision =
                   stage_profile.model_component_revision
               AND EXISTS (
                   SELECT 1 FROM public.model_runtime_capacity_routes AS route
                   WHERE route.worker_instance_id = worker.id
                     AND route.model_residency_id = residency.id
                     AND route.capacity_pool_id = pool.id
                     AND route.stage_profile_revision_id = option.stage_profile_revision_id
               )
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
                      AND COALESCE(observation.capacity_vector ->> 'concurrency', observation.capacity_vector ->> 'active_stage_slots')
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
DROP FUNCTION public.vela_worker_has_live_stage_allocation(uuid,bigint,bigint);
-- +goose StatementEnd
