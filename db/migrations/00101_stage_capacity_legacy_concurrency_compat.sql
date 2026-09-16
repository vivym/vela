-- +goose Up
-- +goose StatementBegin
-- Compatibility for workers provisioned before canonical concurrency publication.
-- active_stage_slots is the same bounded slot count and is only used when
-- concurrency is absent; no readiness is synthesized without a live observation.

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
                ON (worker.capacity_pool_id = pool.id
                    OR EXISTS (
                        SELECT 1
                        FROM public.worker_profile_revisions AS shared_profile
                        WHERE shared_profile.id = worker.worker_profile_revision_id
                          AND shared_profile.device_set_shape ->> 'shared_slot_exception' = 'H3_AUX_ENCODER_VAE'
                          AND stage_profile.model_component_revision = 'h3-live-vae_decoder-20260916'
                    ))
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


-- +goose StatementEnd
