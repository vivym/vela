-- +goose Up
-- +goose StatementBegin
CREATE TABLE model_runtime_capacity_routes (
    model_residency_id uuid PRIMARY KEY,
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    capacity_pool_id uuid NOT NULL,
    stage_profile_revision_id uuid NOT NULL,
    residency_plan_revision_id uuid REFERENCES residency_plan_revisions(id),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (worker_instance_id, capacity_pool_id),
    UNIQUE (worker_instance_id, stage_profile_revision_id),
    FOREIGN KEY (capacity_pool_id, stage_profile_revision_id)
        REFERENCES capacity_pools(id, stage_profile_revision_id)
);
ALTER TABLE model_runtime_capacity_routes OWNER TO vela_fleet_owner;
REVOKE ALL ON TABLE model_runtime_capacity_routes FROM PUBLIC;
GRANT SELECT ON TABLE model_runtime_capacity_routes TO
    vela_stage_scheduler_owner,
    vela_attempt_coordinator_owner,
    vela_internal,
    vela_h3_campaign_evidence;

-- Existing resident processes keep their exact historical route during an
-- upgrade. New plans must declare every route explicitly through the wrapper
-- below; worker_instances.capacity_pool_id is no longer consulted by runtime
-- scheduling authority.
INSERT INTO model_runtime_capacity_routes (
    model_residency_id, worker_instance_id, capacity_pool_id,
    stage_profile_revision_id, residency_plan_revision_id
)
SELECT residency.id, worker.id, pool.id, pool.stage_profile_revision_id,
       worker.residency_plan_revision_id
FROM model_residencies AS residency
JOIN worker_instances AS worker ON worker.id = residency.worker_instance_id
JOIN capacity_pools AS pool ON pool.id = worker.capacity_pool_id
JOIN stage_profile_revisions AS profile
  ON profile.id = pool.stage_profile_revision_id
 AND profile.model_component_revision = residency.model_component_revision
ON CONFLICT (model_residency_id) DO NOTHING;

ALTER FUNCTION vela_apply_residency_plan(jsonb)
    RENAME TO vela_apply_residency_plan_v63;
REVOKE EXECUTE ON FUNCTION vela_apply_residency_plan_v63(jsonb)
    FROM vela_fleet, vela_internal;

CREATE FUNCTION vela_apply_residency_plan(p_plan jsonb)
RETURNS TABLE (plan_revision_id uuid, worker_instance_count integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_result record;
    v_worker jsonb;
    v_route jsonb;
    v_worker_id uuid;
    v_worker_profile_id uuid;
    v_route_count integer;
    v_existing model_runtime_capacity_routes%ROWTYPE;
BEGIN
    SELECT applied.plan_revision_id, applied.worker_instance_count
      INTO STRICT v_result
    FROM public.vela_apply_residency_plan_v63(p_plan) AS applied;

    FOR v_worker IN
        SELECT value FROM jsonb_array_elements(p_plan -> 'worker_instances')
    LOOP
        v_worker_id := (v_worker ->> 'id')::uuid;
        v_worker_profile_id := (v_worker ->> 'worker_profile_revision_id')::uuid;
        IF jsonb_typeof(v_worker -> 'model_runtime_routes') <> 'array'
           OR jsonb_array_length(v_worker -> 'model_runtime_routes') = 0
           OR jsonb_array_length(v_worker -> 'model_runtime_routes') > 64 THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'worker_instance_model_runtime_routes_invalid',
                MESSAGE = 'WorkerInstance must declare bounded ModelRuntime routes';
        END IF;
        v_route_count := 0;
        FOR v_route IN
            SELECT value FROM jsonb_array_elements(v_worker -> 'model_runtime_routes')
        LOOP
            IF (v_route ->> 'model_residency_id')::uuid IS NULL
               OR (v_route ->> 'capacity_pool_id')::uuid IS NULL
               OR (v_route ->> 'stage_profile_revision_id')::uuid IS NULL
               OR NOT EXISTS (
                    SELECT 1
                    FROM public.capacity_pools AS pool
                    JOIN public.stage_profile_revisions AS profile
                      ON profile.id = pool.stage_profile_revision_id
                    JOIN public.worker_instances AS worker
                      ON worker.id = v_worker_id
                     AND worker.worker_profile_revision_id =
                         profile.worker_profile_revision_id
                    WHERE pool.id = (v_route ->> 'capacity_pool_id')::uuid
                      AND pool.stage_profile_revision_id =
                          (v_route ->> 'stage_profile_revision_id')::uuid
                      AND pool.residency_plan_revision_id = v_result.plan_revision_id
                      AND profile.worker_profile_revision_id = v_worker_profile_id
                      AND profile.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
               ) THEN
                RAISE EXCEPTION USING
                    ERRCODE = '23514',
                    CONSTRAINT = 'model_runtime_capacity_route_profile_mismatch',
                    MESSAGE = 'ModelRuntime route does not match its certified Worker/Profile authority';
            END IF;

            INSERT INTO public.model_runtime_capacity_routes (
                model_residency_id, worker_instance_id, capacity_pool_id,
                stage_profile_revision_id, residency_plan_revision_id
            ) VALUES (
                (v_route ->> 'model_residency_id')::uuid,
                v_worker_id,
                (v_route ->> 'capacity_pool_id')::uuid,
                (v_route ->> 'stage_profile_revision_id')::uuid,
                v_result.plan_revision_id
            )
            ON CONFLICT (model_residency_id) DO NOTHING;

            SELECT route.* INTO STRICT v_existing
            FROM public.model_runtime_capacity_routes AS route
            WHERE route.model_residency_id =
                  (v_route ->> 'model_residency_id')::uuid;
            IF v_existing.worker_instance_id <> v_worker_id
               OR v_existing.capacity_pool_id <>
                  (v_route ->> 'capacity_pool_id')::uuid
               OR v_existing.stage_profile_revision_id <>
                  (v_route ->> 'stage_profile_revision_id')::uuid
               OR v_existing.residency_plan_revision_id IS DISTINCT FROM
                  v_result.plan_revision_id THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    CONSTRAINT = 'model_runtime_capacity_route_replay_conflict',
                    MESSAGE = 'ModelRuntime route replay does not match';
            END IF;
            v_route_count := v_route_count + 1;
        END LOOP;
        IF v_route_count = 0 THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'worker_instance_model_runtime_routes_invalid',
                MESSAGE = 'WorkerInstance has no ModelRuntime route';
        END IF;
    END LOOP;

    RETURN QUERY SELECT v_result.plan_revision_id, v_result.worker_instance_count;
END
$$;
ALTER FUNCTION vela_apply_residency_plan(jsonb) OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION vela_apply_residency_plan(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_apply_residency_plan(jsonb) TO vela_fleet;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure
    );
    v_old := E'      AND worker.capacity_pool_id = v_pool.id\n'
        || E'      AND worker.instance_epoch = (p_authority ->> ''worker_instance_epoch'')::bigint\n';
    v_new := E'      AND EXISTS (\n'
        || E'          SELECT 1 FROM public.model_runtime_capacity_routes AS route\n'
        || E'          WHERE route.worker_instance_id = worker.id\n'
        || E'            AND route.model_residency_id =\n'
        || E'                (p_authority ->> ''model_residency_id'')::uuid\n'
        || E'            AND route.capacity_pool_id = v_pool.id\n'
        || E'            AND route.stage_profile_revision_id =\n'
        || E'                v_pool.stage_profile_revision_id\n'
        || E'      )\n'
        || E'      AND worker.instance_epoch = (p_authority ->> ''worker_instance_epoch'')::bigint\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_route_scheduler_dependency_drift',
            MESSAGE = 'StageScheduler WorkerInstance route dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_verify_stage_worker_member_registration(jsonb)'::regprocedure
    );
    v_old := E'        JOIN capacity_pools AS pool ON pool.id = worker.capacity_pool_id\n'
        || E'        JOIN stage_profile_revisions AS profile\n'
        || E'          ON profile.id = v_stage_profile_revision_id\n';
    v_new := E'        JOIN model_runtime_capacity_routes AS route\n'
        || E'          ON route.worker_instance_id = worker.id\n'
        || E'         AND route.model_residency_id = v_model_residency_id\n'
        || E'         AND route.stage_profile_revision_id = v_stage_profile_revision_id\n'
        || E'        JOIN capacity_pools AS pool ON pool.id = route.capacity_pool_id\n'
        || E'        JOIN stage_profile_revisions AS profile\n'
        || E'          ON profile.id = v_stage_profile_revision_id\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_route_registration_dependency_drift',
            MESSAGE = 'ModelRuntime registration route dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_read_stage_worker_acquire_authority_v1(uuid)'::regprocedure
    );
    v_old := E'    SELECT pool.* INTO v_pool FROM capacity_pools AS pool\n'
        || E'    WHERE pool.id = v_worker.capacity_pool_id;\n';
    v_new := E'    SELECT pool.* INTO v_pool\n'
        || E'    FROM model_runtime_capacity_routes AS route\n'
        || E'    JOIN capacity_pools AS pool ON pool.id = route.capacity_pool_id\n'
        || E'    WHERE route.worker_instance_id = v_worker.id\n'
        || E'      AND route.model_residency_id = v_intent.model_residency_id\n'
        || E'      AND route.stage_profile_revision_id =\n'
        || E'          v_intent.stage_profile_revision_id;\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_route_acquire_dependency_drift',
            MESSAGE = 'Stage Worker acquire route dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_lock_stage_graph_ready_capacity_path(uuid,uuid)'::regprocedure
    );
    v_old := E'              JOIN public.worker_instances AS worker\n'
        || E'                ON worker.capacity_pool_id = pool.id\n'
        || E'               AND worker.lifecycle_state = ''READY''\n'
        || E'               AND worker.reachability_state = ''CONNECTED''\n'
        || E'              JOIN public.model_residencies AS residency\n'
        || E'                ON residency.worker_instance_id = worker.id\n'
        || E'               AND residency.worker_instance_epoch = worker.instance_epoch\n'
        || E'               AND residency.model_component_revision =\n'
        || E'                   stage_profile.model_component_revision\n';
    v_new := E'              JOIN public.worker_instances AS worker\n'
        || E'                ON worker.lifecycle_state = ''READY''\n'
        || E'               AND worker.reachability_state = ''CONNECTED''\n'
        || E'              JOIN public.model_residencies AS residency\n'
        || E'                ON residency.worker_instance_id = worker.id\n'
        || E'               AND residency.worker_instance_epoch = worker.instance_epoch\n'
        || E'               AND residency.model_component_revision =\n'
        || E'                   stage_profile.model_component_revision\n'
        || E'               AND EXISTS (\n'
        || E'                   SELECT 1\n'
        || E'                   FROM public.model_runtime_capacity_routes AS route\n'
        || E'                   WHERE route.worker_instance_id = worker.id\n'
        || E'                     AND route.model_residency_id = residency.id\n'
        || E'                     AND route.capacity_pool_id = pool.id\n'
        || E'                     AND route.stage_profile_revision_id =\n'
        || E'                         option.stage_profile_revision_id\n'
        || E'               )\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_route_admission_dependency_drift',
            MESSAGE = 'Admission capacity path route dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_lock_stage_graph_ready_capacity_path(uuid,uuid)'::regprocedure
    );
    v_old := E'              JOIN public.worker_instances AS worker\n'
        || E'                ON worker.lifecycle_state = ''READY''\n'
        || E'               AND worker.reachability_state = ''CONNECTED''\n'
        || E'              JOIN public.model_residencies AS residency\n'
        || E'                ON residency.worker_instance_id = worker.id\n'
        || E'               AND residency.worker_instance_epoch = worker.instance_epoch\n'
        || E'               AND residency.model_component_revision =\n'
        || E'                   stage_profile.model_component_revision\n'
        || E'               AND EXISTS (\n'
        || E'                   SELECT 1\n'
        || E'                   FROM public.model_runtime_capacity_routes AS route\n'
        || E'                   WHERE route.worker_instance_id = worker.id\n'
        || E'                     AND route.model_residency_id = residency.id\n'
        || E'                     AND route.capacity_pool_id = pool.id\n'
        || E'                     AND route.stage_profile_revision_id =\n'
        || E'                         option.stage_profile_revision_id\n'
        || E'               )\n';
    v_new := E'              JOIN public.worker_instances AS worker\n'
        || E'                ON worker.capacity_pool_id = pool.id\n'
        || E'               AND worker.lifecycle_state = ''READY''\n'
        || E'               AND worker.reachability_state = ''CONNECTED''\n'
        || E'              JOIN public.model_residencies AS residency\n'
        || E'                ON residency.worker_instance_id = worker.id\n'
        || E'               AND residency.worker_instance_epoch = worker.instance_epoch\n'
        || E'               AND residency.model_component_revision =\n'
        || E'                   stage_profile.model_component_revision\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_route_admission_rollback_drift',
            MESSAGE = 'Admission capacity path route rollback changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_read_stage_worker_acquire_authority_v1(uuid)'::regprocedure
    );
    v_old := E'    SELECT pool.* INTO v_pool\n'
        || E'    FROM model_runtime_capacity_routes AS route\n'
        || E'    JOIN capacity_pools AS pool ON pool.id = route.capacity_pool_id\n'
        || E'    WHERE route.worker_instance_id = v_worker.id\n'
        || E'      AND route.model_residency_id = v_intent.model_residency_id\n'
        || E'      AND route.stage_profile_revision_id =\n'
        || E'          v_intent.stage_profile_revision_id;\n';
    v_new := E'    SELECT pool.* INTO v_pool FROM capacity_pools AS pool\n'
        || E'    WHERE pool.id = v_worker.capacity_pool_id;\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_route_acquire_rollback_drift',
            MESSAGE = 'Stage Worker acquire route rollback changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_verify_stage_worker_member_registration(jsonb)'::regprocedure
    );
    v_old := E'        JOIN model_runtime_capacity_routes AS route\n'
        || E'          ON route.worker_instance_id = worker.id\n'
        || E'         AND route.model_residency_id = v_model_residency_id\n'
        || E'         AND route.stage_profile_revision_id = v_stage_profile_revision_id\n'
        || E'        JOIN capacity_pools AS pool ON pool.id = route.capacity_pool_id\n'
        || E'        JOIN stage_profile_revisions AS profile\n'
        || E'          ON profile.id = v_stage_profile_revision_id\n';
    v_new := E'        JOIN capacity_pools AS pool ON pool.id = worker.capacity_pool_id\n'
        || E'        JOIN stage_profile_revisions AS profile\n'
        || E'          ON profile.id = v_stage_profile_revision_id\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_route_registration_rollback_drift',
            MESSAGE = 'ModelRuntime registration route rollback changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure
    );
    v_old := E'      AND EXISTS (\n'
        || E'          SELECT 1 FROM public.model_runtime_capacity_routes AS route\n'
        || E'          WHERE route.worker_instance_id = worker.id\n'
        || E'            AND route.model_residency_id =\n'
        || E'                (p_authority ->> ''model_residency_id'')::uuid\n'
        || E'            AND route.capacity_pool_id = v_pool.id\n'
        || E'            AND route.stage_profile_revision_id =\n'
        || E'                v_pool.stage_profile_revision_id\n'
        || E'      )\n'
        || E'      AND worker.instance_epoch = (p_authority ->> ''worker_instance_epoch'')::bigint\n';
    v_new := E'      AND worker.capacity_pool_id = v_pool.id\n'
        || E'      AND worker.instance_epoch = (p_authority ->> ''worker_instance_epoch'')::bigint\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_route_scheduler_rollback_drift',
            MESSAGE = 'StageScheduler route rollback changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

REVOKE EXECUTE ON FUNCTION vela_apply_residency_plan(jsonb) FROM vela_fleet;
DROP FUNCTION vela_apply_residency_plan(jsonb);
ALTER FUNCTION vela_apply_residency_plan_v63(jsonb)
    RENAME TO vela_apply_residency_plan;
GRANT EXECUTE ON FUNCTION vela_apply_residency_plan(jsonb) TO vela_fleet;
DROP TABLE model_runtime_capacity_routes;
-- +goose StatementEnd
