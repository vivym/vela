-- +goose Up
-- +goose StatementBegin
-- Extract the existing candidate policy verbatim. Advisory reads do not lock or
-- persist a snapshot; every actual claim still captures fresh durable evidence.
DO $migration$
DECLARE
    v_definition text;
    v_start integer;
    v_end integer;
BEGIN
    v_definition := pg_get_functiondef('vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure);
    v_start := strpos(v_definition, '    INSERT INTO public.stage_scheduler_snapshot_traces (');
    v_end := strpos(v_definition, '    RETURN QUERY SELECT v_snapshot_id, v_snapshot;');
    IF v_start = 0 OR v_end <= v_start
       OR strpos(v_definition, E'\n    FOR KEY SHARE OF pool;') = 0
       OR strpos(v_definition, E'\n    FOR SHARE;') = 0 THEN
        RAISE EXCEPTION 'Stage scheduler candidate reader dependency changed';
    END IF;
    v_definition := substring(v_definition FROM 1 FOR v_start - 1)
        || substring(v_definition FROM v_end);
    v_definition := replace(v_definition,
        'public.vela_capture_stage_scheduler_snapshot(p_authority jsonb)',
        'public.vela_read_stage_scheduler_snapshot(p_authority jsonb)');
    v_definition := replace(v_definition, E' LANGUAGE plpgsql\n', E' LANGUAGE plpgsql\n STABLE\n');
    v_definition := replace(v_definition, E'\n    FOR KEY SHARE OF pool;', ';');
    v_definition := replace(v_definition, E'\n    FOR SHARE;', ';');
    EXECUTE v_definition;
END
$migration$;
ALTER FUNCTION vela_read_stage_scheduler_snapshot(jsonb) OWNER TO vela_stage_scheduler_owner;
REVOKE ALL ON FUNCTION vela_read_stage_scheduler_snapshot(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_scheduler_snapshot(jsonb) TO vela_attempt_coordinator_owner;

CREATE OR REPLACE FUNCTION vela_capture_stage_scheduler_snapshot(p_authority jsonb)
RETURNS TABLE (snapshot_id uuid, snapshot jsonb)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_snapshot_id uuid;
    v_snapshot jsonb;
BEGIN
    IF p_authority IS NULL OR jsonb_typeof(p_authority) <> 'object'
       OR (p_authority ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'StageScheduler authority is invalid';
    END IF;
    -- Preserve migration 80's parent-before-counter order for durable capture.
    PERFORM 1 FROM public.capacity_pools AS pool
    WHERE pool.id = (p_authority ->> 'capacity_pool_id')::uuid
    FOR KEY SHARE OF pool;
    PERFORM 1 FROM public.stage_capacity_pool_counters AS counter
    WHERE counter.capacity_pool_id = (p_authority ->> 'capacity_pool_id')::uuid
    FOR SHARE;
    SELECT captured.snapshot_id, captured.snapshot INTO STRICT v_snapshot_id, v_snapshot
    FROM public.vela_read_stage_scheduler_snapshot(p_authority) AS captured;
    INSERT INTO public.stage_scheduler_snapshot_traces (
        id, algorithm_revision, evaluated_at, valid_until, capacity_pool_id,
        capacity_pool_version,
        worker_instance_id, worker_instance_epoch, device_set_digest,
        membership_digest, model_residency_id, model_runtime_epoch,
        observation_sequence, capacity_vector, snapshot
    ) VALUES (
        v_snapshot_id, v_snapshot ->> 'algorithm_revision',
        (v_snapshot ->> 'evaluated_at')::timestamptz,
        (v_snapshot ->> 'valid_until')::timestamptz,
        (v_snapshot ->> 'capacity_pool_id')::uuid,
        (v_snapshot ->> 'capacity_pool_version')::bigint,
        (v_snapshot ->> 'worker_instance_id')::uuid,
        (v_snapshot ->> 'worker_instance_epoch')::bigint,
        decode(p_authority ->> 'device_set_digest', 'hex'),
        decode(p_authority ->> 'membership_digest', 'hex'),
        (p_authority ->> 'model_residency_id')::uuid,
        (p_authority ->> 'model_runtime_epoch')::bigint,
        (v_snapshot ->> 'observation_sequence')::bigint,
        p_authority -> 'capacity_vector', v_snapshot
    );
    RETURN QUERY SELECT v_snapshot_id, v_snapshot;
END
$$;

CREATE OR REPLACE FUNCTION vela_stage_worker_acquire_queue_empty(p_command jsonb)
RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_intent public.stage_worker_acquire_intents%ROWTYPE;
    v_decision text;
    v_authority jsonb;
    v_snapshot jsonb;
BEGIN
    IF EXISTS (SELECT 1 FROM public.stage_worker_acquire_intents AS intent
        WHERE intent.command_id = (p_command ->> 'command_id')::uuid) THEN
        RETURN false;
    END IF;
    SELECT * INTO v_intent FROM jsonb_populate_record(
        NULL::public.stage_worker_acquire_intents,
        p_command || jsonb_build_object('spiffe_id_digest',
            decode(p_command ->> 'spiffe_id_digest', 'hex')));
    SELECT checked.decision, checked.authority INTO STRICT v_decision, v_authority
    FROM public.vela_stage_worker_acquire_intent(v_intent) AS checked;
    IF v_decision <> 'AUTHORIZED' THEN RETURN false; END IF;
    IF EXISTS (
        SELECT 1 FROM public.stage_allocations AS allocation
        WHERE allocation.worker_instance_id = v_intent.worker_instance_id
          AND allocation.state = 'ALLOCATED'
    ) THEN RETURN false; END IF;
    SELECT captured.snapshot INTO STRICT v_snapshot
    FROM public.vela_read_stage_scheduler_snapshot(v_authority || jsonb_build_object(
        'schema_version', 1,
        'observation_sequence', v_authority -> 'capacity_observation_sequence'
    )) AS captured;
    -- This only rejects a wholly filtered candidate set. Selection, fairness,
    -- new snapshots, claim retries and CAS remain in the original scheduler.
    RETURN NOT EXISTS (
        SELECT 1 FROM jsonb_array_elements(v_snapshot -> 'candidates') AS candidate
        WHERE jsonb_array_length(candidate -> 'filter_reasons') = 0
    );
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_stage_worker_acquire_queue_empty(p_command jsonb)
RETURNS boolean
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_intent public.stage_worker_acquire_intents%ROWTYPE;
    v_decision text;
    v_authority jsonb;
BEGIN
    -- Any durable intent must take the full replay path, even after its queue
    -- entry disappeared or Worker authority expired.
    IF EXISTS (SELECT 1 FROM public.stage_worker_acquire_intents AS intent
        WHERE intent.command_id = (p_command ->> 'command_id')::uuid) THEN
        RETURN false;
    END IF;
    SELECT * INTO v_intent FROM jsonb_populate_record(
        NULL::public.stage_worker_acquire_intents,
        p_command || jsonb_build_object('spiffe_id_digest',
            decode(p_command ->> 'spiffe_id_digest', 'hex')));
    SELECT checked.decision, checked.authority INTO STRICT v_decision, v_authority
    FROM public.vela_stage_worker_acquire_intent(v_intent) AS checked;
    IF v_decision <> 'AUTHORIZED' THEN RETURN false; END IF;
    -- This is an empty-queue hint, not a second scheduler eligibility policy.
    -- Contention or blocked work continues through Filter/Fairness/Score/Pick.
    RETURN NOT EXISTS (
        SELECT 1 FROM public.stage_ready_queue_entries AS ready
        WHERE ready.capacity_pool_id = (v_authority ->> 'capacity_pool_id')::uuid
    ) AND NOT EXISTS (
        SELECT 1 FROM public.stage_allocations AS allocation
        WHERE allocation.worker_instance_id = v_intent.worker_instance_id
          AND allocation.state = 'ALLOCATED'
    );
END
$$;

DO $migration$
DECLARE
    v_definition text;
    v_insert text := $body$    INSERT INTO public.stage_scheduler_snapshot_traces (
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
$body$;
BEGIN
    v_definition := pg_get_functiondef('vela_read_stage_scheduler_snapshot(jsonb)'::regprocedure);
    IF strpos(v_definition, '    RETURN QUERY SELECT v_snapshot_id, v_snapshot;') = 0
       OR strpos(v_definition, E'      AND pool.state = ''ACTIVE'';') = 0
       OR strpos(v_definition, '    WHERE counter.capacity_pool_id = v_pool.id;') = 0 THEN
        RAISE EXCEPTION 'Stage scheduler candidate reader rollback dependency changed';
    END IF;
    v_definition := replace(v_definition,
        'public.vela_read_stage_scheduler_snapshot(p_authority jsonb)',
        'public.vela_capture_stage_scheduler_snapshot(p_authority jsonb)');
    v_definition := replace(v_definition, ' STABLE', ' VOLATILE');
    v_definition := replace(v_definition, E'      AND pool.state = ''ACTIVE'';',
        E'      AND pool.state = ''ACTIVE''\n    FOR KEY SHARE OF pool;');
    v_definition := replace(v_definition, '    WHERE counter.capacity_pool_id = v_pool.id;',
        E'    WHERE counter.capacity_pool_id = v_pool.id\n    FOR SHARE;');
    v_definition := replace(v_definition, '    RETURN QUERY SELECT v_snapshot_id, v_snapshot;',
        v_insert || '    RETURN QUERY SELECT v_snapshot_id, v_snapshot;');
    EXECUTE v_definition;
END
$migration$;
DROP FUNCTION vela_read_stage_scheduler_snapshot(jsonb);
-- +goose StatementEnd
