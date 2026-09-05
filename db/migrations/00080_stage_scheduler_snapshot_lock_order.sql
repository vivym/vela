-- +goose Up
-- +goose StatementBegin
-- Snapshot and decision insertion take this parent lock through foreign keys.
-- Take it before the counter lock to match Admission graph instantiation.
GRANT UPDATE (id) ON capacity_pools TO vela_stage_scheduler_owner;
DO $$
DECLARE v_definition text; v_old text; v_new text;
BEGIN
    v_definition := pg_get_functiondef('vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure);
    v_old := E'      AND pool.state = ''ACTIVE'';';
    v_new := E'      AND pool.state = ''ACTIVE''\n    FOR KEY SHARE OF pool;';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageScheduler snapshot parent lock dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
    v_definition := pg_get_functiondef('vela_claim_stage_scheduler_decision(jsonb)'::regprocedure);
    v_old := E'    SELECT counter.* INTO v_counter\n'
        || E'    FROM public.stage_capacity_pool_counters AS counter\n'
        || E'    WHERE counter.capacity_pool_id = v_trace.capacity_pool_id\n'
        || E'    FOR UPDATE;';
    v_new := E'    PERFORM 1 FROM public.capacity_pools AS pool\n'
        || E'    WHERE pool.id = v_trace.capacity_pool_id FOR KEY SHARE OF pool;\n' || v_old;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION 'StageScheduler claim parent lock dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE v_definition text; v_old text; v_new text;
BEGIN
    v_definition := pg_get_functiondef('vela_claim_stage_scheduler_decision(jsonb)'::regprocedure);
    v_old := E'    SELECT counter.* INTO v_counter\n'
        || E'    FROM public.stage_capacity_pool_counters AS counter\n'
        || E'    WHERE counter.capacity_pool_id = v_trace.capacity_pool_id\n'
        || E'    FOR UPDATE;';
    v_new := E'    PERFORM 1 FROM public.capacity_pools AS pool\n'
        || E'    WHERE pool.id = v_trace.capacity_pool_id FOR KEY SHARE OF pool;\n' || v_old;
    IF strpos(v_definition, v_new) = 0 THEN
        RAISE EXCEPTION 'StageScheduler claim parent lock rollback dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_new, v_old);
    v_definition := pg_get_functiondef('vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure);
    v_old := E'      AND pool.state = ''ACTIVE'';';
    v_new := E'      AND pool.state = ''ACTIVE''\n    FOR KEY SHARE OF pool;';
    IF strpos(v_definition, v_new) = 0 THEN
        RAISE EXCEPTION 'StageScheduler snapshot parent lock rollback dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_new, v_old);
END
$$;
REVOKE UPDATE (id) ON capacity_pools FROM vela_stage_scheduler_owner;
-- +goose StatementEnd
