-- +goose Up
-- +goose StatementBegin
-- READY queue admission holds the CapacityPool parent before its counter.
-- Finalizing a claim must acquire that parent before counters and FK checks.
DO $migration$
DECLARE
    v_signature text;
    v_indent text;
    v_definition text;
    v_counter_lock text;
    v_pool_lock text;
BEGIN
    FOR v_signature, v_indent IN
        SELECT entry.signature, entry.indentation FROM (VALUES
            ('public.vela_commit_stage_scheduler_claim(uuid,uuid)', '    '),
            ('public.vela_abandon_stage_scheduler_claim(uuid,text)', '    '),
            ('public.vela_reconcile_expired_stage_scheduler_claims(integer)', '        '),
            ('public.vela_account_stage_scheduler_fairness(uuid,timestamptz)', '    ')
        ) AS entry(signature, indentation)
    LOOP
        v_definition := pg_get_functiondef(v_signature::regprocedure);
        v_counter_lock := v_indent || E'PERFORM 1\n'
            || v_indent || E'FROM public.stage_capacity_pool_counters AS counter\n'
            || v_indent || E'WHERE counter.capacity_pool_id = v_claim.capacity_pool_id\n'
            || v_indent || 'FOR UPDATE;';
        v_pool_lock := v_indent || E'PERFORM 1 FROM public.capacity_pools AS pool\n'
            || v_indent || E'WHERE pool.id = v_claim.capacity_pool_id FOR KEY SHARE OF pool;\n';
        IF strpos(v_definition, v_counter_lock) = 0 OR
           (length(v_definition) - length(replace(v_definition, v_counter_lock, '')))
               <> length(v_counter_lock) OR strpos(v_definition, v_pool_lock) <> 0 THEN
            RAISE EXCEPTION 'StageScheduler claim finalization lock dependency changed: %', v_signature;
        END IF;
        EXECUTE replace(v_definition, v_counter_lock, v_pool_lock || v_counter_lock);
    END LOOP;
END
$migration$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $migration$
DECLARE
    v_signature text;
    v_indent text;
    v_definition text;
    v_counter_lock text;
    v_pool_lock text;
BEGIN
    FOR v_signature, v_indent IN
        SELECT entry.signature, entry.indentation FROM (VALUES
            ('public.vela_commit_stage_scheduler_claim(uuid,uuid)', '    '),
            ('public.vela_abandon_stage_scheduler_claim(uuid,text)', '    '),
            ('public.vela_reconcile_expired_stage_scheduler_claims(integer)', '        '),
            ('public.vela_account_stage_scheduler_fairness(uuid,timestamptz)', '    ')
        ) AS entry(signature, indentation)
    LOOP
        v_definition := pg_get_functiondef(v_signature::regprocedure);
        v_counter_lock := v_indent || E'PERFORM 1\n'
            || v_indent || E'FROM public.stage_capacity_pool_counters AS counter\n'
            || v_indent || E'WHERE counter.capacity_pool_id = v_claim.capacity_pool_id\n'
            || v_indent || 'FOR UPDATE;';
        v_pool_lock := v_indent || E'PERFORM 1 FROM public.capacity_pools AS pool\n'
            || v_indent || E'WHERE pool.id = v_claim.capacity_pool_id FOR KEY SHARE OF pool;\n';
        IF strpos(v_definition, v_pool_lock || v_counter_lock) = 0 OR
           (length(v_definition) - length(replace(v_definition, v_pool_lock, '')))
               <> length(v_pool_lock) THEN
            RAISE EXCEPTION 'StageScheduler claim finalization lock rollback dependency changed: %', v_signature;
        END IF;
        EXECUTE replace(v_definition, v_pool_lock || v_counter_lock, v_counter_lock);
    END LOOP;
END
$migration$;
-- +goose StatementEnd
