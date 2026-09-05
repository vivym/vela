-- +goose Up
-- +goose StatementBegin
-- Both durable acquisition and the idle probe use the same authority readers.
-- Preserve the previous entry point only for an exact schema rollback.
DO $$
DECLARE
    v_definition text;
    v_original text;
    v_source text;
    v_target text;
    v_lookup text;
BEGIN
    FOR v_source, v_target IN SELECT * FROM (VALUES
        ('vela_read_stage_worker_acquire_authority_v1', 'vela_stage_worker_acquire_intent_base'),
        ('vela_read_stage_worker_acquire_authority_v2', 'vela_stage_worker_acquire_intent_barrier'),
        ('vela_read_stage_worker_acquire_authority', 'vela_stage_worker_acquire_intent')
    ) AS reader(source_name, target_name)
    LOOP
        v_original := pg_get_functiondef(('public.' || v_source || '(uuid)')::regprocedure);
        v_definition := replace(v_original, v_source || '(p_command_id uuid)',
            v_target || '(p_intent public.stage_worker_acquire_intents)');
        IF v_definition = v_original THEN
            RAISE EXCEPTION 'Stage acquire authority reader signature changed: %', v_source;
        END IF;
        v_definition := replace(v_definition,
            'v_intent stage_worker_acquire_intents%ROWTYPE;',
            'v_intent stage_worker_acquire_intents%ROWTYPE := p_intent;');
        IF v_source = 'vela_read_stage_worker_acquire_authority_v1' THEN
            v_lookup := E'    SELECT intent.* INTO v_intent FROM stage_worker_acquire_intents AS intent\n'
                || E'    WHERE intent.command_id = p_command_id;\n';
            IF strpos(v_definition, v_lookup) = 0 THEN
                RAISE EXCEPTION 'Stage acquire base lookup changed';
            END IF;
            v_definition := replace(v_definition, v_lookup, '');
        ELSIF v_source = 'vela_read_stage_worker_acquire_authority_v2' THEN
            v_lookup := E'    SELECT intent.* INTO STRICT v_intent\n'
                || E'    FROM stage_worker_acquire_intents AS intent\n'
                || E'    WHERE intent.command_id = p_command_id;\n';
            IF strpos(v_definition, v_lookup) = 0 THEN
                RAISE EXCEPTION 'Stage acquire barrier lookup changed';
            END IF;
            v_definition := replace(v_definition, v_lookup, '');
            v_definition := replace(v_definition,
                'vela_read_stage_worker_acquire_authority_v1(p_command_id)',
                'vela_stage_worker_acquire_intent_base(p_intent)');
        ELSE
            v_definition := replace(v_definition,
                'vela_read_stage_worker_acquire_authority_v2(p_command_id)',
                'vela_stage_worker_acquire_intent_barrier(p_intent)');
        END IF;
        IF strpos(v_definition, 'p_command_id') <> 0 THEN
            RAISE EXCEPTION 'Stage acquire reader still depends on durable lookup: %', v_source;
        END IF;
        EXECUTE v_definition;
        EXECUTE format('ALTER FUNCTION public.%I(public.stage_worker_acquire_intents) OWNER TO vela_attempt_coordinator_owner', v_target);
        EXECUTE format('REVOKE ALL ON FUNCTION public.%I(public.stage_worker_acquire_intents) FROM PUBLIC', v_target);
    END LOOP;
END
$$;

ALTER FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    RENAME TO vela_read_stage_worker_acquire_authority_v77;
REVOKE EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority_v77(uuid)
    FROM vela_stage_worker_control;

CREATE FUNCTION vela_read_stage_worker_acquire_authority(p_command_id uuid)
RETURNS TABLE (decision text, reason text, authority jsonb)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_intent public.stage_worker_acquire_intents%ROWTYPE;
BEGIN
    SELECT intent.* INTO v_intent FROM public.stage_worker_acquire_intents AS intent
    WHERE intent.command_id = p_command_id;
    RETURN QUERY SELECT * FROM public.vela_stage_worker_acquire_intent(v_intent);
END
$$;
ALTER FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_worker_acquire_authority(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    TO vela_stage_worker_control;

CREATE FUNCTION vela_stage_worker_acquire_queue_empty(p_command jsonb)
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
GRANT SELECT ON stage_ready_queue_entries TO vela_attempt_coordinator_owner;
ALTER FUNCTION vela_stage_worker_acquire_queue_empty(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_stage_worker_acquire_queue_empty(jsonb) FROM PUBLIC;

ALTER FUNCTION vela_begin_stage_worker_acquire(jsonb)
    RENAME TO vela_begin_stage_worker_acquire_v77;
REVOKE EXECUTE ON FUNCTION vela_begin_stage_worker_acquire_v77(jsonb)
    FROM vela_stage_worker_control;
CREATE FUNCTION vela_begin_stage_worker_acquire(p_command jsonb)
RETURNS TABLE (requested_at timestamptz, result_kind text, assignment_wire bytea,
    retry_after_ms bigint, detail text)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_retry bigint := (p_command ->> 'idle_retry_after_ms')::bigint;
    v_payload jsonb := p_command - 'idle_retry_after_ms';
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) IS DISTINCT FROM 'object'
       OR (p_command ->> 'schema_version')::integer IS DISTINCT FROM 1 THEN
        RAISE EXCEPTION USING ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_acquire_invalid', MESSAGE = 'Stage Worker acquire command is invalid';
    END IF;
    IF v_retry IS NOT NULL AND v_retry NOT BETWEEN 1 AND 3600000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_acquire_invalid', MESSAGE = 'Idle retry delay is invalid';
    END IF;
    -- A new client may have used its optional hint against a pre-78 schema.
    -- Preserve that exact recorded digest across the migration boundary.
    IF EXISTS (SELECT 1 FROM public.stage_worker_acquire_intents AS intent
        WHERE intent.command_id = (p_command ->> 'command_id')::uuid
          AND intent.request_digest = sha256(convert_to(p_command::text, 'UTF8'))) THEN
        RETURN QUERY SELECT * FROM public.vela_begin_stage_worker_acquire_v77(p_command);
        RETURN;
    END IF;
    IF v_retry IS NOT NULL AND public.vela_stage_worker_acquire_queue_empty(v_payload) THEN
        RETURN QUERY SELECT clock_timestamp(), 'NO_WORK'::text, NULL::bytea, v_retry, NULL::text;
        RETURN;
    END IF;
    RETURN QUERY SELECT * FROM public.vela_begin_stage_worker_acquire_v77(v_payload);
END
$$;
ALTER FUNCTION vela_begin_stage_worker_acquire(jsonb) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_begin_stage_worker_acquire(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_begin_stage_worker_acquire(jsonb)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP FUNCTION vela_begin_stage_worker_acquire(jsonb);
ALTER FUNCTION vela_begin_stage_worker_acquire_v77(jsonb)
    RENAME TO vela_begin_stage_worker_acquire;
GRANT EXECUTE ON FUNCTION vela_begin_stage_worker_acquire(jsonb) TO vela_stage_worker_control;
DROP FUNCTION vela_stage_worker_acquire_queue_empty(jsonb);
DROP FUNCTION vela_read_stage_worker_acquire_authority(uuid);
ALTER FUNCTION vela_read_stage_worker_acquire_authority_v77(uuid)
    RENAME TO vela_read_stage_worker_acquire_authority;
GRANT EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    TO vela_stage_worker_control;
DROP FUNCTION vela_stage_worker_acquire_intent(public.stage_worker_acquire_intents);
DROP FUNCTION vela_stage_worker_acquire_intent_barrier(public.stage_worker_acquire_intents);
DROP FUNCTION vela_stage_worker_acquire_intent_base(public.stage_worker_acquire_intents);
REVOKE SELECT ON stage_ready_queue_entries FROM vela_attempt_coordinator_owner;
-- +goose StatementEnd
