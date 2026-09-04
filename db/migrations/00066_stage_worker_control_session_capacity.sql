-- +goose Up
-- +goose StatementBegin
-- Once the Stage Worker publishes capacity, the Node Agent remains the
-- topology/health authority but no longer writes capacity for that
-- WorkerInstance epoch. Rewrite the legacy combined observation entrypoint so
-- its structural evidence can continue without competing in the Stage Worker
-- sequence range.
ALTER TABLE public.capacity_observations
    ADD COLUMN stage_worker_control_session_epoch bigint,
    ADD COLUMN stage_worker_last_observed_at timestamptz,
    ADD CONSTRAINT capacity_observations_stage_worker_lease_valid CHECK (
        (
            stage_worker_control_session_epoch IS NULL
            AND stage_worker_last_observed_at IS NULL
        ) OR (
            stage_worker_control_session_epoch > 0
            AND stage_worker_last_observed_at >= observed_at
            AND stage_worker_last_observed_at < expires_at
        )
    );

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_observe_worker_instance(jsonb)'::regprocedure
    );
    v_old := E'    IF EXISTS (\n'
        || E'        SELECT 1 FROM public.capacity_observations AS observation\n'
        || E'        WHERE observation.worker_instance_id = v_worker.id\n'
        || E'          AND observation.observation_sequence >= (v_capacity ->> ''sequence'')::bigint\n'
        || E'    ) THEN\n'
        || E'        RAISE EXCEPTION USING\n'
        || E'            ERRCODE = ''55000'',\n'
        || E'            CONSTRAINT = ''capacity_observation_sequence_stale'',\n'
        || E'            MESSAGE = ''CapacityObservation sequence must increase monotonically'';\n'
        || E'    END IF;\n'
        || E'    INSERT INTO public.capacity_observations (\n'
        || E'        worker_instance_id, worker_instance_epoch, observation_sequence,\n'
        || E'        capacity_vector, observed_at, expires_at, observed_by\n'
        || E'    ) VALUES (\n'
        || E'        v_worker.id, v_worker.instance_epoch, (v_capacity ->> ''sequence'')::bigint,\n'
        || E'        v_capacity -> ''vector'', (v_capacity ->> ''observed_at'')::timestamptz,\n'
        || E'        (v_capacity ->> ''expires_at'')::timestamptz, v_observed_by\n'
        || E'    );\n';
    v_new := E'    IF NOT EXISTS (\n'
        || E'        SELECT 1 FROM public.capacity_observations AS observation\n'
        || E'        WHERE observation.worker_instance_id = v_worker.id\n'
        || E'          AND observation.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'          AND observation.observation_sequence::numeric >\n'
        || E'              v_worker.instance_epoch::numeric * 4294967296\n'
        || E'          AND observation.observation_sequence::numeric <=\n'
        || E'              v_worker.instance_epoch::numeric * 4294967296 + 4294967295\n'
        || E'    ) THEN\n'
        || E'        IF EXISTS (\n'
        || E'            SELECT 1 FROM public.capacity_observations AS observation\n'
        || E'            WHERE observation.worker_instance_id = v_worker.id\n'
        || E'              AND observation.observation_sequence >= (v_capacity ->> ''sequence'')::bigint\n'
        || E'        ) THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''55000'',\n'
        || E'                CONSTRAINT = ''capacity_observation_sequence_stale'',\n'
        || E'                MESSAGE = ''CapacityObservation sequence must increase monotonically'';\n'
        || E'        END IF;\n'
        || E'        INSERT INTO public.capacity_observations (\n'
        || E'            worker_instance_id, worker_instance_epoch, observation_sequence,\n'
        || E'            capacity_vector, observed_at, expires_at, observed_by\n'
        || E'        ) VALUES (\n'
        || E'            v_worker.id, v_worker.instance_epoch, (v_capacity ->> ''sequence'')::bigint,\n'
        || E'            v_capacity -> ''vector'', (v_capacity ->> ''observed_at'')::timestamptz,\n'
        || E'            (v_capacity ->> ''expires_at'')::timestamptz, v_observed_by\n'
        || E'        );\n'
        || E'    END IF;\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_capacity_takeover_dependency_drift',
            MESSAGE = 'WorkerInstance capacity observation entrypoint changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

-- A Stage Worker control reconnect advances the WorkerInstance-wide stream
-- epoch. After Stage capacity takeover, a Node Agent with a lower startup epoch
-- receives the durable epoch without mutating topology, health, membership, or
-- reachability. Its next observation must carry that exact durable epoch.
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_observe_worker_instance(jsonb)'::regprocedure
    );
    v_old := E'    IF v_worker.lifecycle_state IN (''FENCED'', ''RETIRED'')\n'
        || E'       OR v_worker.instance_epoch <> (p_evidence ->> ''instance_epoch'')::bigint\n'
        || E'       OR v_worker.control_session_epoch <> (p_evidence ->> ''control_session_epoch'')::bigint THEN\n';
    v_new := E'    IF v_worker.lifecycle_state NOT IN (''FENCED'', ''RETIRED'')\n'
        || E'       AND v_worker.instance_epoch = (p_evidence ->> ''instance_epoch'')::bigint\n'
        || E'       AND (p_evidence ->> ''control_session_epoch'')::bigint <\n'
        || E'           v_worker.control_session_epoch\n'
        || E'       AND EXISTS (\n'
        || E'           SELECT 1\n'
        || E'           FROM public.capacity_observations AS observation\n'
        || E'           WHERE observation.worker_instance_id = v_worker.id\n'
        || E'             AND observation.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'             AND observation.observation_sequence::numeric >\n'
        || E'                 v_worker.instance_epoch::numeric * 4294967296\n'
        || E'             AND observation.observation_sequence::numeric <=\n'
        || E'                 v_worker.instance_epoch::numeric * 4294967296 + 4294967295\n'
        || E'       ) THEN\n'
        || E'        SELECT max(residency.model_runtime_epoch)\n'
        || E'          INTO v_model_runtime_epoch\n'
        || E'        FROM public.model_residencies AS residency\n'
        || E'        WHERE residency.worker_instance_id = v_worker.id\n'
        || E'          AND residency.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'          AND residency.state IN (''READY'', ''WARMING'');\n'
        || E'        IF v_model_runtime_epoch IS NULL THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''23514'',\n'
        || E'                CONSTRAINT = ''worker_instance_model_residency_required'',\n'
        || E'                MESSAGE = ''WorkerInstance has no durable ModelRuntime'';\n'
        || E'        END IF;\n'
        || E'        RETURN QUERY SELECT\n'
        || E'            v_worker.id, v_worker.instance_epoch,\n'
        || E'            v_worker.control_session_epoch, v_model_runtime_epoch,\n'
        || E'            v_worker.lifecycle_state;\n'
        || E'        RETURN;\n'
        || E'    END IF;\n'
        || E'    IF v_worker.lifecycle_state IN (''FENCED'', ''RETIRED'')\n'
        || E'       OR v_worker.instance_epoch <> (p_evidence ->> ''instance_epoch'')::bigint\n'
        || E'       OR v_worker.control_session_epoch <> (p_evidence ->> ''control_session_epoch'')::bigint THEN\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_node_epoch_sync_dependency_drift',
            MESSAGE = 'WorkerInstance epoch validation entrypoint changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

-- Once Stage Worker capacity exists, the Node Agent still refreshes device,
-- member, topology, and health evidence. ModelRuntime epochs are then owned by
-- Stage Worker registration, so stale Node Agent residency templates must not
-- overwrite or reject the durable epoch.
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_observe_worker_instance(jsonb)'::regprocedure
    );
    v_old := E'    FOR v_residency IN SELECT value FROM jsonb_array_elements(p_evidence -> ''residencies'') LOOP\n';
    v_new := E'    IF NOT EXISTS (\n'
        || E'        SELECT 1 FROM public.capacity_observations AS observation\n'
        || E'        WHERE observation.worker_instance_id = v_worker.id\n'
        || E'          AND observation.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'          AND observation.observation_sequence::numeric >\n'
        || E'              v_worker.instance_epoch::numeric * 4294967296\n'
        || E'          AND observation.observation_sequence::numeric <=\n'
        || E'              v_worker.instance_epoch::numeric * 4294967296 + 4294967295\n'
        || E'    ) THEN\n'
        || E'        FOR v_residency IN SELECT value FROM jsonb_array_elements(p_evidence -> ''residencies'') LOOP\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_residency_takeover_dependency_drift',
            MESSAGE = 'WorkerInstance residency observation entrypoint changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);

    v_old := E'    END LOOP;\n'
        || E'    IF jsonb_array_length(p_evidence -> ''residencies'') = 0 THEN\n'
        || E'        RAISE EXCEPTION USING\n'
        || E'            ERRCODE = ''23514'',\n'
        || E'            CONSTRAINT = ''worker_instance_model_residency_required'',\n'
        || E'            MESSAGE = ''WorkerInstance has no resident ModelRuntime'';\n'
        || E'    END IF;\n\n'
        || E'    v_capacity := p_evidence -> ''capacity'';\n';
    v_new := E'        END LOOP;\n'
        || E'        IF jsonb_array_length(p_evidence -> ''residencies'') = 0 THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''23514'',\n'
        || E'                CONSTRAINT = ''worker_instance_model_residency_required'',\n'
        || E'                MESSAGE = ''WorkerInstance has no resident ModelRuntime'';\n'
        || E'        END IF;\n'
        || E'    ELSE\n'
        || E'        SELECT max(residency.model_runtime_epoch)\n'
        || E'          INTO v_model_runtime_epoch\n'
        || E'        FROM public.model_residencies AS residency\n'
        || E'        WHERE residency.worker_instance_id = v_worker.id\n'
        || E'          AND residency.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'          AND residency.state IN (''READY'', ''WARMING'');\n'
        || E'        IF v_model_runtime_epoch IS NULL THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''23514'',\n'
        || E'                CONSTRAINT = ''worker_instance_model_residency_required'',\n'
        || E'                MESSAGE = ''WorkerInstance has no durable ModelRuntime'';\n'
        || E'        END IF;\n'
        || E'    END IF;\n\n'
        || E'    v_capacity := p_evidence -> ''capacity'';\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_residency_takeover_dependency_drift',
            MESSAGE = 'WorkerInstance residency observation tail changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

CREATE FUNCTION vela_reconnect_stage_worker_control(p_evidence jsonb)
RETURNS TABLE (
    instance_epoch bigint,
    control_session_epoch bigint,
    model_runtime_epoch bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker public.worker_instances%ROWTYPE;
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_control_session_epoch bigint;
    v_spiffe_id_digest bytea;
    v_member_id uuid;
    v_leader_member_id uuid;
    v_model_runtime_epoch bigint;
    v_control_session_id text;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_evidence IS NULL OR jsonb_typeof(p_evidence) <> 'object'
       OR (p_evidence ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_control_reconnect_invalid',
            MESSAGE = 'Stage Worker control reconnect evidence is invalid';
    END IF;
    v_worker_instance_id := (p_evidence ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_evidence ->> 'worker_instance_epoch')::bigint;
    v_control_session_epoch := (p_evidence ->> 'control_session_epoch')::bigint;
    v_spiffe_id_digest := decode(p_evidence ->> 'spiffe_id_digest', 'hex');
    IF v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR v_control_session_epoch <= 0
       OR octet_length(v_spiffe_id_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_control_reconnect_invalid',
            MESSAGE = 'Stage Worker control reconnect fields are invalid';
    END IF;

    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = v_worker_instance_id
    FOR UPDATE;
    IF NOT FOUND OR v_worker.instance_epoch <> v_worker_instance_epoch
       OR v_worker.lifecycle_state <> 'READY' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_epoch_conflict',
            MESSAGE = 'Stage Worker reconnect uses stale execution identity';
    END IF;

    SELECT min(member.id::text)::uuid INTO v_member_id
    FROM public.worker_members AS member
    WHERE member.worker_instance_id = v_worker.id
      AND member.worker_instance_epoch = v_worker.instance_epoch
      AND member.identity_digest = v_spiffe_id_digest
      AND member.readiness = 'READY'
    HAVING count(*) = 1;
    IF v_member_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '28000',
            CONSTRAINT = 'stage_worker_control_identity_conflict',
            MESSAGE = 'Stage Worker mTLS identity does not match one READY WorkerMember';
    END IF;

    SELECT min(member.id::text)::uuid INTO v_leader_member_id
    FROM public.worker_members AS member
    WHERE member.worker_instance_id = v_worker.id
      AND member.worker_instance_epoch = v_worker.instance_epoch
      AND member.readiness = 'READY';

    SELECT max(residency.model_runtime_epoch) INTO v_model_runtime_epoch
    FROM public.model_residencies AS residency
    WHERE residency.worker_instance_id = v_worker.id
      AND residency.worker_instance_epoch = v_worker.instance_epoch
      AND residency.state IN ('READY', 'WARMING');
    IF v_model_runtime_epoch IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_model_residency_required',
            MESSAGE = 'Stage Worker reconnect requires a READY or WARMING ModelRuntime';
    END IF;

    v_control_session_id := 'stage-worker-control/' || v_worker.id::text || '/'
        || v_control_session_epoch::text;
    IF v_worker.control_session_epoch = v_control_session_epoch
       AND v_worker.control_session_id = v_control_session_id THEN
        RETURN QUERY SELECT
            v_worker.instance_epoch, v_worker.control_session_epoch,
            v_model_runtime_epoch;
        RETURN;
    END IF;
    IF v_control_session_epoch < v_worker.control_session_epoch
       OR v_member_id <> v_leader_member_id THEN
        RETURN QUERY SELECT
            v_worker.instance_epoch, v_worker.control_session_epoch,
            v_model_runtime_epoch;
        RETURN;
    END IF;
    IF v_worker.control_session_epoch = 9223372036854775807 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_control_session_exhausted',
            MESSAGE = 'Stage Worker control session epoch is exhausted';
    END IF;

    -- PostgreSQL, not the member, signs the next epoch. A stale or corrupt
    -- local high-water can therefore recover in one synchronization round
    -- without permitting an arbitrary client-selected jump.
    v_control_session_epoch := v_worker.control_session_epoch + 1;
    v_control_session_id := 'stage-worker-control/' || v_worker.id::text || '/'
        || v_control_session_epoch::text;

    UPDATE public.worker_instances AS worker
    SET control_session_epoch = v_control_session_epoch,
        control_session_id = v_control_session_id,
        reachability_state = 'CONNECTED',
        observed_at = GREATEST(worker.observed_at, v_now),
        observed_by = 'stage-worker-control/' || v_member_id::text
    WHERE worker.id = v_worker.id;

    RETURN QUERY SELECT
        v_worker.instance_epoch, v_control_session_epoch, v_model_runtime_epoch;
END
$$;
ALTER FUNCTION vela_reconnect_stage_worker_control(jsonb)
    OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION vela_reconnect_stage_worker_control(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_reconnect_stage_worker_control(jsonb)
    TO vela_attempt_coordinator_owner;

CREATE FUNCTION vela_lock_stage_worker_control_session(
    p_worker_instance_id uuid,
    p_worker_instance_epoch bigint,
    p_control_session_epoch bigint
)
RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_matches boolean;
BEGIN
    SELECT worker.instance_epoch = p_worker_instance_epoch
           AND worker.control_session_epoch = p_control_session_epoch
      INTO v_matches
    FROM public.worker_instances AS worker
    WHERE worker.id = p_worker_instance_id
    FOR UPDATE;
    RETURN COALESCE(v_matches, false);
END
$$;
ALTER FUNCTION vela_lock_stage_worker_control_session(uuid, bigint, bigint)
    OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION vela_lock_stage_worker_control_session(uuid, bigint, bigint)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_lock_stage_worker_control_session(uuid, bigint, bigint)
    TO vela_attempt_coordinator_owner;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_register_stage_worker_runtime(jsonb)'::regprocedure
    );
    v_old := E'    v_now timestamptz := clock_timestamp();\nBEGIN\n';
    v_new := E'    v_now timestamptz := clock_timestamp();\n'
        || E'    v_requested_worker_id uuid;\n'
        || E'    v_requested_worker_epoch bigint;\n'
        || E'    v_requested_control_epoch bigint;\n'
        || E'    v_capacity_observation_sequence bigint;\n'
        || E'    v_stage_sequence_base numeric;\n'
        || E'    v_session_matches boolean;\n'
        || E'    v_durable_instance_epoch bigint;\n'
        || E'    v_durable_control_session_epoch bigint;\n'
        || E'    v_durable_model_runtime_epoch bigint;\n'
        || E'BEGIN\n'
        || E'    IF COALESCE((p_evidence ->> ''stage_worker_control_reconnect'')::boolean, false) THEN\n'
        || E'        SELECT reconnected.instance_epoch,\n'
        || E'               reconnected.control_session_epoch,\n'
        || E'               reconnected.model_runtime_epoch\n'
        || E'          INTO v_durable_instance_epoch,\n'
        || E'               v_durable_control_session_epoch,\n'
        || E'               v_durable_model_runtime_epoch\n'
        || E'        FROM public.vela_reconnect_stage_worker_control(p_evidence) AS reconnected;\n'
        || E'        RETURN QUERY SELECT\n'
        || E'            (p_evidence ->> ''worker_instance_id'')::uuid,\n'
        || E'            v_durable_instance_epoch,\n'
        || E'            false,\n'
        || E'            ''Stage Worker control session synchronized''::text,\n'
        || E'            v_durable_control_session_epoch,\n'
        || E'            NULL::uuid;\n'
        || E'        RETURN;\n'
        || E'    END IF;\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_registration_dependency_drift',
            MESSAGE = 'ModelRuntime registration declaration changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);

    v_old := E'    PERFORM vela_lock_model_runtime_epoch_gate(\n'
        || E'        (p_evidence ->> ''model_residency_id'')::uuid\n'
        || E'    );\n'
        || E'    SELECT residency.* INTO v_residency\n'
        || E'    FROM model_residencies AS residency\n'
        || E'    WHERE residency.id = (p_evidence ->> ''model_residency_id'')::uuid\n'
        || E'    FOR UPDATE;\n'
        || E'    IF NOT FOUND THEN\n'
        || E'        RETURN QUERY SELECT\n'
        || E'            v_worker_instance_id, v_worker_instance_epoch, false,\n'
        || E'            ''ModelRuntime residency is not durable Fleet authority''::text,\n'
        || E'            NULL::bigint, NULL::uuid;\n'
        || E'        RETURN;\n'
        || E'    END IF;\n';
    v_new := E'    v_requested_worker_id := (p_evidence ->> ''worker_instance_id'')::uuid;\n'
        || E'    v_requested_worker_epoch := (p_evidence ->> ''worker_instance_epoch'')::bigint;\n'
        || E'    v_requested_control_epoch := (p_evidence ->> ''control_session_epoch'')::bigint;\n'
        || E'    v_capacity_observation_sequence :=\n'
        || E'        (p_evidence ->> ''capacity_observation_sequence'')::bigint;\n'
        || E'    v_stage_sequence_base := v_requested_worker_epoch::numeric * 4294967296;\n'
        || E'    IF v_capacity_observation_sequence = 0 OR (\n'
        || E'        v_capacity_observation_sequence::numeric > v_stage_sequence_base\n'
        || E'        AND v_capacity_observation_sequence::numeric <=\n'
        || E'            v_stage_sequence_base + 4294967295\n'
        || E'    ) THEN\n'
        || E'        SELECT public.vela_lock_stage_worker_control_session(\n'
        || E'            v_requested_worker_id,\n'
        || E'            v_requested_worker_epoch,\n'
        || E'            v_requested_control_epoch\n'
        || E'        ) INTO v_session_matches;\n'
        || E'        IF NOT v_session_matches THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''55000'',\n'
        || E'                CONSTRAINT = ''stage_worker_control_session_conflict'',\n'
        || E'                MESSAGE = ''Stage Worker registration uses stale control authority'';\n'
        || E'        END IF;\n'
        || E'    ELSE\n'
        || E'        PERFORM public.vela_lock_stage_worker_control_session(\n'
        || E'            v_requested_worker_id,\n'
        || E'            v_requested_worker_epoch,\n'
        || E'            NULL\n'
        || E'        );\n'
        || E'    END IF;\n'
        || E'    PERFORM vela_lock_model_runtime_epoch_gate(\n'
        || E'        (p_evidence ->> ''model_residency_id'')::uuid\n'
        || E'    );\n'
        || E'    SELECT residency.* INTO v_residency\n'
        || E'    FROM model_residencies AS residency\n'
        || E'    WHERE residency.id = (p_evidence ->> ''model_residency_id'')::uuid\n'
        || E'    FOR UPDATE;\n'
        || E'    IF NOT FOUND THEN\n'
        || E'        RETURN QUERY SELECT\n'
        || E'            v_worker_instance_id, v_worker_instance_epoch, false,\n'
        || E'            ''ModelRuntime residency is not durable Fleet authority''::text,\n'
        || E'            NULL::bigint, NULL::uuid;\n'
        || E'        RETURN;\n'
        || E'    END IF;\n'
        || E'    IF v_capacity_observation_sequence = 0 THEN\n'
        || E'        SELECT observation.observation_sequence\n'
        || E'          INTO v_capacity_observation_sequence\n'
        || E'        FROM public.capacity_observations AS observation\n'
        || E'        WHERE observation.worker_instance_id = v_requested_worker_id\n'
        || E'          AND observation.worker_instance_epoch = v_requested_worker_epoch\n'
        || E'          AND observation.observation_sequence::numeric > v_stage_sequence_base\n'
        || E'          AND observation.observation_sequence::numeric <=\n'
        || E'              v_stage_sequence_base + 4294967295\n'
        || E'          AND observation.expires_at > clock_timestamp()\n'
        || E'        ORDER BY observation.observation_sequence DESC\n'
        || E'        LIMIT 1;\n'
        || E'        IF v_capacity_observation_sequence IS NULL THEN\n'
        || E'            RETURN QUERY SELECT\n'
        || E'                v_requested_worker_id, v_requested_worker_epoch, false,\n'
        || E'                ''Stage Worker registration requires current capacity authority''::text,\n'
        || E'                NULL::bigint, NULL::uuid;\n'
        || E'            RETURN;\n'
        || E'        END IF;\n'
        || E'        p_evidence := jsonb_set(\n'
        || E'            p_evidence,\n'
        || E'            ''{capacity_observation_sequence}'',\n'
        || E'            to_jsonb(v_capacity_observation_sequence)\n'
        || E'        );\n'
        || E'    END IF;\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_registration_dependency_drift',
            MESSAGE = 'ModelRuntime registration lock sequence changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

-- Fleet observation and Stage registration both lock WorkerInstance before
-- ModelRuntime residency. Make ASSIGN use the same global order so it cannot
-- hold the residency gate while waiting for the WorkerInstance.
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_apply_stage_command(jsonb)'::regprocedure
    );
    v_old := E'    IF v_kind = ''ASSIGN'' THEN\n'
        || E'        v_residency_id := (p_command ->> ''model_residency_id'')::uuid;\n'
        || E'    ELSIF v_kind IN (''START'', ''COMPLETE'', ''FAIL'') THEN\n'
        || E'        SELECT lease.model_residency_id INTO v_residency_id\n'
        || E'        FROM public.stage_leases AS lease\n'
        || E'        WHERE lease.id = (p_command ->> ''stage_lease_id'')::uuid;\n'
        || E'    END IF;\n'
        || E'    IF v_residency_id IS NOT NULL THEN\n'
        || E'        PERFORM public.vela_lock_model_runtime_epoch_gate(v_residency_id);\n'
        || E'    END IF;\n';
    v_new := E'    IF v_kind = ''ASSIGN'' THEN\n'
        || E'        PERFORM public.vela_lock_stage_worker_control_session(\n'
        || E'            (p_command ->> ''worker_instance_id'')::uuid,\n'
        || E'            (p_command ->> ''worker_instance_epoch'')::bigint,\n'
        || E'            NULL\n'
        || E'        );\n'
        || E'        v_residency_id := (p_command ->> ''model_residency_id'')::uuid;\n'
        || E'    ELSIF v_kind IN (''START'', ''COMPLETE'', ''FAIL'') THEN\n'
        || E'        SELECT lease.model_residency_id INTO v_residency_id\n'
        || E'        FROM public.stage_leases AS lease\n'
        || E'        WHERE lease.id = (p_command ->> ''stage_lease_id'')::uuid;\n'
        || E'    END IF;\n'
        || E'    IF v_residency_id IS NOT NULL THEN\n'
        || E'        PERFORM public.vela_lock_model_runtime_epoch_gate(v_residency_id);\n'
        || E'    END IF;\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_assign_lock_order_dependency_drift',
            MESSAGE = 'Stage ASSIGN runtime epoch lock entrypoint changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

CREATE FUNCTION vela_report_stage_worker_capacity_v66(p_observation jsonb)
RETURNS TABLE (
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    ready boolean,
    reason text,
    capacity_observation_sequence bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker public.worker_instances%ROWTYPE;
    v_profile public.worker_profile_revisions%ROWTYPE;
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_control_session_epoch bigint;
    v_observation_sequence bigint;
    v_capacity_vector jsonb;
    v_observed_at timestamptz;
    v_expires_at timestamptz;
    v_spiffe_id_digest bytea;
    v_member_id uuid;
    v_leader_member_id uuid;
    v_observed_by text;
    v_latest_observation public.capacity_observations%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_observation IS NULL OR jsonb_typeof(p_observation) <> 'object'
       OR (p_observation ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_capacity_report_invalid',
            MESSAGE = 'Stage Worker capacity report is invalid';
    END IF;
    v_worker_instance_id := (p_observation ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_observation ->> 'worker_instance_epoch')::bigint;
    v_control_session_epoch := (p_observation ->> 'control_session_epoch')::bigint;
    v_observation_sequence := (p_observation ->> 'observation_sequence')::bigint;
    v_capacity_vector := p_observation -> 'capacity_vector';
    v_observed_at := (p_observation ->> 'observed_at')::timestamptz;
    v_expires_at := (p_observation ->> 'expires_at')::timestamptz;
    v_spiffe_id_digest := decode(p_observation ->> 'spiffe_id_digest', 'hex');
    IF v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR v_control_session_epoch <= 0 OR v_observation_sequence <= 0
       OR v_observation_sequence::numeric <=
          v_worker_instance_epoch::numeric * 4294967296
       OR v_observation_sequence::numeric >
          v_worker_instance_epoch::numeric * 4294967296 + 4294967295
       OR jsonb_typeof(v_capacity_vector) <> 'object'
       OR v_capacity_vector = '{}'::jsonb
       OR v_observed_at IS NULL OR v_expires_at IS NULL
       OR v_expires_at <= v_observed_at
       OR v_expires_at > v_observed_at + interval '1 hour'
       OR v_observed_at > v_now + interval '30 seconds'
       OR v_expires_at <= v_now
       OR octet_length(v_spiffe_id_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_capacity_report_invalid',
            MESSAGE = 'Stage Worker capacity report fields are invalid';
    END IF;

    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = v_worker_instance_id
    FOR UPDATE;
    IF NOT FOUND OR v_worker.instance_epoch <> v_worker_instance_epoch
       OR v_worker.control_session_epoch <> v_control_session_epoch
       OR v_worker.lifecycle_state <> 'READY'
       OR v_worker.reachability_state <> 'CONNECTED' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_capacity_authority_conflict',
            MESSAGE = 'Stage Worker capacity report uses stale authority';
    END IF;
    SELECT min(member.id::text)::uuid INTO v_member_id
    FROM public.worker_members AS member
    WHERE member.worker_instance_id = v_worker.id
      AND member.worker_instance_epoch = v_worker.instance_epoch
      AND member.identity_digest = v_spiffe_id_digest
      AND member.readiness = 'READY'
    HAVING count(*) = 1;
    IF v_member_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '28000',
            CONSTRAINT = 'stage_worker_control_identity_conflict',
            MESSAGE = 'Stage Worker mTLS identity does not match one READY WorkerMember';
    END IF;
    SELECT min(member.id::text)::uuid INTO v_leader_member_id
    FROM public.worker_members AS member
    WHERE member.worker_instance_id = v_worker.id
      AND member.worker_instance_epoch = v_worker.instance_epoch
      AND member.readiness = 'READY';
    IF v_member_id <> v_leader_member_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'stage_worker_capacity_reporter_conflict',
            MESSAGE = 'Only the leader WorkerMember may report capacity';
    END IF;
    SELECT profile.* INTO STRICT v_profile
    FROM public.worker_profile_revisions AS profile
    WHERE profile.id = v_worker.worker_profile_revision_id
      AND profile.state IN ('CERTIFIED', 'CANARY', 'ACTIVE');
    IF EXISTS (
        SELECT 1
        FROM jsonb_each_text(v_capacity_vector) AS observed(resource_name, quantity)
        LEFT JOIN jsonb_each_text(v_profile.capacity_limits) AS certified(resource_name, quantity)
          USING (resource_name)
        WHERE certified.resource_name IS NULL
           OR observed.quantity::numeric < 0
           OR observed.quantity::numeric > certified.quantity::numeric
           OR trunc(observed.quantity::numeric) <> observed.quantity::numeric
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'capacity_observation_exceeds_worker_profile',
            MESSAGE = 'Stage Worker capacity exceeds certified WorkerProfile limits';
    END IF;

    v_observed_by := 'stage-worker-control/' || v_member_id::text;
    SELECT observation.* INTO v_latest_observation
    FROM public.capacity_observations AS observation
    WHERE observation.worker_instance_id = v_worker.id
      AND observation.worker_instance_epoch = v_worker.instance_epoch
      AND observation.observation_sequence::numeric >
          v_worker.instance_epoch::numeric * 4294967296
      AND observation.observation_sequence::numeric <=
          v_worker.instance_epoch::numeric * 4294967296 + 4294967295
    ORDER BY observation.observation_sequence DESC
    LIMIT 1
    FOR UPDATE;

    IF v_latest_observation.observation_sequence IS NOT NULL
       AND v_observation_sequence < v_latest_observation.observation_sequence THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_capacity_sequence_stale',
            MESSAGE = 'Stage Worker capacity sequence is stale';
    ELSIF v_latest_observation.observation_sequence IS NOT NULL
          AND v_observation_sequence = v_latest_observation.observation_sequence THEN
        IF v_latest_observation.stage_worker_control_session_epoch <>
               v_control_session_epoch
           OR v_latest_observation.capacity_vector <> v_capacity_vector THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_worker_capacity_replay_conflict',
                MESSAGE = 'Stage Worker capacity replay conflicts with durable evidence';
        END IF;
        IF v_observed_at < v_latest_observation.stage_worker_last_observed_at
           OR v_expires_at < v_latest_observation.expires_at THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_worker_capacity_renewal_stale',
                MESSAGE = 'Stage Worker capacity renewal time must not move backward';
        END IF;
        UPDATE public.capacity_observations AS observation
        SET expires_at = v_expires_at,
            stage_worker_last_observed_at = v_observed_at
        WHERE observation.worker_instance_id = v_worker.id
          AND observation.observation_sequence =
              v_latest_observation.observation_sequence;
        UPDATE public.worker_instances AS worker
        SET reachability_state = 'CONNECTED',
            observed_at = GREATEST(worker.observed_at, v_now),
            observed_by = v_observed_by
        WHERE worker.id = v_worker.id;
        RETURN QUERY SELECT
            v_worker.id, v_worker.instance_epoch, true,
            'capacity lease renewed'::text,
            v_latest_observation.observation_sequence;
        RETURN;
    END IF;

    INSERT INTO public.capacity_observations (
        worker_instance_id, worker_instance_epoch, observation_sequence,
        capacity_vector, observed_at, expires_at, observed_by,
        stage_worker_control_session_epoch, stage_worker_last_observed_at
    ) VALUES (
        v_worker.id, v_worker.instance_epoch, v_observation_sequence,
        v_capacity_vector, v_observed_at, v_expires_at, v_observed_by,
        v_control_session_epoch, v_observed_at
    );
    UPDATE public.worker_instances AS worker
    SET reachability_state = 'CONNECTED',
        observed_at = GREATEST(worker.observed_at, v_now),
        observed_by = v_observed_by
    WHERE worker.id = v_worker.id;

    RETURN QUERY SELECT
        v_worker.id, v_worker.instance_epoch, true,
        'capacity observation recorded'::text, v_observation_sequence;
END
$$;
ALTER FUNCTION vela_report_stage_worker_capacity_v66(jsonb)
    OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION vela_report_stage_worker_capacity_v66(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_report_stage_worker_capacity_v66(jsonb)
    TO vela_attempt_coordinator_owner;

CREATE OR REPLACE FUNCTION vela_verify_stage_capacity_observation(p_observation jsonb)
RETURNS TABLE (
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    ready boolean,
    reason text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_control_session_epoch bigint;
    v_observation_sequence bigint;
    v_capacity_vector jsonb;
    v_observed_at timestamptz;
    v_expires_at timestamptz;
    v_spiffe_id_digest bytea;
    v_stage_sequence_base numeric;
    v_ready boolean;
BEGIN
    IF p_observation IS NULL OR jsonb_typeof(p_observation) <> 'object'
       OR (p_observation ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_capacity_observation_invalid',
            MESSAGE = 'Stage Worker capacity observation is invalid';
    END IF;
    v_worker_instance_id := (p_observation ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_observation ->> 'worker_instance_epoch')::bigint;
    v_control_session_epoch := (p_observation ->> 'control_session_epoch')::bigint;
    v_observation_sequence := (p_observation ->> 'observation_sequence')::bigint;
    v_capacity_vector := p_observation -> 'capacity_vector';
    v_observed_at := (p_observation ->> 'observed_at')::timestamptz;
    v_expires_at := (p_observation ->> 'expires_at')::timestamptz;
    v_spiffe_id_digest := decode(p_observation ->> 'spiffe_id_digest', 'hex');
    IF v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR v_control_session_epoch <= 0 OR v_observation_sequence <= 0
       OR jsonb_typeof(v_capacity_vector) <> 'object'
       OR v_observed_at IS NULL OR v_expires_at IS NULL
       OR v_expires_at <= v_observed_at
       OR octet_length(v_spiffe_id_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_capacity_observation_invalid',
            MESSAGE = 'Stage Worker capacity observation fields are invalid';
    END IF;

    v_stage_sequence_base := v_worker_instance_epoch::numeric * 4294967296;
    IF v_observation_sequence::numeric > v_stage_sequence_base
       AND v_observation_sequence::numeric <= v_stage_sequence_base + 4294967295 THEN
        RETURN QUERY
        SELECT reported.worker_instance_id,
               reported.worker_instance_epoch,
               reported.ready,
               jsonb_build_object(
                   'reason', reported.reason,
                   'capacity_observation_sequence',
                   reported.capacity_observation_sequence
               )::text
        FROM public.vela_report_stage_worker_capacity_v66(p_observation) AS reported;
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM worker_instances AS worker
        JOIN capacity_observations AS observation
          ON observation.worker_instance_id = worker.id
         AND observation.worker_instance_epoch = worker.instance_epoch
         AND observation.observation_sequence = v_observation_sequence
        WHERE worker.id = v_worker_instance_id
          AND worker.instance_epoch = v_worker_instance_epoch
          AND worker.control_session_epoch = v_control_session_epoch
          AND worker.lifecycle_state = 'READY'
          AND worker.reachability_state = 'CONNECTED'
          AND observation.capacity_vector = v_capacity_vector
          AND observation.observed_at = v_observed_at
          AND observation.expires_at = v_expires_at
          AND observation.expires_at > clock_timestamp()
          AND EXISTS (
              SELECT 1 FROM worker_members AS member
              WHERE member.worker_instance_id = worker.id
                AND member.worker_instance_epoch = worker.instance_epoch
                AND member.identity_digest = v_spiffe_id_digest
                AND member.readiness = 'READY'
          )
    ) INTO v_ready;

    RETURN QUERY SELECT
        v_worker_instance_id,
        v_worker_instance_epoch,
        v_ready,
        CASE WHEN v_ready THEN 'capacity observation verified'
             ELSE 'capacity observation does not match durable Fleet authority' END;
END
$$;
ALTER FUNCTION vela_verify_stage_capacity_observation(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_verify_stage_capacity_observation(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_verify_stage_capacity_observation(jsonb)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE public.capacity_observations IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.capacity_observations AS observation
        WHERE observation.observation_sequence::numeric >
                  observation.worker_instance_epoch::numeric * 4294967296
          AND observation.observation_sequence::numeric <=
                  observation.worker_instance_epoch::numeric * 4294967296 + 4294967295
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_capacity_rollback_is_unsafe',
            MESSAGE = 'Cannot contract Stage Worker capacity while durable Stage observations exist';
    END IF;
END
$$;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_apply_stage_command(jsonb)'::regprocedure
    );
    v_old := E'    IF v_kind = ''ASSIGN'' THEN\n'
        || E'        PERFORM public.vela_lock_stage_worker_control_session(\n'
        || E'            (p_command ->> ''worker_instance_id'')::uuid,\n'
        || E'            (p_command ->> ''worker_instance_epoch'')::bigint,\n'
        || E'            NULL\n'
        || E'        );\n'
        || E'        v_residency_id := (p_command ->> ''model_residency_id'')::uuid;\n'
        || E'    ELSIF v_kind IN (''START'', ''COMPLETE'', ''FAIL'') THEN\n'
        || E'        SELECT lease.model_residency_id INTO v_residency_id\n'
        || E'        FROM public.stage_leases AS lease\n'
        || E'        WHERE lease.id = (p_command ->> ''stage_lease_id'')::uuid;\n'
        || E'    END IF;\n'
        || E'    IF v_residency_id IS NOT NULL THEN\n'
        || E'        PERFORM public.vela_lock_model_runtime_epoch_gate(v_residency_id);\n'
        || E'    END IF;\n';
    v_new := E'    IF v_kind = ''ASSIGN'' THEN\n'
        || E'        v_residency_id := (p_command ->> ''model_residency_id'')::uuid;\n'
        || E'    ELSIF v_kind IN (''START'', ''COMPLETE'', ''FAIL'') THEN\n'
        || E'        SELECT lease.model_residency_id INTO v_residency_id\n'
        || E'        FROM public.stage_leases AS lease\n'
        || E'        WHERE lease.id = (p_command ->> ''stage_lease_id'')::uuid;\n'
        || E'    END IF;\n'
        || E'    IF v_residency_id IS NOT NULL THEN\n'
        || E'        PERFORM public.vela_lock_model_runtime_epoch_gate(v_residency_id);\n'
        || E'    END IF;\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_assign_lock_order_rollback_drift',
            MESSAGE = 'Stage ASSIGN runtime epoch rollback entrypoint changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

CREATE OR REPLACE FUNCTION vela_verify_stage_capacity_observation(p_observation jsonb)
RETURNS TABLE (
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    ready boolean,
    reason text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_control_session_epoch bigint;
    v_observation_sequence bigint;
    v_capacity_vector jsonb;
    v_observed_at timestamptz;
    v_expires_at timestamptz;
    v_spiffe_id_digest bytea;
    v_ready boolean;
BEGIN
    IF p_observation IS NULL OR jsonb_typeof(p_observation) <> 'object'
       OR (p_observation ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_capacity_observation_invalid',
            MESSAGE = 'Stage Worker capacity observation is invalid';
    END IF;
    v_worker_instance_id := (p_observation ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_observation ->> 'worker_instance_epoch')::bigint;
    v_control_session_epoch := (p_observation ->> 'control_session_epoch')::bigint;
    v_observation_sequence := (p_observation ->> 'observation_sequence')::bigint;
    v_capacity_vector := p_observation -> 'capacity_vector';
    v_observed_at := (p_observation ->> 'observed_at')::timestamptz;
    v_expires_at := (p_observation ->> 'expires_at')::timestamptz;
    v_spiffe_id_digest := decode(p_observation ->> 'spiffe_id_digest', 'hex');

    IF v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR v_control_session_epoch <= 0 OR v_observation_sequence <= 0
       OR jsonb_typeof(v_capacity_vector) <> 'object'
       OR v_observed_at IS NULL OR v_expires_at IS NULL
       OR v_expires_at <= v_observed_at
       OR octet_length(v_spiffe_id_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_capacity_observation_invalid',
            MESSAGE = 'Stage Worker capacity observation fields are invalid';
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM worker_instances AS worker
        JOIN capacity_observations AS observation
          ON observation.worker_instance_id = worker.id
         AND observation.worker_instance_epoch = worker.instance_epoch
         AND observation.observation_sequence = v_observation_sequence
        WHERE worker.id = v_worker_instance_id
          AND worker.instance_epoch = v_worker_instance_epoch
          AND worker.control_session_epoch = v_control_session_epoch
          AND worker.lifecycle_state = 'READY'
          AND worker.reachability_state = 'CONNECTED'
          AND observation.capacity_vector = v_capacity_vector
          AND observation.observed_at = v_observed_at
          AND observation.expires_at = v_expires_at
          AND observation.expires_at > clock_timestamp()
          AND EXISTS (
              SELECT 1 FROM worker_members AS member
              WHERE member.worker_instance_id = worker.id
                AND member.worker_instance_epoch = worker.instance_epoch
                AND member.identity_digest = v_spiffe_id_digest
                AND member.readiness = 'READY'
          )
    ) INTO v_ready;

    RETURN QUERY SELECT
        v_worker_instance_id,
        v_worker_instance_epoch,
        v_ready,
        CASE WHEN v_ready THEN 'capacity observation verified'
             ELSE 'capacity observation does not match durable Fleet authority' END;
END
$$;
REVOKE EXECUTE ON FUNCTION vela_report_stage_worker_capacity_v66(jsonb)
    FROM vela_attempt_coordinator_owner;
DROP FUNCTION vela_report_stage_worker_capacity_v66(jsonb);
ALTER TABLE public.capacity_observations
    DROP CONSTRAINT capacity_observations_stage_worker_lease_valid,
    DROP COLUMN stage_worker_last_observed_at,
    DROP COLUMN stage_worker_control_session_epoch;

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_register_stage_worker_runtime(jsonb)'::regprocedure
    );
    v_old := E'    v_requested_worker_id := (p_evidence ->> ''worker_instance_id'')::uuid;\n'
        || E'    v_requested_worker_epoch := (p_evidence ->> ''worker_instance_epoch'')::bigint;\n'
        || E'    v_requested_control_epoch := (p_evidence ->> ''control_session_epoch'')::bigint;\n'
        || E'    v_capacity_observation_sequence :=\n'
        || E'        (p_evidence ->> ''capacity_observation_sequence'')::bigint;\n'
        || E'    v_stage_sequence_base := v_requested_worker_epoch::numeric * 4294967296;\n'
        || E'    IF v_capacity_observation_sequence = 0 OR (\n'
        || E'        v_capacity_observation_sequence::numeric > v_stage_sequence_base\n'
        || E'        AND v_capacity_observation_sequence::numeric <=\n'
        || E'            v_stage_sequence_base + 4294967295\n'
        || E'    ) THEN\n'
        || E'        SELECT public.vela_lock_stage_worker_control_session(\n'
        || E'            v_requested_worker_id,\n'
        || E'            v_requested_worker_epoch,\n'
        || E'            v_requested_control_epoch\n'
        || E'        ) INTO v_session_matches;\n'
        || E'        IF NOT v_session_matches THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''55000'',\n'
        || E'                CONSTRAINT = ''stage_worker_control_session_conflict'',\n'
        || E'                MESSAGE = ''Stage Worker registration uses stale control authority'';\n'
        || E'        END IF;\n'
        || E'    ELSE\n'
        || E'        PERFORM public.vela_lock_stage_worker_control_session(\n'
        || E'            v_requested_worker_id,\n'
        || E'            v_requested_worker_epoch,\n'
        || E'            NULL\n'
        || E'        );\n'
        || E'    END IF;\n'
        || E'    PERFORM vela_lock_model_runtime_epoch_gate(\n'
        || E'        (p_evidence ->> ''model_residency_id'')::uuid\n'
        || E'    );\n'
        || E'    SELECT residency.* INTO v_residency\n'
        || E'    FROM model_residencies AS residency\n'
        || E'    WHERE residency.id = (p_evidence ->> ''model_residency_id'')::uuid\n'
        || E'    FOR UPDATE;\n'
        || E'    IF NOT FOUND THEN\n'
        || E'        RETURN QUERY SELECT\n'
        || E'            v_worker_instance_id, v_worker_instance_epoch, false,\n'
        || E'            ''ModelRuntime residency is not durable Fleet authority''::text,\n'
        || E'            NULL::bigint, NULL::uuid;\n'
        || E'        RETURN;\n'
        || E'    END IF;\n'
        || E'    IF v_capacity_observation_sequence = 0 THEN\n'
        || E'        SELECT observation.observation_sequence\n'
        || E'          INTO v_capacity_observation_sequence\n'
        || E'        FROM public.capacity_observations AS observation\n'
        || E'        WHERE observation.worker_instance_id = v_requested_worker_id\n'
        || E'          AND observation.worker_instance_epoch = v_requested_worker_epoch\n'
        || E'          AND observation.observation_sequence::numeric > v_stage_sequence_base\n'
        || E'          AND observation.observation_sequence::numeric <=\n'
        || E'              v_stage_sequence_base + 4294967295\n'
        || E'          AND observation.expires_at > clock_timestamp()\n'
        || E'        ORDER BY observation.observation_sequence DESC\n'
        || E'        LIMIT 1;\n'
        || E'        IF v_capacity_observation_sequence IS NULL THEN\n'
        || E'            RETURN QUERY SELECT\n'
        || E'                v_requested_worker_id, v_requested_worker_epoch, false,\n'
        || E'                ''Stage Worker registration requires current capacity authority''::text,\n'
        || E'                NULL::bigint, NULL::uuid;\n'
        || E'            RETURN;\n'
        || E'        END IF;\n'
        || E'        p_evidence := jsonb_set(\n'
        || E'            p_evidence,\n'
        || E'            ''{capacity_observation_sequence}'',\n'
        || E'            to_jsonb(v_capacity_observation_sequence)\n'
        || E'        );\n'
        || E'    END IF;\n';
    v_new := E'    PERFORM vela_lock_model_runtime_epoch_gate(\n'
        || E'        (p_evidence ->> ''model_residency_id'')::uuid\n'
        || E'    );\n'
        || E'    SELECT residency.* INTO v_residency\n'
        || E'    FROM model_residencies AS residency\n'
        || E'    WHERE residency.id = (p_evidence ->> ''model_residency_id'')::uuid\n'
        || E'    FOR UPDATE;\n'
        || E'    IF NOT FOUND THEN\n'
        || E'        RETURN QUERY SELECT\n'
        || E'            v_worker_instance_id, v_worker_instance_epoch, false,\n'
        || E'            ''ModelRuntime residency is not durable Fleet authority''::text,\n'
        || E'            NULL::bigint, NULL::uuid;\n'
        || E'        RETURN;\n'
        || E'    END IF;\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_registration_rollback_drift',
            MESSAGE = 'ModelRuntime registration rollback lock sequence changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);

    v_old := E'    v_now timestamptz := clock_timestamp();\n'
        || E'    v_requested_worker_id uuid;\n'
        || E'    v_requested_worker_epoch bigint;\n'
        || E'    v_requested_control_epoch bigint;\n'
        || E'    v_capacity_observation_sequence bigint;\n'
        || E'    v_stage_sequence_base numeric;\n'
        || E'    v_session_matches boolean;\n'
        || E'    v_durable_instance_epoch bigint;\n'
        || E'    v_durable_control_session_epoch bigint;\n'
        || E'    v_durable_model_runtime_epoch bigint;\n'
        || E'BEGIN\n'
        || E'    IF COALESCE((p_evidence ->> ''stage_worker_control_reconnect'')::boolean, false) THEN\n'
        || E'        SELECT reconnected.instance_epoch,\n'
        || E'               reconnected.control_session_epoch,\n'
        || E'               reconnected.model_runtime_epoch\n'
        || E'          INTO v_durable_instance_epoch,\n'
        || E'               v_durable_control_session_epoch,\n'
        || E'               v_durable_model_runtime_epoch\n'
        || E'        FROM public.vela_reconnect_stage_worker_control(p_evidence) AS reconnected;\n'
        || E'        RETURN QUERY SELECT\n'
        || E'            (p_evidence ->> ''worker_instance_id'')::uuid,\n'
        || E'            v_durable_instance_epoch,\n'
        || E'            false,\n'
        || E'            ''Stage Worker control session synchronized''::text,\n'
        || E'            v_durable_control_session_epoch,\n'
        || E'            NULL::uuid;\n'
        || E'        RETURN;\n'
        || E'    END IF;\n';
    v_new := E'    v_now timestamptz := clock_timestamp();\nBEGIN\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_registration_rollback_drift',
            MESSAGE = 'ModelRuntime registration rollback declaration changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
REVOKE EXECUTE ON FUNCTION vela_lock_stage_worker_control_session(uuid, bigint, bigint)
    FROM vela_attempt_coordinator_owner;
DROP FUNCTION vela_lock_stage_worker_control_session(uuid, bigint, bigint);
REVOKE EXECUTE ON FUNCTION vela_reconnect_stage_worker_control(jsonb)
    FROM vela_attempt_coordinator_owner;
DROP FUNCTION vela_reconnect_stage_worker_control(jsonb);

DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_observe_worker_instance(jsonb)'::regprocedure
    );
    v_old := E'    IF v_worker.lifecycle_state NOT IN (''FENCED'', ''RETIRED'')\n'
        || E'       AND v_worker.instance_epoch = (p_evidence ->> ''instance_epoch'')::bigint\n'
        || E'       AND (p_evidence ->> ''control_session_epoch'')::bigint <\n'
        || E'           v_worker.control_session_epoch\n'
        || E'       AND EXISTS (\n'
        || E'           SELECT 1\n'
        || E'           FROM public.capacity_observations AS observation\n'
        || E'           WHERE observation.worker_instance_id = v_worker.id\n'
        || E'             AND observation.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'             AND observation.observation_sequence::numeric >\n'
        || E'                 v_worker.instance_epoch::numeric * 4294967296\n'
        || E'             AND observation.observation_sequence::numeric <=\n'
        || E'                 v_worker.instance_epoch::numeric * 4294967296 + 4294967295\n'
        || E'       ) THEN\n'
        || E'        SELECT max(residency.model_runtime_epoch)\n'
        || E'          INTO v_model_runtime_epoch\n'
        || E'        FROM public.model_residencies AS residency\n'
        || E'        WHERE residency.worker_instance_id = v_worker.id\n'
        || E'          AND residency.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'          AND residency.state IN (''READY'', ''WARMING'');\n'
        || E'        IF v_model_runtime_epoch IS NULL THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''23514'',\n'
        || E'                CONSTRAINT = ''worker_instance_model_residency_required'',\n'
        || E'                MESSAGE = ''WorkerInstance has no durable ModelRuntime'';\n'
        || E'        END IF;\n'
        || E'        RETURN QUERY SELECT\n'
        || E'            v_worker.id, v_worker.instance_epoch,\n'
        || E'            v_worker.control_session_epoch, v_model_runtime_epoch,\n'
        || E'            v_worker.lifecycle_state;\n'
        || E'        RETURN;\n'
        || E'    END IF;\n'
        || E'    IF v_worker.lifecycle_state IN (''FENCED'', ''RETIRED'')\n'
        || E'       OR v_worker.instance_epoch <> (p_evidence ->> ''instance_epoch'')::bigint\n'
        || E'       OR v_worker.control_session_epoch <> (p_evidence ->> ''control_session_epoch'')::bigint THEN\n';
    v_new := E'    IF v_worker.lifecycle_state IN (''FENCED'', ''RETIRED'')\n'
        || E'       OR v_worker.instance_epoch <> (p_evidence ->> ''instance_epoch'')::bigint\n'
        || E'       OR v_worker.control_session_epoch <> (p_evidence ->> ''control_session_epoch'')::bigint THEN\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_node_epoch_sync_rollback_drift',
            MESSAGE = 'WorkerInstance epoch validation rollback entrypoint changed';
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
        'public.vela_observe_worker_instance(jsonb)'::regprocedure
    );
    v_old := E'    IF NOT EXISTS (\n'
        || E'        SELECT 1 FROM public.capacity_observations AS observation\n'
        || E'        WHERE observation.worker_instance_id = v_worker.id\n'
        || E'          AND observation.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'          AND observation.observation_sequence::numeric >\n'
        || E'              v_worker.instance_epoch::numeric * 4294967296\n'
        || E'          AND observation.observation_sequence::numeric <=\n'
        || E'              v_worker.instance_epoch::numeric * 4294967296 + 4294967295\n'
        || E'    ) THEN\n'
        || E'        IF EXISTS (\n'
        || E'            SELECT 1 FROM public.capacity_observations AS observation\n'
        || E'            WHERE observation.worker_instance_id = v_worker.id\n'
        || E'              AND observation.observation_sequence >= (v_capacity ->> ''sequence'')::bigint\n'
        || E'        ) THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''55000'',\n'
        || E'                CONSTRAINT = ''capacity_observation_sequence_stale'',\n'
        || E'                MESSAGE = ''CapacityObservation sequence must increase monotonically'';\n'
        || E'        END IF;\n'
        || E'        INSERT INTO public.capacity_observations (\n'
        || E'            worker_instance_id, worker_instance_epoch, observation_sequence,\n'
        || E'            capacity_vector, observed_at, expires_at, observed_by\n'
        || E'        ) VALUES (\n'
        || E'            v_worker.id, v_worker.instance_epoch, (v_capacity ->> ''sequence'')::bigint,\n'
        || E'            v_capacity -> ''vector'', (v_capacity ->> ''observed_at'')::timestamptz,\n'
        || E'            (v_capacity ->> ''expires_at'')::timestamptz, v_observed_by\n'
        || E'        );\n'
        || E'    END IF;\n';
    v_new := E'    IF EXISTS (\n'
        || E'        SELECT 1 FROM public.capacity_observations AS observation\n'
        || E'        WHERE observation.worker_instance_id = v_worker.id\n'
        || E'          AND observation.observation_sequence >= (v_capacity ->> ''sequence'')::bigint\n'
        || E'    ) THEN\n'
        || E'        RAISE EXCEPTION USING\n'
        || E'            ERRCODE = ''55000'',\n'
        || E'            CONSTRAINT = ''capacity_observation_sequence_stale'',\n'
        || E'            MESSAGE = ''CapacityObservation sequence must increase monotonically'';\n'
        || E'    END IF;\n'
        || E'    INSERT INTO public.capacity_observations (\n'
        || E'        worker_instance_id, worker_instance_epoch, observation_sequence,\n'
        || E'        capacity_vector, observed_at, expires_at, observed_by\n'
        || E'    ) VALUES (\n'
        || E'        v_worker.id, v_worker.instance_epoch, (v_capacity ->> ''sequence'')::bigint,\n'
        || E'        v_capacity -> ''vector'', (v_capacity ->> ''observed_at'')::timestamptz,\n'
        || E'        (v_capacity ->> ''expires_at'')::timestamptz, v_observed_by\n'
        || E'    );\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_capacity_takeover_rollback_drift',
            MESSAGE = 'WorkerInstance capacity observation rollback changed';
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
        'public.vela_observe_worker_instance(jsonb)'::regprocedure
    );
    v_old := E'    IF NOT EXISTS (\n'
        || E'        SELECT 1 FROM public.capacity_observations AS observation\n'
        || E'        WHERE observation.worker_instance_id = v_worker.id\n'
        || E'          AND observation.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'          AND observation.observation_sequence::numeric >\n'
        || E'              v_worker.instance_epoch::numeric * 4294967296\n'
        || E'          AND observation.observation_sequence::numeric <=\n'
        || E'              v_worker.instance_epoch::numeric * 4294967296 + 4294967295\n'
        || E'    ) THEN\n'
        || E'        FOR v_residency IN SELECT value FROM jsonb_array_elements(p_evidence -> ''residencies'') LOOP\n';
    v_new := E'    FOR v_residency IN SELECT value FROM jsonb_array_elements(p_evidence -> ''residencies'') LOOP\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_residency_takeover_rollback_drift',
            MESSAGE = 'WorkerInstance residency observation rollback changed';
    END IF;
    v_definition := replace(v_definition, v_old, v_new);

    v_old := E'        END LOOP;\n'
        || E'        IF jsonb_array_length(p_evidence -> ''residencies'') = 0 THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''23514'',\n'
        || E'                CONSTRAINT = ''worker_instance_model_residency_required'',\n'
        || E'                MESSAGE = ''WorkerInstance has no resident ModelRuntime'';\n'
        || E'        END IF;\n'
        || E'    ELSE\n'
        || E'        SELECT max(residency.model_runtime_epoch)\n'
        || E'          INTO v_model_runtime_epoch\n'
        || E'        FROM public.model_residencies AS residency\n'
        || E'        WHERE residency.worker_instance_id = v_worker.id\n'
        || E'          AND residency.worker_instance_epoch = v_worker.instance_epoch\n'
        || E'          AND residency.state IN (''READY'', ''WARMING'');\n'
        || E'        IF v_model_runtime_epoch IS NULL THEN\n'
        || E'            RAISE EXCEPTION USING\n'
        || E'                ERRCODE = ''23514'',\n'
        || E'                CONSTRAINT = ''worker_instance_model_residency_required'',\n'
        || E'                MESSAGE = ''WorkerInstance has no durable ModelRuntime'';\n'
        || E'        END IF;\n'
        || E'    END IF;\n\n'
        || E'    v_capacity := p_evidence -> ''capacity'';\n';
    v_new := E'    END LOOP;\n'
        || E'    IF jsonb_array_length(p_evidence -> ''residencies'') = 0 THEN\n'
        || E'        RAISE EXCEPTION USING\n'
        || E'            ERRCODE = ''23514'',\n'
        || E'            CONSTRAINT = ''worker_instance_model_residency_required'',\n'
        || E'            MESSAGE = ''WorkerInstance has no resident ModelRuntime'';\n'
        || E'    END IF;\n\n'
        || E'    v_capacity := p_evidence -> ''capacity'';\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_residency_takeover_rollback_drift',
            MESSAGE = 'WorkerInstance residency observation rollback tail changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
-- +goose StatementEnd
