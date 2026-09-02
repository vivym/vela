-- +goose Up
-- +goose StatementBegin
ALTER FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    RENAME TO vela_read_stage_assignment_execution_v3;
REVOKE EXECUTE ON FUNCTION vela_read_stage_assignment_execution_v3(uuid,uuid)
    FROM vela_stage_worker_control;

CREATE FUNCTION vela_read_stage_assignment_execution(
    p_command_id uuid,
    p_claim_id uuid
) RETURNS TABLE (snapshot jsonb)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_snapshot jsonb;
    v_stage_kind text;
    v_h3 jsonb;
    v_parameters jsonb;
    v_stored_root_inputs jsonb;
    v_root_inputs jsonb := '[]'::jsonb;
    v_root_input_fetches jsonb := '[]'::jsonb;
    v_condition_count integer;
    v_root_input_count integer;
    v_valid_root_input_count integer;
BEGIN
    SELECT legacy.snapshot INTO STRICT v_snapshot
    FROM vela_read_stage_assignment_execution_v3(p_command_id, p_claim_id) AS legacy;

    SELECT definition.stage_kind INTO STRICT v_stage_kind
    FROM stage_runs AS run
    JOIN stage_definition_revisions AS definition
      ON definition.id = run.stage_definition_revision_id
    WHERE run.id = (v_snapshot ->> 'stage_run_id')::uuid;

    IF v_stage_kind IN ('ENCODER', 'DIT', 'VAE_DECODER') THEN
        v_h3 := v_snapshot #> '{parameters,h3}';
        IF jsonb_typeof(v_h3) <> 'object'
           OR NOT (v_h3 ? 'parameters')
           OR NOT (v_h3 ? 'root_inputs')
           OR (SELECT count(*) FROM jsonb_object_keys(v_h3)) <> 2 THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'h3_stage_parameters_snapshot_invalid',
                MESSAGE = 'Accepted H3 Job lacks a frozen execution snapshot';
        END IF;
        v_parameters := v_h3 -> 'parameters';
        v_stored_root_inputs := v_h3 -> 'root_inputs';
        IF jsonb_typeof(v_parameters) <> 'object'
           OR jsonb_typeof(v_parameters -> 'canonical_request') <> 'object'
           OR jsonb_typeof(v_parameters #> '{canonical_request,conditions}') <> 'array'
           OR jsonb_typeof(v_parameters -> 'sampling') <> 'object'
           OR (v_parameters ->> 'schema_revision')::integer <> 1
           OR jsonb_typeof(v_stored_root_inputs) <> 'array' THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'h3_stage_parameters_snapshot_invalid',
                MESSAGE = 'Accepted H3 Job execution snapshot is malformed';
        END IF;

        v_condition_count := jsonb_array_length(
            v_parameters #> '{canonical_request,conditions}'
        );
        v_root_input_count := jsonb_array_length(v_stored_root_inputs);
        SELECT count(*) INTO v_valid_root_input_count
        FROM jsonb_array_elements(v_stored_root_inputs)
             WITH ORDINALITY AS entry(value, position)
        WHERE jsonb_typeof(entry.value) = 'object'
          AND (SELECT count(*) FROM jsonb_object_keys(entry.value)) = 5
          AND (entry.value ->> 'condition_index')::integer = entry.position - 1
          AND length(entry.value ->> 'uri') BETWEEN 1 AND 16384
          AND length(entry.value ->> 'download_url') BETWEEN 1 AND 16384
          AND entry.value ->> 'download_url' LIKE 'https://%'
          AND entry.value ->> 'sha256' ~ '^[0-9a-f]{64}$'
          AND (entry.value ->> 'size_bytes')::bigint BETWEEN 1 AND 34359738368;
        IF v_root_input_count <> v_condition_count
           OR v_valid_root_input_count <> v_root_input_count THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'h3_root_input_snapshot_invalid',
                MESSAGE = 'Accepted H3 Job root inputs do not match canonical conditions';
        END IF;

        IF v_stage_kind = 'ENCODER' THEN
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'condition_index', (entry.value ->> 'condition_index')::integer,
                'uri', entry.value ->> 'uri',
                'sha256', entry.value ->> 'sha256',
                'size_bytes', (entry.value ->> 'size_bytes')::bigint
            ) ORDER BY entry.position), '[]'::jsonb) INTO v_root_inputs
            FROM jsonb_array_elements(v_stored_root_inputs)
                 WITH ORDINALITY AS entry(value, position);
            SELECT COALESCE(jsonb_agg(jsonb_build_object(
                'condition_index', (entry.value ->> 'condition_index')::integer,
                'sha256', entry.value ->> 'sha256',
                'download_url', entry.value ->> 'download_url'
            ) ORDER BY entry.position), '[]'::jsonb) INTO v_root_input_fetches
            FROM jsonb_array_elements(v_stored_root_inputs)
                 WITH ORDINALITY AS entry(value, position);
        END IF;

        v_snapshot := jsonb_set(v_snapshot, '{parameters}', v_parameters, true);
        v_snapshot := jsonb_set(v_snapshot, '{root_inputs}', v_root_inputs, true);
        v_snapshot := jsonb_set(
            v_snapshot,
            '{root_input_fetches}',
            v_root_input_fetches,
            true
        );
    END IF;
    RETURN QUERY SELECT v_snapshot;
END
$$;
ALTER FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_read_stage_assignment_execution(uuid,uuid);
ALTER FUNCTION vela_read_stage_assignment_execution_v3(uuid,uuid)
    RENAME TO vela_read_stage_assignment_execution;
GRANT EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    TO vela_stage_worker_control;
-- +goose StatementEnd
