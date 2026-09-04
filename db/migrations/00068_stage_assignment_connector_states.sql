-- +goose Up
-- +goose StatementBegin
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure
    );
    v_old := 'revision.state IN (''ACTIVE'', ''DRAINING'')';
    v_new := 'revision.state IN (''CERTIFIED'', ''CANARY'', ''ACTIVE'', ''DRAINING'')';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_assignment_connector_states_dependency_drift',
            MESSAGE = 'StageAssignment Connector-state dependency changed';
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
        'public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure
    );
    v_old := 'revision.state IN (''CERTIFIED'', ''CANARY'', ''ACTIVE'', ''DRAINING'')';
    v_new := 'revision.state IN (''ACTIVE'', ''DRAINING'')';
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <>
       length(v_old) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_assignment_connector_states_rollback_drift',
            MESSAGE = 'StageAssignment Connector-state rollback changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
-- +goose StatementEnd
