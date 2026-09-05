-- +goose Up
-- +goose StatementBegin
DO $migration$
DECLARE
    v_definition text;
    v_original text := $body$          AND materialization.state = 'ACTIVE'
          AND statement_timestamp() < materialization.expires_at$body$;
    v_replacement text := $body$          AND (
              (materialization.state = 'ACTIVE'
               AND statement_timestamp() < materialization.expires_at)
              OR (materialization.state = 'COMMITTED' AND EXISTS (
                  SELECT 1 FROM stage_artifact_commands AS command
                  WHERE command.stage_attempt_id = materialization.stage_attempt_id
                    AND command.command_kind = 'COMMIT'
                    AND command.result ->> 'artifact_id' = materialization.artifact_id::text
              ))
              OR (materialization.state = 'REVOKED'
                  AND materialization.revoke_reason = 'LOCAL_SOURCE_LOST' AND EXISTS (
                  SELECT 1 FROM stage_artifact_commands AS command
                  WHERE command.stage_attempt_id = materialization.stage_attempt_id
                    AND command.command_kind = 'SOURCE_LOST'
                    AND command.result ->> 'stage_run_id' = materialization.stage_run_id::text
              ))
          )$body$;
    v_guard text := $body$       OR v_materialization.token_digest <> v_token_digest$body$;
BEGIN
    -- The mutation entrypoints still require the exact recorded command digest;
    -- terminal leases cannot authorize a new COMMIT or SOURCE_LOST operation.
    v_definition := pg_get_functiondef('vela_is_stage_materialization_authority_active(jsonb)'::regprocedure);
    IF strpos(v_definition, v_original) = 0 THEN
        RAISE EXCEPTION 'Materialization authority replay dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_original, v_replacement);

    v_definition := pg_get_functiondef('vela_fail_stage_materialization_source(jsonb)'::regprocedure);
    IF strpos(v_definition, v_guard) = 0 OR
       (length(v_definition) - length(replace(v_definition, v_guard, ''))) <> length(v_guard) THEN
        RAISE EXCEPTION 'Materialization source-loss deadline dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_guard,
        v_guard || E'\n       OR clock_timestamp() >= v_materialization.expires_at'
                || E'\n       OR clock_timestamp() >= v_job.job_expires_at'
                || E'\n       OR v_lost_at >= v_materialization.expires_at');
END
$migration$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $migration$
DECLARE
    v_definition text;
    v_original text := $body$          AND (
              (materialization.state = 'ACTIVE'
               AND statement_timestamp() < materialization.expires_at)
              OR (materialization.state = 'COMMITTED' AND EXISTS (
                  SELECT 1 FROM stage_artifact_commands AS command
                  WHERE command.stage_attempt_id = materialization.stage_attempt_id
                    AND command.command_kind = 'COMMIT'
                    AND command.result ->> 'artifact_id' = materialization.artifact_id::text
              ))
              OR (materialization.state = 'REVOKED'
                  AND materialization.revoke_reason = 'LOCAL_SOURCE_LOST' AND EXISTS (
                  SELECT 1 FROM stage_artifact_commands AS command
                  WHERE command.stage_attempt_id = materialization.stage_attempt_id
                    AND command.command_kind = 'SOURCE_LOST'
                    AND command.result ->> 'stage_run_id' = materialization.stage_run_id::text
              ))
          )$body$;
    v_replacement text := $body$          AND materialization.state = 'ACTIVE'
          AND statement_timestamp() < materialization.expires_at$body$;
    v_guard text := E'\n       OR clock_timestamp() >= v_materialization.expires_at'
                || E'\n       OR clock_timestamp() >= v_job.job_expires_at'
                || E'\n       OR v_lost_at >= v_materialization.expires_at';
BEGIN
    v_definition := pg_get_functiondef('vela_is_stage_materialization_authority_active(jsonb)'::regprocedure);
    IF strpos(v_definition, v_original) = 0 THEN
        RAISE EXCEPTION 'Materialization authority replay rollback dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_original, v_replacement);

    v_definition := pg_get_functiondef('vela_fail_stage_materialization_source(jsonb)'::regprocedure);
    IF strpos(v_definition, v_guard) = 0 THEN
        RAISE EXCEPTION 'Materialization source-loss deadline rollback dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_guard, '');
END
$migration$;
-- +goose StatementEnd
