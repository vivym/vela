-- +goose Up
-- +goose StatementBegin
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure
    );
    v_old := E'                WHEN v_active >= v_limit THEN jsonb_build_array(''CAPACITY_EXHAUSTED'')\n';
    v_new := E'                WHEN attempt.graph_state = ''QUEUED''\n'
        || E'                     AND job.state IN (''QUEUED'', ''RETRY_WAIT'')\n'
        || E'                     AND job.billable_started_at IS NULL\n'
        || E'                     AND attempt.started_at IS NULL\n'
        || E'                     AND EXISTS (\n'
        || E'                         SELECT 1 FROM public.projects AS project\n'
        || E'                         WHERE project.id = job.project_id\n'
        || E'                           AND project.organization_id = job.organization_id\n'
        || E'                           AND project.running_count >= project.running_limit\n'
        || E'                     )\n'
        || E'                THEN jsonb_build_array(''PROJECT_CAPACITY_EXHAUSTED'')\n'
        || v_old;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_scheduler_project_capacity_dependency_drift',
            MESSAGE = 'StageScheduler capacity filter dependency changed';
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
        'public.vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure
    );
    v_new := E'                WHEN v_active >= v_limit THEN jsonb_build_array(''CAPACITY_EXHAUSTED'')\n';
    v_old := E'                WHEN attempt.graph_state = ''QUEUED''\n'
        || E'                     AND job.state IN (''QUEUED'', ''RETRY_WAIT'')\n'
        || E'                     AND job.billable_started_at IS NULL\n'
        || E'                     AND attempt.started_at IS NULL\n'
        || E'                     AND EXISTS (\n'
        || E'                         SELECT 1 FROM public.projects AS project\n'
        || E'                         WHERE project.id = job.project_id\n'
        || E'                           AND project.organization_id = job.organization_id\n'
        || E'                           AND project.running_count >= project.running_limit\n'
        || E'                     )\n'
        || E'                THEN jsonb_build_array(''PROJECT_CAPACITY_EXHAUSTED'')\n'
        || v_new;
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_scheduler_project_capacity_rollback_drift',
            MESSAGE = 'StageScheduler capacity filter rollback changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
-- +goose StatementEnd
