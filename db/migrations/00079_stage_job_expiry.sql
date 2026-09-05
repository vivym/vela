-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_guard_stage_job_deadline() RETURNS trigger
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE v_job_id uuid; v_deadline timestamptz;
BEGIN
    IF TG_TABLE_NAME = 'jobs' THEN
        v_job_id := NEW.id;
    ELSIF TG_TABLE_NAME = 'stage_graph_finalization_claims' THEN
        v_job_id := NEW.job_id;
    ELSIF TG_TABLE_NAME = 'stage_runs' THEN
        SELECT attempt.job_id INTO v_job_id FROM public.attempts AS attempt
        WHERE attempt.id = NEW.attempt_id;
    ELSE
        SELECT attempt.job_id INTO v_job_id
        FROM public.stage_runs AS run JOIN public.attempts AS attempt ON attempt.id = run.attempt_id
        WHERE run.id = NEW.stage_run_id;
    END IF;
    -- The deadline is an immutable Admission snapshot. Reading it introduces no
    -- parent-after-child lock, and the clock is evaluated after the writer's locks.
    SELECT job.job_expires_at INTO STRICT v_deadline FROM public.jobs AS job WHERE job.id = v_job_id;
    IF v_deadline <= clock_timestamp() THEN
        RAISE EXCEPTION USING ERRCODE = '40001', CONSTRAINT = 'stage_job_expired',
            MESSAGE = 'Stage Job expiry prohibits new execution or completion authority';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_guard_stage_job_deadline() OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_guard_stage_job_deadline() FROM PUBLIC;

CREATE TRIGGER stage_runs_job_deadline
BEFORE UPDATE OF state ON stage_runs
FOR EACH ROW WHEN (OLD.state IS DISTINCT FROM NEW.state
    AND NEW.state IN ('ASSIGNED', 'RUNNING', 'MATERIALIZING', 'SUCCEEDED'))
EXECUTE FUNCTION vela_guard_stage_job_deadline();
CREATE TRIGGER stage_worker_commands_job_deadline
BEFORE INSERT ON stage_worker_commands
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_job_deadline();
CREATE TRIGGER stage_graph_finalization_job_deadline
BEFORE INSERT ON stage_graph_finalization_claims
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_job_deadline();
CREATE TRIGGER jobs_visible_completion_deadline
BEFORE UPDATE OF state ON jobs
FOR EACH ROW WHEN (OLD.state IS DISTINCT FROM NEW.state AND NEW.state = 'SUCCEEDED')
EXECUTE FUNCTION vela_guard_stage_job_deadline();

CREATE FUNCTION vela_private.vela_expire_job_finalization_claims(p_job_id uuid, p_expired_at timestamptz)
RETURNS void LANGUAGE sql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    UPDATE public.stage_graph_finalization_claims
    SET state = 'EXPIRED', expired_at = p_expired_at, updated_at = p_expired_at
    WHERE job_id = p_job_id AND state = 'ACTIVE'
$$;
ALTER FUNCTION vela_private.vela_expire_job_finalization_claims(uuid, timestamptz) OWNER TO vela_internal;
REVOKE ALL ON FUNCTION vela_private.vela_expire_job_finalization_claims(uuid, timestamptz) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_private.vela_expire_job_finalization_claims(uuid, timestamptz)
    TO vela_attempt_coordinator_owner;

DO $$
DECLARE v_definition text; v_old text; v_new text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_terminalize_stage_graph_failure(uuid,uuid,uuid,text)'::regprocedure);
    EXECUTE replace(v_definition, 'FUNCTION public.vela_terminalize_stage_graph_failure(',
        'FUNCTION public.vela_terminalize_stage_graph_failure_v78(');
    v_old := E'    IF v_job.state NOT IN (''QUEUED'', ''RETRY_WAIT'', ''RUNNING'')\n'
        || E'       OR v_attempt.graph_state NOT IN (''QUEUED'', ''RUNNING'')\n';
    v_new := E'    IF v_job.state NOT IN (''QUEUED'', ''RETRY_WAIT'', ''RUNNING'', ''FINALIZING'')\n'
        || E'       OR v_attempt.graph_state NOT IN (''QUEUED'', ''RUNNING'', ''FINALIZING'')\n';
    IF strpos(v_definition, v_old) = 0 THEN RAISE EXCEPTION 'Job expiry terminal state dependency changed'; END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'       OR NOT EXISTS (\n';
    v_new := E'       OR NOT ((p_stage_run_id IS NULL AND (\n'
        || E'                (p_failure_class = ''JOB_EXPIRED'' AND v_job.job_expires_at <= clock_timestamp())\n'
        || E'                OR (p_failure_class = ''FINALIZATION_DEADLINE_EXPIRED'' AND v_job.state = ''FINALIZING''\n'
        || E'                    AND v_attempt.finalization_deadline_at <= clock_timestamp()))) OR EXISTS (\n';
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'       ) THEN\n';
    v_definition := replace(v_definition, v_old, E'       )) THEN\n');
    v_definition := replace(v_definition, 'WHEN run.id = p_stage_run_id THEN',
        'WHEN p_stage_run_id IS NULL OR run.id = p_stage_run_id THEN');
    v_old := E'    UPDATE public.stage_attempts AS physical\n'
        || E'    SET state = ''CANCELED'', ended_at = v_now, updated_at = v_now\n';
    v_new := E'    UPDATE public.stage_attempts AS physical\n'
        || E'    SET state = CASE WHEN p_stage_run_id IS NULL THEN ''LOST''::public.stage_attempt_state\n'
        || E'                    ELSE ''CANCELED''::public.stage_attempt_state END,\n'
        || E'        failure_class = CASE WHEN p_stage_run_id IS NULL THEN p_failure_class ELSE physical.failure_class END,\n'
        || E'        failure_fingerprint = CASE WHEN p_stage_run_id IS NULL\n'
        || E'            THEN sha256(convert_to(v_job.id::text || '':'' || p_failure_class, ''UTF8''))\n'
        || E'            ELSE physical.failure_fingerprint END,\n'
        || E'        ended_at = v_now, updated_at = v_now\n';
    IF strpos(v_definition, v_old) = 0 THEN RAISE EXCEPTION 'Job expiry physical failure dependency changed'; END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'    SET state = ''FAILED'', version = job.version + 1, updated_at = v_now WHERE job.id = v_job.id;';
    v_new := E'    SET state = ''FAILED'', version = job.version + 1,\n'
        || E'        current_fence = job.current_fence + CASE WHEN p_stage_run_id IS NULL THEN 1 ELSE 0 END,\n'
        || E'        updated_at = v_now WHERE job.id = v_job.id;';
    IF strpos(v_definition, v_old) = 0 THEN RAISE EXCEPTION 'Job expiry graph fence dependency changed'; END IF;
    v_definition := replace(v_definition, v_old, v_new);
    v_old := E'    PERFORM vela_private.vela_insert_stage_authority_expired_event(\n';
    v_new := E'    PERFORM vela_private.vela_expire_job_finalization_claims(v_job.id, v_now);\n' || v_old;
    v_definition := replace(v_definition, v_old, v_new);
    EXECUTE v_definition;
END
$$;
ALTER FUNCTION vela_terminalize_stage_graph_failure_v78(uuid,uuid,uuid,text) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_terminalize_stage_graph_failure_v78(uuid,uuid,uuid,text) FROM PUBLIC;

ALTER FUNCTION vela_reconcile_stage_graphs(integer) RENAME TO vela_reconcile_stage_graphs_v78;
REVOKE ALL ON FUNCTION vela_reconcile_stage_graphs_v78(integer) FROM vela_attempt_coordinator;
CREATE FUNCTION vela_reconcile_stage_graphs(p_limit integer)
RETURNS TABLE (attempt_id uuid, stage_run_id uuid, stage_state text,
    stage_fence bigint, stage_version bigint, reason text)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE v_candidate record; v_count integer := 0;
BEGIN
    IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', CONSTRAINT = 'stage_reconcile_limit_invalid',
            MESSAGE = 'Stage graph reconcile limit must be between 1 and 1000';
    END IF;
    FOR v_candidate IN
        SELECT job.id AS job_id, attempt.id AS attempt_id,
            CASE WHEN job.job_expires_at <= clock_timestamp() THEN 'JOB_EXPIRED'::text
                ELSE 'FINALIZATION_DEADLINE_EXPIRED'::text END AS failure_class
        FROM public.jobs AS job JOIN public.attempts AS attempt
          ON attempt.job_id = job.id AND attempt.fence = job.current_fence
        WHERE job.state IN ('QUEUED', 'RETRY_WAIT', 'RUNNING', 'FINALIZING')
          AND attempt.graph_state IN ('QUEUED', 'RUNNING', 'FINALIZING')
          AND (job.job_expires_at <= clock_timestamp()
              OR (job.state = 'FINALIZING' AND attempt.finalization_deadline_at <= clock_timestamp()))
        ORDER BY job.job_expires_at, job.id LIMIT p_limit
        FOR UPDATE OF job SKIP LOCKED
    LOOP
        PERFORM public.vela_terminalize_stage_graph_failure(
            v_candidate.job_id, v_candidate.attempt_id, NULL, v_candidate.failure_class);
        RETURN QUERY SELECT run.attempt_id, run.id, run.state::text, run.fence, run.version, v_candidate.failure_class
        FROM public.stage_runs AS run WHERE run.attempt_id = v_candidate.attempt_id
        ORDER BY (run.state = 'FAILED') DESC, run.id LIMIT 1;
        v_count := v_count + 1;
    END LOOP;
    IF v_count < p_limit THEN
        RETURN QUERY SELECT * FROM public.vela_reconcile_stage_graphs_v78(p_limit - v_count);
    END IF;
END
$$;
ALTER FUNCTION vela_reconcile_stage_graphs(integer) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_reconcile_stage_graphs(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_reconcile_stage_graphs(integer) TO vela_attempt_coordinator;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP FUNCTION vela_reconcile_stage_graphs(integer);
ALTER FUNCTION vela_reconcile_stage_graphs_v78(integer) RENAME TO vela_reconcile_stage_graphs;
GRANT EXECUTE ON FUNCTION vela_reconcile_stage_graphs(integer) TO vela_attempt_coordinator;
DO $$
BEGIN
    EXECUTE replace(pg_get_functiondef('public.vela_terminalize_stage_graph_failure_v78(uuid,uuid,uuid,text)'::regprocedure),
        'FUNCTION public.vela_terminalize_stage_graph_failure_v78(', 'FUNCTION public.vela_terminalize_stage_graph_failure(');
END
$$;
DROP FUNCTION vela_terminalize_stage_graph_failure_v78(uuid,uuid,uuid,text);
DROP FUNCTION vela_private.vela_expire_job_finalization_claims(uuid, timestamptz);
DROP TRIGGER jobs_visible_completion_deadline ON jobs;
DROP TRIGGER stage_graph_finalization_job_deadline ON stage_graph_finalization_claims;
DROP TRIGGER stage_worker_commands_job_deadline ON stage_worker_commands;
DROP TRIGGER stage_runs_job_deadline ON stage_runs;
DROP FUNCTION vela_guard_stage_job_deadline();
-- +goose StatementEnd
