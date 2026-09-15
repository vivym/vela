-- +goose Up
-- Keep the authenticated assignment lookup as the only worker-facing read.
-- Diagnostic context does not authorize an execution or grant jobs table access.
-- +goose StatementBegin
ALTER FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    RENAME TO vela_read_stage_assignment_execution_v4;
REVOKE EXECUTE ON FUNCTION vela_read_stage_assignment_execution_v4(uuid,uuid)
    FROM vela_stage_worker_control;

CREATE FUNCTION vela_read_stage_assignment_execution(p_command_id uuid, p_claim_id uuid)
RETURNS TABLE (snapshot jsonb)
LANGUAGE plpgsql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_snapshot jsonb;
    v_parent text;
BEGIN
    SELECT prior.snapshot INTO STRICT v_snapshot
    FROM vela_read_stage_assignment_execution_v4(p_command_id, p_claim_id) AS prior;
    SELECT job.origin_trace_parent INTO STRICT v_parent
    FROM jobs AS job
    JOIN attempts AS attempt ON attempt.job_id = job.id
    WHERE job.id = (v_snapshot ->> 'job_id')::uuid
      AND attempt.id = (v_snapshot ->> 'attempt_id')::uuid;
    RETURN QUERY SELECT v_snapshot || jsonb_build_object('origin_trace_parent', v_parent);
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
DROP FUNCTION vela_read_stage_assignment_execution(uuid,uuid);
ALTER FUNCTION vela_read_stage_assignment_execution_v4(uuid,uuid)
    RENAME TO vela_read_stage_assignment_execution;
GRANT EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    TO vela_stage_worker_control;
-- +goose StatementEnd
