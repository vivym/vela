-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_is_stage_failure_authority_replayable(p_claim jsonb)
RETURNS boolean
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT EXISTS (
        SELECT 1
        FROM public.stage_leases AS lease
        JOIN public.stage_allocations AS allocation ON allocation.id = lease.stage_allocation_id
        JOIN public.stage_attempts AS physical ON physical.id = lease.stage_attempt_id
        JOIN public.worker_instances AS worker ON worker.id = lease.worker_instance_id
        JOIN public.attempt_coordinator_commands AS command
          ON command.attempt_id = lease.attempt_id AND command.stage_run_id = lease.stage_run_id
         AND command.command_kind = 'FAIL'
         AND command.result ->> 'stage_attempt_id' = lease.stage_attempt_id::text
        WHERE lease.id = (p_claim ->> 'stage_lease_id')::uuid
          AND lease.stage_allocation_id = (p_claim ->> 'stage_allocation_id')::uuid
          AND allocation.stage_attempt_id = physical.id
          AND allocation.worker_instance_id = worker.id
          AND allocation.worker_instance_epoch = worker.instance_epoch
          AND lease.attempt_id = (p_claim ->> 'attempt_id')::uuid
          AND lease.stage_run_id = (p_claim ->> 'stage_run_id')::uuid
          AND lease.stage_attempt_id = (p_claim ->> 'stage_attempt_id')::uuid
          AND lease.attempt_fence = (p_claim ->> 'attempt_fence')::bigint
          AND lease.stage_fence = (p_claim ->> 'stage_fence')::bigint
          AND lease.token_digest = decode(p_claim ->> 'token_digest', 'hex')
          AND lease.state = 'REVOKED' AND lease.revoke_reason = 'STAGE_FAILED'
          AND allocation.state = 'RELEASED' AND physical.state = 'FAILED'
          AND lease.worker_instance_id = (p_claim ->> 'worker_instance_id')::uuid
          AND lease.worker_instance_epoch = (p_claim ->> 'worker_instance_epoch')::bigint
          AND worker.instance_epoch = lease.worker_instance_epoch
          AND worker.control_session_epoch = (p_claim ->> 'control_session_epoch')::bigint
          AND worker.lifecycle_state = 'READY' AND worker.reachability_state = 'CONNECTED'
          AND (command.result ->> 'stage_fence')::bigint = lease.stage_fence + 1
          AND (command.result ->> 'stage_version')::bigint = (p_claim ->> 'stage_version')::bigint + 1
          AND command.result ->> 'stage_state' IN ('RETRY_WAIT', 'FAILED')
          AND (
              NOT EXISTS (SELECT 1 FROM public.stage_authority_renewals AS renewal
                  WHERE renewal.stage_lease_id = lease.id)
              OR EXISTS (SELECT 1 FROM public.stage_authority_renewals AS renewal
                  WHERE renewal.stage_lease_id = lease.id
                    AND renewal.stage_version = (p_claim ->> 'stage_version')::bigint
                    AND renewal.authority_digest = decode(p_claim ->> 'authority_digest', 'hex'))
          )
    )
$$;
ALTER FUNCTION vela_is_stage_failure_authority_replayable(jsonb) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_is_stage_failure_authority_replayable(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_is_stage_failure_authority_replayable(jsonb) TO vela_stage_worker_control;

DO $migration$
DECLARE
    v_definition text;
    v_guard text := $body$           OR v_failed_at < v_stage_attempt.assigned_at$body$;
BEGIN
    -- The existing exact command receipt is returned before these mutable
    -- guards. A newly authorized FAIL must remain live after waiting on locks.
    v_definition := pg_get_functiondef('vela_apply_stage_command(jsonb)'::regprocedure);
    IF strpos(v_definition, v_guard) = 0 OR
       (length(v_definition) - length(replace(v_definition, v_guard, ''))) <> length(v_guard) THEN
        RAISE EXCEPTION 'Stage failure deadline dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_guard,
        v_guard || E'\n           OR clock_timestamp() >= public.vela_stage_lease_effective_expires_at(v_stage_lease.id)'
                || E'\n           OR clock_timestamp() >= GREATEST(v_stage_lease.local_deadline_at, ('
                || E'\n               SELECT max(renewal.local_deadline_at) FROM public.stage_authority_renewals AS renewal'
                || E'\n               WHERE renewal.stage_lease_id = v_stage_lease.id))'
                || E'\n           OR clock_timestamp() >= v_job.job_expires_at');
END
$migration$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $migration$
DECLARE
    v_definition text;
    v_guard text := E'\n           OR clock_timestamp() >= public.vela_stage_lease_effective_expires_at(v_stage_lease.id)'
                || E'\n           OR clock_timestamp() >= GREATEST(v_stage_lease.local_deadline_at, ('
                || E'\n               SELECT max(renewal.local_deadline_at) FROM public.stage_authority_renewals AS renewal'
                || E'\n               WHERE renewal.stage_lease_id = v_stage_lease.id))'
                || E'\n           OR clock_timestamp() >= v_job.job_expires_at';
BEGIN
    v_definition := pg_get_functiondef('vela_apply_stage_command(jsonb)'::regprocedure);
    IF strpos(v_definition, v_guard) = 0 THEN
        RAISE EXCEPTION 'Stage failure deadline rollback dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_guard, '');
END
$migration$;
DROP FUNCTION vela_is_stage_failure_authority_replayable(jsonb);
-- +goose StatementEnd
