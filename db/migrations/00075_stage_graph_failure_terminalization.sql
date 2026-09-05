-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_terminalize_stage_graph_failure(
    p_job_id uuid, p_attempt_id uuid, p_stage_run_id uuid, p_failure_class text
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_job public.jobs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_credit public.credit_reservations%ROWTYPE;
    v_now timestamptz;
    v_rows bigint;
BEGIN
    SELECT * INTO STRICT v_job FROM public.jobs WHERE id = p_job_id FOR UPDATE;
    SELECT * INTO STRICT v_attempt FROM public.attempts
    WHERE id = p_attempt_id AND job_id = v_job.id FOR UPDATE;
    IF v_job.state = 'FAILED' AND v_attempt.graph_state = 'FAILED' THEN RETURN; END IF;
    IF v_job.state NOT IN ('QUEUED', 'RETRY_WAIT', 'RUNNING')
       OR v_attempt.graph_state NOT IN ('QUEUED', 'RUNNING')
       OR v_attempt.fence <> v_job.current_fence
       OR p_failure_class IS NULL OR length(p_failure_class) NOT BETWEEN 1 AND 100
       OR NOT EXISTS (
           SELECT 1 FROM public.stage_runs AS run
           JOIN public.stage_attempts AS physical ON physical.stage_run_id = run.id
           WHERE run.id = p_stage_run_id AND run.attempt_id = v_attempt.id
             AND run.state IN ('ASSIGNED', 'RUNNING', 'MATERIALIZING')
             AND physical.state IN ('FAILED', 'LOST')
             AND physical.failure_class = p_failure_class
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '40001',
            CONSTRAINT = 'stage_graph_failure_authority_stale',
            MESSAGE = 'Stage graph failure requires current failed physical authority';
    END IF;
    PERFORM run.id FROM public.stage_runs AS run
    WHERE run.attempt_id = v_attempt.id ORDER BY run.id FOR UPDATE;
    PERFORM physical.id FROM public.stage_attempts AS physical
    WHERE physical.attempt_id = v_attempt.id ORDER BY physical.id FOR UPDATE;
    PERFORM lease.id FROM public.stage_leases AS lease
    WHERE lease.attempt_id = v_attempt.id ORDER BY lease.id FOR UPDATE;
    PERFORM allocation.id FROM public.stage_allocations AS allocation
    WHERE allocation.attempt_id = v_attempt.id ORDER BY allocation.id FOR UPDATE;
    PERFORM materialization.id FROM public.stage_materialization_leases AS materialization
    WHERE materialization.attempt_id = v_attempt.id ORDER BY materialization.id FOR UPDATE;
    PERFORM project.id FROM public.projects AS project WHERE project.id = v_job.project_id FOR UPDATE;
    SELECT * INTO STRICT v_credit FROM public.credit_reservations
    WHERE job_id = v_job.id FOR UPDATE;
    PERFORM account.organization_id FROM public.organization_credit_accounts AS account
    WHERE account.organization_id = v_job.organization_id AND account.currency = v_credit.currency
    FOR UPDATE;
    IF v_credit.state <> 'RESERVED' THEN
        RAISE EXCEPTION 'Live graph failure must retain RESERVED credit' USING ERRCODE = '23514';
    END IF;
    v_now := clock_timestamp();
    UPDATE public.stage_leases AS lease
    SET state = 'REVOKED', revoked_at = v_now, revoke_reason = 'GRAPH_TERMINAL'
    WHERE lease.attempt_id = v_attempt.id AND lease.state = 'ACTIVE';
    -- Revocation fences future requests; it does not prove a disconnected
    -- sibling has stopped executing its last signed authority.
    UPDATE public.stage_allocations AS allocation
    SET state = 'RELEASED', released_at = v_now, release_reason = 'TERMINAL_AUTHORITY_EXPIRED'
    WHERE allocation.attempt_id = v_attempt.id AND allocation.state = 'ALLOCATED'
      AND EXISTS (
          SELECT 1 FROM public.stage_leases AS lease
          WHERE lease.stage_allocation_id = allocation.id
            AND public.vela_stage_lease_effective_expires_at(lease.id) <= v_now
      );
    UPDATE public.stage_materialization_leases AS materialization
    SET state = 'REVOKED', revoked_at = v_now, revoke_reason = 'GRAPH_TERMINAL'
    WHERE materialization.attempt_id = v_attempt.id AND materialization.state = 'ACTIVE';
    UPDATE public.stage_attempts AS physical
    SET state = 'CANCELED', ended_at = v_now, updated_at = v_now
    WHERE physical.attempt_id = v_attempt.id
      AND physical.state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED');
    UPDATE public.stage_runs AS run
    SET state = CASE WHEN run.id = p_stage_run_id THEN 'FAILED'::public.stage_run_state
            ELSE 'CANCELED'::public.stage_run_state END,
        fence = run.fence + 1, next_retry_at = NULL, version = run.version + 1, updated_at = v_now
    WHERE run.attempt_id = v_attempt.id AND run.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED');
    UPDATE public.stage_retry_budgets AS budget
    SET state = CASE WHEN budget.attempts_consumed = budget.max_attempts
            THEN 'EXHAUSTED'::public.stage_retry_budget_state
            ELSE 'CANCELED'::public.stage_retry_budget_state END,
        version = budget.version + 1, updated_at = v_now
    FROM public.stage_runs AS run
    WHERE run.id = budget.stage_run_id AND run.attempt_id = v_attempt.id AND budget.state = 'ACTIVE';
    UPDATE public.attempt_retry_budgets AS budget
    SET state = CASE WHEN budget.consumed_resource_units = budget.max_resource_units
            THEN 'EXHAUSTED'::public.stage_retry_budget_state
            ELSE 'CANCELED'::public.stage_retry_budget_state END,
        version = budget.version + 1, updated_at = v_now
    WHERE budget.attempt_id = v_attempt.id AND budget.state = 'ACTIVE';
    UPDATE public.stage_storage_reservations AS reservation
    SET state = 'RELEASED', updated_at = v_now
    WHERE reservation.attempt_id = v_attempt.id AND reservation.state = 'RESERVED';
    UPDATE public.attempts AS attempt
    SET state = 'FAILED', graph_state = 'FAILED', ended_at = v_now, updated_at = v_now
    WHERE attempt.id = v_attempt.id;
    UPDATE public.jobs AS job
    SET state = 'FAILED', version = job.version + 1, updated_at = v_now WHERE job.id = v_job.id;
    UPDATE public.projects AS project
    SET running_count = project.running_count - CASE WHEN v_job.billable_started_at IS NOT NULL THEN 1 ELSE 0 END,
        queued_count = project.queued_count - CASE WHEN v_job.billable_started_at IS NULL THEN 1 ELSE 0 END,
        retry_wait_count = project.retry_wait_count - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END
    WHERE project.id = v_job.project_id;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN RAISE EXCEPTION 'Failure Project counter owner is missing'; END IF;
    UPDATE public.credit_reservations AS reservation
    SET state = 'RELEASED', updated_at = v_now WHERE reservation.id = v_credit.id;
    UPDATE public.organization_credit_accounts AS account
    SET reserved_minor = account.reserved_minor - v_credit.amount_minor,
        version = account.version + 1, updated_at = v_now
    WHERE account.organization_id = v_job.organization_id AND account.currency = v_credit.currency;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN RAISE EXCEPTION 'Failure credit account owner is missing'; END IF;
    PERFORM vela_private.vela_insert_stage_authority_expired_event(
        v_job.id, v_attempt.id, p_failure_class, v_now
    );
END
$$;
ALTER FUNCTION vela_terminalize_stage_graph_failure(uuid, uuid, uuid, text)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_terminalize_stage_graph_failure(uuid, uuid, uuid, text) FROM PUBLIC;

DO $$
DECLARE
    v_signature text;
    v_name text;
    v_definition text;
    v_start text;
    v_end text;
    v_replacement text;
    v_start_at integer;
    v_end_at integer;
    v_prefix text;
BEGIN
    -- Keep private rollback copies, preserving the live entrypoint OIDs and ACLs.
    FOR v_signature, v_name, v_start, v_end, v_replacement IN
        SELECT * FROM (VALUES
            ('public.vela_apply_stage_command(jsonb)', 'vela_apply_stage_command',
             E'        ELSE\n            UPDATE public.stage_retry_budgets AS budget\n',
             E'            v_result := jsonb_build_object(\n                ''stage_run_id'', v_run.id,\n                ''stage_attempt_id'', v_stage_attempt.id,\n                ''stage_state'', ''FAILED'',',
             E'        ELSE\n            PERFORM public.vela_terminalize_stage_graph_failure(\n'
                 || E'                v_job.id, v_attempt.id, v_run.id, v_failure_class\n            );\n'),
            ('public.vela_fail_stage_materialization_source(jsonb)', 'vela_fail_stage_materialization_source',
             E'    ELSE\n        UPDATE stage_retry_budgets AS budget SET state = CASE\n',
             E'        v_result := jsonb_build_object(\n            ''stage_run_id'', v_run.id, ''stage_state'', ''FAILED'',',
             E'    ELSE\n        PERFORM public.vela_terminalize_stage_graph_failure(\n'
                 || E'            v_job.id, v_attempt.id, v_run.id, ''LOCAL_MATERIALIZATION_SOURCE_LOST''\n        );\n'),
            ('public.vela_fence_assigned_stage_for_runtime_epoch(uuid,uuid,bigint,bigint,timestamptz)',
             'vela_fence_assigned_stage_for_runtime_epoch',
             E'    PERFORM 1 FROM public.projects AS project\n',
             E'\nEND\n',
             E'    PERFORM public.vela_terminalize_stage_graph_failure(\n'
                 || E'        v_job.id, v_attempt.id, v_run.id, ''WORKER_LOST''\n    );\n'),
            ('public.vela_recover_expired_stage_authority(uuid)', 'vela_recover_expired_stage_authority',
             E'    ELSE\n        PERFORM project.id FROM public.projects AS project\n',
             E'    END IF;\n    RETURN QUERY SELECT run.attempt_id, run.id, run.state::text,',
             E'    ELSE\n        PERFORM public.vela_terminalize_stage_graph_failure(\n'
                 || E'            v_job.id, v_attempt.id, v_run.id, v_failure_class\n        );\n')
        ) AS rewrite(signature, name, start_marker, end_marker, replacement)
    LOOP
        v_definition := pg_get_functiondef(v_signature::regprocedure);
        EXECUTE replace(v_definition, 'FUNCTION public.' || v_name || '(',
            'FUNCTION public.' || v_name || '_v74(');
        EXECUTE format('ALTER FUNCTION %s OWNER TO vela_attempt_coordinator_owner',
            replace(v_signature, v_name, v_name || '_v74'));
        EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC',
            replace(v_signature, v_name, v_name || '_v74'));
        v_start_at := strpos(v_definition, v_start);
        IF v_start_at = 0 OR
           (length(v_definition) - length(replace(v_definition, v_start, ''))) <> length(v_start) THEN
            RAISE EXCEPTION 'Failure terminalization start dependency changed: %', v_signature;
        END IF;
        v_end_at := strpos(substr(v_definition, v_start_at + length(v_start)), v_end);
        IF v_end_at = 0 THEN
            RAISE EXCEPTION 'Failure terminalization end dependency changed: %', v_signature;
        END IF;
        v_end_at := v_start_at + length(v_start) + v_end_at - 1;
        EXECUTE left(v_definition, v_start_at - 1) || v_replacement || substr(v_definition, v_end_at);
    END LOOP;

    -- Parent Job serialization must precede every child authority lock. Worker
    -- control also follows registration's Worker -> runtime gate -> Job order.
    FOR v_signature, v_name, v_start, v_prefix IN
        SELECT * FROM (VALUES
            ('public.vela_start_stage_worker_command(jsonb)', 'vela_start_stage_worker_command',
             E'    SELECT attempt.* INTO v_attempt\n    FROM attempts AS attempt\n', 'worker'),
            ('public.vela_heartbeat_stage_worker_command(jsonb)', 'vela_heartbeat_stage_worker_command',
             E'    SELECT attempt.* INTO v_attempt\n    FROM attempts AS attempt\n', 'worker'),
            ('public.vela_reattach_stage_worker_command(jsonb)', 'vela_reattach_stage_worker_command',
             E'    SELECT attempt.* INTO v_attempt\n    FROM attempts AS attempt\n', 'worker'),
            ('public.vela_commit_stage_artifact(jsonb)', 'vela_commit_stage_artifact',
             E'    SELECT materialization.* INTO v_materialization\n', 'materialization'),
            ('public.vela_fail_stage_materialization_source(jsonb)', 'vela_fail_stage_materialization_source',
             E'    SELECT materialization.* INTO v_materialization\n', 'materialization')
        ) AS lock_order(signature, name, start_marker, prefix_kind)
    LOOP
        v_definition := pg_get_functiondef(v_signature::regprocedure);
        IF v_name <> 'vela_fail_stage_materialization_source' THEN
            EXECUTE replace(v_definition, 'FUNCTION public.' || v_name || '(',
                'FUNCTION public.' || v_name || '_v74(');
            EXECUTE format('ALTER FUNCTION %s OWNER TO vela_attempt_coordinator_owner',
                replace(v_signature, v_name, v_name || '_v74'));
            EXECUTE format('REVOKE ALL ON FUNCTION %s FROM PUBLIC',
                replace(v_signature, v_name, v_name || '_v74'));
        END IF;
        IF strpos(v_definition, v_start) = 0 OR
           (length(v_definition) - length(replace(v_definition, v_start, ''))) <> length(v_start) THEN
            RAISE EXCEPTION 'Stage parent lock dependency changed: %', v_signature;
        END IF;
        IF v_prefix = 'worker' THEN
            v_replacement := E'    PERFORM public.vela_lock_stage_worker_control_session(\n'
                || E'        v_worker_instance_id, v_worker_instance_epoch, v_control_session_epoch\n    );\n'
                || E'    PERFORM public.vela_lock_model_runtime_epoch_gate(v_model_residency_id);\n'
                || E'    PERFORM job.id FROM public.jobs AS job\n'
                || E'    JOIN public.attempts AS parent_attempt ON parent_attempt.job_id = job.id\n'
                || E'    WHERE parent_attempt.id = v_attempt_id FOR UPDATE OF job;\n\n';
        ELSE
            v_replacement := E'    PERFORM job.id FROM public.jobs AS job\n'
                || E'    JOIN public.stage_materialization_leases AS authority ON authority.job_id = job.id\n'
                || E'    WHERE authority.id = v_lease_id FOR UPDATE OF job;\n\n';
        END IF;
        EXECUTE replace(v_definition, v_start, v_replacement || v_start);
    END LOOP;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE v_signature text; v_name text; v_definition text;
BEGIN
    FOR v_signature, v_name IN SELECT * FROM (VALUES
        ('public.vela_apply_stage_command_v74(jsonb)', 'vela_apply_stage_command'),
        ('public.vela_fail_stage_materialization_source_v74(jsonb)', 'vela_fail_stage_materialization_source'),
        ('public.vela_fence_assigned_stage_for_runtime_epoch_v74(uuid,uuid,bigint,bigint,timestamptz)',
            'vela_fence_assigned_stage_for_runtime_epoch'),
        ('public.vela_recover_expired_stage_authority_v74(uuid)', 'vela_recover_expired_stage_authority'),
        ('public.vela_start_stage_worker_command_v74(jsonb)', 'vela_start_stage_worker_command'),
        ('public.vela_heartbeat_stage_worker_command_v74(jsonb)', 'vela_heartbeat_stage_worker_command'),
        ('public.vela_reattach_stage_worker_command_v74(jsonb)', 'vela_reattach_stage_worker_command'),
        ('public.vela_commit_stage_artifact_v74(jsonb)', 'vela_commit_stage_artifact')
    ) AS rollback(signature, name)
    LOOP
        v_definition := pg_get_functiondef(v_signature::regprocedure);
        EXECUTE replace(v_definition, 'FUNCTION public.' || v_name || '_v74(',
            'FUNCTION public.' || v_name || '(');
        EXECUTE 'DROP FUNCTION ' || v_signature;
    END LOOP;
END
$$;
DROP FUNCTION vela_terminalize_stage_graph_failure(uuid, uuid, uuid, text);
-- +goose StatementEnd
