-- +goose Up
-- +goose StatementBegin
CREATE TABLE model_runtime_epoch_registrations (
    model_residency_id uuid NOT NULL REFERENCES model_residencies(id),
    model_runtime_epoch bigint NOT NULL CHECK (model_runtime_epoch > 0),
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    worker_member_id uuid NOT NULL REFERENCES worker_members(id),
    worker_member_epoch bigint NOT NULL CHECK (worker_member_epoch > 0),
    readiness_evidence_digest bytea NOT NULL CHECK (
        octet_length(readiness_evidence_digest) = 32
    ),
    spiffe_id_digest bytea NOT NULL CHECK (octet_length(spiffe_id_digest) = 32),
    registered_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (model_residency_id, model_runtime_epoch, worker_member_id),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch)
);
ALTER TABLE model_runtime_epoch_registrations OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON TABLE model_runtime_epoch_registrations FROM PUBLIC;
GRANT SELECT, UPDATE ON model_residencies TO vela_attempt_coordinator_owner;

CREATE FUNCTION vela_lock_model_runtime_epoch_gate(p_model_residency_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_model_residency_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_runtime_epoch_gate_invalid',
            MESSAGE = 'ModelRuntime epoch gate requires a ModelResidency';
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_model_residency_id::text, 590059)
    );
END
$$;
ALTER FUNCTION vela_lock_model_runtime_epoch_gate(uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_lock_model_runtime_epoch_gate(uuid) FROM PUBLIC;

-- Serialize every lease-producing or lease-consuming command with runtime
-- epoch registration. Rewrite the retained Stage-only function in place so
-- migration 58's removed Legacy authority dependencies do not survive under a
-- second function name.
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_apply_stage_command(jsonb)'::regprocedure
    );
    v_old := E'    v_stage_run_id := (p_command ->> ''stage_run_id'')::uuid;\n'
        || E'    IF v_kind = ''FAIL'' THEN\n';
    v_new := E'    v_stage_run_id := (p_command ->> ''stage_run_id'')::uuid;\n'
        || E'    IF v_kind = ''ASSIGN'' THEN\n'
        || E'        v_residency_id := (p_command ->> ''model_residency_id'')::uuid;\n'
        || E'    ELSIF v_kind IN (''START'', ''COMPLETE'', ''FAIL'') THEN\n'
        || E'        SELECT lease.model_residency_id INTO v_residency_id\n'
        || E'        FROM public.stage_leases AS lease\n'
        || E'        WHERE lease.id = (p_command ->> ''stage_lease_id'')::uuid;\n'
        || E'    END IF;\n'
        || E'    IF v_residency_id IS NOT NULL THEN\n'
        || E'        PERFORM public.vela_lock_model_runtime_epoch_gate(v_residency_id);\n'
        || E'    END IF;\n'
        || E'    IF v_kind = ''FAIL'' THEN\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_epoch_stage_command_dependency_drift',
            MESSAGE = 'vela_apply_stage_command entrypoint changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;

CREATE FUNCTION vela_fence_assigned_stage_for_runtime_epoch(
    p_stage_lease_id uuid,
    p_model_residency_id uuid,
    p_old_model_runtime_epoch bigint,
    p_new_model_runtime_epoch bigint,
    p_fenced_at timestamptz
)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_job public.jobs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_run public.stage_runs%ROWTYPE;
    v_physical public.stage_attempts%ROWTYPE;
    v_lease public.stage_leases%ROWTYPE;
    v_allocation public.stage_allocations%ROWTYPE;
    v_stage_budget public.stage_retry_budgets%ROWTYPE;
    v_attempt_budget public.attempt_retry_budgets%ROWTYPE;
    v_credit public.credit_reservations%ROWTYPE;
    v_job_id uuid;
    v_retry_at timestamptz := p_fenced_at + interval '1 second';
    v_retry_allowed boolean;
    v_billable boolean;
    v_failure_fingerprint bytea;
    v_rows bigint;
BEGIN
    SELECT attempt.job_id INTO v_job_id
    FROM public.stage_leases AS lease
    JOIN public.attempts AS attempt ON attempt.id = lease.attempt_id
    WHERE lease.id = p_stage_lease_id;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT job.* INTO v_job
    FROM public.jobs AS job WHERE job.id = v_job_id FOR UPDATE;
    SELECT attempt.* INTO v_attempt
    FROM public.attempts AS attempt
    JOIN public.stage_leases AS lease ON lease.attempt_id = attempt.id
    WHERE lease.id = p_stage_lease_id FOR UPDATE OF attempt;
    SELECT run.* INTO v_run
    FROM public.stage_runs AS run
    JOIN public.stage_leases AS lease ON lease.stage_run_id = run.id
    WHERE lease.id = p_stage_lease_id FOR UPDATE OF run;
    SELECT physical.* INTO v_physical
    FROM public.stage_attempts AS physical
    JOIN public.stage_leases AS lease ON lease.stage_attempt_id = physical.id
    WHERE lease.id = p_stage_lease_id FOR UPDATE OF physical;
    SELECT lease.* INTO v_lease
    FROM public.stage_leases AS lease
    WHERE lease.id = p_stage_lease_id FOR UPDATE;
    IF v_lease.state <> 'ACTIVE' THEN
        RETURN;
    END IF;
    SELECT allocation.* INTO v_allocation
    FROM public.stage_allocations AS allocation
    WHERE allocation.id = v_lease.stage_allocation_id FOR UPDATE;
    SELECT budget.* INTO v_attempt_budget
    FROM public.attempt_retry_budgets AS budget
    WHERE budget.attempt_id = v_attempt.id FOR UPDATE;
    SELECT budget.* INTO v_stage_budget
    FROM public.stage_retry_budgets AS budget
    WHERE budget.stage_run_id = v_run.id FOR UPDATE;

    v_billable := v_attempt.graph_state = 'RUNNING';
    IF v_attempt.graph_state NOT IN ('QUEUED', 'RUNNING')
       OR v_attempt.fence <> v_job.current_fence
       OR (v_attempt.graph_state = 'QUEUED' AND (
           v_job.state NOT IN ('QUEUED', 'RETRY_WAIT')
           OR v_job.billable_started_at IS NOT NULL
           OR v_attempt.started_at IS NOT NULL
       ))
       OR (v_attempt.graph_state = 'RUNNING' AND (
           v_job.state <> 'RUNNING'
           OR v_job.billable_started_at IS NULL
           OR v_attempt.started_at IS NULL
       ))
       OR v_run.state <> 'ASSIGNED'
       OR v_physical.state <> 'ASSIGNED'
       OR v_physical.started_at IS NOT NULL
       OR v_lease.model_residency_id <> p_model_residency_id
       OR v_lease.model_runtime_epoch <> p_old_model_runtime_epoch
       OR v_lease.attempt_fence <> v_attempt.fence
       OR v_lease.stage_fence <> v_run.fence
       OR v_allocation.state <> 'ALLOCATED'
       OR v_allocation.model_residency_id <> p_model_residency_id
       OR v_allocation.model_runtime_epoch <> p_old_model_runtime_epoch
       OR v_attempt_budget.state <> 'ACTIVE'
       OR v_stage_budget.state <> 'ACTIVE'
       OR p_new_model_runtime_epoch <= p_old_model_runtime_epoch
       OR p_fenced_at < v_physical.assigned_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'model_runtime_assigned_stage_fence_stale',
            MESSAGE = 'Assigned Stage authority changed during ModelRuntime epoch fencing';
    END IF;

    v_failure_fingerprint := pg_catalog.sha256(convert_to(
        p_model_residency_id::text || ':' || p_old_model_runtime_epoch::text
        || ':' || p_new_model_runtime_epoch::text,
        'UTF8'
    ));
    v_retry_allowed := 'WORKER_LOST' = ANY(v_job.execution_retryable_failure_classes)
        AND v_stage_budget.attempts_consumed < v_stage_budget.max_attempts
        AND v_retry_at < v_job.job_expires_at;

    UPDATE public.stage_leases AS lease
    SET state = 'REVOKED', revoked_at = p_fenced_at,
        revoke_reason = 'MODEL_RUNTIME_EPOCH_ADVANCED'
    WHERE lease.id = v_lease.id AND lease.state = 'ACTIVE';
    UPDATE public.stage_allocations AS allocation
    SET state = 'RELEASED', released_at = p_fenced_at,
        release_reason = 'MODEL_RUNTIME_EPOCH_ADVANCED'
    WHERE allocation.id = v_allocation.id AND allocation.state = 'ALLOCATED';
    UPDATE public.stage_attempts AS physical
    SET state = 'LOST', ended_at = p_fenced_at,
        failure_class = 'WORKER_LOST',
        failure_fingerprint = v_failure_fingerprint,
        resource_totals = '{}'::jsonb,
        updated_at = p_fenced_at
    WHERE physical.id = v_physical.id AND physical.state = 'ASSIGNED';

    IF v_retry_allowed THEN
        UPDATE public.stage_runs AS run
        SET state = 'RETRY_WAIT', fence = run.fence + 1,
            retry_count = run.retry_count + 1,
            next_retry_at = v_retry_at,
            version = run.version + 1,
            updated_at = p_fenced_at
        WHERE run.id = v_run.id AND run.state = 'ASSIGNED'
          AND run.version = v_run.version AND run.fence = v_run.fence;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'model_runtime_assigned_stage_fence_stale',
                MESSAGE = 'StageRun changed during ModelRuntime epoch fencing';
        END IF;
        RETURN;
    END IF;

    PERFORM 1 FROM public.projects AS project
    WHERE project.id = v_job.project_id
      AND project.organization_id = v_job.organization_id
    FOR UPDATE;
    SELECT reservation.* INTO STRICT v_credit
    FROM public.credit_reservations AS reservation
    WHERE reservation.job_id = v_job.id FOR UPDATE;
    PERFORM 1 FROM public.organization_credit_accounts AS account
    WHERE account.organization_id = v_job.organization_id
      AND account.currency = v_credit.currency
    FOR UPDATE;

    UPDATE public.stage_leases AS lease
    SET state = 'REVOKED', revoked_at = p_fenced_at,
        revoke_reason = 'MODEL_RUNTIME_EPOCH_ADVANCED'
    WHERE lease.attempt_id = v_attempt.id AND lease.state = 'ACTIVE';
    UPDATE public.stage_allocations AS allocation
    SET state = 'RELEASED', released_at = p_fenced_at,
        release_reason = 'MODEL_RUNTIME_EPOCH_ADVANCED'
    WHERE allocation.attempt_id = v_attempt.id
      AND allocation.state = 'ALLOCATED';
    UPDATE public.stage_attempts AS physical
    SET state = 'CANCELED', ended_at = p_fenced_at,
        updated_at = p_fenced_at
    WHERE physical.attempt_id = v_attempt.id
      AND physical.state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED');
    UPDATE public.stage_runs AS run
    SET state = CASE WHEN run.id = v_run.id
            THEN 'FAILED'::public.stage_run_state
            ELSE 'CANCELED'::public.stage_run_state END,
        fence = run.fence + 1,
        next_retry_at = NULL,
        version = run.version + 1,
        updated_at = p_fenced_at
    WHERE run.attempt_id = v_attempt.id
      AND run.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED');
    UPDATE public.stage_retry_budgets AS budget
    SET state = CASE
            WHEN run.id = v_run.id
                 AND budget.attempts_consumed = budget.max_attempts
            THEN 'EXHAUSTED'::public.stage_retry_budget_state
            ELSE 'CANCELED'::public.stage_retry_budget_state END,
        version = budget.version + 1,
        updated_at = p_fenced_at
    FROM public.stage_runs AS run
    WHERE run.id = budget.stage_run_id
      AND run.attempt_id = v_attempt.id
      AND budget.state = 'ACTIVE';
    UPDATE public.attempt_retry_budgets AS budget
    SET state = 'CANCELED', version = budget.version + 1,
        updated_at = p_fenced_at
    WHERE budget.attempt_id = v_attempt.id AND budget.state = 'ACTIVE';
    UPDATE public.stage_storage_reservations AS reservation
    SET state = 'RELEASED', updated_at = p_fenced_at
    WHERE reservation.attempt_id = v_attempt.id
      AND reservation.state = 'RESERVED';
    UPDATE public.attempts AS attempt
    SET state = 'FAILED', graph_state = 'FAILED',
        ended_at = p_fenced_at, updated_at = p_fenced_at
    WHERE attempt.id = v_attempt.id
      AND attempt.fence = v_attempt.fence
      AND attempt.graph_state = v_attempt.graph_state;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'model_runtime_assigned_stage_fence_stale',
            MESSAGE = 'Attempt changed during ModelRuntime epoch fencing';
    END IF;
    UPDATE public.jobs AS job
    SET state = 'FAILED', version = job.version + 1,
        updated_at = p_fenced_at
    WHERE job.id = v_job.id AND job.version = v_job.version
      AND job.current_fence = v_job.current_fence
      AND job.state = v_job.state;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'model_runtime_assigned_stage_fence_stale',
            MESSAGE = 'Job changed during ModelRuntime epoch fencing';
    END IF;

    IF v_billable THEN
        UPDATE public.projects AS project
        SET running_count = project.running_count - 1
        WHERE project.id = v_job.project_id
          AND project.organization_id = v_job.organization_id
          AND project.running_count > 0;
    ELSE
        UPDATE public.projects AS project
        SET queued_count = project.queued_count - 1,
            retry_wait_count = project.retry_wait_count
                - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END
        WHERE project.id = v_job.project_id
          AND project.organization_id = v_job.organization_id
          AND project.queued_count > 0
          AND (v_job.state <> 'RETRY_WAIT' OR project.retry_wait_count > 0);
    END IF;
    UPDATE public.credit_reservations AS reservation
    SET state = 'RELEASED', updated_at = p_fenced_at
    WHERE reservation.id = v_credit.id AND reservation.state = 'RESERVED';
    UPDATE public.organization_credit_accounts AS account
    SET reserved_minor = account.reserved_minor - v_credit.amount_minor,
        version = account.version + 1,
        updated_at = p_fenced_at
    WHERE account.organization_id = v_job.organization_id
      AND account.currency = v_credit.currency
      AND account.reserved_minor >= v_credit.amount_minor;
END
$$;
ALTER FUNCTION vela_fence_assigned_stage_for_runtime_epoch(
    uuid, uuid, bigint, bigint, timestamptz
) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_fence_assigned_stage_for_runtime_epoch(
    uuid, uuid, bigint, bigint, timestamptz
) FROM PUBLIC;

CREATE FUNCTION vela_register_stage_worker_runtime(p_evidence jsonb)
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
    v_residency public.model_residencies%ROWTYPE;
    v_current_evidence jsonb;
    v_current_members jsonb;
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_worker_member_id uuid;
    v_worker_member_epoch bigint;
    v_new_epoch bigint;
    v_readiness_digest bytea;
    v_spiffe_digest bytea;
    v_prevalidated boolean;
    v_verified boolean;
    v_matched boolean;
    v_expected_member_count integer;
    v_now timestamptz := clock_timestamp();
    v_active record;
    v_consumed_resource_units bigint;
    v_failure_command jsonb;
BEGIN
    IF p_evidence IS NULL OR jsonb_typeof(p_evidence) <> 'object'
       OR (p_evidence ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_runtime_epoch_registration_invalid',
            MESSAGE = 'ModelRuntime epoch registration is invalid';
    END IF;

    v_worker_instance_id := (p_evidence ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_evidence ->> 'worker_instance_epoch')::bigint;
    v_worker_member_id := (p_evidence ->> 'worker_member_id')::uuid;
    v_worker_member_epoch := (p_evidence ->> 'worker_member_epoch')::bigint;
    v_new_epoch := (p_evidence ->> 'model_runtime_epoch')::bigint;
    v_readiness_digest := decode(p_evidence ->> 'readiness_evidence_digest', 'hex');
    v_spiffe_digest := decode(p_evidence ->> 'spiffe_id_digest', 'hex');
    IF v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR v_worker_member_id IS NULL OR v_worker_member_epoch <= 0
       OR v_new_epoch <= 0 OR octet_length(v_readiness_digest) <> 32
       OR octet_length(v_spiffe_digest) <> 32
       OR jsonb_typeof(p_evidence -> 'devices') <> 'array'
       OR jsonb_typeof(p_evidence -> 'members') <> 'array'
       OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements(p_evidence -> 'devices') AS requested(device)
           GROUP BY requested.device ->> 'device_id'
           HAVING count(*) > 1
       )
       OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements(p_evidence -> 'members') AS requested(member)
           GROUP BY requested.member ->> 'worker_member_id'
           HAVING count(*) > 1
       )
       OR EXISTS (
           SELECT 1
           FROM jsonb_array_elements(p_evidence -> 'members') AS requested(member)
           WHERE (requested.member ->> 'model_runtime_epoch')::bigint
                 IS DISTINCT FROM v_new_epoch
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'model_runtime_epoch_registration_invalid',
            MESSAGE = 'ModelRuntime epoch registration fields are invalid';
    END IF;

    PERFORM public.vela_lock_model_runtime_epoch_gate(
        (p_evidence ->> 'model_residency_id')::uuid
    );
    SELECT residency.* INTO v_residency
    FROM public.model_residencies AS residency
    WHERE residency.id = (p_evidence ->> 'model_residency_id')::uuid
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT
            v_worker_instance_id, v_worker_instance_epoch, false,
            'ModelRuntime residency is not durable Fleet authority'::text;
        RETURN;
    END IF;

    SELECT COALESCE(jsonb_agg(
        jsonb_set(member.value, '{model_runtime_epoch}',
            to_jsonb(v_residency.model_runtime_epoch), true)
        ORDER BY member.ordinality
    ), '[]'::jsonb)
    INTO v_current_members
    FROM jsonb_array_elements(p_evidence -> 'members')
         WITH ORDINALITY AS member(value, ordinality);
    v_current_evidence := jsonb_set(
        jsonb_set(
            p_evidence,
            '{model_runtime_epoch}',
            to_jsonb(v_residency.model_runtime_epoch),
            true
        ),
        '{members}',
        v_current_members,
        true
    );
    SELECT verified.ready INTO v_prevalidated
    FROM public.vela_verify_stage_worker_registration(v_current_evidence) AS verified;
    IF NOT COALESCE(v_prevalidated, false) OR v_new_epoch < v_residency.model_runtime_epoch THEN
        RETURN QUERY SELECT
            v_worker_instance_id, v_worker_instance_epoch, false,
            'ModelRuntime epoch evidence does not match durable Fleet authority'::text;
        RETURN;
    END IF;

    SELECT worker.desired_member_count INTO v_expected_member_count
    FROM public.worker_instances AS worker
    WHERE worker.id = v_residency.worker_instance_id
      AND worker.instance_epoch = v_residency.worker_instance_epoch;
    IF v_expected_member_count IS DISTINCT FROM 1 THEN
        RETURN QUERY SELECT
            v_worker_instance_id, v_worker_instance_epoch, false,
            'Multi-member ModelRuntime registration requires a gang coordinator'::text;
        RETURN;
    END IF;

    INSERT INTO public.model_runtime_epoch_registrations (
        model_residency_id, model_runtime_epoch,
        worker_instance_id, worker_instance_epoch,
        worker_member_id, worker_member_epoch,
        readiness_evidence_digest, spiffe_id_digest, registered_at
    ) VALUES (
        v_residency.id, v_new_epoch,
        v_worker_instance_id, v_worker_instance_epoch,
        v_worker_member_id, v_worker_member_epoch,
        v_readiness_digest, v_spiffe_digest, v_now
    )
    ON CONFLICT (model_residency_id, model_runtime_epoch, worker_member_id) DO UPDATE SET
        registered_at = public.model_runtime_epoch_registrations.registered_at
    WHERE public.model_runtime_epoch_registrations.worker_instance_id = EXCLUDED.worker_instance_id
      AND public.model_runtime_epoch_registrations.worker_instance_epoch = EXCLUDED.worker_instance_epoch
      AND public.model_runtime_epoch_registrations.worker_member_id = EXCLUDED.worker_member_id
      AND public.model_runtime_epoch_registrations.worker_member_epoch = EXCLUDED.worker_member_epoch
      AND public.model_runtime_epoch_registrations.readiness_evidence_digest = EXCLUDED.readiness_evidence_digest
      AND public.model_runtime_epoch_registrations.spiffe_id_digest = EXCLUDED.spiffe_id_digest
    RETURNING true INTO v_matched;
    IF NOT COALESCE(v_matched, false) THEN
        RETURN QUERY SELECT
            v_worker_instance_id, v_worker_instance_epoch, false,
            'ModelRuntime epoch registration replay does not match'::text;
        RETURN;
    END IF;

    IF v_new_epoch > v_residency.model_runtime_epoch THEN
        FOR v_active IN
            SELECT lease.id AS lease_id,
                   lease.attempt_id,
                   lease.stage_run_id,
                   lease.stage_attempt_id,
                   lease.attempt_fence,
                   lease.stage_fence,
                   run.version AS stage_version,
                   physical.state AS physical_state,
                   physical.started_at,
                   allocation.capacity_vector
            FROM public.stage_leases AS lease
            JOIN public.attempts AS attempt ON attempt.id = lease.attempt_id
            JOIN public.jobs AS job ON job.id = attempt.job_id
            JOIN public.stage_runs AS run ON run.id = lease.stage_run_id
            JOIN public.stage_attempts AS physical ON physical.id = lease.stage_attempt_id
            JOIN public.stage_allocations AS allocation ON allocation.id = lease.stage_allocation_id
            WHERE lease.model_residency_id = v_residency.id
              AND lease.model_runtime_epoch = v_residency.model_runtime_epoch
              AND lease.state = 'ACTIVE'
            ORDER BY job.id, attempt.id, run.id, lease.id
            FOR UPDATE OF job, attempt, run, physical, lease, allocation
        LOOP
            IF v_active.physical_state = 'ASSIGNED' THEN
                PERFORM public.vela_fence_assigned_stage_for_runtime_epoch(
                    v_active.lease_id,
                    v_residency.id,
                    v_residency.model_runtime_epoch,
                    v_new_epoch,
                    v_now
                );
                CONTINUE;
            END IF;
            IF v_active.started_at IS NULL THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'model_runtime_active_stage_start_missing',
                    MESSAGE = 'Active Stage has no Billable Start for runtime epoch fencing';
            END IF;
            v_consumed_resource_units := GREATEST(
                1,
                CEIL(EXTRACT(EPOCH FROM (v_now - v_active.started_at)))::bigint
                * GREATEST(
                    1,
                    COALESCE((v_active.capacity_vector ->> 'gpu_count')::bigint, 1)
                )
            );
            v_failure_command := jsonb_build_object(
                'schema_version', 1,
                'command_kind', 'FAIL',
                'command_id', gen_random_uuid(),
                'attempt_id', v_active.attempt_id,
                'stage_run_id', v_active.stage_run_id,
                'expected_attempt_fence', v_active.attempt_fence,
                'expected_stage_fence', v_active.stage_fence,
                'expected_stage_version', v_active.stage_version,
                'stage_attempt_id', v_active.stage_attempt_id,
                'stage_lease_id', v_active.lease_id,
                'failure_class', 'WORKER_LOST',
                'failure_fingerprint', encode(pg_catalog.sha256(convert_to(
                    v_residency.id::text || ':' || v_residency.model_runtime_epoch::text
                    || ':' || v_new_epoch::text,
                    'UTF8'
                )), 'hex'),
                'consumed_resource_units', v_consumed_resource_units,
                'failed_at', v_now,
                'retry_at', v_now + interval '1 second'
            );
            PERFORM * FROM public.vela_apply_stage_command(v_failure_command);
        END LOOP;

        UPDATE public.model_residencies AS residency
        SET model_runtime_epoch = v_new_epoch,
            state = 'READY',
            ready_at = v_now,
            observed_at = v_now,
            observed_by = 'stage-worker-runtime/' || v_worker_member_id::text
        WHERE residency.id = v_residency.id
          AND residency.model_runtime_epoch = v_residency.model_runtime_epoch;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'model_runtime_epoch_registration_raced',
                MESSAGE = 'ModelRuntime epoch authority changed during registration';
        END IF;
    ELSE
        UPDATE public.model_residencies AS residency
        SET observed_at = GREATEST(residency.observed_at, v_now),
            observed_by = 'stage-worker-runtime/' || v_worker_member_id::text
        WHERE residency.id = v_residency.id;
    END IF;

    SELECT residency.model_runtime_epoch = v_new_epoch
           AND residency.state = 'READY'
           AND EXISTS (
               SELECT 1
               FROM public.model_runtime_epoch_registrations AS registration
               WHERE registration.model_residency_id = residency.id
                 AND registration.model_runtime_epoch = v_new_epoch
                 AND registration.worker_instance_id = residency.worker_instance_id
                 AND registration.worker_instance_epoch = residency.worker_instance_epoch
                 AND registration.worker_member_id = v_worker_member_id
                 AND registration.worker_member_epoch = v_worker_member_epoch
                 AND registration.spiffe_id_digest = v_spiffe_digest
           )
    INTO v_verified
    FROM public.model_residencies AS residency
    WHERE residency.id = v_residency.id;
    IF NOT COALESCE(v_verified, false) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'model_runtime_epoch_registration_postcheck_failed',
            MESSAGE = 'ModelRuntime epoch registration failed durable postcheck';
    END IF;
    RETURN QUERY SELECT
        v_worker_instance_id, v_worker_instance_epoch, true,
        'ModelRuntime epoch registered as durable Fleet authority'::text;
END
$$;
ALTER FUNCTION vela_register_stage_worker_runtime(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_register_stage_worker_runtime(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_register_stage_worker_runtime(jsonb)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE EXECUTE ON FUNCTION vela_register_stage_worker_runtime(jsonb)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_register_stage_worker_runtime(jsonb);
DROP FUNCTION vela_fence_assigned_stage_for_runtime_epoch(
    uuid, uuid, bigint, bigint, timestamptz
);
DO $$
DECLARE
    v_definition text;
    v_old text;
    v_new text;
BEGIN
    v_definition := pg_get_functiondef(
        'public.vela_apply_stage_command(jsonb)'::regprocedure
    );
    v_old := E'    v_stage_run_id := (p_command ->> ''stage_run_id'')::uuid;\n'
        || E'    IF v_kind = ''ASSIGN'' THEN\n'
        || E'        v_residency_id := (p_command ->> ''model_residency_id'')::uuid;\n'
        || E'    ELSIF v_kind IN (''START'', ''COMPLETE'', ''FAIL'') THEN\n'
        || E'        SELECT lease.model_residency_id INTO v_residency_id\n'
        || E'        FROM public.stage_leases AS lease\n'
        || E'        WHERE lease.id = (p_command ->> ''stage_lease_id'')::uuid;\n'
        || E'    END IF;\n'
        || E'    IF v_residency_id IS NOT NULL THEN\n'
        || E'        PERFORM public.vela_lock_model_runtime_epoch_gate(v_residency_id);\n'
        || E'    END IF;\n'
        || E'    IF v_kind = ''FAIL'' THEN\n';
    v_new := E'    v_stage_run_id := (p_command ->> ''stage_run_id'')::uuid;\n'
        || E'    IF v_kind = ''FAIL'' THEN\n';
    IF strpos(v_definition, v_old) = 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'model_runtime_epoch_stage_command_dependency_drift',
            MESSAGE = 'vela_apply_stage_command epoch gate changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
DROP FUNCTION vela_lock_model_runtime_epoch_gate(uuid);
REVOKE SELECT, UPDATE ON model_residencies FROM vela_attempt_coordinator_owner;
DROP TABLE model_runtime_epoch_registrations;
-- +goose StatementEnd
