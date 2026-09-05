-- +goose Up
-- +goose StatementBegin
GRANT SELECT ON public.non_content_job_roots, public.non_content_attempt_roots,
    public.device_set_members
    TO vela_attempt_coordinator_owner;

CREATE FUNCTION public.vela_read_stage_terminal_history(p_request jsonb)
RETURNS jsonb
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_lease public.stage_leases%ROWTYPE;
    v_run public.stage_runs%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_epoch public.worker_instance_epochs%ROWTYPE;
    v_job public.non_content_job_roots%ROWTYPE;
    v_member_id uuid;
    v_budget integer;
    v_count bigint;
    v_min integer;
    v_max integer;
    v_cutoff bigint;
    v_allocation record;
    v_barrier public.model_runtime_barriers%ROWTYPE;
    v_members jsonb;
    v_devices jsonb;
    v_allocations jsonb := '[]'::jsonb;
    v_assignment_wire bytea;
    v_renewal_wire bytea;
    v_authority_digest bytea;
    v_spiffe_digest bytea;
    v_no jsonb := jsonb_build_object('schema_version', 1, 'eligible', false);
BEGIN
    IF p_request IS NULL OR jsonb_typeof(p_request) <> 'object'
       OR (p_request ->> 'schema_version')::integer IS DISTINCT FROM 1
       OR COALESCE((p_request ->> 'worker_instance_epoch')::bigint, 0) <= 0
       OR COALESCE((p_request ->> 'control_session_epoch')::bigint, 0) <= 0
       OR COALESCE((p_request ->> 'execution_sequence')::bigint, 0) <= 0 THEN
        RETURN v_no || jsonb_build_object('reason', 'INVALID_REQUEST');
    END IF;
    v_authority_digest := decode(p_request ->> 'authority_digest', 'hex');
    v_spiffe_digest := decode(p_request ->> 'spiffe_id_digest', 'hex');
    IF octet_length(v_authority_digest) IS DISTINCT FROM 32
       OR octet_length(v_spiffe_digest) IS DISTINCT FROM 32 THEN
        RETURN v_no || jsonb_build_object('reason', 'INVALID_REQUEST');
    END IF;

    SELECT lease.* INTO v_lease
    FROM public.stage_leases AS lease
    JOIN public.stage_allocations AS allocation
      ON allocation.id = lease.stage_allocation_id
     AND allocation.stage_attempt_id = lease.stage_attempt_id
     AND allocation.stage_run_id = lease.stage_run_id
     AND allocation.attempt_id = lease.attempt_id
    JOIN public.stage_attempts AS physical
      ON physical.id = lease.stage_attempt_id
     AND physical.stage_run_id = lease.stage_run_id
     AND physical.attempt_id = lease.attempt_id
    WHERE lease.id = (p_request ->> 'stage_lease_id')::uuid
      AND lease.stage_allocation_id = (p_request ->> 'stage_allocation_id')::uuid
      AND lease.stage_attempt_id = (p_request ->> 'stage_attempt_id')::uuid
      AND lease.stage_run_id = (p_request ->> 'stage_run_id')::uuid
      AND lease.attempt_id = (p_request ->> 'attempt_id')::uuid
      AND lease.worker_instance_id = (p_request ->> 'worker_instance_id')::uuid
      AND lease.worker_instance_epoch = (p_request ->> 'worker_instance_epoch')::bigint
      AND lease.token_digest = decode(p_request ->> 'token_digest', 'hex')
      AND lease.model_residency_id = (p_request ->> 'model_residency_id')::uuid
      AND lease.model_runtime_epoch = (p_request ->> 'model_runtime_barrier_generation')::bigint
      AND physical.selected_stage_profile_revision_id = (p_request ->> 'stage_profile_revision_id')::uuid
      AND allocation.execution_sequence = (p_request ->> 'execution_sequence')::bigint;
    IF NOT FOUND THEN
        RETURN v_no || jsonb_build_object('reason', 'HISTORICAL_IDENTITY_UNAVAILABLE');
    END IF;
    SELECT run.* INTO v_run FROM public.stage_runs AS run WHERE run.id = v_lease.stage_run_id;
    SELECT job.* INTO v_job
    FROM public.non_content_attempt_roots AS attempt
    JOIN public.non_content_job_roots AS job
      ON job.id = attempt.job_id
     AND job.organization_id = attempt.organization_id
     AND job.project_id = attempt.project_id
    WHERE attempt.id = v_run.attempt_id
      AND attempt.organization_id = v_run.organization_id
      AND attempt.project_id = v_run.project_id
      AND job.id = (p_request ->> 'job_id')::uuid;
    IF NOT FOUND THEN
        RETURN v_no || jsonb_build_object('reason', 'HISTORICAL_IDENTITY_UNAVAILABLE');
    END IF;
    SELECT worker.* INTO v_worker FROM public.worker_instances AS worker
    WHERE worker.id = v_lease.worker_instance_id
      AND worker.instance_epoch = v_lease.worker_instance_epoch
      AND worker.control_session_epoch = (p_request ->> 'control_session_epoch')::bigint;
    IF NOT FOUND THEN
        RETURN v_no || jsonb_build_object('reason', 'WORKER_SESSION_CHANGED');
    END IF;
    SELECT epoch.* INTO v_epoch FROM public.worker_instance_epochs AS epoch
    WHERE epoch.worker_instance_id = v_worker.id AND epoch.epoch = v_worker.instance_epoch
      AND epoch.device_set_digest = v_lease.device_set_digest
      AND epoch.membership_digest = v_lease.membership_digest;
    IF NOT FOUND THEN
        RETURN v_no || jsonb_build_object('reason', 'WORKER_TOPOLOGY_UNAVAILABLE');
    END IF;
    SELECT member.id INTO v_member_id
    FROM public.worker_members AS member
    JOIN public.model_runtime_barriers AS barrier
      ON barrier.model_residency_id = v_lease.model_residency_id
     AND barrier.barrier_generation = v_lease.model_runtime_epoch
     AND barrier.leader_worker_member_id = member.id
    WHERE member.worker_instance_id = v_worker.id
      AND member.worker_instance_epoch = v_worker.instance_epoch
      AND member.identity_digest = v_spiffe_digest;
    IF NOT FOUND THEN
        RETURN v_no || jsonb_build_object('reason', 'CALLER_NOT_HISTORICAL_LEADER');
    END IF;
    IF v_run.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED') THEN
        RETURN v_no || jsonb_build_object('reason', 'STAGE_RUN_NOT_TERMINAL');
    END IF;

    -- Check the whole StageRun before restricting the set to this Worker.
    SELECT budget.attempts_consumed INTO v_budget
    FROM public.stage_retry_budgets AS budget WHERE budget.stage_run_id = v_run.id;
    SELECT count(*), min(physical_attempt_number), max(physical_attempt_number)
      INTO v_count, v_min, v_max
    FROM public.stage_attempts WHERE stage_run_id = v_run.id;
    IF v_budget IS NULL OR v_budget <= 0 OR v_budget > 256
       OR v_count <> v_budget OR v_min <> 1 OR v_max <> v_budget THEN
        RETURN v_no || jsonb_build_object('reason', 'ATTEMPT_HISTORY_INCOMPLETE');
    END IF;
    SELECT count(*) INTO v_count FROM public.attempt_coordinator_commands AS command
    WHERE command.stage_run_id = v_run.id AND command.command_kind = 'ASSIGN';
    IF v_count <> v_budget OR EXISTS (
        SELECT 1 FROM public.stage_attempts AS physical
        LEFT JOIN public.stage_allocations AS allocation ON allocation.stage_attempt_id = physical.id
        WHERE physical.stage_run_id = v_run.id
          AND (allocation.id IS NULL
               OR allocation.stage_run_id <> physical.stage_run_id
               OR allocation.attempt_id <> physical.attempt_id
               OR physical.attempt_id <> v_run.attempt_id
               OR physical.organization_id <> v_run.organization_id
               OR physical.project_id <> v_run.project_id
               OR (SELECT count(*) FROM public.attempt_coordinator_commands AS command
                   WHERE command.stage_run_id = v_run.id AND command.command_kind = 'ASSIGN'
                     AND command.attempt_id = v_run.attempt_id AND command.job_id = v_job.id
                     AND command.result ->> 'stage_attempt_id' = physical.id::text) <> 1
               OR (SELECT count(*) FROM public.stage_leases AS lease
                   WHERE lease.stage_attempt_id = physical.id) <> 1
               OR NOT EXISTS (
                   SELECT 1 FROM public.stage_leases AS lease
                   WHERE lease.stage_attempt_id = physical.id
                     AND lease.stage_allocation_id = allocation.id
                     AND lease.stage_run_id = allocation.stage_run_id
                     AND lease.attempt_id = allocation.attempt_id
                     AND lease.worker_instance_id = allocation.worker_instance_id
                     AND lease.worker_instance_epoch = allocation.worker_instance_epoch
                     AND lease.device_set_digest = allocation.device_set_digest
                     AND lease.membership_digest = allocation.membership_digest
                     AND lease.model_residency_id = allocation.model_residency_id
                     AND lease.model_runtime_epoch = allocation.model_runtime_epoch))
    ) THEN
        RETURN v_no || jsonb_build_object('reason', 'ALLOCATION_HISTORY_INCOMPLETE');
    END IF;

    SELECT max(execution_sequence) INTO v_cutoff FROM public.stage_allocations
    WHERE stage_run_id = v_run.id AND worker_instance_id = v_worker.id
      AND worker_instance_epoch = v_worker.instance_epoch;
    IF v_cutoff IS NULL OR EXISTS (
        SELECT 1 FROM public.stage_allocations
        WHERE stage_run_id = v_run.id AND worker_instance_id = v_worker.id
          AND worker_instance_epoch = v_worker.instance_epoch AND execution_sequence IS NULL
    ) THEN
        RETURN v_no || jsonb_build_object('reason', 'UNNUMBERED_ALLOCATION_HISTORY');
    END IF;
    SELECT jsonb_agg(jsonb_build_object('device_id', device_id, 'device_epoch', device_epoch)
                     ORDER BY device_id) INTO v_devices
    FROM public.device_set_members WHERE device_set_id = v_epoch.device_set_id;
    IF v_devices IS NULL OR jsonb_array_length(v_devices) <> v_worker.desired_device_count
       OR jsonb_array_length(v_devices) > 64 THEN
        RETURN v_no || jsonb_build_object('reason', 'WORKER_TOPOLOGY_UNAVAILABLE');
    END IF;

    FOR v_allocation IN
        SELECT allocation.*, physical.selected_stage_profile_revision_id,
               lease.id AS stage_lease_id, lease.execution_nonce,
               residency.runtime_identity, residency.worker_instance_id AS residency_worker_id,
               residency.worker_instance_epoch AS residency_worker_epoch
        FROM public.stage_allocations AS allocation
        JOIN public.stage_attempts AS physical ON physical.id = allocation.stage_attempt_id
        JOIN public.stage_leases AS lease ON lease.stage_allocation_id = allocation.id
        LEFT JOIN public.model_residencies AS residency ON residency.id = allocation.model_residency_id
        WHERE allocation.stage_run_id = v_run.id AND allocation.worker_instance_id = v_worker.id
          AND allocation.worker_instance_epoch = v_worker.instance_epoch
        ORDER BY allocation.execution_sequence
    LOOP
        IF v_allocation.runtime_identity IS NULL
           OR v_allocation.residency_worker_id <> v_worker.id
           OR v_allocation.residency_worker_epoch <> v_worker.instance_epoch
           OR v_allocation.device_set_digest <> v_epoch.device_set_digest
           OR v_allocation.membership_digest <> v_epoch.membership_digest THEN
            RETURN v_no || jsonb_build_object('reason', 'RUNTIME_HISTORY_INCOMPLETE');
        END IF;
        SELECT barrier.* INTO v_barrier FROM public.model_runtime_barriers AS barrier
        WHERE barrier.model_residency_id = v_allocation.model_residency_id
          AND barrier.barrier_generation = v_allocation.model_runtime_epoch
          AND barrier.worker_instance_id = v_worker.id
          AND barrier.worker_instance_epoch = v_worker.instance_epoch;
        IF NOT FOUND OR v_barrier.expected_member_count <> v_worker.desired_member_count
           OR v_barrier.expected_member_count > 64 THEN
            RETURN v_no || jsonb_build_object('reason', 'RUNTIME_HISTORY_INCOMPLETE');
        END IF;
        SELECT count(*) INTO v_count FROM public.model_runtime_epoch_registrations
        WHERE model_residency_id = v_barrier.model_residency_id
          AND barrier_generation = v_barrier.barrier_generation;
        IF v_count <> v_barrier.expected_member_count OR EXISTS (
            SELECT 1 FROM public.model_runtime_epoch_registrations AS registration
            LEFT JOIN public.worker_members AS member ON member.id = registration.worker_member_id
            WHERE registration.model_residency_id = v_barrier.model_residency_id
              AND registration.barrier_generation = v_barrier.barrier_generation
              AND (registration.worker_instance_id <> v_worker.id
                   OR registration.worker_instance_epoch <> v_worker.instance_epoch
                   OR member.id IS NULL OR member.worker_instance_id <> v_worker.id
                   OR member.worker_instance_epoch <> v_worker.instance_epoch
                   OR member.member_epoch <> registration.worker_member_epoch
                   OR member.identity_digest <> registration.spiffe_id_digest
                   OR member.device_subset_digest <> registration.device_subset_digest)
        ) THEN
            RETURN v_no || jsonb_build_object('reason', 'RUNTIME_HISTORY_INCOMPLETE');
        END IF;
        SELECT jsonb_agg(jsonb_build_object(
            'worker_member_id', worker_member_id, 'member_epoch', worker_member_epoch,
            'model_runtime_epoch', local_model_runtime_epoch,
            'identity_digest', encode(spiffe_id_digest, 'hex'),
            'device_subset_digest', encode(device_subset_digest, 'hex')
        ) ORDER BY worker_member_id) INTO v_members
        FROM public.model_runtime_epoch_registrations
        WHERE model_residency_id = v_barrier.model_residency_id
          AND barrier_generation = v_barrier.barrier_generation;
        v_allocations := v_allocations || jsonb_build_array(jsonb_build_object(
            'stage_attempt_id', v_allocation.stage_attempt_id,
            'stage_allocation_id', v_allocation.id, 'stage_lease_id', v_allocation.stage_lease_id,
            'execution_sequence', v_allocation.execution_sequence,
            'execution_nonce', encode(v_allocation.execution_nonce, 'hex'),
            'model_residency_id', v_allocation.model_residency_id,
            'model_runtime_identity', v_allocation.runtime_identity,
            'barrier_generation', v_allocation.model_runtime_epoch,
            'stage_profile_revision_id', v_allocation.selected_stage_profile_revision_id,
            'members', v_members
        ));
    END LOOP;
    IF octet_length(v_allocations::text) > 2097152 THEN
        RETURN v_no || jsonb_build_object('reason', 'RUNTIME_HISTORY_TOO_LARGE');
    END IF;

    SELECT renewal.renewed_authority INTO v_renewal_wire
    FROM public.stage_authority_renewals AS renewal
    WHERE renewal.stage_lease_id = v_lease.id
      AND renewal.stage_run_id = v_run.id AND renewal.stage_attempt_id = v_lease.stage_attempt_id
      AND renewal.authority_digest = v_authority_digest;
    IF v_renewal_wire IS NULL AND NULLIF(p_request ->> 'acquire_command_id', '') IS NOT NULL THEN
        -- This wire is an internal candidate, never an authenticated disposition.
        -- Go must decode it and match the full canonical signed authority.
        SELECT result.assignment_wire INTO v_assignment_wire
        FROM public.stage_worker_acquire_intents AS intent
        JOIN public.stage_worker_acquire_results AS result ON result.command_id = intent.command_id
        WHERE intent.command_id = (p_request ->> 'acquire_command_id')::uuid
          AND result.result_kind = 'ASSIGNMENT'
          AND intent.worker_instance_id = v_worker.id
          AND intent.worker_instance_epoch = v_worker.instance_epoch
          AND intent.model_residency_id = v_lease.model_residency_id
          AND intent.model_runtime_epoch = v_lease.model_runtime_epoch
          AND intent.stage_profile_revision_id = (p_request ->> 'stage_profile_revision_id')::uuid
          AND intent.capacity_observation_sequence = (p_request ->> 'capacity_observation_sequence')::bigint
          AND intent.spiffe_id_digest = v_spiffe_digest;
    END IF;
    IF v_renewal_wire IS NULL AND v_assignment_wire IS NULL THEN
        RETURN v_no || jsonb_build_object('reason', 'SIGNED_HISTORY_UNAVAILABLE');
    END IF;
    RETURN jsonb_build_object(
        'schema_version', 1, 'eligible', true, 'reason', 'HISTORY_COMPLETE',
        'organization_id', v_job.organization_id, 'project_id', v_job.project_id,
        'job_id', v_job.id, 'attempt_id', v_run.attempt_id, 'stage_run_id', v_run.id,
        'terminal_state', v_run.state, 'stage_fence', v_run.fence, 'stage_version', v_run.version,
        'worker_instance_id', v_worker.id, 'worker_instance_epoch', v_worker.instance_epoch,
        'worker_member_id', v_member_id, 'control_session_epoch', v_worker.control_session_epoch,
        'device_set_digest', encode(v_epoch.device_set_digest, 'hex'),
        'membership_digest', encode(v_epoch.membership_digest, 'hex'), 'devices', v_devices,
        'cutoff', v_cutoff, 'allocations', v_allocations,
        'assignment_wire', encode(v_assignment_wire, 'hex'),
        'renewal_wire', encode(v_renewal_wire, 'hex'), 'observed_at', statement_timestamp()
    );
END
$$;
ALTER FUNCTION public.vela_read_stage_terminal_history(jsonb) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION public.vela_read_stage_terminal_history(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.vela_read_stage_terminal_history(jsonb) TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP FUNCTION public.vela_read_stage_terminal_history(jsonb);
REVOKE SELECT ON public.non_content_job_roots, public.non_content_attempt_roots,
    public.device_set_members
    FROM vela_attempt_coordinator_owner;
-- +goose StatementEnd
