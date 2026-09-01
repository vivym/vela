-- +goose Up
-- +goose StatementBegin
ALTER FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    RENAME TO vela_read_stage_worker_acquire_authority_v1;
REVOKE EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority_v1(uuid)
    FROM vela_stage_worker_control;

CREATE FUNCTION vela_read_stage_worker_acquire_authority(p_command_id uuid)
RETURNS TABLE (decision text, reason text, authority jsonb)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_decision text;
    v_reason text;
    v_authority jsonb;
    v_intent stage_worker_acquire_intents%ROWTYPE;
    v_barrier model_runtime_barriers%ROWTYPE;
    v_caller_worker_member_id uuid;
    v_members jsonb;
BEGIN
    SELECT legacy.decision, legacy.reason, legacy.authority
      INTO v_decision, v_reason, v_authority
    FROM vela_read_stage_worker_acquire_authority_v1(p_command_id) AS legacy;
    IF v_decision IS DISTINCT FROM 'AUTHORIZED' THEN
        RETURN QUERY SELECT v_decision, v_reason, v_authority;
        RETURN;
    END IF;
    SELECT intent.* INTO STRICT v_intent
    FROM stage_worker_acquire_intents AS intent
    WHERE intent.command_id = p_command_id;
    SELECT member.id INTO v_caller_worker_member_id
    FROM worker_members AS member
    WHERE member.worker_instance_id = v_intent.worker_instance_id
      AND member.worker_instance_epoch = v_intent.worker_instance_epoch
      AND member.identity_digest = v_intent.spiffe_id_digest
      AND member.readiness = 'READY';
    SELECT barrier.* INTO v_barrier
    FROM model_runtime_barriers AS barrier
    JOIN model_residencies AS residency
      ON residency.id = barrier.model_residency_id
     AND residency.worker_instance_id = barrier.worker_instance_id
     AND residency.worker_instance_epoch = barrier.worker_instance_epoch
     AND residency.model_runtime_epoch = barrier.barrier_generation
     AND residency.state = 'READY'
    WHERE barrier.model_residency_id = v_intent.model_residency_id
      AND barrier.barrier_generation = v_intent.model_runtime_epoch
      AND barrier.worker_instance_id = v_intent.worker_instance_id
      AND barrier.worker_instance_epoch = v_intent.worker_instance_epoch
      AND barrier.state = 'READY';
    IF v_barrier.model_residency_id IS NULL THEN
        RETURN QUERY SELECT
            'STALE'::text,
            'ModelRuntime gang barrier is not current'::text,
            NULL::jsonb;
        RETURN;
    END IF;
    IF v_caller_worker_member_id IS NULL
       OR v_caller_worker_member_id <> v_barrier.leader_worker_member_id THEN
        RETURN QUERY SELECT
            'REJECTED'::text,
            'Only the certified gang leader may acquire Stage work'::text,
            NULL::jsonb;
        RETURN;
    END IF;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'worker_member_id', member.id,
        'member_epoch', member.member_epoch,
        'model_runtime_epoch', registration.local_model_runtime_epoch
    ) ORDER BY member.id), '[]'::jsonb) INTO v_members
    FROM model_runtime_epoch_registrations AS registration
    JOIN worker_members AS member
      ON member.id = registration.worker_member_id
     AND member.worker_instance_id = registration.worker_instance_id
     AND member.worker_instance_epoch = registration.worker_instance_epoch
     AND member.member_epoch = registration.worker_member_epoch
     AND member.identity_digest = registration.spiffe_id_digest
     AND member.device_subset_digest = registration.device_subset_digest
     AND member.readiness = 'READY'
    WHERE registration.model_residency_id = v_barrier.model_residency_id
      AND registration.barrier_generation = v_barrier.barrier_generation
      AND registration.worker_instance_id = v_barrier.worker_instance_id
      AND registration.worker_instance_epoch = v_barrier.worker_instance_epoch;
    IF jsonb_array_length(v_members) <> v_barrier.expected_member_count THEN
        RETURN QUERY SELECT
            'STALE'::text,
            'ModelRuntime gang barrier membership is incomplete'::text,
            NULL::jsonb;
        RETURN;
    END IF;
    v_authority := jsonb_set(v_authority, '{members}', v_members, true);
    v_authority := jsonb_set(
        v_authority,
        '{model_runtime_barrier_generation}',
        to_jsonb(v_barrier.barrier_generation),
        true
    );
    RETURN QUERY SELECT v_decision, v_reason, v_authority;
END
$$;
ALTER FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_worker_acquire_authority(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    TO vela_stage_worker_control;

ALTER FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    RENAME TO vela_read_stage_assignment_execution_v1;
REVOKE EXECUTE ON FUNCTION vela_read_stage_assignment_execution_v1(uuid,uuid)
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
    v_barrier model_runtime_barriers%ROWTYPE;
    v_members jsonb;
BEGIN
    SELECT legacy.snapshot INTO STRICT v_snapshot
    FROM vela_read_stage_assignment_execution_v1(p_command_id, p_claim_id) AS legacy;
    SELECT barrier.* INTO v_barrier
    FROM model_runtime_barriers AS barrier
    JOIN model_residencies AS residency
      ON residency.id = barrier.model_residency_id
     AND residency.worker_instance_id = barrier.worker_instance_id
     AND residency.worker_instance_epoch = barrier.worker_instance_epoch
     AND residency.model_runtime_epoch = barrier.barrier_generation
     AND residency.state = 'READY'
    WHERE barrier.model_residency_id = (v_snapshot ->> 'model_residency_id')::uuid
      AND barrier.barrier_generation = (v_snapshot ->> 'model_runtime_epoch')::bigint
      AND barrier.worker_instance_id = (v_snapshot ->> 'worker_instance_id')::uuid
      AND barrier.worker_instance_epoch = (v_snapshot ->> 'worker_instance_epoch')::bigint
      AND barrier.state = 'READY';
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'worker_member_id', member.id,
        'member_epoch', member.member_epoch,
        'model_runtime_epoch', registration.local_model_runtime_epoch
    ) ORDER BY member.id), '[]'::jsonb) INTO v_members
    FROM model_runtime_epoch_registrations AS registration
    JOIN worker_members AS member
      ON member.id = registration.worker_member_id
     AND member.worker_instance_id = registration.worker_instance_id
     AND member.worker_instance_epoch = registration.worker_instance_epoch
     AND member.member_epoch = registration.worker_member_epoch
     AND member.identity_digest = registration.spiffe_id_digest
     AND member.device_subset_digest = registration.device_subset_digest
     AND member.readiness = 'READY'
    WHERE registration.model_residency_id = v_barrier.model_residency_id
      AND registration.barrier_generation = v_barrier.barrier_generation
      AND registration.worker_instance_id = v_barrier.worker_instance_id
      AND registration.worker_instance_epoch = v_barrier.worker_instance_epoch;
    IF v_barrier.model_residency_id IS NULL
       OR jsonb_array_length(v_members) <> v_barrier.expected_member_count THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_worker_acquire_execution_stale',
            MESSAGE = 'Stage Worker assignment ModelRuntime gang barrier is stale';
    END IF;
    v_snapshot := jsonb_set(v_snapshot, '{members}', v_members, true);
    v_snapshot := jsonb_set(
        v_snapshot,
        '{model_runtime_barrier_generation}',
        to_jsonb(v_barrier.barrier_generation),
        true
    );
    RETURN QUERY SELECT v_snapshot;
END
$$;
ALTER FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    TO vela_stage_worker_control;

CREATE FUNCTION vela_read_stage_authority_member_epochs(p_stage_lease_id uuid)
RETURNS TABLE (members jsonb)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'id', member.id,
        'epoch', member.member_epoch,
        'identity_digest', encode(member.identity_digest, 'hex'),
        'readiness', member.readiness::text,
        'model_runtime_epoch', registration.local_model_runtime_epoch
    ) ORDER BY member.id), '[]'::jsonb)
    FROM stage_leases AS lease
    JOIN model_runtime_barriers AS barrier
      ON barrier.model_residency_id = lease.model_residency_id
     AND barrier.barrier_generation = lease.model_runtime_epoch
     AND barrier.worker_instance_id = lease.worker_instance_id
     AND barrier.worker_instance_epoch = lease.worker_instance_epoch
     AND barrier.state = 'READY'
    JOIN model_runtime_epoch_registrations AS registration
      ON registration.model_residency_id = barrier.model_residency_id
     AND registration.barrier_generation = barrier.barrier_generation
     AND registration.worker_instance_id = barrier.worker_instance_id
     AND registration.worker_instance_epoch = barrier.worker_instance_epoch
    JOIN worker_members AS member
      ON member.id = registration.worker_member_id
     AND member.worker_instance_id = registration.worker_instance_id
     AND member.worker_instance_epoch = registration.worker_instance_epoch
     AND member.member_epoch = registration.worker_member_epoch
     AND member.identity_digest = registration.spiffe_id_digest
     AND member.device_subset_digest = registration.device_subset_digest
     AND member.readiness = 'READY'
    WHERE lease.id = p_stage_lease_id
    HAVING count(*) = max(barrier.expected_member_count);
$$;
ALTER FUNCTION vela_read_stage_authority_member_epochs(uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_authority_member_epochs(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_authority_member_epochs(uuid)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
REVOKE EXECUTE ON FUNCTION vela_read_stage_authority_member_epochs(uuid)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_read_stage_authority_member_epochs(uuid);

REVOKE EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_read_stage_assignment_execution(uuid,uuid);
ALTER FUNCTION vela_read_stage_assignment_execution_v1(uuid,uuid)
    RENAME TO vela_read_stage_assignment_execution;
GRANT EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    TO vela_stage_worker_control;

REVOKE EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_read_stage_worker_acquire_authority(uuid);
ALTER FUNCTION vela_read_stage_worker_acquire_authority_v1(uuid)
    RENAME TO vela_read_stage_worker_acquire_authority;
GRANT EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    TO vela_stage_worker_control;
-- +goose StatementEnd
