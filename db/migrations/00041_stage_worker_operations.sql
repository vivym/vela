-- +goose Up
-- +goose StatementBegin
ALTER TABLE stage_worker_commands
    DROP CONSTRAINT stage_worker_commands_command_kind_check,
    ADD CONSTRAINT stage_worker_commands_command_kind_check
        CHECK (command_kind IN ('START', 'HEARTBEAT', 'REATTACH'));

CREATE TABLE stage_worker_reattachments (
    command_id uuid PRIMARY KEY REFERENCES stage_worker_commands(command_id),
    stage_lease_id uuid NOT NULL REFERENCES stage_leases(id),
    stage_run_id uuid NOT NULL,
    stage_attempt_id uuid NOT NULL,
    control_session_epoch bigint NOT NULL CHECK (control_session_epoch > 0),
    observed_runtime_state text NOT NULL CHECK (
        observed_runtime_state IN (
            'PREPARING', 'PREPARED', 'RUNNING', 'CANCELING', 'STOPPED',
            'OUTPUT_READY', 'OUTPUT_SEALED', 'FAILED'
        )
    ),
    local_receipt_id text CHECK (
        local_receipt_id IS NULL OR length(local_receipt_id) BETWEEN 1 AND 1000
    ),
    local_receipt_digest bytea CHECK (
        local_receipt_digest IS NULL OR octet_length(local_receipt_digest) = 32
    ),
    reattached_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (stage_run_id, stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id),
    CHECK ((local_receipt_id IS NULL) = (local_receipt_digest IS NULL))
);

CREATE TRIGGER stage_worker_reattachments_immutable
BEFORE UPDATE OR DELETE ON stage_worker_reattachments
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_worker_evidence_mutation();

ALTER TABLE stage_worker_reattachments OWNER TO vela_attempt_coordinator_owner;

CREATE FUNCTION vela_read_stage_scheduler_claim(p_claim_id uuid)
RETURNS TABLE (
    claim_id uuid,
    decision_id uuid,
    claim_state text,
    command_payload jsonb
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT claim.id, claim.decision_id, claim.state::text, claim.command_payload
    FROM stage_scheduler_claims AS claim
    WHERE claim.id = p_claim_id;
$$;
REVOKE ALL ON FUNCTION vela_read_stage_scheduler_claim(uuid) FROM PUBLIC;
ALTER FUNCTION vela_read_stage_scheduler_claim(uuid)
    OWNER TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_read_stage_scheduler_claim(uuid)
    TO vela_stage_scheduler;

CREATE FUNCTION vela_verify_stage_worker_registration(p_evidence jsonb)
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
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_control_session_epoch bigint;
    v_model_residency_id uuid;
    v_model_runtime_epoch bigint;
    v_stage_profile_revision_id uuid;
    v_worker_member_id uuid;
    v_worker_member_epoch bigint;
    v_capacity_observation_sequence bigint;
    v_device_set_digest bytea;
    v_membership_digest bytea;
    v_spiffe_id_digest bytea;
    v_readiness_evidence_digest bytea;
    v_runtime_identity text;
    v_devices jsonb;
    v_members jsonb;
    v_ready boolean;
BEGIN
    IF p_evidence IS NULL OR jsonb_typeof(p_evidence) <> 'object'
       OR (p_evidence ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_registration_invalid',
            MESSAGE = 'Stage Worker registration evidence is invalid';
    END IF;
    v_worker_instance_id := (p_evidence ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_evidence ->> 'worker_instance_epoch')::bigint;
    v_control_session_epoch := (p_evidence ->> 'control_session_epoch')::bigint;
    v_model_residency_id := (p_evidence ->> 'model_residency_id')::uuid;
    v_model_runtime_epoch := (p_evidence ->> 'model_runtime_epoch')::bigint;
    v_stage_profile_revision_id :=
        (p_evidence ->> 'stage_profile_revision_id')::uuid;
    v_worker_member_id := (p_evidence ->> 'worker_member_id')::uuid;
    v_worker_member_epoch := (p_evidence ->> 'worker_member_epoch')::bigint;
    v_capacity_observation_sequence :=
        (p_evidence ->> 'capacity_observation_sequence')::bigint;
    v_device_set_digest := decode(p_evidence ->> 'device_set_digest', 'hex');
    v_membership_digest := decode(p_evidence ->> 'membership_digest', 'hex');
    v_spiffe_id_digest := decode(p_evidence ->> 'spiffe_id_digest', 'hex');
    v_readiness_evidence_digest :=
        decode(p_evidence ->> 'readiness_evidence_digest', 'hex');
    v_runtime_identity := p_evidence ->> 'runtime_identity';
    v_devices := p_evidence -> 'devices';
    v_members := p_evidence -> 'members';

    IF v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR v_control_session_epoch <= 0 OR v_model_residency_id IS NULL
       OR v_model_runtime_epoch <= 0 OR v_stage_profile_revision_id IS NULL
       OR v_worker_member_id IS NULL OR v_worker_member_epoch <= 0
       OR v_capacity_observation_sequence <= 0
       OR octet_length(v_device_set_digest) <> 32
       OR octet_length(v_membership_digest) <> 32
       OR octet_length(v_spiffe_id_digest) <> 32
       OR octet_length(v_readiness_evidence_digest) <> 32
       OR v_runtime_identity IS NULL OR length(v_runtime_identity) NOT BETWEEN 1 AND 500
       OR jsonb_typeof(v_devices) <> 'array' OR jsonb_array_length(v_devices) = 0
       OR jsonb_array_length(v_devices) > 64
       OR jsonb_typeof(v_members) <> 'array' OR jsonb_array_length(v_members) = 0
       OR jsonb_array_length(v_members) > 64 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_registration_invalid',
            MESSAGE = 'Stage Worker registration evidence fields are invalid';
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM worker_instances AS worker
        JOIN capacity_pools AS pool ON pool.id = worker.capacity_pool_id
        JOIN stage_profile_revisions AS profile
          ON profile.id = v_stage_profile_revision_id
        JOIN model_residencies AS residency
          ON residency.id = v_model_residency_id
         AND residency.worker_instance_id = worker.id
         AND residency.worker_instance_epoch = worker.instance_epoch
        JOIN worker_members AS caller
          ON caller.id = v_worker_member_id
         AND caller.worker_instance_id = worker.id
         AND caller.worker_instance_epoch = worker.instance_epoch
        JOIN capacity_observations AS observation
          ON observation.worker_instance_id = worker.id
         AND observation.worker_instance_epoch = worker.instance_epoch
         AND observation.observation_sequence = v_capacity_observation_sequence
        WHERE worker.id = v_worker_instance_id
          AND worker.instance_epoch = v_worker_instance_epoch
          AND worker.control_session_epoch = v_control_session_epoch
          AND worker.lifecycle_state = 'READY'
          AND worker.reachability_state = 'CONNECTED'
          AND worker.device_set_digest = v_device_set_digest
          AND worker.membership_digest = v_membership_digest
          AND worker.worker_profile_revision_id = profile.worker_profile_revision_id
          AND pool.stage_profile_revision_id = profile.id
          AND pool.state = 'ACTIVE'
          AND profile.state IN ('CERTIFIED', 'ACTIVE')
          AND residency.model_component_revision = profile.model_component_revision
          AND residency.runtime_image_digest = profile.runtime_image_digest
          AND residency.runtime_identity = v_runtime_identity
          AND residency.model_runtime_epoch = v_model_runtime_epoch
          AND residency.state = 'READY'
          AND (
              residency.warmup_evidence_digest = v_readiness_evidence_digest
              OR residency.canary_evidence_digest = v_readiness_evidence_digest
          )
          AND caller.member_epoch = v_worker_member_epoch
          AND caller.identity_digest = v_spiffe_id_digest
          AND caller.readiness = 'READY'
          AND observation.expires_at > clock_timestamp()
          AND jsonb_array_length(v_devices) = worker.desired_device_count
          AND jsonb_array_length(v_members) = worker.desired_member_count
          AND NOT EXISTS (
              SELECT 1
              FROM jsonb_array_elements(v_devices) AS requested(device)
              LEFT JOIN active_device_bindings AS binding
                ON binding.worker_instance_id = worker.id
               AND binding.worker_instance_epoch = worker.instance_epoch
               AND binding.device_id = (requested.device ->> 'device_id')::uuid
               AND binding.device_epoch = (requested.device ->> 'device_epoch')::bigint
              WHERE binding.device_id IS NULL
          )
          AND NOT EXISTS (
              SELECT 1
              FROM jsonb_array_elements(v_members) AS requested(member)
              LEFT JOIN worker_members AS durable_member
                ON durable_member.worker_instance_id = worker.id
               AND durable_member.worker_instance_epoch = worker.instance_epoch
               AND durable_member.id = (requested.member ->> 'worker_member_id')::uuid
               AND durable_member.member_epoch =
                   (requested.member ->> 'member_epoch')::bigint
              WHERE durable_member.id IS NULL
                 OR durable_member.readiness <> 'READY'
                 OR (requested.member ->> 'model_runtime_epoch')::bigint
                    <> v_model_runtime_epoch
          )
          AND (
              SELECT count(*) FROM active_device_bindings AS binding
              WHERE binding.worker_instance_id = worker.id
                AND binding.worker_instance_epoch = worker.instance_epoch
          ) = jsonb_array_length(v_devices)
          AND (
              SELECT count(*) FROM worker_members AS member
              WHERE member.worker_instance_id = worker.id
                AND member.worker_instance_epoch = worker.instance_epoch
          ) = jsonb_array_length(v_members)
          AND vela_worker_instance_authority_matches(
              worker.id, worker.instance_epoch,
              v_device_set_digest, v_membership_digest,
              v_model_residency_id, v_model_runtime_epoch
          )
    ) INTO v_ready;

    RETURN QUERY SELECT
        v_worker_instance_id,
        v_worker_instance_epoch,
        v_ready,
        CASE WHEN v_ready THEN 'fleet evidence verified'
             ELSE 'fleet evidence does not match durable authority' END;
END
$$;
REVOKE ALL ON FUNCTION vela_verify_stage_worker_registration(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_verify_stage_worker_registration(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
GRANT EXECUTE ON FUNCTION vela_verify_stage_worker_registration(jsonb)
    TO vela_stage_worker_control;

CREATE FUNCTION vela_verify_stage_capacity_observation(p_observation jsonb)
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
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_control_session_epoch bigint;
    v_observation_sequence bigint;
    v_capacity_vector jsonb;
    v_observed_at timestamptz;
    v_expires_at timestamptz;
    v_spiffe_id_digest bytea;
    v_ready boolean;
BEGIN
    IF p_observation IS NULL OR jsonb_typeof(p_observation) <> 'object'
       OR (p_observation ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_capacity_observation_invalid',
            MESSAGE = 'Stage Worker capacity observation is invalid';
    END IF;
    v_worker_instance_id := (p_observation ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_observation ->> 'worker_instance_epoch')::bigint;
    v_control_session_epoch := (p_observation ->> 'control_session_epoch')::bigint;
    v_observation_sequence := (p_observation ->> 'observation_sequence')::bigint;
    v_capacity_vector := p_observation -> 'capacity_vector';
    v_observed_at := (p_observation ->> 'observed_at')::timestamptz;
    v_expires_at := (p_observation ->> 'expires_at')::timestamptz;
    v_spiffe_id_digest := decode(p_observation ->> 'spiffe_id_digest', 'hex');

    IF v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR v_control_session_epoch <= 0 OR v_observation_sequence <= 0
       OR jsonb_typeof(v_capacity_vector) <> 'object'
       OR v_observed_at IS NULL OR v_expires_at IS NULL
       OR v_expires_at <= v_observed_at
       OR octet_length(v_spiffe_id_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_capacity_observation_invalid',
            MESSAGE = 'Stage Worker capacity observation fields are invalid';
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM worker_instances AS worker
        JOIN capacity_observations AS observation
          ON observation.worker_instance_id = worker.id
         AND observation.worker_instance_epoch = worker.instance_epoch
         AND observation.observation_sequence = v_observation_sequence
        WHERE worker.id = v_worker_instance_id
          AND worker.instance_epoch = v_worker_instance_epoch
          AND worker.control_session_epoch = v_control_session_epoch
          AND worker.lifecycle_state = 'READY'
          AND worker.reachability_state = 'CONNECTED'
          AND observation.capacity_vector = v_capacity_vector
          AND observation.observed_at = v_observed_at
          AND observation.expires_at = v_expires_at
          AND observation.expires_at > clock_timestamp()
          AND EXISTS (
              SELECT 1 FROM worker_members AS member
              WHERE member.worker_instance_id = worker.id
                AND member.worker_instance_epoch = worker.instance_epoch
                AND member.identity_digest = v_spiffe_id_digest
                AND member.readiness = 'READY'
          )
    ) INTO v_ready;

    RETURN QUERY SELECT
        v_worker_instance_id,
        v_worker_instance_epoch,
        v_ready,
        CASE WHEN v_ready THEN 'capacity observation verified'
             ELSE 'capacity observation does not match durable Fleet authority' END;
END
$$;
REVOKE ALL ON FUNCTION vela_verify_stage_capacity_observation(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_verify_stage_capacity_observation(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
GRANT EXECUTE ON FUNCTION vela_verify_stage_capacity_observation(jsonb)
    TO vela_stage_worker_control;

CREATE FUNCTION vela_reattach_stage_worker_command(p_command jsonb)
RETURNS TABLE (stage_version bigint, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid;
    v_attempt_id uuid;
    v_stage_run_id uuid;
    v_stage_attempt_id uuid;
    v_stage_allocation_id uuid;
    v_stage_lease_id uuid;
    v_expected_attempt_fence bigint;
    v_expected_stage_fence bigint;
    v_expected_stage_version bigint;
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_device_set_digest bytea;
    v_membership_digest bytea;
    v_model_residency_id uuid;
    v_model_runtime_epoch bigint;
    v_capacity_observation_sequence bigint;
    v_capacity_vector jsonb;
    v_lease_token_digest bytea;
    v_execution_nonce bytea;
    v_control_session_epoch bigint;
    v_spiffe_id_digest bytea;
    v_current_authority_digest bytea;
    v_observed_runtime_state text;
    v_local_receipt_id text;
    v_local_receipt_digest bytea;
    v_request_digest bytea;
    v_existing stage_worker_commands%ROWTYPE;
    v_attempt attempts%ROWTYPE;
    v_run stage_runs%ROWTYPE;
    v_physical stage_attempts%ROWTYPE;
    v_allocation stage_allocations%ROWTYPE;
    v_lease stage_leases%ROWTYPE;
    v_worker worker_instances%ROWTYPE;
    v_residency model_residencies%ROWTYPE;
    v_current_renewal stage_authority_renewals%ROWTYPE;
    v_last_heartbeat stage_worker_heartbeats%ROWTYPE;
    v_capacity_observation_active boolean;
    v_identity_active boolean;
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) <> 'object'
       OR (p_command ->> 'schema_version')::integer <> 1
       OR p_command ->> 'command_kind' <> 'REATTACH' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_reattach_command_invalid',
            MESSAGE = 'Stage Worker Reattach command is invalid';
    END IF;

    v_command_id := (p_command ->> 'command_id')::uuid;
    v_attempt_id := (p_command ->> 'attempt_id')::uuid;
    v_stage_run_id := (p_command ->> 'stage_run_id')::uuid;
    v_stage_attempt_id := (p_command ->> 'stage_attempt_id')::uuid;
    v_stage_allocation_id := (p_command ->> 'stage_allocation_id')::uuid;
    v_stage_lease_id := (p_command ->> 'stage_lease_id')::uuid;
    v_expected_attempt_fence := (p_command ->> 'expected_attempt_fence')::bigint;
    v_expected_stage_fence := (p_command ->> 'expected_stage_fence')::bigint;
    v_expected_stage_version := (p_command ->> 'expected_stage_version')::bigint;
    v_worker_instance_id := (p_command ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_command ->> 'worker_instance_epoch')::bigint;
    v_device_set_digest := decode(p_command ->> 'device_set_digest', 'hex');
    v_membership_digest := decode(p_command ->> 'membership_digest', 'hex');
    v_model_residency_id := (p_command ->> 'model_residency_id')::uuid;
    v_model_runtime_epoch := (p_command ->> 'model_runtime_epoch')::bigint;
    v_capacity_observation_sequence :=
        (p_command ->> 'capacity_observation_sequence')::bigint;
    v_capacity_vector := p_command -> 'capacity_vector';
    v_lease_token_digest := decode(p_command ->> 'lease_token_digest', 'hex');
    v_execution_nonce := decode(p_command ->> 'execution_nonce', 'hex');
    v_control_session_epoch := (p_command ->> 'control_session_epoch')::bigint;
    v_spiffe_id_digest := decode(p_command ->> 'spiffe_id_digest', 'hex');
    v_current_authority_digest :=
        decode(p_command ->> 'current_authority_digest', 'hex');
    v_observed_runtime_state := p_command ->> 'observed_runtime_state';
    v_local_receipt_id := p_command ->> 'local_receipt_id';
    v_local_receipt_digest := decode(p_command ->> 'local_receipt_digest', 'hex');
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));

    IF v_command_id IS NULL OR v_attempt_id IS NULL OR v_stage_run_id IS NULL
       OR v_stage_attempt_id IS NULL OR v_stage_allocation_id IS NULL
       OR v_stage_lease_id IS NULL OR v_expected_attempt_fence <= 0
       OR v_expected_stage_fence <= 0 OR v_expected_stage_version <= 0
       OR v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR octet_length(v_device_set_digest) <> 32
       OR octet_length(v_membership_digest) <> 32
       OR v_model_residency_id IS NULL OR v_model_runtime_epoch <= 0
       OR v_capacity_observation_sequence <= 0
       OR jsonb_typeof(v_capacity_vector) <> 'object'
       OR octet_length(v_lease_token_digest) <> 32
       OR octet_length(v_execution_nonce) <> 32
       OR v_control_session_epoch <= 0 OR octet_length(v_spiffe_id_digest) <> 32
       OR octet_length(v_current_authority_digest) <> 32
       OR v_observed_runtime_state NOT IN (
           'PREPARING', 'PREPARED', 'RUNNING', 'CANCELING', 'STOPPED',
           'OUTPUT_READY', 'OUTPUT_SEALED', 'FAILED'
       )
       OR (v_local_receipt_id IS NULL) <> (v_local_receipt_digest IS NULL)
       OR (v_local_receipt_id IS NOT NULL
           AND length(v_local_receipt_id) NOT BETWEEN 1 AND 1000)
       OR (v_local_receipt_digest IS NOT NULL
           AND octet_length(v_local_receipt_digest) <> 32) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_reattach_command_invalid',
            MESSAGE = 'Stage Worker Reattach command fields are invalid';
    END IF;

    SELECT command.* INTO v_existing
    FROM stage_worker_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'REATTACH'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_worker_command_replay_mismatch',
                MESSAGE = 'Stage Worker command replay does not match';
        END IF;
        RETURN QUERY
        SELECT (v_existing.result ->> 'stage_version')::bigint, true;
        RETURN;
    END IF;

    SELECT attempt.* INTO v_attempt
    FROM attempts AS attempt
    WHERE attempt.id = v_attempt_id
    FOR UPDATE;
    SELECT run.* INTO v_run
    FROM stage_runs AS run
    WHERE run.id = v_stage_run_id AND run.attempt_id = v_attempt_id
    FOR UPDATE;
    SELECT physical.* INTO v_physical
    FROM stage_attempts AS physical
    WHERE physical.id = v_stage_attempt_id
      AND physical.stage_run_id = v_stage_run_id
    FOR UPDATE;
    SELECT allocation.* INTO v_allocation
    FROM stage_allocations AS allocation
    WHERE allocation.id = v_stage_allocation_id
      AND allocation.stage_attempt_id = v_stage_attempt_id
    FOR UPDATE;
    SELECT lease.* INTO v_lease
    FROM stage_leases AS lease
    WHERE lease.id = v_stage_lease_id
      AND lease.stage_attempt_id = v_stage_attempt_id
      AND lease.stage_allocation_id = v_stage_allocation_id
    FOR UPDATE;
    SELECT worker.* INTO v_worker
    FROM worker_instances AS worker
    WHERE worker.id = v_worker_instance_id;
    SELECT residency.* INTO v_residency
    FROM model_residencies AS residency
    WHERE residency.id = v_model_residency_id;
    SELECT renewal.* INTO v_current_renewal
    FROM stage_authority_renewals AS renewal
    WHERE renewal.stage_lease_id = v_stage_lease_id
    ORDER BY renewal.issued_at DESC, renewal.command_id DESC
    LIMIT 1;
    SELECT heartbeat.* INTO v_last_heartbeat
    FROM stage_worker_heartbeats AS heartbeat
    WHERE heartbeat.stage_lease_id = v_stage_lease_id
    ORDER BY heartbeat.sequence DESC
    LIMIT 1;
    SELECT EXISTS (
        SELECT 1
        FROM worker_members AS member
        WHERE member.worker_instance_id = v_worker_instance_id
          AND member.worker_instance_epoch = v_worker_instance_epoch
          AND member.identity_digest = v_spiffe_id_digest
          AND member.readiness = 'READY'
    ) INTO v_identity_active;
    v_capacity_observation_active := vela_lock_exact_capacity_observation(
        v_worker_instance_id, v_worker_instance_epoch,
        v_capacity_observation_sequence, v_capacity_vector
    );

    IF v_attempt.id IS NULL OR v_attempt.execution_authority_kind <> 'STAGE_GRAPH'
       OR v_attempt.fence <> v_expected_attempt_fence
       OR v_attempt.graph_state NOT IN ('QUEUED', 'RUNNING')
       OR v_run.id IS NULL OR v_run.state NOT IN ('ASSIGNED', 'RUNNING')
       OR v_run.fence <> v_expected_stage_fence
       OR v_run.version <> v_expected_stage_version
       OR v_physical.id IS NULL OR v_physical.state NOT IN ('ASSIGNED', 'RUNNING')
       OR v_allocation.id IS NULL OR v_allocation.state <> 'ALLOCATED'
       OR v_allocation.worker_instance_id <> v_worker_instance_id
       OR v_allocation.worker_instance_epoch <> v_worker_instance_epoch
       OR v_allocation.device_set_digest <> v_device_set_digest
       OR v_allocation.membership_digest <> v_membership_digest
       OR v_allocation.model_residency_id <> v_model_residency_id
       OR v_allocation.model_runtime_epoch <> v_model_runtime_epoch
       OR v_allocation.capacity_vector <> v_capacity_vector
       OR v_lease.id IS NULL OR v_lease.state <> 'ACTIVE'
       OR v_lease.attempt_fence <> v_expected_attempt_fence
       OR v_lease.stage_fence <> v_expected_stage_fence
       OR v_lease.worker_instance_id <> v_worker_instance_id
       OR v_lease.worker_instance_epoch <> v_worker_instance_epoch
       OR v_lease.device_set_digest <> v_device_set_digest
       OR v_lease.membership_digest <> v_membership_digest
       OR v_lease.model_residency_id <> v_model_residency_id
       OR v_lease.model_runtime_epoch <> v_model_runtime_epoch
       OR v_lease.token_digest <> v_lease_token_digest
       OR v_lease.execution_nonce <> v_execution_nonce
       OR v_worker.id IS NULL OR v_worker.instance_epoch <> v_worker_instance_epoch
       OR v_worker.control_session_epoch <> v_control_session_epoch
       OR v_residency.id IS NULL OR v_residency.worker_instance_id <> v_worker_instance_id
       OR v_residency.worker_instance_epoch <> v_worker_instance_epoch
       OR v_residency.model_runtime_epoch <> v_model_runtime_epoch
       OR v_residency.state <> 'READY'
       OR NOT v_identity_active OR NOT v_capacity_observation_active
       OR NOT vela_worker_instance_authority_matches(
           v_worker_instance_id, v_worker_instance_epoch,
           v_device_set_digest, v_membership_digest,
           v_model_residency_id, v_model_runtime_epoch
       )
       OR (v_current_renewal.command_id IS NOT NULL
           AND v_current_renewal.authority_digest <> v_current_authority_digest)
       OR (v_run.state = 'ASSIGNED'
           AND v_observed_runtime_state NOT IN ('PREPARING', 'PREPARED'))
       OR (v_run.state = 'RUNNING'
           AND v_observed_runtime_state NOT IN (
               'RUNNING', 'CANCELING', 'OUTPUT_READY', 'OUTPUT_SEALED'
           ))
       OR (v_last_heartbeat.command_id IS NOT NULL
           AND v_last_heartbeat.runtime_state <> v_observed_runtime_state)
       OR (v_local_receipt_id IS NOT NULL
           AND (v_last_heartbeat.command_id IS NULL
                OR v_last_heartbeat.local_receipt_id <> v_local_receipt_id
                OR v_last_heartbeat.local_receipt_digest <> v_local_receipt_digest))
       OR (v_observed_runtime_state IN ('OUTPUT_READY', 'OUTPUT_SEALED')
           AND v_local_receipt_id IS NULL) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_worker_reattach_authority_stale',
            MESSAGE = 'Stage Worker Reattach authority is stale';
    END IF;

    INSERT INTO stage_worker_commands (
        command_id, command_kind, stage_lease_id, stage_run_id,
        stage_attempt_id, request_digest, current_authority_digest, result
    ) VALUES (
        v_command_id, 'REATTACH', v_stage_lease_id, v_stage_run_id,
        v_stage_attempt_id, v_request_digest, v_current_authority_digest,
        jsonb_build_object('stage_version', v_expected_stage_version)
    );
    INSERT INTO stage_worker_reattachments (
        command_id, stage_lease_id, stage_run_id, stage_attempt_id,
        control_session_epoch, observed_runtime_state,
        local_receipt_id, local_receipt_digest
    ) VALUES (
        v_command_id, v_stage_lease_id, v_stage_run_id, v_stage_attempt_id,
        v_control_session_epoch, v_observed_runtime_state,
        v_local_receipt_id, v_local_receipt_digest
    );
    RETURN QUERY SELECT v_expected_stage_version, false;
END
$$;
REVOKE ALL ON FUNCTION vela_reattach_stage_worker_command(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_reattach_stage_worker_command(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
GRANT EXECUTE ON FUNCTION vela_reattach_stage_worker_command(jsonb)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_worker_reattachments)
       OR EXISTS (
           SELECT 1 FROM stage_worker_commands WHERE command_kind = 'REATTACH'
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_operations_rollback_is_unsafe',
            MESSAGE = 'Cannot contract Stage Worker operations while durable reattach evidence exists';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_reattach_stage_worker_command(jsonb)
    FROM vela_stage_worker_control;
REVOKE EXECUTE ON FUNCTION vela_read_stage_scheduler_claim(uuid)
    FROM vela_stage_scheduler;
REVOKE EXECUTE ON FUNCTION vela_verify_stage_capacity_observation(jsonb)
    FROM vela_stage_worker_control;
REVOKE EXECUTE ON FUNCTION vela_verify_stage_worker_registration(jsonb)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_reattach_stage_worker_command(jsonb);
DROP FUNCTION vela_read_stage_scheduler_claim(uuid);
DROP FUNCTION vela_verify_stage_capacity_observation(jsonb);
DROP FUNCTION vela_verify_stage_worker_registration(jsonb);
DROP TRIGGER stage_worker_reattachments_immutable ON stage_worker_reattachments;
DROP TABLE stage_worker_reattachments;

ALTER TABLE stage_worker_commands
    DROP CONSTRAINT stage_worker_commands_command_kind_check,
    ADD CONSTRAINT stage_worker_commands_command_kind_check
        CHECK (command_kind IN ('START', 'HEARTBEAT'));
-- +goose StatementEnd
