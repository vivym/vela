-- +goose Up
-- +goose StatementBegin
CREATE TABLE stage_worker_commands (
    command_id uuid PRIMARY KEY,
    command_kind text NOT NULL CHECK (command_kind IN ('START', 'HEARTBEAT')),
    stage_lease_id uuid NOT NULL REFERENCES stage_leases(id),
    stage_run_id uuid NOT NULL,
    stage_attempt_id uuid NOT NULL,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    current_authority_digest bytea NOT NULL
        CHECK (octet_length(current_authority_digest) = 32),
    result jsonb NOT NULL CHECK (jsonb_typeof(result) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (stage_run_id, stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id)
);

CREATE TABLE stage_authority_renewals (
    command_id uuid PRIMARY KEY REFERENCES stage_worker_commands(command_id),
    stage_lease_id uuid NOT NULL REFERENCES stage_leases(id),
    stage_run_id uuid NOT NULL,
    stage_attempt_id uuid NOT NULL,
    stage_version bigint NOT NULL CHECK (stage_version > 0),
    control_session_epoch bigint NOT NULL CHECK (control_session_epoch > 0),
    signing_key_id text NOT NULL CHECK (length(signing_key_id) BETWEEN 1 AND 100),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    local_deadline_at timestamptz NOT NULL,
    authority_digest bytea NOT NULL CHECK (octet_length(authority_digest) = 32),
    renewed_authority bytea NOT NULL CHECK (
        octet_length(renewed_authority) BETWEEN 1 AND 65536
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stage_lease_id, issued_at),
    FOREIGN KEY (stage_run_id, stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id),
    CHECK (expires_at > issued_at),
    CHECK (local_deadline_at > issued_at AND local_deadline_at <= expires_at),
    CHECK (expires_at <= issued_at + interval '7 days')
);

CREATE TABLE stage_worker_heartbeats (
    command_id uuid PRIMARY KEY REFERENCES stage_worker_commands(command_id),
    stage_lease_id uuid NOT NULL REFERENCES stage_leases(id),
    stage_run_id uuid NOT NULL,
    stage_attempt_id uuid NOT NULL,
    sequence bigint NOT NULL CHECK (sequence > 0),
    runtime_state text NOT NULL CHECK (
        runtime_state IN (
            'PREPARING', 'PREPARED', 'RUNNING', 'CANCELING', 'STOPPED',
            'OUTPUT_READY', 'OUTPUT_SEALED', 'FAILED'
        )
    ),
    bounded_status jsonb NOT NULL,
    local_receipt_id text CHECK (
        local_receipt_id IS NULL OR length(local_receipt_id) BETWEEN 1 AND 1000
    ),
    local_receipt_digest bytea CHECK (
        local_receipt_digest IS NULL OR octet_length(local_receipt_digest) = 32
    ),
    observed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stage_lease_id, sequence),
    FOREIGN KEY (stage_run_id, stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id),
    CHECK ((local_receipt_id IS NULL) = (local_receipt_digest IS NULL))
);

CREATE FUNCTION vela_reject_stage_worker_evidence_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'stage_worker_evidence_is_immutable',
        MESSAGE = 'Stage Worker execution evidence is immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_stage_worker_evidence_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_stage_worker_evidence_mutation()
    OWNER TO vela_attempt_coordinator_owner;

CREATE TRIGGER stage_worker_commands_immutable
BEFORE UPDATE OR DELETE ON stage_worker_commands
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_worker_evidence_mutation();
CREATE TRIGGER stage_authority_renewals_immutable
BEFORE UPDATE OR DELETE ON stage_authority_renewals
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_worker_evidence_mutation();
CREATE TRIGGER stage_worker_heartbeats_immutable
BEFORE UPDATE OR DELETE ON stage_worker_heartbeats
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_worker_evidence_mutation();

CREATE FUNCTION vela_start_stage_worker_command(p_command jsonb)
RETURNS TABLE (
    renewed_authority bytea,
    stage_version bigint,
    replayed boolean
)
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
    v_renewed_stage_version bigint;
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
    v_started_at timestamptz;
    v_renewed_signing_key_id text;
    v_renewed_issued_at timestamptz;
    v_renewed_expires_at timestamptz;
    v_renewed_local_deadline_at timestamptz;
    v_current_authority bytea;
    v_current_authority_digest bytea;
    v_renewed_authority bytea;
    v_renewed_authority_digest bytea;
    v_request_digest bytea;
    v_existing stage_worker_commands%ROWTYPE;
    v_attempt attempts%ROWTYPE;
    v_run stage_runs%ROWTYPE;
    v_physical stage_attempts%ROWTYPE;
    v_allocation stage_allocations%ROWTYPE;
    v_lease stage_leases%ROWTYPE;
    v_worker worker_instances%ROWTYPE;
    v_result_stage_run_id uuid;
    v_result_stage_attempt_id uuid;
    v_result_state text;
    v_result_stage_fence bigint;
    v_result_stage_version bigint;
    v_result_replayed boolean;
    v_capacity_observation_active boolean;
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) <> 'object'
       OR (p_command ->> 'schema_version')::integer <> 1
       OR p_command ->> 'command_kind' <> 'START' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_start_command_invalid',
            MESSAGE = 'Stage Worker Start command is invalid';
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
    v_renewed_stage_version := (p_command ->> 'renewed_stage_version')::bigint;
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
    v_started_at := (p_command ->> 'started_at')::timestamptz;
    v_renewed_signing_key_id := p_command ->> 'renewed_signing_key_id';
    v_renewed_issued_at := (p_command ->> 'renewed_issued_at')::timestamptz;
    v_renewed_expires_at := (p_command ->> 'renewed_expires_at')::timestamptz;
    v_renewed_local_deadline_at :=
        (p_command ->> 'renewed_local_deadline_at')::timestamptz;
    v_current_authority := decode(p_command ->> 'current_authority', 'hex');
    v_current_authority_digest :=
        decode(p_command ->> 'current_authority_digest', 'hex');
    v_renewed_authority := decode(p_command ->> 'renewed_authority', 'hex');
    v_renewed_authority_digest :=
        decode(p_command ->> 'renewed_authority_digest', 'hex');
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));

    IF v_command_id IS NULL OR v_attempt_id IS NULL OR v_stage_run_id IS NULL
       OR v_stage_attempt_id IS NULL OR v_stage_allocation_id IS NULL
       OR v_stage_lease_id IS NULL OR v_expected_attempt_fence <= 0
       OR v_expected_stage_fence <= 0 OR v_expected_stage_version <= 0
       OR v_renewed_stage_version <> v_expected_stage_version + 1
       OR v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR octet_length(v_device_set_digest) <> 32
       OR octet_length(v_membership_digest) <> 32
       OR v_model_residency_id IS NULL OR v_model_runtime_epoch <= 0
       OR v_capacity_observation_sequence <= 0
       OR jsonb_typeof(v_capacity_vector) <> 'object'
       OR octet_length(v_lease_token_digest) <> 32
       OR octet_length(v_execution_nonce) <> 32
       OR v_control_session_epoch <= 0 OR v_started_at IS NULL
       OR v_renewed_signing_key_id IS NULL
       OR length(v_renewed_signing_key_id) NOT BETWEEN 1 AND 100
       OR v_renewed_issued_at IS NULL OR v_renewed_expires_at IS NULL
       OR v_renewed_local_deadline_at IS NULL
       OR octet_length(v_current_authority) NOT BETWEEN 1 AND 65536
       OR octet_length(v_current_authority_digest) <> 32
       OR sha256(v_current_authority) <> v_current_authority_digest
       OR octet_length(v_renewed_authority) NOT BETWEEN 1 AND 65536
       OR octet_length(v_renewed_authority_digest) <> 32
       OR sha256(v_renewed_authority) <> v_renewed_authority_digest
       OR v_renewed_issued_at < v_started_at
       OR v_renewed_expires_at <= v_renewed_issued_at
       OR v_renewed_local_deadline_at <= v_renewed_issued_at
       OR v_renewed_local_deadline_at > v_renewed_expires_at
       OR v_renewed_expires_at > v_renewed_issued_at + interval '7 days' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_start_command_invalid',
            MESSAGE = 'Stage Worker Start command fields are invalid';
    END IF;

    SELECT command.* INTO v_existing
    FROM stage_worker_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'START'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_worker_command_replay_mismatch',
                MESSAGE = 'Stage Worker command replay does not match';
        END IF;
        RETURN QUERY
        SELECT renewal.renewed_authority,
               (v_existing.result ->> 'stage_version')::bigint,
               true
        FROM stage_authority_renewals AS renewal
        WHERE renewal.command_id = v_existing.command_id;
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
    v_capacity_observation_active := vela_lock_exact_capacity_observation(
        v_worker_instance_id, v_worker_instance_epoch,
        v_capacity_observation_sequence, v_capacity_vector
    );

    IF v_attempt.id IS NULL OR v_attempt.execution_authority_kind <> 'STAGE_GRAPH'
       OR v_attempt.fence <> v_expected_attempt_fence
       OR v_run.id IS NULL OR v_run.state <> 'ASSIGNED'
       OR v_run.fence <> v_expected_stage_fence
       OR v_run.version <> v_expected_stage_version
       OR v_physical.id IS NULL OR v_physical.state <> 'ASSIGNED'
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
       OR v_started_at < v_lease.issued_at OR v_started_at >= v_lease.expires_at
       OR v_renewed_issued_at <= v_lease.issued_at
       OR v_renewed_expires_at <= v_lease.expires_at
       OR NOT vela_worker_instance_authority_matches(
           v_worker_instance_id, v_worker_instance_epoch,
           v_device_set_digest, v_membership_digest,
           v_model_residency_id, v_model_runtime_epoch
       )
       OR NOT v_capacity_observation_active THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_worker_start_authority_stale',
            MESSAGE = 'Stage Worker Start authority is stale';
    END IF;

    SELECT applied.stage_run_id, applied.stage_attempt_id, applied.stage_state,
           applied.stage_fence, applied.stage_version, applied.replayed
    INTO v_result_stage_run_id, v_result_stage_attempt_id, v_result_state,
         v_result_stage_fence, v_result_stage_version, v_result_replayed
    FROM vela_apply_stage_command(p_command) AS applied;
    IF v_result_stage_run_id <> v_stage_run_id
       OR v_result_stage_attempt_id <> v_stage_attempt_id
       OR v_result_state <> 'RUNNING'
       OR v_result_stage_fence <> v_expected_stage_fence
       OR v_result_stage_version <> v_renewed_stage_version
       OR v_result_replayed THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_worker_start_transition_stale',
            MESSAGE = 'Stage Worker Start transition result is stale';
    END IF;

    INSERT INTO stage_worker_commands (
        command_id, command_kind, stage_lease_id, stage_run_id,
        stage_attempt_id, request_digest, current_authority_digest, result
    ) VALUES (
        v_command_id, 'START', v_stage_lease_id, v_stage_run_id,
        v_stage_attempt_id, v_request_digest, v_current_authority_digest,
        jsonb_build_object('stage_version', v_result_stage_version)
    );
    INSERT INTO stage_authority_renewals (
        command_id, stage_lease_id, stage_run_id, stage_attempt_id,
        stage_version, control_session_epoch, signing_key_id,
        issued_at, expires_at, local_deadline_at, authority_digest,
        renewed_authority
    ) VALUES (
        v_command_id, v_stage_lease_id, v_stage_run_id, v_stage_attempt_id,
        v_result_stage_version, v_control_session_epoch, v_renewed_signing_key_id,
        v_renewed_issued_at, v_renewed_expires_at, v_renewed_local_deadline_at,
        v_renewed_authority_digest, v_renewed_authority
    );

    RETURN QUERY SELECT v_renewed_authority, v_result_stage_version, false;
END
$$;
REVOKE ALL ON FUNCTION vela_start_stage_worker_command(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_start_stage_worker_command(jsonb)
    OWNER TO vela_attempt_coordinator_owner;

CREATE FUNCTION vela_heartbeat_stage_worker_command(p_command jsonb)
RETURNS TABLE (
    renewed_authority bytea,
    stage_version bigint,
    replayed boolean
)
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
    v_renewed_stage_version bigint;
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
    v_sequence bigint;
    v_runtime_state text;
    v_bounded_status jsonb;
    v_local_receipt_id text;
    v_local_receipt_digest bytea;
    v_observed_at timestamptz;
    v_renewed_signing_key_id text;
    v_renewed_issued_at timestamptz;
    v_renewed_expires_at timestamptz;
    v_renewed_local_deadline_at timestamptz;
    v_current_authority bytea;
    v_current_authority_digest bytea;
    v_renewed_authority bytea;
    v_renewed_authority_digest bytea;
    v_request_digest bytea;
    v_existing stage_worker_commands%ROWTYPE;
    v_attempt attempts%ROWTYPE;
    v_job jobs%ROWTYPE;
    v_run stage_runs%ROWTYPE;
    v_physical stage_attempts%ROWTYPE;
    v_allocation stage_allocations%ROWTYPE;
    v_lease stage_leases%ROWTYPE;
    v_worker worker_instances%ROWTYPE;
    v_current_renewal stage_authority_renewals%ROWTYPE;
    v_last_sequence bigint;
    v_capacity_observation_active boolean;
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) <> 'object'
       OR (p_command ->> 'schema_version')::integer <> 1
       OR p_command ->> 'command_kind' <> 'HEARTBEAT' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_heartbeat_command_invalid',
            MESSAGE = 'Stage Worker Heartbeat command is invalid';
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
    v_renewed_stage_version := (p_command ->> 'renewed_stage_version')::bigint;
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
    v_sequence := (p_command ->> 'sequence')::bigint;
    v_runtime_state := p_command ->> 'runtime_state';
    v_bounded_status := p_command -> 'bounded_status';
    v_local_receipt_id := p_command ->> 'local_receipt_id';
    v_local_receipt_digest := decode(p_command ->> 'local_receipt_digest', 'hex');
    v_observed_at := (p_command ->> 'observed_at')::timestamptz;
    v_renewed_signing_key_id := p_command ->> 'renewed_signing_key_id';
    v_renewed_issued_at := (p_command ->> 'renewed_issued_at')::timestamptz;
    v_renewed_expires_at := (p_command ->> 'renewed_expires_at')::timestamptz;
    v_renewed_local_deadline_at :=
        (p_command ->> 'renewed_local_deadline_at')::timestamptz;
    v_current_authority := decode(p_command ->> 'current_authority', 'hex');
    v_current_authority_digest :=
        decode(p_command ->> 'current_authority_digest', 'hex');
    v_renewed_authority := decode(p_command ->> 'renewed_authority', 'hex');
    v_renewed_authority_digest :=
        decode(p_command ->> 'renewed_authority_digest', 'hex');
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));

    IF v_command_id IS NULL OR v_attempt_id IS NULL OR v_stage_run_id IS NULL
       OR v_stage_attempt_id IS NULL OR v_stage_allocation_id IS NULL
       OR v_stage_lease_id IS NULL OR v_expected_attempt_fence <= 0
       OR v_expected_stage_fence <= 0 OR v_expected_stage_version <= 0
       OR v_renewed_stage_version <> v_expected_stage_version
       OR v_worker_instance_id IS NULL OR v_worker_instance_epoch <= 0
       OR octet_length(v_device_set_digest) <> 32
       OR octet_length(v_membership_digest) <> 32
       OR v_model_residency_id IS NULL OR v_model_runtime_epoch <= 0
       OR v_capacity_observation_sequence <= 0
       OR jsonb_typeof(v_capacity_vector) <> 'object'
       OR octet_length(v_lease_token_digest) <> 32
       OR octet_length(v_execution_nonce) <> 32
       OR v_control_session_epoch <= 0 OR v_sequence <= 0
       OR v_runtime_state NOT IN (
           'PREPARING', 'PREPARED', 'RUNNING', 'CANCELING', 'STOPPED',
           'OUTPUT_READY', 'OUTPUT_SEALED', 'FAILED'
       )
       OR v_bounded_status IS NULL
       OR octet_length(convert_to(v_bounded_status::text, 'UTF8')) > 65536
       OR (v_local_receipt_id IS NULL) <> (v_local_receipt_digest IS NULL)
       OR (v_local_receipt_id IS NOT NULL
           AND length(v_local_receipt_id) NOT BETWEEN 1 AND 1000)
       OR (v_local_receipt_digest IS NOT NULL
           AND octet_length(v_local_receipt_digest) <> 32)
       OR v_observed_at IS NULL OR v_renewed_signing_key_id IS NULL
       OR length(v_renewed_signing_key_id) NOT BETWEEN 1 AND 100
       OR v_renewed_issued_at IS NULL OR v_renewed_expires_at IS NULL
       OR v_renewed_local_deadline_at IS NULL
       OR octet_length(v_current_authority) NOT BETWEEN 1 AND 65536
       OR octet_length(v_current_authority_digest) <> 32
       OR sha256(v_current_authority) <> v_current_authority_digest
       OR octet_length(v_renewed_authority) NOT BETWEEN 1 AND 65536
       OR octet_length(v_renewed_authority_digest) <> 32
       OR sha256(v_renewed_authority) <> v_renewed_authority_digest
       OR v_renewed_issued_at < v_observed_at
       OR v_renewed_expires_at <= v_renewed_issued_at
       OR v_renewed_local_deadline_at <= v_renewed_issued_at
       OR v_renewed_local_deadline_at > v_renewed_expires_at
       OR v_renewed_expires_at > v_renewed_issued_at + interval '7 days' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_worker_heartbeat_command_invalid',
            MESSAGE = 'Stage Worker Heartbeat command fields are invalid';
    END IF;

    SELECT command.* INTO v_existing
    FROM stage_worker_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'HEARTBEAT'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_worker_command_replay_mismatch',
                MESSAGE = 'Stage Worker command replay does not match';
        END IF;
        RETURN QUERY
        SELECT renewal.renewed_authority,
               (v_existing.result ->> 'stage_version')::bigint,
               true
        FROM stage_authority_renewals AS renewal
        WHERE renewal.command_id = v_existing.command_id;
        RETURN;
    END IF;

    SELECT attempt.* INTO v_attempt
    FROM attempts AS attempt
    WHERE attempt.id = v_attempt_id
    FOR UPDATE;
    SELECT job.* INTO v_job
    FROM jobs AS job
    WHERE job.id = v_attempt.job_id
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
    SELECT renewal.* INTO v_current_renewal
    FROM stage_authority_renewals AS renewal
    WHERE renewal.stage_lease_id = v_stage_lease_id
    ORDER BY renewal.issued_at DESC, renewal.command_id DESC
    LIMIT 1;
    SELECT COALESCE(max(heartbeat.sequence), 0) INTO v_last_sequence
    FROM stage_worker_heartbeats AS heartbeat
    WHERE heartbeat.stage_lease_id = v_stage_lease_id;
    v_capacity_observation_active := vela_lock_exact_capacity_observation(
        v_worker_instance_id, v_worker_instance_epoch,
        v_capacity_observation_sequence, v_capacity_vector
    );

    IF v_attempt.id IS NULL OR v_attempt.execution_authority_kind <> 'STAGE_GRAPH'
       OR v_attempt.graph_state <> 'RUNNING'
       OR v_attempt.fence <> v_expected_attempt_fence
       OR v_job.id IS NULL OR v_job.state <> 'RUNNING'
       OR v_job.current_fence <> v_expected_attempt_fence
       OR v_job.billable_started_at IS NULL
       OR v_run.id IS NULL OR v_run.state <> 'RUNNING'
       OR v_run.fence <> v_expected_stage_fence
       OR v_run.version <> v_expected_stage_version
       OR v_physical.id IS NULL OR v_physical.state <> 'RUNNING'
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
       OR v_current_renewal.command_id IS NULL
       OR v_current_renewal.stage_version <> v_expected_stage_version
       OR v_current_renewal.control_session_epoch <> v_control_session_epoch
       OR v_current_renewal.authority_digest <> v_current_authority_digest
       OR v_observed_at < v_current_renewal.issued_at
       OR v_observed_at >= v_current_renewal.expires_at
       OR v_renewed_issued_at <= v_current_renewal.issued_at
       OR v_renewed_expires_at <= v_current_renewal.expires_at
       OR NOT vela_worker_instance_authority_matches(
           v_worker_instance_id, v_worker_instance_epoch,
           v_device_set_digest, v_membership_digest,
           v_model_residency_id, v_model_runtime_epoch
       )
       OR NOT v_capacity_observation_active THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_worker_heartbeat_authority_stale',
            MESSAGE = 'Stage Worker Heartbeat authority is stale';
    END IF;
    IF v_sequence <> v_last_sequence + 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_worker_heartbeat_sequence_stale',
            MESSAGE = 'Stage Worker Heartbeat sequence is stale';
    END IF;

    INSERT INTO stage_worker_commands (
        command_id, command_kind, stage_lease_id, stage_run_id,
        stage_attempt_id, request_digest, current_authority_digest, result
    ) VALUES (
        v_command_id, 'HEARTBEAT', v_stage_lease_id, v_stage_run_id,
        v_stage_attempt_id, v_request_digest, v_current_authority_digest,
        jsonb_build_object('stage_version', v_expected_stage_version)
    );
    INSERT INTO stage_authority_renewals (
        command_id, stage_lease_id, stage_run_id, stage_attempt_id,
        stage_version, control_session_epoch, signing_key_id,
        issued_at, expires_at, local_deadline_at, authority_digest,
        renewed_authority
    ) VALUES (
        v_command_id, v_stage_lease_id, v_stage_run_id, v_stage_attempt_id,
        v_expected_stage_version, v_control_session_epoch,
        v_renewed_signing_key_id, v_renewed_issued_at, v_renewed_expires_at,
        v_renewed_local_deadline_at, v_renewed_authority_digest,
        v_renewed_authority
    );
    INSERT INTO stage_worker_heartbeats (
        command_id, stage_lease_id, stage_run_id, stage_attempt_id,
        sequence, runtime_state, bounded_status, local_receipt_id,
        local_receipt_digest, observed_at
    ) VALUES (
        v_command_id, v_stage_lease_id, v_stage_run_id, v_stage_attempt_id,
        v_sequence, v_runtime_state, v_bounded_status, v_local_receipt_id,
        v_local_receipt_digest, v_observed_at
    );

    RETURN QUERY SELECT v_renewed_authority, v_expected_stage_version, false;
END
$$;
REVOKE ALL ON FUNCTION vela_heartbeat_stage_worker_command(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_heartbeat_stage_worker_command(jsonb)
    OWNER TO vela_attempt_coordinator_owner;

CREATE OR REPLACE FUNCTION vela_read_stage_authority_snapshot(
    p_stage_lease_id uuid,
    p_capacity_observation_sequence bigint
) RETURNS TABLE (
    job_id uuid,
    attempt_id uuid,
    attempt_fence bigint,
    attempt_state text,
    stage_run_id uuid,
    stage_fence bigint,
    stage_version bigint,
    stage_run_state text,
    stage_attempt_id uuid,
    stage_attempt_state text,
    stage_profile_revision_id uuid,
    stage_allocation_id uuid,
    stage_allocation_state text,
    capacity_vector jsonb,
    stage_lease_id uuid,
    stage_lease_state text,
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    device_set_digest bytea,
    membership_digest bytea,
    model_residency_id uuid,
    model_runtime_epoch bigint,
    token_digest bytea,
    signing_key_id text,
    execution_nonce bytea,
    issued_at timestamptz,
    expires_at timestamptz,
    local_deadline_at timestamptz,
    control_session_epoch bigint,
    worker_lifecycle_state text,
    worker_reachability_state text,
    runtime_identity text,
    residency_state text,
    capacity_observation_active boolean,
    members jsonb,
    devices jsonb
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT attempt.job_id, attempt.id, attempt.fence, attempt.state::text,
           run.id, run.fence, run.version, run.state::text,
           physical.id, physical.state::text,
           physical.selected_stage_profile_revision_id,
           allocation.id, allocation.state::text, allocation.capacity_vector,
           lease.id, lease.state::text,
           lease.worker_instance_id, lease.worker_instance_epoch,
           lease.device_set_digest, lease.membership_digest,
           lease.model_residency_id, lease.model_runtime_epoch,
           lease.token_digest,
           COALESCE(renewal.signing_key_id, lease.signing_key_id),
           lease.execution_nonce,
           COALESCE(renewal.issued_at, lease.issued_at),
           COALESCE(renewal.expires_at, lease.expires_at),
           COALESCE(renewal.local_deadline_at, lease.local_deadline_at),
           worker.control_session_epoch, worker.lifecycle_state::text,
           worker.reachability_state::text, residency.runtime_identity,
           residency.state::text,
           EXISTS (
               SELECT 1 FROM capacity_observations AS observation
               WHERE observation.worker_instance_id = worker.id
                 AND observation.worker_instance_epoch = worker.instance_epoch
                 AND observation.observation_sequence =
                     p_capacity_observation_sequence
                 AND observation.capacity_vector = allocation.capacity_vector
                 AND observation.expires_at > statement_timestamp()
                 AND NOT EXISTS (
                     SELECT 1 FROM capacity_observations AS newer
                     WHERE newer.worker_instance_id = worker.id
                       AND newer.observation_sequence >
                           p_capacity_observation_sequence
                 )
           ),
           COALESCE(member_snapshot.members, '[]'::jsonb),
           COALESCE(device_snapshot.devices, '[]'::jsonb)
    FROM stage_leases AS lease
    JOIN attempts AS attempt ON attempt.id = lease.attempt_id
    JOIN stage_runs AS run ON run.id = lease.stage_run_id
    JOIN stage_attempts AS physical ON physical.id = lease.stage_attempt_id
    JOIN stage_allocations AS allocation ON allocation.id = lease.stage_allocation_id
    JOIN worker_instances AS worker ON worker.id = lease.worker_instance_id
    JOIN model_residencies AS residency ON residency.id = lease.model_residency_id
    LEFT JOIN LATERAL (
        SELECT recorded.signing_key_id, recorded.issued_at,
               recorded.expires_at, recorded.local_deadline_at
        FROM stage_authority_renewals AS recorded
        WHERE recorded.stage_lease_id = lease.id
        ORDER BY recorded.issued_at DESC, recorded.command_id DESC
        LIMIT 1
    ) AS renewal ON true
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'id', member.id,
                'epoch', member.member_epoch,
                'identity_digest', encode(member.identity_digest, 'hex'),
                'readiness', member.readiness::text
            ) ORDER BY member.id
        ) AS members
        FROM worker_members AS member
        WHERE member.worker_instance_id = worker.id
          AND member.worker_instance_epoch = worker.instance_epoch
    ) AS member_snapshot ON true
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'id', binding.device_id,
                'epoch', binding.device_epoch
            ) ORDER BY binding.device_id
        ) AS devices
        FROM active_device_bindings AS binding
        WHERE binding.worker_instance_id = worker.id
          AND binding.worker_instance_epoch = worker.instance_epoch
    ) AS device_snapshot ON true
    WHERE lease.id = p_stage_lease_id
      AND (run.state <> 'RUNNING' OR renewal.issued_at IS NOT NULL);
$$;
ALTER FUNCTION vela_read_stage_authority_snapshot(uuid,bigint)
    OWNER TO vela_attempt_coordinator_owner;

ALTER TABLE stage_worker_commands OWNER TO vela_attempt_coordinator_owner;
ALTER TABLE stage_authority_renewals OWNER TO vela_attempt_coordinator_owner;
ALTER TABLE stage_worker_heartbeats OWNER TO vela_attempt_coordinator_owner;
GRANT EXECUTE ON FUNCTION vela_start_stage_worker_command(jsonb)
    TO vela_stage_worker_control;
GRANT EXECUTE ON FUNCTION vela_heartbeat_stage_worker_command(jsonb)
    TO vela_stage_worker_control;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_worker_commands)
       OR EXISTS (SELECT 1 FROM stage_authority_renewals)
       OR EXISTS (SELECT 1 FROM stage_worker_heartbeats) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_execution_rollback_is_unsafe',
            MESSAGE = 'Stage Worker execution evidence prevents schema rollback';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_start_stage_worker_command(jsonb)
    FROM vela_stage_worker_control;
REVOKE EXECUTE ON FUNCTION vela_heartbeat_stage_worker_command(jsonb)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_heartbeat_stage_worker_command(jsonb);
DROP FUNCTION vela_start_stage_worker_command(jsonb);

CREATE OR REPLACE FUNCTION vela_read_stage_authority_snapshot(
    p_stage_lease_id uuid,
    p_capacity_observation_sequence bigint
) RETURNS TABLE (
    job_id uuid,
    attempt_id uuid,
    attempt_fence bigint,
    attempt_state text,
    stage_run_id uuid,
    stage_fence bigint,
    stage_version bigint,
    stage_run_state text,
    stage_attempt_id uuid,
    stage_attempt_state text,
    stage_profile_revision_id uuid,
    stage_allocation_id uuid,
    stage_allocation_state text,
    capacity_vector jsonb,
    stage_lease_id uuid,
    stage_lease_state text,
    worker_instance_id uuid,
    worker_instance_epoch bigint,
    device_set_digest bytea,
    membership_digest bytea,
    model_residency_id uuid,
    model_runtime_epoch bigint,
    token_digest bytea,
    signing_key_id text,
    execution_nonce bytea,
    issued_at timestamptz,
    expires_at timestamptz,
    local_deadline_at timestamptz,
    control_session_epoch bigint,
    worker_lifecycle_state text,
    worker_reachability_state text,
    runtime_identity text,
    residency_state text,
    capacity_observation_active boolean,
    members jsonb,
    devices jsonb
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT attempt.job_id, attempt.id, attempt.fence, attempt.state::text,
           run.id, run.fence, run.version, run.state::text,
           physical.id, physical.state::text,
           physical.selected_stage_profile_revision_id,
           allocation.id, allocation.state::text, allocation.capacity_vector,
           lease.id, lease.state::text,
           lease.worker_instance_id, lease.worker_instance_epoch,
           lease.device_set_digest, lease.membership_digest,
           lease.model_residency_id, lease.model_runtime_epoch,
           lease.token_digest, lease.signing_key_id, lease.execution_nonce,
           lease.issued_at, lease.expires_at, lease.local_deadline_at,
           worker.control_session_epoch, worker.lifecycle_state::text,
           worker.reachability_state::text, residency.runtime_identity,
           residency.state::text,
           EXISTS (
               SELECT 1 FROM capacity_observations AS observation
               WHERE observation.worker_instance_id = worker.id
                 AND observation.worker_instance_epoch = worker.instance_epoch
                 AND observation.observation_sequence =
                     p_capacity_observation_sequence
                 AND observation.capacity_vector = allocation.capacity_vector
                 AND observation.expires_at > statement_timestamp()
           ),
           COALESCE(member_snapshot.members, '[]'::jsonb),
           COALESCE(device_snapshot.devices, '[]'::jsonb)
    FROM stage_leases AS lease
    JOIN attempts AS attempt ON attempt.id = lease.attempt_id
    JOIN stage_runs AS run ON run.id = lease.stage_run_id
    JOIN stage_attempts AS physical ON physical.id = lease.stage_attempt_id
    JOIN stage_allocations AS allocation ON allocation.id = lease.stage_allocation_id
    JOIN worker_instances AS worker ON worker.id = lease.worker_instance_id
    JOIN model_residencies AS residency ON residency.id = lease.model_residency_id
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'id', member.id,
                'epoch', member.member_epoch,
                'identity_digest', encode(member.identity_digest, 'hex'),
                'readiness', member.readiness::text
            ) ORDER BY member.id
        ) AS members
        FROM worker_members AS member
        WHERE member.worker_instance_id = worker.id
          AND member.worker_instance_epoch = worker.instance_epoch
    ) AS member_snapshot ON true
    LEFT JOIN LATERAL (
        SELECT jsonb_agg(
            jsonb_build_object(
                'id', binding.device_id,
                'epoch', binding.device_epoch
            ) ORDER BY binding.device_id
        ) AS devices
        FROM active_device_bindings AS binding
        WHERE binding.worker_instance_id = worker.id
          AND binding.worker_instance_epoch = worker.instance_epoch
    ) AS device_snapshot ON true
    WHERE lease.id = p_stage_lease_id;
$$;
ALTER FUNCTION vela_read_stage_authority_snapshot(uuid,bigint)
    OWNER TO vela_attempt_coordinator_owner;

DROP TRIGGER stage_worker_heartbeats_immutable ON stage_worker_heartbeats;
DROP TRIGGER stage_authority_renewals_immutable ON stage_authority_renewals;
DROP TRIGGER stage_worker_commands_immutable ON stage_worker_commands;
DROP FUNCTION vela_reject_stage_worker_evidence_mutation();
DROP TABLE stage_worker_heartbeats;
DROP TABLE stage_authority_renewals;
DROP TABLE stage_worker_commands;
-- +goose StatementEnd
