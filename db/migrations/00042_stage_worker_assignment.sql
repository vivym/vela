-- +goose Up
-- +goose StatementBegin
CREATE TYPE stage_worker_acquire_result_kind AS ENUM (
    'ASSIGNMENT', 'NO_WORK', 'STALE', 'REJECTED'
);

CREATE TABLE stage_worker_acquire_intents (
    command_id uuid PRIMARY KEY,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    control_session_epoch bigint NOT NULL CHECK (control_session_epoch > 0),
    capacity_observation_sequence bigint NOT NULL
        CHECK (capacity_observation_sequence > 0),
    model_residency_id uuid NOT NULL REFERENCES model_residencies(id),
    model_runtime_epoch bigint NOT NULL CHECK (model_runtime_epoch > 0),
    stage_profile_revision_id uuid NOT NULL REFERENCES stage_profile_revisions(id),
    spiffe_id_digest bytea NOT NULL CHECK (octet_length(spiffe_id_digest) = 32),
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE stage_worker_acquire_results (
    command_id uuid PRIMARY KEY REFERENCES stage_worker_acquire_intents(command_id),
    result_kind stage_worker_acquire_result_kind NOT NULL,
    assignment_wire bytea,
    retry_after_ms bigint,
    detail text CHECK (detail IS NULL OR length(detail) BETWEEN 1 AND 1000),
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (result_kind = 'ASSIGNMENT' AND octet_length(assignment_wire) BETWEEN 1 AND 4194304
            AND retry_after_ms IS NULL AND detail IS NULL)
        OR (result_kind = 'NO_WORK' AND assignment_wire IS NULL
            AND retry_after_ms BETWEEN 1 AND 3600000 AND detail IS NULL)
        OR (result_kind IN ('STALE', 'REJECTED') AND assignment_wire IS NULL
            AND retry_after_ms IS NULL AND detail IS NOT NULL)
    )
);

CREATE TRIGGER stage_worker_acquire_intents_immutable
BEFORE UPDATE OR DELETE ON stage_worker_acquire_intents
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_worker_evidence_mutation();
CREATE TRIGGER stage_worker_acquire_results_immutable
BEFORE UPDATE OR DELETE ON stage_worker_acquire_results
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_worker_evidence_mutation();

ALTER TABLE stage_worker_acquire_intents OWNER TO vela_attempt_coordinator_owner;
ALTER TABLE stage_worker_acquire_results OWNER TO vela_attempt_coordinator_owner;
ALTER TABLE stage_worker_acquire_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_worker_acquire_intents FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_worker_acquire_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_worker_acquire_results FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_begin_stage_worker_acquire(p_command jsonb)
RETURNS TABLE (
    requested_at timestamptz,
    result_kind text,
    assignment_wire bytea,
    retry_after_ms bigint,
    detail text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid;
    v_worker_instance_id uuid;
    v_worker_instance_epoch bigint;
    v_control_session_epoch bigint;
    v_capacity_observation_sequence bigint;
    v_model_residency_id uuid;
    v_model_runtime_epoch bigint;
    v_stage_profile_revision_id uuid;
    v_spiffe_id_digest bytea;
    v_request_digest bytea;
    v_intent stage_worker_acquire_intents%ROWTYPE;
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) <> 'object'
       OR (p_command ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_worker_acquire_invalid',
            MESSAGE = 'Stage Worker acquire command is invalid';
    END IF;
    v_command_id := (p_command ->> 'command_id')::uuid;
    v_worker_instance_id := (p_command ->> 'worker_instance_id')::uuid;
    v_worker_instance_epoch := (p_command ->> 'worker_instance_epoch')::bigint;
    v_control_session_epoch := (p_command ->> 'control_session_epoch')::bigint;
    v_capacity_observation_sequence :=
        (p_command ->> 'capacity_observation_sequence')::bigint;
    v_model_residency_id := (p_command ->> 'model_residency_id')::uuid;
    v_model_runtime_epoch := (p_command ->> 'model_runtime_epoch')::bigint;
    v_stage_profile_revision_id :=
        (p_command ->> 'stage_profile_revision_id')::uuid;
    v_spiffe_id_digest := decode(p_command ->> 'spiffe_id_digest', 'hex');
    IF v_command_id IS NULL OR v_worker_instance_id IS NULL
       OR v_worker_instance_epoch <= 0 OR v_control_session_epoch <= 0
       OR v_capacity_observation_sequence <= 0 OR v_model_residency_id IS NULL
       OR v_model_runtime_epoch <= 0 OR v_stage_profile_revision_id IS NULL
       OR octet_length(v_spiffe_id_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_worker_acquire_invalid',
            MESSAGE = 'Stage Worker acquire command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    INSERT INTO stage_worker_acquire_intents (
        command_id, request_digest, worker_instance_id, worker_instance_epoch,
        control_session_epoch, capacity_observation_sequence, model_residency_id,
        model_runtime_epoch, stage_profile_revision_id, spiffe_id_digest
    ) VALUES (
        v_command_id, v_request_digest, v_worker_instance_id, v_worker_instance_epoch,
        v_control_session_epoch, v_capacity_observation_sequence, v_model_residency_id,
        v_model_runtime_epoch, v_stage_profile_revision_id, v_spiffe_id_digest
    ) ON CONFLICT (command_id) DO NOTHING;

    SELECT intent.* INTO STRICT v_intent
    FROM stage_worker_acquire_intents AS intent
    WHERE intent.command_id = v_command_id;
    IF v_intent.request_digest <> v_request_digest
       OR v_intent.worker_instance_id <> v_worker_instance_id
       OR v_intent.worker_instance_epoch <> v_worker_instance_epoch
       OR v_intent.control_session_epoch <> v_control_session_epoch
       OR v_intent.capacity_observation_sequence <> v_capacity_observation_sequence
       OR v_intent.model_residency_id <> v_model_residency_id
       OR v_intent.model_runtime_epoch <> v_model_runtime_epoch
       OR v_intent.stage_profile_revision_id <> v_stage_profile_revision_id
       OR v_intent.spiffe_id_digest <> v_spiffe_id_digest THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_worker_acquire_replay_mismatch',
            MESSAGE = 'Stage Worker acquire command replay does not match';
    END IF;
    RETURN QUERY
    SELECT v_intent.requested_at, result.result_kind::text,
           result.assignment_wire, result.retry_after_ms, result.detail
    FROM (SELECT 1) AS singleton
    LEFT JOIN stage_worker_acquire_results AS result
      ON result.command_id = v_intent.command_id;
END
$$;
ALTER FUNCTION vela_begin_stage_worker_acquire(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_begin_stage_worker_acquire(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_begin_stage_worker_acquire(jsonb)
    TO vela_stage_worker_control;

CREATE FUNCTION vela_read_stage_worker_acquire_authority(p_command_id uuid)
RETURNS TABLE (decision text, reason text, authority jsonb)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_intent stage_worker_acquire_intents%ROWTYPE;
    v_worker worker_instances%ROWTYPE;
    v_epoch worker_instance_epochs%ROWTYPE;
    v_pool capacity_pools%ROWTYPE;
    v_residency model_residencies%ROWTYPE;
    v_capacity_vector jsonb;
    v_capacity_expires_at timestamptz;
    v_members jsonb;
    v_devices jsonb;
BEGIN
    SELECT intent.* INTO v_intent FROM stage_worker_acquire_intents AS intent
    WHERE intent.command_id = p_command_id;
    IF v_intent.command_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_worker_acquire_invalid',
            MESSAGE = 'Stage Worker acquire intent does not exist';
    END IF;
    SELECT worker.* INTO v_worker FROM worker_instances AS worker
    WHERE worker.id = v_intent.worker_instance_id;
    IF v_worker.id IS NULL OR NOT EXISTS (
        SELECT 1 FROM worker_members AS member
        WHERE member.worker_instance_id = v_intent.worker_instance_id
          AND member.worker_instance_epoch = v_intent.worker_instance_epoch
          AND member.identity_digest = v_intent.spiffe_id_digest
          AND member.readiness = 'READY'
    ) THEN
        RETURN QUERY SELECT 'REJECTED'::text, 'Worker identity is not authorized'::text,
            NULL::jsonb;
        RETURN;
    END IF;
    SELECT epoch.* INTO v_epoch FROM worker_instance_epochs AS epoch
    WHERE epoch.worker_instance_id = v_intent.worker_instance_id
      AND epoch.epoch = v_intent.worker_instance_epoch;
    SELECT pool.* INTO v_pool FROM capacity_pools AS pool
    WHERE pool.id = v_worker.capacity_pool_id;
    SELECT residency.* INTO v_residency FROM model_residencies AS residency
    WHERE residency.id = v_intent.model_residency_id;
    SELECT observation.capacity_vector, observation.expires_at
      INTO v_capacity_vector, v_capacity_expires_at
    FROM capacity_observations AS observation
    WHERE observation.worker_instance_id = v_intent.worker_instance_id
      AND observation.worker_instance_epoch = v_intent.worker_instance_epoch
      AND observation.observation_sequence = v_intent.capacity_observation_sequence;
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'worker_member_id', member.id, 'member_epoch', member.member_epoch,
        'model_runtime_epoch', v_intent.model_runtime_epoch
    ) ORDER BY member.id), '[]'::jsonb) INTO v_members
    FROM worker_members AS member
    WHERE member.worker_instance_id = v_intent.worker_instance_id
      AND member.worker_instance_epoch = v_intent.worker_instance_epoch
      AND member.readiness = 'READY';
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'device_id', binding.device_id, 'device_epoch', binding.device_epoch
    ) ORDER BY binding.device_id), '[]'::jsonb) INTO v_devices
    FROM active_device_bindings AS binding
    WHERE binding.worker_instance_id = v_intent.worker_instance_id
      AND binding.worker_instance_epoch = v_intent.worker_instance_epoch;

    IF v_worker.instance_epoch <> v_intent.worker_instance_epoch
       OR v_worker.control_session_epoch <> v_intent.control_session_epoch
       OR v_worker.lifecycle_state <> 'READY' OR v_worker.reachability_state <> 'CONNECTED'
       OR v_epoch.worker_instance_id IS NULL OR v_epoch.ended_at IS NOT NULL
       OR v_pool.id IS NULL OR v_pool.state <> 'ACTIVE'
       OR v_pool.stage_profile_revision_id <> v_intent.stage_profile_revision_id
       OR v_residency.id IS NULL OR v_residency.worker_instance_id <> v_worker.id
       OR v_residency.worker_instance_epoch <> v_intent.worker_instance_epoch
       OR v_residency.model_runtime_epoch <> v_intent.model_runtime_epoch
       OR v_residency.state <> 'READY' OR v_capacity_vector IS NULL
       OR v_capacity_expires_at <= statement_timestamp()
       OR jsonb_array_length(v_members) <> v_worker.desired_member_count
       OR jsonb_array_length(v_devices) <> v_worker.desired_device_count THEN
        RETURN QUERY SELECT 'STALE'::text, 'Worker Fleet authority is stale'::text,
            NULL::jsonb;
        RETURN;
    END IF;
    RETURN QUERY SELECT 'AUTHORIZED'::text, 'Worker authority is current'::text,
        jsonb_build_object(
            'capacity_pool_id', v_pool.id,
            'stage_profile_revision_id', v_intent.stage_profile_revision_id,
            'worker_instance_id', v_worker.id,
            'worker_instance_epoch', v_worker.instance_epoch,
            'device_set_digest', encode(v_epoch.device_set_digest, 'hex'),
            'membership_digest', encode(v_epoch.membership_digest, 'hex'),
            'model_residency_id', v_residency.id,
            'model_runtime_identity', v_residency.runtime_identity,
            'model_runtime_epoch', v_residency.model_runtime_epoch,
            'capacity_observation_sequence', v_intent.capacity_observation_sequence,
            'capacity_vector', v_capacity_vector,
            'members', v_members,
            'devices', v_devices
        );
END
$$;
ALTER FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_worker_acquire_authority(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    TO vela_stage_worker_control;

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
    v_intent stage_worker_acquire_intents%ROWTYPE;
    v_claim stage_scheduler_claims%ROWTYPE;
    v_job jobs%ROWTYPE;
    v_attempt attempts%ROWTYPE;
    v_run stage_runs%ROWTYPE;
    v_physical stage_attempts%ROWTYPE;
    v_allocation stage_allocations%ROWTYPE;
    v_lease stage_leases%ROWTYPE;
    v_worker worker_instances%ROWTYPE;
    v_residency model_residencies%ROWTYPE;
    v_definition stage_definition_revisions%ROWTYPE;
    v_members jsonb;
    v_devices jsonb;
    v_inputs jsonb;
    v_dependency_count integer;
BEGIN
    SELECT intent.* INTO v_intent FROM stage_worker_acquire_intents AS intent
    WHERE intent.command_id = p_command_id;
    SELECT claim.* INTO v_claim FROM stage_scheduler_claims AS claim
    WHERE claim.id = p_claim_id;
    SELECT physical.* INTO v_physical FROM stage_attempts AS physical
    WHERE physical.id = v_claim.stage_attempt_id;
    SELECT run.* INTO v_run FROM stage_runs AS run WHERE run.id = v_claim.stage_run_id;
    SELECT attempt.* INTO v_attempt FROM attempts AS attempt WHERE attempt.id = v_run.attempt_id;
    SELECT job.* INTO v_job FROM jobs AS job WHERE job.id = v_attempt.job_id;
    SELECT allocation.* INTO v_allocation FROM stage_allocations AS allocation
    WHERE allocation.id = v_claim.stage_allocation_id;
    SELECT lease.* INTO v_lease FROM stage_leases AS lease
    WHERE lease.id = v_claim.stage_lease_id;
    SELECT worker.* INTO v_worker FROM worker_instances AS worker
    WHERE worker.id = v_claim.worker_instance_id;
    SELECT residency.* INTO v_residency FROM model_residencies AS residency
    WHERE residency.id = v_lease.model_residency_id;
    SELECT definition.* INTO v_definition FROM stage_definition_revisions AS definition
    WHERE definition.id = v_run.stage_definition_revision_id;

    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'worker_member_id', member.id, 'member_epoch', member.member_epoch,
        'model_runtime_epoch', v_lease.model_runtime_epoch
    ) ORDER BY member.id), '[]'::jsonb) INTO v_members
    FROM worker_members AS member
    WHERE member.worker_instance_id = v_lease.worker_instance_id
      AND member.worker_instance_epoch = v_lease.worker_instance_epoch
      AND member.readiness = 'READY';
    SELECT COALESCE(jsonb_agg(jsonb_build_object(
        'device_id', binding.device_id, 'device_epoch', binding.device_epoch
    ) ORDER BY binding.device_id), '[]'::jsonb) INTO v_devices
    FROM active_device_bindings AS binding
    WHERE binding.worker_instance_id = v_lease.worker_instance_id
      AND binding.worker_instance_epoch = v_lease.worker_instance_epoch;
    SELECT count(*) INTO v_dependency_count FROM stage_dependencies AS dependency
    WHERE dependency.destination_stage_run_id = v_run.id;
    SELECT COALESCE(jsonb_agg(input.input ORDER BY input.destination_port), '[]'::jsonb)
      INTO v_inputs
    FROM (
        SELECT dependency.destination_port,
            jsonb_build_object(
                'stage_artifact_id', artifact.id,
                'object_version', artifact.object_version,
                'sha256', encode(artifact.sha256, 'hex'),
                'size_bytes', artifact.size_bytes,
                'stage_interface_revision_id', artifact.stage_interface_revision_id,
                'artifact_expires_at', artifact.expires_at,
                'pin_id', pin.id,
                'connector_revision_id', connector.connector_revision_id
            ) AS input
        FROM stage_dependencies AS dependency
        JOIN stage_runs AS source ON source.id = dependency.source_stage_run_id
        JOIN stage_artifacts AS artifact ON artifact.id = source.winner_stage_artifact_id
          AND artifact.state = 'COMMITTED'
        JOIN stage_artifact_pins AS pin ON pin.stage_artifact_id = artifact.id
          AND pin.owner_job_id = v_job.id AND pin.owner_stage_run_id = v_run.id
          AND pin.pin_kind = 'EXECUTION' AND pin.state = 'ACTIVE'
          AND pin.exact_object_version = artifact.object_version
        JOIN execution_graph_edges AS edge
          ON edge.execution_graph_revision_id = v_run.execution_graph_revision_id
         AND edge.source_stage_key = source.stage_key
         AND edge.source_port = dependency.source_port
         AND edge.destination_stage_key = v_run.stage_key
         AND edge.destination_port = dependency.destination_port
        JOIN LATERAL (
            SELECT option.connector_revision_id
            FROM execution_profile_connector_options AS option
            JOIN connector_revisions AS revision
              ON revision.id = option.connector_revision_id
             AND revision.state IN ('ACTIVE', 'DRAINING')
            WHERE option.execution_profile_revision_id =
                    v_attempt.execution_profile_revision_id
              AND option.execution_graph_revision_id = v_run.execution_graph_revision_id
              AND option.execution_graph_edge_id = edge.id
            ORDER BY option.preference DESC, option.connector_revision_id
            LIMIT 1
        ) AS connector ON true
        WHERE dependency.destination_stage_run_id = v_run.id
    ) AS input;

    IF v_intent.command_id IS NULL OR v_claim.id IS NULL OR v_claim.state <> 'COMMITTED'
       OR v_physical.id IS NULL OR v_physical.state <> 'ASSIGNED'
       OR v_run.id IS NULL OR v_run.state <> 'ASSIGNED'
       OR v_attempt.id IS NULL OR v_attempt.graph_state NOT IN ('QUEUED', 'RUNNING')
       OR v_job.id IS NULL OR v_job.state NOT IN ('QUEUED', 'RUNNING')
       OR v_job.request_content_deleted_at IS NOT NULL
       OR v_allocation.id IS NULL OR v_allocation.state <> 'ALLOCATED'
       OR v_lease.id IS NULL OR v_lease.state <> 'ACTIVE'
       OR v_lease.expires_at <= statement_timestamp()
       OR v_claim.worker_instance_id <> v_intent.worker_instance_id
       OR v_physical.selected_stage_profile_revision_id <>
            v_intent.stage_profile_revision_id
       OR v_lease.worker_instance_epoch <> v_intent.worker_instance_epoch
       OR v_lease.model_residency_id <> v_intent.model_residency_id
       OR v_lease.model_runtime_epoch <> v_intent.model_runtime_epoch
       OR (v_claim.command_payload ->> 'capacity_observation_sequence')::bigint <>
            v_intent.capacity_observation_sequence
       OR v_worker.control_session_epoch <> v_intent.control_session_epoch
       OR NOT EXISTS (
            SELECT 1 FROM worker_members AS member
            WHERE member.worker_instance_id = v_worker.id
              AND member.worker_instance_epoch = v_worker.instance_epoch
              AND member.identity_digest = v_intent.spiffe_id_digest
              AND member.readiness = 'READY'
       )
       OR NOT vela_worker_instance_authority_matches(
            v_lease.worker_instance_id, v_lease.worker_instance_epoch,
            v_lease.device_set_digest, v_lease.membership_digest,
            v_lease.model_residency_id, v_lease.model_runtime_epoch
       )
       OR jsonb_array_length(v_members) <> v_worker.desired_member_count
       OR jsonb_array_length(v_devices) <> v_worker.desired_device_count
       OR jsonb_array_length(v_inputs) <> v_dependency_count THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_worker_acquire_execution_stale',
            MESSAGE = format(
                'Stage Worker assignment execution authority is stale: intent=%s claim=%s physical=%s run=%s attempt=%s job=%s allocation=%s lease=%s session=%s/%s member=%s authority=%s members=%s/%s devices=%s/%s inputs=%s/%s',
                v_intent.command_id IS NOT NULL, v_claim.state::text,
                v_physical.state::text, v_run.state::text,
                v_attempt.graph_state::text, v_job.state::text,
                v_allocation.state::text, v_lease.state::text,
                v_worker.control_session_epoch, v_intent.control_session_epoch,
                EXISTS (
                    SELECT 1 FROM worker_members AS member
                    WHERE member.worker_instance_id = v_worker.id
                      AND member.worker_instance_epoch = v_worker.instance_epoch
                      AND member.identity_digest = v_intent.spiffe_id_digest
                      AND member.readiness = 'READY'
                ),
                vela_worker_instance_authority_matches(
                    v_lease.worker_instance_id, v_lease.worker_instance_epoch,
                    v_lease.device_set_digest, v_lease.membership_digest,
                    v_lease.model_residency_id, v_lease.model_runtime_epoch
                ),
                jsonb_array_length(v_members), v_worker.desired_member_count,
                jsonb_array_length(v_devices), v_worker.desired_device_count,
                jsonb_array_length(v_inputs), v_dependency_count
            );
    END IF;
    RETURN QUERY SELECT jsonb_build_object(
        'job_id', v_job.id,
        'attempt_id', v_attempt.id,
        'attempt_fence', v_attempt.fence,
        'stage_run_id', v_run.id,
        'stage_fence', v_run.fence,
        'stage_version', v_run.version,
        'stage_attempt_id', v_physical.id,
        'stage_allocation_id', v_allocation.id,
        'stage_lease_id', v_lease.id,
        'stage_profile_revision_id', v_physical.selected_stage_profile_revision_id,
        'worker_instance_id', v_lease.worker_instance_id,
        'worker_instance_epoch', v_lease.worker_instance_epoch,
        'device_set_digest', encode(v_lease.device_set_digest, 'hex'),
        'membership_digest', encode(v_lease.membership_digest, 'hex'),
        'model_residency_id', v_lease.model_residency_id,
        'model_runtime_identity', v_residency.runtime_identity,
        'model_runtime_epoch', v_lease.model_runtime_epoch,
        'capacity_observation_sequence', v_intent.capacity_observation_sequence,
        'capacity_vector', v_allocation.capacity_vector,
        'lease_token_digest', encode(v_lease.token_digest, 'hex'),
        'execution_nonce', encode(v_lease.execution_nonce, 'hex'),
        'signing_key_id', v_lease.signing_key_id,
        'issued_at', v_lease.issued_at,
        'expires_at', v_lease.expires_at,
        'local_deadline_at', v_lease.local_deadline_at,
        'parameters', v_job.request_content,
        'expected_output_manifest', v_definition.output_ports,
        'members', v_members,
        'devices', v_devices,
        'inputs', v_inputs
    );
END
$$;
ALTER FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    TO vela_stage_worker_control;

CREATE FUNCTION vela_complete_stage_worker_acquire(p_result jsonb)
RETURNS TABLE (
    result_kind text,
    assignment_wire bytea,
    retry_after_ms bigint,
    detail text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid;
    v_result_kind stage_worker_acquire_result_kind;
    v_assignment_wire bytea;
    v_retry_after_ms bigint;
    v_detail text;
    v_existing stage_worker_acquire_results%ROWTYPE;
BEGIN
    IF p_result IS NULL OR jsonb_typeof(p_result) <> 'object'
       OR (p_result ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_worker_acquire_result_invalid',
            MESSAGE = 'Stage Worker acquire result is invalid';
    END IF;
    v_command_id := (p_result ->> 'command_id')::uuid;
    v_result_kind := (p_result ->> 'result_kind')::stage_worker_acquire_result_kind;
    v_assignment_wire := CASE WHEN p_result ->> 'assignment_wire' IS NULL THEN NULL
        ELSE decode(p_result ->> 'assignment_wire', 'hex') END;
    v_retry_after_ms := (p_result ->> 'retry_after_ms')::bigint;
    v_detail := p_result ->> 'detail';
    IF v_command_id IS NULL OR NOT EXISTS (
        SELECT 1 FROM stage_worker_acquire_intents AS intent
        WHERE intent.command_id = v_command_id
    ) OR (v_result_kind = 'ASSIGNMENT' AND
            (octet_length(v_assignment_wire) NOT BETWEEN 1 AND 4194304
             OR v_retry_after_ms IS NOT NULL OR v_detail IS NOT NULL))
       OR (v_result_kind = 'NO_WORK' AND
            (v_assignment_wire IS NOT NULL OR v_retry_after_ms NOT BETWEEN 1 AND 3600000
             OR v_detail IS NOT NULL))
       OR (v_result_kind IN ('STALE', 'REJECTED') AND
            (v_assignment_wire IS NOT NULL OR v_retry_after_ms IS NOT NULL
             OR length(v_detail) NOT BETWEEN 1 AND 1000)) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_worker_acquire_result_invalid',
            MESSAGE = 'Stage Worker acquire result fields are invalid';
    END IF;
    INSERT INTO stage_worker_acquire_results (
        command_id, result_kind, assignment_wire, retry_after_ms, detail
    ) VALUES (
        v_command_id, v_result_kind, v_assignment_wire, v_retry_after_ms, v_detail
    ) ON CONFLICT (command_id) DO NOTHING;
    SELECT result.* INTO STRICT v_existing
    FROM stage_worker_acquire_results AS result
    WHERE result.command_id = v_command_id;
    IF v_existing.result_kind <> v_result_kind
       OR v_existing.assignment_wire IS DISTINCT FROM v_assignment_wire
       OR v_existing.retry_after_ms IS DISTINCT FROM v_retry_after_ms
       OR v_existing.detail IS DISTINCT FROM v_detail THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_worker_acquire_result_replay_mismatch',
            MESSAGE = 'Stage Worker acquire result replay does not match';
    END IF;
    RETURN QUERY SELECT v_existing.result_kind::text, v_existing.assignment_wire,
        v_existing.retry_after_ms, v_existing.detail;
END
$$;
ALTER FUNCTION vela_complete_stage_worker_acquire(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_complete_stage_worker_acquire(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_complete_stage_worker_acquire(jsonb)
    TO vela_stage_worker_control;

GRANT SELECT, INSERT ON stage_worker_acquire_intents, stage_worker_acquire_results
    TO vela_attempt_coordinator_owner;
GRANT SELECT ON stage_scheduler_claims TO vela_attempt_coordinator_owner;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_worker_acquire_intents)
       OR EXISTS (SELECT 1 FROM stage_worker_acquire_results) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_worker_assignment_rollback_is_unsafe',
            MESSAGE = 'Cannot contract Stage Worker assignment while durable acquire evidence exists';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_complete_stage_worker_acquire(jsonb)
    FROM vela_stage_worker_control;
REVOKE SELECT ON stage_scheduler_claims FROM vela_attempt_coordinator_owner;
REVOKE EXECUTE ON FUNCTION vela_read_stage_assignment_execution(uuid,uuid)
    FROM vela_stage_worker_control;
REVOKE EXECUTE ON FUNCTION vela_read_stage_worker_acquire_authority(uuid)
    FROM vela_stage_worker_control;
REVOKE EXECUTE ON FUNCTION vela_begin_stage_worker_acquire(jsonb)
    FROM vela_stage_worker_control;
DROP FUNCTION vela_complete_stage_worker_acquire(jsonb);
DROP FUNCTION vela_read_stage_assignment_execution(uuid,uuid);
DROP FUNCTION vela_read_stage_worker_acquire_authority(uuid);
DROP FUNCTION vela_begin_stage_worker_acquire(jsonb);
DROP TRIGGER stage_worker_acquire_results_immutable ON stage_worker_acquire_results;
DROP TRIGGER stage_worker_acquire_intents_immutable ON stage_worker_acquire_intents;
DROP TABLE stage_worker_acquire_results;
DROP TABLE stage_worker_acquire_intents;
DROP TYPE stage_worker_acquire_result_kind;
-- +goose StatementEnd
