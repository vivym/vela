-- +goose Up
-- +goose StatementBegin
CREATE TYPE stage_materialization_lease_state AS ENUM (
    'ACTIVE', 'COMMITTED', 'EXPIRED', 'REVOKED'
);
CREATE TYPE stage_artifact_state AS ENUM ('COMMITTED', 'DELETION_BLOCKED', 'DELETED');
CREATE TYPE stage_artifact_pin_kind AS ENUM ('EXECUTION', 'FINALIZATION', 'CACHE');
CREATE TYPE stage_artifact_pin_state AS ENUM ('ACTIVE', 'RELEASED');
CREATE TYPE edge_buffer_credit_state AS ENUM ('HELD', 'RELEASED');
CREATE TYPE stage_transfer_ticket_state AS ENUM ('ACTIVE', 'CONSUMED', 'REVOKED', 'EXPIRED');

ALTER TABLE stage_storage_reservations
    ADD COLUMN reservation_policy jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(reservation_policy) = 'object'),
    ADD COLUMN expires_at timestamptz;
UPDATE stage_storage_reservations AS reservation
SET expires_at = job.job_expires_at
FROM jobs AS job
WHERE job.id = reservation.job_id;
ALTER TABLE stage_storage_reservations
    ALTER COLUMN expires_at SET NOT NULL,
    ADD CONSTRAINT stage_storage_reservations_expiry_after_creation
        CHECK (expires_at > created_at);

CREATE FUNCTION vela_bind_stage_storage_reservation_expiry() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    SELECT job.job_expires_at INTO STRICT NEW.expires_at
    FROM jobs AS job
    WHERE job.id = NEW.job_id
      AND job.organization_id = NEW.organization_id
      AND job.project_id = NEW.project_id;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_bind_stage_storage_reservation_expiry()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_bind_stage_storage_reservation_expiry() FROM PUBLIC;
CREATE TRIGGER stage_storage_reservations_bind_expiry
BEFORE INSERT ON stage_storage_reservations
FOR EACH ROW EXECUTE FUNCTION vela_bind_stage_storage_reservation_expiry();

CREATE TABLE stage_artifact_commands (
    command_id uuid PRIMARY KEY,
    command_kind text NOT NULL CHECK (
        command_kind IN (
            'SEAL', 'COMMIT', 'SOURCE_LOST', 'ISSUE_TRANSFER', 'CONSUME_TRANSFER'
        )
    ),
    attempt_id uuid,
    stage_run_id uuid,
    stage_attempt_id uuid,
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    result jsonb NOT NULL CHECK (jsonb_typeof(result) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (attempt_id, stage_run_id) REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (stage_run_id, stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id)
);

CREATE TABLE stage_materialization_leases (
    id uuid PRIMARY KEY,
    artifact_id uuid NOT NULL UNIQUE,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    stage_run_id uuid NOT NULL,
    stage_attempt_id uuid NOT NULL UNIQUE,
    output_port text NOT NULL CHECK (length(output_port) BETWEEN 1 AND 100),
    stage_interface_revision_id uuid NOT NULL REFERENCES stage_interface_revisions(id),
    object_key text NOT NULL UNIQUE CHECK (
        length(object_key) BETWEEN 1 AND 1024
        AND object_key LIKE 'artifacts/stage/%'
        AND object_key NOT LIKE '%//%'
    ),
    content_type text NOT NULL CHECK (length(content_type) BETWEEN 1 AND 200),
    expected_sha256 bytea NOT NULL CHECK (octet_length(expected_sha256) = 32),
    expected_size_bytes bigint NOT NULL CHECK (expected_size_bytes > 0),
    lineage_digest bytea NOT NULL CHECK (octet_length(lineage_digest) = 32),
    local_receipt_id text NOT NULL CHECK (length(local_receipt_id) BETWEEN 1 AND 1000),
    local_receipt_digest bytea NOT NULL CHECK (octet_length(local_receipt_digest) = 32),
    manifest_sha256 bytea NOT NULL CHECK (octet_length(manifest_sha256) = 32),
    token_digest bytea NOT NULL CHECK (octet_length(token_digest) = 32),
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    stage_fence bigint NOT NULL CHECK (stage_fence > 0),
    stage_version bigint NOT NULL CHECK (stage_version > 0),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    state stage_materialization_lease_state NOT NULL DEFAULT 'ACTIVE',
    committed_at timestamptz,
    revoked_at timestamptz,
    revoke_reason text CHECK (revoke_reason IS NULL OR length(revoke_reason) BETWEEN 1 AND 500),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (attempt_id, stage_run_id, stage_attempt_id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES attempts(organization_id, project_id, id),
    FOREIGN KEY (attempt_id, stage_run_id) REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (stage_run_id, stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id),
    CHECK (expires_at > issued_at),
    CHECK ((state = 'COMMITTED') = (committed_at IS NOT NULL)),
    CHECK ((state IN ('EXPIRED', 'REVOKED')) = (revoked_at IS NOT NULL)),
    CHECK ((revoked_at IS NULL) = (revoke_reason IS NULL))
);

CREATE UNIQUE INDEX stage_materialization_leases_one_active_per_run_idx
    ON stage_materialization_leases(stage_run_id) WHERE state = 'ACTIVE';

CREATE TABLE stage_artifacts (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    producer_stage_run_id uuid NOT NULL,
    producer_stage_attempt_id uuid NOT NULL UNIQUE,
    output_port text NOT NULL CHECK (length(output_port) BETWEEN 1 AND 100),
    stage_interface_revision_id uuid NOT NULL REFERENCES stage_interface_revisions(id),
    object_key text NOT NULL UNIQUE CHECK (
        length(object_key) BETWEEN 1 AND 1024
        AND object_key LIKE 'artifacts/stage/%'
        AND object_key NOT LIKE '%//%'
    ),
    object_version text NOT NULL CHECK (length(object_version) BETWEEN 1 AND 1000),
    content_type text NOT NULL CHECK (length(content_type) BETWEEN 1 AND 200),
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    lineage_digest bytea NOT NULL CHECK (octet_length(lineage_digest) = 32),
    state stage_artifact_state NOT NULL DEFAULT 'COMMITTED',
    committed_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    deletion_fence bigint NOT NULL DEFAULT 1 CHECK (deletion_fence > 0),
    deleted_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (object_key, object_version),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (producer_stage_run_id, id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES attempts(organization_id, project_id, id),
    FOREIGN KEY (attempt_id, producer_stage_run_id)
        REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (producer_stage_run_id, producer_stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id),
    CHECK (expires_at > committed_at),
    CHECK ((state = 'DELETED') = (deleted_at IS NOT NULL))
);

ALTER TABLE stage_runs
    ADD COLUMN winner_stage_artifact_id uuid,
    ADD CONSTRAINT stage_runs_winner_stage_artifact_fk
        FOREIGN KEY (id, winner_stage_artifact_id)
        REFERENCES stage_artifacts(producer_stage_run_id, id);

CREATE TABLE stage_artifact_inputs (
    stage_artifact_id uuid NOT NULL REFERENCES stage_artifacts(id),
    input_ordinal integer NOT NULL CHECK (input_ordinal >= 0),
    input_stage_artifact_id uuid REFERENCES stage_artifacts(id),
    root_input_digest bytea CHECK (
        root_input_digest IS NULL OR octet_length(root_input_digest) = 32
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (stage_artifact_id, input_ordinal),
    CHECK ((input_stage_artifact_id IS NULL) <> (root_input_digest IS NULL)),
    CHECK (stage_artifact_id <> input_stage_artifact_id)
);

CREATE TABLE stage_artifact_pins (
    id uuid PRIMARY KEY,
    stage_artifact_id uuid NOT NULL REFERENCES stage_artifacts(id),
    exact_object_version text NOT NULL CHECK (length(exact_object_version) BETWEEN 1 AND 1000),
    pin_kind stage_artifact_pin_kind NOT NULL,
    owner_job_id uuid NOT NULL REFERENCES jobs(id),
    owner_stage_run_id uuid REFERENCES stage_runs(id),
    state stage_artifact_pin_state NOT NULL DEFAULT 'ACTIVE',
    acquired_at timestamptz NOT NULL,
    released_at timestamptz,
    release_reason text CHECK (release_reason IS NULL OR length(release_reason) BETWEEN 1 AND 500),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK ((state = 'RELEASED') = (released_at IS NOT NULL)),
    CHECK ((released_at IS NULL) = (release_reason IS NULL))
);

CREATE UNIQUE INDEX stage_artifact_pins_one_active_execution_owner_idx
    ON stage_artifact_pins(stage_artifact_id, owner_job_id, owner_stage_run_id, pin_kind)
    NULLS NOT DISTINCT
    WHERE state = 'ACTIVE';

CREATE TABLE edge_buffer_credits (
    id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL REFERENCES attempts(id),
    source_stage_run_id uuid NOT NULL REFERENCES stage_runs(id),
    destination_stage_run_id uuid NOT NULL REFERENCES stage_runs(id),
    destination_port text NOT NULL CHECK (length(destination_port) BETWEEN 1 AND 100),
    buffer_class text NOT NULL CHECK (length(buffer_class) BETWEEN 1 AND 100),
    stage_artifact_id uuid NOT NULL REFERENCES stage_artifacts(id),
    held_count integer NOT NULL DEFAULT 1 CHECK (held_count = 1),
    held_bytes bigint NOT NULL CHECK (held_bytes > 0),
    state edge_buffer_credit_state NOT NULL DEFAULT 'HELD',
    acquired_at timestamptz NOT NULL,
    released_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (destination_stage_run_id, destination_port),
    FOREIGN KEY (attempt_id, source_stage_run_id, destination_stage_run_id)
        REFERENCES stage_dependencies(attempt_id, source_stage_run_id, destination_stage_run_id),
    CHECK ((state = 'RELEASED') = (released_at IS NOT NULL))
);

CREATE TABLE transfer_tickets (
    id uuid PRIMARY KEY,
    stage_artifact_id uuid NOT NULL REFERENCES stage_artifacts(id),
    stage_artifact_pin_id uuid NOT NULL REFERENCES stage_artifact_pins(id),
    exact_object_version text NOT NULL CHECK (length(exact_object_version) BETWEEN 1 AND 1000),
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    destination_worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    destination_worker_instance_epoch bigint NOT NULL CHECK (destination_worker_instance_epoch > 0),
    destination_model_residency_id uuid NOT NULL REFERENCES model_residencies(id),
    destination_model_runtime_epoch bigint NOT NULL CHECK (destination_model_runtime_epoch > 0),
    connector_revision_id uuid NOT NULL REFERENCES connector_revisions(id),
    token_digest bytea NOT NULL CHECK (octet_length(token_digest) = 32),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    state stage_transfer_ticket_state NOT NULL DEFAULT 'ACTIVE',
    consumed_at timestamptz,
    outcome_digest bytea CHECK (
        outcome_digest IS NULL OR octet_length(outcome_digest) = 32
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (token_digest),
    FOREIGN KEY (destination_worker_instance_id, destination_worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    CHECK (expires_at > issued_at),
    CHECK ((state = 'CONSUMED') = (consumed_at IS NOT NULL)),
    CHECK ((consumed_at IS NULL) = (outcome_digest IS NULL))
);

CREATE FUNCTION vela_reject_stage_artifact_immutable_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'stage_artifact_evidence_immutable',
        MESSAGE = 'StageArtifact evidence is immutable';
END
$$;
ALTER FUNCTION vela_reject_stage_artifact_immutable_mutation()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_reject_stage_artifact_immutable_mutation() FROM PUBLIC;

CREATE TRIGGER stage_artifact_commands_immutable
BEFORE UPDATE OR DELETE ON stage_artifact_commands
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_artifact_immutable_mutation();
CREATE TRIGGER stage_artifacts_identity_immutable
BEFORE UPDATE OR DELETE ON stage_artifacts
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_artifact_immutable_mutation();
CREATE TRIGGER stage_artifact_inputs_immutable
BEFORE UPDATE OR DELETE ON stage_artifact_inputs
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_artifact_immutable_mutation();

CREATE FUNCTION vela_seal_stage_output(p_command jsonb)
RETURNS TABLE (
    materialization_lease_id uuid,
    artifact_id uuid,
    object_key text,
    content_type text,
    expected_sha256 bytea,
    expected_size_bytes bigint,
    issued_at timestamptz,
    expires_at timestamptz,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_attempt_id uuid := (p_command ->> 'attempt_id')::uuid;
    v_stage_run_id uuid := (p_command ->> 'stage_run_id')::uuid;
    v_stage_attempt_id uuid := (p_command ->> 'stage_attempt_id')::uuid;
    v_allocation_id uuid := (p_command ->> 'stage_allocation_id')::uuid;
    v_stage_lease_id uuid := (p_command ->> 'stage_lease_id')::uuid;
    v_materialization_lease_id uuid := (p_command ->> 'materialization_lease_id')::uuid;
    v_artifact_id uuid := (p_command ->> 'artifact_id')::uuid;
    v_expected_attempt_fence bigint := (p_command ->> 'expected_attempt_fence')::bigint;
    v_expected_stage_fence bigint := (p_command ->> 'expected_stage_fence')::bigint;
    v_expected_stage_version bigint := (p_command ->> 'expected_stage_version')::bigint;
    v_output_port text := p_command ->> 'output_port';
    v_local_receipt_id text := p_command ->> 'local_receipt_id';
    v_local_receipt_digest bytea := decode(p_command ->> 'local_receipt_digest', 'hex');
    v_manifest_sha256 bytea := decode(p_command ->> 'manifest_sha256', 'hex');
    v_expected_sha256 bytea := decode(p_command ->> 'sha256', 'hex');
    v_lineage_digest bytea := decode(p_command ->> 'lineage_digest', 'hex');
    v_token_digest bytea := decode(p_command ->> 'token_digest', 'hex');
    v_size_bytes bigint := (p_command ->> 'size_bytes')::bigint;
    v_object_key text := p_command ->> 'object_key';
    v_content_type text := p_command ->> 'content_type';
    v_sealed_at timestamptz := (p_command ->> 'sealed_at')::timestamptz;
    v_expires_at timestamptz := (p_command ->> 'lease_expires_at')::timestamptz;
    v_request_digest bytea;
    v_existing stage_artifact_commands%ROWTYPE;
    v_job jobs%ROWTYPE;
    v_attempt attempts%ROWTYPE;
    v_run stage_runs%ROWTYPE;
    v_physical stage_attempts%ROWTYPE;
    v_allocation stage_allocations%ROWTYPE;
    v_lease stage_leases%ROWTYPE;
    v_reservation stage_storage_reservations%ROWTYPE;
    v_interface_id uuid;
    v_interface_max_bytes bigint;
    v_result jsonb;
BEGIN
    IF p_command IS NULL OR (p_command ->> 'schema_version')::integer <> 1
       OR v_command_id IS NULL OR v_attempt_id IS NULL OR v_stage_run_id IS NULL
       OR v_stage_attempt_id IS NULL OR v_allocation_id IS NULL OR v_stage_lease_id IS NULL
       OR v_materialization_lease_id IS NULL OR v_artifact_id IS NULL
       OR v_expected_attempt_fence <= 0 OR v_expected_stage_fence <= 0
       OR v_expected_stage_version <= 0 OR v_output_port IS NULL
       OR length(v_output_port) NOT BETWEEN 1 AND 100
       OR v_local_receipt_id IS NULL OR length(v_local_receipt_id) NOT BETWEEN 1 AND 1000
       OR octet_length(v_local_receipt_digest) <> 32
       OR octet_length(v_manifest_sha256) <> 32
       OR octet_length(v_expected_sha256) <> 32
       OR octet_length(v_lineage_digest) <> 32
       OR octet_length(v_token_digest) <> 32
       OR v_size_bytes <= 0 OR v_object_key NOT LIKE 'artifacts/stage/%'
       OR length(v_object_key) > 1024 OR v_object_key LIKE '%//%'
       OR v_content_type IS NULL OR length(v_content_type) NOT BETWEEN 1 AND 200
       OR v_sealed_at IS NULL OR v_expires_at <= v_sealed_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_output_seal_command_invalid',
            MESSAGE = 'Stage output seal command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing FROM stage_artifact_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'SEAL' OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_artifact_command_replay_mismatch',
                MESSAGE = 'StageArtifact command replay does not match';
        END IF;
        RETURN QUERY SELECT
            (v_existing.result ->> 'materialization_lease_id')::uuid,
            (v_existing.result ->> 'artifact_id')::uuid,
            v_existing.result ->> 'object_key', v_existing.result ->> 'content_type',
            decode(v_existing.result ->> 'sha256', 'hex'),
            (v_existing.result ->> 'size_bytes')::bigint,
            (v_existing.result ->> 'issued_at')::timestamptz,
            (v_existing.result ->> 'expires_at')::timestamptz, true;
        RETURN;
    END IF;

    SELECT attempt.job_id INTO v_job.id FROM attempts AS attempt WHERE attempt.id = v_attempt_id;
    SELECT job.* INTO v_job FROM jobs AS job WHERE job.id = v_job.id FOR UPDATE;
    SELECT attempt.* INTO v_attempt FROM attempts AS attempt WHERE attempt.id = v_attempt_id FOR UPDATE;
    SELECT run.* INTO v_run FROM stage_runs AS run
    WHERE run.id = v_stage_run_id AND run.attempt_id = v_attempt_id FOR UPDATE;
    SELECT physical.* INTO v_physical FROM stage_attempts AS physical
    WHERE physical.id = v_stage_attempt_id AND physical.stage_run_id = v_stage_run_id FOR UPDATE;
    SELECT allocation.* INTO v_allocation FROM stage_allocations AS allocation
    WHERE allocation.id = v_allocation_id AND allocation.stage_attempt_id = v_stage_attempt_id FOR UPDATE;
    SELECT lease.* INTO v_lease FROM stage_leases AS lease
    WHERE lease.id = v_stage_lease_id AND lease.stage_attempt_id = v_stage_attempt_id FOR UPDATE;
    SELECT reservation.* INTO v_reservation
    FROM stage_storage_reservations AS reservation
    WHERE reservation.attempt_id = v_attempt_id FOR UPDATE;
    SELECT (definition.output_ports ->> v_output_port)::uuid, interface.max_bytes
    INTO v_interface_id, v_interface_max_bytes
    FROM stage_definition_revisions AS definition
    JOIN stage_interface_revisions AS interface
      ON interface.id = (definition.output_ports ->> v_output_port)::uuid
    WHERE definition.id = v_run.stage_definition_revision_id;
    IF v_attempt.execution_authority_kind <> 'STAGE_GRAPH'
       OR v_attempt.graph_state <> 'RUNNING' OR v_attempt.fence <> v_expected_attempt_fence
       OR v_attempt.fence <> v_job.current_fence OR v_job.state <> 'RUNNING'
       OR v_run.state <> 'RUNNING' OR v_run.fence <> v_expected_stage_fence
       OR v_run.version <> v_expected_stage_version OR v_physical.state <> 'RUNNING'
       OR v_allocation.state <> 'ALLOCATED' OR v_lease.state <> 'ACTIVE'
       OR v_lease.stage_allocation_id <> v_allocation.id
       OR v_lease.attempt_fence <> v_attempt.fence OR v_lease.stage_fence <> v_run.fence
       OR v_sealed_at < v_physical.started_at OR v_sealed_at >= v_lease.expires_at
       OR v_interface_id IS NULL OR v_size_bytes > v_interface_max_bytes
       OR v_reservation.id IS NULL OR v_reservation.state <> 'RESERVED'
       OR v_reservation.expires_at <= v_sealed_at
       OR v_expires_at > v_reservation.expires_at
       OR v_reservation.consumed_bytes + v_size_bytes > v_reservation.reserved_bytes THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_output_seal_authority_stale',
            MESSAGE = 'Stage output seal authority, interface, or bounds are stale';
    END IF;

    UPDATE stage_attempts SET state = 'OUTPUT_SEALED', output_sealed_at = v_sealed_at,
        updated_at = v_sealed_at WHERE id = v_physical.id AND state = 'RUNNING';
    UPDATE stage_allocations SET state = 'RELEASED', released_at = v_sealed_at,
        release_reason = 'OUTPUT_SEALED_LOCAL'
    WHERE id = v_allocation.id AND state = 'ALLOCATED';
    UPDATE stage_leases SET state = 'REVOKED', revoked_at = v_sealed_at,
        revoke_reason = 'OUTPUT_SEALED_LOCAL'
    WHERE id = v_lease.id AND state = 'ACTIVE';
    UPDATE stage_runs SET state = 'MATERIALIZING', version = version + 1,
        updated_at = v_sealed_at
    WHERE id = v_run.id AND state = 'RUNNING' AND version = v_run.version;

    INSERT INTO stage_materialization_leases (
        id, artifact_id, organization_id, project_id, job_id, attempt_id,
        stage_run_id, stage_attempt_id, output_port, stage_interface_revision_id,
        object_key, content_type, expected_sha256, expected_size_bytes,
        lineage_digest, local_receipt_id, local_receipt_digest, manifest_sha256,
        token_digest, attempt_fence, stage_fence, stage_version, issued_at, expires_at
    ) VALUES (
        v_materialization_lease_id, v_artifact_id, v_job.organization_id,
        v_job.project_id, v_job.id, v_attempt.id, v_run.id, v_physical.id,
        v_output_port, v_interface_id, v_object_key, v_content_type,
        v_expected_sha256, v_size_bytes, v_lineage_digest, v_local_receipt_id,
        v_local_receipt_digest, v_manifest_sha256, v_token_digest,
        v_attempt.fence, v_run.fence,
        v_run.version + 1, v_sealed_at, v_expires_at
    );
    v_result := jsonb_build_object(
        'materialization_lease_id', v_materialization_lease_id,
        'artifact_id', v_artifact_id, 'object_key', v_object_key,
        'content_type', v_content_type, 'sha256', encode(v_expected_sha256, 'hex'),
        'size_bytes', v_size_bytes, 'issued_at', v_sealed_at, 'expires_at', v_expires_at
    );
    INSERT INTO stage_artifact_commands (
        command_id, command_kind, attempt_id, stage_run_id, stage_attempt_id,
        request_digest, result
    ) VALUES (
        v_command_id, 'SEAL', v_attempt.id, v_run.id, v_physical.id,
        v_request_digest, v_result
    );
    RETURN QUERY SELECT v_materialization_lease_id, v_artifact_id, v_object_key,
        v_content_type, v_expected_sha256, v_size_bytes, v_sealed_at, v_expires_at, false;
END
$$;
ALTER FUNCTION vela_seal_stage_output(jsonb) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_seal_stage_output(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_seal_stage_output(jsonb) TO vela_stage_artifact;

CREATE FUNCTION vela_is_stage_materialization_authority_active(p_claim jsonb)
RETURNS boolean
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_lease_id uuid := (p_claim ->> 'materialization_lease_id')::uuid;
    v_artifact_id uuid := (p_claim ->> 'artifact_id')::uuid;
    v_token_digest bytea := decode(p_claim ->> 'token_digest', 'hex');
    v_worker_id uuid := (p_claim ->> 'source_worker_instance_id')::uuid;
    v_worker_epoch bigint := (p_claim ->> 'source_worker_instance_epoch')::bigint;
    v_member_id uuid := (p_claim ->> 'source_worker_member_id')::uuid;
    v_member_epoch bigint := (p_claim ->> 'source_worker_member_epoch')::bigint;
    v_control_session_epoch bigint := (p_claim ->> 'control_session_epoch')::bigint;
BEGIN
    IF p_claim IS NULL OR (p_claim ->> 'schema_version')::integer <> 1
       OR v_lease_id IS NULL OR v_artifact_id IS NULL
       OR octet_length(v_token_digest) <> 32 OR v_worker_id IS NULL
       OR v_worker_epoch <= 0 OR v_member_id IS NULL OR v_member_epoch <= 0
       OR v_control_session_epoch <= 0 THEN
        RETURN false;
    END IF;
    RETURN EXISTS (
        SELECT 1
        FROM stage_materialization_leases AS materialization
        JOIN stage_allocations AS allocation
          ON allocation.stage_attempt_id = materialization.stage_attempt_id
        JOIN worker_instances AS worker ON worker.id = allocation.worker_instance_id
        JOIN worker_members AS member
          ON member.worker_instance_id = worker.id AND member.id = v_member_id
        WHERE materialization.id = v_lease_id
          AND materialization.artifact_id = v_artifact_id
          AND materialization.token_digest = v_token_digest
          AND materialization.state = 'ACTIVE'
          AND statement_timestamp() < materialization.expires_at
          AND allocation.worker_instance_id = v_worker_id
          AND allocation.worker_instance_epoch = v_worker_epoch
          AND worker.instance_epoch = v_worker_epoch
          AND worker.control_session_epoch = v_control_session_epoch
          AND worker.lifecycle_state = 'READY'
          AND worker.reachability_state = 'CONNECTED'
          AND member.worker_instance_epoch = v_worker_epoch
          AND member.member_epoch = v_member_epoch
          AND member.readiness = 'READY'
    );
END
$$;
ALTER FUNCTION vela_is_stage_materialization_authority_active(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_is_stage_materialization_authority_active(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_is_stage_materialization_authority_active(jsonb)
    TO vela_stage_artifact;

CREATE FUNCTION vela_commit_stage_artifact(p_command jsonb)
RETURNS TABLE (
    artifact_id uuid,
    object_key text,
    object_version text,
    sha256 bytea,
    size_bytes bigint,
    committed_at timestamptz,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_progress_receipt_id uuid := (p_command ->> 'progress_receipt_id')::uuid;
    v_lease_id uuid := (p_command ->> 'materialization_lease_id')::uuid;
    v_artifact_id uuid := (p_command ->> 'artifact_id')::uuid;
    v_object_key text := p_command ->> 'object_key';
    v_object_version text := p_command ->> 'object_version';
    v_sha256 bytea := decode(p_command ->> 'sha256', 'hex');
    v_size_bytes bigint := (p_command ->> 'size_bytes')::bigint;
    v_token_digest bytea := decode(p_command ->> 'token_digest', 'hex');
    v_committed_at timestamptz := (p_command ->> 'committed_at')::timestamptz;
    v_request_digest bytea;
    v_existing stage_artifact_commands%ROWTYPE;
    v_materialization stage_materialization_leases%ROWTYPE;
    v_job jobs%ROWTYPE;
    v_attempt attempts%ROWTYPE;
    v_run stage_runs%ROWTYPE;
    v_physical stage_attempts%ROWTYPE;
    v_reservation stage_storage_reservations%ROWTYPE;
    v_result jsonb;
BEGIN
    IF p_command IS NULL OR (p_command ->> 'schema_version')::integer <> 1
       OR v_command_id IS NULL OR v_progress_receipt_id IS NULL OR v_lease_id IS NULL
       OR v_artifact_id IS NULL OR v_object_key IS NULL OR v_object_version IS NULL
       OR length(v_object_version) NOT BETWEEN 1 AND 1000
       OR octet_length(v_sha256) <> 32 OR octet_length(v_token_digest) <> 32
       OR v_size_bytes <= 0 OR v_committed_at IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_artifact_commit_command_invalid',
            MESSAGE = 'StageArtifact commit command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing FROM stage_artifact_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'COMMIT' OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_artifact_command_replay_mismatch',
                MESSAGE = 'StageArtifact command replay does not match';
        END IF;
        RETURN QUERY SELECT
            (v_existing.result ->> 'artifact_id')::uuid,
            v_existing.result ->> 'object_key', v_existing.result ->> 'object_version',
            decode(v_existing.result ->> 'sha256', 'hex'),
            (v_existing.result ->> 'size_bytes')::bigint,
            (v_existing.result ->> 'committed_at')::timestamptz, true;
        RETURN;
    END IF;

    SELECT materialization.* INTO v_materialization
    FROM stage_materialization_leases AS materialization
    WHERE materialization.id = v_lease_id FOR UPDATE;
    SELECT job.* INTO v_job FROM jobs AS job WHERE job.id = v_materialization.job_id FOR UPDATE;
    SELECT attempt.* INTO v_attempt FROM attempts AS attempt
    WHERE attempt.id = v_materialization.attempt_id FOR UPDATE;
    SELECT run.* INTO v_run FROM stage_runs AS run
    WHERE run.id = v_materialization.stage_run_id FOR UPDATE;
    SELECT physical.* INTO v_physical FROM stage_attempts AS physical
    WHERE physical.id = v_materialization.stage_attempt_id FOR UPDATE;
    SELECT reservation.* INTO v_reservation FROM stage_storage_reservations AS reservation
    WHERE reservation.attempt_id = v_attempt.id FOR UPDATE;
    IF v_materialization.id IS NULL OR v_materialization.state <> 'ACTIVE'
       OR v_materialization.artifact_id <> v_artifact_id
       OR v_materialization.object_key <> v_object_key
       OR v_materialization.expected_sha256 <> v_sha256
       OR v_materialization.expected_size_bytes <> v_size_bytes
       OR v_materialization.token_digest <> v_token_digest
       OR v_committed_at < v_materialization.issued_at
       OR v_committed_at >= v_materialization.expires_at
       OR v_job.state <> 'RUNNING' OR v_job.current_fence <> v_materialization.attempt_fence
       OR v_attempt.graph_state <> 'RUNNING' OR v_attempt.fence <> v_materialization.attempt_fence
       OR v_run.state <> 'MATERIALIZING' OR v_run.fence <> v_materialization.stage_fence
       OR v_run.version <> v_materialization.stage_version
       OR v_physical.state <> 'OUTPUT_SEALED'
       OR v_reservation.state <> 'RESERVED'
       OR v_reservation.expires_at <= v_committed_at
       OR v_reservation.consumed_bytes + v_size_bytes > v_reservation.reserved_bytes THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_artifact_commit_authority_stale',
            MESSAGE = 'StageArtifact commit authority, version, or capacity is stale';
    END IF;

    INSERT INTO stage_artifacts (
        id, organization_id, project_id, job_id, attempt_id,
        producer_stage_run_id, producer_stage_attempt_id, output_port,
        stage_interface_revision_id, object_key, object_version, content_type, sha256,
        size_bytes, lineage_digest, committed_at, expires_at
    ) VALUES (
        v_artifact_id, v_materialization.organization_id, v_materialization.project_id,
        v_materialization.job_id, v_materialization.attempt_id,
        v_materialization.stage_run_id, v_materialization.stage_attempt_id,
        v_materialization.output_port, v_materialization.stage_interface_revision_id,
        v_object_key, v_object_version, v_materialization.content_type, v_sha256, v_size_bytes,
        v_materialization.lineage_digest, v_committed_at, v_reservation.expires_at
    );
    INSERT INTO stage_artifact_inputs (
        stage_artifact_id, input_ordinal, input_stage_artifact_id
    )
    SELECT v_artifact_id, row_number() OVER (ORDER BY dependency.destination_port) - 1,
        source.winner_stage_artifact_id
    FROM stage_dependencies AS dependency
    JOIN stage_runs AS source ON source.id = dependency.source_stage_run_id
    WHERE dependency.destination_stage_run_id = v_run.id
      AND source.winner_stage_artifact_id IS NOT NULL;

    v_result := jsonb_build_object(
        'artifact_id', v_artifact_id, 'object_key', v_object_key,
        'object_version', v_object_version, 'sha256', encode(v_sha256, 'hex'),
        'size_bytes', v_size_bytes, 'committed_at', v_committed_at
    );
    INSERT INTO stage_artifact_commands (
        command_id, command_kind, attempt_id, stage_run_id, stage_attempt_id,
        request_digest, result
    ) VALUES (
        v_command_id, 'COMMIT', v_attempt.id, v_run.id, v_physical.id,
        v_request_digest, v_result
    );
    INSERT INTO attempt_coordinator_commands (
        command_id, command_kind, job_id, attempt_id, stage_run_id,
        request_digest, result
    ) VALUES (
        v_command_id, 'COMPLETE', v_job.id, v_attempt.id, v_run.id,
        v_request_digest, jsonb_build_object(
            'stage_run_id', v_run.id, 'stage_attempt_id', v_physical.id,
            'stage_state', 'SUCCEEDED', 'stage_fence', v_run.fence,
            'stage_version', v_run.version + 1
        )
    );
    INSERT INTO stage_progress_receipts (
        id, attempt_id, stage_run_id, stage_attempt_id, progress_kind,
        source_identity, output_digest, command_id, committed_at
    ) VALUES (
        v_progress_receipt_id, v_attempt.id, v_run.id, v_physical.id,
        'PHYSICAL_OUTPUT', 'stage-artifact:' || v_artifact_id::text ||
        '@' || v_object_version, v_sha256, v_command_id, v_committed_at
    );
    UPDATE stage_materialization_leases SET state = 'COMMITTED',
        committed_at = v_committed_at WHERE id = v_materialization.id AND state = 'ACTIVE';
    UPDATE stage_storage_reservations SET consumed_bytes = consumed_bytes + v_size_bytes,
        updated_at = v_committed_at WHERE id = v_reservation.id AND state = 'RESERVED';
    UPDATE stage_attempts SET state = 'SUCCEEDED', ended_at = v_committed_at,
        updated_at = v_committed_at
    WHERE id = v_physical.id AND state = 'OUTPUT_SEALED';
    UPDATE stage_runs SET state = 'SUCCEEDED', winner_stage_attempt_id = v_physical.id,
        winner_output_identity = v_progress_receipt_id,
        winner_stage_artifact_id = v_artifact_id,
        version = version + 1, updated_at = v_committed_at
    WHERE id = v_run.id AND state = 'MATERIALIZING' AND version = v_run.version;
    UPDATE stage_dependencies SET satisfied_progress_receipt_id = v_progress_receipt_id
    WHERE attempt_id = v_attempt.id AND source_stage_run_id = v_run.id
      AND satisfied_progress_receipt_id IS NULL;

    INSERT INTO stage_artifact_pins (
        id, stage_artifact_id, exact_object_version, pin_kind,
        owner_job_id, owner_stage_run_id, acquired_at
    )
    SELECT gen_random_uuid(), v_artifact_id, v_object_version, 'EXECUTION',
        v_job.id, dependency.destination_stage_run_id, v_committed_at
    FROM stage_dependencies AS dependency
    WHERE dependency.attempt_id = v_attempt.id
      AND dependency.source_stage_run_id = v_run.id;
    INSERT INTO edge_buffer_credits (
        id, attempt_id, source_stage_run_id, destination_stage_run_id,
        destination_port, buffer_class, stage_artifact_id, held_bytes, acquired_at
    )
    SELECT gen_random_uuid(), v_attempt.id, dependency.source_stage_run_id,
        dependency.destination_stage_run_id, dependency.destination_port,
        edge.buffer_class, v_artifact_id, v_size_bytes, v_committed_at
    FROM stage_dependencies AS dependency
    JOIN stage_runs AS source ON source.id = dependency.source_stage_run_id
    JOIN stage_runs AS destination ON destination.id = dependency.destination_stage_run_id
    JOIN execution_graph_edges AS edge
      ON edge.execution_graph_revision_id = source.execution_graph_revision_id
     AND edge.source_stage_key = source.stage_key
     AND edge.source_port = dependency.source_port
     AND edge.destination_stage_key = destination.stage_key
     AND edge.destination_port = dependency.destination_port
    WHERE dependency.attempt_id = v_attempt.id
      AND dependency.source_stage_run_id = v_run.id;
    UPDATE stage_runs AS destination
    SET state = 'READY', version = destination.version + 1, updated_at = v_committed_at
    WHERE destination.attempt_id = v_attempt.id AND destination.state = 'BLOCKED'
      AND EXISTS (
          SELECT 1 FROM stage_dependencies AS inbound
          WHERE inbound.destination_stage_run_id = destination.id
      )
      AND NOT EXISTS (
          SELECT 1 FROM stage_dependencies AS inbound
          WHERE inbound.destination_stage_run_id = destination.id
            AND inbound.satisfied_progress_receipt_id IS NULL
      );
    PERFORM vela_private.vela_begin_stage_graph_finalization(
        v_attempt.id, v_run.id, v_committed_at
    );
    RETURN QUERY SELECT v_artifact_id, v_object_key, v_object_version,
        v_sha256, v_size_bytes, v_committed_at, false;
END
$$;
ALTER FUNCTION vela_commit_stage_artifact(jsonb) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_commit_stage_artifact(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_commit_stage_artifact(jsonb) TO vela_stage_artifact;

CREATE FUNCTION vela_private.vela_insert_stage_materialization_source_lost_event(
    p_event_id uuid,
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_job_version bigint,
    p_attempt_id uuid,
    p_attempt_number integer,
    p_attempt_fence bigint,
    p_job_fence bigint,
    p_occurred_at timestamptz
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_payload bytea;
BEGIN
    v_payload :=
        vela_private.vela_proto_string(1, p_organization_id::text)
        || vela_private.vela_proto_string(2, p_project_id::text)
        || vela_private.vela_proto_string(3, p_job_id::text)
        || vela_private.vela_proto_string(4, p_attempt_id::text)
        || vela_private.vela_proto_uint(5, p_attempt_number)
        || vela_private.vela_proto_uint(6, p_attempt_fence)
        || vela_private.vela_proto_uint(7, p_job_fence)
        || vela_private.vela_proto_string(8, 'LOCAL_MATERIALIZATION_SOURCE_LOST')
        || vela_private.vela_proto_string(9, 'FAILED')
        || vela_private.vela_proto_uint(10, 0)
        || vela_private.vela_proto_uint(11, 0)
        || vela_private.vela_proto_bytes(
            12,
            vela_private.vela_proto_timestamp(p_occurred_at)
        )
        || vela_private.vela_proto_uint(13, 0)
        || vela_private.vela_proto_uint(14, 0);
    PERFORM vela_private.vela_insert_canonical_cancellation_event(
        p_event_id,
        p_organization_id,
        p_project_id,
        p_job_id,
        p_job_version,
        'job.failed',
        p_occurred_at,
        24,
        v_payload
    );
END
$$;
ALTER FUNCTION vela_private.vela_insert_stage_materialization_source_lost_event(
    uuid, uuid, uuid, uuid, bigint, uuid, integer, bigint, bigint, timestamptz
) OWNER TO vela_internal;
REVOKE ALL ON FUNCTION vela_private.vela_insert_stage_materialization_source_lost_event(
    uuid, uuid, uuid, uuid, bigint, uuid, integer, bigint, bigint, timestamptz
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_private.vela_insert_stage_materialization_source_lost_event(
    uuid, uuid, uuid, uuid, bigint, uuid, integer, bigint, bigint, timestamptz
) TO vela_attempt_coordinator_owner;

CREATE FUNCTION vela_fail_stage_materialization_source(p_command jsonb)
RETURNS TABLE (
    stage_run_id uuid,
    stage_state text,
    stage_fence bigint,
    stage_version bigint,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_lease_id uuid := (p_command ->> 'materialization_lease_id')::uuid;
    v_token_digest bytea := decode(p_command ->> 'token_digest', 'hex');
    v_failure_fingerprint bytea := decode(p_command ->> 'failure_fingerprint', 'hex');
    v_consumed_resource_units bigint := (p_command ->> 'consumed_resource_units')::bigint;
    v_lost_at timestamptz := (p_command ->> 'lost_at')::timestamptz;
    v_retry_at timestamptz := (p_command ->> 'retry_at')::timestamptz;
    v_request_digest bytea;
    v_existing stage_artifact_commands%ROWTYPE;
    v_materialization stage_materialization_leases%ROWTYPE;
    v_job jobs%ROWTYPE;
    v_attempt attempts%ROWTYPE;
    v_run stage_runs%ROWTYPE;
    v_physical stage_attempts%ROWTYPE;
    v_stage_budget stage_retry_budgets%ROWTYPE;
    v_attempt_budget attempt_retry_budgets%ROWTYPE;
    v_result jsonb;
    v_retry_allowed boolean;
    v_failure_event_id uuid;
BEGIN
    IF v_command_id IS NULL OR v_lease_id IS NULL
       OR octet_length(v_token_digest) <> 32
       OR octet_length(v_failure_fingerprint) <> 32 OR v_consumed_resource_units <= 0
       OR v_lost_at IS NULL OR v_retry_at <= v_lost_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_materialization_source_lost_invalid',
            MESSAGE = 'Stage materialization source-loss command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing FROM stage_artifact_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'SOURCE_LOST'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_artifact_command_replay_mismatch',
                MESSAGE = 'StageArtifact command replay does not match';
        END IF;
        RETURN QUERY SELECT
            (v_existing.result ->> 'stage_run_id')::uuid,
            v_existing.result ->> 'stage_state',
            (v_existing.result ->> 'stage_fence')::bigint,
            (v_existing.result ->> 'stage_version')::bigint, true;
        RETURN;
    END IF;
    SELECT materialization.* INTO v_materialization
    FROM stage_materialization_leases AS materialization
    WHERE materialization.id = v_lease_id FOR UPDATE;
    SELECT job.* INTO v_job FROM jobs AS job WHERE job.id = v_materialization.job_id FOR UPDATE;
    SELECT attempt.* INTO v_attempt FROM attempts AS attempt
    WHERE attempt.id = v_materialization.attempt_id FOR UPDATE;
    SELECT run.* INTO v_run FROM stage_runs AS run
    WHERE run.id = v_materialization.stage_run_id FOR UPDATE;
    SELECT physical.* INTO v_physical FROM stage_attempts AS physical
    WHERE physical.id = v_materialization.stage_attempt_id FOR UPDATE;
    SELECT budget.* INTO v_stage_budget FROM stage_retry_budgets AS budget
    WHERE budget.stage_run_id = v_run.id FOR UPDATE;
    SELECT budget.* INTO v_attempt_budget FROM attempt_retry_budgets AS budget
    WHERE budget.attempt_id = v_attempt.id FOR UPDATE;
    IF v_materialization.id IS NULL OR v_materialization.state <> 'ACTIVE'
       OR v_materialization.token_digest <> v_token_digest
       OR v_job.state <> 'RUNNING' OR v_job.current_fence <> v_materialization.attempt_fence
       OR v_attempt.graph_state <> 'RUNNING' OR v_attempt.fence <> v_materialization.attempt_fence
       OR v_run.state <> 'MATERIALIZING' OR v_run.fence <> v_materialization.stage_fence
       OR v_run.version <> v_materialization.stage_version
       OR v_physical.state <> 'OUTPUT_SEALED'
       OR v_stage_budget.stage_run_id IS NULL
       OR v_attempt_budget.attempt_id IS NULL
       OR v_lost_at < v_materialization.issued_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_materialization_source_lost_stale',
            MESSAGE = 'Stage materialization source-loss authority or retry budget is stale';
    END IF;
    v_retry_allowed := v_stage_budget.state = 'ACTIVE'
        AND v_stage_budget.attempts_consumed < v_stage_budget.max_attempts
        AND v_attempt_budget.state = 'ACTIVE'
        AND v_attempt_budget.consumed_resource_units + v_consumed_resource_units
            < v_attempt_budget.max_resource_units
        AND v_retry_at < v_job.job_expires_at;
    UPDATE stage_materialization_leases SET state = 'REVOKED', revoked_at = v_lost_at,
        revoke_reason = 'LOCAL_SOURCE_LOST'
    WHERE id = v_materialization.id AND state = 'ACTIVE';
    UPDATE stage_attempts SET state = 'LOST', ended_at = v_lost_at,
        failure_class = 'LOCAL_MATERIALIZATION_SOURCE_LOST',
        failure_fingerprint = v_failure_fingerprint, updated_at = v_lost_at
    WHERE id = v_physical.id AND state = 'OUTPUT_SEALED';
    UPDATE attempt_retry_budgets SET
        consumed_resource_units = LEAST(
            max_resource_units,
            consumed_resource_units + v_consumed_resource_units
        ),
        state = CASE WHEN v_retry_allowed
            THEN 'ACTIVE'::stage_retry_budget_state
            WHEN consumed_resource_units + v_consumed_resource_units >= max_resource_units
            THEN 'EXHAUSTED'::stage_retry_budget_state
            ELSE 'CANCELED'::stage_retry_budget_state END,
        version = version + 1,
        updated_at = v_lost_at
    WHERE attempt_id = v_attempt.id;
    IF v_retry_allowed THEN
        UPDATE stage_runs SET state = 'RETRY_WAIT', fence = fence + 1,
            retry_count = retry_count + 1, next_retry_at = v_retry_at,
            version = version + 1, updated_at = v_lost_at
        WHERE id = v_run.id AND state = 'MATERIALIZING' AND version = v_run.version;
        v_result := jsonb_build_object(
            'stage_run_id', v_run.id, 'stage_state', 'RETRY_WAIT',
            'stage_fence', v_run.fence + 1, 'stage_version', v_run.version + 1
        );
    ELSE
        UPDATE stage_retry_budgets AS budget SET state = CASE
                WHEN budget.attempts_consumed = budget.max_attempts
                THEN 'EXHAUSTED'::stage_retry_budget_state
                ELSE 'CANCELED'::stage_retry_budget_state END,
            version = budget.version + 1, updated_at = v_lost_at
        WHERE budget.stage_run_id = v_run.id;
        UPDATE stage_runs AS run SET
            state = CASE WHEN run.id = v_run.id
                THEN 'FAILED'::stage_run_state
                ELSE 'CANCELED'::stage_run_state END,
            fence = run.fence + 1, next_retry_at = NULL,
            version = run.version + 1, updated_at = v_lost_at
        WHERE run.attempt_id = v_attempt.id
          AND run.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED');
        UPDATE stage_storage_reservations SET state = 'RELEASED', updated_at = v_lost_at
        WHERE attempt_id = v_attempt.id AND state = 'RESERVED';
        UPDATE attempts SET state = 'FAILED', graph_state = 'FAILED', ended_at = v_lost_at,
            updated_at = v_lost_at
        WHERE id = v_attempt.id AND graph_state = 'RUNNING';
        v_failure_event_id := gen_random_uuid();
        PERFORM vela_private.vela_insert_stage_materialization_source_lost_event(
            v_failure_event_id,
            v_job.organization_id,
            v_job.project_id,
            v_job.id,
            v_job.version + 1,
            v_attempt.id,
            v_attempt.attempt_number,
            v_attempt.fence,
            v_job.current_fence,
            v_lost_at
        );
        UPDATE jobs SET state = 'FAILED', version = version + 1, updated_at = v_lost_at
        WHERE id = v_job.id AND state = 'RUNNING'
          AND current_fence = v_attempt.fence;
        UPDATE projects SET running_count = running_count - 1
        WHERE id = v_job.project_id AND organization_id = v_job.organization_id
          AND running_count > 0;
        UPDATE credit_reservations SET state = 'RELEASED', updated_at = v_lost_at
        WHERE job_id = v_job.id AND state = 'RESERVED';
        UPDATE organization_credit_accounts SET
            reserved_minor = reserved_minor - v_job.pricing_quoted_amount_minor,
            version = version + 1, updated_at = v_lost_at
        WHERE organization_id = v_job.organization_id
          AND currency = v_job.pricing_currency
          AND reserved_minor >= v_job.pricing_quoted_amount_minor;
        v_result := jsonb_build_object(
            'stage_run_id', v_run.id, 'stage_state', 'FAILED',
            'stage_fence', v_run.fence + 1, 'stage_version', v_run.version + 1
        );
    END IF;
    INSERT INTO stage_artifact_commands (
        command_id, command_kind, attempt_id, stage_run_id, stage_attempt_id,
        request_digest, result
    ) VALUES (
        v_command_id, 'SOURCE_LOST', v_attempt.id, v_run.id, v_physical.id,
        v_request_digest, v_result
    );
    RETURN QUERY SELECT
        v_run.id,
        v_result ->> 'stage_state',
        (v_result ->> 'stage_fence')::bigint,
        (v_result ->> 'stage_version')::bigint,
        false;
END
$$;
ALTER FUNCTION vela_fail_stage_materialization_source(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_fail_stage_materialization_source(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_fail_stage_materialization_source(jsonb)
    TO vela_stage_artifact;

CREATE FUNCTION vela_issue_stage_transfer_ticket(p_command jsonb)
RETURNS TABLE (ticket_id uuid, expires_at timestamptz, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_ticket_id uuid := (p_command ->> 'ticket_id')::uuid;
    v_artifact_id uuid := (p_command ->> 'artifact_id')::uuid;
    v_pin_id uuid := (p_command ->> 'pin_id')::uuid;
    v_worker_id uuid := (p_command ->> 'destination_worker_instance_id')::uuid;
    v_worker_epoch bigint := (p_command ->> 'destination_worker_instance_epoch')::bigint;
    v_residency_id uuid := (p_command ->> 'destination_model_residency_id')::uuid;
    v_runtime_epoch bigint := (p_command ->> 'destination_model_runtime_epoch')::bigint;
    v_connector_id uuid := (p_command ->> 'connector_revision_id')::uuid;
    v_token_digest bytea := decode(p_command ->> 'token_digest', 'hex');
    v_issued_at timestamptz := (p_command ->> 'issued_at')::timestamptz;
    v_expires_at timestamptz := (p_command ->> 'expires_at')::timestamptz;
    v_request_digest bytea;
    v_existing stage_artifact_commands%ROWTYPE;
    v_artifact stage_artifacts%ROWTYPE;
    v_pin stage_artifact_pins%ROWTYPE;
BEGIN
    IF v_command_id IS NULL OR v_ticket_id IS NULL OR v_artifact_id IS NULL
       OR v_pin_id IS NULL OR v_worker_id IS NULL OR v_worker_epoch <= 0
       OR v_residency_id IS NULL OR v_runtime_epoch <= 0 OR v_connector_id IS NULL
       OR octet_length(v_token_digest) <> 32 OR v_issued_at IS NULL
       OR v_expires_at <= v_issued_at OR v_expires_at > v_issued_at + interval '15 minutes' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_transfer_ticket_command_invalid',
            MESSAGE = 'Stage TransferTicket command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing FROM stage_artifact_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'ISSUE_TRANSFER'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_artifact_command_replay_mismatch',
                MESSAGE = 'StageArtifact command replay does not match';
        END IF;
        RETURN QUERY SELECT (v_existing.result ->> 'ticket_id')::uuid,
            (v_existing.result ->> 'expires_at')::timestamptz, true;
        RETURN;
    END IF;
    SELECT artifact.* INTO v_artifact FROM stage_artifacts AS artifact
    WHERE artifact.id = v_artifact_id FOR SHARE;
    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin
    WHERE pin.id = v_pin_id AND pin.stage_artifact_id = v_artifact_id FOR SHARE;
    IF v_artifact.state <> 'COMMITTED' OR v_artifact.expires_at <= v_expires_at
       OR v_pin.state <> 'ACTIVE' OR v_pin.exact_object_version <> v_artifact.object_version
       OR NOT vela_worker_instance_authority_matches(
           v_worker_id, v_worker_epoch,
           (SELECT epoch.device_set_digest FROM worker_instance_epochs AS epoch
            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),
           (SELECT epoch.membership_digest FROM worker_instance_epochs AS epoch
            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),
           v_residency_id, v_runtime_epoch
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_transfer_ticket_authority_stale',
            MESSAGE = 'Stage TransferTicket pin, Artifact, or destination authority is stale';
    END IF;
    INSERT INTO transfer_tickets (
        id, stage_artifact_id, stage_artifact_pin_id, exact_object_version,
        sha256, size_bytes, destination_worker_instance_id,
        destination_worker_instance_epoch, destination_model_residency_id,
        destination_model_runtime_epoch, connector_revision_id, token_digest,
        issued_at, expires_at
    ) VALUES (
        v_ticket_id, v_artifact.id, v_pin.id, v_artifact.object_version,
        v_artifact.sha256, v_artifact.size_bytes, v_worker_id, v_worker_epoch,
        v_residency_id, v_runtime_epoch, v_connector_id, v_token_digest,
        v_issued_at, v_expires_at
    );
    INSERT INTO stage_artifact_commands (
        command_id, command_kind, attempt_id, stage_run_id, request_digest, result
    ) VALUES (
        v_command_id, 'ISSUE_TRANSFER', v_artifact.attempt_id,
        v_artifact.producer_stage_run_id, v_request_digest,
        jsonb_build_object('ticket_id', v_ticket_id, 'expires_at', v_expires_at)
    );
    RETURN QUERY SELECT v_ticket_id, v_expires_at, false;
END
$$;
ALTER FUNCTION vela_issue_stage_transfer_ticket(jsonb) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_issue_stage_transfer_ticket(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_issue_stage_transfer_ticket(jsonb) TO vela_stage_artifact;

CREATE FUNCTION vela_resolve_stage_transfer_ticket(p_command jsonb)
RETURNS TABLE (
    ticket_id uuid,
    artifact_id uuid,
    object_key text,
    object_version text,
    sha256 bytea,
    size_bytes bigint,
    content_type text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_ticket_id uuid := (p_command ->> 'ticket_id')::uuid;
    v_token_digest bytea := decode(p_command ->> 'token_digest', 'hex');
    v_worker_id uuid := (p_command ->> 'destination_worker_instance_id')::uuid;
    v_worker_epoch bigint := (p_command ->> 'destination_worker_instance_epoch')::bigint;
    v_residency_id uuid := (p_command ->> 'destination_model_residency_id')::uuid;
    v_runtime_epoch bigint := (p_command ->> 'destination_model_runtime_epoch')::bigint;
    v_connector_id uuid := (p_command ->> 'connector_revision_id')::uuid;
    v_resolved_at timestamptz := (p_command ->> 'resolved_at')::timestamptz;
    v_ticket transfer_tickets%ROWTYPE;
    v_artifact stage_artifacts%ROWTYPE;
    v_pin stage_artifact_pins%ROWTYPE;
BEGIN
    IF v_ticket_id IS NULL OR octet_length(v_token_digest) <> 32
       OR v_worker_id IS NULL OR v_worker_epoch <= 0 OR v_residency_id IS NULL
       OR v_runtime_epoch <= 0 OR v_connector_id IS NULL OR v_resolved_at IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_transfer_resolve_command_invalid',
            MESSAGE = 'Stage TransferTicket resolve command fields are invalid';
    END IF;
    SELECT ticket.* INTO v_ticket FROM transfer_tickets AS ticket
    WHERE ticket.id = v_ticket_id FOR SHARE;
    SELECT artifact.* INTO v_artifact FROM stage_artifacts AS artifact
    WHERE artifact.id = v_ticket.stage_artifact_id FOR SHARE;
    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin
    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR SHARE;
    IF v_ticket.state NOT IN ('ACTIVE', 'CONSUMED')
       OR v_ticket.token_digest <> v_token_digest
       OR v_ticket.destination_worker_instance_id <> v_worker_id
       OR v_ticket.destination_worker_instance_epoch <> v_worker_epoch
       OR v_ticket.destination_model_residency_id <> v_residency_id
       OR v_ticket.destination_model_runtime_epoch <> v_runtime_epoch
       OR v_ticket.connector_revision_id <> v_connector_id
       OR v_resolved_at < v_ticket.issued_at OR v_resolved_at >= v_ticket.expires_at
       OR v_artifact.state <> 'COMMITTED'
       OR v_artifact.object_version <> v_ticket.exact_object_version
       OR v_pin.state <> 'ACTIVE'
       OR v_pin.exact_object_version <> v_artifact.object_version
       OR NOT vela_worker_instance_authority_matches(
           v_worker_id, v_worker_epoch,
           (SELECT epoch.device_set_digest FROM worker_instance_epochs AS epoch
            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),
           (SELECT epoch.membership_digest FROM worker_instance_epochs AS epoch
            WHERE epoch.worker_instance_id = v_worker_id AND epoch.epoch = v_worker_epoch),
           v_residency_id, v_runtime_epoch
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_transfer_ticket_resolve_stale',
            MESSAGE = 'Stage TransferTicket exact authority is stale or expired';
    END IF;
    RETURN QUERY SELECT v_ticket.id, v_artifact.id, v_artifact.object_key,
        v_artifact.object_version, v_artifact.sha256, v_artifact.size_bytes,
        v_artifact.content_type;
END
$$;
ALTER FUNCTION vela_resolve_stage_transfer_ticket(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_resolve_stage_transfer_ticket(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_resolve_stage_transfer_ticket(jsonb) TO vela_stage_artifact;

CREATE FUNCTION vela_consume_stage_transfer_ticket(p_command jsonb)
RETURNS TABLE (ticket_id uuid, consumed_at timestamptz, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_ticket_id uuid := (p_command ->> 'ticket_id')::uuid;
    v_token_digest bytea := decode(p_command ->> 'token_digest', 'hex');
    v_outcome_digest bytea := decode(p_command ->> 'outcome_digest', 'hex');
    v_consumed_at timestamptz := (p_command ->> 'consumed_at')::timestamptz;
    v_request_digest bytea;
    v_existing stage_artifact_commands%ROWTYPE;
    v_ticket transfer_tickets%ROWTYPE;
    v_artifact stage_artifacts%ROWTYPE;
    v_pin stage_artifact_pins%ROWTYPE;
BEGIN
    IF v_command_id IS NULL OR v_ticket_id IS NULL
       OR octet_length(v_token_digest) <> 32 OR octet_length(v_outcome_digest) <> 32
       OR v_consumed_at IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_transfer_consume_command_invalid',
            MESSAGE = 'Stage TransferTicket consume command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing FROM stage_artifact_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'CONSUME_TRANSFER'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_artifact_command_replay_mismatch',
                MESSAGE = 'StageArtifact command replay does not match';
        END IF;
        RETURN QUERY SELECT (v_existing.result ->> 'ticket_id')::uuid,
            (v_existing.result ->> 'consumed_at')::timestamptz, true;
        RETURN;
    END IF;
    SELECT ticket.* INTO v_ticket FROM transfer_tickets AS ticket
    WHERE ticket.id = v_ticket_id FOR UPDATE;
    SELECT artifact.* INTO v_artifact FROM stage_artifacts AS artifact
    WHERE artifact.id = v_ticket.stage_artifact_id FOR SHARE;
    SELECT pin.* INTO v_pin FROM stage_artifact_pins AS pin
    WHERE pin.id = v_ticket.stage_artifact_pin_id FOR UPDATE;
    IF v_ticket.state <> 'ACTIVE' OR v_ticket.token_digest <> v_token_digest
       OR v_consumed_at < v_ticket.issued_at OR v_consumed_at >= v_ticket.expires_at
       OR v_artifact.state <> 'COMMITTED'
       OR v_artifact.object_version <> v_ticket.exact_object_version
       OR v_pin.state <> 'ACTIVE' OR v_pin.stage_artifact_id <> v_artifact.id
       OR v_pin.exact_object_version <> v_artifact.object_version THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_transfer_ticket_consume_stale',
            MESSAGE = 'Stage TransferTicket is stale, expired, or revoked';
    END IF;
    UPDATE transfer_tickets SET state = 'CONSUMED', consumed_at = v_consumed_at,
        outcome_digest = v_outcome_digest WHERE id = v_ticket.id AND state = 'ACTIVE';
    UPDATE edge_buffer_credits SET state = 'RELEASED', released_at = v_consumed_at
    WHERE stage_artifact_id = v_artifact.id
      AND destination_stage_run_id = v_pin.owner_stage_run_id
      AND state = 'HELD';
    INSERT INTO stage_artifact_commands (
        command_id, command_kind, attempt_id, stage_run_id, request_digest, result
    ) VALUES (
        v_command_id, 'CONSUME_TRANSFER', v_artifact.attempt_id,
        v_artifact.producer_stage_run_id, v_request_digest,
        jsonb_build_object('ticket_id', v_ticket.id, 'consumed_at', v_consumed_at)
    );
    RETURN QUERY SELECT v_ticket.id, v_consumed_at, false;
END
$$;
ALTER FUNCTION vela_consume_stage_transfer_ticket(jsonb) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_consume_stage_transfer_ticket(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_consume_stage_transfer_ticket(jsonb) TO vela_stage_artifact;

ALTER TABLE stage_artifact_commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_artifact_commands FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_materialization_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_materialization_leases FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_artifacts FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_artifact_inputs ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_artifact_inputs FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_artifact_pins ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_artifact_pins FORCE ROW LEVEL SECURITY;
ALTER TABLE edge_buffer_credits ENABLE ROW LEVEL SECURITY;
ALTER TABLE edge_buffer_credits FORCE ROW LEVEL SECURITY;
ALTER TABLE transfer_tickets ENABLE ROW LEVEL SECURITY;
ALTER TABLE transfer_tickets FORCE ROW LEVEL SECURITY;

GRANT USAGE ON SCHEMA public TO vela_stage_artifact;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    stage_artifact_commands, stage_materialization_leases, stage_artifacts,
    stage_artifact_inputs, stage_artifact_pins, edge_buffer_credits, transfer_tickets
TO vela_attempt_coordinator_owner;
GRANT SELECT, UPDATE ON stage_storage_reservations TO vela_attempt_coordinator_owner;
GRANT SELECT ON
    stage_definition_revisions, stage_interface_revisions, execution_graph_edges,
    connector_revisions, worker_instance_epochs
TO vela_attempt_coordinator_owner;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_artifact_commands)
       OR EXISTS (SELECT 1 FROM stage_materialization_leases)
       OR EXISTS (SELECT 1 FROM stage_artifacts)
       OR EXISTS (SELECT 1 FROM stage_artifact_pins)
       OR EXISTS (SELECT 1 FROM edge_buffer_credits)
       OR EXISTS (SELECT 1 FROM transfer_tickets) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_artifact_rollback_is_unsafe',
            MESSAGE = 'StageArtifact authority and evidence must be empty before rollback';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_consume_stage_transfer_ticket(jsonb) FROM vela_stage_artifact;
DROP FUNCTION vela_consume_stage_transfer_ticket(jsonb);
REVOKE EXECUTE ON FUNCTION vela_resolve_stage_transfer_ticket(jsonb) FROM vela_stage_artifact;
DROP FUNCTION vela_resolve_stage_transfer_ticket(jsonb);
REVOKE EXECUTE ON FUNCTION vela_issue_stage_transfer_ticket(jsonb) FROM vela_stage_artifact;
DROP FUNCTION vela_issue_stage_transfer_ticket(jsonb);
REVOKE EXECUTE ON FUNCTION vela_commit_stage_artifact(jsonb) FROM vela_stage_artifact;
DROP FUNCTION vela_commit_stage_artifact(jsonb);
REVOKE EXECUTE ON FUNCTION vela_is_stage_materialization_authority_active(jsonb)
    FROM vela_stage_artifact;
DROP FUNCTION vela_is_stage_materialization_authority_active(jsonb);
REVOKE EXECUTE ON FUNCTION vela_fail_stage_materialization_source(jsonb)
    FROM vela_stage_artifact;
DROP FUNCTION vela_fail_stage_materialization_source(jsonb);
REVOKE EXECUTE ON FUNCTION vela_private.vela_insert_stage_materialization_source_lost_event(
    uuid, uuid, uuid, uuid, bigint, uuid, integer, bigint, bigint, timestamptz
) FROM vela_attempt_coordinator_owner;
DROP FUNCTION vela_private.vela_insert_stage_materialization_source_lost_event(
    uuid, uuid, uuid, uuid, bigint, uuid, integer, bigint, bigint, timestamptz
);
REVOKE EXECUTE ON FUNCTION vela_seal_stage_output(jsonb) FROM vela_stage_artifact;
DROP FUNCTION vela_seal_stage_output(jsonb);
REVOKE USAGE ON SCHEMA public FROM vela_stage_artifact;
REVOKE SELECT ON
    stage_definition_revisions, stage_interface_revisions, execution_graph_edges,
    connector_revisions, worker_instance_epochs
FROM vela_attempt_coordinator_owner;

ALTER TABLE stage_runs DROP CONSTRAINT stage_runs_winner_stage_artifact_fk;
ALTER TABLE stage_runs DROP COLUMN winner_stage_artifact_id;
DROP TRIGGER stage_artifact_inputs_immutable ON stage_artifact_inputs;
DROP TRIGGER stage_artifacts_identity_immutable ON stage_artifacts;
DROP TRIGGER stage_artifact_commands_immutable ON stage_artifact_commands;
DROP FUNCTION vela_reject_stage_artifact_immutable_mutation();
DROP TABLE transfer_tickets;
DROP TABLE edge_buffer_credits;
DROP TABLE stage_artifact_pins;
DROP TABLE stage_artifact_inputs;
DROP TABLE stage_artifacts;
DROP INDEX stage_materialization_leases_one_active_per_run_idx;
DROP TABLE stage_materialization_leases;
DROP TABLE stage_artifact_commands;
DROP TRIGGER stage_storage_reservations_bind_expiry ON stage_storage_reservations;
DROP FUNCTION vela_bind_stage_storage_reservation_expiry();
ALTER TABLE stage_storage_reservations
    DROP CONSTRAINT stage_storage_reservations_expiry_after_creation,
    DROP COLUMN expires_at,
    DROP COLUMN reservation_policy;
DROP TYPE stage_transfer_ticket_state;
DROP TYPE edge_buffer_credit_state;
DROP TYPE stage_artifact_pin_state;
DROP TYPE stage_artifact_pin_kind;
DROP TYPE stage_artifact_state;
DROP TYPE stage_materialization_lease_state;
-- +goose StatementEnd
