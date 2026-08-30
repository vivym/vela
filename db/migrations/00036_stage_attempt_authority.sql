-- +goose Up
-- +goose StatementBegin
CREATE TYPE execution_authority_kind AS ENUM ('LEGACY_WORKER', 'STAGE_GRAPH');
CREATE TYPE graph_attempt_state AS ENUM (
    'QUEUED', 'RUNNING', 'FINALIZING', 'SUCCEEDED', 'FAILED', 'CANCELED'
);
CREATE TYPE stage_run_state AS ENUM (
    'BLOCKED', 'READY', 'ASSIGNED', 'RUNNING', 'MATERIALIZING',
    'RETRY_WAIT', 'SUCCEEDED', 'FAILED', 'CANCELED'
);
CREATE TYPE stage_attempt_state AS ENUM (
    'ASSIGNED', 'RUNNING', 'OUTPUT_SEALED', 'SUCCEEDED', 'FAILED', 'LOST', 'CANCELED'
);
CREATE TYPE stage_allocation_state AS ENUM ('ALLOCATED', 'RELEASED');
CREATE TYPE stage_lease_state AS ENUM ('ACTIVE', 'REVOKED', 'EXPIRED');
CREATE TYPE stage_progress_kind AS ENUM ('PHYSICAL_OUTPUT', 'EXACT_CACHE');
CREATE TYPE stage_retry_budget_state AS ENUM ('ACTIVE', 'EXHAUSTED', 'CANCELED');
CREATE TYPE stage_storage_reservation_state AS ENUM ('RESERVED', 'CONSUMED', 'RELEASED');

CREATE TABLE execution_graph_snapshots (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL UNIQUE,
    execution_graph_revision_id uuid NOT NULL REFERENCES execution_graph_revisions(id),
    execution_profile_revision_id uuid NOT NULL,
    graph_content_digest bytea NOT NULL CHECK (octet_length(graph_content_digest) = 32),
    topological_order text[] NOT NULL CHECK (cardinality(topological_order) > 0),
    snapshot_contract jsonb NOT NULL CHECK (jsonb_typeof(snapshot_contract) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (id, execution_graph_revision_id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (execution_profile_revision_id, execution_graph_revision_id)
        REFERENCES execution_profile_revisions(id, execution_graph_revision_id)
);

ALTER TABLE attempts
    ADD COLUMN execution_authority_kind execution_authority_kind
        NOT NULL DEFAULT 'LEGACY_WORKER',
    ADD COLUMN graph_state graph_attempt_state,
    ADD COLUMN execution_graph_snapshot_id uuid,
    ALTER COLUMN execution_profile_revision_id DROP NOT NULL,
    ALTER COLUMN worker_pool_id DROP NOT NULL,
    ALTER COLUMN worker_id DROP NOT NULL,
    ALTER COLUMN worker_epoch DROP NOT NULL,
    ALTER COLUMN assigned_at DROP NOT NULL,
    ALTER COLUMN profile_certification_id DROP NOT NULL,
    ADD CONSTRAINT attempts_execution_graph_snapshot_fk
        FOREIGN KEY (execution_graph_snapshot_id)
        REFERENCES execution_graph_snapshots(id),
    ADD CONSTRAINT attempts_authority_shape CHECK (
        (
            execution_authority_kind = 'LEGACY_WORKER'
            AND graph_state IS NULL
            AND execution_graph_snapshot_id IS NULL
            AND execution_profile_revision_id IS NOT NULL
            AND worker_pool_id IS NOT NULL
            AND worker_id IS NOT NULL
            AND worker_epoch IS NOT NULL
            AND assigned_at IS NOT NULL
            AND profile_certification_id IS NOT NULL
        )
        OR (
            execution_authority_kind = 'STAGE_GRAPH'
            AND graph_state IS NOT NULL
            AND execution_graph_snapshot_id IS NOT NULL
            AND execution_profile_revision_id IS NULL
            AND worker_pool_id IS NULL
            AND worker_id IS NULL
            AND worker_epoch IS NULL
            AND assigned_at IS NULL
            AND profile_certification_id IS NULL
            AND scheduler_dispatch_intent_id IS NULL
        )
    ),
    ADD CONSTRAINT attempts_graph_state_projection CHECK (
        execution_authority_kind <> 'STAGE_GRAPH'
        OR state::text = CASE graph_state
            WHEN 'QUEUED' THEN 'ASSIGNED'
            ELSE graph_state::text
        END
    );

CREATE TABLE stage_runs (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    execution_graph_snapshot_id uuid NOT NULL,
    execution_graph_revision_id uuid NOT NULL,
    stage_key text NOT NULL CHECK (
        length(stage_key) BETWEEN 1 AND 100
        AND stage_key ~ '^[a-z][a-z0-9_-]*$'
    ),
    stage_definition_revision_id uuid NOT NULL,
    allowed_stage_profile_revision_ids uuid[] NOT NULL
        CHECK (cardinality(allowed_stage_profile_revision_ids) > 0),
    state stage_run_state NOT NULL,
    fence bigint NOT NULL DEFAULT 1 CHECK (fence > 0),
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_retry_at timestamptz,
    winner_stage_attempt_id uuid,
    winner_output_identity uuid,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (attempt_id, stage_key),
    UNIQUE (attempt_id, id),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES attempts(organization_id, project_id, id),
    FOREIGN KEY (execution_graph_snapshot_id, execution_graph_revision_id)
        REFERENCES execution_graph_snapshots(id, execution_graph_revision_id),
    FOREIGN KEY (execution_graph_revision_id, stage_key, stage_definition_revision_id)
        REFERENCES execution_graph_stages(
            execution_graph_revision_id, stage_key, stage_definition_revision_id
        ),
    CHECK ((state = 'RETRY_WAIT') = (next_retry_at IS NOT NULL)),
    CHECK (
        state <> 'SUCCEEDED'
        OR (winner_stage_attempt_id IS NOT NULL OR winner_output_identity IS NOT NULL)
    )
);

CREATE TABLE stage_dependencies (
    attempt_id uuid NOT NULL,
    source_stage_run_id uuid NOT NULL,
    destination_stage_run_id uuid NOT NULL,
    source_port text NOT NULL CHECK (length(source_port) BETWEEN 1 AND 100),
    destination_port text NOT NULL CHECK (length(destination_port) BETWEEN 1 AND 100),
    satisfied_progress_receipt_id uuid,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (destination_stage_run_id, destination_port),
    UNIQUE (attempt_id, source_stage_run_id, destination_stage_run_id),
    FOREIGN KEY (attempt_id, source_stage_run_id)
        REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (attempt_id, destination_stage_run_id)
        REFERENCES stage_runs(attempt_id, id),
    CHECK (source_stage_run_id <> destination_stage_run_id)
);

CREATE TABLE stage_attempts (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    stage_run_id uuid NOT NULL,
    physical_attempt_number integer NOT NULL CHECK (physical_attempt_number > 0),
    state stage_attempt_state NOT NULL DEFAULT 'ASSIGNED',
    selected_stage_profile_revision_id uuid NOT NULL REFERENCES stage_profile_revisions(id),
    assigned_at timestamptz NOT NULL,
    started_at timestamptz,
    output_sealed_at timestamptz,
    ended_at timestamptz,
    failure_class text CHECK (failure_class IS NULL OR length(failure_class) BETWEEN 1 AND 100),
    failure_fingerprint bytea CHECK (
        failure_fingerprint IS NULL OR octet_length(failure_fingerprint) = 32
    ),
    resource_totals jsonb NOT NULL DEFAULT '{}'::jsonb
        CHECK (jsonb_typeof(resource_totals) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stage_run_id, physical_attempt_number),
    UNIQUE (stage_run_id, id),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES attempts(organization_id, project_id, id),
    FOREIGN KEY (attempt_id, stage_run_id) REFERENCES stage_runs(attempt_id, id),
    CHECK (started_at IS NULL OR started_at >= assigned_at),
    CHECK (output_sealed_at IS NULL OR output_sealed_at >= started_at),
    CHECK (ended_at IS NULL OR ended_at >= assigned_at),
    CHECK (state <> 'ASSIGNED' OR started_at IS NULL),
    CHECK (state NOT IN ('RUNNING', 'OUTPUT_SEALED', 'SUCCEEDED') OR started_at IS NOT NULL),
    CHECK (state NOT IN ('OUTPUT_SEALED', 'SUCCEEDED') OR output_sealed_at IS NOT NULL),
    CHECK (state NOT IN ('SUCCEEDED', 'FAILED', 'LOST', 'CANCELED') OR ended_at IS NOT NULL)
);

CREATE UNIQUE INDEX stage_attempts_one_active_per_stage_run_idx
    ON stage_attempts(stage_run_id)
    WHERE state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED');

CREATE TABLE stage_allocations (
    id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL,
    stage_run_id uuid NOT NULL,
    stage_attempt_id uuid NOT NULL UNIQUE,
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    capacity_pool_id uuid NOT NULL REFERENCES capacity_pools(id),
    device_set_digest bytea NOT NULL CHECK (octet_length(device_set_digest) = 32),
    membership_digest bytea NOT NULL CHECK (octet_length(membership_digest) = 32),
    model_residency_id uuid NOT NULL REFERENCES model_residencies(id),
    model_runtime_epoch bigint NOT NULL CHECK (model_runtime_epoch > 0),
    capacity_vector jsonb NOT NULL CHECK (jsonb_typeof(capacity_vector) = 'object'),
    state stage_allocation_state NOT NULL DEFAULT 'ALLOCATED',
    allocated_at timestamptz NOT NULL,
    released_at timestamptz,
    release_reason text CHECK (release_reason IS NULL OR length(release_reason) BETWEEN 1 AND 500),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stage_attempt_id, id),
    FOREIGN KEY (stage_run_id, stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id),
    FOREIGN KEY (attempt_id, stage_run_id) REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    CHECK ((state = 'RELEASED') = (released_at IS NOT NULL)),
    CHECK ((released_at IS NULL) = (release_reason IS NULL)),
    CHECK (released_at IS NULL OR released_at >= allocated_at)
);

CREATE TABLE stage_leases (
    id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL,
    stage_run_id uuid NOT NULL,
    stage_attempt_id uuid NOT NULL,
    stage_allocation_id uuid NOT NULL,
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    stage_fence bigint NOT NULL CHECK (stage_fence > 0),
    worker_instance_id uuid NOT NULL,
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    device_set_digest bytea NOT NULL CHECK (octet_length(device_set_digest) = 32),
    membership_digest bytea NOT NULL CHECK (octet_length(membership_digest) = 32),
    model_residency_id uuid NOT NULL,
    model_runtime_epoch bigint NOT NULL CHECK (model_runtime_epoch > 0),
    token_digest bytea NOT NULL CHECK (octet_length(token_digest) = 32),
    signing_key_id text NOT NULL CHECK (length(signing_key_id) BETWEEN 1 AND 100),
    execution_nonce bytea NOT NULL CHECK (octet_length(execution_nonce) = 32),
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    local_deadline_at timestamptz NOT NULL,
    state stage_lease_state NOT NULL DEFAULT 'ACTIVE',
    revoked_at timestamptz,
    revoke_reason text CHECK (revoke_reason IS NULL OR length(revoke_reason) BETWEEN 1 AND 500),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (stage_run_id, stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id),
    FOREIGN KEY (stage_attempt_id, stage_allocation_id)
        REFERENCES stage_allocations(stage_attempt_id, id),
    FOREIGN KEY (attempt_id, stage_run_id) REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch),
    FOREIGN KEY (model_residency_id) REFERENCES model_residencies(id),
    CHECK (expires_at > issued_at),
    CHECK (local_deadline_at <= expires_at AND local_deadline_at > issued_at),
    CHECK ((state = 'ACTIVE') = (revoked_at IS NULL)),
    CHECK ((revoked_at IS NULL) = (revoke_reason IS NULL)),
    CHECK (revoked_at IS NULL OR revoked_at >= issued_at)
);

CREATE UNIQUE INDEX stage_leases_one_active_per_stage_run_idx
    ON stage_leases(stage_run_id) WHERE state = 'ACTIVE';

CREATE TABLE attempt_retry_budgets (
    attempt_id uuid PRIMARY KEY REFERENCES attempts(id),
    max_resource_units bigint NOT NULL CHECK (max_resource_units > 0),
    consumed_resource_units bigint NOT NULL DEFAULT 0 CHECK (consumed_resource_units >= 0),
    state stage_retry_budget_state NOT NULL DEFAULT 'ACTIVE',
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (consumed_resource_units <= max_resource_units),
    CHECK (state <> 'EXHAUSTED' OR consumed_resource_units = max_resource_units)
);

CREATE TABLE stage_retry_budgets (
    stage_run_id uuid PRIMARY KEY REFERENCES stage_runs(id),
    max_attempts integer NOT NULL CHECK (max_attempts > 0),
    attempts_consumed integer NOT NULL DEFAULT 0 CHECK (attempts_consumed >= 0),
    state stage_retry_budget_state NOT NULL DEFAULT 'ACTIVE',
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (attempts_consumed <= max_attempts),
    CHECK (state <> 'EXHAUSTED' OR attempts_consumed = max_attempts)
);

CREATE TABLE stage_storage_reservations (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL UNIQUE REFERENCES attempts(id),
    reserved_bytes bigint NOT NULL CHECK (reserved_bytes > 0),
    consumed_bytes bigint NOT NULL DEFAULT 0 CHECK (consumed_bytes >= 0),
    state stage_storage_reservation_state NOT NULL DEFAULT 'RESERVED',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    CHECK (consumed_bytes <= reserved_bytes)
);

CREATE TABLE attempt_coordinator_commands (
    command_id uuid PRIMARY KEY,
    command_kind text NOT NULL CHECK (
        command_kind IN ('INSTANTIATE', 'ASSIGN', 'START', 'COMPLETE', 'FAIL',
                         'EXACT_CACHE_ADVANCE', 'CANCEL', 'RECONCILE')
    ),
    job_id uuid NOT NULL REFERENCES jobs(id),
    attempt_id uuid REFERENCES attempts(id),
    stage_run_id uuid REFERENCES stage_runs(id),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    result jsonb NOT NULL CHECK (jsonb_typeof(result) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE stage_progress_receipts (
    id uuid PRIMARY KEY,
    attempt_id uuid NOT NULL,
    stage_run_id uuid NOT NULL,
    stage_attempt_id uuid,
    progress_kind stage_progress_kind NOT NULL,
    source_identity text NOT NULL CHECK (length(source_identity) BETWEEN 1 AND 1000),
    output_digest bytea NOT NULL CHECK (octet_length(output_digest) = 32),
    command_id uuid NOT NULL UNIQUE REFERENCES attempt_coordinator_commands(command_id),
    committed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stage_run_id),
    FOREIGN KEY (attempt_id, stage_run_id) REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (stage_run_id, stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id)
);

ALTER TABLE stage_dependencies
    ADD CONSTRAINT stage_dependencies_satisfied_receipt_fk
        FOREIGN KEY (satisfied_progress_receipt_id) REFERENCES stage_progress_receipts(id);
ALTER TABLE stage_runs
    ADD CONSTRAINT stage_runs_winner_attempt_fk
        FOREIGN KEY (id, winner_stage_attempt_id)
        REFERENCES stage_attempts(stage_run_id, id),
    ADD CONSTRAINT stage_runs_winner_output_fk
        FOREIGN KEY (winner_output_identity) REFERENCES stage_progress_receipts(id);

CREATE FUNCTION vela_guard_attempt_coordinator_writer() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF current_user <> 'vela_attempt_coordinator_owner' THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'attempt_coordinator_writer_required',
            MESSAGE = 'Stage graph authority is writable only by AttemptCoordinator';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_attempt_coordinator_writer() FROM PUBLIC;
ALTER FUNCTION vela_guard_attempt_coordinator_writer() OWNER TO vela_attempt_coordinator_owner;

CREATE FUNCTION vela_guard_stage_graph_attempt_writer() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF (
        (TG_OP = 'INSERT' AND NEW.execution_authority_kind = 'STAGE_GRAPH')
        OR (TG_OP = 'UPDATE' AND (
            OLD.execution_authority_kind = 'STAGE_GRAPH'
            OR NEW.execution_authority_kind = 'STAGE_GRAPH'
        ))
        OR (TG_OP = 'DELETE' AND OLD.execution_authority_kind = 'STAGE_GRAPH')
    ) AND current_user <> 'vela_attempt_coordinator_owner' THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'attempt_coordinator_writer_required',
            MESSAGE = 'Stage graph Attempt is writable only by AttemptCoordinator';
    END IF;
    RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_stage_graph_attempt_writer() FROM PUBLIC;
ALTER FUNCTION vela_guard_stage_graph_attempt_writer() OWNER TO vela_attempt_coordinator_owner;
CREATE TRIGGER attempts_guard_stage_graph_writer
BEFORE INSERT OR UPDATE OR DELETE ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_guard_stage_graph_attempt_writer();

CREATE FUNCTION vela_reject_attempt_coordinator_immutable_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'attempt_coordinator_authority_is_immutable',
        MESSAGE = 'AttemptCoordinator immutable authority cannot be changed';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_attempt_coordinator_immutable_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_attempt_coordinator_immutable_mutation()
    OWNER TO vela_attempt_coordinator_owner;

CREATE TRIGGER execution_graph_snapshots_immutable
BEFORE UPDATE OR DELETE ON execution_graph_snapshots
FOR EACH ROW EXECUTE FUNCTION vela_reject_attempt_coordinator_immutable_mutation();
CREATE TRIGGER attempt_coordinator_commands_immutable
BEFORE UPDATE OR DELETE ON attempt_coordinator_commands
FOR EACH ROW EXECUTE FUNCTION vela_reject_attempt_coordinator_immutable_mutation();
CREATE TRIGGER stage_progress_receipts_immutable
BEFORE UPDATE OR DELETE ON stage_progress_receipts
FOR EACH ROW EXECUTE FUNCTION vela_reject_attempt_coordinator_immutable_mutation();

CREATE TRIGGER stage_runs_attempt_coordinator_writer
BEFORE INSERT OR UPDATE OR DELETE ON stage_runs
FOR EACH ROW EXECUTE FUNCTION vela_guard_attempt_coordinator_writer();
CREATE TRIGGER stage_dependencies_attempt_coordinator_writer
BEFORE INSERT OR UPDATE OR DELETE ON stage_dependencies
FOR EACH ROW EXECUTE FUNCTION vela_guard_attempt_coordinator_writer();
CREATE TRIGGER stage_attempts_attempt_coordinator_writer
BEFORE INSERT OR UPDATE OR DELETE ON stage_attempts
FOR EACH ROW EXECUTE FUNCTION vela_guard_attempt_coordinator_writer();
CREATE TRIGGER stage_allocations_attempt_coordinator_writer
BEFORE INSERT OR UPDATE OR DELETE ON stage_allocations
FOR EACH ROW EXECUTE FUNCTION vela_guard_attempt_coordinator_writer();
CREATE TRIGGER stage_leases_attempt_coordinator_writer
BEFORE INSERT OR UPDATE OR DELETE ON stage_leases
FOR EACH ROW EXECUTE FUNCTION vela_guard_attempt_coordinator_writer();
CREATE TRIGGER attempt_retry_budgets_attempt_coordinator_writer
BEFORE INSERT OR UPDATE OR DELETE ON attempt_retry_budgets
FOR EACH ROW EXECUTE FUNCTION vela_guard_attempt_coordinator_writer();
CREATE TRIGGER stage_retry_budgets_attempt_coordinator_writer
BEFORE INSERT OR UPDATE OR DELETE ON stage_retry_budgets
FOR EACH ROW EXECUTE FUNCTION vela_guard_attempt_coordinator_writer();
CREATE TRIGGER stage_storage_reservations_attempt_coordinator_writer
BEFORE INSERT OR UPDATE OR DELETE ON stage_storage_reservations
FOR EACH ROW EXECUTE FUNCTION vela_guard_attempt_coordinator_writer();

CREATE OR REPLACE FUNCTION vela_bind_attempt_profile_certification() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_profile_certification_id uuid;
BEGIN
    IF NEW.execution_authority_kind = 'STAGE_GRAPH' THEN
        IF NEW.execution_profile_revision_id IS NOT NULL
           OR NEW.profile_certification_id IS NOT NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'attempts_graph_legacy_profile_forbidden',
                MESSAGE = 'Stage graph Attempt cannot bind a legacy ExecutionProfile';
        END IF;
        RETURN NEW;
    END IF;
    SELECT certification.id INTO v_profile_certification_id
    FROM public.jobs AS job
    JOIN public.profile_certifications AS certification
      ON certification.model_revision_id = job.model_revision_id
     AND certification.generation_preset_revision_id = job.generation_preset_revision_id
     AND certification.output_spec_id = job.output_spec_id
     AND certification.execution_profile_revision_id = NEW.execution_profile_revision_id
     AND certification.state = 'ACTIVE'
     AND certification.invalidated_at IS NULL
    WHERE job.id = NEW.job_id
    FOR SHARE OF certification;
    IF NOT FOUND
       OR (
           NEW.profile_certification_id IS NOT NULL
           AND NEW.profile_certification_id <> v_profile_certification_id
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempts_profile_certification_active',
            MESSAGE = 'Attempt requires the exact active ProfileCertification';
    END IF;
    NEW.profile_certification_id := v_profile_certification_id;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_bind_attempt_profile_certification() FROM PUBLIC;

CREATE OR REPLACE FUNCTION vela_enforce_scheduler_dispatch_attempt() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_required boolean;
    v_rows bigint;
BEGIN
    IF NEW.execution_authority_kind = 'STAGE_GRAPH' THEN
        IF NEW.scheduler_dispatch_intent_id IS NOT NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'attempts_graph_scheduler_dispatch_forbidden',
                MESSAGE = 'Stage graph parent Attempt cannot consume a legacy dispatch intent';
        END IF;
        RETURN NEW;
    END IF;
    SELECT require_dispatch_intent INTO STRICT v_required
    FROM public.scheduler_dispatch_protocol_state WHERE singleton
    FOR SHARE;
    IF NEW.scheduler_dispatch_intent_id IS NULL THEN
        IF v_required THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'attempts_scheduler_dispatch_required',
                MESSAGE = 'Assignment requires a live Scheduler dispatch claim';
        END IF;
        RETURN NEW;
    END IF;
    UPDATE public.scheduler_dispatch_intents AS dispatch
    SET state = 'COMMITTED', committed_at = NEW.assigned_at
    FROM public.jobs AS job
    WHERE dispatch.id = NEW.scheduler_dispatch_intent_id
      AND dispatch.state = 'CLAIMED'
      AND dispatch.claim_expires_at > clock_timestamp()
      AND dispatch.organization_id = NEW.organization_id
      AND dispatch.project_id = NEW.project_id
      AND dispatch.job_id = NEW.job_id
      AND dispatch.service_class_revision_id = job.service_class_revision_id
      AND dispatch.worker_pool_id = NEW.worker_pool_id
      AND dispatch.execution_profile_revision_id = NEW.execution_profile_revision_id
      AND dispatch.worker_id = NEW.worker_id
      AND dispatch.worker_epoch = NEW.worker_epoch
      AND job.id = NEW.job_id
      AND job.version = dispatch.expected_job_version;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempts_scheduler_dispatch_exact_claim',
            MESSAGE = 'Assignment does not match a live Scheduler dispatch claim';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_scheduler_dispatch_attempt() FROM PUBLIC;

CREATE OR REPLACE FUNCTION vela_guard_fleet_assignment_writer() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_enforced boolean;
BEGIN
    IF NEW.execution_authority_kind = 'STAGE_GRAPH' THEN
        RETURN NEW;
    END IF;
    SELECT state.enforced INTO STRICT v_enforced
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton
    FOR SHARE;
    IF (v_enforced AND NEW.fleet_protocol_version <> 2)
       OR (NOT v_enforced AND NEW.fleet_protocol_version <> 1) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempts_fleet_assignment_protocol',
            MESSAGE = 'Assignment writer does not match the active Fleet protocol';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_fleet_assignment_writer() FROM PUBLIC;

CREATE OR REPLACE FUNCTION vela_validate_job_state_transition() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.state = OLD.state THEN
        RETURN NEW;
    END IF;
    IF NOT (
        (OLD.state = 'QUEUED' AND NEW.state IN ('ASSIGNED', 'RUNNING', 'CANCELED', 'FAILED'))
        OR (OLD.state = 'ASSIGNED' AND NEW.state IN ('RUNNING', 'RETRY_WAIT', 'CANCELED', 'FAILED'))
        OR (OLD.state = 'RUNNING' AND NEW.state IN ('FINALIZING', 'RETRY_WAIT', 'CANCELING', 'FAILED'))
        OR (OLD.state = 'FINALIZING' AND NEW.state IN ('SUCCEEDED', 'RETRY_WAIT', 'CANCELING', 'FAILED'))
        OR (OLD.state = 'RETRY_WAIT' AND NEW.state IN ('ASSIGNED', 'RUNNING', 'CANCELED', 'FAILED'))
        OR (OLD.state = 'CANCELING' AND NEW.state = 'CANCELED')
    ) THEN
        RAISE EXCEPTION 'invalid Job state transition from % to %', OLD.state, NEW.state
            USING ERRCODE = '23514';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_job_state_transition() FROM PUBLIC;
ALTER FUNCTION vela_validate_job_state_transition() OWNER TO vela_internal;

CREATE OR REPLACE FUNCTION vela_reject_billable_start_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.billable_started_at IS NOT NULL THEN
            RAISE EXCEPTION 'Billable Start must be unset when a Job is created';
        END IF;
        RETURN NEW;
    END IF;
    IF OLD.billable_started_at IS NOT NULL
        AND NEW.billable_started_at IS DISTINCT FROM OLD.billable_started_at THEN
        RAISE EXCEPTION 'Billable Start cannot be changed or cleared';
    END IF;
    IF OLD.billable_started_at IS NULL
        AND NEW.billable_started_at IS NOT NULL
        AND (OLD.state NOT IN ('QUEUED', 'ASSIGNED', 'RETRY_WAIT') OR NEW.state <> 'RUNNING') THEN
        RAISE EXCEPTION 'Billable Start requires first graph progress into RUNNING';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_billable_start_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_billable_start_mutation() OWNER TO vela_internal;

CREATE FUNCTION vela_instantiate_stage_graph(p_command jsonb)
RETURNS TABLE (
    snapshot_id uuid,
    attempt_id uuid,
    attempt_fence bigint,
    stage_run_count integer,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid;
    v_job_id uuid;
    v_expected_job_version bigint;
    v_expected_job_fence bigint;
    v_snapshot_id uuid;
    v_graph_id uuid;
    v_profile_id uuid;
    v_attempt_id uuid;
    v_reservation_id uuid;
    v_reserved_bytes bigint;
    v_request_digest bytea;
    v_existing public.attempt_coordinator_commands%ROWTYPE;
    v_job public.jobs%ROWTYPE;
    v_graph public.execution_graph_revisions%ROWTYPE;
    v_next_fence bigint;
    v_attempt_number integer;
    v_stage_count integer;
    v_result jsonb;
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) <> 'object'
       OR (p_command ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_graph_instantiate_command_invalid',
            MESSAGE = 'Stage graph instantiate command is invalid';
    END IF;
    v_command_id := (p_command ->> 'command_id')::uuid;
    v_job_id := (p_command ->> 'job_id')::uuid;
    v_expected_job_version := (p_command ->> 'expected_job_version')::bigint;
    v_expected_job_fence := (p_command ->> 'expected_job_fence')::bigint;
    v_snapshot_id := (p_command ->> 'execution_graph_snapshot_id')::uuid;
    v_graph_id := (p_command ->> 'execution_graph_revision_id')::uuid;
    v_profile_id := (p_command ->> 'execution_profile_revision_id')::uuid;
    v_attempt_id := (p_command ->> 'attempt_id')::uuid;
    v_reservation_id := (p_command ->> 'storage_reservation_id')::uuid;
    v_reserved_bytes := (p_command ->> 'reserved_storage_bytes')::bigint;
    IF v_command_id IS NULL OR v_job_id IS NULL OR v_snapshot_id IS NULL
       OR v_graph_id IS NULL OR v_profile_id IS NULL OR v_attempt_id IS NULL
       OR v_reservation_id IS NULL OR v_expected_job_version <= 0
       OR v_expected_job_fence < 0 OR v_reserved_bytes <= 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_graph_instantiate_command_invalid',
            MESSAGE = 'Stage graph instantiate command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));

    SELECT command.* INTO v_existing
    FROM public.attempt_coordinator_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'INSTANTIATE'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'attempt_coordinator_command_replay_mismatch',
                MESSAGE = 'AttemptCoordinator command replay does not match';
        END IF;
        RETURN QUERY SELECT
            (v_existing.result ->> 'snapshot_id')::uuid,
            (v_existing.result ->> 'attempt_id')::uuid,
            (v_existing.result ->> 'attempt_fence')::bigint,
            (v_existing.result ->> 'stage_run_count')::integer,
            true;
        RETURN;
    END IF;

    SELECT job.* INTO v_job
    FROM public.jobs AS job
    WHERE job.id = v_job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '23503',
            CONSTRAINT = 'stage_graph_job_not_found',
            MESSAGE = 'Stage graph Job does not exist';
    END IF;
    IF v_job.version <> v_expected_job_version
       OR v_job.current_fence <> v_expected_job_fence
       OR v_job.state NOT IN ('QUEUED', 'RETRY_WAIT') THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_graph_job_fence_stale',
            MESSAGE = 'Stage graph Job version or fence is stale';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.attempts AS attempt
        WHERE attempt.job_id = v_job.id
          AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_active_attempt_exists',
            MESSAGE = 'Job already has an active Attempt';
    END IF;

    SELECT graph.* INTO v_graph
    FROM public.execution_graph_revisions AS graph
    WHERE graph.id = v_graph_id
      AND graph.model_revision_id = v_job.model_revision_id
      AND graph.state = 'ACTIVE';
    IF NOT FOUND OR NOT EXISTS (
        SELECT 1
        FROM public.execution_profile_revisions AS profile
        WHERE profile.id = v_profile_id
          AND profile.execution_graph_revision_id = v_graph_id
          AND profile.model_revision_id = v_job.model_revision_id
          AND profile.state = 'ACTIVE'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_snapshot_authority_inactive',
            MESSAGE = 'Stage graph snapshot requires active graph and profile authority';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.execution_graph_stages AS graph_stage
        WHERE graph_stage.execution_graph_revision_id = v_graph_id
          AND NOT EXISTS (
              SELECT 1
              FROM public.execution_profile_stage_options AS option
              WHERE option.execution_profile_revision_id = v_profile_id
                AND option.execution_graph_revision_id = v_graph_id
                AND option.stage_key = graph_stage.stage_key
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_snapshot_profile_incomplete',
            MESSAGE = 'Stage graph profile does not cover every graph stage';
    END IF;

    v_next_fence := v_job.current_fence + 1;
    SELECT COALESCE(max(attempt.attempt_number), 0) + 1 INTO v_attempt_number
    FROM public.attempts AS attempt WHERE attempt.job_id = v_job.id;

    INSERT INTO public.execution_graph_snapshots (
        id, organization_id, project_id, job_id, execution_graph_revision_id,
        execution_profile_revision_id, graph_content_digest, topological_order,
        snapshot_contract
    ) VALUES (
        v_snapshot_id, v_job.organization_id, v_job.project_id, v_job.id, v_graph.id,
        v_profile_id, v_graph.content_digest, v_graph.topological_order,
        jsonb_build_object(
            'schema_version', 1,
            'graph_id', v_graph.id,
            'graph_digest', encode(v_graph.content_digest, 'hex'),
            'profile_id', v_profile_id,
            'stage_options', (
                SELECT jsonb_agg(jsonb_build_object(
                    'stage_key', option.stage_key,
                    'stage_profile_revision_id', option.stage_profile_revision_id,
                    'preference', option.preference,
                    'eligibility_metadata', option.eligibility_metadata
                ) ORDER BY option.stage_key, option.preference, option.stage_profile_revision_id)
                FROM public.execution_profile_stage_options AS option
                WHERE option.execution_profile_revision_id = v_profile_id
                  AND option.execution_graph_revision_id = v_graph.id
            ),
            'connector_options', COALESCE((
                SELECT jsonb_agg(jsonb_build_object(
                    'edge_id', option.execution_graph_edge_id,
                    'connector_revision_id', option.connector_revision_id,
                    'preference', option.preference,
                    'required_topology_policy', option.required_topology_policy
                ) ORDER BY option.execution_graph_edge_id, option.preference,
                           option.connector_revision_id)
                FROM public.execution_profile_connector_options AS option
                WHERE option.execution_profile_revision_id = v_profile_id
                  AND option.execution_graph_revision_id = v_graph.id
            ), '[]'::jsonb)
        )
    );

    UPDATE public.jobs
    SET current_fence = v_next_fence,
        version = version + 1,
        updated_at = transaction_timestamp()
    WHERE id = v_job.id
      AND version = v_job.version
      AND current_fence = v_job.current_fence;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_graph_job_fence_stale',
            MESSAGE = 'Stage graph Job changed during instantiation';
    END IF;

    INSERT INTO public.attempts (
        id, organization_id, project_id, job_id, attempt_number,
        execution_profile_revision_id, worker_pool_id, worker_id, worker_epoch,
        state, fence, assigned_at, execution_authority_kind, graph_state,
        execution_graph_snapshot_id, profile_certification_id,
        scheduler_dispatch_intent_id
    ) VALUES (
        v_attempt_id, v_job.organization_id, v_job.project_id, v_job.id,
        v_attempt_number, NULL, NULL, NULL, NULL, 'ASSIGNED', v_next_fence, NULL,
        'STAGE_GRAPH', 'QUEUED', v_snapshot_id, NULL, NULL
    );
    UPDATE public.retry_runtime_states AS runtime
    SET attempts_started = runtime.attempts_started + 1,
        version = runtime.version + 1,
        updated_at = transaction_timestamp()
    WHERE runtime.job_id = v_job.id
      AND runtime.attempts_started < v_job.execution_max_attempts;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_parent_attempt_budget_exhausted',
            MESSAGE = 'Parent Attempt retry budget is exhausted';
    END IF;

    INSERT INTO public.stage_runs (
        id, organization_id, project_id, attempt_id,
        execution_graph_snapshot_id, execution_graph_revision_id,
        stage_key, stage_definition_revision_id,
        allowed_stage_profile_revision_ids, state
    )
    SELECT
        gen_random_uuid(), v_job.organization_id, v_job.project_id, v_attempt_id,
        v_snapshot_id, graph_stage.execution_graph_revision_id,
        graph_stage.stage_key, graph_stage.stage_definition_revision_id,
        ARRAY(
            SELECT option.stage_profile_revision_id
            FROM public.execution_profile_stage_options AS option
            WHERE option.execution_profile_revision_id = v_profile_id
              AND option.execution_graph_revision_id = v_graph.id
              AND option.stage_key = graph_stage.stage_key
            ORDER BY option.preference, option.stage_profile_revision_id
        ),
        CASE WHEN EXISTS (
            SELECT 1 FROM public.execution_graph_edges AS edge
            WHERE edge.execution_graph_revision_id = v_graph.id
              AND edge.destination_stage_key = graph_stage.stage_key
        ) THEN 'BLOCKED'::public.stage_run_state
          ELSE 'READY'::public.stage_run_state END
    FROM public.execution_graph_stages AS graph_stage
    WHERE graph_stage.execution_graph_revision_id = v_graph.id;
    GET DIAGNOSTICS v_stage_count = ROW_COUNT;
    IF v_stage_count <> cardinality(v_graph.topological_order) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'stage_graph_snapshot_stage_count_mismatch',
            MESSAGE = 'Stage graph snapshot stage count does not match activation order';
    END IF;

    INSERT INTO public.stage_dependencies (
        attempt_id, source_stage_run_id, destination_stage_run_id,
        source_port, destination_port
    )
    SELECT
        v_attempt_id, source_run.id, destination_run.id,
        edge.source_port, edge.destination_port
    FROM public.execution_graph_edges AS edge
    JOIN public.stage_runs AS source_run
      ON source_run.attempt_id = v_attempt_id
     AND source_run.stage_key = edge.source_stage_key
    JOIN public.stage_runs AS destination_run
      ON destination_run.attempt_id = v_attempt_id
     AND destination_run.stage_key = edge.destination_stage_key
    WHERE edge.execution_graph_revision_id = v_graph.id;

    INSERT INTO public.attempt_retry_budgets (attempt_id, max_resource_units)
    VALUES (v_attempt_id, v_job.execution_max_total_compute_seconds);
    INSERT INTO public.stage_retry_budgets (stage_run_id, max_attempts)
    SELECT run.id, v_job.execution_max_attempts
    FROM public.stage_runs AS run WHERE run.attempt_id = v_attempt_id;
    INSERT INTO public.stage_storage_reservations (
        id, organization_id, project_id, job_id, attempt_id, reserved_bytes
    ) VALUES (
        v_reservation_id, v_job.organization_id, v_job.project_id,
        v_job.id, v_attempt_id, v_reserved_bytes
    );

    v_result := jsonb_build_object(
        'snapshot_id', v_snapshot_id,
        'attempt_id', v_attempt_id,
        'attempt_fence', v_next_fence,
        'stage_run_count', v_stage_count
    );
    INSERT INTO public.attempt_coordinator_commands (
        command_id, command_kind, job_id, attempt_id, request_digest, result
    ) VALUES (
        v_command_id, 'INSTANTIATE', v_job.id, v_attempt_id, v_request_digest, v_result
    );
    RETURN QUERY SELECT v_snapshot_id, v_attempt_id, v_next_fence, v_stage_count, false;
END
$$;
REVOKE ALL ON FUNCTION vela_instantiate_stage_graph(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_instantiate_stage_graph(jsonb) OWNER TO vela_attempt_coordinator_owner;

CREATE FUNCTION vela_apply_stage_command(p_command jsonb)
RETURNS TABLE (
    stage_run_id uuid,
    stage_attempt_id uuid,
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
    v_kind text;
    v_command_id uuid;
    v_attempt_id uuid;
    v_stage_run_id uuid;
    v_expected_attempt_fence bigint;
    v_expected_stage_fence bigint;
    v_expected_stage_version bigint;
    v_stage_attempt_id uuid;
    v_allocation_id uuid;
    v_lease_id uuid;
    v_profile_id uuid;
    v_capacity_pool_id uuid;
    v_worker_id uuid;
    v_worker_epoch bigint;
    v_device_set_digest bytea;
    v_membership_digest bytea;
    v_residency_id uuid;
    v_model_runtime_epoch bigint;
    v_token_digest bytea;
    v_execution_nonce bytea;
    v_signing_key_id text;
    v_issued_at timestamptz;
    v_expires_at timestamptz;
    v_local_deadline_at timestamptz;
    v_request_digest bytea;
    v_existing public.attempt_coordinator_commands%ROWTYPE;
    v_job_id uuid;
    v_job public.jobs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_run public.stage_runs%ROWTYPE;
    v_stage_budget public.stage_retry_budgets%ROWTYPE;
    v_physical_attempt_number integer;
    v_result jsonb;
    v_started_at timestamptz;
    v_stage_attempt public.stage_attempts%ROWTYPE;
    v_stage_lease public.stage_leases%ROWTYPE;
    v_rows bigint;
    v_receipt_id uuid;
    v_source_identity text;
    v_output_digest bytea;
    v_advanced_at timestamptz;
    v_completed_at timestamptz;
    v_failure_class text;
    v_failure_fingerprint bytea;
    v_consumed_resource_units bigint;
    v_failed_at timestamptz;
    v_retry_at timestamptz;
    v_attempt_budget public.attempt_retry_budgets%ROWTYPE;
    v_retry_allowed boolean;
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) <> 'object'
       OR (p_command ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_command_invalid',
            MESSAGE = 'Stage command is invalid';
    END IF;
    v_kind := p_command ->> 'command_kind';
    v_command_id := (p_command ->> 'command_id')::uuid;
    v_attempt_id := (p_command ->> 'attempt_id')::uuid;
    v_stage_run_id := (p_command ->> 'stage_run_id')::uuid;
    IF v_kind = 'FAIL' THEN
        v_expected_attempt_fence := (p_command ->> 'expected_attempt_fence')::bigint;
        v_expected_stage_fence := (p_command ->> 'expected_stage_fence')::bigint;
        v_expected_stage_version := (p_command ->> 'expected_stage_version')::bigint;
        v_stage_attempt_id := (p_command ->> 'stage_attempt_id')::uuid;
        v_lease_id := (p_command ->> 'stage_lease_id')::uuid;
        v_failure_class := p_command ->> 'failure_class';
        v_failure_fingerprint := decode(p_command ->> 'failure_fingerprint', 'hex');
        v_consumed_resource_units := (p_command ->> 'consumed_resource_units')::bigint;
        v_failed_at := (p_command ->> 'failed_at')::timestamptz;
        v_retry_at := (p_command ->> 'retry_at')::timestamptz;
        IF v_command_id IS NULL OR v_attempt_id IS NULL OR v_stage_run_id IS NULL
           OR v_expected_attempt_fence <= 0 OR v_expected_stage_fence <= 0
           OR v_expected_stage_version <= 0 OR v_stage_attempt_id IS NULL
           OR v_lease_id IS NULL OR v_failure_class IS NULL
           OR length(v_failure_class) NOT BETWEEN 1 AND 100
           OR octet_length(v_failure_fingerprint) <> 32
           OR v_consumed_resource_units <= 0 OR v_failed_at IS NULL
           OR v_retry_at IS NULL OR v_retry_at <= v_failed_at THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'stage_fail_command_invalid',
                MESSAGE = 'Stage failure command fields are invalid';
        END IF;
        v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
        SELECT command.* INTO v_existing
        FROM public.attempt_coordinator_commands AS command
        WHERE command.command_id = v_command_id;
        IF FOUND THEN
            IF v_existing.command_kind <> 'FAIL'
               OR v_existing.request_digest <> v_request_digest THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    CONSTRAINT = 'attempt_coordinator_command_replay_mismatch',
                    MESSAGE = 'AttemptCoordinator command replay does not match';
            END IF;
            RETURN QUERY SELECT
                (v_existing.result ->> 'stage_run_id')::uuid,
                (v_existing.result ->> 'stage_attempt_id')::uuid,
                v_existing.result ->> 'stage_state',
                (v_existing.result ->> 'stage_fence')::bigint,
                (v_existing.result ->> 'stage_version')::bigint,
                true;
            RETURN;
        END IF;
        SELECT attempt.job_id INTO v_job_id
        FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '23503',
                CONSTRAINT = 'stage_command_attempt_not_found',
                MESSAGE = 'Stage command parent Attempt does not exist';
        END IF;
        SELECT job.* INTO v_job
        FROM public.jobs AS job WHERE job.id = v_job_id FOR UPDATE;
        SELECT attempt.* INTO v_attempt
        FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id FOR UPDATE;
        SELECT run.* INTO v_run
        FROM public.stage_runs AS run
        WHERE run.id = v_stage_run_id AND run.attempt_id = v_attempt_id
        FOR UPDATE;
        SELECT physical.* INTO v_stage_attempt
        FROM public.stage_attempts AS physical
        WHERE physical.id = v_stage_attempt_id AND physical.stage_run_id = v_stage_run_id
        FOR UPDATE;
        SELECT lease.* INTO v_stage_lease
        FROM public.stage_leases AS lease
        WHERE lease.id = v_lease_id
          AND lease.stage_attempt_id = v_stage_attempt_id
          AND lease.stage_run_id = v_stage_run_id
        FOR UPDATE;
        SELECT budget.* INTO v_attempt_budget
        FROM public.attempt_retry_budgets AS budget
        WHERE budget.attempt_id = v_attempt_id
        FOR UPDATE;
        SELECT budget.* INTO v_stage_budget
        FROM public.stage_retry_budgets AS budget
        WHERE budget.stage_run_id = v_stage_run_id
        FOR UPDATE;
        IF v_attempt.execution_authority_kind <> 'STAGE_GRAPH'
           OR v_attempt.graph_state <> 'RUNNING'
           OR v_attempt.fence <> v_expected_attempt_fence
           OR v_attempt.fence <> v_job.current_fence
           OR v_job.state <> 'RUNNING'
           OR v_run.fence <> v_expected_stage_fence
           OR v_run.version <> v_expected_stage_version
           OR v_run.state NOT IN ('ASSIGNED', 'RUNNING', 'MATERIALIZING')
           OR v_stage_attempt.state NOT IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED')
           OR v_stage_lease.state <> 'ACTIVE'
           OR v_stage_lease.attempt_fence <> v_attempt.fence
           OR v_stage_lease.stage_fence <> v_run.fence
           OR v_failed_at < v_stage_attempt.assigned_at
           OR v_attempt_budget.state <> 'ACTIVE'
           OR v_stage_budget.state <> 'ACTIVE' THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_fail_authority_stale',
                MESSAGE = 'Stage failure Lease, fence, version, or budget authority is stale';
        END IF;
        v_retry_allowed := v_failure_class = ANY(v_job.execution_retryable_failure_classes)
            AND v_stage_budget.attempts_consumed < v_stage_budget.max_attempts
            AND v_attempt_budget.consumed_resource_units + v_consumed_resource_units
                < v_attempt_budget.max_resource_units
            AND v_retry_at < v_job.job_expires_at;

        UPDATE public.stage_leases AS lease
        SET state = 'REVOKED', revoked_at = v_failed_at,
            revoke_reason = 'STAGE_FAILED'
        WHERE lease.id = v_stage_lease.id AND lease.state = 'ACTIVE';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_fail_authority_stale',
                MESSAGE = 'StageLease changed during failure';
        END IF;
        UPDATE public.stage_allocations AS allocation
        SET state = 'RELEASED', released_at = v_failed_at,
            release_reason = 'STAGE_FAILED'
        WHERE allocation.id = v_stage_lease.stage_allocation_id
          AND allocation.state = 'ALLOCATED';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_fail_authority_stale',
                MESSAGE = 'StageAllocation changed during failure';
        END IF;
        UPDATE public.stage_attempts AS physical
        SET state = 'FAILED', ended_at = v_failed_at,
            failure_class = v_failure_class,
            failure_fingerprint = v_failure_fingerprint,
            resource_totals = jsonb_build_object(
                'resource_units', v_consumed_resource_units
            ),
            updated_at = v_failed_at
        WHERE physical.id = v_stage_attempt.id
          AND physical.state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED');
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_fail_authority_stale',
                MESSAGE = 'StageAttempt changed during failure';
        END IF;
        UPDATE public.attempt_retry_budgets AS budget
        SET consumed_resource_units = LEAST(
                budget.max_resource_units,
                budget.consumed_resource_units + v_consumed_resource_units
            ),
            state = CASE WHEN v_retry_allowed
                THEN 'ACTIVE'::public.stage_retry_budget_state
                WHEN budget.consumed_resource_units + v_consumed_resource_units
                    >= budget.max_resource_units
                THEN 'EXHAUSTED'::public.stage_retry_budget_state
                ELSE 'CANCELED'::public.stage_retry_budget_state END,
            version = budget.version + 1,
            updated_at = v_failed_at
        WHERE budget.attempt_id = v_attempt.id
          AND budget.version = v_attempt_budget.version;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_fail_budget_stale',
                MESSAGE = 'Attempt retry budget changed during failure';
        END IF;

        IF v_retry_allowed THEN
            UPDATE public.stage_runs AS run
            SET state = 'RETRY_WAIT', fence = run.fence + 1,
                retry_count = run.retry_count + 1, next_retry_at = v_retry_at,
                version = run.version + 1, updated_at = v_failed_at
            WHERE run.id = v_run.id AND run.version = v_run.version
              AND run.fence = v_run.fence;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'stage_fail_authority_stale',
                    MESSAGE = 'StageRun changed during failure';
            END IF;
            v_result := jsonb_build_object(
                'stage_run_id', v_run.id,
                'stage_attempt_id', v_stage_attempt.id,
                'stage_state', 'RETRY_WAIT',
                'stage_fence', v_run.fence + 1,
                'stage_version', v_run.version + 1
            );
        ELSE
            UPDATE public.stage_retry_budgets AS budget
            SET state = CASE
                    WHEN budget.attempts_consumed = budget.max_attempts
                    THEN 'EXHAUSTED'::public.stage_retry_budget_state
                    ELSE 'CANCELED'::public.stage_retry_budget_state
                END,
                version = budget.version + 1,
                updated_at = v_failed_at
            WHERE budget.stage_run_id = v_run.id;
            UPDATE public.stage_runs AS run
            SET state = CASE WHEN run.id = v_run.id
                    THEN 'FAILED'::public.stage_run_state
                    ELSE 'CANCELED'::public.stage_run_state END,
                fence = run.fence + 1, next_retry_at = NULL,
                version = run.version + 1, updated_at = v_failed_at
            WHERE run.attempt_id = v_attempt.id
              AND run.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED');
            UPDATE public.stage_storage_reservations AS reservation
            SET state = 'RELEASED', updated_at = v_failed_at
            WHERE reservation.attempt_id = v_attempt.id
              AND reservation.state = 'RESERVED';
            UPDATE public.attempts AS attempt
            SET state = 'FAILED', graph_state = 'FAILED', ended_at = v_failed_at,
                updated_at = v_failed_at
            WHERE attempt.id = v_attempt.id AND attempt.graph_state = 'RUNNING';
            UPDATE public.jobs AS job
            SET state = 'FAILED', version = job.version + 1, updated_at = v_failed_at
            WHERE job.id = v_job.id AND job.state = 'RUNNING'
              AND job.current_fence = v_attempt.fence;
            UPDATE public.projects AS project
            SET running_count = project.running_count - 1
            WHERE project.id = v_job.project_id
              AND project.organization_id = v_job.organization_id
              AND project.running_count > 0;
            UPDATE public.credit_reservations AS reservation
            SET state = 'RELEASED', updated_at = v_failed_at
            WHERE reservation.job_id = v_job.id AND reservation.state = 'RESERVED';
            UPDATE public.organization_credit_accounts AS account
            SET reserved_minor = account.reserved_minor - v_job.pricing_quoted_amount_minor,
                version = account.version + 1, updated_at = v_failed_at
            WHERE account.organization_id = v_job.organization_id
              AND account.currency = v_job.pricing_currency
              AND account.reserved_minor >= v_job.pricing_quoted_amount_minor;
            v_result := jsonb_build_object(
                'stage_run_id', v_run.id,
                'stage_attempt_id', v_stage_attempt.id,
                'stage_state', 'FAILED',
                'stage_fence', v_run.fence + 1,
                'stage_version', v_run.version + 1
            );
        END IF;
        INSERT INTO public.attempt_coordinator_commands (
            command_id, command_kind, job_id, attempt_id, stage_run_id,
            request_digest, result
        ) VALUES (
            v_command_id, 'FAIL', v_job.id, v_attempt.id, v_run.id,
            v_request_digest, v_result
        );
        RETURN QUERY SELECT
            v_run.id, v_stage_attempt.id, v_result ->> 'stage_state',
            (v_result ->> 'stage_fence')::bigint,
            (v_result ->> 'stage_version')::bigint, false;
        RETURN;
    END IF;
    IF v_kind = 'COMPLETE' THEN
        v_expected_attempt_fence := (p_command ->> 'expected_attempt_fence')::bigint;
        v_expected_stage_fence := (p_command ->> 'expected_stage_fence')::bigint;
        v_expected_stage_version := (p_command ->> 'expected_stage_version')::bigint;
        v_stage_attempt_id := (p_command ->> 'stage_attempt_id')::uuid;
        v_lease_id := (p_command ->> 'stage_lease_id')::uuid;
        v_receipt_id := (p_command ->> 'progress_receipt_id')::uuid;
        v_source_identity := p_command ->> 'output_identity';
        v_output_digest := decode(p_command ->> 'output_digest', 'hex');
        v_completed_at := (p_command ->> 'completed_at')::timestamptz;
        IF v_command_id IS NULL OR v_attempt_id IS NULL OR v_stage_run_id IS NULL
           OR v_expected_attempt_fence <= 0 OR v_expected_stage_fence <= 0
           OR v_expected_stage_version <= 0 OR v_stage_attempt_id IS NULL
           OR v_lease_id IS NULL OR v_receipt_id IS NULL
           OR v_source_identity IS NULL OR length(v_source_identity) NOT BETWEEN 1 AND 1000
           OR octet_length(v_output_digest) <> 32 OR v_completed_at IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'stage_complete_command_invalid',
                MESSAGE = 'Stage completion command fields are invalid';
        END IF;
        v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
        SELECT command.* INTO v_existing
        FROM public.attempt_coordinator_commands AS command
        WHERE command.command_id = v_command_id;
        IF FOUND THEN
            IF v_existing.command_kind <> 'COMPLETE'
               OR v_existing.request_digest <> v_request_digest THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    CONSTRAINT = 'attempt_coordinator_command_replay_mismatch',
                    MESSAGE = 'AttemptCoordinator command replay does not match';
            END IF;
            RETURN QUERY SELECT
                (v_existing.result ->> 'stage_run_id')::uuid,
                (v_existing.result ->> 'stage_attempt_id')::uuid,
                v_existing.result ->> 'stage_state',
                (v_existing.result ->> 'stage_fence')::bigint,
                (v_existing.result ->> 'stage_version')::bigint,
                true;
            RETURN;
        END IF;
        SELECT attempt.job_id INTO v_job_id
        FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '23503',
                CONSTRAINT = 'stage_command_attempt_not_found',
                MESSAGE = 'Stage command parent Attempt does not exist';
        END IF;
        SELECT job.* INTO v_job
        FROM public.jobs AS job WHERE job.id = v_job_id FOR UPDATE;
        SELECT attempt.* INTO v_attempt
        FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id FOR UPDATE;
        SELECT run.* INTO v_run
        FROM public.stage_runs AS run
        WHERE run.id = v_stage_run_id AND run.attempt_id = v_attempt_id
        FOR UPDATE;
        SELECT physical.* INTO v_stage_attempt
        FROM public.stage_attempts AS physical
        WHERE physical.id = v_stage_attempt_id AND physical.stage_run_id = v_stage_run_id
        FOR UPDATE;
        SELECT lease.* INTO v_stage_lease
        FROM public.stage_leases AS lease
        WHERE lease.id = v_lease_id
          AND lease.stage_attempt_id = v_stage_attempt_id
          AND lease.stage_run_id = v_stage_run_id
        FOR UPDATE;
        IF v_attempt.execution_authority_kind <> 'STAGE_GRAPH'
           OR v_attempt.graph_state <> 'RUNNING'
           OR v_attempt.fence <> v_expected_attempt_fence
           OR v_attempt.fence <> v_job.current_fence
           OR v_job.state <> 'RUNNING'
           OR v_job.billable_started_at IS NULL
           OR v_run.fence <> v_expected_stage_fence
           OR v_run.version <> v_expected_stage_version
           OR v_run.state <> 'RUNNING'
           OR v_stage_attempt.state <> 'RUNNING'
           OR v_stage_lease.state <> 'ACTIVE'
           OR v_stage_lease.attempt_fence <> v_attempt.fence
           OR v_stage_lease.stage_fence <> v_run.fence
           OR v_completed_at < v_stage_attempt.started_at
           OR v_completed_at >= v_stage_lease.expires_at
           OR NOT public.vela_worker_instance_authority_matches(
               v_stage_lease.worker_instance_id,
               v_stage_lease.worker_instance_epoch,
               v_stage_lease.device_set_digest,
               v_stage_lease.membership_digest,
               v_stage_lease.model_residency_id,
               v_stage_lease.model_runtime_epoch
           ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_complete_authority_stale',
                MESSAGE = 'Stage completion Lease, fence, version, or runtime authority is stale';
        END IF;

        v_result := jsonb_build_object(
            'stage_run_id', v_run.id,
            'stage_attempt_id', v_stage_attempt.id,
            'stage_state', 'SUCCEEDED',
            'stage_fence', v_run.fence,
            'stage_version', v_run.version + 1
        );
        INSERT INTO public.attempt_coordinator_commands (
            command_id, command_kind, job_id, attempt_id, stage_run_id,
            request_digest, result
        ) VALUES (
            v_command_id, 'COMPLETE', v_job.id, v_attempt.id,
            v_run.id, v_request_digest, v_result
        );
        INSERT INTO public.stage_progress_receipts (
            id, attempt_id, stage_run_id, stage_attempt_id, progress_kind,
            source_identity, output_digest, command_id, committed_at
        ) VALUES (
            v_receipt_id, v_attempt.id, v_run.id, v_stage_attempt.id,
            'PHYSICAL_OUTPUT', v_source_identity, v_output_digest,
            v_command_id, v_completed_at
        );
        UPDATE public.stage_leases AS lease
        SET state = 'REVOKED', revoked_at = v_completed_at,
            revoke_reason = 'OUTPUT_COMMITTED'
        WHERE lease.id = v_stage_lease.id AND lease.state = 'ACTIVE';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_complete_authority_stale',
                MESSAGE = 'StageLease changed during completion';
        END IF;
        UPDATE public.stage_allocations AS allocation
        SET state = 'RELEASED', released_at = v_completed_at,
            release_reason = 'OUTPUT_COMMITTED'
        WHERE allocation.id = v_stage_lease.stage_allocation_id
          AND allocation.state = 'ALLOCATED';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_complete_authority_stale',
                MESSAGE = 'StageAllocation changed during completion';
        END IF;
        UPDATE public.stage_attempts AS physical
        SET state = 'SUCCEEDED', output_sealed_at = v_completed_at,
            ended_at = v_completed_at, updated_at = v_completed_at
        WHERE physical.id = v_stage_attempt.id AND physical.state = 'RUNNING';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_complete_authority_stale',
                MESSAGE = 'StageAttempt changed during completion';
        END IF;
        UPDATE public.stage_runs AS run
        SET state = 'SUCCEEDED', winner_stage_attempt_id = v_stage_attempt.id,
            winner_output_identity = v_receipt_id,
            version = run.version + 1, updated_at = v_completed_at
        WHERE run.id = v_run.id AND run.version = v_run.version AND run.state = 'RUNNING';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_complete_authority_stale',
                MESSAGE = 'StageRun changed during completion';
        END IF;
        UPDATE public.stage_dependencies AS dependency
        SET satisfied_progress_receipt_id = v_receipt_id
        WHERE dependency.attempt_id = v_attempt.id
          AND dependency.source_stage_run_id = v_run.id
          AND dependency.satisfied_progress_receipt_id IS NULL;
        UPDATE public.stage_runs AS destination
        SET state = 'READY', version = destination.version + 1,
            updated_at = v_completed_at
        WHERE destination.attempt_id = v_attempt.id
          AND destination.state = 'BLOCKED'
          AND EXISTS (
              SELECT 1 FROM public.stage_dependencies AS inbound
              WHERE inbound.attempt_id = v_attempt.id
                AND inbound.destination_stage_run_id = destination.id
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.stage_dependencies AS inbound
              WHERE inbound.attempt_id = v_attempt.id
                AND inbound.destination_stage_run_id = destination.id
                AND inbound.satisfied_progress_receipt_id IS NULL
          );

        IF NOT EXISTS (
            SELECT 1 FROM public.stage_dependencies AS outgoing
            WHERE outgoing.attempt_id = v_attempt.id
              AND outgoing.source_stage_run_id = v_run.id
        ) THEN
            UPDATE public.attempts AS attempt
            SET state = 'FINALIZING', graph_state = 'FINALIZING',
                finalization_started_at = v_completed_at,
                finalization_deadline_at = v_completed_at
                    + make_interval(secs => v_job.execution_max_finalization_seconds_per_attempt),
                updated_at = v_completed_at
            WHERE attempt.id = v_attempt.id
              AND attempt.state = 'RUNNING'
              AND attempt.graph_state = 'RUNNING';
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'stage_final_output_transition_stale',
                    MESSAGE = 'Parent Attempt changed before finalization';
            END IF;
            UPDATE public.jobs AS job
            SET state = 'FINALIZING', version = job.version + 1,
                updated_at = v_completed_at
            WHERE job.id = v_job.id
              AND job.state = 'RUNNING'
              AND job.current_fence = v_attempt.fence;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'stage_final_output_transition_stale',
                    MESSAGE = 'Job changed before finalization';
            END IF;
        END IF;
        RETURN QUERY SELECT
            v_run.id, v_stage_attempt.id, 'SUCCEEDED'::text,
            v_run.fence, v_run.version + 1, false;
        RETURN;
    END IF;
    IF v_kind = 'EXACT_CACHE_ADVANCE' THEN
        v_expected_attempt_fence := (p_command ->> 'expected_attempt_fence')::bigint;
        v_expected_stage_fence := (p_command ->> 'expected_stage_fence')::bigint;
        v_expected_stage_version := (p_command ->> 'expected_stage_version')::bigint;
        v_receipt_id := (p_command ->> 'progress_receipt_id')::uuid;
        v_source_identity := p_command ->> 'cache_source_identity';
        v_output_digest := decode(p_command ->> 'output_digest', 'hex');
        v_advanced_at := (p_command ->> 'advanced_at')::timestamptz;
        IF v_command_id IS NULL OR v_attempt_id IS NULL OR v_stage_run_id IS NULL
           OR v_expected_attempt_fence <= 0 OR v_expected_stage_fence <= 0
           OR v_expected_stage_version <= 0 OR v_receipt_id IS NULL
           OR v_source_identity IS NULL OR length(v_source_identity) NOT BETWEEN 1 AND 1000
           OR octet_length(v_output_digest) <> 32 OR v_advanced_at IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'stage_exact_cache_command_invalid',
                MESSAGE = 'Exact cache advance command fields are invalid';
        END IF;
        v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
        SELECT command.* INTO v_existing
        FROM public.attempt_coordinator_commands AS command
        WHERE command.command_id = v_command_id;
        IF FOUND THEN
            IF v_existing.command_kind <> 'EXACT_CACHE_ADVANCE'
               OR v_existing.request_digest <> v_request_digest THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    CONSTRAINT = 'attempt_coordinator_command_replay_mismatch',
                    MESSAGE = 'AttemptCoordinator command replay does not match';
            END IF;
            RETURN QUERY SELECT
                (v_existing.result ->> 'stage_run_id')::uuid,
                (v_existing.result ->> 'stage_attempt_id')::uuid,
                v_existing.result ->> 'stage_state',
                (v_existing.result ->> 'stage_fence')::bigint,
                (v_existing.result ->> 'stage_version')::bigint,
                true;
            RETURN;
        END IF;
        SELECT attempt.job_id INTO v_job_id
        FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '23503',
                CONSTRAINT = 'stage_command_attempt_not_found',
                MESSAGE = 'Stage command parent Attempt does not exist';
        END IF;
        SELECT job.* INTO v_job
        FROM public.jobs AS job WHERE job.id = v_job_id FOR UPDATE;
        SELECT attempt.* INTO v_attempt
        FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id FOR UPDATE;
        SELECT run.* INTO v_run
        FROM public.stage_runs AS run
        WHERE run.id = v_stage_run_id AND run.attempt_id = v_attempt_id
        FOR UPDATE;
        IF NOT FOUND OR v_attempt.execution_authority_kind <> 'STAGE_GRAPH'
           OR v_attempt.graph_state NOT IN ('QUEUED', 'RUNNING')
           OR v_attempt.fence <> v_expected_attempt_fence
           OR v_attempt.fence <> v_job.current_fence
           OR v_job.state NOT IN ('QUEUED', 'RETRY_WAIT', 'RUNNING')
           OR v_run.fence <> v_expected_stage_fence
           OR v_run.version <> v_expected_stage_version
           OR v_run.state <> 'READY' THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_exact_cache_authority_stale',
                MESSAGE = 'Exact cache advance fence, version, or state is stale';
        END IF;
        IF v_attempt.graph_state = 'QUEUED' THEN
            IF v_job.state NOT IN ('QUEUED', 'RETRY_WAIT')
               OR v_job.billable_started_at IS NOT NULL
               OR v_attempt.started_at IS NOT NULL THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'stage_first_progress_stale',
                    MESSAGE = 'First graph progress authority is stale';
            END IF;
            UPDATE public.projects AS project
            SET queued_count = project.queued_count - 1,
                retry_wait_count = project.retry_wait_count
                    - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END,
                running_count = project.running_count + 1
            WHERE project.id = v_job.project_id
              AND project.organization_id = v_job.organization_id
              AND project.queued_count > 0
              AND project.running_count < project.running_limit
              AND (v_job.state <> 'RETRY_WAIT' OR project.retry_wait_count > 0);
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            IF v_rows <> 1 THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    CONSTRAINT = 'stage_first_progress_project_capacity_changed',
                    MESSAGE = 'Project capacity changed before first graph progress';
            END IF;
            UPDATE public.worker_pools AS pool
            SET queued_count = pool.queued_count - 1,
                retry_wait_count = pool.retry_wait_count
                    - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END
            WHERE pool.id = v_job.worker_pool_id
              AND pool.queued_count > 0
              AND (v_job.state <> 'RETRY_WAIT' OR pool.retry_wait_count > 0);
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            IF v_rows <> 1 THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    CONSTRAINT = 'stage_first_progress_pool_capacity_changed',
                    MESSAGE = 'Admission pool counters changed before first graph progress';
            END IF;
            UPDATE public.jobs AS job
            SET state = 'RUNNING', billable_started_at = v_advanced_at,
                version = job.version + 1, updated_at = v_advanced_at
            WHERE job.id = v_job.id
              AND job.version = v_job.version
              AND job.current_fence = v_job.current_fence
              AND job.state = v_job.state;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'stage_first_progress_stale',
                    MESSAGE = 'Job changed before first graph progress';
            END IF;
            UPDATE public.attempts AS attempt
            SET state = 'RUNNING', graph_state = 'RUNNING',
                started_at = v_advanced_at, updated_at = v_advanced_at
            WHERE attempt.id = v_attempt.id
              AND attempt.state = 'ASSIGNED'
              AND attempt.graph_state = 'QUEUED'
              AND attempt.fence = v_attempt.fence;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'stage_first_progress_stale',
                    MESSAGE = 'Attempt changed before first graph progress';
            END IF;
        ELSIF v_job.state <> 'RUNNING' OR v_job.billable_started_at IS NULL
              OR v_attempt.started_at IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'stage_running_requires_billable_start',
                MESSAGE = 'Running graph is missing immutable Billable Start';
        END IF;

        v_result := jsonb_build_object(
            'stage_run_id', v_run.id,
            'stage_attempt_id', '00000000-0000-0000-0000-000000000000'::uuid,
            'stage_state', 'SUCCEEDED',
            'stage_fence', v_run.fence,
            'stage_version', v_run.version + 1
        );
        INSERT INTO public.attempt_coordinator_commands (
            command_id, command_kind, job_id, attempt_id, stage_run_id,
            request_digest, result
        ) VALUES (
            v_command_id, 'EXACT_CACHE_ADVANCE', v_job.id, v_attempt.id,
            v_run.id, v_request_digest, v_result
        );
        INSERT INTO public.stage_progress_receipts (
            id, attempt_id, stage_run_id, stage_attempt_id, progress_kind,
            source_identity, output_digest, command_id, committed_at
        ) VALUES (
            v_receipt_id, v_attempt.id, v_run.id, NULL, 'EXACT_CACHE',
            v_source_identity, v_output_digest, v_command_id, v_advanced_at
        );
        UPDATE public.stage_runs AS run
        SET state = 'SUCCEEDED', winner_output_identity = v_receipt_id,
            version = run.version + 1, updated_at = v_advanced_at
        WHERE run.id = v_run.id AND run.version = v_run.version AND run.state = 'READY';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_exact_cache_authority_stale',
                MESSAGE = 'StageRun changed during exact cache advance';
        END IF;
        UPDATE public.stage_dependencies AS dependency
        SET satisfied_progress_receipt_id = v_receipt_id
        WHERE dependency.attempt_id = v_attempt.id
          AND dependency.source_stage_run_id = v_run.id
          AND dependency.satisfied_progress_receipt_id IS NULL;
        UPDATE public.stage_runs AS destination
        SET state = 'READY', version = destination.version + 1,
            updated_at = v_advanced_at
        WHERE destination.attempt_id = v_attempt.id
          AND destination.state = 'BLOCKED'
          AND EXISTS (
              SELECT 1 FROM public.stage_dependencies AS inbound
              WHERE inbound.attempt_id = v_attempt.id
                AND inbound.destination_stage_run_id = destination.id
          )
          AND NOT EXISTS (
              SELECT 1 FROM public.stage_dependencies AS inbound
              WHERE inbound.attempt_id = v_attempt.id
                AND inbound.destination_stage_run_id = destination.id
                AND inbound.satisfied_progress_receipt_id IS NULL
          );
        RETURN QUERY SELECT
            v_run.id, '00000000-0000-0000-0000-000000000000'::uuid,
            'SUCCEEDED'::text, v_run.fence, v_run.version + 1, false;
        RETURN;
    END IF;
    IF v_kind = 'START' THEN
        v_expected_attempt_fence := (p_command ->> 'expected_attempt_fence')::bigint;
        v_expected_stage_fence := (p_command ->> 'expected_stage_fence')::bigint;
        v_expected_stage_version := (p_command ->> 'expected_stage_version')::bigint;
        v_stage_attempt_id := (p_command ->> 'stage_attempt_id')::uuid;
        v_lease_id := (p_command ->> 'stage_lease_id')::uuid;
        v_started_at := (p_command ->> 'started_at')::timestamptz;
        IF v_command_id IS NULL OR v_attempt_id IS NULL OR v_stage_run_id IS NULL
           OR v_expected_attempt_fence <= 0 OR v_expected_stage_fence <= 0
           OR v_expected_stage_version <= 0 OR v_stage_attempt_id IS NULL
           OR v_lease_id IS NULL OR v_started_at IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'stage_start_command_invalid',
                MESSAGE = 'Stage start command fields are invalid';
        END IF;
        v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
        SELECT command.* INTO v_existing
        FROM public.attempt_coordinator_commands AS command
        WHERE command.command_id = v_command_id;
        IF FOUND THEN
            IF v_existing.command_kind <> 'START'
               OR v_existing.request_digest <> v_request_digest THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    CONSTRAINT = 'attempt_coordinator_command_replay_mismatch',
                    MESSAGE = 'AttemptCoordinator command replay does not match';
            END IF;
            RETURN QUERY SELECT
                (v_existing.result ->> 'stage_run_id')::uuid,
                (v_existing.result ->> 'stage_attempt_id')::uuid,
                v_existing.result ->> 'stage_state',
                (v_existing.result ->> 'stage_fence')::bigint,
                (v_existing.result ->> 'stage_version')::bigint,
                true;
            RETURN;
        END IF;

        SELECT attempt.job_id INTO v_job_id
        FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '23503',
                CONSTRAINT = 'stage_command_attempt_not_found',
                MESSAGE = 'Stage command parent Attempt does not exist';
        END IF;
        SELECT job.* INTO v_job
        FROM public.jobs AS job WHERE job.id = v_job_id FOR UPDATE;
        SELECT attempt.* INTO v_attempt
        FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id FOR UPDATE;
        SELECT run.* INTO v_run
        FROM public.stage_runs AS run
        WHERE run.id = v_stage_run_id AND run.attempt_id = v_attempt_id
        FOR UPDATE;
        SELECT physical.* INTO v_stage_attempt
        FROM public.stage_attempts AS physical
        WHERE physical.id = v_stage_attempt_id AND physical.stage_run_id = v_stage_run_id
        FOR UPDATE;
        SELECT lease.* INTO v_stage_lease
        FROM public.stage_leases AS lease
        WHERE lease.id = v_lease_id
          AND lease.stage_attempt_id = v_stage_attempt_id
          AND lease.stage_run_id = v_stage_run_id
        FOR UPDATE;
        IF v_attempt.execution_authority_kind <> 'STAGE_GRAPH'
           OR v_attempt.graph_state NOT IN ('QUEUED', 'RUNNING')
           OR v_attempt.fence <> v_expected_attempt_fence
           OR v_attempt.fence <> v_job.current_fence
           OR v_job.state NOT IN ('QUEUED', 'RETRY_WAIT', 'RUNNING')
           OR v_run.fence <> v_expected_stage_fence
           OR v_run.version <> v_expected_stage_version
           OR v_run.state <> 'ASSIGNED'
           OR v_stage_attempt.state <> 'ASSIGNED'
           OR v_stage_lease.state <> 'ACTIVE'
           OR v_stage_lease.attempt_fence <> v_attempt.fence
           OR v_stage_lease.stage_fence <> v_run.fence
           OR v_started_at < v_stage_lease.issued_at
           OR v_started_at >= v_stage_lease.expires_at
           OR NOT public.vela_worker_instance_authority_matches(
               v_stage_lease.worker_instance_id,
               v_stage_lease.worker_instance_epoch,
               v_stage_lease.device_set_digest,
               v_stage_lease.membership_digest,
               v_stage_lease.model_residency_id,
               v_stage_lease.model_runtime_epoch
           ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_start_authority_stale',
                MESSAGE = 'Stage start Lease, fence, version, or runtime authority is stale';
        END IF;

        IF v_attempt.graph_state = 'QUEUED' THEN
            IF v_job.state NOT IN ('QUEUED', 'RETRY_WAIT')
               OR v_job.billable_started_at IS NOT NULL
               OR v_attempt.started_at IS NOT NULL THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'stage_first_progress_stale',
                    MESSAGE = 'First graph progress authority is stale';
            END IF;
            UPDATE public.projects AS project
            SET queued_count = project.queued_count - 1,
                retry_wait_count = project.retry_wait_count
                    - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END,
                running_count = project.running_count + 1
            WHERE project.id = v_job.project_id
              AND project.organization_id = v_job.organization_id
              AND project.queued_count > 0
              AND project.running_count < project.running_limit
              AND (
                  v_job.state <> 'RETRY_WAIT' OR project.retry_wait_count > 0
              );
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            IF v_rows <> 1 THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    CONSTRAINT = 'stage_first_progress_project_capacity_changed',
                    MESSAGE = 'Project capacity changed before first graph progress';
            END IF;
            UPDATE public.worker_pools AS pool
            SET queued_count = pool.queued_count - 1,
                retry_wait_count = pool.retry_wait_count
                    - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END
            WHERE pool.id = v_job.worker_pool_id
              AND pool.queued_count > 0
              AND (
                  v_job.state <> 'RETRY_WAIT' OR pool.retry_wait_count > 0
              );
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            IF v_rows <> 1 THEN
                RAISE EXCEPTION USING
                    ERRCODE = '55000',
                    CONSTRAINT = 'stage_first_progress_pool_capacity_changed',
                    MESSAGE = 'Admission pool counters changed before first graph progress';
            END IF;
            UPDATE public.jobs AS job
            SET state = 'RUNNING',
                billable_started_at = v_started_at,
                version = job.version + 1,
                updated_at = v_started_at
            WHERE job.id = v_job.id
              AND job.version = v_job.version
              AND job.current_fence = v_job.current_fence
              AND job.state = v_job.state;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'stage_first_progress_stale',
                    MESSAGE = 'Job changed before first graph progress';
            END IF;
            UPDATE public.attempts AS attempt
            SET state = 'RUNNING', graph_state = 'RUNNING',
                started_at = v_started_at, updated_at = v_started_at
            WHERE attempt.id = v_attempt.id
              AND attempt.state = 'ASSIGNED'
              AND attempt.graph_state = 'QUEUED'
              AND attempt.fence = v_attempt.fence;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = '40001',
                    CONSTRAINT = 'stage_first_progress_stale',
                    MESSAGE = 'Attempt changed before first graph progress';
            END IF;
        ELSIF v_job.state <> 'RUNNING' OR v_job.billable_started_at IS NULL
              OR v_attempt.started_at IS NULL THEN
            RAISE EXCEPTION USING
                ERRCODE = '23514',
                CONSTRAINT = 'stage_running_requires_billable_start',
                MESSAGE = 'Running graph is missing immutable Billable Start';
        END IF;

        UPDATE public.stage_attempts AS physical
        SET state = 'RUNNING', started_at = v_started_at, updated_at = v_started_at
        WHERE physical.id = v_stage_attempt.id AND physical.state = 'ASSIGNED';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_start_authority_stale',
                MESSAGE = 'StageAttempt changed before start';
        END IF;
        UPDATE public.stage_runs AS run
        SET state = 'RUNNING', version = run.version + 1, updated_at = v_started_at
        WHERE run.id = v_run.id AND run.version = v_run.version AND run.state = 'ASSIGNED';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '40001',
                CONSTRAINT = 'stage_start_authority_stale',
                MESSAGE = 'StageRun changed before start';
        END IF;
        v_result := jsonb_build_object(
            'stage_run_id', v_run.id,
            'stage_attempt_id', v_stage_attempt.id,
            'stage_state', 'RUNNING',
            'stage_fence', v_run.fence,
            'stage_version', v_run.version + 1
        );
        INSERT INTO public.attempt_coordinator_commands (
            command_id, command_kind, job_id, attempt_id, stage_run_id,
            request_digest, result
        ) VALUES (
            v_command_id, 'START', v_job.id, v_attempt.id, v_run.id,
            v_request_digest, v_result
        );
        RETURN QUERY SELECT
            v_run.id, v_stage_attempt.id, 'RUNNING'::text,
            v_run.fence, v_run.version + 1, false;
        RETURN;
    END IF;
    IF v_kind IS DISTINCT FROM 'ASSIGN' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_command_kind_unsupported',
            MESSAGE = 'Stage command kind is not implemented by this schema revision';
    END IF;
    v_expected_attempt_fence := (p_command ->> 'expected_attempt_fence')::bigint;
    v_expected_stage_fence := (p_command ->> 'expected_stage_fence')::bigint;
    v_expected_stage_version := (p_command ->> 'expected_stage_version')::bigint;
    v_stage_attempt_id := (p_command ->> 'stage_attempt_id')::uuid;
    v_allocation_id := (p_command ->> 'stage_allocation_id')::uuid;
    v_lease_id := (p_command ->> 'stage_lease_id')::uuid;
    v_profile_id := (p_command ->> 'stage_profile_revision_id')::uuid;
    v_capacity_pool_id := (p_command ->> 'capacity_pool_id')::uuid;
    v_worker_id := (p_command ->> 'worker_instance_id')::uuid;
    v_worker_epoch := (p_command ->> 'worker_instance_epoch')::bigint;
    v_device_set_digest := decode(p_command ->> 'device_set_digest', 'hex');
    v_membership_digest := decode(p_command ->> 'membership_digest', 'hex');
    v_residency_id := (p_command ->> 'model_residency_id')::uuid;
    v_model_runtime_epoch := (p_command ->> 'model_runtime_epoch')::bigint;
    v_token_digest := decode(p_command ->> 'token_digest', 'hex');
    v_execution_nonce := decode(p_command ->> 'execution_nonce', 'hex');
    v_signing_key_id := p_command ->> 'signing_key_id';
    v_issued_at := (p_command ->> 'issued_at')::timestamptz;
    v_expires_at := (p_command ->> 'expires_at')::timestamptz;
    v_local_deadline_at := (p_command ->> 'local_deadline_at')::timestamptz;
    IF v_command_id IS NULL OR v_attempt_id IS NULL OR v_stage_run_id IS NULL
       OR v_expected_attempt_fence <= 0 OR v_expected_stage_fence <= 0
       OR v_expected_stage_version <= 0 OR v_stage_attempt_id IS NULL
       OR v_allocation_id IS NULL OR v_lease_id IS NULL OR v_profile_id IS NULL
       OR v_capacity_pool_id IS NULL OR v_worker_id IS NULL OR v_worker_epoch <= 0
       OR octet_length(v_device_set_digest) <> 32
       OR octet_length(v_membership_digest) <> 32 OR v_residency_id IS NULL
       OR v_model_runtime_epoch <= 0 OR octet_length(v_token_digest) <> 32
       OR octet_length(v_execution_nonce) <> 32 OR v_signing_key_id IS NULL
       OR length(v_signing_key_id) NOT BETWEEN 1 AND 100
       OR v_issued_at IS NULL OR v_expires_at <= v_issued_at
       OR v_local_deadline_at <= v_issued_at OR v_local_deadline_at > v_expires_at
       OR jsonb_typeof(p_command -> 'capacity_vector') <> 'object' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_assign_command_invalid',
            MESSAGE = 'Stage assign command fields are invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing
    FROM public.attempt_coordinator_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'ASSIGN'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'attempt_coordinator_command_replay_mismatch',
                MESSAGE = 'AttemptCoordinator command replay does not match';
        END IF;
        RETURN QUERY SELECT
            (v_existing.result ->> 'stage_run_id')::uuid,
            (v_existing.result ->> 'stage_attempt_id')::uuid,
            v_existing.result ->> 'stage_state',
            (v_existing.result ->> 'stage_fence')::bigint,
            (v_existing.result ->> 'stage_version')::bigint,
            true;
        RETURN;
    END IF;

    SELECT attempt.job_id INTO v_job_id
    FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '23503',
            CONSTRAINT = 'stage_command_attempt_not_found',
            MESSAGE = 'Stage command parent Attempt does not exist';
    END IF;
    SELECT job.* INTO v_job
    FROM public.jobs AS job WHERE job.id = v_job_id FOR UPDATE;
    SELECT attempt.* INTO v_attempt
    FROM public.attempts AS attempt WHERE attempt.id = v_attempt_id FOR UPDATE;
    SELECT run.* INTO v_run
    FROM public.stage_runs AS run
    WHERE run.id = v_stage_run_id AND run.attempt_id = v_attempt_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '23503',
            CONSTRAINT = 'stage_command_run_not_found',
            MESSAGE = 'StageRun does not exist in parent Attempt';
    END IF;
    IF v_attempt.execution_authority_kind <> 'STAGE_GRAPH'
       OR NOT (
           (
               v_attempt.graph_state = 'QUEUED'
               AND v_job.state IN ('QUEUED', 'RETRY_WAIT')
               AND v_job.billable_started_at IS NULL
               AND v_attempt.started_at IS NULL
           )
           OR (
               v_attempt.graph_state = 'RUNNING'
               AND v_job.state = 'RUNNING'
               AND v_job.billable_started_at IS NOT NULL
               AND v_attempt.started_at IS NOT NULL
           )
       )
       OR v_attempt.fence <> v_expected_attempt_fence
       OR v_attempt.fence <> v_job.current_fence
       OR v_run.fence <> v_expected_stage_fence
       OR v_run.version <> v_expected_stage_version
       OR v_run.state <> 'READY' THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_command_authority_stale',
            MESSAGE = 'Stage command fence, version, or state is stale';
    END IF;
    IF NOT v_profile_id = ANY(v_run.allowed_stage_profile_revision_ids)
       OR NOT EXISTS (
           SELECT 1
           FROM public.capacity_pools AS pool
           JOIN public.stage_profile_revisions AS profile
             ON profile.id = pool.stage_profile_revision_id
           JOIN public.model_residencies AS residency
             ON residency.id = v_residency_id
            AND residency.worker_instance_id = v_worker_id
            AND residency.worker_instance_epoch = v_worker_epoch
            AND residency.model_component_revision = profile.model_component_revision
            AND residency.model_runtime_epoch = v_model_runtime_epoch
            AND residency.state = 'READY'
           WHERE pool.id = v_capacity_pool_id
             AND pool.stage_profile_revision_id = v_profile_id
             AND pool.state = 'ACTIVE'
       )
       OR NOT public.vela_worker_instance_authority_matches(
           v_worker_id, v_worker_epoch, v_device_set_digest, v_membership_digest,
           v_residency_id, v_model_runtime_epoch
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_assign_worker_authority_ineligible',
            MESSAGE = 'Stage assignment WorkerInstance authority is not eligible';
    END IF;

    SELECT budget.* INTO v_stage_budget
    FROM public.stage_retry_budgets AS budget
    WHERE budget.stage_run_id = v_run.id
    FOR UPDATE;
    IF NOT FOUND OR v_stage_budget.state <> 'ACTIVE'
       OR v_stage_budget.attempts_consumed >= v_stage_budget.max_attempts THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_retry_budget_exhausted',
            MESSAGE = 'Stage retry budget is exhausted';
    END IF;
    v_physical_attempt_number := v_stage_budget.attempts_consumed + 1;

    INSERT INTO public.stage_attempts (
        id, organization_id, project_id, attempt_id, stage_run_id,
        physical_attempt_number, state, selected_stage_profile_revision_id, assigned_at
    ) VALUES (
        v_stage_attempt_id, v_attempt.organization_id, v_attempt.project_id,
        v_attempt.id, v_run.id, v_physical_attempt_number, 'ASSIGNED',
        v_profile_id, v_issued_at
    );
    INSERT INTO public.stage_allocations (
        id, attempt_id, stage_run_id, stage_attempt_id, worker_instance_id,
        worker_instance_epoch, capacity_pool_id, device_set_digest,
        membership_digest, model_residency_id, model_runtime_epoch,
        capacity_vector, state, allocated_at
    ) VALUES (
        v_allocation_id, v_attempt.id, v_run.id, v_stage_attempt_id, v_worker_id,
        v_worker_epoch, v_capacity_pool_id, v_device_set_digest,
        v_membership_digest, v_residency_id, v_model_runtime_epoch,
        p_command -> 'capacity_vector', 'ALLOCATED', v_issued_at
    );
    INSERT INTO public.stage_leases (
        id, attempt_id, stage_run_id, stage_attempt_id, stage_allocation_id,
        attempt_fence, stage_fence, worker_instance_id, worker_instance_epoch,
        device_set_digest, membership_digest, model_residency_id,
        model_runtime_epoch, token_digest, signing_key_id, execution_nonce,
        issued_at, expires_at, local_deadline_at, state
    ) VALUES (
        v_lease_id, v_attempt.id, v_run.id, v_stage_attempt_id, v_allocation_id,
        v_attempt.fence, v_run.fence, v_worker_id, v_worker_epoch,
        v_device_set_digest, v_membership_digest, v_residency_id,
        v_model_runtime_epoch, v_token_digest, v_signing_key_id,
        v_execution_nonce, v_issued_at, v_expires_at, v_local_deadline_at, 'ACTIVE'
    );
    UPDATE public.stage_retry_budgets AS budget
    SET attempts_consumed = budget.attempts_consumed + 1,
        version = budget.version + 1,
        updated_at = transaction_timestamp()
    WHERE budget.stage_run_id = v_run.id
      AND budget.version = v_stage_budget.version;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_retry_budget_stale',
            MESSAGE = 'Stage retry budget changed during assignment';
    END IF;
    UPDATE public.stage_runs AS run
    SET state = 'ASSIGNED', version = run.version + 1,
        updated_at = transaction_timestamp()
    WHERE run.id = v_run.id AND run.version = v_run.version AND run.state = 'READY';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_command_authority_stale',
            MESSAGE = 'StageRun changed during assignment';
    END IF;

    v_result := jsonb_build_object(
        'stage_run_id', v_run.id,
        'stage_attempt_id', v_stage_attempt_id,
        'stage_state', 'ASSIGNED',
        'stage_fence', v_run.fence,
        'stage_version', v_run.version + 1
    );
    INSERT INTO public.attempt_coordinator_commands (
        command_id, command_kind, job_id, attempt_id, stage_run_id,
        request_digest, result
    ) VALUES (
        v_command_id, 'ASSIGN', v_job.id, v_attempt.id, v_run.id,
        v_request_digest, v_result
    );
    RETURN QUERY SELECT
        v_run.id, v_stage_attempt_id, 'ASSIGNED'::text,
        v_run.fence, v_run.version + 1, false;
END
$$;
REVOKE ALL ON FUNCTION vela_apply_stage_command(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_apply_stage_command(jsonb) OWNER TO vela_attempt_coordinator_owner;

ALTER TABLE execution_graph_snapshots ENABLE ROW LEVEL SECURITY;
ALTER TABLE execution_graph_snapshots FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_runs ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_runs FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_dependencies ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_dependencies FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_attempts FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_allocations ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_allocations FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_leases ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_leases FORCE ROW LEVEL SECURITY;
ALTER TABLE attempt_retry_budgets ENABLE ROW LEVEL SECURITY;
ALTER TABLE attempt_retry_budgets FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_retry_budgets ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_retry_budgets FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_storage_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_storage_reservations FORCE ROW LEVEL SECURITY;
ALTER TABLE attempt_coordinator_commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE attempt_coordinator_commands FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_progress_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_progress_receipts FORCE ROW LEVEL SECURITY;

GRANT USAGE ON SCHEMA public TO vela_attempt_coordinator;
GRANT EXECUTE ON FUNCTION vela_instantiate_stage_graph(jsonb) TO vela_attempt_coordinator;
GRANT EXECUTE ON FUNCTION vela_apply_stage_command(jsonb) TO vela_attempt_coordinator;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    execution_graph_snapshots, stage_runs, stage_dependencies, stage_attempts,
    stage_allocations, stage_leases, attempt_retry_budgets, stage_retry_budgets,
    stage_storage_reservations, attempt_coordinator_commands, stage_progress_receipts
TO vela_attempt_coordinator_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    attempts, jobs, retry_runtime_states, projects, worker_pools,
    credit_reservations, organization_credit_accounts
TO vela_attempt_coordinator_owner;
GRANT SELECT ON
    execution_graph_revisions, execution_graph_stages, execution_graph_edges,
    execution_profile_revisions, execution_profile_stage_options,
    execution_profile_connector_options, stage_profile_revisions
TO vela_attempt_coordinator_owner;
GRANT SELECT ON
    capacity_pools, worker_instances, worker_instance_epochs, worker_members,
    active_device_bindings, model_residencies, capacity_observations
TO vela_attempt_coordinator_owner;
GRANT EXECUTE ON FUNCTION vela_worker_instance_authority_matches(
    uuid, bigint, bytea, bytea, uuid, bigint
) TO vela_attempt_coordinator_owner;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM attempts WHERE execution_authority_kind = 'STAGE_GRAPH')
       OR EXISTS (SELECT 1 FROM execution_graph_snapshots)
       OR EXISTS (SELECT 1 FROM attempt_coordinator_commands) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_attempt_authority_rollback_is_unsafe',
            MESSAGE = 'Stage Attempt authority must be empty before rollback';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_apply_stage_command(jsonb) FROM vela_attempt_coordinator;
DROP FUNCTION vela_apply_stage_command(jsonb);
REVOKE EXECUTE ON FUNCTION vela_instantiate_stage_graph(jsonb) FROM vela_attempt_coordinator;
DROP FUNCTION vela_instantiate_stage_graph(jsonb);
DROP TRIGGER attempts_guard_stage_graph_writer ON attempts;
DROP FUNCTION vela_guard_stage_graph_attempt_writer();
DROP TRIGGER stage_storage_reservations_attempt_coordinator_writer ON stage_storage_reservations;
DROP TRIGGER stage_retry_budgets_attempt_coordinator_writer ON stage_retry_budgets;
DROP TRIGGER attempt_retry_budgets_attempt_coordinator_writer ON attempt_retry_budgets;
DROP TRIGGER stage_leases_attempt_coordinator_writer ON stage_leases;
DROP TRIGGER stage_allocations_attempt_coordinator_writer ON stage_allocations;
DROP TRIGGER stage_attempts_attempt_coordinator_writer ON stage_attempts;
DROP TRIGGER stage_dependencies_attempt_coordinator_writer ON stage_dependencies;
DROP TRIGGER stage_runs_attempt_coordinator_writer ON stage_runs;
DROP TRIGGER stage_progress_receipts_immutable ON stage_progress_receipts;
DROP TRIGGER attempt_coordinator_commands_immutable ON attempt_coordinator_commands;
DROP TRIGGER execution_graph_snapshots_immutable ON execution_graph_snapshots;
DROP FUNCTION vela_reject_attempt_coordinator_immutable_mutation();
DROP FUNCTION vela_guard_attempt_coordinator_writer();

ALTER TABLE stage_runs DROP CONSTRAINT stage_runs_winner_output_fk;
ALTER TABLE stage_runs DROP CONSTRAINT stage_runs_winner_attempt_fk;
ALTER TABLE stage_dependencies DROP CONSTRAINT stage_dependencies_satisfied_receipt_fk;
DROP TABLE stage_progress_receipts;
DROP TABLE attempt_coordinator_commands;
DROP TABLE stage_storage_reservations;
DROP TABLE stage_retry_budgets;
DROP TABLE attempt_retry_budgets;
DROP TABLE stage_leases;
DROP TABLE stage_allocations;
DROP TABLE stage_attempts;
DROP TABLE stage_dependencies;
DROP TABLE stage_runs;

ALTER TABLE attempts
    DROP CONSTRAINT attempts_graph_state_projection,
    DROP CONSTRAINT attempts_authority_shape,
    DROP CONSTRAINT attempts_execution_graph_snapshot_fk,
    DROP COLUMN execution_graph_snapshot_id,
    DROP COLUMN graph_state,
    DROP COLUMN execution_authority_kind,
    ALTER COLUMN execution_profile_revision_id SET NOT NULL,
    ALTER COLUMN worker_pool_id SET NOT NULL,
    ALTER COLUMN worker_id SET NOT NULL,
    ALTER COLUMN worker_epoch SET NOT NULL,
    ALTER COLUMN assigned_at SET NOT NULL,
    ALTER COLUMN profile_certification_id SET NOT NULL;
DROP TABLE execution_graph_snapshots;

DROP TYPE stage_storage_reservation_state;
DROP TYPE stage_retry_budget_state;
DROP TYPE stage_progress_kind;
DROP TYPE stage_lease_state;
DROP TYPE stage_allocation_state;
DROP TYPE stage_attempt_state;
DROP TYPE stage_run_state;
DROP TYPE graph_attempt_state;
DROP TYPE execution_authority_kind;
-- +goose StatementEnd
