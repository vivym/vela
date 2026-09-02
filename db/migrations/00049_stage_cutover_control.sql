-- +goose Up
-- +goose StatementBegin
CREATE TYPE stage_cutover_scope AS ENUM ('INTERNAL', 'PRODUCTION');
CREATE TYPE stage_cutover_mode AS ENUM ('LEGACY_ONLY', 'COHORT', 'STAGE_ONLY');

CREATE TABLE stage_cutover_revisions (
    id uuid PRIMARY KEY,
    revision bigint NOT NULL UNIQUE CHECK (revision > 0),
    previous_revision_id uuid REFERENCES stage_cutover_revisions(id),
    rollback_of_revision_id uuid REFERENCES stage_cutover_revisions(id),
    scope stage_cutover_scope NOT NULL,
    mode stage_cutover_mode NOT NULL,
    model_revision_id uuid REFERENCES model_revisions(id),
    stage_cohort_basis_points integer NOT NULL CHECK (
        stage_cohort_basis_points BETWEEN 0 AND 10000
    ),
    execution_graph_revision_id uuid REFERENCES execution_graph_revisions(id),
    execution_profile_revision_id uuid,
    reserved_storage_bytes bigint NOT NULL CHECK (reserved_storage_bytes >= 0),
    minimum_observation_seconds integer NOT NULL CHECK (
        minimum_observation_seconds BETWEEN 1 AND 2592000
    ),
    release_digest bytea NOT NULL CHECK (octet_length(release_digest) = 32),
    configuration_revision text NOT NULL CHECK (
        length(configuration_revision) BETWEEN 1 AND 300
        AND btrim(configuration_revision) = configuration_revision
        AND configuration_revision !~ '[[:cntrl:]]'
    ),
    configuration_digest bytea NOT NULL CHECK (
        octet_length(configuration_digest) = 32
    ),
    connector_set_digest bytea NOT NULL CHECK (
        octet_length(connector_set_digest) = 32
    ),
    launch_manifest_digest bytea REFERENCES production_gate_manifests(manifest_digest),
    activated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    activated_by text NOT NULL CHECK (
        length(activated_by) BETWEEN 1 AND 300
        AND btrim(activated_by) = activated_by
        AND activated_by !~ '[[:cntrl:]]'
    ),
    reason text NOT NULL CHECK (
        length(reason) BETWEEN 1 AND 2000
        AND btrim(reason) = reason
        AND reason !~ '[[:cntrl:]]'
    ),
    CHECK (
        (
            mode = 'LEGACY_ONLY'
            AND stage_cohort_basis_points = 0
            AND execution_graph_revision_id IS NULL
            AND execution_profile_revision_id IS NULL
            AND model_revision_id IS NULL
            AND reserved_storage_bytes = 0
        ) OR (
            mode = 'COHORT'
            AND stage_cohort_basis_points BETWEEN 1 AND 9999
            AND execution_graph_revision_id IS NOT NULL
            AND execution_profile_revision_id IS NOT NULL
            AND model_revision_id IS NOT NULL
            AND reserved_storage_bytes > 0
        ) OR (
            mode = 'STAGE_ONLY'
            AND stage_cohort_basis_points = 10000
            AND execution_graph_revision_id IS NOT NULL
            AND execution_profile_revision_id IS NOT NULL
            AND model_revision_id IS NOT NULL
            AND reserved_storage_bytes > 0
        )
    ),
    CHECK (
        scope <> 'PRODUCTION'
        OR mode = 'LEGACY_ONLY'
        OR launch_manifest_digest IS NOT NULL
    ),
    UNIQUE (id, execution_graph_revision_id, execution_profile_revision_id),
    CONSTRAINT stage_cutover_profile_graph
        FOREIGN KEY (execution_profile_revision_id, execution_graph_revision_id)
        REFERENCES execution_profile_revisions(id, execution_graph_revision_id)
);

CREATE TABLE stage_cutover_control (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    current_revision_id uuid NOT NULL REFERENCES stage_cutover_revisions(id),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE stage_cutover_internal_projects (
    cutover_revision_id uuid NOT NULL REFERENCES stage_cutover_revisions(id),
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    authorized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    authorized_by text NOT NULL CHECK (
        length(authorized_by) BETWEEN 1 AND 300
        AND btrim(authorized_by) = authorized_by
        AND authorized_by !~ '[[:cntrl:]]'
    ),
    PRIMARY KEY (cutover_revision_id, organization_id, project_id),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id)
);

INSERT INTO stage_cutover_revisions (
    id, revision, scope, mode, stage_cohort_basis_points,
    reserved_storage_bytes, minimum_observation_seconds,
    release_digest, configuration_revision, configuration_digest,
    connector_set_digest, activated_by, reason
) VALUES (
    '00000000-0000-0000-0000-000000000049', 1, 'INTERNAL', 'LEGACY_ONLY', 0,
    0, 1,
    decode(repeat('00', 32), 'hex'), 'migration-00049-legacy-baseline',
    sha256(convert_to('migration-00049-legacy-baseline', 'UTF8')),
    sha256(convert_to('', 'UTF8')),
    'migration-00049', 'freeze the pre-cutover legacy routing baseline'
);
INSERT INTO stage_cutover_control (singleton, current_revision_id)
VALUES (true, '00000000-0000-0000-0000-000000000049');

CREATE FUNCTION vela_reject_stage_cutover_history_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'stage_cutover_history_immutable',
        MESSAGE = TG_TABLE_NAME || ' is append-only';
END
$$;

CREATE FUNCTION vela_guard_stage_cutover_control_writer() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF current_user <> 'vela_catalog_promotion_owner' THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'stage_cutover_control_function_required',
            MESSAGE = 'Stage cutover control may change only through its versioned function';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER stage_cutover_revisions_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON stage_cutover_revisions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_cutover_history_mutation();
CREATE TRIGGER stage_cutover_control_writer
BEFORE UPDATE OR DELETE OR TRUNCATE ON stage_cutover_control
FOR EACH STATEMENT EXECUTE FUNCTION vela_guard_stage_cutover_control_writer();
CREATE TRIGGER stage_cutover_internal_projects_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON stage_cutover_internal_projects
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_cutover_history_mutation();

CREATE FUNCTION vela_execution_profile_connector_set_digest(
    p_execution_profile_revision_id uuid,
    p_execution_graph_revision_id uuid
) RETURNS bytea
LANGUAGE sql
STABLE
SET search_path = pg_catalog, public
AS $$
    SELECT sha256(convert_to(COALESCE(string_agg(
        option.execution_graph_edge_id::text || ':' ||
        option.connector_revision_id::text || ':' ||
        encode(connector.content_digest, 'hex') || ':' ||
        option.preference::text || ':' || option.required_topology_policy::text,
        E'\n' ORDER BY option.execution_graph_edge_id, option.preference,
        option.connector_revision_id
    ), ''), 'UTF8'))
    FROM public.execution_profile_connector_options AS option
    JOIN public.connector_revisions AS connector
      ON connector.id = option.connector_revision_id
    WHERE option.execution_profile_revision_id = p_execution_profile_revision_id
      AND option.execution_graph_revision_id = p_execution_graph_revision_id
$$;

CREATE FUNCTION vela_activate_stage_cutover(
    p_id uuid,
    p_revision bigint,
    p_previous_revision_id uuid,
    p_scope stage_cutover_scope,
    p_mode stage_cutover_mode,
    p_stage_cohort_basis_points integer,
    p_execution_graph_revision_id uuid,
    p_execution_profile_revision_id uuid,
    p_reserved_storage_bytes bigint,
    p_minimum_observation_seconds integer,
    p_release_digest bytea,
    p_configuration_revision text,
    p_configuration_digest bytea,
    p_connector_set_digest bytea,
    p_launch_manifest_digest bytea,
    p_activated_by text,
    p_reason text
) RETURNS stage_cutover_revisions
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_control public.stage_cutover_control%ROWTYPE;
    v_previous public.stage_cutover_revisions%ROWTYPE;
    v_result public.stage_cutover_revisions%ROWTYPE;
    v_manifest public.production_gate_manifests%ROWTYPE;
    v_connector_digest bytea;
    v_model_revision_id uuid;
    v_rollback_of uuid;
BEGIN
    IF p_id IS NULL OR p_revision <= 1 OR p_previous_revision_id IS NULL
       OR p_scope IS NULL OR p_mode IS NULL
       OR p_stage_cohort_basis_points NOT BETWEEN 0 AND 10000
       OR p_reserved_storage_bytes < 0
       OR p_minimum_observation_seconds NOT BETWEEN 1 AND 2592000
       OR p_release_digest IS NULL OR octet_length(p_release_digest) <> 32
       OR p_configuration_digest IS NULL OR octet_length(p_configuration_digest) <> 32
       OR p_connector_set_digest IS NULL OR octet_length(p_connector_set_digest) <> 32
       OR p_configuration_revision IS NULL
       OR length(p_configuration_revision) NOT BETWEEN 1 AND 300
       OR btrim(p_configuration_revision) <> p_configuration_revision
       OR p_configuration_revision ~ '[[:cntrl:]]'
       OR p_activated_by IS NULL OR length(p_activated_by) NOT BETWEEN 1 AND 300
       OR btrim(p_activated_by) <> p_activated_by OR p_activated_by ~ '[[:cntrl:]]'
       OR p_reason IS NULL OR length(p_reason) NOT BETWEEN 1 AND 2000
       OR btrim(p_reason) <> p_reason OR p_reason ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_cutover_revision_invalid',
            MESSAGE = 'Stage cutover revision fields are invalid';
    END IF;

    SELECT control.* INTO STRICT v_control
    FROM public.stage_cutover_control AS control
    WHERE control.singleton
    FOR UPDATE;
    SELECT revision.* INTO STRICT v_previous
    FROM public.stage_cutover_revisions AS revision
    WHERE revision.id = v_control.current_revision_id
    FOR SHARE;

    IF p_previous_revision_id <> v_previous.id OR p_revision <> v_previous.revision + 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_cutover_revision_stale',
            MESSAGE = 'Stage cutover revision does not extend the current revision';
    END IF;
    IF p_mode = 'LEGACY_ONLY' THEN
        IF p_stage_cohort_basis_points <> 0
           OR p_execution_graph_revision_id IS NOT NULL
           OR p_execution_profile_revision_id IS NOT NULL
           OR p_reserved_storage_bytes <> 0 THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'stage_cutover_legacy_shape_invalid',
                MESSAGE = 'LEGACY_ONLY cutover revision has Stage authority';
        END IF;
    ELSE
        IF (p_mode = 'COHORT' AND p_stage_cohort_basis_points NOT BETWEEN 1 AND 9999)
           OR (p_mode = 'STAGE_ONLY' AND p_stage_cohort_basis_points <> 10000)
           OR p_execution_graph_revision_id IS NULL
           OR p_execution_profile_revision_id IS NULL
           OR p_reserved_storage_bytes <= 0 THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'stage_cutover_stage_shape_invalid',
                MESSAGE = 'Stage cutover revision has invalid graph, profile, cohort, or storage authority';
        END IF;
        SELECT graph.model_revision_id INTO v_model_revision_id
            FROM public.execution_graph_revisions AS graph
            JOIN public.execution_profile_revisions AS profile
              ON profile.execution_graph_revision_id = graph.id
             AND profile.model_revision_id = graph.model_revision_id
            WHERE graph.id = p_execution_graph_revision_id
              AND profile.id = p_execution_profile_revision_id
              AND graph.state = 'ACTIVE'
              AND profile.state = 'ACTIVE';
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_cutover_graph_profile_inactive',
                MESSAGE = 'Stage cutover requires one exact active graph/profile pair';
        END IF;
        SELECT public.vela_execution_profile_connector_set_digest(
            p_execution_profile_revision_id,
            p_execution_graph_revision_id
        ) INTO v_connector_digest;
        IF v_connector_digest IS DISTINCT FROM p_connector_set_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_cutover_connector_set_mismatch',
                MESSAGE = 'Stage cutover connector-set digest does not match the active profile';
        END IF;
    END IF;

    IF p_launch_manifest_digest IS NOT NULL THEN
        IF octet_length(p_launch_manifest_digest) <> 32 THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'stage_cutover_launch_manifest_invalid',
                MESSAGE = 'Stage cutover Launch Receipt manifest digest is invalid';
        END IF;
        SELECT manifest.* INTO v_manifest
        FROM public.production_gate_manifests AS manifest
        WHERE manifest.manifest_digest = p_launch_manifest_digest
          AND manifest.sealed_at IS NOT NULL
          AND manifest.receipt_count = 9
        FOR SHARE;
        IF NOT FOUND OR v_manifest.release_digest <> p_release_digest
           OR v_manifest.configuration_revision <> p_configuration_revision THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_cutover_launch_manifest_mismatch',
                MESSAGE = 'Stage cutover Launch Receipt manifest is absent, unsealed, or mismatched';
        END IF;
    ELSIF p_scope = 'PRODUCTION' AND p_mode <> 'LEGACY_ONLY' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_launch_manifest_required',
            MESSAGE = 'Production Stage cutover requires a sealed Launch Receipt manifest';
    END IF;

    IF p_stage_cohort_basis_points < v_previous.stage_cohort_basis_points THEN
        v_rollback_of := v_previous.id;
    END IF;
    INSERT INTO public.stage_cutover_revisions (
        id, revision, previous_revision_id, rollback_of_revision_id,
        scope, mode, model_revision_id, stage_cohort_basis_points,
        execution_graph_revision_id, execution_profile_revision_id,
        reserved_storage_bytes, minimum_observation_seconds,
        release_digest, configuration_revision, configuration_digest,
        connector_set_digest, launch_manifest_digest, activated_by, reason
    ) VALUES (
        p_id, p_revision, p_previous_revision_id, v_rollback_of,
        p_scope, p_mode, v_model_revision_id, p_stage_cohort_basis_points,
        p_execution_graph_revision_id, p_execution_profile_revision_id,
        p_reserved_storage_bytes, p_minimum_observation_seconds,
        p_release_digest, p_configuration_revision, p_configuration_digest,
        p_connector_set_digest, p_launch_manifest_digest, p_activated_by, p_reason
    ) RETURNING * INTO v_result;

    UPDATE public.stage_cutover_control
    SET current_revision_id = v_result.id,
        updated_at = v_result.activated_at
    WHERE singleton;
    RETURN v_result;
END
$$;

CREATE FUNCTION vela_authorize_stage_cutover_internal_project(
    p_cutover_revision_id uuid,
    p_organization_id uuid,
    p_project_id uuid,
    p_authorized_by text
) RETURNS stage_cutover_internal_projects
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_revision public.stage_cutover_revisions%ROWTYPE;
    v_result public.stage_cutover_internal_projects%ROWTYPE;
BEGIN
    IF p_cutover_revision_id IS NULL OR p_organization_id IS NULL
       OR p_project_id IS NULL OR p_authorized_by IS NULL
       OR length(p_authorized_by) NOT BETWEEN 1 AND 300
       OR btrim(p_authorized_by) <> p_authorized_by
       OR p_authorized_by ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_cutover_internal_project_invalid',
            MESSAGE = 'Internal Stage cutover project authorization is invalid';
    END IF;
    SELECT revision.* INTO STRICT v_revision
    FROM public.stage_cutover_control AS control
    JOIN public.stage_cutover_revisions AS revision
      ON revision.id = control.current_revision_id
    WHERE control.singleton
      AND revision.id = p_cutover_revision_id
    FOR SHARE OF control, revision;
    IF v_revision.scope <> 'INTERNAL' OR v_revision.mode = 'LEGACY_ONLY' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_internal_project_scope_mismatch',
            MESSAGE = 'Only the current INTERNAL Stage revision accepts project authorization';
    END IF;
    INSERT INTO public.stage_cutover_internal_projects (
        cutover_revision_id, organization_id, project_id, authorized_by
    ) VALUES (
        p_cutover_revision_id, p_organization_id, p_project_id, p_authorized_by
    ) ON CONFLICT (cutover_revision_id, organization_id, project_id) DO NOTHING
    RETURNING * INTO v_result;
    IF NOT FOUND THEN
        SELECT binding.* INTO STRICT v_result
        FROM public.stage_cutover_internal_projects AS binding
        WHERE binding.cutover_revision_id = p_cutover_revision_id
          AND binding.organization_id = p_organization_id
          AND binding.project_id = p_project_id;
        IF v_result.authorized_by <> p_authorized_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_cutover_internal_project_replay_mismatch',
                MESSAGE = 'Internal Stage cutover project authorization replay changed';
        END IF;
    END IF;
    RETURN v_result;
END
$$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM public.attempts
        WHERE execution_authority_kind = 'STAGE_GRAPH'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_upgrade_requires_no_stage_attempts',
            MESSAGE = 'Migration 49 requires pre-cutover Stage Attempts to be drained';
    END IF;
END
$$;

-- Migration 49 changes Stage Jobs to carry no legacy WorkerPool. Redefine the
-- v36 runtime entry points here so upgrades from an already-applied v48 schema
-- receive the same counter behavior as fresh databases.
CREATE OR REPLACE FUNCTION vela_apply_stage_command(p_command jsonb)
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
    v_capacity_observation_sequence bigint;
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

        PERFORM vela_private.vela_begin_stage_graph_finalization(
            v_attempt.id, v_run.id, v_completed_at
        );
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
            IF v_job.worker_pool_id IS NOT NULL THEN
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
        PERFORM vela_private.vela_begin_stage_graph_finalization(
            v_attempt.id, v_run.id, v_advanced_at
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
            IF v_job.worker_pool_id IS NOT NULL THEN
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
    v_capacity_observation_sequence :=
        (p_command ->> 'capacity_observation_sequence')::bigint;
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
       OR v_capacity_observation_sequence <= 0
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
       )
       OR NOT public.vela_lock_exact_capacity_observation(
           v_worker_id, v_worker_epoch, v_capacity_observation_sequence,
           p_command -> 'capacity_vector'
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
CREATE OR REPLACE FUNCTION vela_cancel_stage_graph(p_command jsonb)
RETURNS TABLE (
    cancellation_id uuid,
    job_id uuid,
    decision cancellation_decision,
    job_state job_state,
    previous_job_state job_state,
    job_version bigint,
    cancellation_fence bigint,
    attempt_id uuid,
    worker_id uuid,
    worker_epoch bigint,
    attempt_fence bigint,
    authority_lease_id uuid,
    authority_lease_phase lease_phase,
    authority_lease_owner_kind lease_owner_kind,
    authority_lease_owner_id text,
    authority_lease_expires_at timestamptz,
    billable boolean,
    charge_id uuid,
    charge_amount_minor bigint,
    charge_currency text,
    charge_reason charge_reason,
    charge_posted_at timestamptz,
    decided_at timestamptz,
    created boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_project_id uuid := public.vela_current_project_id();
    v_principal_id uuid := public.vela_current_principal_id();
    v_scope text := public.vela_current_request_scope();
    v_job_id uuid := (p_command ->> 'job_id')::uuid;
    v_cancellation_id uuid := (p_command ->> 'cancellation_id')::uuid;
    v_charge_id uuid := (p_command ->> 'charge_id')::uuid;
    v_cancel_requested_event_id uuid :=
        (p_command ->> 'cancel_requested_event_id')::uuid;
    v_canceling_event_id uuid := (p_command ->> 'canceling_event_id')::uuid;
    v_canceled_event_id uuid := (p_command ->> 'canceled_event_id')::uuid;
    v_charge_posted_event_id uuid :=
        (p_command ->> 'charge_posted_event_id')::uuid;
    v_invoice_export_event_id uuid :=
        (p_command ->> 'invoice_export_event_id')::uuid;
    v_job public.jobs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_latest_authority public.execution_authority_kind;
    v_decision public.job_cancellation_decisions%ROWTYPE;
    v_reservation public.credit_reservations%ROWTYPE;
    v_stage_lease_ids uuid[];
    v_billable boolean;
    v_has_physical_authority boolean;
    v_cancellation_decision public.cancellation_decision;
    v_result_job_state public.job_state;
    v_decided_at timestamptz := transaction_timestamp();
    v_final_job_version bigint;
    v_rows bigint;
BEGIN
    IF p_command IS NULL OR jsonb_typeof(p_command) <> 'object'
       OR (p_command ->> 'schema_version')::integer <> 1
       OR v_job_id IS NULL OR v_cancellation_id IS NULL OR v_charge_id IS NULL
       OR v_cancel_requested_event_id IS NULL OR v_canceling_event_id IS NULL
       OR v_canceled_event_id IS NULL OR v_charge_posted_event_id IS NULL
       OR v_invoice_export_event_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_graph_cancellation_command_invalid',
            MESSAGE = 'Stage graph cancellation command is invalid';
    END IF;
    IF v_scope IS DISTINCT FROM 'jobs:cancel'
       OR v_organization_id IS NULL OR v_project_id IS NULL
       OR v_principal_id IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '28000',
            CONSTRAINT = 'stage_graph_cancellation_context_invalid',
            MESSAGE = 'Valid jobs:cancel request context is required';
    END IF;

    SELECT existing.* INTO v_decision
    FROM public.job_cancellation_decisions AS existing
    WHERE existing.job_id = v_job_id
      AND existing.organization_id = v_organization_id
      AND existing.project_id = v_project_id
      AND existing.execution_authority_kind = 'STAGE_GRAPH';
    IF FOUND THEN
        RETURN QUERY
        SELECT
            v_decision.id, v_decision.job_id, v_decision.decision,
            current_job.state, v_decision.previous_job_state,
            current_job.version, v_decision.cancellation_fence,
            NULL::uuid, NULL::uuid, NULL::bigint, NULL::bigint,
            NULL::uuid, NULL::public.lease_phase,
            NULL::public.lease_owner_kind, NULL::text, NULL::timestamptz,
            v_decision.billable, charge.id, charge.amount_minor,
            charge.currency, charge.reason, charge.posted_at,
            v_decision.decided_at, false
        FROM public.jobs AS current_job
        LEFT JOIN public.charges AS charge
          ON charge.cancellation_id = v_decision.id
        WHERE current_job.id = v_decision.job_id;
        RETURN;
    END IF;

    SELECT candidate.* INTO v_job
    FROM public.jobs AS candidate
    WHERE candidate.id = v_job_id
      AND candidate.organization_id = v_organization_id
      AND candidate.project_id = v_project_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job is not visible in this Project' USING ERRCODE = 'P0002';
    END IF;
    SELECT attempt.execution_authority_kind INTO v_latest_authority
    FROM public.attempts AS attempt
    WHERE attempt.job_id = v_job.id
    ORDER BY attempt.attempt_number DESC
    LIMIT 1;
    IF v_latest_authority IS DISTINCT FROM 'STAGE_GRAPH' THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0003',
            CONSTRAINT = 'stage_graph_cancellation_not_applicable',
            MESSAGE = 'Job does not use Stage graph execution authority';
    END IF;
    SELECT existing.* INTO v_decision
    FROM public.job_cancellation_decisions AS existing
    WHERE existing.job_id = v_job.id
      AND existing.organization_id = v_organization_id
      AND existing.project_id = v_project_id
      AND existing.execution_authority_kind = 'STAGE_GRAPH';
    IF FOUND THEN
        RETURN QUERY
        SELECT
            v_decision.id, v_decision.job_id, v_decision.decision,
            v_job.state, v_decision.previous_job_state,
            v_job.version, v_decision.cancellation_fence,
            NULL::uuid, NULL::uuid, NULL::bigint, NULL::bigint,
            NULL::uuid, NULL::public.lease_phase,
            NULL::public.lease_owner_kind, NULL::text, NULL::timestamptz,
            v_decision.billable, charge.id, charge.amount_minor,
            charge.currency, charge.reason, charge.posted_at,
            v_decision.decided_at, false
        FROM (SELECT 1) AS singleton
        LEFT JOIN public.charges AS charge
          ON charge.cancellation_id = v_decision.id;
        RETURN;
    END IF;
    IF v_job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED', 'CANCELING') THEN
        RETURN QUERY SELECT
            NULL::uuid,
            v_job.id,
            CASE
                WHEN v_job.state = 'SUCCEEDED'
                THEN 'ALREADY_SUCCEEDED'::public.cancellation_decision
                WHEN v_job.state = 'FAILED'
                THEN 'ALREADY_FAILED'::public.cancellation_decision
                ELSE v_job.state::text::public.cancellation_decision
            END,
            v_job.state, v_job.state, v_job.version, v_job.current_fence,
            NULL::uuid, NULL::uuid, NULL::bigint, NULL::bigint,
            NULL::uuid, NULL::public.lease_phase,
            NULL::public.lease_owner_kind, NULL::text, NULL::timestamptz,
            false, NULL::uuid, NULL::bigint, NULL::text,
            NULL::public.charge_reason, NULL::timestamptz,
            v_job.updated_at, false;
        RETURN;
    END IF;
    IF v_job.state NOT IN ('QUEUED', 'RETRY_WAIT', 'RUNNING', 'FINALIZING') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_cancellation_state_invalid',
            MESSAGE = 'Stage graph Job is not cancelable';
    END IF;

    SELECT candidate.* INTO v_attempt
    FROM public.attempts AS candidate
    WHERE candidate.job_id = v_job.id
      AND candidate.execution_authority_kind = 'STAGE_GRAPH'
    ORDER BY candidate.attempt_number DESC
    LIMIT 1
    FOR UPDATE;
    IF NOT FOUND OR v_attempt.fence <> v_job.current_fence
       OR v_attempt.graph_state::text <> (CASE v_job.state
            WHEN 'QUEUED' THEN 'QUEUED'
            WHEN 'RETRY_WAIT' THEN 'QUEUED'
            ELSE v_job.state::text
          END) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001',
            CONSTRAINT = 'stage_graph_cancellation_authority_stale',
            MESSAGE = 'Stage graph cancellation authority changed';
    END IF;
    v_billable := v_job.state IN ('RUNNING', 'FINALIZING');
    IF v_billable <> (v_job.billable_started_at IS NOT NULL) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'stage_graph_cancellation_billable_start_invalid',
            MESSAGE = 'Stage graph cancellation Billable Start is inconsistent';
    END IF;

    PERFORM run.id
    FROM public.stage_runs AS run
    WHERE run.attempt_id = v_attempt.id
    ORDER BY run.id
    FOR UPDATE;
    PERFORM physical.id
    FROM public.stage_attempts AS physical
    WHERE physical.attempt_id = v_attempt.id
    ORDER BY physical.id
    FOR UPDATE;
    PERFORM lease.id
    FROM public.stage_leases AS lease
    WHERE lease.attempt_id = v_attempt.id
    ORDER BY lease.id
    FOR UPDATE;
    SELECT COALESCE(array_agg(lease.id ORDER BY lease.id), '{}'::uuid[])
    INTO v_stage_lease_ids
    FROM public.stage_leases AS lease
    WHERE lease.attempt_id = v_attempt.id AND lease.state = 'ACTIVE';
    v_has_physical_authority := cardinality(v_stage_lease_ids) > 0;
    v_cancellation_decision := CASE
        WHEN v_billable AND v_has_physical_authority
        THEN 'CANCELING'::public.cancellation_decision
        ELSE 'CANCELED'::public.cancellation_decision
    END;
    v_result_job_state := CASE
        WHEN v_cancellation_decision = 'CANCELING'
        THEN 'CANCELING'::public.job_state
        ELSE 'CANCELED'::public.job_state
    END;

    PERFORM 1 FROM public.projects
    WHERE id = v_job.project_id AND organization_id = v_job.organization_id
    FOR UPDATE;
    SELECT reservation.* INTO STRICT v_reservation
    FROM public.credit_reservations AS reservation
    WHERE reservation.job_id = v_job.id
    FOR UPDATE;
    PERFORM 1 FROM public.organization_credit_accounts
    WHERE organization_id = v_job.organization_id
      AND currency = v_reservation.currency
    FOR UPDATE;
    IF v_reservation.state <> 'RESERVED' THEN
        RAISE EXCEPTION 'cancelable Job must retain RESERVED credit'
            USING ERRCODE = '23514';
    END IF;

    v_final_job_version := v_job.version + CASE
        WHEN v_billable AND NOT v_has_physical_authority THEN 2
        ELSE 1
    END;
    INSERT INTO public.job_cancellation_decisions (
        id, organization_id, project_id, job_id,
        requested_by_principal_id, previous_job_state, decision, billable,
        cancellation_fence, job_version, decided_at,
        execution_authority_kind, stage_graph_attempt_id,
        stage_graph_attempt_fence, stage_lease_ids,
        stage_cancel_requested_event_id, stage_canceling_event_id,
        stage_canceled_event_id
    ) VALUES (
        v_cancellation_id, v_job.organization_id, v_job.project_id, v_job.id,
        v_principal_id, v_job.state, v_cancellation_decision, v_billable,
        v_job.current_fence + 1, v_final_job_version, v_decided_at,
        'STAGE_GRAPH', v_attempt.id, v_attempt.fence, v_stage_lease_ids,
        v_cancel_requested_event_id, v_canceling_event_id,
        v_canceled_event_id
    );

    UPDATE public.stage_leases AS lease
    SET state = 'REVOKED', revoked_at = v_decided_at,
        revoke_reason = 'CUSTOMER_CANCELLATION'
    WHERE lease.attempt_id = v_attempt.id AND lease.state = 'ACTIVE';
    UPDATE public.stage_allocations AS allocation
    SET state = 'RELEASED', released_at = v_decided_at,
        release_reason = 'CUSTOMER_CANCELLATION'
    WHERE allocation.attempt_id = v_attempt.id
      AND allocation.state = 'ALLOCATED'
      AND NOT EXISTS (
          SELECT 1
          FROM public.stage_leases AS retained_lease
          WHERE retained_lease.stage_allocation_id = allocation.id
            AND retained_lease.id = ANY(v_stage_lease_ids)
      );
    UPDATE public.stage_attempts AS physical
    SET state = 'CANCELED', ended_at = v_decided_at,
        updated_at = v_decided_at
    WHERE physical.attempt_id = v_attempt.id
      AND physical.state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED');
    UPDATE public.stage_runs AS run
    SET state = 'CANCELED', fence = run.fence + 1,
        next_retry_at = NULL, version = run.version + 1,
        updated_at = v_decided_at
    WHERE run.attempt_id = v_attempt.id
      AND run.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED');
    UPDATE public.stage_retry_budgets AS budget
    SET state = 'CANCELED', version = budget.version + 1,
        updated_at = v_decided_at
    FROM public.stage_runs AS run
    WHERE run.id = budget.stage_run_id
      AND run.attempt_id = v_attempt.id
      AND budget.state = 'ACTIVE';
    UPDATE public.attempt_retry_budgets AS budget
    SET state = 'CANCELED', version = budget.version + 1,
        updated_at = v_decided_at
    WHERE budget.attempt_id = v_attempt.id AND budget.state = 'ACTIVE';
    UPDATE public.stage_storage_reservations AS reservation
    SET state = 'RELEASED', updated_at = v_decided_at
    WHERE reservation.attempt_id = v_attempt.id
      AND reservation.state = 'RESERVED';
    UPDATE public.attempts AS attempt
    SET state = 'CANCELED', graph_state = 'CANCELED',
        ended_at = v_decided_at, updated_at = v_decided_at
    WHERE attempt.id = v_attempt.id
      AND attempt.fence = v_attempt.fence
      AND attempt.graph_state = v_attempt.graph_state;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'Stage graph Attempt changed during cancellation'
            USING ERRCODE = '40001';
    END IF;

    IF v_cancellation_decision = 'CANCELING' THEN
        UPDATE public.jobs AS job
        SET state = 'CANCELING', current_fence = job.current_fence + 1,
            version = job.version + 1, updated_at = v_decided_at
        WHERE job.id = v_job.id AND job.version = v_job.version
          AND job.current_fence = v_job.current_fence
          AND job.state = v_job.state;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Stage graph Job changed during cancellation'
                USING ERRCODE = '40001';
        END IF;
    ELSIF v_billable THEN
        UPDATE public.jobs AS job
        SET state = 'CANCELING', current_fence = job.current_fence + 1,
            version = job.version + 1, updated_at = v_decided_at
        WHERE job.id = v_job.id AND job.version = v_job.version
          AND job.current_fence = v_job.current_fence
          AND job.state = v_job.state;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Stage graph Job changed during cancellation'
                USING ERRCODE = '40001';
        END IF;
        UPDATE public.jobs AS job
        SET state = 'CANCELED', version = job.version + 1,
            updated_at = v_decided_at
        WHERE job.id = v_job.id AND job.version = v_job.version + 1
          AND job.current_fence = v_job.current_fence + 1
          AND job.state = 'CANCELING';
    ELSE
        UPDATE public.jobs AS job
        SET state = 'CANCELED', current_fence = job.current_fence + 1,
            version = job.version + 1, updated_at = v_decided_at
        WHERE job.id = v_job.id AND job.version = v_job.version
          AND job.current_fence = v_job.current_fence
          AND job.state = v_job.state;
    END IF;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'Stage graph Job terminalization changed during cancellation'
            USING ERRCODE = '40001';
    END IF;

    IF v_billable THEN
        UPDATE public.projects AS project
        SET running_count = project.running_count - 1
        WHERE project.id = v_job.project_id
          AND project.organization_id = v_job.organization_id
          AND project.running_count > 0;
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Project running counter changed during Stage graph cancellation'
                USING ERRCODE = '23514';
        END IF;
    ELSE
        UPDATE public.projects AS project
        SET queued_count = project.queued_count - 1,
            retry_wait_count = project.retry_wait_count
                - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END
        WHERE project.id = v_job.project_id
          AND project.organization_id = v_job.organization_id
          AND project.queued_count > 0
          AND (v_job.state <> 'RETRY_WAIT' OR project.retry_wait_count > 0);
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        IF v_rows <> 1 THEN
            RAISE EXCEPTION 'Project queue counters changed during Stage graph cancellation'
                USING ERRCODE = '23514';
        END IF;
        IF v_job.worker_pool_id IS NOT NULL THEN
            UPDATE public.worker_pools AS pool
            SET queued_count = pool.queued_count - 1,
                retry_wait_count = pool.retry_wait_count
                    - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END
            WHERE pool.id = v_job.worker_pool_id
              AND pool.queued_count > 0
              AND (v_job.state <> 'RETRY_WAIT' OR pool.retry_wait_count > 0);
            GET DIAGNOSTICS v_rows = ROW_COUNT;
            IF v_rows <> 1 THEN
                RAISE EXCEPTION 'Worker pool queue counters changed during Stage graph cancellation'
                    USING ERRCODE = '23514';
            END IF;
        END IF;
    END IF;

    UPDATE public.credit_reservations AS reservation
    SET state = CASE WHEN v_billable
            THEN 'CONSUMED'::public.credit_reservation_state
            ELSE 'RELEASED'::public.credit_reservation_state END,
        updated_at = v_decided_at
    WHERE reservation.id = v_reservation.id AND reservation.state = 'RESERVED';
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'CreditReservation changed during Stage graph cancellation'
            USING ERRCODE = '23514';
    END IF;
    UPDATE public.organization_credit_accounts AS account
    SET reserved_minor = account.reserved_minor - v_reservation.amount_minor,
        unsettled_posted_minor = account.unsettled_posted_minor
            + CASE WHEN v_billable THEN v_reservation.amount_minor ELSE 0 END,
        version = account.version + 1, updated_at = v_decided_at
    WHERE account.organization_id = v_job.organization_id
      AND account.currency = v_reservation.currency
      AND account.reserved_minor >= v_reservation.amount_minor;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'Organization credit changed during Stage graph cancellation'
            USING ERRCODE = '23514';
    END IF;
    IF v_billable THEN
        INSERT INTO public.charges (
            id, organization_id, project_id, job_id,
            credit_reservation_id, cancellation_id, reason,
            amount_minor, currency, posted_at
        ) VALUES (
            v_charge_id, v_job.organization_id, v_job.project_id, v_job.id,
            v_reservation.id, v_cancellation_id, 'CUSTOMER_CANCELLATION',
            v_reservation.amount_minor, v_reservation.currency, v_decided_at
        );
    END IF;
    PERFORM vela_private.vela_insert_stage_graph_cancellation_outbox_events(
        v_cancellation_id, v_charge_posted_event_id, v_invoice_export_event_id
    );

    RETURN QUERY SELECT
        v_cancellation_id, v_job.id, v_cancellation_decision,
        v_result_job_state, v_job.state, v_final_job_version,
        v_job.current_fence + 1,
        NULL::uuid, NULL::uuid, NULL::bigint, NULL::bigint,
        NULL::uuid, NULL::public.lease_phase,
        NULL::public.lease_owner_kind, NULL::text, NULL::timestamptz,
        v_billable,
        CASE WHEN v_billable THEN v_charge_id ELSE NULL END::uuid,
        CASE WHEN v_billable THEN v_reservation.amount_minor ELSE NULL END::bigint,
        CASE WHEN v_billable THEN v_reservation.currency ELSE NULL END::text,
        CASE WHEN v_billable THEN
            'CUSTOMER_CANCELLATION'::public.charge_reason ELSE NULL END,
        CASE WHEN v_billable THEN v_decided_at ELSE NULL END::timestamptz,
        v_decided_at, true;
END
$$;

ALTER TABLE jobs
    ADD COLUMN execution_authority_kind execution_authority_kind
        NOT NULL DEFAULT 'LEGACY_WORKER',
    ADD COLUMN stage_cutover_revision_id uuid,
    ADD COLUMN execution_graph_revision_id uuid REFERENCES execution_graph_revisions(id),
    ADD COLUMN stage_execution_profile_revision_id uuid,
    ALTER COLUMN worker_pool_id DROP NOT NULL,
    ADD CONSTRAINT jobs_stage_profile_graph
        FOREIGN KEY (stage_execution_profile_revision_id, execution_graph_revision_id)
        REFERENCES execution_profile_revisions(id, execution_graph_revision_id),
    ADD CONSTRAINT jobs_stage_cutover_authority
        FOREIGN KEY (
            stage_cutover_revision_id,
            execution_graph_revision_id,
            stage_execution_profile_revision_id
        ) REFERENCES stage_cutover_revisions(
            id,
            execution_graph_revision_id,
            execution_profile_revision_id
        ),
    ADD CONSTRAINT jobs_execution_authority_shape CHECK (
        (
            execution_authority_kind = 'LEGACY_WORKER'
            AND worker_pool_id IS NOT NULL
            AND stage_cutover_revision_id IS NULL
            AND execution_graph_revision_id IS NULL
            AND stage_execution_profile_revision_id IS NULL
        ) OR (
            execution_authority_kind = 'STAGE_GRAPH'
            AND worker_pool_id IS NULL
            AND stage_cutover_revision_id IS NOT NULL
            AND execution_graph_revision_id IS NOT NULL
            AND stage_execution_profile_revision_id IS NOT NULL
        )
    );

CREATE FUNCTION vela_guard_job_execution_authority() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF TG_OP = 'UPDATE' AND ROW(
        NEW.execution_authority_kind,
        NEW.stage_cutover_revision_id,
        NEW.execution_graph_revision_id,
        NEW.stage_execution_profile_revision_id,
        NEW.worker_pool_id
    ) IS DISTINCT FROM ROW(
        OLD.execution_authority_kind,
        OLD.stage_cutover_revision_id,
        OLD.execution_graph_revision_id,
        OLD.stage_execution_profile_revision_id,
        OLD.worker_pool_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'job_execution_authority_immutable',
            MESSAGE = 'Accepted Job execution authority cannot be converted';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION vela_enforce_attempt_job_authority() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_job_authority public.execution_authority_kind;
BEGIN
    SELECT job.execution_authority_kind INTO v_job_authority
    FROM public.jobs AS job
    WHERE job.id = NEW.job_id;
    IF v_job_authority IS DISTINCT FROM NEW.execution_authority_kind THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempt_job_execution_authority_mismatch',
            MESSAGE = 'Attempt authority must match the immutable Accepted Job authority';
    END IF;
    IF TG_OP = 'UPDATE' AND NEW.execution_authority_kind IS DISTINCT FROM OLD.execution_authority_kind THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempt_execution_authority_immutable',
            MESSAGE = 'Attempt execution authority cannot be converted';
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION vela_enforce_graph_snapshot_job_authority() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_job public.jobs%ROWTYPE;
BEGIN
    SELECT job.* INTO v_job
    FROM public.jobs AS job
    WHERE job.id = NEW.job_id;
    IF NOT FOUND
       OR v_job.execution_authority_kind <> 'STAGE_GRAPH'
       OR v_job.stage_cutover_revision_id IS NULL
       OR v_job.execution_graph_revision_id IS DISTINCT FROM NEW.execution_graph_revision_id
       OR v_job.stage_execution_profile_revision_id
            IS DISTINCT FROM NEW.execution_profile_revision_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'graph_snapshot_job_execution_authority_mismatch',
            MESSAGE = 'ExecutionGraphSnapshot must match the immutable Accepted Job authority';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER jobs_execution_authority_immutable
BEFORE UPDATE OF execution_authority_kind, stage_cutover_revision_id,
    execution_graph_revision_id, stage_execution_profile_revision_id, worker_pool_id
ON jobs FOR EACH ROW EXECUTE FUNCTION vela_guard_job_execution_authority();
CREATE TRIGGER attempts_match_job_execution_authority
BEFORE INSERT OR UPDATE OF execution_authority_kind, job_id ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_enforce_attempt_job_authority();
CREATE TRIGGER execution_graph_snapshots_match_job_authority
BEFORE INSERT OR UPDATE OF job_id, execution_graph_revision_id,
    execution_profile_revision_id ON execution_graph_snapshots
FOR EACH ROW EXECUTE FUNCTION vela_enforce_graph_snapshot_job_authority();

CREATE FUNCTION vela_resolve_job_execution_route(
    p_organization_id uuid,
    p_project_id uuid,
    p_model_revision_id uuid
) RETURNS TABLE (
    execution_authority_kind execution_authority_kind,
    stage_cutover_revision_id uuid,
    execution_graph_revision_id uuid,
    execution_profile_revision_id uuid,
    reserved_storage_bytes bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_revision public.stage_cutover_revisions%ROWTYPE;
    v_bucket integer;
    v_stage boolean;
BEGIN
    IF p_organization_id IS NULL OR p_project_id IS NULL
       OR p_model_revision_id IS NULL
       OR p_organization_id IS DISTINCT FROM public.vela_current_organization_id()
       OR p_project_id IS DISTINCT FROM public.vela_current_project_id() THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'stage_cutover_request_scope_mismatch',
            MESSAGE = 'Stage cutover route must match the authenticated request scope';
    END IF;
    SELECT revision.* INTO STRICT v_revision
    FROM public.stage_cutover_control AS control
    JOIN public.stage_cutover_revisions AS revision
      ON revision.id = control.current_revision_id
    WHERE control.singleton
    FOR SHARE OF control, revision;

    v_bucket := (
        get_byte(sha256(uuid_send(p_organization_id) || uuid_send(p_project_id)), 0) * 256
        + get_byte(sha256(uuid_send(p_organization_id) || uuid_send(p_project_id)), 1)
    ) % 10000;
    v_stage := v_revision.model_revision_id = p_model_revision_id
        AND (
            v_revision.scope = 'PRODUCTION'
            OR EXISTS (
                SELECT 1
                FROM public.stage_cutover_internal_projects AS binding
                WHERE binding.cutover_revision_id = v_revision.id
                  AND binding.organization_id = p_organization_id
                  AND binding.project_id = p_project_id
            )
        )
        AND (
            v_revision.mode = 'STAGE_ONLY'
            OR (v_revision.mode = 'COHORT'
                AND v_bucket < v_revision.stage_cohort_basis_points)
        );
    IF v_stage AND NOT EXISTS (
        SELECT 1
        FROM public.execution_graph_revisions AS graph
        JOIN public.execution_profile_revisions AS profile
          ON profile.execution_graph_revision_id = graph.id
         AND profile.model_revision_id = graph.model_revision_id
        WHERE graph.id = v_revision.execution_graph_revision_id
          AND graph.model_revision_id = p_model_revision_id
          AND profile.id = v_revision.execution_profile_revision_id
          AND graph.state = 'ACTIVE'
          AND profile.state = 'ACTIVE'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_route_authority_inactive',
            MESSAGE = 'Current Stage cutover graph/profile authority is no longer active';
    END IF;
    RETURN QUERY SELECT
        CASE WHEN v_stage THEN 'STAGE_GRAPH'::public.execution_authority_kind
             ELSE 'LEGACY_WORKER'::public.execution_authority_kind END,
        v_revision.id,
        CASE WHEN v_stage THEN v_revision.execution_graph_revision_id ELSE NULL END,
        CASE WHEN v_stage THEN v_revision.execution_profile_revision_id ELSE NULL END,
        CASE WHEN v_stage THEN v_revision.reserved_storage_bytes ELSE 0 END;
END
$$;

CREATE TABLE legacy_authority_inventory_snapshots (
    id uuid PRIMARY KEY,
    cutover_revision_id uuid NOT NULL REFERENCES stage_cutover_revisions(id),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    observed_by text NOT NULL CHECK (
        length(observed_by) BETWEEN 1 AND 300
        AND btrim(observed_by) = observed_by
        AND observed_by !~ '[[:cntrl:]]'
    ),
    nonterminal_jobs bigint NOT NULL CHECK (nonterminal_jobs >= 0),
    nonterminal_attempts bigint NOT NULL CHECK (nonterminal_attempts >= 0),
    active_execution_leases bigint NOT NULL CHECK (active_execution_leases >= 0),
    active_finalization_leases bigint NOT NULL CHECK (active_finalization_leases >= 0),
    active_artifact_uploads bigint NOT NULL CHECK (active_artifact_uploads >= 0),
    unpublished_outbox_events bigint NOT NULL CHECK (unpublished_outbox_events >= 0),
    retained_published_outbox_events bigint NOT NULL CHECK (
        retained_published_outbox_events >= 0
    ),
    scheduler_inbox_backlog bigint NOT NULL CHECK (scheduler_inbox_backlog >= 0),
    retry_recovery_backlog bigint NOT NULL CHECK (retry_recovery_backlog >= 0),
    total_count bigint NOT NULL CHECK (total_count >= 0),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    CHECK (total_count = nonterminal_jobs + nonterminal_attempts
        + active_execution_leases + active_finalization_leases
        + active_artifact_uploads + unpublished_outbox_events
        + retained_published_outbox_events
        + scheduler_inbox_backlog + retry_recovery_backlog)
);

CREATE TRIGGER legacy_authority_inventory_snapshots_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON legacy_authority_inventory_snapshots
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_cutover_history_mutation();

CREATE FUNCTION vela_capture_legacy_authority_inventory(
    p_snapshot_id uuid,
    p_observed_by text
) RETURNS legacy_authority_inventory_snapshots
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_existing public.legacy_authority_inventory_snapshots%ROWTYPE;
    v_result public.legacy_authority_inventory_snapshots%ROWTYPE;
    v_cutover_revision_id uuid;
BEGIN
    IF p_snapshot_id IS NULL OR p_observed_by IS NULL
       OR length(p_observed_by) NOT BETWEEN 1 AND 300
       OR btrim(p_observed_by) <> p_observed_by
       OR p_observed_by ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_authority_inventory_request_invalid',
            MESSAGE = 'Legacy authority inventory request is invalid';
    END IF;
    SELECT snapshot.* INTO v_existing
    FROM public.legacy_authority_inventory_snapshots AS snapshot
    WHERE snapshot.id = p_snapshot_id;
    IF FOUND THEN
        IF v_existing.observed_by <> p_observed_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'legacy_authority_inventory_replay_mismatch',
                MESSAGE = 'Legacy authority inventory replay identity changed';
        END IF;
        RETURN v_existing;
    END IF;

    SELECT current_revision_id INTO STRICT v_cutover_revision_id
    FROM public.stage_cutover_control WHERE singleton;

    SELECT
        count(*) FILTER (WHERE source = 'jobs'),
        count(*) FILTER (WHERE source = 'attempts'),
        count(*) FILTER (WHERE source = 'execution_leases'),
        count(*) FILTER (WHERE source = 'finalization_leases'),
        count(*) FILTER (WHERE source = 'uploads'),
        count(*) FILTER (WHERE source = 'unpublished_outbox'),
        count(*) FILTER (WHERE source = 'retained_published_outbox'),
        count(*) FILTER (WHERE source = 'inbox'),
        count(*) FILTER (WHERE source = 'retry')
    INTO
        v_result.nonterminal_jobs,
        v_result.nonterminal_attempts,
        v_result.active_execution_leases,
        v_result.active_finalization_leases,
        v_result.active_artifact_uploads,
        v_result.unpublished_outbox_events,
        v_result.retained_published_outbox_events,
        v_result.scheduler_inbox_backlog,
        v_result.retry_recovery_backlog
    FROM (
        SELECT 'jobs' AS source FROM public.jobs AS job
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND job.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')
        UNION ALL
        SELECT 'attempts' FROM public.attempts AS attempt
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
          AND attempt.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')
        UNION ALL
        SELECT 'execution_leases'
        FROM public.attempt_leases AS lease
        JOIN public.attempts AS attempt ON attempt.id = lease.attempt_id
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
          AND lease.phase = 'EXECUTION'
          AND lease.revoked_at IS NULL
          AND lease.expires_at > clock_timestamp()
        UNION ALL
        SELECT 'finalization_leases'
        FROM public.attempt_leases AS lease
        JOIN public.attempts AS attempt ON attempt.id = lease.attempt_id
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
          AND lease.phase = 'FINALIZATION'
          AND lease.revoked_at IS NULL
          AND lease.expires_at > clock_timestamp()
        UNION ALL
        SELECT 'uploads'
        FROM public.artifact_uploads AS upload
        JOIN public.attempts AS attempt ON attempt.id = upload.attempt_id
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
          AND upload.state IN ('INITIATED', 'UPLOADING', 'UPLOADED')
        UNION ALL
        SELECT 'unpublished_outbox'
        FROM public.outbox_events AS event
        JOIN public.jobs AS job ON job.id = event.aggregate_id
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND event.published_at IS NULL
        UNION ALL
        SELECT 'retained_published_outbox'
        FROM public.outbox_events AS event
        JOIN public.jobs AS job ON job.id = event.aggregate_id
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND event.published_at IS NOT NULL
        UNION ALL
        SELECT 'inbox'
        FROM public.outbox_events AS event
        JOIN public.jobs AS job ON job.id = event.aggregate_id
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND event.event_type = 'job.ready'
          AND event.published_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM public.inbox_receipts AS receipt
              WHERE receipt.consumer_name = 'scheduler'
                AND receipt.event_id = event.event_id
          )
        UNION ALL
        SELECT 'retry' FROM public.jobs AS job
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND job.state = 'RETRY_WAIT'
    ) AS inventory;

    v_result.id := p_snapshot_id;
    v_result.cutover_revision_id := v_cutover_revision_id;
    v_result.observed_at := clock_timestamp();
    v_result.observed_by := p_observed_by;
    v_result.total_count := v_result.nonterminal_jobs + v_result.nonterminal_attempts
        + v_result.active_execution_leases + v_result.active_finalization_leases
        + v_result.active_artifact_uploads + v_result.unpublished_outbox_events
        + v_result.retained_published_outbox_events
        + v_result.scheduler_inbox_backlog + v_result.retry_recovery_backlog;
    v_result.content_digest := sha256(convert_to(jsonb_build_object(
        'schema_version', 1,
        'id', v_result.id,
        'cutover_revision_id', v_result.cutover_revision_id,
        'observed_at', v_result.observed_at,
        'observed_by', v_result.observed_by,
        'nonterminal_jobs', v_result.nonterminal_jobs,
        'nonterminal_attempts', v_result.nonterminal_attempts,
        'active_execution_leases', v_result.active_execution_leases,
        'active_finalization_leases', v_result.active_finalization_leases,
        'active_artifact_uploads', v_result.active_artifact_uploads,
        'unpublished_outbox_events', v_result.unpublished_outbox_events,
        'retained_published_outbox_events', v_result.retained_published_outbox_events,
        'scheduler_inbox_backlog', v_result.scheduler_inbox_backlog,
        'retry_recovery_backlog', v_result.retry_recovery_backlog,
        'total_count', v_result.total_count
    )::text, 'UTF8'));
    INSERT INTO public.legacy_authority_inventory_snapshots VALUES (v_result.*)
    RETURNING * INTO v_result;
    RETURN v_result;
END
$$;


ALTER TABLE stage_cutover_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE stage_cutover_control OWNER TO vela_catalog_promotion_owner;
ALTER TABLE stage_cutover_internal_projects OWNER TO vela_catalog_promotion_owner;
ALTER TABLE legacy_authority_inventory_snapshots OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_reject_stage_cutover_history_mutation()
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_guard_stage_cutover_control_writer()
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_execution_profile_connector_set_digest(uuid, uuid)
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_activate_stage_cutover(
    uuid, bigint, uuid, stage_cutover_scope, stage_cutover_mode, integer,
    uuid, uuid, bigint, integer, bytea, text, bytea, bytea, bytea, text, text
) OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_authorize_stage_cutover_internal_project(uuid, uuid, uuid, text)
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_resolve_job_execution_route(uuid, uuid, uuid)
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_guard_job_execution_authority()
    OWNER TO vela_internal;
ALTER FUNCTION vela_enforce_attempt_job_authority()
    OWNER TO vela_internal;
ALTER FUNCTION vela_enforce_graph_snapshot_job_authority()
    OWNER TO vela_internal;
ALTER FUNCTION vela_capture_legacy_authority_inventory(uuid, text)
    OWNER TO vela_catalog_promotion_owner;

REVOKE ALL ON stage_cutover_revisions, stage_cutover_control,
    stage_cutover_internal_projects, legacy_authority_inventory_snapshots
FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_execution_profile_connector_set_digest(uuid, uuid),
    vela_activate_stage_cutover(
        uuid, bigint, uuid, stage_cutover_scope, stage_cutover_mode, integer,
        uuid, uuid, bigint, integer, bytea, text, bytea, bytea, bytea, text, text
    ),
    vela_authorize_stage_cutover_internal_project(uuid, uuid, uuid, text),
    vela_resolve_job_execution_route(uuid, uuid, uuid),
    vela_guard_job_execution_authority(),
    vela_enforce_attempt_job_authority(),
    vela_enforce_graph_snapshot_job_authority(),
    vela_capture_legacy_authority_inventory(uuid, text)
FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_activate_stage_cutover(
    uuid, bigint, uuid, stage_cutover_scope, stage_cutover_mode, integer,
    uuid, uuid, bigint, integer, bytea, text, bytea, bytea, bytea, text, text
) TO vela_catalog_promotion;
GRANT EXECUTE ON FUNCTION vela_execution_profile_connector_set_digest(uuid, uuid),
    vela_authorize_stage_cutover_internal_project(uuid, uuid, uuid, text)
TO vela_catalog_promotion;
GRANT EXECUTE ON FUNCTION vela_capture_legacy_authority_inventory(uuid, text)
TO vela_catalog_promotion, vela_catalog_promotion_owner;
GRANT EXECUTE ON FUNCTION vela_current_organization_id(),
    vela_current_project_id()
TO vela_catalog_promotion_owner;
GRANT SELECT ON jobs, attempts, attempt_leases, artifact_uploads,
    outbox_events, inbox_receipts
TO vela_catalog_promotion_owner;
GRANT SELECT ON stage_cutover_revisions, stage_cutover_control
TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_resolve_job_execution_route(uuid, uuid, uuid)
TO vela_request;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM legacy_authority_inventory_snapshots)
       OR EXISTS (
           SELECT 1 FROM stage_cutover_revisions
           WHERE id <> '00000000-0000-0000-0000-000000000049'
       )
       OR EXISTS (SELECT 1 FROM jobs WHERE execution_authority_kind = 'STAGE_GRAPH') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_control_rollback_is_unsafe',
            MESSAGE = 'Stage cutover evidence and Stage-routed Jobs must be empty before rollback';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_resolve_job_execution_route(uuid, uuid, uuid)
    FROM vela_request;
REVOKE EXECUTE ON FUNCTION vela_execution_profile_connector_set_digest(uuid, uuid),
    vela_authorize_stage_cutover_internal_project(uuid, uuid, uuid, text)
FROM vela_catalog_promotion;
REVOKE EXECUTE ON FUNCTION vela_capture_legacy_authority_inventory(uuid, text)
FROM vela_catalog_promotion, vela_catalog_promotion_owner;
REVOKE EXECUTE ON FUNCTION vela_current_organization_id(),
    vela_current_project_id()
FROM vela_catalog_promotion_owner;
REVOKE SELECT ON jobs, attempts, attempt_leases, artifact_uploads,
    outbox_events, inbox_receipts
FROM vela_catalog_promotion_owner;
REVOKE SELECT ON stage_cutover_revisions, stage_cutover_control
FROM vela_internal;
REVOKE EXECUTE ON FUNCTION vela_activate_stage_cutover(
    uuid, bigint, uuid, stage_cutover_scope, stage_cutover_mode, integer,
    uuid, uuid, bigint, integer, bytea, text, bytea, bytea, bytea, text, text
) FROM vela_catalog_promotion;

DROP TRIGGER execution_graph_snapshots_match_job_authority
    ON execution_graph_snapshots;
DROP TRIGGER attempts_match_job_execution_authority ON attempts;
DROP TRIGGER jobs_execution_authority_immutable ON jobs;
DROP FUNCTION vela_enforce_graph_snapshot_job_authority();
DROP FUNCTION vela_enforce_attempt_job_authority();
DROP FUNCTION vela_guard_job_execution_authority();
ALTER TABLE jobs
    DROP CONSTRAINT jobs_execution_authority_shape,
    DROP CONSTRAINT jobs_stage_cutover_authority,
    DROP CONSTRAINT jobs_stage_profile_graph,
    DROP COLUMN stage_execution_profile_revision_id,
    DROP COLUMN execution_graph_revision_id,
    DROP COLUMN stage_cutover_revision_id,
    DROP COLUMN execution_authority_kind,
    ALTER COLUMN worker_pool_id SET NOT NULL;

DROP FUNCTION vela_capture_legacy_authority_inventory(uuid, text);
DROP FUNCTION vela_resolve_job_execution_route(uuid, uuid, uuid);
DROP FUNCTION vela_authorize_stage_cutover_internal_project(uuid, uuid, uuid, text);
DROP FUNCTION vela_activate_stage_cutover(
    uuid, bigint, uuid, stage_cutover_scope, stage_cutover_mode, integer,
    uuid, uuid, bigint, integer, bytea, text, bytea, bytea, bytea, text, text
);
DROP FUNCTION vela_execution_profile_connector_set_digest(uuid, uuid);
DROP TRIGGER legacy_authority_inventory_snapshots_immutable
    ON legacy_authority_inventory_snapshots;
DROP TABLE legacy_authority_inventory_snapshots;
DROP TRIGGER stage_cutover_internal_projects_immutable
    ON stage_cutover_internal_projects;
DROP TABLE stage_cutover_internal_projects;
DROP TRIGGER stage_cutover_control_writer ON stage_cutover_control;
DROP TRIGGER stage_cutover_revisions_immutable ON stage_cutover_revisions;
DROP FUNCTION vela_guard_stage_cutover_control_writer();
DROP FUNCTION vela_reject_stage_cutover_history_mutation();
DROP TABLE stage_cutover_control;
DROP TABLE stage_cutover_revisions;
DROP TYPE stage_cutover_mode;
DROP TYPE stage_cutover_scope;
-- +goose StatementEnd
