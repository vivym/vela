-- +goose Up
-- +goose StatementBegin
CREATE TYPE stage_cache_scope AS ENUM ('PROJECT', 'ORGANIZATION');
CREATE TYPE stage_cache_entry_state AS ENUM (
    'LIVE', 'EVICTING', 'EVICTED', 'DELETED', 'EXPIRED'
);
CREATE TYPE stage_cache_reference_state AS ENUM ('ACTIVE', 'RELEASED');

CREATE TABLE project_stage_cache_controls (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    cache_policy_revision_id uuid NOT NULL REFERENCES stage_cache_policy_revisions(id),
    enabled boolean NOT NULL DEFAULT true,
    max_entries integer NOT NULL CHECK (max_entries > 0),
    max_bytes bigint NOT NULL CHECK (max_bytes > 0),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (project_id, cache_policy_revision_id),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id)
);

CREATE TABLE organization_stage_cache_authorizations (
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    cache_policy_revision_id uuid NOT NULL REFERENCES stage_cache_policy_revisions(id),
    enabled boolean NOT NULL DEFAULT false,
    max_entries integer NOT NULL CHECK (max_entries > 0),
    max_bytes bigint NOT NULL CHECK (max_bytes > 0),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (organization_id, cache_policy_revision_id)
);

CREATE TABLE stage_cache_entries (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    scope stage_cache_scope NOT NULL,
    scope_project_id uuid,
    source_project_id uuid NOT NULL,
    source_job_id uuid NOT NULL,
    source_stage_run_id uuid NOT NULL,
    cache_policy_revision_id uuid NOT NULL REFERENCES stage_cache_policy_revisions(id),
    stage_profile_revision_id uuid NOT NULL REFERENCES stage_profile_revisions(id),
    result_equivalence_revision_id uuid NOT NULL
        REFERENCES stage_result_equivalence_revisions(id),
    stage_key text NOT NULL CHECK (
        length(stage_key) BETWEEN 1 AND 100
        AND stage_key ~ '^[a-z][a-z0-9_-]*$'
    ),
    cache_key_digest bytea NOT NULL CHECK (octet_length(cache_key_digest) = 32),
    stage_artifact_id uuid NOT NULL REFERENCES stage_artifacts(id),
    exact_object_version text NOT NULL CHECK (
        length(exact_object_version) BETWEEN 1 AND 1000
    ),
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    expected_saved_compute_minor bigint NOT NULL DEFAULT 0
        CHECK (expected_saved_compute_minor >= 0),
    carry_cost_minor bigint NOT NULL DEFAULT 0 CHECK (carry_cost_minor >= 0),
    hit_count bigint NOT NULL DEFAULT 0 CHECK (hit_count >= 0),
    last_accessed_at timestamptz NOT NULL,
    admitted_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    state stage_cache_entry_state NOT NULL DEFAULT 'LIVE',
    deletion_requested_at timestamptz,
    terminal_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, source_project_id, id),
    FOREIGN KEY (organization_id, source_project_id)
        REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, source_project_id, source_job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (organization_id, source_project_id, source_stage_run_id)
        REFERENCES stage_runs(organization_id, project_id, id),
    CHECK (
        (scope = 'PROJECT' AND scope_project_id = source_project_id)
        OR (scope = 'ORGANIZATION' AND scope_project_id IS NULL)
    ),
    CHECK (expires_at > admitted_at),
    CHECK (
        (state = 'LIVE' AND deletion_requested_at IS NULL AND terminal_at IS NULL)
        OR (state = 'EVICTING' AND deletion_requested_at IS NOT NULL AND terminal_at IS NULL)
        OR (state IN ('EVICTED', 'DELETED', 'EXPIRED') AND terminal_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX stage_cache_entries_one_reusable_key_idx
    ON stage_cache_entries (
        organization_id, scope, scope_project_id, cache_policy_revision_id,
        result_equivalence_revision_id, cache_key_digest
    ) NULLS NOT DISTINCT
    WHERE state IN ('LIVE', 'EVICTING');
CREATE INDEX stage_cache_entries_source_job_idx
    ON stage_cache_entries(source_job_id, state);
CREATE INDEX stage_cache_entries_shadow_eviction_idx
    ON stage_cache_entries (
        organization_id, scope, scope_project_id, state, expires_at,
        last_accessed_at, id
    );

CREATE TABLE stage_cache_references (
    id uuid PRIMARY KEY,
    stage_cache_entry_id uuid NOT NULL REFERENCES stage_cache_entries(id),
    stage_artifact_id uuid NOT NULL REFERENCES stage_artifacts(id),
    execution_pin_id uuid UNIQUE REFERENCES stage_artifact_pins(id),
    owner_job_id uuid NOT NULL REFERENCES jobs(id),
    owner_stage_run_id uuid REFERENCES stage_runs(id),
    state stage_cache_reference_state NOT NULL DEFAULT 'ACTIVE',
    acquired_at timestamptz NOT NULL,
    released_at timestamptz,
    release_reason text CHECK (
        release_reason IS NULL OR length(release_reason) BETWEEN 1 AND 500
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE NULLS NOT DISTINCT (
        stage_cache_entry_id, owner_job_id, owner_stage_run_id
    ),
    CHECK ((state = 'RELEASED') = (released_at IS NOT NULL)),
    CHECK ((released_at IS NULL) = (release_reason IS NULL))
);

CREATE TABLE stage_cache_commands (
    command_id uuid PRIMARY KEY,
    command_kind text NOT NULL CHECK (
        command_kind IN ('ADMIT', 'HIT', 'DELETE', 'RELEASE_PIN')
    ),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    result jsonb NOT NULL CHECK (jsonb_typeof(result) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE FUNCTION vela_validate_stage_cache_entry_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.id <> OLD.id
       OR NEW.organization_id <> OLD.organization_id
       OR NEW.scope <> OLD.scope
       OR NEW.scope_project_id IS DISTINCT FROM OLD.scope_project_id
       OR NEW.source_project_id <> OLD.source_project_id
       OR NEW.source_job_id <> OLD.source_job_id
       OR NEW.source_stage_run_id <> OLD.source_stage_run_id
       OR NEW.cache_policy_revision_id <> OLD.cache_policy_revision_id
       OR NEW.stage_profile_revision_id <> OLD.stage_profile_revision_id
       OR NEW.result_equivalence_revision_id <> OLD.result_equivalence_revision_id
       OR NEW.stage_key <> OLD.stage_key
       OR NEW.cache_key_digest <> OLD.cache_key_digest
       OR NEW.stage_artifact_id <> OLD.stage_artifact_id
       OR NEW.exact_object_version <> OLD.exact_object_version
       OR NEW.sha256 <> OLD.sha256
       OR NEW.size_bytes <> OLD.size_bytes
       OR NEW.expected_saved_compute_minor <> OLD.expected_saved_compute_minor
       OR NEW.carry_cost_minor <> OLD.carry_cost_minor
       OR NEW.admitted_at <> OLD.admitted_at
       OR NEW.expires_at <> OLD.expires_at
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_cache_entry_identity_immutable',
            MESSAGE = 'Stage Cache entry exact identity is immutable';
    END IF;
    IF NEW.hit_count < OLD.hit_count OR NEW.last_accessed_at < OLD.last_accessed_at
       OR (OLD.deletion_requested_at IS NOT NULL
           AND NEW.deletion_requested_at IS DISTINCT FROM OLD.deletion_requested_at)
       OR (OLD.terminal_at IS NOT NULL AND NEW.terminal_at IS DISTINCT FROM OLD.terminal_at)
       OR (OLD.state <> NEW.state AND NOT (
           (OLD.state = 'LIVE' AND NEW.state IN ('EVICTING', 'EVICTED', 'DELETED', 'EXPIRED'))
           OR (OLD.state = 'EVICTING' AND NEW.state IN ('EVICTED', 'DELETED', 'EXPIRED'))
       )) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_cache_entry_state_transition',
            MESSAGE = 'Stage Cache entry lifecycle may only move forward';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_validate_stage_cache_entry_mutation()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_validate_stage_cache_entry_mutation() FROM PUBLIC;
CREATE TRIGGER stage_cache_entries_mutation_valid
BEFORE UPDATE ON stage_cache_entries
FOR EACH ROW EXECUTE FUNCTION vela_validate_stage_cache_entry_mutation();

CREATE FUNCTION vela_validate_stage_cache_reference_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.id <> OLD.id
       OR NEW.stage_cache_entry_id <> OLD.stage_cache_entry_id
       OR NEW.stage_artifact_id <> OLD.stage_artifact_id
       OR NEW.execution_pin_id IS DISTINCT FROM OLD.execution_pin_id
       OR NEW.owner_job_id <> OLD.owner_job_id
       OR NEW.owner_stage_run_id IS DISTINCT FROM OLD.owner_stage_run_id
       OR NEW.acquired_at <> OLD.acquired_at
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_cache_reference_identity_immutable',
            MESSAGE = 'Stage Cache reference identity is immutable';
    END IF;
    IF (OLD.state <> NEW.state AND NOT (
           OLD.state = 'ACTIVE' AND NEW.state = 'RELEASED'
       )) OR (OLD.state = 'RELEASED' AND to_jsonb(NEW) IS DISTINCT FROM to_jsonb(OLD)) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_cache_reference_state_transition',
            MESSAGE = 'Stage Cache reference lifecycle may only move forward';
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_validate_stage_cache_reference_mutation()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_validate_stage_cache_reference_mutation() FROM PUBLIC;
CREATE TRIGGER stage_cache_references_mutation_valid
BEFORE UPDATE ON stage_cache_references
FOR EACH ROW EXECUTE FUNCTION vela_validate_stage_cache_reference_mutation();

CREATE FUNCTION vela_reject_stage_cache_command_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'stage_cache_command_immutable',
        MESSAGE = 'Stage cache command evidence is immutable';
END
$$;
ALTER FUNCTION vela_reject_stage_cache_command_mutation()
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_reject_stage_cache_command_mutation() FROM PUBLIC;
CREATE TRIGGER stage_cache_commands_immutable
BEFORE UPDATE OR DELETE ON stage_cache_commands
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_cache_command_mutation();

CREATE FUNCTION vela_set_project_stage_cache_control(p_command jsonb)
RETURNS TABLE (version bigint, enabled boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := (p_command ->> 'organization_id')::uuid;
    v_project_id uuid := (p_command ->> 'project_id')::uuid;
    v_policy_id uuid := (p_command ->> 'cache_policy_revision_id')::uuid;
    v_enabled boolean := (p_command ->> 'enabled')::boolean;
    v_max_entries integer := (p_command ->> 'max_entries')::integer;
    v_max_bytes bigint := (p_command ->> 'max_bytes')::bigint;
    v_updated_at timestamptz := (p_command ->> 'updated_at')::timestamptz;
BEGIN
    IF v_organization_id IS NULL OR v_project_id IS NULL OR v_policy_id IS NULL
       OR v_enabled IS NULL OR v_max_entries <= 0 OR v_max_bytes <= 0
       OR v_updated_at IS NULL OR NOT EXISTS (
           SELECT 1 FROM projects AS project
           WHERE project.organization_id = v_organization_id
             AND project.id = v_project_id
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_cache_control_invalid',
            MESSAGE = 'Project Stage cache control is invalid';
    END IF;
    INSERT INTO project_stage_cache_controls (
        organization_id, project_id, cache_policy_revision_id,
        enabled, max_entries, max_bytes, updated_at
    ) VALUES (
        v_organization_id, v_project_id, v_policy_id,
        v_enabled, v_max_entries, v_max_bytes, v_updated_at
    )
    ON CONFLICT (project_id, cache_policy_revision_id) DO UPDATE
    SET enabled = EXCLUDED.enabled,
        max_entries = EXCLUDED.max_entries,
        max_bytes = EXCLUDED.max_bytes,
        version = project_stage_cache_controls.version + 1,
        updated_at = EXCLUDED.updated_at
    WHERE project_stage_cache_controls.organization_id = EXCLUDED.organization_id
      AND EXCLUDED.updated_at >= project_stage_cache_controls.updated_at;
    RETURN QUERY SELECT control.version, control.enabled
    FROM project_stage_cache_controls AS control
    WHERE control.organization_id = v_organization_id
      AND control.project_id = v_project_id
      AND control.cache_policy_revision_id = v_policy_id;
END
$$;
ALTER FUNCTION vela_set_project_stage_cache_control(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_set_project_stage_cache_control(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_set_project_stage_cache_control(jsonb)
    TO vela_attempt_coordinator;

CREATE FUNCTION vela_set_organization_stage_cache_authorization(p_command jsonb)
RETURNS TABLE (version bigint, enabled boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := (p_command ->> 'organization_id')::uuid;
    v_policy_id uuid := (p_command ->> 'cache_policy_revision_id')::uuid;
    v_enabled boolean := (p_command ->> 'enabled')::boolean;
    v_max_entries integer := (p_command ->> 'max_entries')::integer;
    v_max_bytes bigint := (p_command ->> 'max_bytes')::bigint;
    v_updated_at timestamptz := (p_command ->> 'updated_at')::timestamptz;
BEGIN
    IF v_organization_id IS NULL OR v_policy_id IS NULL OR v_enabled IS NULL
       OR v_max_entries <= 0 OR v_max_bytes <= 0 OR v_updated_at IS NULL
       OR NOT EXISTS (
           SELECT 1 FROM customer_organizations WHERE id = v_organization_id
       ) OR NOT EXISTS (
           SELECT 1 FROM stage_cache_policy_revisions AS policy
           WHERE policy.id = v_policy_id
             AND policy.scope_ceiling = 'ORGANIZATION'
             AND policy.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_cache_organization_authorization_invalid',
            MESSAGE = 'Organization Stage cache authorization is invalid';
    END IF;
    INSERT INTO organization_stage_cache_authorizations (
        organization_id, cache_policy_revision_id, enabled,
        max_entries, max_bytes, updated_at
    ) VALUES (
        v_organization_id, v_policy_id, v_enabled,
        v_max_entries, v_max_bytes, v_updated_at
    )
    ON CONFLICT (organization_id, cache_policy_revision_id) DO UPDATE
    SET enabled = EXCLUDED.enabled,
        max_entries = EXCLUDED.max_entries,
        max_bytes = EXCLUDED.max_bytes,
        version = organization_stage_cache_authorizations.version + 1,
        updated_at = EXCLUDED.updated_at
    WHERE EXCLUDED.updated_at >= organization_stage_cache_authorizations.updated_at;
    RETURN QUERY SELECT auth.version, auth.enabled
    FROM organization_stage_cache_authorizations AS auth
    WHERE auth.organization_id = v_organization_id
      AND auth.cache_policy_revision_id = v_policy_id;
END
$$;
ALTER FUNCTION vela_set_organization_stage_cache_authorization(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_set_organization_stage_cache_authorization(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_set_organization_stage_cache_authorization(jsonb)
    TO vela_attempt_coordinator;

CREATE FUNCTION vela_admit_stage_cache_entry(p_command jsonb)
RETURNS TABLE (
    entry_id uuid, artifact_id uuid, object_version text,
    expires_at timestamptz, replayed boolean, deduplicated boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_entry_id uuid := (p_command ->> 'entry_id')::uuid;
    v_artifact_id uuid := (p_command ->> 'artifact_id')::uuid;
    v_policy_id uuid := (p_command ->> 'cache_policy_revision_id')::uuid;
    v_profile_id uuid := (p_command ->> 'stage_profile_revision_id')::uuid;
    v_equivalence_id uuid := (p_command ->> 'result_equivalence_revision_id')::uuid;
    v_scope stage_cache_scope := (p_command ->> 'scope')::stage_cache_scope;
    v_stage_key text := p_command ->> 'stage_key';
    v_key_digest bytea := decode(p_command ->> 'cache_key_digest', 'hex');
    v_saved_minor bigint := (p_command ->> 'expected_saved_compute_minor')::bigint;
    v_carry_minor bigint := (p_command ->> 'carry_cost_minor')::bigint;
    v_admitted_at timestamptz := (p_command ->> 'admitted_at')::timestamptz;
    v_expires_at timestamptz := (p_command ->> 'expires_at')::timestamptz;
    v_request_digest bytea;
    v_existing_command stage_cache_commands%ROWTYPE;
    v_existing_entry stage_cache_entries%ROWTYPE;
    v_artifact stage_artifacts%ROWTYPE;
    v_run stage_runs%ROWTYPE;
    v_physical stage_attempts%ROWTYPE;
    v_job jobs%ROWTYPE;
    v_policy stage_cache_policy_revisions%ROWTYPE;
    v_profile stage_profile_revisions%ROWTYPE;
    v_scope_project_id uuid;
    v_max_entries integer;
    v_max_bytes bigint;
    v_live_entries bigint;
    v_live_bytes bigint;
    v_result jsonb;
BEGIN
    IF v_command_id IS NULL OR v_entry_id IS NULL OR v_artifact_id IS NULL
       OR v_policy_id IS NULL OR v_profile_id IS NULL OR v_equivalence_id IS NULL
       OR v_stage_key IS NULL OR length(v_stage_key) NOT BETWEEN 1 AND 100
       OR octet_length(v_key_digest) <> 32 OR v_saved_minor < 0 OR v_carry_minor < 0
       OR v_admitted_at IS NULL OR v_expires_at <= v_admitted_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_cache_admit_invalid',
            MESSAGE = 'Stage cache admission command is invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing_command
    FROM stage_cache_commands AS command WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing_command.command_kind <> 'ADMIT'
           OR v_existing_command.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_cache_command_replay_mismatch',
                MESSAGE = 'Stage cache command replay does not match';
        END IF;
        RETURN QUERY SELECT
            (v_existing_command.result ->> 'entry_id')::uuid,
            (v_existing_command.result ->> 'artifact_id')::uuid,
            v_existing_command.result ->> 'object_version',
            (v_existing_command.result ->> 'expires_at')::timestamptz,
            true,
            (v_existing_command.result ->> 'deduplicated')::boolean;
        RETURN;
    END IF;
    SELECT artifact.* INTO STRICT v_artifact
    FROM stage_artifacts AS artifact WHERE artifact.id = v_artifact_id FOR SHARE;
    SELECT run.* INTO STRICT v_run
    FROM stage_runs AS run WHERE run.id = v_artifact.producer_stage_run_id FOR SHARE;
    SELECT physical.* INTO STRICT v_physical
    FROM stage_attempts AS physical WHERE physical.id = v_artifact.producer_stage_attempt_id
    FOR SHARE;
    SELECT job.* INTO STRICT v_job
    FROM jobs AS job WHERE job.id = v_artifact.job_id FOR SHARE;
    SELECT policy.* INTO STRICT v_policy
    FROM stage_cache_policy_revisions AS policy WHERE policy.id = v_policy_id FOR SHARE;
    SELECT profile.* INTO STRICT v_profile
    FROM stage_profile_revisions AS profile WHERE profile.id = v_profile_id FOR SHARE;
    IF v_artifact.state <> 'COMMITTED' OR v_artifact.expires_at <= v_expires_at
       OR v_job.request_content_deleted_at IS NOT NULL
       OR v_job.state IN ('CANCELING', 'CANCELED', 'FAILED')
       OR v_run.stage_key <> v_stage_key
       OR v_physical.selected_stage_profile_revision_id <> v_profile_id
       OR v_profile.result_equivalence_revision_id <> v_equivalence_id
       OR v_policy.state NOT IN ('CERTIFIED', 'CANARY', 'ACTIVE')
       OR NOT (v_stage_key = ANY(v_policy.allowed_stage_keys))
       OR v_expires_at > v_admitted_at + make_interval(secs => v_policy.ttl_seconds)
       OR (v_scope = 'ORGANIZATION' AND v_policy.scope_ceiling <> 'ORGANIZATION') THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_cache_admit_authority_stale',
            MESSAGE = 'Stage cache Artifact, profile, policy, or deletion authority is stale';
    END IF;
    v_scope_project_id := CASE WHEN v_scope = 'PROJECT' THEN v_artifact.project_id END;
    IF v_scope = 'PROJECT' THEN
        SELECT control.max_entries, control.max_bytes
        INTO v_max_entries, v_max_bytes
        FROM project_stage_cache_controls AS control
        WHERE control.organization_id = v_artifact.organization_id
          AND control.project_id = v_artifact.project_id
          AND control.cache_policy_revision_id = v_policy_id
          AND control.enabled
        FOR UPDATE;
    ELSE
        SELECT auth.max_entries, auth.max_bytes
        INTO v_max_entries, v_max_bytes
        FROM organization_stage_cache_authorizations AS auth
        WHERE auth.organization_id = v_artifact.organization_id
          AND auth.cache_policy_revision_id = v_policy_id
          AND auth.enabled
        FOR UPDATE;
    END IF;
    IF v_max_entries IS NULL OR v_max_bytes IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501', CONSTRAINT = 'stage_cache_policy_disabled',
            MESSAGE = 'Stage cache policy is disabled or Organization scope is unauthorized';
    END IF;
    SELECT entry.* INTO v_existing_entry
    FROM stage_cache_entries AS entry
    WHERE entry.organization_id = v_artifact.organization_id
      AND entry.scope = v_scope
      AND entry.scope_project_id IS NOT DISTINCT FROM v_scope_project_id
      AND entry.cache_policy_revision_id = v_policy_id
      AND entry.result_equivalence_revision_id = v_equivalence_id
      AND entry.cache_key_digest = v_key_digest
      AND entry.state IN ('LIVE', 'EVICTING')
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing_entry.state <> 'LIVE'
           OR v_existing_entry.sha256 <> v_artifact.sha256 THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_cache_key_collision',
                MESSAGE = 'Stage cache key maps to a non-reusable or different exact result';
        END IF;
        v_result := jsonb_build_object(
            'entry_id', v_existing_entry.id,
            'artifact_id', v_existing_entry.stage_artifact_id,
            'object_version', v_existing_entry.exact_object_version,
            'expires_at', v_existing_entry.expires_at,
            'deduplicated', true
        );
        INSERT INTO stage_cache_commands (
            command_id, command_kind, request_digest, result
        ) VALUES (v_command_id, 'ADMIT', v_request_digest, v_result);
        RETURN QUERY SELECT v_existing_entry.id, v_existing_entry.stage_artifact_id,
            v_existing_entry.exact_object_version, v_existing_entry.expires_at,
            false, true;
        RETURN;
    END IF;
    SELECT count(*), coalesce(sum(entry.size_bytes), 0)
    INTO v_live_entries, v_live_bytes
    FROM stage_cache_entries AS entry
    WHERE entry.organization_id = v_artifact.organization_id
      AND entry.scope = v_scope
      AND entry.scope_project_id IS NOT DISTINCT FROM v_scope_project_id
      AND entry.cache_policy_revision_id = v_policy_id
      AND entry.state = 'LIVE';
    IF v_live_entries + 1 > v_max_entries
       OR v_live_bytes + v_artifact.size_bytes > v_max_bytes THEN
        RAISE EXCEPTION USING
            ERRCODE = '54000', CONSTRAINT = 'stage_cache_quota_exceeded',
            MESSAGE = 'Stage cache admission exceeds configured quota';
    END IF;
    INSERT INTO stage_cache_entries (
        id, organization_id, scope, scope_project_id,
        source_project_id, source_job_id, source_stage_run_id,
        cache_policy_revision_id, stage_profile_revision_id,
        result_equivalence_revision_id, stage_key, cache_key_digest,
        stage_artifact_id, exact_object_version, sha256, size_bytes,
        expected_saved_compute_minor, carry_cost_minor,
        last_accessed_at, admitted_at, expires_at
    ) VALUES (
        v_entry_id, v_artifact.organization_id, v_scope, v_scope_project_id,
        v_artifact.project_id, v_artifact.job_id, v_artifact.producer_stage_run_id,
        v_policy_id, v_profile_id, v_equivalence_id, v_stage_key, v_key_digest,
        v_artifact.id, v_artifact.object_version, v_artifact.sha256, v_artifact.size_bytes,
        v_saved_minor, v_carry_minor, v_admitted_at, v_admitted_at, v_expires_at
    );
    INSERT INTO stage_cache_references (
        id, stage_cache_entry_id, stage_artifact_id, owner_job_id,
        owner_stage_run_id, acquired_at
    ) VALUES (
        gen_random_uuid(), v_entry_id, v_artifact.id, v_artifact.job_id,
        v_artifact.producer_stage_run_id, v_admitted_at
    );
    v_result := jsonb_build_object(
        'entry_id', v_entry_id, 'artifact_id', v_artifact.id,
        'object_version', v_artifact.object_version,
        'expires_at', v_expires_at, 'deduplicated', false
    );
    INSERT INTO stage_cache_commands (
        command_id, command_kind, request_digest, result
    ) VALUES (v_command_id, 'ADMIT', v_request_digest, v_result);
    RETURN QUERY SELECT v_entry_id, v_artifact.id, v_artifact.object_version,
        v_expires_at, false, false;
END
$$;
ALTER FUNCTION vela_admit_stage_cache_entry(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_admit_stage_cache_entry(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_admit_stage_cache_entry(jsonb)
    TO vela_attempt_coordinator;

CREATE FUNCTION vela_hit_stage_cache(p_command jsonb)
RETURNS TABLE (
    entry_id uuid, artifact_id uuid, pin_id uuid, object_version text,
    sha256 bytea, size_bytes bigint, stage_run_id uuid, stage_state text,
    stage_fence bigint, stage_version bigint, replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_entry_id uuid := (p_command ->> 'entry_id')::uuid;
    v_pin_id uuid := (p_command ->> 'pin_id')::uuid;
    v_attempt_id uuid := (p_command ->> 'attempt_id')::uuid;
    v_stage_run_id uuid := (p_command ->> 'stage_run_id')::uuid;
    v_profile_id uuid := (p_command ->> 'stage_profile_revision_id')::uuid;
    v_expected_organization_id uuid := (p_command ->> 'expected_organization_id')::uuid;
    v_expected_project_id uuid := (p_command ->> 'expected_project_id')::uuid;
    v_expected_attempt_fence bigint := (p_command ->> 'expected_attempt_fence')::bigint;
    v_expected_stage_fence bigint := (p_command ->> 'expected_stage_fence')::bigint;
    v_expected_stage_version bigint := (p_command ->> 'expected_stage_version')::bigint;
    v_progress_receipt_id uuid := (p_command ->> 'progress_receipt_id')::uuid;
    v_key_digest bytea := decode(p_command ->> 'cache_key_digest', 'hex');
    v_hit_at timestamptz := (p_command ->> 'hit_at')::timestamptz;
    v_request_digest bytea;
    v_existing_command stage_cache_commands%ROWTYPE;
    v_entry stage_cache_entries%ROWTYPE;
    v_artifact stage_artifacts%ROWTYPE;
    v_source_job jobs%ROWTYPE;
    v_target_job jobs%ROWTYPE;
    v_attempt attempts%ROWTYPE;
    v_run stage_runs%ROWTYPE;
    v_policy stage_cache_policy_revisions%ROWTYPE;
    v_profile stage_profile_revisions%ROWTYPE;
    v_decision_stage_run_id uuid;
    v_decision_stage_attempt_id uuid;
    v_decision_state text;
    v_decision_fence bigint;
    v_decision_version bigint;
    v_decision_replayed boolean;
    v_result jsonb;
BEGIN
    IF v_command_id IS NULL OR v_entry_id IS NULL OR v_pin_id IS NULL
       OR v_attempt_id IS NULL OR v_stage_run_id IS NULL OR v_profile_id IS NULL
       OR v_expected_organization_id IS NULL OR v_expected_project_id IS NULL
       OR v_expected_attempt_fence <= 0 OR v_expected_stage_fence <= 0
       OR v_expected_stage_version <= 0 OR v_progress_receipt_id IS NULL
       OR octet_length(v_key_digest) <> 32 OR v_hit_at IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_cache_hit_invalid',
            MESSAGE = 'Stage cache hit command is invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing_command
    FROM stage_cache_commands AS command WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing_command.command_kind <> 'HIT'
           OR v_existing_command.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'stage_cache_command_replay_mismatch',
                MESSAGE = 'Stage cache command replay does not match';
        END IF;
        RETURN QUERY SELECT
            (v_existing_command.result ->> 'entry_id')::uuid,
            (v_existing_command.result ->> 'artifact_id')::uuid,
            (v_existing_command.result ->> 'pin_id')::uuid,
            v_existing_command.result ->> 'object_version',
            decode(v_existing_command.result ->> 'sha256', 'hex'),
            (v_existing_command.result ->> 'size_bytes')::bigint,
            (v_existing_command.result ->> 'stage_run_id')::uuid,
            v_existing_command.result ->> 'stage_state',
            (v_existing_command.result ->> 'stage_fence')::bigint,
            (v_existing_command.result ->> 'stage_version')::bigint,
            true;
        RETURN;
    END IF;
    SELECT attempt.* INTO STRICT v_attempt
    FROM attempts AS attempt WHERE attempt.id = v_attempt_id FOR UPDATE;
    SELECT job.* INTO STRICT v_target_job
    FROM jobs AS job WHERE job.id = v_attempt.job_id FOR UPDATE;
    SELECT run.* INTO STRICT v_run
    FROM stage_runs AS run
    WHERE run.id = v_stage_run_id AND run.attempt_id = v_attempt_id FOR UPDATE;
    SELECT entry.* INTO STRICT v_entry
    FROM stage_cache_entries AS entry WHERE entry.id = v_entry_id FOR UPDATE;
    SELECT artifact.* INTO STRICT v_artifact
    FROM stage_artifacts AS artifact WHERE artifact.id = v_entry.stage_artifact_id FOR SHARE;
    SELECT job.* INTO STRICT v_source_job
    FROM jobs AS job WHERE job.id = v_entry.source_job_id FOR SHARE;
    SELECT policy.* INTO STRICT v_policy
    FROM stage_cache_policy_revisions AS policy
    WHERE policy.id = v_entry.cache_policy_revision_id FOR SHARE;
    SELECT profile.* INTO STRICT v_profile
    FROM stage_profile_revisions AS profile WHERE profile.id = v_profile_id FOR SHARE;
    IF v_target_job.organization_id <> v_expected_organization_id
       OR v_target_job.project_id <> v_expected_project_id
       OR v_target_job.organization_id <> v_entry.organization_id
       OR (v_entry.scope = 'PROJECT' AND v_target_job.project_id <> v_entry.scope_project_id)
       OR v_entry.cache_key_digest <> v_key_digest
       OR v_entry.state <> 'LIVE' OR v_entry.expires_at <= v_hit_at
       OR v_artifact.state <> 'COMMITTED'
       OR v_artifact.object_version <> v_entry.exact_object_version
       OR v_artifact.sha256 <> v_entry.sha256
       OR v_artifact.expires_at <= v_hit_at
       OR v_source_job.request_content_deleted_at IS NOT NULL
       OR v_policy.state NOT IN ('CERTIFIED', 'CANARY', 'ACTIVE')
       OR v_profile.result_equivalence_revision_id <> v_entry.result_equivalence_revision_id
       OR NOT (v_profile_id = ANY(v_run.allowed_stage_profile_revision_ids))
       OR v_run.stage_key <> v_entry.stage_key THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_cache_entry_unavailable',
            MESSAGE = 'Stage cache entry, scope, exact version, policy, or graph authority is unavailable';
    END IF;
    IF v_entry.scope = 'PROJECT' THEN
        PERFORM 1 FROM project_stage_cache_controls AS control
        WHERE control.organization_id = v_target_job.organization_id
          AND control.project_id = v_target_job.project_id
          AND control.cache_policy_revision_id = v_entry.cache_policy_revision_id
          AND control.enabled
        FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '42501', CONSTRAINT = 'stage_cache_policy_disabled',
                MESSAGE = 'Project Stage cache policy is disabled';
        END IF;
    ELSE
        PERFORM 1 FROM organization_stage_cache_authorizations AS auth
        WHERE auth.organization_id = v_target_job.organization_id
          AND auth.cache_policy_revision_id = v_entry.cache_policy_revision_id
          AND auth.enabled
        FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '42501', CONSTRAINT = 'stage_cache_organization_scope_denied',
                MESSAGE = 'Organization Stage cache scope is not authorized';
        END IF;
    END IF;
    INSERT INTO stage_artifact_pins (
        id, stage_artifact_id, exact_object_version, pin_kind,
        owner_job_id, owner_stage_run_id, acquired_at
    ) VALUES (
        v_pin_id, v_artifact.id, v_artifact.object_version, 'EXECUTION',
        v_target_job.id, v_run.id, v_hit_at
    );
    SELECT decision.stage_run_id, decision.stage_attempt_id,
           decision.stage_state, decision.stage_fence,
           decision.stage_version, decision.replayed
    INTO v_decision_stage_run_id, v_decision_stage_attempt_id,
         v_decision_state, v_decision_fence,
         v_decision_version, v_decision_replayed
    FROM vela_apply_stage_command(jsonb_build_object(
        'schema_version', 1,
        'command_kind', 'EXACT_CACHE_ADVANCE',
        'command_id', v_command_id,
        'attempt_id', v_attempt_id,
        'stage_run_id', v_stage_run_id,
        'expected_attempt_fence', v_expected_attempt_fence,
        'expected_stage_fence', v_expected_stage_fence,
        'expected_stage_version', v_expected_stage_version,
        'progress_receipt_id', v_progress_receipt_id,
        'cache_source_identity', 'stage-cache-entry/' || v_entry.id::text,
        'output_digest', encode(v_entry.sha256, 'hex'),
        'advanced_at', v_hit_at
    )) AS decision;
    IF v_decision_stage_run_id <> v_stage_run_id
       OR v_decision_state <> 'SUCCEEDED' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_cache_graph_advance_inconsistent',
            MESSAGE = 'Stage cache graph advancement returned inconsistent authority';
    END IF;
    UPDATE stage_cache_entries
    SET hit_count = hit_count + 1, last_accessed_at = v_hit_at
    WHERE id = v_entry.id AND state = 'LIVE';
    INSERT INTO stage_cache_references (
        id, stage_cache_entry_id, stage_artifact_id, execution_pin_id,
        owner_job_id, owner_stage_run_id, acquired_at
    ) VALUES (
        gen_random_uuid(), v_entry.id, v_artifact.id, v_pin_id,
        v_target_job.id, v_run.id, v_hit_at
    );
    v_result := jsonb_build_object(
        'entry_id', v_entry.id, 'artifact_id', v_artifact.id,
        'pin_id', v_pin_id, 'object_version', v_artifact.object_version,
        'sha256', encode(v_artifact.sha256, 'hex'), 'size_bytes', v_artifact.size_bytes,
        'stage_run_id', v_decision_stage_run_id, 'stage_state', v_decision_state,
        'stage_fence', v_decision_fence, 'stage_version', v_decision_version
    );
    INSERT INTO stage_cache_commands (
        command_id, command_kind, request_digest, result
    ) VALUES (v_command_id, 'HIT', v_request_digest, v_result);
    RETURN QUERY SELECT v_entry.id, v_artifact.id, v_pin_id,
        v_artifact.object_version, v_artifact.sha256, v_artifact.size_bytes,
        v_decision_stage_run_id, v_decision_state,
        v_decision_fence, v_decision_version, false;
END
$$;
ALTER FUNCTION vela_hit_stage_cache(jsonb) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_hit_stage_cache(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_hit_stage_cache(jsonb) TO vela_attempt_coordinator;

CREATE FUNCTION vela_release_stage_cache_execution_pin(p_command jsonb)
RETURNS TABLE (pin_id uuid, released_at timestamptz, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_pin_id uuid := (p_command ->> 'pin_id')::uuid;
    v_owner_job_id uuid := (p_command ->> 'owner_job_id')::uuid;
    v_owner_stage_run_id uuid := (p_command ->> 'owner_stage_run_id')::uuid;
    v_reason text := p_command ->> 'release_reason';
    v_released_at timestamptz := (p_command ->> 'released_at')::timestamptz;
    v_request_digest bytea;
    v_existing stage_cache_commands%ROWTYPE;
    v_result jsonb;
BEGIN
    IF v_command_id IS NULL OR v_pin_id IS NULL OR v_owner_job_id IS NULL
       OR v_owner_stage_run_id IS NULL OR v_reason IS NULL
       OR length(v_reason) NOT BETWEEN 1 AND 500 OR v_released_at IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '22023',
            CONSTRAINT = 'stage_cache_pin_release_invalid',
            MESSAGE = 'Stage cache pin release command is invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing FROM stage_cache_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'RELEASE_PIN'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING ERRCODE = '55000',
                CONSTRAINT = 'stage_cache_command_replay_mismatch',
                MESSAGE = 'Stage cache command replay does not match';
        END IF;
        RETURN QUERY SELECT (v_existing.result ->> 'pin_id')::uuid,
            (v_existing.result ->> 'released_at')::timestamptz, true;
        RETURN;
    END IF;
    UPDATE stage_artifact_pins AS pin
    SET state = 'RELEASED', released_at = v_released_at, release_reason = v_reason
    FROM stage_cache_references AS reference
    WHERE reference.execution_pin_id = pin.id
      AND reference.state = 'ACTIVE'
      AND pin.id = v_pin_id AND pin.owner_job_id = v_owner_job_id
      AND pin.owner_stage_run_id = v_owner_stage_run_id
      AND pin.pin_kind = 'EXECUTION' AND pin.state = 'ACTIVE';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '40001',
            CONSTRAINT = 'stage_cache_pin_release_stale',
            MESSAGE = 'Stage cache ExecutionPin is stale';
    END IF;
    UPDATE stage_cache_references
    SET state = 'RELEASED', released_at = v_released_at, release_reason = v_reason
    WHERE execution_pin_id = v_pin_id AND owner_job_id = v_owner_job_id
      AND owner_stage_run_id = v_owner_stage_run_id AND state = 'ACTIVE';
    v_result := jsonb_build_object('pin_id', v_pin_id, 'released_at', v_released_at);
    INSERT INTO stage_cache_commands (
        command_id, command_kind, request_digest, result
    ) VALUES (v_command_id, 'RELEASE_PIN', v_request_digest, v_result);
    RETURN QUERY SELECT v_pin_id, v_released_at, false;
END
$$;
ALTER FUNCTION vela_release_stage_cache_execution_pin(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_release_stage_cache_execution_pin(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_release_stage_cache_execution_pin(jsonb)
    TO vela_attempt_coordinator;

CREATE FUNCTION vela_request_stage_cache_deletion(p_command jsonb)
RETURNS TABLE (deleted_count bigint, blocked_count bigint, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_command_id uuid := (p_command ->> 'command_id')::uuid;
    v_organization_id uuid := (p_command ->> 'organization_id')::uuid;
    v_project_id uuid := (p_command ->> 'project_id')::uuid;
    v_source_job_id uuid := (p_command ->> 'source_job_id')::uuid;
    v_requested_at timestamptz := (p_command ->> 'requested_at')::timestamptz;
    v_request_digest bytea;
    v_existing stage_cache_commands%ROWTYPE;
    v_deleted bigint;
    v_blocked bigint;
    v_result jsonb;
BEGIN
    IF v_command_id IS NULL OR v_organization_id IS NULL OR v_project_id IS NULL
       OR v_source_job_id IS NULL OR v_requested_at IS NULL OR NOT EXISTS (
           SELECT 1 FROM jobs AS job
           WHERE job.id = v_source_job_id
             AND job.organization_id = v_organization_id
             AND job.project_id = v_project_id
       ) THEN
        RAISE EXCEPTION USING ERRCODE = '22023',
            CONSTRAINT = 'stage_cache_deletion_invalid',
            MESSAGE = 'Stage cache deletion command is invalid';
    END IF;
    v_request_digest := sha256(convert_to(p_command::text, 'UTF8'));
    SELECT command.* INTO v_existing FROM stage_cache_commands AS command
    WHERE command.command_id = v_command_id;
    IF FOUND THEN
        IF v_existing.command_kind <> 'DELETE'
           OR v_existing.request_digest <> v_request_digest THEN
            RAISE EXCEPTION USING ERRCODE = '55000',
                CONSTRAINT = 'stage_cache_command_replay_mismatch',
                MESSAGE = 'Stage cache command replay does not match';
        END IF;
        RETURN QUERY SELECT
            (v_existing.result ->> 'deleted_count')::bigint,
            (v_existing.result ->> 'blocked_count')::bigint, true;
        RETURN;
    END IF;
    WITH candidates AS (
        SELECT entry.id, EXISTS (
            SELECT 1 FROM stage_artifact_pins AS pin
            WHERE pin.stage_artifact_id = entry.stage_artifact_id
              AND pin.exact_object_version = entry.exact_object_version
              AND pin.state = 'ACTIVE'
        ) AS pinned
        FROM stage_cache_entries AS entry
        WHERE entry.organization_id = v_organization_id
          AND entry.source_project_id = v_project_id
          AND entry.source_job_id = v_source_job_id
          AND entry.state = 'LIVE'
        FOR UPDATE
    ), updated AS (
        UPDATE stage_cache_entries AS entry
        SET state = CASE
                WHEN candidate.pinned THEN 'EVICTING'::stage_cache_entry_state
                ELSE 'DELETED'::stage_cache_entry_state
            END,
            deletion_requested_at = v_requested_at,
            terminal_at = CASE WHEN candidate.pinned THEN NULL ELSE v_requested_at END
        FROM candidates AS candidate
        WHERE entry.id = candidate.id
        RETURNING candidate.pinned
    )
    SELECT count(*) FILTER (WHERE NOT pinned), count(*) FILTER (WHERE pinned)
    INTO v_deleted, v_blocked FROM updated;
    UPDATE stage_cache_references AS reference
    SET state = 'RELEASED', released_at = v_requested_at,
        release_reason = 'CONTENT_DELETION'
    FROM stage_cache_entries AS entry
    WHERE reference.stage_cache_entry_id = entry.id
      AND entry.source_job_id = v_source_job_id
      AND reference.execution_pin_id IS NULL
      AND reference.state = 'ACTIVE';
    v_result := jsonb_build_object(
        'deleted_count', coalesce(v_deleted, 0),
        'blocked_count', coalesce(v_blocked, 0)
    );
    INSERT INTO stage_cache_commands (
        command_id, command_kind, request_digest, result
    ) VALUES (v_command_id, 'DELETE', v_request_digest, v_result);
    RETURN QUERY SELECT coalesce(v_deleted, 0), coalesce(v_blocked, 0), false;
END
$$;
ALTER FUNCTION vela_request_stage_cache_deletion(jsonb)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_request_stage_cache_deletion(jsonb) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_request_stage_cache_deletion(jsonb)
    TO vela_attempt_coordinator;

CREATE FUNCTION vela_reconcile_stage_cache_deletions(
    p_observed_at timestamptz,
    p_limit integer
)
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_changed bigint;
BEGIN
    IF p_observed_at IS NULL OR p_limit NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023',
            CONSTRAINT = 'stage_cache_reconcile_invalid',
            MESSAGE = 'Stage cache reconciliation request is invalid';
    END IF;
    WITH candidates AS (
        SELECT entry.id
        FROM stage_cache_entries AS entry
        WHERE entry.state = 'EVICTING'
          AND NOT EXISTS (
              SELECT 1 FROM stage_artifact_pins AS pin
              WHERE pin.stage_artifact_id = entry.stage_artifact_id
                AND pin.exact_object_version = entry.exact_object_version
                AND pin.state = 'ACTIVE'
          )
        ORDER BY entry.deletion_requested_at, entry.id
        LIMIT p_limit
        FOR UPDATE SKIP LOCKED
    )
    UPDATE stage_cache_entries AS entry
    SET state = 'DELETED', terminal_at = p_observed_at
    FROM candidates AS candidate
    WHERE entry.id = candidate.id;
    GET DIAGNOSTICS v_changed = ROW_COUNT;
    RETURN v_changed;
END
$$;
ALTER FUNCTION vela_reconcile_stage_cache_deletions(timestamptz, integer)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_reconcile_stage_cache_deletions(timestamptz, integer)
    FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_reconcile_stage_cache_deletions(timestamptz, integer)
    TO vela_attempt_coordinator;

GRANT SELECT, INSERT, UPDATE, DELETE ON
    project_stage_cache_controls,
    organization_stage_cache_authorizations,
    stage_cache_entries,
    stage_cache_references,
    stage_cache_commands
TO vela_attempt_coordinator_owner;
GRANT SELECT, UPDATE ON stage_cache_policy_revisions, stage_profile_revisions
TO vela_attempt_coordinator_owner;
GRANT SELECT ON customer_organizations TO vela_attempt_coordinator_owner;

ALTER TABLE project_stage_cache_controls ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_stage_cache_controls FORCE ROW LEVEL SECURITY;
ALTER TABLE organization_stage_cache_authorizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE organization_stage_cache_authorizations FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_cache_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_cache_entries FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_cache_references ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_cache_references FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_cache_commands ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_cache_commands FORCE ROW LEVEL SECURITY;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE
    project_stage_cache_controls,
    organization_stage_cache_authorizations,
    stage_cache_entries,
    stage_cache_references,
    stage_cache_commands
IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_cache_entries)
       OR EXISTS (SELECT 1 FROM stage_cache_commands)
       OR EXISTS (SELECT 1 FROM project_stage_cache_controls)
       OR EXISTS (SELECT 1 FROM organization_stage_cache_authorizations) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cache_rollback_is_unsafe',
            MESSAGE = 'cannot remove durable Stage cache authority or evidence';
    END IF;
END
$$;
REVOKE EXECUTE ON FUNCTION vela_reconcile_stage_cache_deletions(timestamptz, integer)
    FROM vela_attempt_coordinator;
DROP FUNCTION vela_reconcile_stage_cache_deletions(timestamptz, integer);
REVOKE EXECUTE ON FUNCTION vela_request_stage_cache_deletion(jsonb)
    FROM vela_attempt_coordinator;
DROP FUNCTION vela_request_stage_cache_deletion(jsonb);
REVOKE EXECUTE ON FUNCTION vela_release_stage_cache_execution_pin(jsonb)
    FROM vela_attempt_coordinator;
DROP FUNCTION vela_release_stage_cache_execution_pin(jsonb);
REVOKE EXECUTE ON FUNCTION vela_hit_stage_cache(jsonb) FROM vela_attempt_coordinator;
DROP FUNCTION vela_hit_stage_cache(jsonb);
REVOKE EXECUTE ON FUNCTION vela_admit_stage_cache_entry(jsonb)
    FROM vela_attempt_coordinator;
DROP FUNCTION vela_admit_stage_cache_entry(jsonb);
REVOKE EXECUTE ON FUNCTION vela_set_organization_stage_cache_authorization(jsonb)
    FROM vela_attempt_coordinator;
DROP FUNCTION vela_set_organization_stage_cache_authorization(jsonb);
REVOKE EXECUTE ON FUNCTION vela_set_project_stage_cache_control(jsonb)
    FROM vela_attempt_coordinator;
DROP FUNCTION vela_set_project_stage_cache_control(jsonb);
DROP TRIGGER stage_cache_commands_immutable ON stage_cache_commands;
DROP FUNCTION vela_reject_stage_cache_command_mutation();
DROP TRIGGER stage_cache_references_mutation_valid ON stage_cache_references;
DROP FUNCTION vela_validate_stage_cache_reference_mutation();
DROP TRIGGER stage_cache_entries_mutation_valid ON stage_cache_entries;
DROP FUNCTION vela_validate_stage_cache_entry_mutation();
DROP TABLE stage_cache_commands;
DROP TABLE stage_cache_references;
DROP TABLE stage_cache_entries;
DROP TABLE organization_stage_cache_authorizations;
DROP TABLE project_stage_cache_controls;
DROP TYPE stage_cache_reference_state;
DROP TYPE stage_cache_entry_state;
DROP TYPE stage_cache_scope;
-- +goose StatementEnd
