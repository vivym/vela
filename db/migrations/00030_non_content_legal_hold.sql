-- +goose Up
-- +goose StatementBegin
CREATE TYPE legal_hold_event_kind AS ENUM ('HOLD_PLACED', 'HOLD_RELEASED');
CREATE TYPE legal_hold_scope AS ENUM ('ORGANIZATION', 'PROJECT', 'JOB');
CREATE TYPE legal_hold_record_class AS ENUM ('METADATA', 'FINANCIAL');
CREATE TYPE legal_hold_state AS ENUM ('ACTIVE', 'RELEASED');

CREATE TABLE compliance_principals (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL UNIQUE
        CHECK (length(stable_id) BETWEEN 1 AND 200 AND btrim(stable_id) = stable_id),
    tls_uri_identity text NOT NULL UNIQUE
        CHECK (
            length(tls_uri_identity) BETWEEN 1 AND 500
            AND btrim(tls_uri_identity) = tls_uri_identity
            AND tls_uri_identity !~ '[[:cntrl:][:space:]]'
        ),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DISABLED')),
    disabled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (status = 'ACTIVE' AND disabled_at IS NULL)
        OR (status = 'DISABLED' AND disabled_at IS NOT NULL)
    )
);

CREATE TABLE compliance_database_bindings (
    database_role name PRIMARY KEY,
    principal_id uuid NOT NULL REFERENCES compliance_principals(id),
    disabled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE compliance_event_cursors (
    principal_id uuid PRIMARY KEY REFERENCES compliance_principals(id),
    last_sequence bigint NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE legal_holds (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    project_id uuid,
    job_id uuid,
    scope legal_hold_scope NOT NULL,
    record_classes legal_hold_record_class[] NOT NULL,
    state legal_hold_state NOT NULL DEFAULT 'ACTIVE',
    placement_principal_id uuid NOT NULL REFERENCES compliance_principals(id),
    placement_source_sequence bigint NOT NULL CHECK (placement_source_sequence > 0),
    placement_reason_code text NOT NULL CHECK (
        placement_reason_code ~ '^[A-Z][A-Z0-9_]{0,99}$'
    ),
    placement_external_reference text NOT NULL CHECK (
        length(placement_external_reference) BETWEEN 1 AND 500
        AND btrim(placement_external_reference) = placement_external_reference
        AND placement_external_reference !~ '[[:cntrl:]]'
    ),
    placement_effective_at timestamptz NOT NULL,
    placed_at timestamptz NOT NULL,
    release_principal_id uuid REFERENCES compliance_principals(id),
    release_source_sequence bigint CHECK (release_source_sequence > 0),
    release_reason_code text CHECK (
        release_reason_code IS NULL OR release_reason_code ~ '^[A-Z][A-Z0-9_]{0,99}$'
    ),
    release_external_reference text CHECK (
        release_external_reference IS NULL
        OR (
            length(release_external_reference) BETWEEN 1 AND 500
            AND btrim(release_external_reference) = release_external_reference
            AND release_external_reference !~ '[[:cntrl:]]'
        )
    ),
    release_effective_at timestamptz,
    released_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, job_id, id),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    CHECK (
        (scope = 'ORGANIZATION' AND project_id IS NULL AND job_id IS NULL)
        OR (scope = 'PROJECT' AND project_id IS NOT NULL AND job_id IS NULL)
        OR (scope = 'JOB' AND project_id IS NOT NULL AND job_id IS NOT NULL)
    ),
    CHECK (
        record_classes = ARRAY['METADATA']::legal_hold_record_class[]
        OR record_classes = ARRAY['FINANCIAL']::legal_hold_record_class[]
        OR record_classes = ARRAY['METADATA', 'FINANCIAL']::legal_hold_record_class[]
    ),
    CHECK (
        (
            state = 'ACTIVE'
            AND release_principal_id IS NULL
            AND release_source_sequence IS NULL
            AND release_reason_code IS NULL
            AND release_external_reference IS NULL
            AND release_effective_at IS NULL
            AND released_at IS NULL
        ) OR (
            state = 'RELEASED'
            AND release_principal_id IS NOT NULL
            AND release_source_sequence IS NOT NULL
            AND release_reason_code IS NOT NULL
            AND release_external_reference IS NOT NULL
            AND release_effective_at IS NOT NULL
            AND released_at IS NOT NULL
            AND release_effective_at >= placement_effective_at
            AND released_at >= placed_at
        )
    )
);

CREATE INDEX legal_holds_active_target_idx
    ON legal_holds (organization_id, project_id, job_id, id)
    WHERE state = 'ACTIVE';

CREATE TABLE legal_hold_events (
    id uuid PRIMARY KEY,
    principal_id uuid NOT NULL REFERENCES compliance_principals(id),
    idempotency_key text NOT NULL CHECK (
        length(idempotency_key) BETWEEN 1 AND 500
        AND btrim(idempotency_key) = idempotency_key
        AND idempotency_key !~ '[[:cntrl:]]'
    ),
    source_sequence bigint NOT NULL CHECK (source_sequence > 0),
    hold_id uuid NOT NULL REFERENCES legal_holds(id),
    kind legal_hold_event_kind NOT NULL,
    scope legal_hold_scope,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    record_classes legal_hold_record_class[],
    reason_code text NOT NULL CHECK (reason_code ~ '^[A-Z][A-Z0-9_]{0,99}$'),
    external_reference text NOT NULL CHECK (
        length(external_reference) BETWEEN 1 AND 500
        AND btrim(external_reference) = external_reference
        AND external_reference !~ '[[:cntrl:]]'
    ),
    effective_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    UNIQUE (principal_id, idempotency_key),
    UNIQUE (principal_id, source_sequence),
    UNIQUE (principal_id, external_reference),
    CHECK (
        (
            kind = 'HOLD_PLACED'
            AND scope IS NOT NULL
            AND organization_id IS NOT NULL
            AND record_classes IS NOT NULL
        ) OR (
            kind = 'HOLD_RELEASED'
            AND scope IS NULL
            AND organization_id IS NULL
            AND project_id IS NULL
            AND job_id IS NULL
            AND record_classes IS NULL
        )
    )
);

ALTER TABLE compliance_principals ENABLE ROW LEVEL SECURITY;
ALTER TABLE compliance_principals FORCE ROW LEVEL SECURITY;
ALTER TABLE compliance_database_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE compliance_database_bindings FORCE ROW LEVEL SECURITY;
ALTER TABLE compliance_event_cursors ENABLE ROW LEVEL SECURITY;
ALTER TABLE compliance_event_cursors FORCE ROW LEVEL SECURITY;
ALTER TABLE legal_holds ENABLE ROW LEVEL SECURITY;
ALTER TABLE legal_holds FORCE ROW LEVEL SECURITY;
ALTER TABLE legal_hold_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE legal_hold_events FORCE ROW LEVEL SECURITY;

ALTER TABLE compliance_principals OWNER TO vela_compliance_owner;
ALTER TABLE compliance_database_bindings OWNER TO vela_compliance_owner;
ALTER TABLE compliance_event_cursors OWNER TO vela_compliance_owner;
ALTER TABLE legal_holds OWNER TO vela_compliance_owner;
ALTER TABLE legal_hold_events OWNER TO vela_compliance_owner;
REVOKE ALL ON TABLE compliance_principals, compliance_database_bindings,
    compliance_event_cursors, legal_holds, legal_hold_events FROM PUBLIC;

CREATE FUNCTION vela_reject_compliance_principal_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
        OR NEW.id IS DISTINCT FROM OLD.id
        OR NEW.stable_id IS DISTINCT FROM OLD.stable_id
        OR NEW.tls_uri_identity IS DISTINCT FROM OLD.tls_uri_identity
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'Compliance Principal identity is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'compliance_principal_identity_is_immutable';
    END IF;
    IF NEW.status IS DISTINCT FROM OLD.status OR NEW.disabled_at IS DISTINCT FROM OLD.disabled_at THEN
        IF NOT (
            OLD.status = 'ACTIVE'
            AND OLD.disabled_at IS NULL
            AND NEW.status = 'DISABLED'
            AND NEW.disabled_at IS NOT NULL
        ) THEN
            RAISE EXCEPTION 'Compliance Principal disablement is permanent'
                USING ERRCODE = '55000',
                    CONSTRAINT = 'compliance_principal_disablement_is_permanent';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_compliance_principal_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_compliance_principal_mutation() OWNER TO vela_compliance_owner;
CREATE TRIGGER compliance_principals_immutable
BEFORE UPDATE OR DELETE ON compliance_principals
FOR EACH ROW EXECUTE FUNCTION vela_reject_compliance_principal_mutation();

CREATE FUNCTION vela_reject_compliance_binding_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
        OR NEW.database_role IS DISTINCT FROM OLD.database_role
        OR NEW.principal_id IS DISTINCT FROM OLD.principal_id
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
        OR (
            NEW.disabled_at IS DISTINCT FROM OLD.disabled_at
            AND NOT (OLD.disabled_at IS NULL AND NEW.disabled_at IS NOT NULL)
        )
    THEN
        RAISE EXCEPTION 'Compliance database binding is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'compliance_database_binding_is_immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_compliance_binding_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_compliance_binding_mutation() OWNER TO vela_compliance_owner;
CREATE TRIGGER compliance_database_bindings_immutable
BEFORE UPDATE OR DELETE ON compliance_database_bindings
FOR EACH ROW EXECUTE FUNCTION vela_reject_compliance_binding_mutation();

CREATE FUNCTION vela_enforce_legal_hold_transition() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Legal Hold evidence is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'legal_hold_evidence_is_immutable';
    END IF;
    IF ROW(
        NEW.id, NEW.organization_id, NEW.project_id, NEW.job_id, NEW.scope,
        NEW.record_classes, NEW.placement_principal_id,
        NEW.placement_source_sequence, NEW.placement_reason_code,
        NEW.placement_external_reference, NEW.placement_effective_at,
        NEW.placed_at, NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id, OLD.organization_id, OLD.project_id, OLD.job_id, OLD.scope,
        OLD.record_classes, OLD.placement_principal_id,
        OLD.placement_source_sequence, OLD.placement_reason_code,
        OLD.placement_external_reference, OLD.placement_effective_at,
        OLD.placed_at, OLD.created_at
    ) OR NOT (
        OLD.state = 'ACTIVE'
        AND NEW.state = 'RELEASED'
        AND OLD.release_principal_id IS NULL
        AND NEW.release_principal_id IS NOT NULL
        AND NEW.release_source_sequence IS NOT NULL
        AND NEW.release_reason_code IS NOT NULL
        AND NEW.release_external_reference IS NOT NULL
        AND NEW.release_effective_at IS NOT NULL
        AND NEW.released_at IS NOT NULL
        AND NEW.updated_at = NEW.released_at
    ) THEN
        RAISE EXCEPTION 'Legal Hold transition is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'legal_hold_transition_is_immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_legal_hold_transition() FROM PUBLIC;
ALTER FUNCTION vela_enforce_legal_hold_transition() OWNER TO vela_compliance_owner;
CREATE TRIGGER legal_holds_immutable
BEFORE UPDATE OR DELETE ON legal_holds
FOR EACH ROW EXECUTE FUNCTION vela_enforce_legal_hold_transition();

CREATE FUNCTION vela_reject_legal_hold_event_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Legal Hold events are immutable'
        USING ERRCODE = '55000', CONSTRAINT = 'legal_hold_event_is_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_legal_hold_event_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_legal_hold_event_mutation() OWNER TO vela_compliance_owner;
CREATE TRIGGER legal_hold_events_immutable
BEFORE UPDATE OR DELETE ON legal_hold_events
FOR EACH ROW EXECUTE FUNCTION vela_reject_legal_hold_event_mutation();

CREATE FUNCTION vela_private.require_compliance_identity()
RETURNS TABLE (principal_id uuid, stable_id text, tls_uri_identity text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    RETURN QUERY
    SELECT principal.id, principal.stable_id, principal.tls_uri_identity
    FROM public.compliance_database_bindings AS binding
    JOIN public.compliance_principals AS principal ON principal.id = binding.principal_id
    WHERE binding.database_role = session_user::name
      AND binding.disabled_at IS NULL
      AND principal.status = 'ACTIVE'
      AND principal.disabled_at IS NULL;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'active Compliance Principal binding is required'
            USING ERRCODE = '28000';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.require_compliance_identity() FROM PUBLIC;
ALTER FUNCTION vela_private.require_compliance_identity() OWNER TO vela_compliance_owner;

CREATE FUNCTION vela_get_compliance_identity()
RETURNS TABLE (principal_id uuid, stable_id text, tls_uri_identity text)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT identity.principal_id, identity.stable_id, identity.tls_uri_identity
    FROM vela_private.require_compliance_identity() AS identity
$$;
REVOKE ALL ON FUNCTION vela_get_compliance_identity() FROM PUBLIC;
ALTER FUNCTION vela_get_compliance_identity() OWNER TO vela_compliance_owner;

CREATE FUNCTION vela_private.lock_active_non_content_legal_holds(
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_record_class legal_hold_record_class
) RETURNS TABLE (hold_id uuid)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_organization_id IS NULL OR p_project_id IS NULL OR p_job_id IS NULL
        OR p_record_class IS NULL
    THEN
        RAISE EXCEPTION 'Legal Hold target is invalid' USING ERRCODE = '22023';
    END IF;
    PERFORM 1
    FROM public.jobs AS job
    WHERE job.organization_id = p_organization_id
      AND job.project_id = p_project_id
      AND job.id = p_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Legal Hold descendant Job is not found' USING ERRCODE = 'P0002';
    END IF;
    RETURN QUERY
    SELECT hold.id
    FROM public.legal_holds AS hold
    WHERE hold.organization_id = p_organization_id
      AND p_record_class = ANY(hold.record_classes)
      AND hold.state = 'ACTIVE'
      AND (
          hold.scope = 'ORGANIZATION'
          OR (hold.scope = 'PROJECT' AND hold.project_id = p_project_id)
          OR (
              hold.scope = 'JOB'
              AND hold.project_id = p_project_id
              AND hold.job_id = p_job_id
          )
      )
    ORDER BY hold.id
    FOR UPDATE;
END
$$;
REVOKE ALL ON FUNCTION vela_private.lock_active_non_content_legal_holds(
    uuid, uuid, uuid, legal_hold_record_class
) FROM PUBLIC;
ALTER FUNCTION vela_private.lock_active_non_content_legal_holds(
    uuid, uuid, uuid, legal_hold_record_class
) OWNER TO vela_compliance_owner;

CREATE FUNCTION vela_apply_legal_hold_event(
    p_event_id uuid,
    p_idempotency_key text,
    p_source_sequence bigint,
    p_hold_id uuid,
    p_kind legal_hold_event_kind,
    p_scope legal_hold_scope,
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_record_classes legal_hold_record_class[],
    p_reason_code text,
    p_external_reference text,
    p_effective_at timestamptz
) RETURNS TABLE (
    event_id uuid,
    replayed boolean,
    hold_id uuid,
    state legal_hold_state,
    scope legal_hold_scope,
    record_classes legal_hold_record_class[],
    recorded_at timestamptz,
    released_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_identity record;
    v_existing public.legal_hold_events%ROWTYPE;
    v_hold public.legal_holds%ROWTYPE;
    v_last_sequence bigint;
    v_recorded_at timestamptz;
BEGIN
    SELECT * INTO STRICT v_identity FROM vela_private.require_compliance_identity();
    IF p_event_id IS NULL OR p_hold_id IS NULL OR p_kind IS NULL
        OR p_source_sequence IS NULL OR p_source_sequence <= 0
        OR p_idempotency_key IS NULL OR length(p_idempotency_key) NOT BETWEEN 1 AND 500
        OR btrim(p_idempotency_key) <> p_idempotency_key
        OR p_idempotency_key ~ '[[:cntrl:]]'
        OR p_reason_code IS NULL OR p_reason_code !~ '^[A-Z][A-Z0-9_]{0,99}$'
        OR p_external_reference IS NULL
        OR length(p_external_reference) NOT BETWEEN 1 AND 500
        OR btrim(p_external_reference) <> p_external_reference
        OR p_external_reference ~ '[[:cntrl:]]'
        OR p_effective_at IS NULL
    THEN
        RAISE EXCEPTION 'invalid Legal Hold event input' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.compliance_event_cursors (principal_id)
    VALUES (v_identity.principal_id)
    ON CONFLICT (principal_id) DO NOTHING;
    SELECT cursor.last_sequence INTO STRICT v_last_sequence
    FROM public.compliance_event_cursors AS cursor
    WHERE cursor.principal_id = v_identity.principal_id
    FOR UPDATE;

    SELECT * INTO v_existing
    FROM public.legal_hold_events AS event
    WHERE event.principal_id = v_identity.principal_id
      AND event.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.source_sequence IS DISTINCT FROM p_source_sequence
            OR v_existing.hold_id IS DISTINCT FROM p_hold_id
            OR v_existing.kind IS DISTINCT FROM p_kind
            OR v_existing.scope IS DISTINCT FROM p_scope
            OR v_existing.organization_id IS DISTINCT FROM p_organization_id
            OR v_existing.project_id IS DISTINCT FROM p_project_id
            OR v_existing.job_id IS DISTINCT FROM p_job_id
            OR v_existing.record_classes IS DISTINCT FROM p_record_classes
            OR v_existing.reason_code IS DISTINCT FROM p_reason_code
            OR v_existing.external_reference IS DISTINCT FROM p_external_reference
            OR v_existing.effective_at IS DISTINCT FROM p_effective_at
        THEN
            RAISE EXCEPTION 'Legal Hold idempotency key conflicts with committed input'
                USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_idempotency_conflict';
        END IF;
        SELECT * INTO STRICT v_hold FROM public.legal_holds WHERE id = p_hold_id;
        RETURN QUERY SELECT
            v_existing.id, true, v_hold.id, v_hold.state, v_hold.scope,
            v_hold.record_classes, v_existing.recorded_at, v_hold.released_at;
        RETURN;
    END IF;

    IF v_last_sequence = 9223372036854775807 OR p_source_sequence <> v_last_sequence + 1 THEN
        RAISE EXCEPTION 'Legal Hold source sequence is out of order'
            USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_sequence_out_of_order';
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.legal_hold_events AS event
        WHERE event.principal_id = v_identity.principal_id
          AND event.external_reference = p_external_reference
    ) THEN
        RAISE EXCEPTION 'Legal Hold external reference conflicts with committed input'
            USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_external_reference_conflict';
    END IF;
    v_recorded_at := clock_timestamp();

    IF p_kind = 'HOLD_PLACED' THEN
        IF p_scope IS NULL OR p_organization_id IS NULL OR p_record_classes IS NULL
            OR NOT (
                (p_scope = 'ORGANIZATION' AND p_project_id IS NULL AND p_job_id IS NULL)
                OR (p_scope = 'PROJECT' AND p_project_id IS NOT NULL AND p_job_id IS NULL)
                OR (p_scope = 'JOB' AND p_project_id IS NOT NULL AND p_job_id IS NOT NULL)
            )
            OR NOT (
                p_record_classes = ARRAY['METADATA']::legal_hold_record_class[]
                OR p_record_classes = ARRAY['FINANCIAL']::legal_hold_record_class[]
                OR p_record_classes = ARRAY['METADATA', 'FINANCIAL']::legal_hold_record_class[]
            )
        THEN
            RAISE EXCEPTION 'invalid Legal Hold placement shape' USING ERRCODE = '22023';
        END IF;
        IF p_scope = 'ORGANIZATION' THEN
            PERFORM 1 FROM public.customer_organizations
            WHERE id = p_organization_id;
        ELSIF p_scope = 'PROJECT' THEN
            PERFORM 1 FROM public.projects
            WHERE organization_id = p_organization_id AND id = p_project_id;
        ELSE
            PERFORM 1 FROM public.jobs
            WHERE organization_id = p_organization_id
              AND project_id = p_project_id AND id = p_job_id;
        END IF;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Legal Hold target is not found' USING ERRCODE = 'P0002';
        END IF;
        INSERT INTO public.legal_holds (
            id, organization_id, project_id, job_id, scope, record_classes,
            placement_principal_id, placement_source_sequence,
            placement_reason_code, placement_external_reference,
            placement_effective_at, placed_at
        ) VALUES (
            p_hold_id, p_organization_id, p_project_id, p_job_id, p_scope,
            p_record_classes, v_identity.principal_id, p_source_sequence,
            p_reason_code, p_external_reference, p_effective_at, v_recorded_at
        );
    ELSIF p_kind = 'HOLD_RELEASED' THEN
        IF p_scope IS NOT NULL OR p_organization_id IS NOT NULL
            OR p_project_id IS NOT NULL OR p_job_id IS NOT NULL
            OR p_record_classes IS NOT NULL
        THEN
            RAISE EXCEPTION 'invalid Legal Hold release shape' USING ERRCODE = '22023';
        END IF;
        SELECT * INTO v_hold FROM public.legal_holds WHERE id = p_hold_id FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Legal Hold is not found' USING ERRCODE = 'P0002';
        END IF;
        IF v_hold.state <> 'ACTIVE' THEN
            RAISE EXCEPTION 'Legal Hold is already released'
                USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_already_released';
        END IF;
        IF p_effective_at < v_hold.placement_effective_at
            OR v_recorded_at < v_hold.placed_at
        THEN
            RAISE EXCEPTION 'Legal Hold release precedes placement'
                USING ERRCODE = 'P0001', CONSTRAINT = 'legal_hold_release_precedes_placement';
        END IF;
        UPDATE public.legal_holds
        SET state = 'RELEASED',
            release_principal_id = v_identity.principal_id,
            release_source_sequence = p_source_sequence,
            release_reason_code = p_reason_code,
            release_external_reference = p_external_reference,
            release_effective_at = p_effective_at,
            released_at = v_recorded_at,
            updated_at = v_recorded_at
        WHERE id = p_hold_id;
    ELSE
        RAISE EXCEPTION 'unsupported Legal Hold event kind' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.legal_hold_events (
        id, principal_id, idempotency_key, source_sequence, hold_id, kind,
        scope, organization_id, project_id, job_id, record_classes,
        reason_code, external_reference, effective_at, recorded_at
    ) VALUES (
        p_event_id, v_identity.principal_id, p_idempotency_key,
        p_source_sequence, p_hold_id, p_kind, p_scope, p_organization_id,
        p_project_id, p_job_id, p_record_classes, p_reason_code,
        p_external_reference, p_effective_at, v_recorded_at
    );
    UPDATE public.compliance_event_cursors
    SET last_sequence = p_source_sequence, updated_at = v_recorded_at
    WHERE principal_id = v_identity.principal_id;
    SELECT * INTO STRICT v_hold FROM public.legal_holds WHERE id = p_hold_id;
    RETURN QUERY SELECT
        p_event_id, false, v_hold.id, v_hold.state, v_hold.scope,
        v_hold.record_classes, v_recorded_at, v_hold.released_at;
END
$$;
REVOKE ALL ON FUNCTION vela_apply_legal_hold_event(
    uuid, text, bigint, uuid, legal_hold_event_kind, legal_hold_scope,
    uuid, uuid, uuid, legal_hold_record_class[], text, text, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_apply_legal_hold_event(
    uuid, text, bigint, uuid, legal_hold_event_kind, legal_hold_scope,
    uuid, uuid, uuid, legal_hold_record_class[], text, text, timestamptz
) OWNER TO vela_compliance_owner;

GRANT USAGE ON SCHEMA public, vela_private TO vela_compliance_owner;
GRANT USAGE ON TYPE legal_hold_event_kind, legal_hold_scope,
    legal_hold_record_class, legal_hold_state TO vela_compliance_owner;
GRANT SELECT ON compliance_principals, compliance_database_bindings,
    compliance_event_cursors, legal_holds, legal_hold_events,
    customer_organizations, projects, jobs TO vela_compliance_owner;
GRANT INSERT ON compliance_event_cursors, legal_holds, legal_hold_events
    TO vela_compliance_owner;
GRANT UPDATE ON compliance_event_cursors, legal_holds TO vela_compliance_owner;
GRANT EXECUTE ON FUNCTION vela_private.require_compliance_identity(),
    vela_private.lock_active_non_content_legal_holds(
        uuid, uuid, uuid, legal_hold_record_class
    ) TO vela_compliance_owner;

GRANT USAGE ON SCHEMA public TO vela_compliance;
GRANT USAGE ON TYPE legal_hold_event_kind, legal_hold_scope,
    legal_hold_record_class, legal_hold_state TO vela_compliance;
GRANT EXECUTE ON FUNCTION vela_get_compliance_identity() TO vela_compliance;
GRANT EXECUTE ON FUNCTION vela_apply_legal_hold_event(
    uuid, text, bigint, uuid, legal_hold_event_kind, legal_hold_scope,
    uuid, uuid, uuid, legal_hold_record_class[], text, text, timestamptz
) TO vela_compliance;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE legal_hold_events, legal_holds, compliance_event_cursors,
    compliance_database_bindings, compliance_principals IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM legal_hold_events)
        OR EXISTS (SELECT 1 FROM legal_holds)
        OR EXISTS (SELECT 1 FROM compliance_event_cursors)
        OR EXISTS (SELECT 1 FROM compliance_database_bindings)
        OR EXISTS (SELECT 1 FROM compliance_principals)
    THEN
        RAISE EXCEPTION 'cannot remove Legal Hold authority with durable evidence or provisioning'
            USING ERRCODE = '55000',
                CONSTRAINT = 'legal_hold_contract_has_durable_evidence';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_apply_legal_hold_event(
    uuid, text, bigint, uuid, legal_hold_event_kind, legal_hold_scope,
    uuid, uuid, uuid, legal_hold_record_class[], text, text, timestamptz
) FROM vela_compliance;
REVOKE EXECUTE ON FUNCTION vela_get_compliance_identity() FROM vela_compliance;
REVOKE USAGE ON TYPE legal_hold_event_kind, legal_hold_scope,
    legal_hold_record_class, legal_hold_state FROM vela_compliance;
REVOKE USAGE ON SCHEMA public FROM vela_compliance;
REVOKE EXECUTE ON FUNCTION vela_private.require_compliance_identity()
    FROM vela_compliance_owner;

DROP FUNCTION vela_apply_legal_hold_event(
    uuid, text, bigint, uuid, legal_hold_event_kind, legal_hold_scope,
    uuid, uuid, uuid, legal_hold_record_class[], text, text, timestamptz
);
DROP FUNCTION vela_private.lock_active_non_content_legal_holds(
    uuid, uuid, uuid, legal_hold_record_class
);
DROP FUNCTION vela_get_compliance_identity();
DROP FUNCTION vela_private.require_compliance_identity();
DROP TRIGGER legal_hold_events_immutable ON legal_hold_events;
DROP FUNCTION vela_reject_legal_hold_event_mutation();
DROP TRIGGER legal_holds_immutable ON legal_holds;
DROP FUNCTION vela_enforce_legal_hold_transition();
DROP TRIGGER compliance_database_bindings_immutable ON compliance_database_bindings;
DROP FUNCTION vela_reject_compliance_binding_mutation();
DROP TRIGGER compliance_principals_immutable ON compliance_principals;
DROP FUNCTION vela_reject_compliance_principal_mutation();

REVOKE UPDATE ON compliance_event_cursors, legal_holds FROM vela_compliance_owner;
REVOKE INSERT ON compliance_event_cursors, legal_holds, legal_hold_events
    FROM vela_compliance_owner;
REVOKE SELECT ON compliance_principals, compliance_database_bindings,
    compliance_event_cursors, legal_holds, legal_hold_events,
    customer_organizations, projects, jobs FROM vela_compliance_owner;
REVOKE USAGE ON TYPE legal_hold_event_kind, legal_hold_scope,
    legal_hold_record_class, legal_hold_state FROM vela_compliance_owner;
REVOKE USAGE ON SCHEMA public, vela_private FROM vela_compliance_owner;

DROP TABLE legal_hold_events;
DROP TABLE legal_holds;
DROP TABLE compliance_event_cursors;
DROP TABLE compliance_database_bindings;
DROP TABLE compliance_principals;
DROP TYPE legal_hold_state;
DROP TYPE legal_hold_record_class;
DROP TYPE legal_hold_scope;
DROP TYPE legal_hold_event_kind;
-- +goose StatementEnd
