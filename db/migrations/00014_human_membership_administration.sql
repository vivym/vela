-- +goose Up
-- +goose StatementBegin
CREATE TYPE human_administration_context_kind AS ENUM (
    'ORGANIZATION',
    'PROJECT'
);

CREATE TYPE human_identity_admin_action AS ENUM (
    'HUMAN_MEMBER_CREATED',
    'HUMAN_MEMBER_DISABLED',
    'ORGANIZATION_ROLE_ASSIGNED',
    'ORGANIZATION_ROLE_REVOKED',
    'PROJECT_ROLE_ASSIGNED',
    'PROJECT_ROLE_REVOKED'
);

CREATE FUNCTION vela_organization_role_scopes(p_role organization_role) RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE p_role
        WHEN 'OrganizationOwner'::public.organization_role THEN
            ARRAY[
                'organization_members:manage',
                'organization_members:read',
                'project_members:manage',
                'project_members:read'
            ]::text[]
        WHEN 'BillingAdmin'::public.organization_role THEN ARRAY[]::text[]
        WHEN 'OrganizationAuditor'::public.organization_role THEN ARRAY[]::text[]
    END
$$;
REVOKE ALL ON FUNCTION vela_organization_role_scopes(organization_role) FROM PUBLIC;
ALTER FUNCTION vela_organization_role_scopes(organization_role) OWNER TO vela_internal;

CREATE OR REPLACE FUNCTION vela_project_role_scopes(p_role project_role) RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE p_role
        WHEN 'ProjectAdmin'::public.project_role THEN
            ARRAY[
                'project_members:manage',
                'project_members:read',
                'service_principals:manage',
                'service_principals:read',
                'webhooks:manage',
                'webhooks:read'
            ]::text[]
        WHEN 'Developer'::public.project_role THEN
            ARRAY['artifacts:read', 'jobs:cancel', 'jobs:read', 'jobs:submit']::text[]
        WHEN 'ProjectViewer'::public.project_role THEN
            ARRAY['artifacts:read', 'jobs:read']::text[]
    END
$$;
REVOKE ALL ON FUNCTION vela_project_role_scopes(project_role) FROM PUBLIC;
ALTER FUNCTION vela_project_role_scopes(project_role) OWNER TO vela_internal;

CREATE TABLE human_organization_auth_sessions (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    principal_id uuid NOT NULL,
    token_proof bytea NOT NULL CHECK (octet_length(token_proof) = 32),
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, id),
    CONSTRAINT human_organization_auth_sessions_token_proof_key
        UNIQUE (organization_id, principal_id, token_proof),
    FOREIGN KEY (organization_id, principal_id)
        REFERENCES human_oidc_bindings(organization_id, principal_id),
    CHECK (expires_at > created_at)
);
CREATE INDEX human_organization_auth_sessions_expiry_idx
    ON human_organization_auth_sessions (expires_at, id);
ALTER TABLE human_organization_auth_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE human_organization_auth_sessions FORCE ROW LEVEL SECURITY;
CREATE TRIGGER human_organization_auth_session_identity_immutable
BEFORE UPDATE ON human_organization_auth_sessions
FOR EACH ROW EXECUTE FUNCTION vela_reject_human_identity_evidence_update();

CREATE TABLE vela_private.human_administration_contexts (
    backend_pid integer PRIMARY KEY,
    transaction_id xid8 NOT NULL,
    actor_session_id uuid NOT NULL,
    actor_context_kind human_administration_context_kind NOT NULL,
    required_scope text NOT NULL CHECK (length(required_scope) BETWEEN 1 AND 100),
    organization_id uuid NOT NULL,
    project_id uuid,
    principal_id uuid NOT NULL,
    established_at timestamptz NOT NULL,
    CHECK (
        actor_context_kind = 'ORGANIZATION'
        OR (actor_context_kind = 'PROJECT' AND project_id IS NOT NULL)
    )
);
ALTER TABLE vela_private.human_administration_contexts OWNER TO vela_internal;
REVOKE ALL ON TABLE vela_private.human_administration_contexts FROM PUBLIC;

CREATE FUNCTION vela_private.establish_human_administration_context(
    p_actor_session_id uuid,
    p_actor_context_kind human_administration_context_kind,
    p_required_scope text,
    p_organization_id uuid,
    p_project_id uuid,
    p_principal_id uuid,
    p_established_at timestamptz
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_transaction_id xid8 := pg_catalog.pg_current_xact_id();
    v_existing_session_id uuid;
    v_existing_context_kind public.human_administration_context_kind;
    v_existing_scope text;
    v_existing_organization_id uuid;
    v_existing_project_id uuid;
    v_existing_principal_id uuid;
BEGIN
    DELETE FROM vela_private.human_administration_contexts AS context
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_stat_activity AS activity
        WHERE activity.pid = context.backend_pid
    );

    SELECT
        context.actor_session_id,
        context.actor_context_kind,
        context.required_scope,
        context.organization_id,
        context.project_id,
        context.principal_id
    INTO
        v_existing_session_id,
        v_existing_context_kind,
        v_existing_scope,
        v_existing_organization_id,
        v_existing_project_id,
        v_existing_principal_id
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = v_transaction_id;

    IF FOUND THEN
        IF v_existing_session_id IS DISTINCT FROM p_actor_session_id
            OR v_existing_context_kind IS DISTINCT FROM p_actor_context_kind
            OR v_existing_scope IS DISTINCT FROM p_required_scope
            OR v_existing_organization_id IS DISTINCT FROM p_organization_id
            OR v_existing_project_id IS DISTINCT FROM p_project_id
            OR v_existing_principal_id IS DISTINCT FROM p_principal_id
        THEN
            RAISE EXCEPTION 'Human administration context is already established'
                USING ERRCODE = '28000';
        END IF;
        RETURN;
    END IF;

    INSERT INTO vela_private.human_administration_contexts (
        backend_pid,
        transaction_id,
        actor_session_id,
        actor_context_kind,
        required_scope,
        organization_id,
        project_id,
        principal_id,
        established_at
    ) VALUES (
        pg_catalog.pg_backend_pid(),
        v_transaction_id,
        p_actor_session_id,
        p_actor_context_kind,
        p_required_scope,
        p_organization_id,
        p_project_id,
        p_principal_id,
        p_established_at
    )
    ON CONFLICT (backend_pid) DO UPDATE
    SET transaction_id = EXCLUDED.transaction_id,
        actor_session_id = EXCLUDED.actor_session_id,
        actor_context_kind = EXCLUDED.actor_context_kind,
        required_scope = EXCLUDED.required_scope,
        organization_id = EXCLUDED.organization_id,
        project_id = EXCLUDED.project_id,
        principal_id = EXCLUDED.principal_id,
        established_at = EXCLUDED.established_at
    WHERE human_administration_contexts.transaction_id <> EXCLUDED.transaction_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human administration context is already established'
            USING ERRCODE = '28000';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.establish_human_administration_context(
    uuid, human_administration_context_kind, text, uuid, uuid, uuid, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_private.establish_human_administration_context(
    uuid, human_administration_context_kind, text, uuid, uuid, uuid, timestamptz
) OWNER TO vela_internal;

CREATE TABLE human_administration_actor_attributions (
    organization_id uuid NOT NULL,
    actor_session_id uuid NOT NULL,
    actor_principal_id uuid NOT NULL,
    actor_context_kind human_administration_context_kind NOT NULL,
    project_id uuid,
    first_attributed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (organization_id, actor_session_id),
    UNIQUE (organization_id, actor_session_id, actor_principal_id),
    FOREIGN KEY (organization_id, actor_principal_id)
        REFERENCES principals(organization_id, id),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id),
    CHECK (
        (actor_context_kind = 'ORGANIZATION' AND project_id IS NULL)
        OR (actor_context_kind = 'PROJECT' AND project_id IS NOT NULL)
    )
);

CREATE TABLE human_identity_events (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid,
    actor_principal_id uuid NOT NULL,
    actor_session_id uuid NOT NULL,
    action human_identity_admin_action NOT NULL,
    target_principal_id uuid NOT NULL,
    organization_role organization_role,
    project_role project_role,
    details jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(details) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, actor_session_id, actor_principal_id)
        REFERENCES human_administration_actor_attributions(
            organization_id, actor_session_id, actor_principal_id
        ),
    FOREIGN KEY (organization_id, target_principal_id)
        REFERENCES principals(organization_id, id),
    CHECK (
        (action IN ('HUMAN_MEMBER_CREATED', 'HUMAN_MEMBER_DISABLED')
            AND project_id IS NULL AND organization_role IS NULL AND project_role IS NULL)
        OR (action IN ('ORGANIZATION_ROLE_ASSIGNED', 'ORGANIZATION_ROLE_REVOKED')
            AND project_id IS NULL AND organization_role IS NOT NULL AND project_role IS NULL)
        OR (action IN ('PROJECT_ROLE_ASSIGNED', 'PROJECT_ROLE_REVOKED')
            AND project_id IS NOT NULL AND organization_role IS NULL AND project_role IS NOT NULL)
    )
);
CREATE INDEX human_identity_events_organization_idx
    ON human_identity_events (organization_id, created_at, id);
CREATE INDEX human_identity_events_project_idx
    ON human_identity_events (organization_id, project_id, created_at, id)
    WHERE project_id IS NOT NULL;

ALTER TABLE human_administration_actor_attributions ENABLE ROW LEVEL SECURITY;
ALTER TABLE human_administration_actor_attributions FORCE ROW LEVEL SECURITY;
ALTER TABLE human_identity_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE human_identity_events FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_reject_human_administration_evidence_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Human administration evidence is immutable'
        USING ERRCODE = '55000',
            CONSTRAINT = 'human_administration_evidence_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_human_administration_evidence_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_human_administration_evidence_mutation() OWNER TO vela_internal;

CREATE TRIGGER human_administration_actor_attributions_immutable
BEFORE UPDATE OR DELETE ON human_administration_actor_attributions
FOR EACH ROW EXECUTE FUNCTION vela_reject_human_administration_evidence_mutation();
CREATE TRIGGER human_identity_events_immutable
BEFORE UPDATE OR DELETE ON human_identity_events
FOR EACH ROW EXECUTE FUNCTION vela_reject_human_administration_evidence_mutation();

CREATE FUNCTION vela_reject_human_member_reenable() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.disabled_at IS NOT NULL
        AND NEW.disabled_at IS DISTINCT FROM OLD.disabled_at
    THEN
        RAISE EXCEPTION 'Human member disablement is permanent'
            USING ERRCODE = '55000',
                CONSTRAINT = 'human_member_disablement_is_permanent';
    END IF;
    IF OLD.disabled_at IS NULL
        AND NEW.disabled_at IS NOT NULL
        AND NEW.disabled_at < OLD.created_at
    THEN
        RAISE EXCEPTION 'Human member disablement predates creation'
            USING ERRCODE = '23514',
                CONSTRAINT = 'human_member_disabled_after_creation';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_human_member_reenable() FROM PUBLIC;
ALTER FUNCTION vela_reject_human_member_reenable() OWNER TO vela_internal;

CREATE TRIGGER human_oidc_bindings_disable_permanent
BEFORE UPDATE OF disabled_at ON human_oidc_bindings
FOR EACH ROW EXECUTE FUNCTION vela_reject_human_member_reenable();

CREATE FUNCTION vela_authenticate_human_organization_oidc(
    p_issuer text,
    p_subject text,
    p_token_proof bytea,
    p_token_expires_at timestamptz
) RETURNS TABLE (
    organization_id uuid,
    principal_id uuid,
    session_id uuid,
    scopes text[]
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid;
    v_principal_id uuid;
    v_session_id uuid;
    v_scopes text[];
    v_session_expires_at timestamptz;
BEGIN
    IF p_issuer IS NULL OR p_subject IS NULL
        OR octet_length(p_token_proof) IS DISTINCT FROM 32
        OR p_token_expires_at IS NULL
        OR NOT p_token_expires_at > clock_timestamp()
    THEN
        RAISE EXCEPTION 'invalid or inactive Human OIDC identity' USING ERRCODE = '28000';
    END IF;

    SELECT binding.organization_id, binding.principal_id
    INTO v_organization_id, v_principal_id
    FROM public.human_oidc_bindings AS binding
    JOIN public.principals AS principal
      ON principal.organization_id = binding.organization_id
     AND principal.id = binding.principal_id
     AND principal.kind = 'HUMAN'
    JOIN public.customer_organizations AS organization
      ON organization.id = binding.organization_id
    WHERE binding.issuer = p_issuer
      AND binding.subject = p_subject
      AND binding.disabled_at IS NULL
      AND organization.status = 'ACTIVE';

    IF NOT FOUND THEN
        RAISE EXCEPTION 'invalid or inactive Human OIDC identity' USING ERRCODE = '28000';
    END IF;

    SELECT COALESCE(array_agg(DISTINCT permission ORDER BY permission), ARRAY[]::text[])
    INTO v_scopes
    FROM public.organization_role_bindings AS binding
    CROSS JOIN LATERAL unnest(public.vela_organization_role_scopes(binding.role)) AS permission
    WHERE binding.organization_id = v_organization_id
      AND binding.principal_id = v_principal_id;

    IF cardinality(v_scopes) > 0 THEN
        v_session_expires_at := LEAST(
            p_token_expires_at,
            clock_timestamp() + interval '5 minutes'
        );

        DELETE FROM public.human_organization_auth_sessions AS session
        WHERE session.organization_id = v_organization_id
          AND session.principal_id = v_principal_id
          AND session.expires_at <= clock_timestamp();

        SELECT session.id
        INTO v_session_id
        FROM public.human_organization_auth_sessions AS session
        WHERE session.organization_id = v_organization_id
          AND session.principal_id = v_principal_id
          AND session.token_proof = p_token_proof
          AND session.expires_at > clock_timestamp();

        IF NOT FOUND THEN
            v_session_id := gen_random_uuid();
            INSERT INTO public.human_organization_auth_sessions (
                id, organization_id, principal_id, token_proof, expires_at
            ) VALUES (
                v_session_id,
                v_organization_id,
                v_principal_id,
                p_token_proof,
                v_session_expires_at
            )
            ON CONFLICT ON CONSTRAINT human_organization_auth_sessions_token_proof_key
                DO NOTHING
            RETURNING id INTO v_session_id;

            IF NOT FOUND THEN
                SELECT session.id
                INTO STRICT v_session_id
                FROM public.human_organization_auth_sessions AS session
                WHERE session.organization_id = v_organization_id
                  AND session.principal_id = v_principal_id
                  AND session.token_proof = p_token_proof
                  AND session.expires_at > clock_timestamp();
            END IF;
        END IF;
    END IF;

    RETURN QUERY SELECT
        v_organization_id,
        v_principal_id,
        v_session_id,
        v_scopes;
END
$$;
REVOKE ALL ON FUNCTION vela_authenticate_human_organization_oidc(
    text, text, bytea, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_authenticate_human_organization_oidc(
    text, text, bytea, timestamptz
) OWNER TO vela_internal;
GRANT USAGE ON SCHEMA public TO vela_human_membership_auth;
GRANT EXECUTE ON FUNCTION vela_authenticate_human_organization_oidc(
    text, text, bytea, timestamptz
) TO vela_human_membership_auth;

CREATE FUNCTION vela_set_organization_identity_admin_context(
    p_actor_session_id uuid,
    p_actor_session_proof bytea,
    p_required_scope text
) RETURNS TABLE (
    organization_id uuid,
    principal_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_organization_id uuid;
    v_principal_id uuid;
    v_transaction_time timestamptz := clock_timestamp();
BEGIN
    IF p_required_scope NOT IN (
        'organization_members:manage',
        'organization_members:read'
    ) OR octet_length(p_actor_session_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid Organization identity administration request'
            USING ERRCODE = '22023';
    END IF;

    SELECT session.organization_id, session.principal_id
    INTO v_organization_id, v_principal_id
    FROM public.human_organization_auth_sessions AS session
    JOIN public.human_oidc_bindings AS human
      ON human.organization_id = session.organization_id
     AND human.principal_id = session.principal_id
     AND human.disabled_at IS NULL
    JOIN public.organization_role_bindings AS role_binding
      ON role_binding.organization_id = session.organization_id
     AND role_binding.principal_id = session.principal_id
    CROSS JOIN LATERAL unnest(
        public.vela_organization_role_scopes(role_binding.role)
    ) AS allowed_scope
    JOIN public.customer_organizations AS organization
      ON organization.id = session.organization_id
     AND organization.status = 'ACTIVE'
    WHERE session.id = p_actor_session_id
      AND session.token_proof = p_actor_session_proof
      AND session.expires_at > v_transaction_time
      AND allowed_scope = p_required_scope
    LIMIT 1;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'credential is no longer active' USING ERRCODE = '28000';
    END IF;

    PERFORM vela_private.establish_human_administration_context(
        p_actor_session_id,
        'ORGANIZATION',
        p_required_scope,
        v_organization_id,
        NULL,
        v_principal_id,
        v_transaction_time
    );

    RETURN QUERY SELECT v_organization_id, v_principal_id, v_transaction_time;
END
$$;
REVOKE ALL ON FUNCTION vela_set_organization_identity_admin_context(
    uuid, bytea, text
) FROM PUBLIC;
ALTER FUNCTION vela_set_organization_identity_admin_context(
    uuid, bytea, text
) OWNER TO vela_internal;

CREATE FUNCTION vela_set_project_membership_admin_context(
    p_actor_session_id uuid,
    p_actor_session_proof bytea,
    p_project_id uuid,
    p_required_scope text
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_context_kind public.human_administration_context_kind;
    v_organization_id uuid;
    v_principal_id uuid;
    v_transaction_time timestamptz := clock_timestamp();
BEGIN
    IF p_project_id IS NULL OR p_required_scope NOT IN (
        'project_members:manage',
        'project_members:read'
    ) OR octet_length(p_actor_session_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid Project membership administration request'
            USING ERRCODE = '22023';
    END IF;

    SELECT session.organization_id, session.principal_id, 'PROJECT'
    INTO v_organization_id, v_principal_id, v_actor_context_kind
    FROM public.human_auth_sessions AS session
    JOIN public.human_oidc_bindings AS human
      ON human.organization_id = session.organization_id
     AND human.principal_id = session.principal_id
     AND human.disabled_at IS NULL
    JOIN public.project_role_bindings AS role_binding
      ON role_binding.organization_id = session.organization_id
     AND role_binding.project_id = session.project_id
     AND role_binding.principal_id = session.principal_id
    CROSS JOIN LATERAL unnest(
        public.vela_project_role_scopes(role_binding.role)
    ) AS allowed_scope
    JOIN public.customer_organizations AS organization
      ON organization.id = session.organization_id
     AND organization.status = 'ACTIVE'
    WHERE session.id = p_actor_session_id
      AND session.project_id = p_project_id
      AND session.token_proof = p_actor_session_proof
      AND session.expires_at > v_transaction_time
      AND allowed_scope = p_required_scope
    LIMIT 1;

    IF NOT FOUND THEN
        SELECT session.organization_id, session.principal_id, 'ORGANIZATION'
        INTO v_organization_id, v_principal_id, v_actor_context_kind
        FROM public.human_organization_auth_sessions AS session
        JOIN public.human_oidc_bindings AS human
          ON human.organization_id = session.organization_id
         AND human.principal_id = session.principal_id
         AND human.disabled_at IS NULL
        JOIN public.organization_role_bindings AS role_binding
          ON role_binding.organization_id = session.organization_id
         AND role_binding.principal_id = session.principal_id
        CROSS JOIN LATERAL unnest(
            public.vela_organization_role_scopes(role_binding.role)
        ) AS allowed_scope
        JOIN public.customer_organizations AS organization
          ON organization.id = session.organization_id
         AND organization.status = 'ACTIVE'
        WHERE session.id = p_actor_session_id
          AND session.token_proof = p_actor_session_proof
          AND session.expires_at > v_transaction_time
          AND allowed_scope = p_required_scope
        LIMIT 1;
    END IF;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'credential is no longer active' USING ERRCODE = '28000';
    END IF;

    PERFORM 1
    FROM public.projects AS project
    WHERE project.organization_id = v_organization_id
      AND project.id = p_project_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Project is not visible' USING ERRCODE = 'P0002';
    END IF;

    PERFORM vela_private.establish_human_administration_context(
        p_actor_session_id,
        v_actor_context_kind,
        p_required_scope,
        v_organization_id,
        p_project_id,
        v_principal_id,
        v_transaction_time
    );

    RETURN QUERY SELECT
        v_organization_id, p_project_id, v_principal_id, v_transaction_time;
END
$$;
REVOKE ALL ON FUNCTION vela_set_project_membership_admin_context(
    uuid, bytea, uuid, text
) FROM PUBLIC;
ALTER FUNCTION vela_set_project_membership_admin_context(
    uuid, bytea, uuid, text
) OWNER TO vela_internal;

CREATE FUNCTION vela_private.record_human_identity_event(
    p_action human_identity_admin_action,
    p_target_principal_id uuid,
    p_organization_role organization_role DEFAULT NULL,
    p_project_role project_role DEFAULT NULL,
    p_details jsonb DEFAULT '{}'::jsonb
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_context vela_private.human_administration_contexts%ROWTYPE;
BEGIN
    SELECT context.*
    INTO v_context
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id();

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human administration authorization is required'
            USING ERRCODE = '42501';
    END IF;

    INSERT INTO public.human_administration_actor_attributions (
        organization_id,
        actor_session_id,
        actor_principal_id,
        actor_context_kind,
        project_id,
        first_attributed_at
    ) VALUES (
        v_context.organization_id,
        v_context.actor_session_id,
        v_context.principal_id,
        v_context.actor_context_kind,
        CASE
            WHEN v_context.actor_context_kind = 'PROJECT' THEN v_context.project_id
            ELSE NULL
        END,
        v_context.established_at
    ) ON CONFLICT ON CONSTRAINT human_administration_actor_attributions_pkey
        DO NOTHING;

    INSERT INTO public.human_identity_events (
        id,
        organization_id,
        project_id,
        actor_principal_id,
        actor_session_id,
        action,
        target_principal_id,
        organization_role,
        project_role,
        details
    ) VALUES (
        gen_random_uuid(),
        v_context.organization_id,
        v_context.project_id,
        v_context.principal_id,
        v_context.actor_session_id,
        p_action,
        p_target_principal_id,
        p_organization_role,
        p_project_role,
        p_details
    );
END
$$;
REVOKE ALL ON FUNCTION vela_private.record_human_identity_event(
    human_identity_admin_action, uuid, organization_role, project_role, jsonb
) FROM PUBLIC;
ALTER FUNCTION vela_private.record_human_identity_event(
    human_identity_admin_action, uuid, organization_role, project_role, jsonb
) OWNER TO vela_internal;

CREATE FUNCTION vela_create_human_member(
    p_principal_id uuid,
    p_organization_id uuid,
    p_issuer text,
    p_subject text,
    p_display_name text
) RETURNS TABLE (
    principal_id uuid,
    organization_id uuid,
    oidc_issuer text,
    oidc_subject text,
    display_name text,
    created_at timestamptz,
    disabled_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_organization_id uuid;
    v_created_at timestamptz;
BEGIN
    SELECT context.organization_id
    INTO v_actor_organization_id
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_members:manage';

    IF NOT FOUND OR v_actor_organization_id IS DISTINCT FROM p_organization_id THEN
        RAISE EXCEPTION 'Human OrganizationOwner authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_principal_id IS NULL OR p_organization_id IS NULL
        OR p_issuer IS NULL OR octet_length(p_issuer) NOT BETWEEN 1 AND 2048
        OR p_subject IS NULL OR octet_length(p_subject) NOT BETWEEN 1 AND 500
        OR p_display_name IS NULL OR length(p_display_name) NOT BETWEEN 1 AND 200
    THEN
        RAISE EXCEPTION 'invalid Human member definition' USING ERRCODE = '22023';
    END IF;

    SELECT
        binding.principal_id,
        binding.organization_id,
        binding.issuer,
        binding.subject,
        principal.display_name,
        binding.created_at,
        binding.disabled_at
    INTO
        principal_id,
        organization_id,
        oidc_issuer,
        oidc_subject,
        display_name,
        created_at,
        disabled_at
    FROM public.human_oidc_bindings AS binding
    JOIN public.principals AS principal
      ON principal.organization_id = binding.organization_id
     AND principal.id = binding.principal_id
     AND principal.kind = 'HUMAN'
    WHERE binding.issuer = p_issuer
      AND binding.subject = p_subject;

    IF FOUND THEN
        IF organization_id IS DISTINCT FROM p_organization_id THEN
            RAISE unique_violation USING
                CONSTRAINT = 'human_oidc_bindings_issuer_subject_key';
        END IF;
        RETURN NEXT;
        RETURN;
    END IF;

    INSERT INTO public.principals (
        id, organization_id, kind, display_name
    ) VALUES (
        p_principal_id, p_organization_id, 'HUMAN', p_display_name
    ) RETURNING principals.created_at INTO v_created_at;

    INSERT INTO public.human_oidc_bindings (
        principal_id,
        organization_id,
        principal_kind,
        issuer,
        subject,
        display_name,
        created_at
    ) VALUES (
        p_principal_id,
        p_organization_id,
        'HUMAN',
        p_issuer,
        p_subject,
        p_display_name,
        v_created_at
    ) ON CONFLICT ON CONSTRAINT human_oidc_bindings_issuer_subject_key DO NOTHING;

    IF NOT FOUND THEN
        DELETE FROM public.principals AS principal
        WHERE principal.organization_id = p_organization_id
          AND principal.id = p_principal_id;

        SELECT
            binding.principal_id,
            binding.organization_id,
            binding.issuer,
            binding.subject,
            principal.display_name,
            binding.created_at,
            binding.disabled_at
        INTO STRICT
            principal_id,
            organization_id,
            oidc_issuer,
            oidc_subject,
            display_name,
            created_at,
            disabled_at
        FROM public.human_oidc_bindings AS binding
        JOIN public.principals AS principal
          ON principal.organization_id = binding.organization_id
         AND principal.id = binding.principal_id
         AND principal.kind = 'HUMAN'
        WHERE binding.issuer = p_issuer
          AND binding.subject = p_subject;

        IF organization_id IS DISTINCT FROM p_organization_id THEN
            RAISE unique_violation USING
                CONSTRAINT = 'human_oidc_bindings_issuer_subject_key';
        END IF;
        RETURN NEXT;
        RETURN;
    END IF;

    PERFORM vela_private.record_human_identity_event(
        'HUMAN_MEMBER_CREATED',
        p_principal_id,
        NULL,
        NULL,
        jsonb_build_object('display_name', p_display_name)
    );

    RETURN QUERY SELECT
        p_principal_id,
        p_organization_id,
        p_issuer,
        p_subject,
        p_display_name,
        v_created_at,
        NULL::timestamptz;
END
$$;
REVOKE ALL ON FUNCTION vela_create_human_member(uuid, uuid, text, text, text) FROM PUBLIC;
ALTER FUNCTION vela_create_human_member(uuid, uuid, text, text, text) OWNER TO vela_internal;

CREATE FUNCTION vela_disable_human_member(
    p_organization_id uuid,
    p_target_principal_id uuid
) RETURNS TABLE (
    principal_id uuid,
    organization_id uuid,
    oidc_issuer text,
    oidc_subject text,
    display_name text,
    created_at timestamptz,
    disabled_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_existing_disabled_at timestamptz;
    v_is_active_owner boolean;
    v_active_owner_count bigint;
BEGIN
    PERFORM 1
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_members:manage'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human OrganizationOwner authorization is required'
            USING ERRCODE = '42501';
    END IF;

    PERFORM 1
    FROM public.customer_organizations AS organization
    WHERE organization.id = p_organization_id
      AND organization.status = 'ACTIVE'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human member is not visible' USING ERRCODE = 'P0002';
    END IF;

    SELECT binding.disabled_at
    INTO v_existing_disabled_at
    FROM public.human_oidc_bindings AS binding
    JOIN public.principals AS principal
      ON principal.organization_id = binding.organization_id
     AND principal.id = binding.principal_id
     AND principal.kind = 'HUMAN'
    WHERE binding.organization_id = p_organization_id
      AND binding.principal_id = p_target_principal_id
    FOR UPDATE OF binding;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human member is not visible' USING ERRCODE = 'P0002';
    END IF;

    IF v_existing_disabled_at IS NOT NULL THEN
        RETURN QUERY
        SELECT
            binding.principal_id,
            binding.organization_id,
            binding.issuer,
            binding.subject,
            principal.display_name,
            binding.created_at,
            binding.disabled_at
        FROM public.human_oidc_bindings AS binding
        JOIN public.principals AS principal
          ON principal.organization_id = binding.organization_id
         AND principal.id = binding.principal_id
         AND principal.kind = 'HUMAN'
        WHERE binding.organization_id = p_organization_id
          AND binding.principal_id = p_target_principal_id;
        RETURN;
    END IF;

    SELECT EXISTS (
        SELECT 1
        FROM public.organization_role_bindings AS role_binding
        WHERE role_binding.organization_id = p_organization_id
          AND role_binding.principal_id = p_target_principal_id
          AND role_binding.role = 'OrganizationOwner'
    ) INTO v_is_active_owner;

    IF v_is_active_owner THEN
        SELECT count(*)
        INTO v_active_owner_count
        FROM public.organization_role_bindings AS role_binding
        JOIN public.human_oidc_bindings AS binding
          ON binding.organization_id = role_binding.organization_id
         AND binding.principal_id = role_binding.principal_id
         AND binding.disabled_at IS NULL
        WHERE role_binding.organization_id = p_organization_id
          AND role_binding.role = 'OrganizationOwner';
        IF v_active_owner_count <= 1 THEN
            RAISE EXCEPTION 'the last active OrganizationOwner cannot be disabled'
                USING ERRCODE = '23P01',
                    CONSTRAINT = 'organization_requires_active_owner';
        END IF;
    END IF;

    UPDATE public.human_oidc_bindings AS binding
    SET disabled_at = clock_timestamp()
    WHERE binding.organization_id = p_organization_id
      AND binding.principal_id = p_target_principal_id;

    PERFORM vela_private.record_human_identity_event(
        'HUMAN_MEMBER_DISABLED',
        p_target_principal_id,
        NULL,
        NULL,
        '{}'::jsonb
    );

    RETURN QUERY
    SELECT
        binding.principal_id,
        binding.organization_id,
        binding.issuer,
        binding.subject,
        principal.display_name,
        binding.created_at,
        binding.disabled_at
    FROM public.human_oidc_bindings AS binding
    JOIN public.principals AS principal
      ON principal.organization_id = binding.organization_id
     AND principal.id = binding.principal_id
     AND principal.kind = 'HUMAN'
    WHERE binding.organization_id = p_organization_id
      AND binding.principal_id = p_target_principal_id;
END
$$;
REVOKE ALL ON FUNCTION vela_disable_human_member(uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_disable_human_member(uuid, uuid) OWNER TO vela_internal;

CREATE FUNCTION vela_list_human_members(
    p_organization_id uuid,
    p_limit integer
) RETURNS TABLE (
    principal_id uuid,
    organization_id uuid,
    oidc_issuer text,
    oidc_subject text,
    display_name text,
    created_at timestamptz,
    disabled_at timestamptz,
    organization_roles text[]
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    PERFORM 1
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_members:read'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human OrganizationOwner authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'invalid Human member list limit' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    SELECT
        binding.principal_id,
        binding.organization_id,
        binding.issuer,
        binding.subject,
        principal.display_name,
        binding.created_at,
        binding.disabled_at,
        COALESCE((
            SELECT array_agg(role_binding.role::text ORDER BY role_binding.role::text)
            FROM public.organization_role_bindings AS role_binding
            WHERE role_binding.organization_id = binding.organization_id
              AND role_binding.principal_id = binding.principal_id
        ), ARRAY[]::text[])
    FROM public.human_oidc_bindings AS binding
    JOIN public.principals AS principal
      ON principal.organization_id = binding.organization_id
     AND principal.id = binding.principal_id
     AND principal.kind = 'HUMAN'
    WHERE binding.organization_id = p_organization_id
    ORDER BY binding.created_at, binding.principal_id
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_human_members(uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_list_human_members(uuid, integer) OWNER TO vela_internal;

CREATE FUNCTION vela_list_project_members(
    p_project_id uuid,
    p_limit integer
) RETURNS TABLE (
    principal_id uuid,
    organization_id uuid,
    project_id uuid,
    display_name text,
    disabled_at timestamptz,
    project_roles text[]
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_organization_id uuid;
BEGIN
    SELECT context.organization_id
    INTO v_organization_id
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.required_scope = 'project_members:read'
      AND context.project_id = p_project_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Project membership authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'invalid Project member list limit' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    SELECT
        binding.principal_id,
        binding.organization_id,
        binding.project_id,
        principal.display_name,
        human.disabled_at,
        array_agg(binding.role::text ORDER BY binding.role::text)::text[]
    FROM public.project_role_bindings AS binding
    JOIN public.human_oidc_bindings AS human
      ON human.organization_id = binding.organization_id
     AND human.principal_id = binding.principal_id
    JOIN public.principals AS principal
      ON principal.organization_id = binding.organization_id
     AND principal.id = binding.principal_id
     AND principal.kind = 'HUMAN'
    WHERE binding.organization_id = v_organization_id
      AND binding.project_id = p_project_id
    GROUP BY
        binding.principal_id,
        binding.organization_id,
        binding.project_id,
        principal.display_name,
        human.disabled_at
    ORDER BY min(binding.assigned_at), binding.principal_id
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_project_members(uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_list_project_members(uuid, integer) OWNER TO vela_internal;

CREATE FUNCTION vela_assign_organization_role(
    p_organization_id uuid,
    p_target_principal_id uuid,
    p_role organization_role
) RETURNS TABLE (
    organization_id uuid,
    principal_id uuid,
    organization_role organization_role,
    active boolean,
    assigned_by_principal_id uuid,
    assigned_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_principal_id uuid;
    v_inserted_count integer;
BEGIN
    SELECT context.principal_id
    INTO v_actor_principal_id
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_members:manage'
      AND context.organization_id = p_organization_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human OrganizationOwner authorization is required'
            USING ERRCODE = '42501';
    END IF;

    PERFORM 1
    FROM public.customer_organizations AS organization
    WHERE organization.id = p_organization_id
      AND organization.status = 'ACTIVE'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human member is not visible' USING ERRCODE = 'P0002';
    END IF;

    PERFORM 1
    FROM public.human_oidc_bindings AS binding
    JOIN public.principals AS principal
      ON principal.organization_id = binding.organization_id
     AND principal.id = binding.principal_id
     AND principal.kind = 'HUMAN'
    WHERE binding.organization_id = p_organization_id
      AND binding.principal_id = p_target_principal_id
      AND binding.disabled_at IS NULL
    FOR UPDATE OF binding;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human member is not visible' USING ERRCODE = 'P0002';
    END IF;

    INSERT INTO public.organization_role_bindings (
        organization_id,
        principal_id,
        role,
        assigned_by_principal_id
    ) VALUES (
        p_organization_id,
        p_target_principal_id,
        p_role,
        v_actor_principal_id
    ) ON CONFLICT ON CONSTRAINT organization_role_bindings_pkey DO NOTHING;
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;

    IF v_inserted_count = 1 THEN
        PERFORM vela_private.record_human_identity_event(
            'ORGANIZATION_ROLE_ASSIGNED',
            p_target_principal_id,
            p_role,
            NULL,
            '{}'::jsonb
        );
    END IF;

    RETURN QUERY
    SELECT
        binding.organization_id,
        binding.principal_id,
        binding.role,
        true,
        binding.assigned_by_principal_id,
        binding.assigned_at
    FROM public.organization_role_bindings AS binding
    WHERE binding.organization_id = p_organization_id
      AND binding.principal_id = p_target_principal_id
      AND binding.role = p_role;
END
$$;
REVOKE ALL ON FUNCTION vela_assign_organization_role(
    uuid, uuid, organization_role
) FROM PUBLIC;
ALTER FUNCTION vela_assign_organization_role(
    uuid, uuid, organization_role
) OWNER TO vela_internal;

CREATE FUNCTION vela_revoke_organization_role(
    p_organization_id uuid,
    p_target_principal_id uuid,
    p_role organization_role
) RETURNS TABLE (
    organization_id uuid,
    principal_id uuid,
    organization_role organization_role,
    active boolean,
    assigned_by_principal_id uuid,
    assigned_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_existing_assigned_by uuid;
    v_existing_assigned_at timestamptz;
    v_target_active boolean;
    v_active_owner_count bigint;
BEGIN
    PERFORM 1
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_members:manage'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human OrganizationOwner authorization is required'
            USING ERRCODE = '42501';
    END IF;

    PERFORM 1
    FROM public.customer_organizations AS organization
    WHERE organization.id = p_organization_id
      AND organization.status = 'ACTIVE'
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human member is not visible' USING ERRCODE = 'P0002';
    END IF;

    SELECT binding.disabled_at IS NULL
    INTO v_target_active
    FROM public.human_oidc_bindings AS binding
    WHERE binding.organization_id = p_organization_id
      AND binding.principal_id = p_target_principal_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human member is not visible' USING ERRCODE = 'P0002';
    END IF;

    SELECT binding.assigned_by_principal_id, binding.assigned_at
    INTO v_existing_assigned_by, v_existing_assigned_at
    FROM public.organization_role_bindings AS binding
    WHERE binding.organization_id = p_organization_id
      AND binding.principal_id = p_target_principal_id
      AND binding.role = p_role
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT
            p_organization_id,
            p_target_principal_id,
            p_role,
            false,
            NULL::uuid,
            NULL::timestamptz;
        RETURN;
    END IF;

    IF p_role = 'OrganizationOwner' AND v_target_active THEN
        SELECT count(*)
        INTO v_active_owner_count
        FROM public.organization_role_bindings AS role_binding
        JOIN public.human_oidc_bindings AS binding
          ON binding.organization_id = role_binding.organization_id
         AND binding.principal_id = role_binding.principal_id
         AND binding.disabled_at IS NULL
        WHERE role_binding.organization_id = p_organization_id
          AND role_binding.role = 'OrganizationOwner';
        IF v_active_owner_count <= 1 THEN
            RAISE EXCEPTION 'the last active OrganizationOwner cannot be revoked'
                USING ERRCODE = '23P01',
                    CONSTRAINT = 'organization_requires_active_owner';
        END IF;
    END IF;

    DELETE FROM public.organization_role_bindings AS binding
    WHERE binding.organization_id = p_organization_id
      AND binding.principal_id = p_target_principal_id
      AND binding.role = p_role;

    PERFORM vela_private.record_human_identity_event(
        'ORGANIZATION_ROLE_REVOKED',
        p_target_principal_id,
        p_role,
        NULL,
        '{}'::jsonb
    );

    RETURN QUERY SELECT
        p_organization_id,
        p_target_principal_id,
        p_role,
        false,
        v_existing_assigned_by,
        v_existing_assigned_at;
END
$$;
REVOKE ALL ON FUNCTION vela_revoke_organization_role(
    uuid, uuid, organization_role
) FROM PUBLIC;
ALTER FUNCTION vela_revoke_organization_role(
    uuid, uuid, organization_role
) OWNER TO vela_internal;

CREATE FUNCTION vela_assign_project_role(
    p_project_id uuid,
    p_target_principal_id uuid,
    p_role project_role
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    assigned_project_role project_role,
    active boolean,
    assigned_by_principal_id uuid,
    assigned_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_principal_id uuid;
    v_organization_id uuid;
    v_inserted_count integer;
BEGIN
    SELECT context.organization_id, context.principal_id
    INTO v_organization_id, v_actor_principal_id
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.required_scope = 'project_members:manage'
      AND context.project_id = p_project_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Project membership authorization is required'
            USING ERRCODE = '42501';
    END IF;

    PERFORM 1
    FROM public.projects AS project
    JOIN public.customer_organizations AS organization
      ON organization.id = project.organization_id
     AND organization.status = 'ACTIVE'
    WHERE project.organization_id = v_organization_id
      AND project.id = p_project_id
    FOR UPDATE OF project;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Project member is not visible' USING ERRCODE = 'P0002';
    END IF;

    PERFORM 1
    FROM public.human_oidc_bindings AS binding
    JOIN public.principals AS principal
      ON principal.organization_id = binding.organization_id
     AND principal.id = binding.principal_id
     AND principal.kind = 'HUMAN'
    WHERE binding.organization_id = v_organization_id
      AND binding.principal_id = p_target_principal_id
      AND binding.disabled_at IS NULL
    FOR UPDATE OF binding;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Project member is not visible' USING ERRCODE = 'P0002';
    END IF;

    INSERT INTO public.project_role_bindings (
        organization_id,
        project_id,
        principal_id,
        role,
        assigned_by_principal_id
    ) VALUES (
        v_organization_id,
        p_project_id,
        p_target_principal_id,
        p_role,
        v_actor_principal_id
    ) ON CONFLICT ON CONSTRAINT project_role_bindings_pkey DO NOTHING;
    GET DIAGNOSTICS v_inserted_count = ROW_COUNT;

    IF v_inserted_count = 1 THEN
        PERFORM vela_private.record_human_identity_event(
            'PROJECT_ROLE_ASSIGNED',
            p_target_principal_id,
            NULL,
            p_role,
            '{}'::jsonb
        );
    END IF;

    RETURN QUERY
    SELECT
        binding.organization_id,
        binding.project_id,
        binding.principal_id,
        binding.role,
        true,
        binding.assigned_by_principal_id,
        binding.assigned_at
    FROM public.project_role_bindings AS binding
    WHERE binding.organization_id = v_organization_id
      AND binding.project_id = p_project_id
      AND binding.principal_id = p_target_principal_id
      AND binding.role = p_role;
END
$$;
REVOKE ALL ON FUNCTION vela_assign_project_role(uuid, uuid, project_role) FROM PUBLIC;
ALTER FUNCTION vela_assign_project_role(uuid, uuid, project_role) OWNER TO vela_internal;

CREATE FUNCTION vela_revoke_project_role(
    p_project_id uuid,
    p_target_principal_id uuid,
    p_role project_role
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    assigned_project_role project_role,
    active boolean,
    assigned_by_principal_id uuid,
    assigned_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_organization_id uuid;
    v_existing_assigned_by uuid;
    v_existing_assigned_at timestamptz;
BEGIN
    SELECT context.organization_id
    INTO v_organization_id
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.required_scope = 'project_members:manage'
      AND context.project_id = p_project_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Project membership authorization is required'
            USING ERRCODE = '42501';
    END IF;

    PERFORM 1
    FROM public.projects AS project
    JOIN public.customer_organizations AS organization
      ON organization.id = project.organization_id
     AND organization.status = 'ACTIVE'
    WHERE project.organization_id = v_organization_id
      AND project.id = p_project_id
    FOR UPDATE OF project;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Project member is not visible' USING ERRCODE = 'P0002';
    END IF;

    PERFORM 1
    FROM public.human_oidc_bindings AS binding
    WHERE binding.organization_id = v_organization_id
      AND binding.principal_id = p_target_principal_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Project member is not visible' USING ERRCODE = 'P0002';
    END IF;

    SELECT binding.assigned_by_principal_id, binding.assigned_at
    INTO v_existing_assigned_by, v_existing_assigned_at
    FROM public.project_role_bindings AS binding
    WHERE binding.organization_id = v_organization_id
      AND binding.project_id = p_project_id
      AND binding.principal_id = p_target_principal_id
      AND binding.role = p_role
    FOR UPDATE;

    IF NOT FOUND THEN
        RETURN QUERY SELECT
            v_organization_id,
            p_project_id,
            p_target_principal_id,
            p_role,
            false,
            NULL::uuid,
            NULL::timestamptz;
        RETURN;
    END IF;

    DELETE FROM public.project_role_bindings AS binding
    WHERE binding.organization_id = v_organization_id
      AND binding.project_id = p_project_id
      AND binding.principal_id = p_target_principal_id
      AND binding.role = p_role;

    PERFORM vela_private.record_human_identity_event(
        'PROJECT_ROLE_REVOKED',
        p_target_principal_id,
        NULL,
        p_role,
        '{}'::jsonb
    );

    RETURN QUERY SELECT
        v_organization_id,
        p_project_id,
        p_target_principal_id,
        p_role,
        false,
        v_existing_assigned_by,
        v_existing_assigned_at;
END
$$;
REVOKE ALL ON FUNCTION vela_revoke_project_role(uuid, uuid, project_role) FROM PUBLIC;
ALTER FUNCTION vela_revoke_project_role(uuid, uuid, project_role) OWNER TO vela_internal;

GRANT USAGE ON SCHEMA public TO vela_human_membership_request;
GRANT EXECUTE ON FUNCTION vela_set_organization_identity_admin_context(uuid, bytea, text),
    vela_set_project_membership_admin_context(uuid, bytea, uuid, text),
    vela_create_human_member(uuid, uuid, text, text, text),
    vela_disable_human_member(uuid, uuid),
    vela_list_human_members(uuid, integer),
    vela_list_project_members(uuid, integer),
    vela_assign_organization_role(uuid, uuid, organization_role),
    vela_revoke_organization_role(uuid, uuid, organization_role),
    vela_assign_project_role(uuid, uuid, project_role),
    vela_revoke_project_role(uuid, uuid, project_role)
TO vela_human_membership_request;

GRANT SELECT, INSERT, UPDATE, DELETE ON human_organization_auth_sessions,
    human_administration_actor_attributions,
    human_identity_events
TO vela_internal;

REVOKE ALL ON human_organization_auth_sessions,
    human_administration_actor_attributions,
    human_identity_events
FROM PUBLIC, vela_request, vela_auth, vela_human_auth, vela_human_membership_auth,
    vela_identity_request, vela_human_membership_request,
    vela_cancel, vela_artifact_request, vela_scheduler, vela_billing,
    vela_webhook, vela_webhook_request;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM human_identity_events) THEN
        RAISE EXCEPTION 'cannot remove Human membership administration with durable evidence'
            USING ERRCODE = '55000',
                CONSTRAINT = 'human_membership_administration_contract_requires_empty_evidence';
    END IF;
END
$$;

REVOKE ALL ON FUNCTION vela_set_organization_identity_admin_context(uuid, bytea, text),
    vela_set_project_membership_admin_context(uuid, bytea, uuid, text),
    vela_create_human_member(uuid, uuid, text, text, text),
    vela_disable_human_member(uuid, uuid),
    vela_list_human_members(uuid, integer),
    vela_list_project_members(uuid, integer),
    vela_assign_organization_role(uuid, uuid, organization_role),
    vela_revoke_organization_role(uuid, uuid, organization_role),
    vela_assign_project_role(uuid, uuid, project_role),
    vela_revoke_project_role(uuid, uuid, project_role)
FROM vela_human_membership_request;
REVOKE ALL ON FUNCTION vela_authenticate_human_organization_oidc(
    text, text, bytea, timestamptz
) FROM vela_human_membership_auth;
REVOKE USAGE ON SCHEMA public FROM vela_human_membership_auth,
    vela_human_membership_request;

DROP FUNCTION vela_revoke_organization_role(uuid, uuid, organization_role);
DROP FUNCTION vela_assign_organization_role(uuid, uuid, organization_role);
DROP FUNCTION vela_revoke_project_role(uuid, uuid, project_role);
DROP FUNCTION vela_assign_project_role(uuid, uuid, project_role);
DROP FUNCTION vela_list_project_members(uuid, integer);
DROP FUNCTION vela_list_human_members(uuid, integer);
DROP FUNCTION vela_disable_human_member(uuid, uuid);
DROP FUNCTION vela_create_human_member(uuid, uuid, text, text, text);
DROP FUNCTION vela_private.record_human_identity_event(
    human_identity_admin_action, uuid, organization_role, project_role, jsonb
);
DROP FUNCTION vela_set_organization_identity_admin_context(uuid, bytea, text);
DROP FUNCTION vela_set_project_membership_admin_context(uuid, bytea, uuid, text);
DROP FUNCTION vela_private.establish_human_administration_context(
    uuid, human_administration_context_kind, text, uuid, uuid, uuid, timestamptz
);
DROP FUNCTION vela_authenticate_human_organization_oidc(text, text, bytea, timestamptz);
DROP TRIGGER human_identity_events_immutable ON human_identity_events;
DROP TRIGGER human_administration_actor_attributions_immutable
    ON human_administration_actor_attributions;
DROP FUNCTION vela_reject_human_administration_evidence_mutation();
DROP TRIGGER human_oidc_bindings_disable_permanent ON human_oidc_bindings;
DROP FUNCTION vela_reject_human_member_reenable();
DROP TABLE human_identity_events;
DROP TABLE human_administration_actor_attributions;
DROP TABLE vela_private.human_administration_contexts;
DROP TABLE human_organization_auth_sessions;
DROP FUNCTION vela_organization_role_scopes(organization_role);
CREATE OR REPLACE FUNCTION vela_project_role_scopes(p_role project_role) RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE p_role
        WHEN 'ProjectAdmin'::public.project_role THEN
            ARRAY[
                'service_principals:manage',
                'service_principals:read',
                'webhooks:manage',
                'webhooks:read'
            ]::text[]
        WHEN 'Developer'::public.project_role THEN
            ARRAY['artifacts:read', 'jobs:cancel', 'jobs:read', 'jobs:submit']::text[]
        WHEN 'ProjectViewer'::public.project_role THEN
            ARRAY['artifacts:read', 'jobs:read']::text[]
    END
$$;
REVOKE ALL ON FUNCTION vela_project_role_scopes(project_role) FROM PUBLIC;
ALTER FUNCTION vela_project_role_scopes(project_role) OWNER TO vela_internal;
DROP TYPE human_identity_admin_action;
DROP TYPE human_administration_context_kind;
-- +goose StatementEnd
