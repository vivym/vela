-- +goose Up
-- +goose StatementBegin
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

ALTER TABLE service_principals
    ADD COLUMN disabled_at timestamptz,
    ADD CONSTRAINT service_principals_disabled_after_creation
        CHECK (disabled_at IS NULL OR disabled_at >= created_at);

ALTER TABLE project_actor_session_attributions
    ADD CONSTRAINT project_actor_session_attributions_actor_identity_key
        UNIQUE (organization_id, project_id, actor_session_id, principal_id);

CREATE TYPE identity_admin_action AS ENUM (
    'SERVICE_PRINCIPAL_CREATED',
    'SERVICE_PRINCIPAL_DISABLED',
    'CREDENTIAL_ISSUED',
    'CREDENTIAL_REVOKED'
);

CREATE TABLE project_identity_events (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    actor_principal_id uuid NOT NULL,
    actor_session_id uuid NOT NULL,
    action identity_admin_action NOT NULL,
    target_principal_id uuid NOT NULL,
    credential_id uuid,
    details jsonb NOT NULL CHECK (jsonb_typeof(details) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (
        organization_id, project_id, actor_session_id, actor_principal_id
    ) REFERENCES project_actor_session_attributions(
        organization_id, project_id, actor_session_id, principal_id
    ),
    FOREIGN KEY (organization_id, project_id, target_principal_id)
        REFERENCES project_principal_attributions(organization_id, project_id, principal_id),
    FOREIGN KEY (organization_id, project_id, credential_id)
        REFERENCES credentials(organization_id, project_id, id)
);

ALTER TABLE project_identity_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_identity_events FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_reject_identity_admin_event_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'identity administration evidence is immutable'
        USING ERRCODE = '55000', CONSTRAINT = 'identity_administration_evidence_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_identity_admin_event_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_identity_admin_event_mutation() OWNER TO vela_internal;

CREATE TRIGGER project_identity_events_immutable
BEFORE UPDATE OR DELETE ON project_identity_events
FOR EACH ROW EXECUTE FUNCTION vela_reject_identity_admin_event_mutation();

CREATE FUNCTION vela_reject_service_principal_identity_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(NEW.organization_id, NEW.project_id, NEW.principal_id)
        IS DISTINCT FROM ROW(OLD.organization_id, OLD.project_id, OLD.principal_id)
    THEN
        RAISE EXCEPTION 'Service Principal identity is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'service_principal_identity_is_immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_service_principal_identity_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_service_principal_identity_mutation() OWNER TO vela_internal;

CREATE TRIGGER service_principals_identity_immutable
BEFORE UPDATE OF organization_id, project_id, principal_id ON service_principals
FOR EACH ROW EXECUTE FUNCTION vela_reject_service_principal_identity_mutation();

CREATE FUNCTION vela_reject_service_credential_ownership_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(NEW.organization_id, NEW.project_id, NEW.principal_id)
        IS DISTINCT FROM ROW(OLD.organization_id, OLD.project_id, OLD.principal_id)
    THEN
        RAISE EXCEPTION 'Service Credential ownership is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'service_credential_ownership_is_immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_service_credential_ownership_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_service_credential_ownership_mutation() OWNER TO vela_internal;

CREATE TRIGGER service_credentials_ownership_immutable
BEFORE UPDATE OF organization_id, project_id, principal_id ON credentials
FOR EACH ROW EXECUTE FUNCTION vela_reject_service_credential_ownership_mutation();

CREATE FUNCTION vela_reject_service_principal_reenable() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.disabled_at IS NOT NULL
        AND NEW.disabled_at IS DISTINCT FROM OLD.disabled_at
    THEN
        RAISE EXCEPTION 'Service Principal disablement is permanent'
            USING ERRCODE = '55000',
                CONSTRAINT = 'service_principal_disable_is_permanent';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_service_principal_reenable() FROM PUBLIC;
ALTER FUNCTION vela_reject_service_principal_reenable() OWNER TO vela_internal;

CREATE TRIGGER service_principals_disable_permanent
BEFORE UPDATE OF disabled_at ON service_principals
FOR EACH ROW EXECUTE FUNCTION vela_reject_service_principal_reenable();

CREATE FUNCTION vela_reject_service_credential_unrevoke() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.revoked_at IS NOT NULL
        AND NEW.revoked_at IS DISTINCT FROM OLD.revoked_at
    THEN
        RAISE EXCEPTION 'Service Credential revocation is permanent'
            USING ERRCODE = '55000',
                CONSTRAINT = 'service_credential_revocation_is_permanent';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_service_credential_unrevoke() FROM PUBLIC;
ALTER FUNCTION vela_reject_service_credential_unrevoke() OWNER TO vela_internal;

CREATE TRIGGER service_credentials_revocation_permanent
BEFORE UPDATE OF revoked_at ON credentials
FOR EACH ROW EXECUTE FUNCTION vela_reject_service_credential_unrevoke();

CREATE FUNCTION vela_set_identity_admin_context(
    p_actor_session_id uuid,
    p_actor_session_proof bytea,
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
    v_actor_kind public.principal_kind;
    v_organization_id uuid;
    v_project_id uuid;
    v_principal_id uuid;
    v_transaction_time timestamptz;
BEGIN
    IF p_required_scope NOT IN (
        'service_principals:manage',
        'service_principals:read'
    ) THEN
        RAISE EXCEPTION 'invalid identity administration scope' USING ERRCODE = '22023';
    END IF;

    SELECT context.organization_id, context.project_id, context.principal_id,
        context.transaction_time
    INTO v_organization_id, v_project_id, v_principal_id, v_transaction_time
    FROM public.vela_set_request_context(
        p_actor_session_id,
        p_actor_session_proof,
        p_required_scope
    ) AS context;

    SELECT context.actor_kind
    INTO v_actor_kind
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.credential_id = p_actor_session_id;

    IF NOT FOUND OR v_actor_kind IS DISTINCT FROM 'HUMAN' THEN
        RAISE EXCEPTION 'Human ProjectAdmin authorization is required'
            USING ERRCODE = '42501';
    END IF;

    IF p_required_scope = 'service_principals:manage' THEN
        INSERT INTO public.project_actor_session_attributions (
            organization_id,
            project_id,
            actor_session_id,
            principal_id,
            actor_kind,
            first_attributed_at
        ) VALUES (
            v_organization_id,
            v_project_id,
            p_actor_session_id,
            v_principal_id,
            'HUMAN',
            clock_timestamp()
        )
        ON CONFLICT ON CONSTRAINT project_actor_session_attributions_pkey DO NOTHING;
    END IF;

    RETURN QUERY SELECT
        v_organization_id,
        v_project_id,
        v_principal_id,
        v_transaction_time;
END
$$;
REVOKE ALL ON FUNCTION vela_set_identity_admin_context(uuid, bytea, text) FROM PUBLIC;
ALTER FUNCTION vela_set_identity_admin_context(uuid, bytea, text) OWNER TO vela_internal;

CREATE FUNCTION vela_create_service_principal(
    p_principal_id uuid,
    p_project_id uuid,
    p_display_name text
) RETURNS TABLE (
    principal_id uuid,
    organization_id uuid,
    project_id uuid,
    display_name text,
    created_at timestamptz,
    disabled_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_principal_id uuid;
    v_actor_session_id uuid;
    v_organization_id uuid;
    v_created_at timestamptz;
BEGIN
    SELECT context.organization_id, context.principal_id, context.credential_id
    INTO v_organization_id, v_actor_principal_id, v_actor_session_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.required_scope = 'service_principals:manage'
      AND context.actor_kind = 'HUMAN'
      AND context.project_id = p_project_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human ProjectAdmin authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_principal_id IS NULL OR p_project_id IS NULL OR
        length(p_display_name) NOT BETWEEN 1 AND 200
    THEN
        RAISE EXCEPTION 'invalid Service Principal definition' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.principals (
        id, organization_id, kind, display_name
    ) VALUES (
        p_principal_id, v_organization_id, 'SERVICE', p_display_name
    ) RETURNING principals.created_at INTO v_created_at;

    INSERT INTO public.service_principals (
        principal_id, organization_id, project_id, created_at
    ) VALUES (
        p_principal_id, v_organization_id, p_project_id, v_created_at
    );

    INSERT INTO public.project_identity_events (
        id,
        organization_id,
        project_id,
        actor_principal_id,
        actor_session_id,
        action,
        target_principal_id,
        details
    ) VALUES (
        gen_random_uuid(),
        v_organization_id,
        p_project_id,
        v_actor_principal_id,
        v_actor_session_id,
        'SERVICE_PRINCIPAL_CREATED',
        p_principal_id,
        jsonb_build_object('display_name', p_display_name)
    );

    RETURN QUERY SELECT
        p_principal_id,
        v_organization_id,
        p_project_id,
        p_display_name,
        v_created_at,
        NULL::timestamptz;
END
$$;
REVOKE ALL ON FUNCTION vela_create_service_principal(uuid, uuid, text) FROM PUBLIC;
ALTER FUNCTION vela_create_service_principal(uuid, uuid, text) OWNER TO vela_internal;

CREATE FUNCTION vela_list_service_principals(
    p_project_id uuid,
    p_limit integer
) RETURNS TABLE (
    principal_id uuid,
    organization_id uuid,
    project_id uuid,
    display_name text,
    created_at timestamptz,
    disabled_at timestamptz
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
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.required_scope = 'service_principals:read'
      AND context.actor_kind = 'HUMAN'
      AND context.project_id = p_project_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human ProjectAdmin authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'invalid Service Principal list limit' USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    SELECT
        service.principal_id,
        service.organization_id,
        service.project_id,
        principal.display_name,
        service.created_at,
        service.disabled_at
    FROM public.service_principals AS service
    JOIN public.principals AS principal
      ON principal.organization_id = service.organization_id
     AND principal.id = service.principal_id
     AND principal.kind = 'SERVICE'
    WHERE service.organization_id = v_organization_id
      AND service.project_id = p_project_id
    ORDER BY service.created_at, service.principal_id
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_service_principals(uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_list_service_principals(uuid, integer) OWNER TO vela_internal;

CREATE FUNCTION vela_service_credential_scopes_valid(p_scopes text[]) RETURNS boolean
LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog
AS $$
    SELECT p_scopes IS NOT NULL
       AND cardinality(p_scopes) BETWEEN 1 AND 6
       AND NOT EXISTS (
           SELECT 1
           FROM unnest(p_scopes) AS scope
           WHERE scope IS NULL OR scope NOT IN (
               'jobs:submit',
               'jobs:read',
               'jobs:cancel',
               'artifacts:read',
               'webhooks:manage',
               'webhooks:read'
           )
       )
       AND cardinality(p_scopes) = (
           SELECT count(DISTINCT scope)::integer FROM unnest(p_scopes) AS scope
       )
$$;
REVOKE ALL ON FUNCTION vela_service_credential_scopes_valid(text[]) FROM PUBLIC;
ALTER FUNCTION vela_service_credential_scopes_valid(text[]) OWNER TO vela_internal;

CREATE FUNCTION vela_issue_service_credential(
    p_credential_id uuid,
    p_project_id uuid,
    p_principal_id uuid,
    p_secret_digest bytea,
    p_scopes text[],
    p_expires_at timestamptz
) RETURNS TABLE (
    credential_id uuid,
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    scopes text[],
    expires_at timestamptz,
    created_at timestamptz,
    revoked_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_principal_id uuid;
    v_actor_session_id uuid;
    v_organization_id uuid;
    v_created_at timestamptz;
    v_scopes text[];
BEGIN
    SELECT context.organization_id, context.principal_id, context.credential_id
    INTO v_organization_id, v_actor_principal_id, v_actor_session_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.required_scope = 'service_principals:manage'
      AND context.actor_kind = 'HUMAN'
      AND context.project_id = p_project_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human ProjectAdmin authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_credential_id IS NULL OR p_principal_id IS NULL
        OR octet_length(p_secret_digest) IS DISTINCT FROM 32
        OR NOT public.vela_service_credential_scopes_valid(p_scopes)
        OR p_expires_at IS NULL
        OR p_expires_at <= clock_timestamp()
        OR p_expires_at > clock_timestamp() + interval '366 days'
    THEN
        RAISE EXCEPTION 'invalid Service Credential definition' USING ERRCODE = '22023';
    END IF;

    PERFORM 1
    FROM public.service_principals AS service
    WHERE service.organization_id = v_organization_id
      AND service.project_id = p_project_id
      AND service.principal_id = p_principal_id
      AND service.disabled_at IS NULL
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Service Principal is not active or visible'
            USING ERRCODE = 'P0002';
    END IF;

    SELECT array_agg(scope ORDER BY scope)
    INTO v_scopes
    FROM unnest(p_scopes) AS scope;

    INSERT INTO public.credentials (
        id,
        organization_id,
        project_id,
        principal_id,
        secret_digest,
        scopes,
        expires_at,
        created_by_principal_id
    ) VALUES (
        p_credential_id,
        v_organization_id,
        p_project_id,
        p_principal_id,
        p_secret_digest,
        v_scopes,
        p_expires_at,
        v_actor_principal_id
    ) RETURNING credentials.created_at INTO v_created_at;

    INSERT INTO public.project_identity_events (
        id,
        organization_id,
        project_id,
        actor_principal_id,
        actor_session_id,
        action,
        target_principal_id,
        credential_id,
        details
    ) VALUES (
        gen_random_uuid(),
        v_organization_id,
        p_project_id,
        v_actor_principal_id,
        v_actor_session_id,
        'CREDENTIAL_ISSUED',
        p_principal_id,
        p_credential_id,
        jsonb_build_object('scopes', to_jsonb(v_scopes), 'expires_at', p_expires_at)
    );

    RETURN QUERY SELECT
        p_credential_id,
        v_organization_id,
        p_project_id,
        p_principal_id,
        v_scopes,
        p_expires_at,
        v_created_at,
        NULL::timestamptz;
END
$$;
REVOKE ALL ON FUNCTION vela_issue_service_credential(
    uuid, uuid, uuid, bytea, text[], timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_issue_service_credential(
    uuid, uuid, uuid, bytea, text[], timestamptz
) OWNER TO vela_internal;

CREATE FUNCTION vela_list_service_credentials(
    p_project_id uuid,
    p_principal_id uuid,
    p_limit integer
) RETURNS TABLE (
    credential_id uuid,
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    scopes text[],
    expires_at timestamptz,
    created_at timestamptz,
    revoked_at timestamptz
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
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.required_scope = 'service_principals:read'
      AND context.actor_kind = 'HUMAN'
      AND context.project_id = p_project_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human ProjectAdmin authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_principal_id IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'invalid Service Credential list request' USING ERRCODE = '22023';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM public.service_principals AS service
        WHERE service.organization_id = v_organization_id
          AND service.project_id = p_project_id
          AND service.principal_id = p_principal_id
    ) THEN
        RAISE EXCEPTION 'Service Principal is not visible' USING ERRCODE = 'P0002';
    END IF;

    RETURN QUERY
    SELECT
        credential.id,
        credential.organization_id,
        credential.project_id,
        credential.principal_id,
        credential.scopes,
        credential.expires_at,
        credential.created_at,
        credential.revoked_at
    FROM public.credentials AS credential
    WHERE credential.organization_id = v_organization_id
      AND credential.project_id = p_project_id
      AND credential.principal_id = p_principal_id
    ORDER BY credential.created_at, credential.id
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_service_credentials(uuid, uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_list_service_credentials(uuid, uuid, integer) OWNER TO vela_internal;

CREATE FUNCTION vela_revoke_service_credential(
    p_project_id uuid,
    p_principal_id uuid,
    p_credential_id uuid
) RETURNS TABLE (
    credential_id uuid,
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    scopes text[],
    expires_at timestamptz,
    created_at timestamptz,
    revoked_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_principal_id uuid;
    v_actor_session_id uuid;
    v_organization_id uuid;
    v_revoked_at timestamptz;
BEGIN
    SELECT context.organization_id, context.principal_id, context.credential_id
    INTO v_organization_id, v_actor_principal_id, v_actor_session_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.required_scope = 'service_principals:manage'
      AND context.actor_kind = 'HUMAN'
      AND context.project_id = p_project_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human ProjectAdmin authorization is required'
            USING ERRCODE = '42501';
    END IF;

    SELECT credential.revoked_at
    INTO v_revoked_at
    FROM public.credentials AS credential
    WHERE credential.organization_id = v_organization_id
      AND credential.project_id = p_project_id
      AND credential.principal_id = p_principal_id
      AND credential.id = p_credential_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Service Credential is not visible' USING ERRCODE = 'P0002';
    END IF;

    IF v_revoked_at IS NULL THEN
        v_revoked_at := clock_timestamp();
        UPDATE public.credentials AS credential
        SET revoked_at = v_revoked_at
        WHERE credential.organization_id = v_organization_id
          AND credential.project_id = p_project_id
          AND credential.principal_id = p_principal_id
          AND credential.id = p_credential_id;

        INSERT INTO public.project_identity_events (
            id,
            organization_id,
            project_id,
            actor_principal_id,
            actor_session_id,
            action,
            target_principal_id,
            credential_id,
            details
        ) VALUES (
            gen_random_uuid(),
            v_organization_id,
            p_project_id,
            v_actor_principal_id,
            v_actor_session_id,
            'CREDENTIAL_REVOKED',
            p_principal_id,
            p_credential_id,
            jsonb_build_object('revoked_at', v_revoked_at)
        );
    END IF;

    RETURN QUERY
    SELECT
        credential.id,
        credential.organization_id,
        credential.project_id,
        credential.principal_id,
        credential.scopes,
        credential.expires_at,
        credential.created_at,
        credential.revoked_at
    FROM public.credentials AS credential
    WHERE credential.organization_id = v_organization_id
      AND credential.project_id = p_project_id
      AND credential.principal_id = p_principal_id
      AND credential.id = p_credential_id;
END
$$;
REVOKE ALL ON FUNCTION vela_revoke_service_credential(uuid, uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_revoke_service_credential(uuid, uuid, uuid) OWNER TO vela_internal;

CREATE FUNCTION vela_disable_service_principal(
    p_project_id uuid,
    p_principal_id uuid
) RETURNS TABLE (
    principal_id uuid,
    organization_id uuid,
    project_id uuid,
    display_name text,
    created_at timestamptz,
    disabled_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_principal_id uuid;
    v_actor_session_id uuid;
    v_organization_id uuid;
    v_disabled_at timestamptz;
    v_revoked_credentials integer := 0;
BEGIN
    SELECT context.organization_id, context.principal_id, context.credential_id
    INTO v_organization_id, v_actor_principal_id, v_actor_session_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.required_scope = 'service_principals:manage'
      AND context.actor_kind = 'HUMAN'
      AND context.project_id = p_project_id;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human ProjectAdmin authorization is required'
            USING ERRCODE = '42501';
    END IF;

    SELECT service.disabled_at
    INTO v_disabled_at
    FROM public.service_principals AS service
    WHERE service.organization_id = v_organization_id
      AND service.project_id = p_project_id
      AND service.principal_id = p_principal_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Service Principal is not visible' USING ERRCODE = 'P0002';
    END IF;

    IF v_disabled_at IS NULL THEN
        v_disabled_at := clock_timestamp();
        UPDATE public.service_principals AS service
        SET disabled_at = v_disabled_at
        WHERE service.organization_id = v_organization_id
          AND service.project_id = p_project_id
          AND service.principal_id = p_principal_id;

        UPDATE public.credentials AS credential
        SET revoked_at = v_disabled_at
        WHERE credential.organization_id = v_organization_id
          AND credential.project_id = p_project_id
          AND credential.principal_id = p_principal_id
          AND credential.revoked_at IS NULL;
        GET DIAGNOSTICS v_revoked_credentials = ROW_COUNT;

        INSERT INTO public.project_identity_events (
            id,
            organization_id,
            project_id,
            actor_principal_id,
            actor_session_id,
            action,
            target_principal_id,
            details
        ) VALUES (
            gen_random_uuid(),
            v_organization_id,
            p_project_id,
            v_actor_principal_id,
            v_actor_session_id,
            'SERVICE_PRINCIPAL_DISABLED',
            p_principal_id,
            jsonb_build_object(
                'disabled_at', v_disabled_at,
                'revoked_credentials', v_revoked_credentials
            )
        );
    END IF;

    RETURN QUERY
    SELECT
        service.principal_id,
        service.organization_id,
        service.project_id,
        principal.display_name,
        service.created_at,
        service.disabled_at
    FROM public.service_principals AS service
    JOIN public.principals AS principal
      ON principal.organization_id = service.organization_id
     AND principal.id = service.principal_id
     AND principal.kind = 'SERVICE'
    WHERE service.organization_id = v_organization_id
      AND service.project_id = p_project_id
      AND service.principal_id = p_principal_id;
END
$$;
REVOKE ALL ON FUNCTION vela_disable_service_principal(uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_disable_service_principal(uuid, uuid) OWNER TO vela_internal;

CREATE OR REPLACE FUNCTION vela_authenticate_service_credential(p_credential_id uuid)
RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    secret_digest bytea,
    scopes text[],
    expires_at timestamptz,
    revoked_at timestamptz
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        credential.organization_id,
        credential.project_id,
        credential.principal_id,
        credential.secret_digest,
        credential.scopes,
        credential.expires_at,
        credential.revoked_at
    FROM public.credentials AS credential
    JOIN public.service_principals AS service
      ON service.organization_id = credential.organization_id
     AND service.project_id = credential.project_id
     AND service.principal_id = credential.principal_id
     AND service.disabled_at IS NULL
    WHERE credential.id = p_credential_id
      AND credential.revoked_at IS NULL
      AND credential.expires_at > clock_timestamp()
$$;
REVOKE ALL ON FUNCTION vela_authenticate_service_credential(uuid) FROM PUBLIC;
ALTER FUNCTION vela_authenticate_service_credential(uuid) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_authenticate_service_credential(uuid) TO vela_auth;

GRANT USAGE ON SCHEMA public TO vela_identity_request;
GRANT EXECUTE ON FUNCTION vela_set_identity_admin_context(uuid, bytea, text),
    vela_create_service_principal(uuid, uuid, text),
    vela_list_service_principals(uuid, integer),
    vela_issue_service_credential(uuid, uuid, uuid, bytea, text[], timestamptz),
    vela_list_service_credentials(uuid, uuid, integer),
    vela_revoke_service_credential(uuid, uuid, uuid),
    vela_disable_service_principal(uuid, uuid)
    TO vela_identity_request;

REVOKE ALL ON project_identity_events
    FROM PUBLIC, vela_auth, vela_human_auth, vela_identity_request, vela_request,
    vela_artifact_request, vela_cancel, vela_webhook_request, vela_webhook;
GRANT SELECT, INSERT, UPDATE, DELETE ON project_identity_events TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE project_identity_events, service_principals, credentials,
    project_actor_session_attributions
    IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM project_identity_events)
    THEN
        RAISE EXCEPTION 'cannot remove Service Principal administration with durable evidence'
            USING ERRCODE = '55000',
                CONSTRAINT = 'identity_administration_contract_requires_empty_evidence';
    END IF;
END
$$;

REVOKE ALL ON FUNCTION vela_set_identity_admin_context(uuid, bytea, text),
    vela_create_service_principal(uuid, uuid, text),
    vela_list_service_principals(uuid, integer),
    vela_issue_service_credential(uuid, uuid, uuid, bytea, text[], timestamptz),
    vela_list_service_credentials(uuid, uuid, integer),
    vela_revoke_service_credential(uuid, uuid, uuid),
    vela_disable_service_principal(uuid, uuid)
    FROM vela_identity_request;
REVOKE USAGE ON SCHEMA public FROM vela_identity_request;

CREATE OR REPLACE FUNCTION vela_authenticate_service_credential(p_credential_id uuid)
RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    secret_digest bytea,
    scopes text[],
    expires_at timestamptz,
    revoked_at timestamptz
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        credential.organization_id,
        credential.project_id,
        credential.principal_id,
        credential.secret_digest,
        credential.scopes,
        credential.expires_at,
        credential.revoked_at
    FROM public.credentials AS credential
    WHERE credential.id = p_credential_id
      AND credential.revoked_at IS NULL
      AND credential.expires_at > clock_timestamp()
$$;
REVOKE ALL ON FUNCTION vela_authenticate_service_credential(uuid) FROM PUBLIC;
ALTER FUNCTION vela_authenticate_service_credential(uuid) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_authenticate_service_credential(uuid) TO vela_auth;

DROP FUNCTION vela_list_service_credentials(uuid, uuid, integer);
DROP FUNCTION vela_revoke_service_credential(uuid, uuid, uuid);
DROP FUNCTION vela_disable_service_principal(uuid, uuid);
DROP FUNCTION vela_issue_service_credential(uuid, uuid, uuid, bytea, text[], timestamptz);
DROP FUNCTION vela_service_credential_scopes_valid(text[]);
DROP FUNCTION vela_list_service_principals(uuid, integer);
DROP FUNCTION vela_create_service_principal(uuid, uuid, text);
DROP FUNCTION vela_set_identity_admin_context(uuid, bytea, text);
DROP TRIGGER service_credentials_revocation_permanent ON credentials;
DROP FUNCTION vela_reject_service_credential_unrevoke();
DROP TRIGGER service_credentials_ownership_immutable ON credentials;
DROP FUNCTION vela_reject_service_credential_ownership_mutation();
DROP TRIGGER service_principals_disable_permanent ON service_principals;
DROP FUNCTION vela_reject_service_principal_reenable();
DROP TRIGGER service_principals_identity_immutable ON service_principals;
DROP FUNCTION vela_reject_service_principal_identity_mutation();
DROP TRIGGER project_identity_events_immutable ON project_identity_events;
DROP FUNCTION vela_reject_identity_admin_event_mutation();
DROP TABLE project_identity_events;
DROP TYPE identity_admin_action;

ALTER TABLE project_actor_session_attributions
    DROP CONSTRAINT project_actor_session_attributions_actor_identity_key;
ALTER TABLE service_principals
    DROP CONSTRAINT service_principals_disabled_after_creation,
    DROP COLUMN disabled_at;

CREATE OR REPLACE FUNCTION vela_project_role_scopes(p_role project_role) RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE p_role
        WHEN 'ProjectAdmin'::public.project_role THEN
            ARRAY['webhooks:manage', 'webhooks:read']::text[]
        WHEN 'Developer'::public.project_role THEN
            ARRAY['artifacts:read', 'jobs:cancel', 'jobs:read', 'jobs:submit']::text[]
        WHEN 'ProjectViewer'::public.project_role THEN
            ARRAY['artifacts:read', 'jobs:read']::text[]
    END
$$;
REVOKE ALL ON FUNCTION vela_project_role_scopes(project_role) FROM PUBLIC;
ALTER FUNCTION vela_project_role_scopes(project_role) OWNER TO vela_internal;
-- +goose StatementEnd
