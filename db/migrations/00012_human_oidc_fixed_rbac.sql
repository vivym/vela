-- +goose Up
-- +goose StatementBegin
CREATE TYPE organization_role AS ENUM (
    'OrganizationOwner',
    'BillingAdmin',
    'OrganizationAuditor'
);

CREATE TYPE project_role AS ENUM (
    'ProjectAdmin',
    'Developer',
    'ProjectViewer'
);

CREATE TABLE human_oidc_bindings (
    principal_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    principal_kind principal_kind NOT NULL DEFAULT 'HUMAN'
        CHECK (principal_kind = 'HUMAN'),
    issuer text NOT NULL CHECK (
        octet_length(issuer) BETWEEN 1 AND 2048
        AND issuer ~ '^https://[^/?#]+(?:/[^?#]*)?$'
    ),
    subject text NOT NULL CHECK (octet_length(subject) BETWEEN 1 AND 500),
    display_name text CHECK (
        display_name IS NULL OR length(display_name) BETWEEN 1 AND 200
    ),
    disabled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (issuer, subject),
    UNIQUE (organization_id, principal_id),
    FOREIGN KEY (organization_id, principal_id, principal_kind)
        REFERENCES principals(organization_id, id, kind)
);

CREATE TABLE organization_role_bindings (
    organization_id uuid NOT NULL,
    principal_id uuid NOT NULL,
    role organization_role NOT NULL,
    assigned_by_principal_id uuid NOT NULL,
    assigned_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (organization_id, principal_id, role),
    FOREIGN KEY (organization_id, principal_id)
        REFERENCES human_oidc_bindings(organization_id, principal_id),
    FOREIGN KEY (organization_id, assigned_by_principal_id)
        REFERENCES principals(organization_id, id)
);

CREATE TABLE project_role_bindings (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    principal_id uuid NOT NULL,
    role project_role NOT NULL,
    assigned_by_principal_id uuid NOT NULL,
    assigned_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (organization_id, project_id, principal_id, role),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, principal_id)
        REFERENCES human_oidc_bindings(organization_id, principal_id),
    FOREIGN KEY (organization_id, assigned_by_principal_id)
        REFERENCES principals(organization_id, id)
);

CREATE TABLE human_auth_sessions (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    principal_id uuid NOT NULL,
    token_proof bytea NOT NULL CHECK (octet_length(token_proof) = 32),
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    CONSTRAINT human_auth_sessions_token_proof_key
        UNIQUE (organization_id, project_id, principal_id, token_proof),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, principal_id)
        REFERENCES human_oidc_bindings(organization_id, principal_id),
    CHECK (expires_at > created_at)
);

CREATE INDEX human_auth_sessions_expiry_idx ON human_auth_sessions (expires_at, id);
CREATE INDEX human_auth_sessions_principal_idx
    ON human_auth_sessions (organization_id, principal_id, expires_at);

CREATE FUNCTION vela_enforce_human_oidc_binding_immutability() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
        AND OLD.principal_id IS NOT DISTINCT FROM NEW.principal_id
        AND OLD.organization_id IS NOT DISTINCT FROM NEW.organization_id
        AND OLD.principal_kind IS NOT DISTINCT FROM NEW.principal_kind
        AND OLD.issuer IS NOT DISTINCT FROM NEW.issuer
        AND OLD.subject IS NOT DISTINCT FROM NEW.subject
        AND OLD.created_at IS NOT DISTINCT FROM NEW.created_at
    THEN
        RETURN NEW;
    END IF;

    RAISE EXCEPTION 'Human identity evidence is immutable'
        USING ERRCODE = '55000', CONSTRAINT = 'human_identity_evidence_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_human_oidc_binding_immutability() FROM PUBLIC;
ALTER FUNCTION vela_enforce_human_oidc_binding_immutability() OWNER TO vela_internal;

CREATE FUNCTION vela_reject_human_identity_evidence_update() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Human identity evidence is immutable'
        USING ERRCODE = '55000', CONSTRAINT = 'human_identity_evidence_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_human_identity_evidence_update() FROM PUBLIC;
ALTER FUNCTION vela_reject_human_identity_evidence_update() OWNER TO vela_internal;

CREATE TRIGGER human_oidc_binding_identity_immutable
BEFORE UPDATE OR DELETE ON human_oidc_bindings
FOR EACH ROW EXECUTE FUNCTION vela_enforce_human_oidc_binding_immutability();

CREATE TRIGGER organization_role_binding_identity_immutable
BEFORE UPDATE ON organization_role_bindings
FOR EACH ROW EXECUTE FUNCTION vela_reject_human_identity_evidence_update();

CREATE TRIGGER project_role_binding_identity_immutable
BEFORE UPDATE ON project_role_bindings
FOR EACH ROW EXECUTE FUNCTION vela_reject_human_identity_evidence_update();

CREATE TRIGGER human_auth_session_identity_immutable
BEFORE UPDATE ON human_auth_sessions
FOR EACH ROW EXECUTE FUNCTION vela_reject_human_identity_evidence_update();

CREATE TABLE project_principal_attributions (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    principal_id uuid NOT NULL,
    principal_kind principal_kind NOT NULL,
    first_attributed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (organization_id, project_id, principal_id),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, principal_id, principal_kind)
        REFERENCES principals(organization_id, id, kind)
);

INSERT INTO project_principal_attributions (
    organization_id,
    project_id,
    principal_id,
    principal_kind,
    first_attributed_at
)
SELECT
    service.organization_id,
    service.project_id,
    service.principal_id,
    'SERVICE',
    service.created_at
FROM service_principals AS service;

CREATE TABLE project_actor_session_attributions (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    actor_session_id uuid NOT NULL,
    principal_id uuid NOT NULL,
    actor_kind principal_kind NOT NULL,
    first_attributed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (organization_id, project_id, actor_session_id),
    FOREIGN KEY (organization_id, project_id, principal_id)
        REFERENCES project_principal_attributions(organization_id, project_id, principal_id),
    FOREIGN KEY (organization_id, principal_id, actor_kind)
        REFERENCES principals(organization_id, id, kind)
);

ALTER TABLE human_oidc_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE human_oidc_bindings FORCE ROW LEVEL SECURITY;
ALTER TABLE organization_role_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE organization_role_bindings FORCE ROW LEVEL SECURITY;
ALTER TABLE project_role_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_role_bindings FORCE ROW LEVEL SECURITY;
ALTER TABLE human_auth_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE human_auth_sessions FORCE ROW LEVEL SECURITY;
ALTER TABLE project_principal_attributions ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_principal_attributions FORCE ROW LEVEL SECURITY;
ALTER TABLE project_actor_session_attributions ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_actor_session_attributions FORCE ROW LEVEL SECURITY;

INSERT INTO project_actor_session_attributions (
    organization_id,
    project_id,
    actor_session_id,
    principal_id,
    actor_kind,
    first_attributed_at
)
SELECT
    credential.organization_id,
    credential.project_id,
    credential.id,
    credential.principal_id,
    'SERVICE',
    credential.created_at
FROM credentials AS credential;

CREATE FUNCTION vela_record_service_actor_session_attribution() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    INSERT INTO public.project_actor_session_attributions (
        organization_id,
        project_id,
        actor_session_id,
        principal_id,
        actor_kind,
        first_attributed_at
    ) VALUES (
        NEW.organization_id,
        NEW.project_id,
        NEW.id,
        NEW.principal_id,
        'SERVICE',
        NEW.created_at
    );
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_record_service_actor_session_attribution() FROM PUBLIC;
ALTER FUNCTION vela_record_service_actor_session_attribution() OWNER TO vela_internal;

CREATE TRIGGER credentials_record_project_actor_session
AFTER INSERT ON credentials
FOR EACH ROW EXECUTE FUNCTION vela_record_service_actor_session_attribution();

CREATE FUNCTION vela_record_project_principal_attribution() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_kind public.principal_kind;
    v_attributed_at timestamptz;
BEGIN
    IF TG_TABLE_NAME = 'service_principals' THEN
        v_kind := 'SERVICE';
        v_attributed_at := NEW.created_at;
    ELSIF TG_TABLE_NAME = 'project_role_bindings' THEN
        v_kind := 'HUMAN';
        v_attributed_at := NEW.assigned_at;
    ELSE
        RAISE EXCEPTION 'unsupported Project Principal attribution source';
    END IF;

    INSERT INTO public.project_principal_attributions (
        organization_id,
        project_id,
        principal_id,
        principal_kind,
        first_attributed_at
    ) VALUES (
        NEW.organization_id,
        NEW.project_id,
        NEW.principal_id,
        v_kind,
        v_attributed_at
    )
    ON CONFLICT (organization_id, project_id, principal_id) DO NOTHING;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_record_project_principal_attribution() FROM PUBLIC;
ALTER FUNCTION vela_record_project_principal_attribution() OWNER TO vela_internal;

CREATE TRIGGER service_principals_record_project_attribution
AFTER INSERT ON service_principals
FOR EACH ROW EXECUTE FUNCTION vela_record_project_principal_attribution();

CREATE TRIGGER project_roles_record_project_attribution
AFTER INSERT ON project_role_bindings
FOR EACH ROW EXECUTE FUNCTION vela_record_project_principal_attribution();

CREATE FUNCTION vela_reject_project_principal_attribution_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Project Principal attribution evidence is immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_project_principal_attribution_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_project_principal_attribution_mutation() OWNER TO vela_internal;

CREATE TRIGGER project_principal_attributions_immutable
BEFORE UPDATE OR DELETE ON project_principal_attributions
FOR EACH ROW EXECUTE FUNCTION vela_reject_project_principal_attribution_mutation();

CREATE TRIGGER project_actor_session_attributions_immutable
BEFORE UPDATE OR DELETE ON project_actor_session_attributions
FOR EACH ROW EXECUTE FUNCTION vela_reject_project_principal_attribution_mutation();

ALTER TABLE jobs
    DROP CONSTRAINT jobs_organization_id_project_id_created_by_principal_id_fkey,
    ADD FOREIGN KEY (organization_id, project_id, created_by_principal_id)
        REFERENCES project_principal_attributions(organization_id, project_id, principal_id);

ALTER TABLE job_cancellation_decisions
    DROP CONSTRAINT job_cancellation_decisions_organization_id_project_id_requ_fkey,
    ADD FOREIGN KEY (organization_id, project_id, requested_by_principal_id)
        REFERENCES project_principal_attributions(organization_id, project_id, principal_id);

ALTER TABLE webhook_subscriptions
    DROP CONSTRAINT webhook_subscriptions_organization_id_project_id_created_b_fkey,
    ADD FOREIGN KEY (organization_id, project_id, created_by_credential_id)
        REFERENCES project_actor_session_attributions(
            organization_id,
            project_id,
            actor_session_id
        );

ALTER TABLE webhook_subscription_secrets
    DROP CONSTRAINT webhook_subscription_secrets_organization_id_project_id_cr_fkey,
    ADD FOREIGN KEY (organization_id, project_id, created_by_credential_id)
        REFERENCES project_actor_session_attributions(
            organization_id,
            project_id,
            actor_session_id
        );

ALTER TABLE webhook_delivery_replays
    DROP CONSTRAINT webhook_delivery_replays_organization_id_project_id_reques_fkey,
    ADD FOREIGN KEY (organization_id, project_id, requested_by_credential_id)
        REFERENCES project_actor_session_attributions(
            organization_id,
            project_id,
            actor_session_id
        );

CREATE FUNCTION vela_project_role_scopes(p_role project_role) RETURNS text[]
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

CREATE FUNCTION vela_authenticate_human_oidc(
    p_issuer text,
    p_subject text,
    p_token_proof bytea,
    p_token_expires_at timestamptz
) RETURNS TABLE (
    organization_id uuid,
    principal_id uuid,
    project_id uuid,
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
    v_project_id uuid;
    v_session_id uuid;
    v_scopes text[];
    v_session_expires_at timestamptz;
    v_returned boolean := false;
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

    v_session_expires_at := LEAST(
        p_token_expires_at,
        clock_timestamp() + interval '5 minutes'
    );

    DELETE FROM public.human_auth_sessions AS session
    WHERE session.organization_id = v_organization_id
      AND session.principal_id = v_principal_id
      AND session.expires_at <= clock_timestamp();

    FOR v_project_id, v_scopes IN
        SELECT
            binding.project_id,
            array_agg(DISTINCT permission ORDER BY permission)::text[]
        FROM public.project_role_bindings AS binding
        CROSS JOIN LATERAL unnest(public.vela_project_role_scopes(binding.role)) AS permission
        WHERE binding.organization_id = v_organization_id
          AND binding.principal_id = v_principal_id
        GROUP BY binding.project_id
        ORDER BY binding.project_id
    LOOP
        SELECT session.id
        INTO v_session_id
        FROM public.human_auth_sessions AS session
        WHERE session.organization_id = v_organization_id
          AND session.project_id = v_project_id
          AND session.principal_id = v_principal_id
          AND session.token_proof = p_token_proof
          AND session.expires_at > clock_timestamp();

        IF NOT FOUND THEN
            v_session_id := gen_random_uuid();
            INSERT INTO public.human_auth_sessions (
                id,
                organization_id,
                project_id,
                principal_id,
                token_proof,
                expires_at
            ) VALUES (
                v_session_id,
                v_organization_id,
                v_project_id,
                v_principal_id,
                p_token_proof,
                v_session_expires_at
            )
            ON CONFLICT ON CONSTRAINT human_auth_sessions_token_proof_key DO NOTHING
            RETURNING id INTO v_session_id;

            IF NOT FOUND THEN
                SELECT session.id
                INTO STRICT v_session_id
                FROM public.human_auth_sessions AS session
                WHERE session.organization_id = v_organization_id
                  AND session.project_id = v_project_id
                  AND session.principal_id = v_principal_id
                  AND session.token_proof = p_token_proof
                  AND session.expires_at > clock_timestamp();
            END IF;
        END IF;
        v_returned := true;
        RETURN QUERY SELECT
            v_organization_id,
            v_principal_id,
            v_project_id,
            v_session_id,
            v_scopes;
    END LOOP;

    IF NOT v_returned THEN
        RETURN QUERY SELECT
            v_organization_id,
            v_principal_id,
            NULL::uuid,
            NULL::uuid,
            ARRAY[]::text[];
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_authenticate_human_oidc(text, text, bytea, timestamptz) FROM PUBLIC;
ALTER FUNCTION vela_authenticate_human_oidc(text, text, bytea, timestamptz) OWNER TO vela_internal;
GRANT USAGE ON SCHEMA public TO vela_human_auth;
GRANT EXECUTE ON FUNCTION vela_authenticate_human_oidc(text, text, bytea, timestamptz)
    TO vela_human_auth;

ALTER TABLE vela_private.request_contexts
    ADD COLUMN actor_kind principal_kind NOT NULL DEFAULT 'SERVICE';

CREATE OR REPLACE FUNCTION vela_set_request_context(
    p_credential_id uuid,
    p_credential_proof bytea,
    p_required_scope text
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, vela_private
AS $$
DECLARE
    v_actor_kind public.principal_kind;
    v_organization_id uuid;
    v_project_id uuid;
    v_principal_id uuid;
    v_transaction_id xid8;
    v_existing_actor_kind public.principal_kind;
    v_existing_credential_id uuid;
    v_existing_required_scope text;
    v_existing_organization_id uuid;
    v_existing_project_id uuid;
    v_existing_principal_id uuid;
BEGIN
    IF octet_length(p_credential_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;

    SELECT 'SERVICE'::public.principal_kind, credential.organization_id,
        credential.project_id, credential.principal_id
    INTO v_actor_kind, v_organization_id, v_project_id, v_principal_id
    FROM public.credentials AS credential
    WHERE credential.id = p_credential_id
      AND pg_catalog.sha256(credential.secret_digest) = pg_catalog.sha256(p_credential_proof)
      AND p_required_scope = ANY(credential.scopes)
      AND credential.revoked_at IS NULL
      AND credential.expires_at > clock_timestamp();

    IF NOT FOUND THEN
        SELECT 'HUMAN'::public.principal_kind, session.organization_id,
            session.project_id, session.principal_id
        INTO v_actor_kind, v_organization_id, v_project_id, v_principal_id
        FROM public.human_auth_sessions AS session
        JOIN public.human_oidc_bindings AS binding
          ON binding.organization_id = session.organization_id
         AND binding.principal_id = session.principal_id
         AND binding.disabled_at IS NULL
        JOIN public.principals AS principal
          ON principal.organization_id = session.organization_id
         AND principal.id = session.principal_id
         AND principal.kind = 'HUMAN'
        JOIN public.projects AS project
          ON project.organization_id = session.organization_id
         AND project.id = session.project_id
        JOIN public.customer_organizations AS organization
          ON organization.id = session.organization_id
         AND organization.status = 'ACTIVE'
        WHERE session.id = p_credential_id
          AND pg_catalog.sha256(session.token_proof) = pg_catalog.sha256(p_credential_proof)
          AND session.expires_at > clock_timestamp()
          AND EXISTS (
              SELECT 1
              FROM public.project_role_bindings AS role_binding
              CROSS JOIN LATERAL unnest(
                  public.vela_project_role_scopes(role_binding.role)
              ) AS permission
              WHERE role_binding.organization_id = session.organization_id
                AND role_binding.project_id = session.project_id
                AND role_binding.principal_id = session.principal_id
                AND permission = p_required_scope
          );
    END IF;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;

    IF v_actor_kind = 'HUMAN' AND p_required_scope = 'webhooks:manage' THEN
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
            p_credential_id,
            v_principal_id,
            'HUMAN',
            clock_timestamp()
        )
        ON CONFLICT ON CONSTRAINT project_actor_session_attributions_pkey DO NOTHING;
    END IF;

    DELETE FROM vela_private.request_contexts AS context
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_stat_activity AS activity
        WHERE activity.pid = context.backend_pid
    );

    v_transaction_id := pg_catalog.pg_current_xact_id();
    SELECT
        context.actor_kind,
        context.credential_id,
        context.required_scope,
        context.organization_id,
        context.project_id,
        context.principal_id
    INTO
        v_existing_actor_kind,
        v_existing_credential_id,
        v_existing_required_scope,
        v_existing_organization_id,
        v_existing_project_id,
        v_existing_principal_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = v_transaction_id;

    IF FOUND THEN
        IF v_existing_actor_kind IS DISTINCT FROM v_actor_kind
            OR v_existing_credential_id IS DISTINCT FROM p_credential_id
            OR v_existing_required_scope IS DISTINCT FROM p_required_scope
            OR v_existing_organization_id IS DISTINCT FROM v_organization_id
            OR v_existing_project_id IS DISTINCT FROM v_project_id
            OR v_existing_principal_id IS DISTINCT FROM v_principal_id
        THEN
            RAISE EXCEPTION 'request context is already established for this transaction'
                USING ERRCODE = '28000';
        END IF;
    ELSE
        INSERT INTO vela_private.request_contexts (
            backend_pid,
            transaction_id,
            credential_id,
            required_scope,
            organization_id,
            project_id,
            principal_id,
            established_at,
            actor_kind
        ) VALUES (
            pg_catalog.pg_backend_pid(),
            v_transaction_id,
            p_credential_id,
            p_required_scope,
            v_organization_id,
            v_project_id,
            v_principal_id,
            transaction_timestamp(),
            v_actor_kind
        )
        ON CONFLICT (backend_pid) DO UPDATE
        SET transaction_id = EXCLUDED.transaction_id,
            credential_id = EXCLUDED.credential_id,
            required_scope = EXCLUDED.required_scope,
            organization_id = EXCLUDED.organization_id,
            project_id = EXCLUDED.project_id,
            principal_id = EXCLUDED.principal_id,
            established_at = EXCLUDED.established_at,
            actor_kind = EXCLUDED.actor_kind;
    END IF;

    RETURN QUERY SELECT
        v_organization_id,
        v_project_id,
        v_principal_id,
        transaction_timestamp();
END
$$;
REVOKE ALL ON FUNCTION vela_set_request_context(uuid, bytea, text) FROM PUBLIC;
ALTER FUNCTION vela_set_request_context(uuid, bytea, text) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_set_request_context(uuid, bytea, text) TO vela_request;

CREATE OR REPLACE FUNCTION vela_set_artifact_request_context(
    p_credential_id uuid,
    p_credential_proof bytea
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_scopes text[];
    v_expires_at timestamptz;
    v_revoked_at timestamptz;
BEGIN
    IF octet_length(p_credential_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;

    SELECT credential.scopes, credential.expires_at, credential.revoked_at
    INTO v_scopes, v_expires_at, v_revoked_at
    FROM public.credentials AS credential
    WHERE credential.id = p_credential_id
      AND pg_catalog.sha256(credential.secret_digest) = pg_catalog.sha256(p_credential_proof)
    FOR SHARE;

    IF FOUND THEN
        IF v_revoked_at IS NOT NULL OR v_expires_at <= clock_timestamp() THEN
            RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
        END IF;
        IF NOT coalesce('artifacts:read' = ANY(v_scopes), false) THEN
            RAISE EXCEPTION 'request credential lacks artifacts:read scope' USING ERRCODE = '42501';
        END IF;
    END IF;

    RETURN QUERY
    SELECT context.organization_id, context.project_id, context.principal_id, context.transaction_time
    FROM public.vela_set_request_context(
        p_credential_id,
        p_credential_proof,
        'artifacts:read'
    ) AS context;
END
$$;
REVOKE ALL ON FUNCTION vela_set_artifact_request_context(uuid, bytea) FROM PUBLIC;
ALTER FUNCTION vela_set_artifact_request_context(uuid, bytea) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_set_artifact_request_context(uuid, bytea)
    TO vela_artifact_request;

CREATE OR REPLACE FUNCTION vela_set_cancellation_request_context(
    p_credential_id uuid,
    p_credential_proof bytea
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_scopes text[];
    v_expires_at timestamptz;
    v_revoked_at timestamptz;
BEGIN
    IF octet_length(p_credential_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;

    SELECT credential.scopes, credential.expires_at, credential.revoked_at
    INTO v_scopes, v_expires_at, v_revoked_at
    FROM public.credentials AS credential
    WHERE credential.id = p_credential_id
      AND pg_catalog.sha256(credential.secret_digest) = pg_catalog.sha256(p_credential_proof)
    FOR SHARE;

    IF FOUND THEN
        IF v_revoked_at IS NOT NULL OR v_expires_at <= clock_timestamp() THEN
            RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
        END IF;
        IF NOT coalesce('jobs:cancel' = ANY(v_scopes), false) THEN
            RAISE EXCEPTION 'request credential lacks jobs:cancel scope' USING ERRCODE = '42501';
        END IF;
    END IF;

    RETURN QUERY
    SELECT context.organization_id, context.project_id, context.principal_id, context.transaction_time
    FROM public.vela_set_request_context(
        p_credential_id,
        p_credential_proof,
        'jobs:cancel'
    ) AS context;
END
$$;
REVOKE ALL ON FUNCTION vela_set_cancellation_request_context(uuid, bytea) FROM PUBLIC;
ALTER FUNCTION vela_set_cancellation_request_context(uuid, bytea) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_set_cancellation_request_context(uuid, bytea) TO vela_cancel;

GRANT SELECT, INSERT, UPDATE, DELETE ON human_oidc_bindings,
    organization_role_bindings, project_role_bindings, human_auth_sessions,
    project_principal_attributions, project_actor_session_attributions TO vela_internal;

REVOKE ALL ON human_oidc_bindings, organization_role_bindings,
    project_role_bindings, human_auth_sessions, project_principal_attributions,
    project_actor_session_attributions
    FROM PUBLIC, vela_auth, vela_human_auth, vela_request, vela_artifact_request,
    vela_cancel, vela_webhook_request;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE
    human_oidc_bindings,
    organization_role_bindings,
    project_role_bindings,
    human_auth_sessions,
    service_principals,
    credentials,
    vela_private.request_contexts,
    jobs,
    job_cancellation_decisions,
    webhook_subscriptions,
    webhook_subscription_secrets,
    webhook_delivery_replays,
    project_principal_attributions,
    project_actor_session_attributions
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM human_oidc_bindings)
        OR EXISTS (SELECT 1 FROM organization_role_bindings)
        OR EXISTS (SELECT 1 FROM project_role_bindings)
        OR EXISTS (SELECT 1 FROM human_auth_sessions)
        OR EXISTS (
            SELECT 1 FROM project_principal_attributions WHERE principal_kind = 'HUMAN'
        )
        OR EXISTS (
            SELECT 1 FROM project_actor_session_attributions WHERE actor_kind = 'HUMAN'
        )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'human_oidc_rbac_contract_requires_empty_evidence',
            MESSAGE = 'Migration 00012 cannot contract while Human identity evidence remains';
    END IF;
END
$$;

ALTER TABLE jobs
    DROP CONSTRAINT jobs_organization_id_project_id_created_by_principal_id_fkey,
    ADD CONSTRAINT jobs_organization_id_project_id_created_by_principal_id_fkey
        FOREIGN KEY (organization_id, project_id, created_by_principal_id)
        REFERENCES service_principals(organization_id, project_id, principal_id);

ALTER TABLE job_cancellation_decisions
    DROP CONSTRAINT job_cancellation_decisions_organization_id_project_id_requ_fkey,
    ADD CONSTRAINT job_cancellation_decisions_organization_id_project_id_requ_fkey
        FOREIGN KEY (organization_id, project_id, requested_by_principal_id)
        REFERENCES service_principals(organization_id, project_id, principal_id);

ALTER TABLE webhook_subscriptions
    DROP CONSTRAINT webhook_subscriptions_organization_id_project_id_created_b_fkey,
    ADD CONSTRAINT webhook_subscriptions_organization_id_project_id_created_b_fkey
        FOREIGN KEY (organization_id, project_id, created_by_credential_id)
        REFERENCES credentials(organization_id, project_id, id);

ALTER TABLE webhook_subscription_secrets
    DROP CONSTRAINT webhook_subscription_secrets_organization_id_project_id_cr_fkey,
    ADD CONSTRAINT webhook_subscription_secrets_organization_id_project_id_cr_fkey
        FOREIGN KEY (organization_id, project_id, created_by_credential_id)
        REFERENCES credentials(organization_id, project_id, id);

ALTER TABLE webhook_delivery_replays
    DROP CONSTRAINT webhook_delivery_replays_organization_id_project_id_reques_fkey,
    ADD CONSTRAINT webhook_delivery_replays_organization_id_project_id_reques_fkey
        FOREIGN KEY (organization_id, project_id, requested_by_credential_id)
        REFERENCES credentials(organization_id, project_id, id);

CREATE OR REPLACE FUNCTION vela_set_request_context(
    p_credential_id uuid,
    p_credential_proof bytea,
    p_required_scope text
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, vela_private
AS $$
DECLARE
    v_organization_id uuid;
    v_project_id uuid;
    v_principal_id uuid;
    v_transaction_id xid8;
    v_existing_credential_id uuid;
    v_existing_required_scope text;
    v_existing_organization_id uuid;
    v_existing_project_id uuid;
    v_existing_principal_id uuid;
BEGIN
    IF octet_length(p_credential_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;

    SELECT c.organization_id, c.project_id, c.principal_id
    INTO v_organization_id, v_project_id, v_principal_id
    FROM public.credentials AS c
    WHERE c.id = p_credential_id
      AND pg_catalog.sha256(c.secret_digest) = pg_catalog.sha256(p_credential_proof)
      AND p_required_scope = ANY(c.scopes)
      AND c.revoked_at IS NULL
      AND c.expires_at > clock_timestamp();

    IF NOT FOUND THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;

    DELETE FROM vela_private.request_contexts AS context
    WHERE NOT EXISTS (
        SELECT 1
        FROM pg_catalog.pg_stat_activity AS activity
        WHERE activity.pid = context.backend_pid
    );

    v_transaction_id := pg_catalog.pg_current_xact_id();
    SELECT
        context.credential_id,
        context.required_scope,
        context.organization_id,
        context.project_id,
        context.principal_id
    INTO
        v_existing_credential_id,
        v_existing_required_scope,
        v_existing_organization_id,
        v_existing_project_id,
        v_existing_principal_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = v_transaction_id;

    IF FOUND THEN
        IF v_existing_credential_id IS DISTINCT FROM p_credential_id
            OR v_existing_required_scope IS DISTINCT FROM p_required_scope
            OR v_existing_organization_id IS DISTINCT FROM v_organization_id
            OR v_existing_project_id IS DISTINCT FROM v_project_id
            OR v_existing_principal_id IS DISTINCT FROM v_principal_id
        THEN
            RAISE EXCEPTION 'request context is already established for this transaction'
                USING ERRCODE = '28000';
        END IF;
    ELSE
        INSERT INTO vela_private.request_contexts (
            backend_pid,
            transaction_id,
            credential_id,
            required_scope,
            organization_id,
            project_id,
            principal_id,
            established_at
        ) VALUES (
            pg_catalog.pg_backend_pid(),
            v_transaction_id,
            p_credential_id,
            p_required_scope,
            v_organization_id,
            v_project_id,
            v_principal_id,
            transaction_timestamp()
        )
        ON CONFLICT (backend_pid) DO UPDATE
        SET transaction_id = EXCLUDED.transaction_id,
            credential_id = EXCLUDED.credential_id,
            required_scope = EXCLUDED.required_scope,
            organization_id = EXCLUDED.organization_id,
            project_id = EXCLUDED.project_id,
            principal_id = EXCLUDED.principal_id,
            established_at = EXCLUDED.established_at;
    END IF;

    RETURN QUERY SELECT
        v_organization_id,
        v_project_id,
        v_principal_id,
        transaction_timestamp();
END
$$;
REVOKE ALL ON FUNCTION vela_set_request_context(uuid, bytea, text) FROM PUBLIC;
ALTER FUNCTION vela_set_request_context(uuid, bytea, text) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_set_request_context(uuid, bytea, text) TO vela_request;

CREATE OR REPLACE FUNCTION vela_set_artifact_request_context(
    p_credential_id uuid,
    p_credential_proof bytea
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_scopes text[];
    v_expires_at timestamptz;
    v_revoked_at timestamptz;
BEGIN
    IF octet_length(p_credential_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;

    SELECT credential.scopes, credential.expires_at, credential.revoked_at
    INTO v_scopes, v_expires_at, v_revoked_at
    FROM public.credentials AS credential
    WHERE credential.id = p_credential_id
      AND pg_catalog.sha256(credential.secret_digest) = pg_catalog.sha256(p_credential_proof)
    FOR SHARE;

    IF NOT FOUND OR v_revoked_at IS NOT NULL OR v_expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;
    IF NOT coalesce('artifacts:read' = ANY(v_scopes), false) THEN
        RAISE EXCEPTION 'request credential lacks artifacts:read scope' USING ERRCODE = '42501';
    END IF;

    RETURN QUERY
    SELECT context.organization_id, context.project_id, context.principal_id, context.transaction_time
    FROM public.vela_set_request_context(
        p_credential_id,
        p_credential_proof,
        'artifacts:read'
    ) AS context;
END
$$;
REVOKE ALL ON FUNCTION vela_set_artifact_request_context(uuid, bytea) FROM PUBLIC;
ALTER FUNCTION vela_set_artifact_request_context(uuid, bytea) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_set_artifact_request_context(uuid, bytea)
    TO vela_artifact_request;

CREATE OR REPLACE FUNCTION vela_set_cancellation_request_context(
    p_credential_id uuid,
    p_credential_proof bytea
) RETURNS TABLE (
    organization_id uuid,
    project_id uuid,
    principal_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_scopes text[];
    v_expires_at timestamptz;
    v_revoked_at timestamptz;
BEGIN
    IF octet_length(p_credential_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;

    SELECT credential.scopes, credential.expires_at, credential.revoked_at
    INTO v_scopes, v_expires_at, v_revoked_at
    FROM public.credentials AS credential
    WHERE credential.id = p_credential_id
      AND pg_catalog.sha256(credential.secret_digest) = pg_catalog.sha256(p_credential_proof)
    FOR SHARE;

    IF NOT FOUND OR v_revoked_at IS NOT NULL OR v_expires_at <= clock_timestamp() THEN
        RAISE EXCEPTION 'invalid or inactive request credential' USING ERRCODE = '28000';
    END IF;
    IF NOT coalesce('jobs:cancel' = ANY(v_scopes), false) THEN
        RAISE EXCEPTION 'request credential lacks jobs:cancel scope' USING ERRCODE = '42501';
    END IF;

    RETURN QUERY
    SELECT context.organization_id, context.project_id, context.principal_id, context.transaction_time
    FROM public.vela_set_request_context(
        p_credential_id,
        p_credential_proof,
        'jobs:cancel'
    ) AS context;
END
$$;
REVOKE ALL ON FUNCTION vela_set_cancellation_request_context(uuid, bytea) FROM PUBLIC;
ALTER FUNCTION vela_set_cancellation_request_context(uuid, bytea) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_set_cancellation_request_context(uuid, bytea) TO vela_cancel;

ALTER TABLE vela_private.request_contexts DROP COLUMN actor_kind;

DROP TRIGGER IF EXISTS credentials_record_project_actor_session ON credentials;
DROP TRIGGER IF EXISTS service_principals_record_project_attribution ON service_principals;
DROP TRIGGER IF EXISTS project_roles_record_project_attribution ON project_role_bindings;
DROP TRIGGER IF EXISTS human_oidc_binding_identity_immutable ON human_oidc_bindings;
DROP TRIGGER IF EXISTS organization_role_binding_identity_immutable
    ON organization_role_bindings;
DROP TRIGGER IF EXISTS project_role_binding_identity_immutable ON project_role_bindings;
DROP TRIGGER IF EXISTS human_auth_session_identity_immutable ON human_auth_sessions;
DROP TRIGGER IF EXISTS project_principal_attributions_immutable ON project_principal_attributions;
DROP TRIGGER IF EXISTS project_actor_session_attributions_immutable
    ON project_actor_session_attributions;

DROP FUNCTION IF EXISTS vela_authenticate_human_oidc(text, text, bytea, timestamptz);
REVOKE USAGE ON SCHEMA public FROM vela_human_auth;
DROP FUNCTION IF EXISTS vela_project_role_scopes(project_role);
DROP FUNCTION IF EXISTS vela_record_service_actor_session_attribution();
DROP FUNCTION IF EXISTS vela_record_project_principal_attribution();
DROP FUNCTION IF EXISTS vela_reject_project_principal_attribution_mutation();
DROP FUNCTION IF EXISTS vela_reject_human_identity_evidence_update();
DROP FUNCTION IF EXISTS vela_enforce_human_oidc_binding_immutability();

DROP TABLE project_actor_session_attributions;
DROP TABLE project_principal_attributions;
DROP TABLE human_auth_sessions;
DROP TABLE project_role_bindings;
DROP TABLE organization_role_bindings;
DROP TABLE human_oidc_bindings;
DROP TYPE project_role;
DROP TYPE organization_role;
-- +goose StatementEnd
