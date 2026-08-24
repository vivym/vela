-- +goose Up
-- +goose StatementBegin
CREATE TYPE break_glass_scope AS ENUM (
    'REQUEST_CONTENT_READ', 'ARTIFACT_READ'
);
CREATE TYPE break_glass_reason_code AS ENUM (
    'CUSTOMER_SUPPORT',
    'SECURITY_INVESTIGATION',
    'SERVICE_RECOVERY',
    'LEGAL_RESPONSE'
);
CREATE TYPE break_glass_outcome_code AS ENUM (
    'CREATED',
    'APPROVED',
    'REVOKED',
    'ALLOWED',
    'SCOPE_DENIED',
    'GRANT_REVOKED',
    'GRANT_EXPIRED',
    'TARGET_NOT_FOUND',
    'CONTENT_DELETED',
    'CONTENT_EXPIRED',
    'CONTENT_UNAVAILABLE',
    'DELIVERED',
    'SIGNING_FAILED',
    'GRANT_INACTIVE'
);
CREATE TYPE break_glass_event_action AS ENUM (
    'REQUEST_CREATED',
    'GRANT_APPROVED',
    'GRANT_REVOKED',
    'REQUEST_CONTENT_AUTHORIZED',
    'REQUEST_CONTENT_DENIED',
    'ARTIFACT_AUTHORIZED',
    'ARTIFACT_DELIVERED',
    'ARTIFACT_DELIVERY_FAILED',
    'ARTIFACT_DENIED'
);

CREATE FUNCTION vela_break_glass_scopes_valid(p_scopes break_glass_scope[])
RETURNS boolean
LANGUAGE sql
IMMUTABLE
SET search_path = pg_catalog, public
AS $$
    SELECT p_scopes IS NOT NULL
       AND cardinality(p_scopes) BETWEEN 1 AND 2
       AND array_position(p_scopes, NULL) IS NULL
       AND cardinality(p_scopes) = (
            SELECT count(DISTINCT scope)::integer
            FROM unnest(p_scopes) AS scope
       )
$$;
REVOKE ALL ON FUNCTION vela_break_glass_scopes_valid(break_glass_scope[]) FROM PUBLIC;
ALTER FUNCTION vela_break_glass_scopes_valid(break_glass_scope[])
    OWNER TO vela_break_glass_owner;

CREATE TABLE platform_operator_oidc_bindings (
    id uuid PRIMARY KEY,
    issuer text NOT NULL CHECK (
        octet_length(issuer) BETWEEN 1 AND 2048
        AND issuer = btrim(issuer)
    ),
    subject text NOT NULL CHECK (octet_length(subject) BETWEEN 1 AND 500),
    display_name text NOT NULL CHECK (length(display_name) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    disabled_at timestamptz,
    UNIQUE (issuer, subject),
    UNIQUE (id, issuer, subject),
    CHECK (disabled_at IS NULL OR disabled_at >= created_at)
);
ALTER TABLE platform_operator_oidc_bindings OWNER TO vela_break_glass_owner;
REVOKE ALL ON TABLE platform_operator_oidc_bindings FROM PUBLIC;
ALTER TABLE platform_operator_oidc_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE platform_operator_oidc_bindings FORCE ROW LEVEL SECURITY;

CREATE TABLE platform_operator_auth_sessions (
    id uuid PRIMARY KEY,
    operator_id uuid NOT NULL REFERENCES platform_operator_oidc_bindings(id),
    token_proof bytea NOT NULL CHECK (octet_length(token_proof) = 32),
    created_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    UNIQUE (id, operator_id),
    CHECK (expires_at > created_at AND expires_at <= created_at + interval '15 minutes')
);
CREATE INDEX platform_operator_auth_sessions_active_idx
    ON platform_operator_auth_sessions (operator_id, expires_at DESC);
CREATE INDEX platform_operator_auth_sessions_proof_idx
    ON platform_operator_auth_sessions (operator_id, token_proof, expires_at DESC);
ALTER TABLE platform_operator_auth_sessions OWNER TO vela_break_glass_owner;
REVOKE ALL ON TABLE platform_operator_auth_sessions FROM PUBLIC;
ALTER TABLE platform_operator_auth_sessions ENABLE ROW LEVEL SECURITY;
ALTER TABLE platform_operator_auth_sessions FORCE ROW LEVEL SECURITY;

CREATE TABLE vela_private.break_glass_request_contexts (
    backend_pid integer PRIMARY KEY,
    transaction_id xid8 NOT NULL,
    operator_id uuid NOT NULL,
    operator_session_id uuid NOT NULL,
    established_at timestamptz NOT NULL
);
ALTER TABLE vela_private.break_glass_request_contexts OWNER TO vela_break_glass_owner;
REVOKE ALL ON TABLE vela_private.break_glass_request_contexts FROM PUBLIC;

CREATE TABLE break_glass_requests (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    scopes break_glass_scope[] NOT NULL CHECK (vela_break_glass_scopes_valid(scopes)),
    reason_code break_glass_reason_code NOT NULL,
    ticket_reference text NOT NULL CHECK (
        ticket_reference ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{2,99}$'
    ),
    requested_duration_seconds integer NOT NULL CHECK (
        requested_duration_seconds BETWEEN 60 AND 3600
    ),
    requester_operator_id uuid NOT NULL,
    requester_session_id uuid NOT NULL,
    idempotency_key text NOT NULL CHECK (
        length(idempotency_key) BETWEEN 1 AND 200
        AND idempotency_key ~ '^[A-Za-z0-9._:-]+$'
    ),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    requested_at timestamptz NOT NULL,
    approval_deadline_at timestamptz NOT NULL,
    UNIQUE (requester_operator_id, idempotency_key),
    UNIQUE (id, organization_id, project_id, job_id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (requester_session_id, requester_operator_id)
        REFERENCES platform_operator_auth_sessions(id, operator_id),
    CHECK (approval_deadline_at = requested_at + interval '1 hour')
);
CREATE INDEX break_glass_requests_target_idx
    ON break_glass_requests (organization_id, project_id, job_id, requested_at DESC);
ALTER TABLE break_glass_requests OWNER TO vela_break_glass_owner;
REVOKE ALL ON TABLE break_glass_requests FROM PUBLIC;
ALTER TABLE break_glass_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE break_glass_requests FORCE ROW LEVEL SECURITY;

CREATE TABLE break_glass_grants (
    id uuid PRIMARY KEY,
    request_id uuid NOT NULL UNIQUE,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    approver_operator_id uuid NOT NULL,
    approver_session_id uuid NOT NULL,
    approved_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    revoked_by_operator_id uuid,
    revoked_by_session_id uuid,
    UNIQUE (id, organization_id, project_id, job_id),
    FOREIGN KEY (request_id, organization_id, project_id, job_id)
        REFERENCES break_glass_requests(id, organization_id, project_id, job_id),
    FOREIGN KEY (approver_session_id, approver_operator_id)
        REFERENCES platform_operator_auth_sessions(id, operator_id),
    FOREIGN KEY (revoked_by_session_id, revoked_by_operator_id)
        REFERENCES platform_operator_auth_sessions(id, operator_id),
    CHECK (expires_at > approved_at AND expires_at <= approved_at + interval '1 hour'),
    CHECK (
        (revoked_at IS NULL AND revoked_by_operator_id IS NULL AND revoked_by_session_id IS NULL)
        OR
        (revoked_at IS NOT NULL AND revoked_by_operator_id IS NOT NULL
            AND revoked_by_session_id IS NOT NULL AND revoked_at >= approved_at)
    )
);
CREATE INDEX break_glass_grants_target_idx
    ON break_glass_grants (organization_id, project_id, job_id, expires_at DESC);
ALTER TABLE break_glass_grants OWNER TO vela_break_glass_owner;
REVOKE ALL ON TABLE break_glass_grants FROM PUBLIC;
ALTER TABLE break_glass_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE break_glass_grants FORCE ROW LEVEL SECURITY;

CREATE TABLE break_glass_events (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    request_id uuid NOT NULL,
    grant_id uuid,
    operator_id uuid NOT NULL,
    operator_session_id uuid NOT NULL,
    action break_glass_event_action NOT NULL,
    scope break_glass_scope NOT NULL,
    outcome_code break_glass_outcome_code NOT NULL,
    created_at timestamptz NOT NULL,
    FOREIGN KEY (request_id, organization_id, project_id, job_id)
        REFERENCES break_glass_requests(id, organization_id, project_id, job_id),
    FOREIGN KEY (grant_id, organization_id, project_id, job_id)
        REFERENCES break_glass_grants(id, organization_id, project_id, job_id),
    FOREIGN KEY (operator_session_id, operator_id)
        REFERENCES platform_operator_auth_sessions(id, operator_id),
    CHECK (
        (action = 'REQUEST_CREATED' AND grant_id IS NULL)
        OR (action <> 'REQUEST_CREATED' AND grant_id IS NOT NULL)
    )
);
CREATE INDEX break_glass_events_organization_idx
    ON break_glass_events (organization_id, created_at DESC, id DESC);
CREATE INDEX break_glass_events_grant_idx
    ON break_glass_events (grant_id, created_at DESC, id DESC);
ALTER TABLE break_glass_events OWNER TO vela_break_glass_owner;
REVOKE ALL ON TABLE break_glass_events FROM PUBLIC;
ALTER TABLE break_glass_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE break_glass_events FORCE ROW LEVEL SECURITY;

CREATE TABLE break_glass_denial_events (
    id uuid PRIMARY KEY,
    attempted_grant_id uuid NOT NULL,
    operator_id uuid NOT NULL,
    operator_session_id uuid NOT NULL,
    action break_glass_event_action NOT NULL,
    scope break_glass_scope NOT NULL,
    outcome_code break_glass_outcome_code NOT NULL,
    created_at timestamptz NOT NULL,
    FOREIGN KEY (operator_session_id, operator_id)
        REFERENCES platform_operator_auth_sessions(id, operator_id),
    CHECK (
        (action = 'REQUEST_CONTENT_DENIED' AND scope = 'REQUEST_CONTENT_READ')
        OR (action = 'ARTIFACT_DENIED' AND scope = 'ARTIFACT_READ')
    ),
    CHECK (outcome_code = 'TARGET_NOT_FOUND')
);
CREATE INDEX break_glass_denial_events_operator_idx
    ON break_glass_denial_events (operator_id, created_at DESC, id DESC);
ALTER TABLE break_glass_denial_events OWNER TO vela_break_glass_owner;
REVOKE ALL ON TABLE break_glass_denial_events FROM PUBLIC;
ALTER TABLE break_glass_denial_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE break_glass_denial_events FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_reject_break_glass_evidence_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Break-glass evidence is immutable'
        USING ERRCODE = '55000',
            CONSTRAINT = 'break_glass_evidence_is_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_break_glass_evidence_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_break_glass_evidence_mutation()
    OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_enforce_platform_operator_binding_immutability() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(NEW.id, NEW.issuer, NEW.subject, NEW.display_name, NEW.created_at)
        IS DISTINCT FROM
       ROW(OLD.id, OLD.issuer, OLD.subject, OLD.display_name, OLD.created_at)
    THEN
        RAISE EXCEPTION 'Platform Operator binding identity is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.disabled_at IS NOT NULL AND NEW.disabled_at IS DISTINCT FROM OLD.disabled_at THEN
        RAISE EXCEPTION 'Platform Operator disablement is permanent'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_platform_operator_binding_immutability() FROM PUBLIC;
ALTER FUNCTION vela_enforce_platform_operator_binding_immutability()
    OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_enforce_break_glass_grant_immutability() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(
        NEW.id, NEW.request_id, NEW.organization_id, NEW.project_id, NEW.job_id,
        NEW.approver_operator_id, NEW.approver_session_id, NEW.approved_at, NEW.expires_at
    ) IS DISTINCT FROM ROW(
        OLD.id, OLD.request_id, OLD.organization_id, OLD.project_id, OLD.job_id,
        OLD.approver_operator_id, OLD.approver_session_id, OLD.approved_at, OLD.expires_at
    ) THEN
        RAISE EXCEPTION 'Break-glass grant identity is immutable' USING ERRCODE = '55000';
    END IF;
    IF OLD.revoked_at IS NOT NULL AND ROW(
        NEW.revoked_at, NEW.revoked_by_operator_id, NEW.revoked_by_session_id
    ) IS DISTINCT FROM ROW(
        OLD.revoked_at, OLD.revoked_by_operator_id, OLD.revoked_by_session_id
    ) THEN
        RAISE EXCEPTION 'Break-glass revocation is permanent' USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_break_glass_grant_immutability() FROM PUBLIC;
ALTER FUNCTION vela_enforce_break_glass_grant_immutability()
    OWNER TO vela_break_glass_owner;

CREATE TRIGGER platform_operator_oidc_bindings_immutable
BEFORE UPDATE ON platform_operator_oidc_bindings
FOR EACH ROW EXECUTE FUNCTION vela_enforce_platform_operator_binding_immutability();
CREATE TRIGGER platform_operator_oidc_bindings_no_delete
BEFORE DELETE ON platform_operator_oidc_bindings
FOR EACH ROW EXECUTE FUNCTION vela_reject_break_glass_evidence_mutation();
CREATE TRIGGER platform_operator_auth_sessions_immutable
BEFORE UPDATE OR DELETE ON platform_operator_auth_sessions
FOR EACH ROW EXECUTE FUNCTION vela_reject_break_glass_evidence_mutation();
CREATE TRIGGER break_glass_requests_immutable
BEFORE UPDATE OR DELETE ON break_glass_requests
FOR EACH ROW EXECUTE FUNCTION vela_reject_break_glass_evidence_mutation();
CREATE TRIGGER break_glass_grants_immutable
BEFORE UPDATE ON break_glass_grants
FOR EACH ROW EXECUTE FUNCTION vela_enforce_break_glass_grant_immutability();
CREATE TRIGGER break_glass_grants_no_delete
BEFORE DELETE ON break_glass_grants
FOR EACH ROW EXECUTE FUNCTION vela_reject_break_glass_evidence_mutation();
CREATE TRIGGER break_glass_events_immutable
BEFORE UPDATE OR DELETE ON break_glass_events
FOR EACH ROW EXECUTE FUNCTION vela_reject_break_glass_evidence_mutation();
CREATE TRIGGER break_glass_denial_events_immutable
BEFORE UPDATE OR DELETE ON break_glass_denial_events
FOR EACH ROW EXECUTE FUNCTION vela_reject_break_glass_evidence_mutation();

CREATE FUNCTION vela_authenticate_platform_operator_oidc(
    p_issuer text,
    p_subject text,
    p_token_proof bytea,
    p_token_expires_at timestamptz
) RETURNS TABLE (
    operator_id uuid,
    session_id uuid,
    expires_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operator_id uuid;
    v_session_id uuid;
    v_now timestamptz := clock_timestamp();
    v_expires_at timestamptz;
BEGIN
    IF p_issuer IS NULL OR p_subject IS NULL
        OR octet_length(p_token_proof) IS DISTINCT FROM 32
        OR p_token_expires_at IS NULL OR p_token_expires_at <= v_now
    THEN
        RAISE EXCEPTION 'invalid Platform Operator OIDC identity' USING ERRCODE = '28000';
    END IF;

    SELECT binding.id
    INTO v_operator_id
    FROM public.platform_operator_oidc_bindings AS binding
    WHERE binding.issuer = p_issuer
      AND binding.subject = p_subject
      AND binding.disabled_at IS NULL;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'invalid Platform Operator OIDC identity' USING ERRCODE = '28000';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(
            v_operator_id::text || ':' || pg_catalog.encode(p_token_proof, 'hex'),
            170017
        )
    );
    v_expires_at := LEAST(p_token_expires_at, v_now + interval '15 minutes');
    SELECT session.id, session.expires_at
    INTO v_session_id, v_expires_at
    FROM public.platform_operator_auth_sessions AS session
    WHERE session.operator_id = v_operator_id
      AND session.token_proof = p_token_proof
      AND session.expires_at > v_now
    ORDER BY session.created_at DESC
    LIMIT 1;

    IF NOT FOUND THEN
        v_session_id := gen_random_uuid();
        v_expires_at := LEAST(p_token_expires_at, v_now + interval '15 minutes');
        INSERT INTO public.platform_operator_auth_sessions (
            id, operator_id, token_proof, created_at, expires_at
        ) VALUES (
            v_session_id, v_operator_id, p_token_proof, v_now, v_expires_at
        );
    END IF;

    RETURN QUERY SELECT v_operator_id, v_session_id, v_expires_at;
END
$$;
REVOKE ALL ON FUNCTION vela_authenticate_platform_operator_oidc(
    text, text, bytea, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_authenticate_platform_operator_oidc(text, text, bytea, timestamptz)
    OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_set_break_glass_request_context(
    p_operator_session_id uuid,
    p_session_proof bytea
) RETURNS TABLE (
    operator_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_operator_id uuid;
    v_transaction_time timestamptz := clock_timestamp();
    v_transaction_id xid8 := pg_catalog.pg_current_xact_id();
    v_existing_operator_id uuid;
    v_existing_session_id uuid;
BEGIN
    IF p_operator_session_id IS NULL
        OR octet_length(p_session_proof) IS DISTINCT FROM 32
    THEN
        RAISE EXCEPTION 'invalid Break-glass request context' USING ERRCODE = '28000';
    END IF;

    SELECT session.operator_id
    INTO v_operator_id
    FROM public.platform_operator_auth_sessions AS session
    JOIN public.platform_operator_oidc_bindings AS binding
      ON binding.id = session.operator_id
     AND binding.disabled_at IS NULL
    WHERE session.id = p_operator_session_id
      AND session.token_proof = p_session_proof
      AND session.expires_at > v_transaction_time;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Platform Operator session is no longer active'
            USING ERRCODE = '28000';
    END IF;

    DELETE FROM vela_private.break_glass_request_contexts AS context
    WHERE NOT EXISTS (
        SELECT 1 FROM pg_catalog.pg_stat_activity AS activity
        WHERE activity.pid = context.backend_pid
    );

    SELECT context.operator_id, context.operator_session_id
    INTO v_existing_operator_id, v_existing_session_id
    FROM vela_private.break_glass_request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = v_transaction_id;
    IF FOUND THEN
        IF v_existing_operator_id IS DISTINCT FROM v_operator_id
            OR v_existing_session_id IS DISTINCT FROM p_operator_session_id
        THEN
            RAISE EXCEPTION 'Break-glass request context is already established'
                USING ERRCODE = '28000';
        END IF;
        RETURN QUERY SELECT v_operator_id, v_transaction_time;
        RETURN;
    END IF;

    INSERT INTO vela_private.break_glass_request_contexts (
        backend_pid, transaction_id, operator_id, operator_session_id, established_at
    ) VALUES (
        pg_catalog.pg_backend_pid(), v_transaction_id, v_operator_id,
        p_operator_session_id, v_transaction_time
    )
    ON CONFLICT (backend_pid) DO UPDATE
    SET transaction_id = EXCLUDED.transaction_id,
        operator_id = EXCLUDED.operator_id,
        operator_session_id = EXCLUDED.operator_session_id,
        established_at = EXCLUDED.established_at
    WHERE break_glass_request_contexts.transaction_id <> EXCLUDED.transaction_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Break-glass request context is already established'
            USING ERRCODE = '28000';
    END IF;

    RETURN QUERY SELECT v_operator_id, v_transaction_time;
END
$$;
REVOKE ALL ON FUNCTION vela_set_break_glass_request_context(uuid, bytea) FROM PUBLIC;
ALTER FUNCTION vela_set_break_glass_request_context(uuid, bytea)
    OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_private.current_break_glass_request_context()
RETURNS TABLE (
    operator_id uuid,
    operator_session_id uuid,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    RETURN QUERY
    SELECT context.operator_id, context.operator_session_id, context.established_at
    FROM vela_private.break_glass_request_contexts AS context
    JOIN public.platform_operator_auth_sessions AS session
      ON session.id = context.operator_session_id
     AND session.operator_id = context.operator_id
     AND session.expires_at > clock_timestamp()
    JOIN public.platform_operator_oidc_bindings AS binding
      ON binding.id = context.operator_id
     AND binding.disabled_at IS NULL
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id();
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Platform Operator session is no longer active'
            USING ERRCODE = '28000';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.current_break_glass_request_context() FROM PUBLIC;
ALTER FUNCTION vela_private.current_break_glass_request_context()
    OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_create_break_glass_request(
    p_request_id uuid,
    p_idempotency_key text,
    p_request_hash bytea,
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_scopes break_glass_scope[],
    p_reason_code break_glass_reason_code,
    p_ticket_reference text,
    p_requested_duration_seconds integer
) RETURNS TABLE (
    request_id uuid,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_operator_id uuid;
    v_session_id uuid;
    v_now timestamptz;
    v_existing_id uuid;
    v_existing_hash bytea;
    v_scopes public.break_glass_scope[];
BEGIN
    SELECT context.operator_id, context.operator_session_id, context.transaction_time
    INTO v_operator_id, v_session_id, v_now
    FROM vela_private.current_break_glass_request_context() AS context;

    IF p_request_id IS NULL OR p_organization_id IS NULL OR p_project_id IS NULL
        OR p_job_id IS NULL OR p_idempotency_key IS NULL
        OR length(p_idempotency_key) NOT BETWEEN 1 AND 200
        OR p_idempotency_key !~ '^[A-Za-z0-9._:-]+$'
        OR octet_length(p_request_hash) IS DISTINCT FROM 32
        OR NOT public.vela_break_glass_scopes_valid(p_scopes)
        OR p_reason_code IS NULL
        OR p_ticket_reference !~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{2,99}$'
        OR p_requested_duration_seconds NOT BETWEEN 60 AND 3600
    THEN
        RAISE EXCEPTION 'invalid Break-glass request' USING ERRCODE = '22023';
    END IF;

    SELECT request.id, request.request_hash
    INTO v_existing_id, v_existing_hash
    FROM public.break_glass_requests AS request
    WHERE request.requester_operator_id = v_operator_id
      AND request.idempotency_key = p_idempotency_key
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing_hash <> p_request_hash THEN
            RAISE EXCEPTION 'Break-glass Idempotency-Key is already used'
                USING ERRCODE = '23505',
                    CONSTRAINT = 'break_glass_requests_requester_idempotency_key';
        END IF;
        RETURN QUERY SELECT v_existing_id, true;
        RETURN;
    END IF;

    PERFORM 1
    FROM public.jobs AS job
    WHERE job.organization_id = p_organization_id
      AND job.project_id = p_project_id
      AND job.id = p_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Break-glass target is not found' USING ERRCODE = '02000';
    END IF;

    SELECT array_agg(scope ORDER BY scope::text)
    INTO v_scopes
    FROM unnest(p_scopes) AS scope;

    INSERT INTO public.break_glass_requests (
        id, organization_id, project_id, job_id, scopes, reason_code,
        ticket_reference, requested_duration_seconds, requester_operator_id,
        requester_session_id, idempotency_key, request_hash, requested_at,
        approval_deadline_at
    ) VALUES (
        p_request_id, p_organization_id, p_project_id, p_job_id, v_scopes,
        p_reason_code, p_ticket_reference, p_requested_duration_seconds,
        v_operator_id, v_session_id, p_idempotency_key, p_request_hash, v_now,
        v_now + interval '1 hour'
    )
    ON CONFLICT (requester_operator_id, idempotency_key) DO NOTHING;
    IF NOT FOUND THEN
        SELECT request.id, request.request_hash
        INTO v_existing_id, v_existing_hash
        FROM public.break_glass_requests AS request
        WHERE request.requester_operator_id = v_operator_id
          AND request.idempotency_key = p_idempotency_key
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Break-glass Idempotency-Key conflict was not retained'
                USING ERRCODE = '55000';
        END IF;
        IF v_existing_hash <> p_request_hash THEN
            RAISE EXCEPTION 'Break-glass Idempotency-Key is already used'
                USING ERRCODE = '23505',
                    CONSTRAINT = 'break_glass_requests_requester_idempotency_key';
        END IF;
        RETURN QUERY SELECT v_existing_id, true;
        RETURN;
    END IF;
    INSERT INTO public.break_glass_events (
        id, organization_id, project_id, job_id, request_id, grant_id,
        operator_id, operator_session_id, action, scope, outcome_code, created_at
    )
    SELECT
        gen_random_uuid(), p_organization_id, p_project_id, p_job_id, p_request_id,
        NULL, v_operator_id, v_session_id, 'REQUEST_CREATED', scope, 'CREATED', v_now
    FROM unnest(v_scopes) AS scope;
    RETURN QUERY SELECT p_request_id, false;
END
$$;
REVOKE ALL ON FUNCTION vela_create_break_glass_request(
    uuid, text, bytea, uuid, uuid, uuid, break_glass_scope[],
    break_glass_reason_code, text, integer
) FROM PUBLIC;
ALTER FUNCTION vela_create_break_glass_request(
    uuid, text, bytea, uuid, uuid, uuid, break_glass_scope[],
    break_glass_reason_code, text, integer
) OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_approve_break_glass_request(
    p_request_id uuid,
    p_grant_id uuid
) RETURNS TABLE (
    grant_id uuid,
    expires_at timestamptz,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_operator_id uuid;
    v_session_id uuid;
    v_now timestamptz;
    v_request public.break_glass_requests%ROWTYPE;
    v_existing public.break_glass_grants%ROWTYPE;
BEGIN
    SELECT context.operator_id, context.operator_session_id, context.transaction_time
    INTO v_operator_id, v_session_id, v_now
    FROM vela_private.current_break_glass_request_context() AS context;

    SELECT request.* INTO v_request
    FROM public.break_glass_requests AS request
    JOIN public.jobs AS job
      ON job.organization_id = request.organization_id
     AND job.project_id = request.project_id
     AND job.id = request.job_id
    WHERE request.id = p_request_id
    FOR UPDATE OF request;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Break-glass request is not found' USING ERRCODE = '02000';
    END IF;
    IF v_request.requester_operator_id = v_operator_id THEN
        RAISE EXCEPTION 'Break-glass request requires a distinct approver'
            USING ERRCODE = '42501';
    END IF;

    SELECT existing_grant.* INTO v_existing
    FROM public.break_glass_grants AS existing_grant
    WHERE existing_grant.request_id = p_request_id;
    IF FOUND THEN
        IF v_existing.approver_operator_id <> v_operator_id THEN
            RAISE EXCEPTION 'Break-glass request is already approved'
                USING ERRCODE = '55000';
        END IF;
        RETURN QUERY SELECT v_existing.id, v_existing.expires_at, true;
        RETURN;
    END IF;
    IF v_request.approval_deadline_at <= v_now THEN
        RAISE EXCEPTION 'Break-glass approval window has expired'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO public.break_glass_grants (
        id, request_id, organization_id, project_id, job_id,
        approver_operator_id, approver_session_id, approved_at, expires_at
    ) VALUES (
        p_grant_id, v_request.id, v_request.organization_id, v_request.project_id,
        v_request.job_id, v_operator_id, v_session_id, v_now,
        v_now + make_interval(secs => v_request.requested_duration_seconds)
    ) RETURNING break_glass_grants.expires_at INTO expires_at;
    INSERT INTO public.break_glass_events (
        id, organization_id, project_id, job_id, request_id, grant_id,
        operator_id, operator_session_id, action, scope, outcome_code, created_at
    )
    SELECT
        gen_random_uuid(), v_request.organization_id, v_request.project_id,
        v_request.job_id, v_request.id, p_grant_id, v_operator_id, v_session_id,
        'GRANT_APPROVED', scope, 'APPROVED', v_now
    FROM unnest(v_request.scopes) AS scope;
    RETURN QUERY SELECT p_grant_id, expires_at, false;
END
$$;
REVOKE ALL ON FUNCTION vela_approve_break_glass_request(uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_approve_break_glass_request(uuid, uuid)
    OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_revoke_break_glass_grant(
    p_grant_id uuid
) RETURNS TABLE (
    revoked_at timestamptz,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_operator_id uuid;
    v_session_id uuid;
    v_now timestamptz;
    v_grant public.break_glass_grants%ROWTYPE;
    v_request public.break_glass_requests%ROWTYPE;
BEGIN
    SELECT context.operator_id, context.operator_session_id, context.transaction_time
    INTO v_operator_id, v_session_id, v_now
    FROM vela_private.current_break_glass_request_context() AS context;
    SELECT existing_grant.* INTO v_grant
    FROM public.break_glass_grants AS existing_grant
    JOIN public.jobs AS job
      ON job.organization_id = existing_grant.organization_id
     AND job.project_id = existing_grant.project_id
     AND job.id = existing_grant.job_id
    WHERE existing_grant.id = p_grant_id
    FOR UPDATE OF existing_grant;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Break-glass grant is not found' USING ERRCODE = '02000';
    END IF;
    IF v_grant.revoked_at IS NOT NULL THEN
        RETURN QUERY SELECT v_grant.revoked_at, true;
        RETURN;
    END IF;
    IF v_grant.expires_at <= v_now THEN
        RAISE EXCEPTION 'Break-glass grant is no longer active'
            USING ERRCODE = '55000',
                CONSTRAINT = 'break_glass_grant_inactive';
    END IF;

    SELECT request.* INTO STRICT v_request
    FROM public.break_glass_requests AS request
    WHERE request.id = v_grant.request_id;

    UPDATE public.break_glass_grants AS target_grant
    SET revoked_at = v_now,
        revoked_by_operator_id = v_operator_id,
        revoked_by_session_id = v_session_id
    WHERE target_grant.id = p_grant_id;
    INSERT INTO public.break_glass_events (
        id, organization_id, project_id, job_id, request_id, grant_id,
        operator_id, operator_session_id, action, scope, outcome_code, created_at
    )
    SELECT
        gen_random_uuid(), v_grant.organization_id, v_grant.project_id,
        v_grant.job_id, v_grant.request_id, v_grant.id, v_operator_id, v_session_id,
        'GRANT_REVOKED', scope, 'REVOKED', v_now
    FROM unnest(v_request.scopes) AS scope;
    RETURN QUERY SELECT v_now, false;
END
$$;
REVOKE ALL ON FUNCTION vela_revoke_break_glass_grant(uuid) FROM PUBLIC;
ALTER FUNCTION vela_revoke_break_glass_grant(uuid)
    OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_get_break_glass_request(
    p_request_id uuid
) RETURNS TABLE (
    request_id uuid,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    scopes text[],
    reason_code text,
    ticket_reference text,
    requested_duration_seconds integer,
    requester_operator_id uuid,
    requested_at timestamptz,
    approval_deadline_at timestamptz,
    grant_id uuid,
    approver_operator_id uuid,
    approved_at timestamptz,
    expires_at timestamptz,
    revoked_at timestamptz,
    revoked_by_operator_id uuid,
    state text,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_now timestamptz;
BEGIN
    SELECT context.transaction_time
    INTO v_now
    FROM vela_private.current_break_glass_request_context() AS context;
    RETURN QUERY
    SELECT
        request.id,
        request.organization_id,
        request.project_id,
        request.job_id,
        request.scopes::text[],
        request.reason_code::text,
        request.ticket_reference,
        request.requested_duration_seconds,
        request.requester_operator_id,
        request.requested_at,
        request.approval_deadline_at,
        active_grant.id,
        active_grant.approver_operator_id,
        active_grant.approved_at,
        active_grant.expires_at,
        active_grant.revoked_at,
        active_grant.revoked_by_operator_id,
        CASE
            WHEN active_grant.id IS NULL AND request.approval_deadline_at <= v_now
                THEN 'EXPIRED'
            WHEN active_grant.id IS NULL THEN 'PENDING'
            WHEN active_grant.revoked_at IS NOT NULL THEN 'REVOKED'
            WHEN active_grant.expires_at <= v_now THEN 'EXPIRED'
            ELSE 'ACTIVE'
        END,
        v_now
    FROM public.break_glass_requests AS request
    LEFT JOIN public.break_glass_grants AS active_grant
      ON active_grant.request_id = request.id
    WHERE request.id = p_request_id;
END
$$;
REVOKE ALL ON FUNCTION vela_get_break_glass_request(uuid) FROM PUBLIC;
ALTER FUNCTION vela_get_break_glass_request(uuid) OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_get_break_glass_grant(
    p_grant_id uuid
) RETURNS TABLE (
    request_id uuid,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    scopes text[],
    reason_code text,
    ticket_reference text,
    requested_duration_seconds integer,
    requester_operator_id uuid,
    requested_at timestamptz,
    approval_deadline_at timestamptz,
    grant_id uuid,
    approver_operator_id uuid,
    approved_at timestamptz,
    expires_at timestamptz,
    revoked_at timestamptz,
    revoked_by_operator_id uuid,
    state text,
    transaction_time timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_request_id uuid;
BEGIN
    PERFORM 1 FROM vela_private.current_break_glass_request_context();
    SELECT target_grant.request_id
    INTO v_request_id
    FROM public.break_glass_grants AS target_grant
    WHERE target_grant.id = p_grant_id;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    RETURN QUERY
    SELECT request.*
    FROM public.vela_get_break_glass_request(v_request_id) AS request;
END
$$;
REVOKE ALL ON FUNCTION vela_get_break_glass_grant(uuid) FROM PUBLIC;
ALTER FUNCTION vela_get_break_glass_grant(uuid) OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_private.authorize_break_glass_scope(
    p_grant_id uuid,
    p_scope public.break_glass_scope
) RETURNS TABLE (
    target_found boolean,
    evidence_event_id uuid,
    operator_id uuid,
    operator_session_id uuid,
    transaction_time timestamptz,
    request_id uuid,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    grant_expires_at timestamptz,
    grant_authorized boolean,
    outcome_code public.break_glass_outcome_code
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_operator_id uuid;
    v_session_id uuid;
    v_now timestamptz;
    v_grant public.break_glass_grants%ROWTYPE;
    v_request public.break_glass_requests%ROWTYPE;
    v_event_id uuid;
    v_authorized boolean := false;
    v_outcome public.break_glass_outcome_code;
BEGIN
    IF p_grant_id IS NULL OR p_scope IS NULL THEN
        RAISE EXCEPTION 'Break-glass grant and scope are required' USING ERRCODE = '22023';
    END IF;

    SELECT context.operator_id, context.operator_session_id, context.transaction_time
    INTO v_operator_id, v_session_id, v_now
    FROM vela_private.current_break_glass_request_context() AS context;

    SELECT target_grant.* INTO v_grant
    FROM public.break_glass_grants AS target_grant
    JOIN public.jobs AS job
      ON job.organization_id = target_grant.organization_id
     AND job.project_id = target_grant.project_id
     AND job.id = target_grant.job_id
    WHERE target_grant.id = p_grant_id
    FOR SHARE OF target_grant;
    IF NOT FOUND THEN
        v_event_id := gen_random_uuid();
        INSERT INTO public.break_glass_denial_events (
            id, attempted_grant_id, operator_id, operator_session_id,
            action, scope, outcome_code, created_at
        ) VALUES (
            v_event_id, p_grant_id, v_operator_id, v_session_id,
            CASE p_scope
                WHEN 'REQUEST_CONTENT_READ' THEN 'REQUEST_CONTENT_DENIED'
                ELSE 'ARTIFACT_DENIED'
            END::public.break_glass_event_action,
            p_scope, 'TARGET_NOT_FOUND', v_now
        );
        RETURN QUERY SELECT false, v_event_id, v_operator_id, v_session_id, v_now,
            NULL::uuid, NULL::uuid, NULL::uuid, NULL::uuid, NULL::timestamptz,
            false, 'TARGET_NOT_FOUND'::public.break_glass_outcome_code;
        RETURN;
    END IF;

    SELECT target_request.* INTO v_request
    FROM public.break_glass_requests AS target_request
    JOIN public.jobs AS job
      ON job.organization_id = target_request.organization_id
     AND job.project_id = target_request.project_id
     AND job.id = target_request.job_id
    WHERE target_request.id = v_grant.request_id
      AND target_request.organization_id = v_grant.organization_id
      AND target_request.project_id = v_grant.project_id
      AND target_request.job_id = v_grant.job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Break-glass target integrity is unavailable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'break_glass_target_integrity';
    END IF;

    IF v_grant.revoked_at IS NOT NULL THEN
        v_outcome := 'GRANT_REVOKED';
    ELSIF v_grant.expires_at <= v_now THEN
        v_outcome := 'GRANT_EXPIRED';
    ELSIF NOT (p_scope = ANY(v_request.scopes)) THEN
        v_outcome := 'SCOPE_DENIED';
    ELSE
        v_outcome := 'ALLOWED';
        v_authorized := true;
    END IF;

    RETURN QUERY SELECT true, NULL::uuid, v_operator_id, v_session_id, v_now,
        v_grant.request_id, v_grant.organization_id, v_grant.project_id,
        v_grant.job_id, v_grant.expires_at, v_authorized, v_outcome;
END
$$;
REVOKE ALL ON FUNCTION vela_private.authorize_break_glass_scope(
    uuid, public.break_glass_scope
) FROM PUBLIC;
ALTER FUNCTION vela_private.authorize_break_glass_scope(
    uuid, public.break_glass_scope
) OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_authorize_break_glass_request_content(
    p_grant_id uuid
) RETURNS TABLE (
    authorized boolean,
    outcome_code text,
    event_id uuid,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    request_id uuid,
    grant_expires_at timestamptz,
    request_content jsonb
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_authorization record;
    v_request_content jsonb;
    v_content_expires_at timestamptz;
    v_content_deleted_at timestamptz;
    v_outcome public.break_glass_outcome_code;
    v_authorized boolean := false;
    v_event_id uuid;
BEGIN
    SELECT scope_authorization.* INTO v_authorization
    FROM vela_private.authorize_break_glass_scope(
        p_grant_id, 'REQUEST_CONTENT_READ'
    ) AS scope_authorization;
    IF NOT v_authorization.target_found THEN
        RETURN QUERY SELECT false, v_authorization.outcome_code::text,
            v_authorization.evidence_event_id, NULL::uuid,
            NULL::uuid, NULL::uuid, NULL::uuid, NULL::timestamptz, NULL::jsonb;
        RETURN;
    END IF;
    v_outcome := v_authorization.outcome_code;
    v_authorized := v_authorization.grant_authorized;
    IF v_authorized THEN
        v_authorized := false;
        SELECT job.request_content, job.request_content_expires_at,
            job.request_content_deleted_at
        INTO v_request_content, v_content_expires_at, v_content_deleted_at
        FROM public.jobs AS job
        WHERE job.organization_id = v_authorization.organization_id
          AND job.project_id = v_authorization.project_id
          AND job.id = v_authorization.job_id;
        IF NOT FOUND THEN
            v_outcome := 'TARGET_NOT_FOUND';
        ELSIF v_content_deleted_at IS NOT NULL
            OR v_request_content = '{"deleted":true}'::jsonb
        THEN
            v_outcome := 'CONTENT_DELETED';
        ELSIF v_content_expires_at <= v_authorization.transaction_time THEN
            v_outcome := 'CONTENT_EXPIRED';
        ELSE
            v_outcome := 'ALLOWED';
            v_authorized := true;
        END IF;
    END IF;

    v_event_id := gen_random_uuid();
    INSERT INTO public.break_glass_events (
        id, organization_id, project_id, job_id, request_id, grant_id,
        operator_id, operator_session_id, action, scope, outcome_code, created_at
    ) VALUES (
        v_event_id, v_authorization.organization_id, v_authorization.project_id,
        v_authorization.job_id, v_authorization.request_id, p_grant_id,
        v_authorization.operator_id, v_authorization.operator_session_id,
        (CASE WHEN v_authorized THEN 'REQUEST_CONTENT_AUTHORIZED'
            ELSE 'REQUEST_CONTENT_DENIED' END)::public.break_glass_event_action,
        'REQUEST_CONTENT_READ', v_outcome, v_authorization.transaction_time
    );
    RETURN QUERY SELECT v_authorized, v_outcome::text, v_event_id,
        v_authorization.organization_id, v_authorization.project_id,
        v_authorization.job_id, v_authorization.request_id,
        v_authorization.grant_expires_at,
        CASE WHEN v_authorized THEN v_request_content ELSE NULL::jsonb END;
END
$$;
REVOKE ALL ON FUNCTION vela_authorize_break_glass_request_content(uuid) FROM PUBLIC;
ALTER FUNCTION vela_authorize_break_glass_request_content(uuid)
    OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_private.available_break_glass_artifact_set(
    p_organization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_read_at timestamptz
) RETURNS uuid
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_artifact_set_id uuid;
    v_artifact_count integer := 0;
    v_artifact record;
BEGIN
    SELECT artifact_set.id INTO v_artifact_set_id
    FROM public.jobs AS job
    JOIN public.artifact_sets AS artifact_set
      ON artifact_set.organization_id = job.organization_id
     AND artifact_set.project_id = job.project_id
     AND artifact_set.job_id = job.id
     AND artifact_set.id = job.result_artifact_set_id
    WHERE job.organization_id = p_organization_id
      AND job.project_id = p_project_id
      AND job.id = p_job_id
      AND job.state = 'SUCCEEDED'
      AND job.request_content_deleted_at IS NULL
      AND artifact_set.retention_expires_at > p_read_at
    FOR SHARE OF job;
    IF NOT FOUND THEN
        RETURN NULL;
    END IF;

    FOR v_artifact IN
        SELECT artifact.state, artifact.retention_expires_at
        FROM public.artifact_set_items AS item
        JOIN public.artifacts AS artifact
          ON artifact.organization_id = item.organization_id
         AND artifact.project_id = item.project_id
         AND artifact.id = item.artifact_id
        WHERE item.organization_id = p_organization_id
          AND item.project_id = p_project_id
          AND item.job_id = p_job_id
          AND item.artifact_set_id = v_artifact_set_id
        FOR SHARE OF artifact
    LOOP
        v_artifact_count := v_artifact_count + 1;
        IF v_artifact.state <> 'COMMITTED'
            OR v_artifact.retention_expires_at IS NULL
            OR v_artifact.retention_expires_at <= p_read_at
        THEN
            RETURN NULL;
        END IF;
    END LOOP;
    IF v_artifact_count = 0 THEN
        RETURN NULL;
    END IF;
    RETURN v_artifact_set_id;
END
$$;
REVOKE ALL ON FUNCTION vela_private.available_break_glass_artifact_set(
    uuid, uuid, uuid, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_private.available_break_glass_artifact_set(
    uuid, uuid, uuid, timestamptz
) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_private.available_break_glass_artifact_set(
    uuid, uuid, uuid, timestamptz
) TO vela_break_glass_owner;

CREATE FUNCTION vela_authorize_break_glass_artifacts(
    p_grant_id uuid
) RETURNS TABLE (
    authorized boolean,
    outcome_code text,
    event_id uuid,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    request_id uuid,
    grant_expires_at timestamptz,
    artifact_set_id uuid,
    retention_expires_at timestamptz,
    committed_at timestamptz,
    artifact_id uuid,
    artifact_kind text,
    ordinal integer,
    object_key text,
    object_version_id text,
    size_bytes bigint,
    sha256 bytea,
    content_type text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_authorization record;
    v_artifact_set public.artifact_sets%ROWTYPE;
    v_artifact_set_id uuid;
    v_outcome public.break_glass_outcome_code;
    v_authorized boolean := false;
    v_event_id uuid;
BEGIN
    SELECT scope_authorization.* INTO v_authorization
    FROM vela_private.authorize_break_glass_scope(
        p_grant_id, 'ARTIFACT_READ'
    ) AS scope_authorization;
    IF NOT v_authorization.target_found THEN
        RETURN QUERY SELECT false, v_authorization.outcome_code::text,
            v_authorization.evidence_event_id, NULL::uuid,
            NULL::uuid, NULL::uuid, NULL::uuid, NULL::timestamptz, NULL::uuid,
            NULL::timestamptz, NULL::timestamptz, NULL::uuid, NULL::text,
            NULL::integer, NULL::text, NULL::text, NULL::bigint, NULL::bytea, NULL::text;
        RETURN;
    END IF;
    v_outcome := v_authorization.outcome_code;
    v_authorized := v_authorization.grant_authorized;
    IF v_authorized THEN
        v_authorized := false;
        v_artifact_set_id := vela_private.available_break_glass_artifact_set(
            v_authorization.organization_id,
            v_authorization.project_id,
            v_authorization.job_id,
            v_authorization.transaction_time
        );
        SELECT artifact_set.* INTO v_artifact_set
        FROM public.artifact_sets AS artifact_set
        WHERE artifact_set.organization_id = v_authorization.organization_id
          AND artifact_set.project_id = v_authorization.project_id
          AND artifact_set.job_id = v_authorization.job_id
          AND artifact_set.id = v_artifact_set_id;
        IF NOT FOUND THEN
            v_outcome := 'CONTENT_UNAVAILABLE';
        ELSE
            v_outcome := 'ALLOWED';
            v_authorized := true;
        END IF;
    END IF;

    v_event_id := gen_random_uuid();
    INSERT INTO public.break_glass_events (
        id, organization_id, project_id, job_id, request_id, grant_id,
        operator_id, operator_session_id, action, scope, outcome_code, created_at
    ) VALUES (
        v_event_id, v_authorization.organization_id, v_authorization.project_id,
        v_authorization.job_id, v_authorization.request_id, p_grant_id,
        v_authorization.operator_id, v_authorization.operator_session_id,
        (CASE WHEN v_authorized THEN 'ARTIFACT_AUTHORIZED'
            ELSE 'ARTIFACT_DENIED' END)::public.break_glass_event_action,
        'ARTIFACT_READ', v_outcome, v_authorization.transaction_time
    );
    IF NOT v_authorized THEN
        RETURN QUERY SELECT false, v_outcome::text, v_event_id,
            v_authorization.organization_id, v_authorization.project_id,
            v_authorization.job_id, v_authorization.request_id,
            v_authorization.grant_expires_at, NULL::uuid,
            NULL::timestamptz, NULL::timestamptz, NULL::uuid, NULL::text,
            NULL::integer, NULL::text, NULL::text, NULL::bigint, NULL::bytea, NULL::text;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT true, v_outcome::text, v_event_id,
        v_authorization.organization_id, v_authorization.project_id,
        v_authorization.job_id, v_authorization.request_id,
        v_authorization.grant_expires_at, v_artifact_set.id,
        v_artifact_set.retention_expires_at, v_artifact_set.committed_at,
        item.artifact_id, item.kind::text, item.ordinal, item.object_key,
        item.object_version_id, item.size_bytes, item.sha256, item.content_type
    FROM public.artifact_set_items AS item
    WHERE item.artifact_set_id = v_artifact_set.id
    ORDER BY CASE item.kind WHEN 'VIDEO' THEN 0 ELSE 1 END, item.ordinal;
END
$$;
REVOKE ALL ON FUNCTION vela_authorize_break_glass_artifacts(uuid) FROM PUBLIC;
ALTER FUNCTION vela_authorize_break_glass_artifacts(uuid)
    OWNER TO vela_break_glass_owner;

CREATE FUNCTION vela_record_break_glass_artifact_delivery(
    p_authorization_event_id uuid,
    p_signing_succeeded boolean
) RETURNS TABLE (
    delivered boolean,
    outcome_code text,
    event_id uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_operator_id uuid;
    v_session_id uuid;
    v_now timestamptz;
    v_authorization public.break_glass_events%ROWTYPE;
    v_grant public.break_glass_grants%ROWTYPE;
    v_grant_found boolean;
    v_artifact_set_id uuid;
    v_delivered boolean;
    v_outcome public.break_glass_outcome_code;
    v_event_id uuid := gen_random_uuid();
BEGIN
    SELECT context.operator_id, context.operator_session_id, context.transaction_time
    INTO v_operator_id, v_session_id, v_now
    FROM vela_private.current_break_glass_request_context() AS context;
    SELECT event.* INTO v_authorization
    FROM public.break_glass_events AS event
    WHERE event.id = p_authorization_event_id
      AND event.action = 'ARTIFACT_AUTHORIZED'
      AND event.operator_id = v_operator_id
      AND event.operator_session_id = v_session_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Artifact authorization event is not found' USING ERRCODE = '02000';
    END IF;
    SELECT target_grant.* INTO v_grant
    FROM public.break_glass_grants AS target_grant
    JOIN public.jobs AS job
      ON job.organization_id = target_grant.organization_id
     AND job.project_id = target_grant.project_id
     AND job.id = target_grant.job_id
    WHERE target_grant.id = v_authorization.grant_id
      AND target_grant.organization_id = v_authorization.organization_id
      AND target_grant.project_id = v_authorization.project_id
      AND target_grant.job_id = v_authorization.job_id
    FOR SHARE OF target_grant;
    v_grant_found := FOUND;

    v_delivered := false;
    IF NOT p_signing_succeeded THEN
        v_outcome := 'SIGNING_FAILED';
    ELSIF NOT v_grant_found
        OR v_grant.revoked_at IS NOT NULL
        OR v_grant.expires_at <= v_now
    THEN
        v_outcome := 'GRANT_INACTIVE';
    ELSE
        v_artifact_set_id := vela_private.available_break_glass_artifact_set(
            v_authorization.organization_id,
            v_authorization.project_id,
            v_authorization.job_id,
            v_now
        );
        IF v_artifact_set_id IS NULL THEN
            v_outcome := 'CONTENT_UNAVAILABLE';
        ELSE
            v_delivered := true;
            v_outcome := 'DELIVERED';
        END IF;
    END IF;
    INSERT INTO public.break_glass_events (
        id, organization_id, project_id, job_id, request_id, grant_id,
        operator_id, operator_session_id, action, scope, outcome_code, created_at
    ) VALUES (
        v_event_id, v_authorization.organization_id, v_authorization.project_id,
        v_authorization.job_id, v_authorization.request_id, v_authorization.grant_id,
        v_operator_id, v_session_id,
        (CASE WHEN v_delivered THEN 'ARTIFACT_DELIVERED'
            ELSE 'ARTIFACT_DELIVERY_FAILED' END)::public.break_glass_event_action,
        'ARTIFACT_READ', v_outcome, v_now
    );
    RETURN QUERY SELECT v_delivered, v_outcome::text, v_event_id;
END
$$;
REVOKE ALL ON FUNCTION vela_record_break_glass_artifact_delivery(uuid, boolean)
FROM PUBLIC;
ALTER FUNCTION vela_record_break_glass_artifact_delivery(uuid, boolean)
    OWNER TO vela_break_glass_owner;

GRANT USAGE ON SCHEMA public TO vela_platform_operator_auth, vela_break_glass_request;
GRANT EXECUTE ON FUNCTION vela_authenticate_platform_operator_oidc(
    text, text, bytea, timestamptz
) TO vela_platform_operator_auth;
GRANT EXECUTE ON FUNCTION vela_set_break_glass_request_context(uuid, bytea),
    vela_create_break_glass_request(
        uuid, text, bytea, uuid, uuid, uuid, break_glass_scope[],
        break_glass_reason_code, text, integer
    ),
    vela_approve_break_glass_request(uuid, uuid),
    vela_revoke_break_glass_grant(uuid),
    vela_get_break_glass_request(uuid),
    vela_get_break_glass_grant(uuid),
    vela_authorize_break_glass_request_content(uuid),
    vela_authorize_break_glass_artifacts(uuid),
    vela_record_break_glass_artifact_delivery(uuid, boolean)
TO vela_break_glass_request;

GRANT USAGE ON SCHEMA public, vela_private TO vela_break_glass_owner;
GRANT SELECT ON jobs, artifact_sets, artifact_set_items, artifacts
TO vela_break_glass_owner;
GRANT SELECT ON break_glass_events TO vela_internal;

CREATE OR REPLACE FUNCTION vela_list_organization_audit_events(
    p_organization_id uuid,
    p_limit integer
) RETURNS TABLE (
    event_id uuid,
    source text,
    action text,
    project_id uuid,
    actor_principal_id uuid,
    actor_session_id uuid,
    target_kind text,
    target_id uuid,
    created_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    IF p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'audit event list limit must be between 1 and 100'
            USING ERRCODE = '22023';
    END IF;

    PERFORM 1
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_audit:read'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Organization audit authorization is required'
            USING ERRCODE = '42501';
    END IF;

    RETURN QUERY
    SELECT event.*
    FROM (
        SELECT
            human_event.id AS event_id,
            'HUMAN_IDENTITY'::text AS source,
            human_event.action::text AS action,
            human_event.project_id,
            human_event.actor_principal_id,
            human_event.actor_session_id,
            'HUMAN_PRINCIPAL'::text AS target_kind,
            human_event.target_principal_id AS target_id,
            human_event.created_at
        FROM public.human_identity_events AS human_event
        WHERE human_event.organization_id = p_organization_id
        UNION ALL
        SELECT
            project_event.id,
            'PROJECT_IDENTITY'::text,
            project_event.action::text,
            project_event.project_id,
            project_event.actor_principal_id,
            project_event.actor_session_id,
            CASE
                WHEN project_event.credential_id IS NULL THEN 'SERVICE_PRINCIPAL'
                ELSE 'CREDENTIAL'
            END::text,
            COALESCE(project_event.credential_id, project_event.target_principal_id),
            project_event.created_at
        FROM public.project_identity_events AS project_event
        WHERE project_event.organization_id = p_organization_id
        UNION ALL
        SELECT
            contact_event.id,
            'SETTLEMENT_CONTACT'::text,
            contact_event.action::text,
            NULL::uuid,
            contact_event.actor_principal_id,
            contact_event.actor_session_id,
            'SETTLEMENT_CONTACT'::text,
            contact_event.contact_id,
            contact_event.created_at
        FROM public.organization_settlement_contact_events AS contact_event
        WHERE contact_event.organization_id = p_organization_id
        UNION ALL
        SELECT
            break_glass_event.id,
            'BREAK_GLASS'::text,
            break_glass_event.action::text,
            break_glass_event.project_id,
            break_glass_event.operator_id,
            break_glass_event.operator_session_id,
            'JOB'::text,
            break_glass_event.job_id,
            break_glass_event.created_at
        FROM public.break_glass_events AS break_glass_event
        WHERE break_glass_event.organization_id = p_organization_id
    ) AS event
    ORDER BY event.created_at DESC, event.event_id DESC
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_organization_audit_events(uuid, integer)
FROM PUBLIC;
ALTER FUNCTION vela_list_organization_audit_events(uuid, integer)
OWNER TO vela_internal;

CREATE FUNCTION vela_list_organization_audit_events_v2(
    p_organization_id uuid,
    p_limit integer
) RETURNS TABLE (
    event_id uuid,
    source text,
    action text,
    project_id uuid,
    actor_principal_id uuid,
    actor_session_id uuid,
    target_kind text,
    target_id uuid,
    created_at timestamptz,
    scope text,
    outcome_code text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    RETURN QUERY
    SELECT
        event.event_id,
        event.source,
        event.action,
        event.project_id,
        event.actor_principal_id,
        event.actor_session_id,
        event.target_kind,
        event.target_id,
        event.created_at,
        break_glass_event.scope::text,
        break_glass_event.outcome_code::text
    FROM public.vela_list_organization_audit_events(
        p_organization_id,
        p_limit
    ) AS event
    LEFT JOIN public.break_glass_events AS break_glass_event
      ON event.source = 'BREAK_GLASS'
     AND break_glass_event.id = event.event_id
    ORDER BY event.created_at DESC, event.event_id DESC;
END
$$;
REVOKE ALL ON FUNCTION vela_list_organization_audit_events_v2(uuid, integer)
FROM PUBLIC;
ALTER FUNCTION vela_list_organization_audit_events_v2(uuid, integer)
OWNER TO vela_internal;
GRANT USAGE ON SCHEMA public TO vela_break_glass_audit_request;
GRANT EXECUTE ON FUNCTION vela_set_organization_identity_admin_context(uuid, bytea, text),
    vela_list_organization_audit_events_v2(uuid, integer)
TO vela_break_glass_audit_request;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE break_glass_denial_events, break_glass_events,
    break_glass_grants, break_glass_requests,
    platform_operator_auth_sessions, platform_operator_oidc_bindings
    IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM platform_operator_oidc_bindings)
        OR EXISTS (SELECT 1 FROM platform_operator_auth_sessions)
        OR EXISTS (SELECT 1 FROM break_glass_requests)
        OR EXISTS (SELECT 1 FROM break_glass_grants)
        OR EXISTS (SELECT 1 FROM break_glass_events)
        OR EXISTS (SELECT 1 FROM break_glass_denial_events)
    THEN
        RAISE EXCEPTION 'cannot remove Break-glass Access with durable evidence'
            USING ERRCODE = '55000',
                CONSTRAINT = 'break_glass_contract_requires_empty_evidence';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_set_organization_identity_admin_context(uuid, bytea, text),
    vela_list_organization_audit_events_v2(uuid, integer)
FROM vela_break_glass_audit_request;
REVOKE USAGE ON SCHEMA public FROM vela_break_glass_audit_request;
DROP FUNCTION vela_list_organization_audit_events_v2(uuid, integer);

CREATE OR REPLACE FUNCTION vela_list_organization_audit_events(
    p_organization_id uuid,
    p_limit integer
) RETURNS TABLE (
    event_id uuid,
    source text,
    action text,
    project_id uuid,
    actor_principal_id uuid,
    actor_session_id uuid,
    target_kind text,
    target_id uuid,
    created_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    IF p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'audit event list limit must be between 1 and 100'
            USING ERRCODE = '22023';
    END IF;

    PERFORM 1
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_audit:read'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Organization audit authorization is required'
            USING ERRCODE = '42501';
    END IF;

    RETURN QUERY
    SELECT event.*
    FROM (
        SELECT
            human_event.id AS event_id,
            'HUMAN_IDENTITY'::text AS source,
            human_event.action::text AS action,
            human_event.project_id,
            human_event.actor_principal_id,
            human_event.actor_session_id,
            'HUMAN_PRINCIPAL'::text AS target_kind,
            human_event.target_principal_id AS target_id,
            human_event.created_at
        FROM public.human_identity_events AS human_event
        WHERE human_event.organization_id = p_organization_id
        UNION ALL
        SELECT
            project_event.id,
            'PROJECT_IDENTITY'::text,
            project_event.action::text,
            project_event.project_id,
            project_event.actor_principal_id,
            project_event.actor_session_id,
            CASE
                WHEN project_event.credential_id IS NULL THEN 'SERVICE_PRINCIPAL'
                ELSE 'CREDENTIAL'
            END::text,
            COALESCE(project_event.credential_id, project_event.target_principal_id),
            project_event.created_at
        FROM public.project_identity_events AS project_event
        WHERE project_event.organization_id = p_organization_id
        UNION ALL
        SELECT
            contact_event.id,
            'SETTLEMENT_CONTACT'::text,
            contact_event.action::text,
            NULL::uuid,
            contact_event.actor_principal_id,
            contact_event.actor_session_id,
            'SETTLEMENT_CONTACT'::text,
            contact_event.contact_id,
            contact_event.created_at
        FROM public.organization_settlement_contact_events AS contact_event
        WHERE contact_event.organization_id = p_organization_id
    ) AS event
    ORDER BY event.created_at DESC, event.event_id DESC
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_organization_audit_events(uuid, integer)
FROM PUBLIC;
ALTER FUNCTION vela_list_organization_audit_events(uuid, integer)
OWNER TO vela_internal;
REVOKE SELECT ON break_glass_events FROM vela_internal;

REVOKE SELECT ON jobs, artifact_sets, artifact_set_items, artifacts
FROM vela_break_glass_owner;
REVOKE EXECUTE ON FUNCTION vela_private.available_break_glass_artifact_set(
    uuid, uuid, uuid, timestamptz
) FROM vela_break_glass_owner;
REVOKE USAGE ON SCHEMA public, vela_private FROM vela_break_glass_owner;
REVOKE EXECUTE ON FUNCTION vela_set_break_glass_request_context(uuid, bytea),
    vela_create_break_glass_request(
        uuid, text, bytea, uuid, uuid, uuid, break_glass_scope[],
        break_glass_reason_code, text, integer
    ),
    vela_approve_break_glass_request(uuid, uuid),
    vela_revoke_break_glass_grant(uuid),
    vela_get_break_glass_request(uuid),
    vela_get_break_glass_grant(uuid),
    vela_authorize_break_glass_request_content(uuid),
    vela_authorize_break_glass_artifacts(uuid),
    vela_record_break_glass_artifact_delivery(uuid, boolean)
FROM vela_break_glass_request;
REVOKE EXECUTE ON FUNCTION vela_authenticate_platform_operator_oidc(
    text, text, bytea, timestamptz
) FROM vela_platform_operator_auth;
REVOKE USAGE ON SCHEMA public FROM vela_platform_operator_auth, vela_break_glass_request;

DROP FUNCTION vela_record_break_glass_artifact_delivery(uuid, boolean);
DROP FUNCTION vela_authorize_break_glass_artifacts(uuid);
DROP FUNCTION vela_private.available_break_glass_artifact_set(
    uuid, uuid, uuid, timestamptz
);
DROP FUNCTION vela_authorize_break_glass_request_content(uuid);
DROP FUNCTION vela_private.authorize_break_glass_scope(uuid, break_glass_scope);
DROP FUNCTION vela_get_break_glass_grant(uuid);
DROP FUNCTION vela_get_break_glass_request(uuid);
DROP FUNCTION vela_revoke_break_glass_grant(uuid);
DROP FUNCTION vela_approve_break_glass_request(uuid, uuid);
DROP FUNCTION vela_create_break_glass_request(
    uuid, text, bytea, uuid, uuid, uuid, break_glass_scope[],
    break_glass_reason_code, text, integer
);
DROP FUNCTION vela_private.current_break_glass_request_context();
DROP FUNCTION vela_set_break_glass_request_context(uuid, bytea);
DROP FUNCTION vela_authenticate_platform_operator_oidc(text, text, bytea, timestamptz);

DROP TRIGGER break_glass_denial_events_immutable ON break_glass_denial_events;
DROP TRIGGER break_glass_events_immutable ON break_glass_events;
DROP TRIGGER break_glass_grants_no_delete ON break_glass_grants;
DROP TRIGGER break_glass_grants_immutable ON break_glass_grants;
DROP TRIGGER break_glass_requests_immutable ON break_glass_requests;
DROP TRIGGER platform_operator_auth_sessions_immutable ON platform_operator_auth_sessions;
DROP TRIGGER platform_operator_oidc_bindings_no_delete ON platform_operator_oidc_bindings;
DROP TRIGGER platform_operator_oidc_bindings_immutable ON platform_operator_oidc_bindings;
DROP FUNCTION vela_enforce_break_glass_grant_immutability();
DROP FUNCTION vela_enforce_platform_operator_binding_immutability();
DROP FUNCTION vela_reject_break_glass_evidence_mutation();

DROP TABLE break_glass_denial_events;
DROP TABLE break_glass_events;
DROP TABLE break_glass_grants;
DROP TABLE break_glass_requests;
DROP TABLE vela_private.break_glass_request_contexts;
DROP TABLE platform_operator_auth_sessions;
DROP TABLE platform_operator_oidc_bindings;
DROP FUNCTION vela_break_glass_scopes_valid(break_glass_scope[]);
DROP TYPE break_glass_event_action;
DROP TYPE break_glass_outcome_code;
DROP TYPE break_glass_reason_code;
DROP TYPE break_glass_scope;
-- +goose StatementEnd
