-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_organization_role_scopes(
    p_role organization_role
) RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE p_role
        WHEN 'OrganizationOwner'::public.organization_role THEN
            ARRAY[
                'organization_audit:read',
                'organization_billing:read',
                'organization_billing_contacts:manage',
                'organization_billing_contacts:read',
                'organization_members:manage',
                'organization_members:read',
                'organization_usage:read',
                'project_members:manage',
                'project_members:read'
            ]::text[]
        WHEN 'BillingAdmin'::public.organization_role THEN
            ARRAY[
                'organization_billing:read',
                'organization_billing_contacts:manage',
                'organization_billing_contacts:read',
                'organization_usage:read'
            ]::text[]
        WHEN 'OrganizationAuditor'::public.organization_role THEN
            ARRAY[
                'organization_audit:read',
                'organization_usage:read'
            ]::text[]
    END
$$;
REVOKE ALL ON FUNCTION vela_organization_role_scopes(organization_role) FROM PUBLIC;
ALTER FUNCTION vela_organization_role_scopes(organization_role) OWNER TO vela_internal;

CREATE TYPE settlement_contact_action AS ENUM (
    'SETTLEMENT_CONTACT_CREATED',
    'SETTLEMENT_CONTACT_DISABLED'
);

CREATE TABLE organization_settlement_contacts (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    display_name text NOT NULL CHECK (length(display_name) BETWEEN 1 AND 200),
    normalized_email text NOT NULL CHECK (
        octet_length(normalized_email) BETWEEN 3 AND 320
        AND normalized_email = lower(normalized_email)
        AND normalized_email = btrim(normalized_email)
        AND normalized_email !~ '[[:space:]]'
        AND length(normalized_email) - length(replace(normalized_email, '@', '')) = 1
        AND position('@' IN normalized_email) BETWEEN 2 AND length(normalized_email) - 1
    ),
    created_by_principal_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    disabled_at timestamptz,
    disabled_by_principal_id uuid,
    CONSTRAINT organization_settlement_contacts_email_key
        UNIQUE (organization_id, normalized_email),
    UNIQUE (organization_id, id),
    FOREIGN KEY (organization_id) REFERENCES customer_organizations(id),
    FOREIGN KEY (organization_id, created_by_principal_id)
        REFERENCES principals(organization_id, id),
    FOREIGN KEY (organization_id, disabled_by_principal_id)
        REFERENCES principals(organization_id, id),
    CHECK (
        (disabled_at IS NULL AND disabled_by_principal_id IS NULL)
        OR (disabled_at IS NOT NULL AND disabled_by_principal_id IS NOT NULL
            AND disabled_at >= created_at)
    )
);
CREATE INDEX organization_settlement_contacts_list_idx
    ON organization_settlement_contacts (organization_id, created_at DESC, id DESC);

CREATE TABLE organization_settlement_contact_events (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    actor_principal_id uuid NOT NULL,
    actor_session_id uuid NOT NULL,
    action settlement_contact_action NOT NULL,
    contact_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, id),
    FOREIGN KEY (organization_id, actor_session_id, actor_principal_id)
        REFERENCES human_administration_actor_attributions(
            organization_id, actor_session_id, actor_principal_id
        ),
    FOREIGN KEY (organization_id, contact_id)
        REFERENCES organization_settlement_contacts(organization_id, id)
);
CREATE INDEX organization_settlement_contact_events_list_idx
    ON organization_settlement_contact_events (organization_id, created_at DESC, id DESC);

ALTER TABLE organization_settlement_contacts
    OWNER TO vela_organization_reporting_owner;
ALTER TABLE organization_settlement_contact_events
    OWNER TO vela_organization_reporting_owner;

ALTER TABLE organization_settlement_contacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE organization_settlement_contacts FORCE ROW LEVEL SECURITY;
ALTER TABLE organization_settlement_contact_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE organization_settlement_contact_events FORCE ROW LEVEL SECURITY;
GRANT SELECT ON organization_settlement_contacts,
    organization_settlement_contact_events TO vela_internal;
GRANT USAGE ON SCHEMA public, vela_private TO vela_organization_reporting_owner;
GRANT SELECT ON vela_private.human_administration_contexts
    TO vela_organization_reporting_owner;
GRANT SELECT, INSERT ON human_administration_actor_attributions
    TO vela_organization_reporting_owner;

CREATE FUNCTION vela_reject_settlement_contact_identity_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(
        NEW.id,
        NEW.organization_id,
        NEW.display_name,
        NEW.normalized_email,
        NEW.created_by_principal_id,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.organization_id,
        OLD.display_name,
        OLD.normalized_email,
        OLD.created_by_principal_id,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'settlement contact identity is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'settlement_contact_identity_is_immutable';
    END IF;
    IF OLD.disabled_at IS NOT NULL
        AND ROW(NEW.disabled_at, NEW.disabled_by_principal_id)
            IS DISTINCT FROM ROW(OLD.disabled_at, OLD.disabled_by_principal_id)
    THEN
        RAISE EXCEPTION 'settlement contact disablement is permanent'
            USING ERRCODE = '55000',
                CONSTRAINT = 'settlement_contact_disablement_is_permanent';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_settlement_contact_identity_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_settlement_contact_identity_mutation()
    OWNER TO vela_organization_reporting_owner;
CREATE TRIGGER organization_settlement_contact_identity_immutable
BEFORE UPDATE ON organization_settlement_contacts
FOR EACH ROW EXECUTE FUNCTION vela_reject_settlement_contact_identity_mutation();

CREATE FUNCTION vela_reject_settlement_contact_evidence_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'settlement contact evidence is immutable'
        USING ERRCODE = '55000',
            CONSTRAINT = 'settlement_contact_evidence_is_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_settlement_contact_evidence_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_settlement_contact_evidence_mutation()
    OWNER TO vela_organization_reporting_owner;
CREATE TRIGGER organization_settlement_contacts_no_delete
BEFORE DELETE ON organization_settlement_contacts
FOR EACH ROW EXECUTE FUNCTION vela_reject_settlement_contact_evidence_mutation();
CREATE TRIGGER organization_settlement_contact_events_immutable
BEFORE UPDATE OR DELETE ON organization_settlement_contact_events
FOR EACH ROW EXECUTE FUNCTION vela_reject_settlement_contact_evidence_mutation();

CREATE OR REPLACE FUNCTION vela_set_organization_identity_admin_context(
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
        'organization_audit:read',
        'organization_billing:read',
        'organization_billing_contacts:manage',
        'organization_billing_contacts:read',
        'organization_members:manage',
        'organization_members:read',
        'organization_usage:read'
    ) OR octet_length(p_actor_session_proof) IS DISTINCT FROM 32 THEN
        RAISE EXCEPTION 'invalid Organization administration request'
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

CREATE FUNCTION vela_get_organization_credit_summary(
    p_organization_id uuid
) RETURNS TABLE (
    organization_id uuid,
    currency text,
    contract_credit_limit_minor bigint,
    reserved_minor bigint,
    unsettled_posted_minor bigint,
    available_minor bigint,
    version bigint,
    updated_at timestamptz
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
      AND context.required_scope = 'organization_billing:read'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Organization billing authorization is required'
            USING ERRCODE = '42501';
    END IF;

    RETURN QUERY
    SELECT
        account.organization_id,
        account.currency,
        account.contract_credit_limit_minor,
        account.reserved_minor,
        account.unsettled_posted_minor,
        account.contract_credit_limit_minor
            - account.reserved_minor
            - account.unsettled_posted_minor,
        account.version,
        account.updated_at
    FROM public.organization_credit_accounts AS account
    JOIN public.customer_organizations AS organization
      ON organization.id = account.organization_id
     AND organization.status = 'ACTIVE'
    WHERE account.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Customer Organization is not visible' USING ERRCODE = 'P0002';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_get_organization_credit_summary(uuid) FROM PUBLIC;
ALTER FUNCTION vela_get_organization_credit_summary(uuid) OWNER TO vela_internal;

CREATE FUNCTION vela_private.require_organization_billing_context(
    p_organization_id uuid
) RETURNS void
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
      AND context.required_scope = 'organization_billing:read'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Organization billing authorization is required'
            USING ERRCODE = '42501';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.require_organization_billing_context(uuid)
FROM PUBLIC;
ALTER FUNCTION vela_private.require_organization_billing_context(uuid)
OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_private.require_organization_billing_context(uuid)
TO vela_billing_owner;

CREATE FUNCTION vela_list_organization_charges(
    p_organization_id uuid,
    p_limit integer
) RETURNS TABLE (
    charge_id uuid,
    project_id uuid,
    job_id uuid,
    reason text,
    amount_minor bigint,
    currency text,
    posted_at timestamptz,
    external_invoice_reference text,
    external_line_reference text,
    exported_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    IF p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'Charge list limit must be between 1 and 100'
            USING ERRCODE = '22023';
    END IF;

    PERFORM vela_private.require_organization_billing_context(p_organization_id);

    RETURN QUERY
    SELECT
        charge.id,
        charge.project_id,
        charge.job_id,
        charge.reason::text,
        charge.amount_minor,
        charge.currency,
        charge.posted_at,
        receipt.external_invoice_reference,
        receipt.external_line_reference,
        receipt.exported_at
    FROM public.charges AS charge
    LEFT JOIN public.invoice_export_receipts AS receipt
      ON receipt.organization_id = charge.organization_id
     AND receipt.project_id = charge.project_id
     AND receipt.job_id = charge.job_id
     AND receipt.charge_id = charge.id
    WHERE charge.organization_id = p_organization_id
    ORDER BY charge.posted_at DESC, charge.id DESC
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_organization_charges(uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_list_organization_charges(uuid, integer) OWNER TO vela_billing_owner;

CREATE FUNCTION vela_private.record_settlement_contact_event(
    p_action settlement_contact_action,
    p_contact_id uuid
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
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_billing_contacts:manage';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human settlement-contact authorization is required'
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
        'ORGANIZATION',
        NULL,
        v_context.established_at
    ) ON CONFLICT ON CONSTRAINT human_administration_actor_attributions_pkey
        DO NOTHING;

    INSERT INTO public.organization_settlement_contact_events (
        id,
        organization_id,
        actor_principal_id,
        actor_session_id,
        action,
        contact_id
    ) VALUES (
        gen_random_uuid(),
        v_context.organization_id,
        v_context.principal_id,
        v_context.actor_session_id,
        p_action,
        p_contact_id
    );
END
$$;
REVOKE ALL ON FUNCTION vela_private.record_settlement_contact_event(
    settlement_contact_action, uuid
) FROM PUBLIC;
ALTER FUNCTION vela_private.record_settlement_contact_event(
    settlement_contact_action, uuid
) OWNER TO vela_organization_reporting_owner;

CREATE FUNCTION vela_create_settlement_contact(
    p_contact_id uuid,
    p_organization_id uuid,
    p_display_name text,
    p_normalized_email text
) RETURNS TABLE (
    contact_id uuid,
    organization_id uuid,
    display_name text,
    normalized_email text,
    created_by_principal_id uuid,
    created_at timestamptz,
    disabled_at timestamptz,
    disabled_by_principal_id uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_principal_id uuid;
BEGIN
    SELECT context.principal_id
    INTO v_actor_principal_id
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_billing_contacts:manage'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human settlement-contact authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_contact_id IS NULL OR p_organization_id IS NULL
        OR p_display_name IS NULL OR length(p_display_name) NOT BETWEEN 1 AND 200
        OR p_display_name IS DISTINCT FROM btrim(p_display_name)
        OR p_normalized_email IS NULL
        OR octet_length(p_normalized_email) NOT BETWEEN 3 AND 320
        OR p_normalized_email IS DISTINCT FROM lower(p_normalized_email)
        OR p_normalized_email IS DISTINCT FROM btrim(p_normalized_email)
        OR p_normalized_email ~ '[[:space:]]'
        OR length(p_normalized_email) - length(replace(p_normalized_email, '@', '')) <> 1
        OR position('@' IN p_normalized_email) NOT BETWEEN 2 AND length(p_normalized_email) - 1
    THEN
        RAISE EXCEPTION 'invalid settlement contact definition' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.organization_settlement_contacts (
        id,
        organization_id,
        display_name,
        normalized_email,
        created_by_principal_id
    ) VALUES (
        p_contact_id,
        p_organization_id,
        p_display_name,
        p_normalized_email,
        v_actor_principal_id
    ) ON CONFLICT ON CONSTRAINT organization_settlement_contacts_email_key
        DO NOTHING;

    IF FOUND THEN
        PERFORM vela_private.record_settlement_contact_event(
            'SETTLEMENT_CONTACT_CREATED', p_contact_id
        );
    END IF;

    RETURN QUERY
    SELECT
        contact.id,
        contact.organization_id,
        contact.display_name,
        contact.normalized_email,
        contact.created_by_principal_id,
        contact.created_at,
        contact.disabled_at,
        contact.disabled_by_principal_id
    FROM public.organization_settlement_contacts AS contact
    WHERE contact.organization_id = p_organization_id
      AND contact.normalized_email = p_normalized_email;
    IF NOT FOUND THEN
        RAISE unique_violation USING
            CONSTRAINT = 'organization_settlement_contacts_pkey';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_create_settlement_contact(uuid, uuid, text, text) FROM PUBLIC;
ALTER FUNCTION vela_create_settlement_contact(uuid, uuid, text, text)
    OWNER TO vela_organization_reporting_owner;

CREATE FUNCTION vela_list_settlement_contacts(
    p_organization_id uuid,
    p_limit integer
) RETURNS TABLE (
    contact_id uuid,
    organization_id uuid,
    display_name text,
    normalized_email text,
    created_by_principal_id uuid,
    created_at timestamptz,
    disabled_at timestamptz,
    disabled_by_principal_id uuid
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
      AND context.required_scope = 'organization_billing_contacts:read'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human settlement-contact authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'settlement contact list limit must be between 1 and 100'
            USING ERRCODE = '22023';
    END IF;

    RETURN QUERY
    SELECT
        contact.id,
        contact.organization_id,
        contact.display_name,
        contact.normalized_email,
        contact.created_by_principal_id,
        contact.created_at,
        contact.disabled_at,
        contact.disabled_by_principal_id
    FROM public.organization_settlement_contacts AS contact
    WHERE contact.organization_id = p_organization_id
    ORDER BY contact.created_at DESC, contact.id DESC
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_settlement_contacts(uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_list_settlement_contacts(uuid, integer)
    OWNER TO vela_organization_reporting_owner;

CREATE FUNCTION vela_disable_settlement_contact(
    p_organization_id uuid,
    p_contact_id uuid
) RETURNS TABLE (
    contact_id uuid,
    organization_id uuid,
    display_name text,
    normalized_email text,
    created_by_principal_id uuid,
    created_at timestamptz,
    disabled_at timestamptz,
    disabled_by_principal_id uuid
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_actor_principal_id uuid;
    v_disabled_at timestamptz;
BEGIN
    SELECT context.principal_id
    INTO v_actor_principal_id
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_billing_contacts:manage'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human settlement-contact authorization is required'
            USING ERRCODE = '42501';
    END IF;
    IF p_contact_id IS NULL THEN
        RAISE EXCEPTION 'settlement contact id is required' USING ERRCODE = '22023';
    END IF;

    SELECT contact.disabled_at
    INTO v_disabled_at
    FROM public.organization_settlement_contacts AS contact
    WHERE contact.organization_id = p_organization_id
      AND contact.id = p_contact_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'settlement contact is not visible' USING ERRCODE = 'P0002';
    END IF;

    IF v_disabled_at IS NULL THEN
        UPDATE public.organization_settlement_contacts AS contact
        SET disabled_at = clock_timestamp(),
            disabled_by_principal_id = v_actor_principal_id
        WHERE contact.organization_id = p_organization_id
          AND contact.id = p_contact_id;
        PERFORM vela_private.record_settlement_contact_event(
            'SETTLEMENT_CONTACT_DISABLED', p_contact_id
        );
    END IF;

    RETURN QUERY
    SELECT
        contact.id,
        contact.organization_id,
        contact.display_name,
        contact.normalized_email,
        contact.created_by_principal_id,
        contact.created_at,
        contact.disabled_at,
        contact.disabled_by_principal_id
    FROM public.organization_settlement_contacts AS contact
    WHERE contact.organization_id = p_organization_id
      AND contact.id = p_contact_id;
END
$$;
REVOKE ALL ON FUNCTION vela_disable_settlement_contact(uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_disable_settlement_contact(uuid, uuid)
    OWNER TO vela_organization_reporting_owner;

CREATE FUNCTION vela_get_organization_usage(
    p_organization_id uuid,
    p_from timestamptz,
    p_to timestamptz
) RETURNS TABLE (
    project_id uuid,
    total_jobs bigint,
    queued_jobs bigint,
    assigned_jobs bigint,
    running_jobs bigint,
    finalizing_jobs bigint,
    retry_wait_jobs bigint,
    canceling_jobs bigint,
    succeeded_jobs bigint,
    failed_jobs bigint,
    canceled_jobs bigint,
    quoted_amount_minor bigint,
    posted_charge_amount_minor bigint,
    currency text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
BEGIN
    IF p_from IS NULL OR p_to IS NULL OR p_from >= p_to
        OR p_to - p_from > interval '366 days'
    THEN
        RAISE EXCEPTION 'usage interval must be positive and no greater than 366 days'
            USING ERRCODE = '22023';
    END IF;

    PERFORM 1
    FROM vela_private.human_administration_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id()
      AND context.actor_context_kind = 'ORGANIZATION'
      AND context.required_scope = 'organization_usage:read'
      AND context.organization_id = p_organization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human Organization usage authorization is required'
            USING ERRCODE = '42501';
    END IF;

    RETURN QUERY
    WITH filtered_jobs AS MATERIALIZED (
        SELECT
            job.id,
            job.organization_id,
            job.project_id,
            job.state,
            job.pricing_quoted_amount_minor
        FROM public.jobs AS job
        WHERE job.organization_id = p_organization_id
          AND job.created_at >= p_from
          AND job.created_at < p_to
    ), project_usage AS (
        SELECT
            project.id AS project_id,
            count(job.id)::bigint AS total_jobs,
            count(job.id) FILTER (WHERE job.state = 'QUEUED')::bigint AS queued_jobs,
            count(job.id) FILTER (WHERE job.state = 'ASSIGNED')::bigint AS assigned_jobs,
            count(job.id) FILTER (WHERE job.state = 'RUNNING')::bigint AS running_jobs,
            count(job.id) FILTER (WHERE job.state = 'FINALIZING')::bigint AS finalizing_jobs,
            count(job.id) FILTER (WHERE job.state = 'RETRY_WAIT')::bigint AS retry_wait_jobs,
            count(job.id) FILTER (WHERE job.state = 'CANCELING')::bigint AS canceling_jobs,
            count(job.id) FILTER (WHERE job.state = 'SUCCEEDED')::bigint AS succeeded_jobs,
            count(job.id) FILTER (WHERE job.state = 'FAILED')::bigint AS failed_jobs,
            count(job.id) FILTER (WHERE job.state = 'CANCELED')::bigint AS canceled_jobs,
            COALESCE(sum(job.pricing_quoted_amount_minor), 0)::bigint AS quoted_amount_minor,
            COALESCE(sum(charge.amount_minor), 0)::bigint AS posted_charge_amount_minor
        FROM public.projects AS project
        LEFT JOIN filtered_jobs AS job
          ON job.organization_id = project.organization_id
         AND job.project_id = project.id
        LEFT JOIN public.charges AS charge
          ON charge.organization_id = job.organization_id
         AND charge.project_id = job.project_id
         AND charge.job_id = job.id
        WHERE project.organization_id = p_organization_id
        GROUP BY project.id
    ), usage_rows AS (
        SELECT
            NULL::uuid AS project_id,
            COALESCE(sum(project.total_jobs), 0)::bigint AS total_jobs,
            COALESCE(sum(project.queued_jobs), 0)::bigint AS queued_jobs,
            COALESCE(sum(project.assigned_jobs), 0)::bigint AS assigned_jobs,
            COALESCE(sum(project.running_jobs), 0)::bigint AS running_jobs,
            COALESCE(sum(project.finalizing_jobs), 0)::bigint AS finalizing_jobs,
            COALESCE(sum(project.retry_wait_jobs), 0)::bigint AS retry_wait_jobs,
            COALESCE(sum(project.canceling_jobs), 0)::bigint AS canceling_jobs,
            COALESCE(sum(project.succeeded_jobs), 0)::bigint AS succeeded_jobs,
            COALESCE(sum(project.failed_jobs), 0)::bigint AS failed_jobs,
            COALESCE(sum(project.canceled_jobs), 0)::bigint AS canceled_jobs,
            COALESCE(sum(project.quoted_amount_minor), 0)::bigint AS quoted_amount_minor,
            COALESCE(sum(project.posted_charge_amount_minor), 0)::bigint
                AS posted_charge_amount_minor
        FROM project_usage AS project
        UNION ALL
        SELECT
            project.*
        FROM project_usage AS project
    )
    SELECT
        usage.project_id,
        usage.total_jobs,
        usage.queued_jobs,
        usage.assigned_jobs,
        usage.running_jobs,
        usage.finalizing_jobs,
        usage.retry_wait_jobs,
        usage.canceling_jobs,
        usage.succeeded_jobs,
        usage.failed_jobs,
        usage.canceled_jobs,
        usage.quoted_amount_minor,
        usage.posted_charge_amount_minor,
        account.currency
    FROM usage_rows AS usage
    JOIN public.organization_credit_accounts AS account
      ON account.organization_id = p_organization_id
    ORDER BY usage.project_id NULLS FIRST;
END
$$;
REVOKE ALL ON FUNCTION vela_get_organization_usage(uuid, timestamptz, timestamptz)
FROM PUBLIC;
ALTER FUNCTION vela_get_organization_usage(uuid, timestamptz, timestamptz)
OWNER TO vela_internal;

CREATE FUNCTION vela_list_organization_audit_events(
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

GRANT USAGE ON SCHEMA public TO vela_organization_billing_request,
    vela_organization_audit_request;
GRANT EXECUTE ON FUNCTION vela_set_organization_identity_admin_context(uuid, bytea, text)
TO vela_organization_billing_request, vela_organization_audit_request;
GRANT EXECUTE ON FUNCTION vela_get_organization_credit_summary(uuid)
TO vela_organization_billing_request;
GRANT EXECUTE ON FUNCTION vela_list_organization_charges(uuid, integer)
TO vela_organization_billing_request;
GRANT EXECUTE ON FUNCTION vela_create_settlement_contact(uuid, uuid, text, text),
    vela_list_settlement_contacts(uuid, integer),
    vela_disable_settlement_contact(uuid, uuid)
TO vela_organization_billing_request;
GRANT EXECUTE ON FUNCTION vela_get_organization_usage(uuid, timestamptz, timestamptz)
TO vela_organization_billing_request, vela_organization_audit_request;
GRANT EXECUTE ON FUNCTION vela_list_organization_audit_events(uuid, integer)
TO vela_organization_audit_request;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE organization_settlement_contact_events,
    organization_settlement_contacts IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM organization_settlement_contacts)
        OR EXISTS (SELECT 1 FROM organization_settlement_contact_events)
    THEN
        RAISE EXCEPTION 'cannot remove settlement-contact administration with durable evidence'
            USING ERRCODE = '55000',
                CONSTRAINT = 'settlement_contact_contract_requires_empty_evidence';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_create_settlement_contact(uuid, uuid, text, text),
    vela_list_settlement_contacts(uuid, integer),
    vela_disable_settlement_contact(uuid, uuid)
FROM vela_organization_billing_request;
REVOKE EXECUTE ON FUNCTION vela_get_organization_usage(uuid, timestamptz, timestamptz)
FROM vela_organization_billing_request, vela_organization_audit_request;
REVOKE EXECUTE ON FUNCTION vela_list_organization_audit_events(uuid, integer)
FROM vela_organization_audit_request;
REVOKE EXECUTE ON FUNCTION vela_list_organization_charges(uuid, integer)
FROM vela_organization_billing_request;
REVOKE EXECUTE ON FUNCTION vela_get_organization_credit_summary(uuid)
FROM vela_organization_billing_request;
REVOKE EXECUTE ON FUNCTION vela_set_organization_identity_admin_context(uuid, bytea, text)
FROM vela_organization_billing_request, vela_organization_audit_request;
REVOKE USAGE ON SCHEMA public FROM vela_organization_billing_request,
    vela_organization_audit_request;
DROP FUNCTION vela_get_organization_credit_summary(uuid);
DROP FUNCTION vela_list_organization_charges(uuid, integer);
REVOKE EXECUTE ON FUNCTION vela_private.require_organization_billing_context(uuid)
FROM vela_billing_owner;
DROP FUNCTION vela_private.require_organization_billing_context(uuid);
DROP FUNCTION vela_get_organization_usage(uuid, timestamptz, timestamptz);
DROP FUNCTION vela_list_organization_audit_events(uuid, integer);
DROP FUNCTION vela_disable_settlement_contact(uuid, uuid);
DROP FUNCTION vela_list_settlement_contacts(uuid, integer);
DROP FUNCTION vela_create_settlement_contact(uuid, uuid, text, text);
DROP FUNCTION vela_private.record_settlement_contact_event(
    settlement_contact_action, uuid
);
DROP TRIGGER organization_settlement_contact_events_immutable
    ON organization_settlement_contact_events;
DROP TRIGGER organization_settlement_contacts_no_delete
    ON organization_settlement_contacts;
DROP FUNCTION vela_reject_settlement_contact_evidence_mutation();
DROP TRIGGER organization_settlement_contact_identity_immutable
    ON organization_settlement_contacts;
DROP FUNCTION vela_reject_settlement_contact_identity_mutation();
REVOKE SELECT ON organization_settlement_contacts,
    organization_settlement_contact_events FROM vela_internal;
DROP TABLE organization_settlement_contact_events;
DROP TABLE organization_settlement_contacts;
DROP TYPE settlement_contact_action;
REVOKE SELECT, INSERT ON human_administration_actor_attributions
    FROM vela_organization_reporting_owner;
REVOKE SELECT ON vela_private.human_administration_contexts
    FROM vela_organization_reporting_owner;
REVOKE USAGE ON SCHEMA public, vela_private FROM vela_organization_reporting_owner;

CREATE OR REPLACE FUNCTION vela_set_organization_identity_admin_context(
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

CREATE OR REPLACE FUNCTION vela_organization_role_scopes(
    p_role organization_role
) RETURNS text[]
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
-- +goose StatementEnd
