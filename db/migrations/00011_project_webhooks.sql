-- +goose Up
-- +goose StatementBegin
CREATE TYPE webhook_subscription_state AS ENUM ('ACTIVE', 'DISABLED');
CREATE TYPE webhook_event_type AS ENUM (
    'job.succeeded',
    'job.failed',
    'job.canceled'
);
CREATE TYPE webhook_delivery_state AS ENUM (
    'PENDING',
    'IN_FLIGHT',
    'DELIVERED',
    'DEAD_LETTER'
);
CREATE TYPE webhook_delivery_attempt_state AS ENUM (
    'STARTED',
    'SUCCEEDED',
    'FAILED',
    'ABANDONED'
);

CREATE TABLE webhook_subscriptions (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    endpoint_url text NOT NULL CHECK (
        length(endpoint_url) BETWEEN 1 AND 2048
        AND endpoint_url ~ '^https://[^[:space:]]+$'
    ),
    event_types webhook_event_type[] NOT NULL CHECK (
        cardinality(event_types) BETWEEN 1 AND 3
    ),
    state webhook_subscription_state NOT NULL DEFAULT 'ACTIVE',
    secret_overlap_seconds integer NOT NULL DEFAULT 86400 CHECK (
        secret_overlap_seconds = 86400
    ),
    created_by_principal_id uuid NOT NULL,
    created_by_credential_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    disabled_at timestamptz,
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, created_by_principal_id)
        REFERENCES principals(organization_id, id),
    FOREIGN KEY (organization_id, project_id, created_by_credential_id)
        REFERENCES credentials(organization_id, project_id, id),
    CHECK (
        (state = 'ACTIVE' AND disabled_at IS NULL)
        OR (state = 'DISABLED' AND disabled_at IS NOT NULL)
    )
);

CREATE TABLE webhook_subscription_secrets (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    revision integer NOT NULL CHECK (revision > 0),
    encryption_key_id text NOT NULL CHECK (
        length(encryption_key_id) BETWEEN 1 AND 200
    ),
    encryption_nonce bytea NOT NULL CHECK (octet_length(encryption_nonce) = 12),
    encrypted_secret bytea NOT NULL CHECK (octet_length(encrypted_secret) >= 16),
    valid_from timestamptz NOT NULL DEFAULT clock_timestamp(),
    valid_until timestamptz,
    created_by_principal_id uuid NOT NULL,
    created_by_credential_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (subscription_id, revision),
    UNIQUE (organization_id, project_id, subscription_id, id),
    FOREIGN KEY (organization_id, project_id, subscription_id)
        REFERENCES webhook_subscriptions(organization_id, project_id, id),
    FOREIGN KEY (organization_id, created_by_principal_id)
        REFERENCES principals(organization_id, id),
    FOREIGN KEY (organization_id, project_id, created_by_credential_id)
        REFERENCES credentials(organization_id, project_id, id),
    CHECK (valid_until IS NULL OR valid_until > valid_from)
);

CREATE UNIQUE INDEX webhook_subscription_current_secret_idx
    ON webhook_subscription_secrets (subscription_id)
    WHERE valid_until IS NULL;

ALTER TABLE outbox_events ADD CONSTRAINT outbox_events_webhook_identity_key
    UNIQUE (
        organization_id,
        project_id,
        event_id,
        aggregate_type,
        aggregate_id,
        aggregate_version,
        event_type
    );

CREATE TABLE webhook_deliveries (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    event_id uuid NOT NULL,
    aggregate_type text NOT NULL DEFAULT 'Job' CHECK (aggregate_type = 'Job'),
    event_type text NOT NULL CHECK (
        event_type IN ('job.succeeded', 'job.failed', 'job.canceled')
    ),
    job_id uuid NOT NULL,
    job_version bigint NOT NULL CHECK (job_version > 0),
    event_occurred_at timestamptz NOT NULL,
    payload jsonb NOT NULL CHECK (
        jsonb_typeof(payload) = 'object'
        AND payload @> '{"schema_version": 1}'::jsonb
        AND payload->>'event_id' = event_id::text
        AND payload->>'event_type' = event_type
        AND payload->>'organization_id' = organization_id::text
        AND payload->>'project_id' = project_id::text
        AND payload->>'job_id' = job_id::text
        AND (payload->>'job_version')::bigint = job_version
        AND payload->>'job_state' = CASE event_type
            WHEN 'job.succeeded' THEN 'SUCCEEDED'
            WHEN 'job.failed' THEN 'FAILED'
            WHEN 'job.canceled' THEN 'CANCELED'
        END
    ),
    state webhook_delivery_state NOT NULL DEFAULT 'PENDING',
    generation integer NOT NULL DEFAULT 1 CHECK (generation > 0),
    attempts integer NOT NULL DEFAULT 0 CHECK (attempts >= 0),
    available_at timestamptz NOT NULL,
    retry_window_started_at timestamptz NOT NULL,
    retry_deadline_at timestamptz NOT NULL CHECK (
        retry_deadline_at = retry_window_started_at + interval '72 hours'
    ),
    claimed_by text,
    claim_token uuid,
    claim_expires_at timestamptz,
    last_attempt_at timestamptz,
    delivered_at timestamptz,
    dead_lettered_at timestamptz,
    last_http_status integer CHECK (
        last_http_status IS NULL OR last_http_status BETWEEN 100 AND 599
    ),
    last_error text CHECK (last_error IS NULL OR length(last_error) <= 2000),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (subscription_id, event_id),
    UNIQUE (organization_id, project_id, subscription_id, id),
    FOREIGN KEY (organization_id, project_id, subscription_id)
        REFERENCES webhook_subscriptions(organization_id, project_id, id),
    FOREIGN KEY (
        organization_id,
        project_id,
        event_id,
        aggregate_type,
        job_id,
        job_version,
        event_type
    ) REFERENCES outbox_events (
        organization_id,
        project_id,
        event_id,
        aggregate_type,
        aggregate_id,
        aggregate_version,
        event_type
    ),
    CHECK (
        (state = 'PENDING' AND claimed_by IS NULL AND claim_token IS NULL
            AND claim_expires_at IS NULL AND delivered_at IS NULL
            AND dead_lettered_at IS NULL)
        OR (state = 'IN_FLIGHT' AND claimed_by IS NOT NULL
            AND length(claimed_by) BETWEEN 1 AND 200 AND claim_token IS NOT NULL
            AND claim_expires_at IS NOT NULL AND delivered_at IS NULL
            AND dead_lettered_at IS NULL)
        OR (state = 'DELIVERED' AND claimed_by IS NULL AND claim_token IS NULL
            AND claim_expires_at IS NULL AND delivered_at IS NOT NULL
            AND dead_lettered_at IS NULL)
        OR (state = 'DEAD_LETTER' AND claimed_by IS NULL AND claim_token IS NULL
            AND claim_expires_at IS NULL AND dead_lettered_at IS NOT NULL)
    )
);

CREATE INDEX webhook_deliveries_ready_idx
    ON webhook_deliveries (available_at, claim_expires_at, created_at, id)
    WHERE state IN ('PENDING', 'IN_FLIGHT');

CREATE TABLE webhook_delivery_attempts (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    generation integer NOT NULL CHECK (generation > 0),
    attempt_number integer NOT NULL CHECK (attempt_number > 0),
    claim_token uuid NOT NULL UNIQUE,
    claimed_by text NOT NULL CHECK (length(claimed_by) BETWEEN 1 AND 200),
    claimed_at timestamptz NOT NULL,
    signature_secret_revisions integer[] NOT NULL CHECK (
        cardinality(signature_secret_revisions) BETWEEN 1 AND 2
    ),
    state webhook_delivery_attempt_state NOT NULL DEFAULT 'STARTED',
    completed_at timestamptz,
    http_status integer CHECK (http_status IS NULL OR http_status BETWEEN 100 AND 599),
    error text CHECK (error IS NULL OR length(error) <= 2000),
    created_at timestamptz NOT NULL,
    UNIQUE (delivery_id, generation, attempt_number),
    UNIQUE (organization_id, project_id, subscription_id, delivery_id, id),
    FOREIGN KEY (organization_id, project_id, subscription_id, delivery_id)
        REFERENCES webhook_deliveries(organization_id, project_id, subscription_id, id),
    CHECK (
        (state = 'STARTED' AND completed_at IS NULL AND http_status IS NULL AND error IS NULL)
        OR (state = 'SUCCEEDED' AND completed_at IS NOT NULL
            AND http_status BETWEEN 200 AND 299 AND error IS NULL)
        OR (state = 'FAILED' AND completed_at IS NOT NULL
            AND (http_status IS NOT NULL OR error IS NOT NULL))
        OR (state = 'ABANDONED' AND completed_at IS NOT NULL
            AND http_status IS NULL AND error IS NOT NULL)
    )
);

CREATE TABLE webhook_delivery_replays (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    subscription_id uuid NOT NULL,
    delivery_id uuid NOT NULL,
    from_state webhook_delivery_state NOT NULL CHECK (
        from_state IN ('DELIVERED', 'DEAD_LETTER')
    ),
    from_generation integer NOT NULL CHECK (from_generation > 0),
    to_generation integer NOT NULL CHECK (to_generation = from_generation + 1),
    requested_by_principal_id uuid NOT NULL,
    requested_by_credential_id uuid NOT NULL,
    requested_at timestamptz NOT NULL,
    UNIQUE (delivery_id, to_generation),
    FOREIGN KEY (organization_id, project_id, subscription_id, delivery_id)
        REFERENCES webhook_deliveries(organization_id, project_id, subscription_id, id),
    FOREIGN KEY (organization_id, requested_by_principal_id)
        REFERENCES principals(organization_id, id),
    FOREIGN KEY (organization_id, project_id, requested_by_credential_id)
        REFERENCES credentials(organization_id, project_id, id)
);

ALTER TABLE webhook_subscriptions ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_subscriptions FORCE ROW LEVEL SECURITY;
ALTER TABLE webhook_subscription_secrets ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_subscription_secrets FORCE ROW LEVEL SECURITY;
ALTER TABLE webhook_deliveries ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_deliveries FORCE ROW LEVEL SECURITY;
ALTER TABLE webhook_delivery_attempts ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_delivery_attempts FORCE ROW LEVEL SECURITY;
ALTER TABLE webhook_delivery_replays ENABLE ROW LEVEL SECURITY;
ALTER TABLE webhook_delivery_replays FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_create_terminal_webhook_deliveries() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz;
    v_job_state text;
BEGIN
    IF NEW.aggregate_type <> 'Job' OR NEW.event_type NOT IN (
        'job.succeeded',
        'job.failed',
        'job.canceled'
    ) THEN
        RETURN NEW;
    END IF;
    v_now := clock_timestamp();
    v_job_state := CASE NEW.event_type
        WHEN 'job.succeeded' THEN 'SUCCEEDED'
        WHEN 'job.failed' THEN 'FAILED'
        WHEN 'job.canceled' THEN 'CANCELED'
    END;

    INSERT INTO webhook_deliveries (
        id,
        organization_id,
        project_id,
        subscription_id,
        event_id,
        aggregate_type,
        event_type,
        job_id,
        job_version,
        event_occurred_at,
        payload,
        available_at,
        retry_window_started_at,
        retry_deadline_at,
        created_at,
        updated_at
    )
    SELECT
        gen_random_uuid(),
        NEW.organization_id,
        NEW.project_id,
        subscription.id,
        NEW.event_id,
        NEW.aggregate_type,
        NEW.event_type,
        NEW.aggregate_id,
        NEW.aggregate_version,
        NEW.occurred_at,
        jsonb_build_object(
            'schema_version', 1,
            'event_id', NEW.event_id,
            'event_type', NEW.event_type,
            'occurred_at', NEW.occurred_at,
            'organization_id', NEW.organization_id,
            'project_id', NEW.project_id,
            'job_id', NEW.aggregate_id,
            'job_version', NEW.aggregate_version,
            'job_state', v_job_state
        ),
        v_now,
        v_now,
        v_now + interval '72 hours',
        v_now,
        v_now
    FROM webhook_subscriptions AS subscription
    WHERE subscription.organization_id = NEW.organization_id
      AND subscription.project_id = NEW.project_id
      AND subscription.state = 'ACTIVE'
      AND NEW.event_type::webhook_event_type = ANY(subscription.event_types)
    FOR SHARE OF subscription;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_create_terminal_webhook_deliveries() FROM PUBLIC;
ALTER FUNCTION vela_create_terminal_webhook_deliveries()
    OWNER TO vela_webhook_owner;

CREATE TRIGGER outbox_terminal_webhook_fanout
AFTER INSERT ON outbox_events
FOR EACH ROW EXECUTE FUNCTION vela_create_terminal_webhook_deliveries();

CREATE FUNCTION vela_validate_webhook_subscription() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_distinct_events integer;
BEGIN
    SELECT count(DISTINCT event_type)
    INTO v_distinct_events
    FROM unnest(NEW.event_types) AS event_type;
    IF v_distinct_events IS DISTINCT FROM cardinality(NEW.event_types) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'webhook_subscription_event_types_unique',
            MESSAGE = 'Webhook Subscription event types must be unique';
    END IF;

    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'ACTIVE' OR NEW.disabled_at IS NOT NULL THEN
            RAISE EXCEPTION 'Webhook Subscription must start ACTIVE';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Webhook Subscription cannot be deleted';
    END IF;
    IF ROW(
        NEW.id,
        NEW.organization_id,
        NEW.project_id,
        NEW.endpoint_url,
        NEW.event_types,
        NEW.secret_overlap_seconds,
        NEW.created_by_principal_id,
        NEW.created_by_credential_id,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.organization_id,
        OLD.project_id,
        OLD.endpoint_url,
        OLD.event_types,
        OLD.secret_overlap_seconds,
        OLD.created_by_principal_id,
        OLD.created_by_credential_id,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION 'Webhook Subscription identity is immutable';
    END IF;
    IF OLD.state <> 'ACTIVE' OR NEW.state <> 'DISABLED' OR
       NEW.disabled_at IS NULL THEN
        RAISE EXCEPTION 'invalid Webhook Subscription state transition';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_webhook_subscription() FROM PUBLIC;
ALTER FUNCTION vela_validate_webhook_subscription() OWNER TO vela_webhook_owner;

CREATE TRIGGER webhook_subscriptions_state_machine
BEFORE INSERT OR UPDATE OR DELETE ON webhook_subscriptions
FOR EACH ROW EXECUTE FUNCTION vela_validate_webhook_subscription();

CREATE FUNCTION vela_enforce_webhook_secret_immutability() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Webhook signing secret evidence cannot be deleted';
    END IF;
    IF ROW(
        NEW.id,
        NEW.organization_id,
        NEW.project_id,
        NEW.subscription_id,
        NEW.revision,
        NEW.encryption_key_id,
        NEW.encryption_nonce,
        NEW.encrypted_secret,
        NEW.valid_from,
        NEW.created_by_principal_id,
        NEW.created_by_credential_id,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.organization_id,
        OLD.project_id,
        OLD.subscription_id,
        OLD.revision,
        OLD.encryption_key_id,
        OLD.encryption_nonce,
        OLD.encrypted_secret,
        OLD.valid_from,
        OLD.created_by_principal_id,
        OLD.created_by_credential_id,
        OLD.created_at
    ) OR OLD.valid_until IS NOT NULL OR NEW.valid_until IS NULL OR
       NEW.valid_until <= clock_timestamp() THEN
        RAISE EXCEPTION 'Webhook signing secret evidence is immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_webhook_secret_immutability() FROM PUBLIC;
ALTER FUNCTION vela_enforce_webhook_secret_immutability() OWNER TO vela_webhook_owner;

CREATE TRIGGER webhook_subscription_secrets_immutable
BEFORE UPDATE OR DELETE ON webhook_subscription_secrets
FOR EACH ROW EXECUTE FUNCTION vela_enforce_webhook_secret_immutability();

CREATE FUNCTION vela_enforce_webhook_delivery_immutability() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'webhook_deliveries_immutable',
            MESSAGE = 'Webhook Delivery evidence is immutable';
    END IF;
    IF ROW(
        NEW.id,
        NEW.organization_id,
        NEW.project_id,
        NEW.subscription_id,
        NEW.event_id,
        NEW.aggregate_type,
        NEW.event_type,
        NEW.job_id,
        NEW.job_version,
        NEW.event_occurred_at,
        NEW.payload,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.organization_id,
        OLD.project_id,
        OLD.subscription_id,
        OLD.event_id,
        OLD.aggregate_type,
        OLD.event_type,
        OLD.job_id,
        OLD.job_version,
        OLD.event_occurred_at,
        OLD.payload,
        OLD.created_at
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'webhook_deliveries_identity_immutable',
            MESSAGE = 'Webhook Delivery identity and payload are immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_webhook_delivery_immutability() FROM PUBLIC;
ALTER FUNCTION vela_enforce_webhook_delivery_immutability()
    OWNER TO vela_webhook_owner;

CREATE TRIGGER webhook_deliveries_immutable
BEFORE INSERT OR UPDATE OR DELETE ON webhook_deliveries
FOR EACH ROW EXECUTE FUNCTION vela_enforce_webhook_delivery_immutability();

CREATE FUNCTION vela_enforce_webhook_delivery_attempt_immutability() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        IF NEW.state <> 'STARTED' THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'webhook_delivery_attempts_immutable',
                MESSAGE = 'Webhook Delivery attempt must start as STARTED';
        END IF;
        RETURN NEW;
    END IF;
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'webhook_delivery_attempts_immutable',
            MESSAGE = 'Webhook Delivery attempt receipts are immutable';
    END IF;
    IF ROW(
        NEW.id,
        NEW.organization_id,
        NEW.project_id,
        NEW.subscription_id,
        NEW.delivery_id,
        NEW.generation,
        NEW.attempt_number,
        NEW.claim_token,
        NEW.claimed_by,
        NEW.claimed_at,
        NEW.signature_secret_revisions,
        NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id,
        OLD.organization_id,
        OLD.project_id,
        OLD.subscription_id,
        OLD.delivery_id,
        OLD.generation,
        OLD.attempt_number,
        OLD.claim_token,
        OLD.claimed_by,
        OLD.claimed_at,
        OLD.signature_secret_revisions,
        OLD.created_at
    ) OR OLD.state <> 'STARTED' OR
       NEW.state NOT IN ('SUCCEEDED', 'FAILED', 'ABANDONED') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'webhook_delivery_attempts_immutable',
            MESSAGE = 'Webhook Delivery attempt receipts are immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_webhook_delivery_attempt_immutability()
    FROM PUBLIC;
ALTER FUNCTION vela_enforce_webhook_delivery_attempt_immutability()
    OWNER TO vela_webhook_owner;

CREATE TRIGGER webhook_delivery_attempts_immutable
BEFORE INSERT OR UPDATE OR DELETE ON webhook_delivery_attempts
FOR EACH ROW EXECUTE FUNCTION vela_enforce_webhook_delivery_attempt_immutability();

CREATE FUNCTION vela_reject_webhook_receipt_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'webhook_delivery_receipts_immutable',
        MESSAGE = 'Webhook Delivery attempt and replay receipts are immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_webhook_receipt_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_webhook_receipt_mutation()
    OWNER TO vela_webhook_owner;

CREATE TRIGGER webhook_delivery_attempts_truncate_immutable
BEFORE TRUNCATE ON webhook_delivery_attempts
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_webhook_receipt_mutation();

CREATE TRIGGER webhook_delivery_replays_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON webhook_delivery_replays
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_webhook_receipt_mutation();

CREATE FUNCTION vela_current_credential_id() RETURNS uuid
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, vela_private
AS $$
    SELECT context.credential_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id_if_assigned()
$$;
REVOKE ALL ON FUNCTION vela_current_credential_id() FROM PUBLIC;
ALTER FUNCTION vela_current_credential_id() OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_current_credential_id()
    TO vela_webhook_owner;

CREATE FUNCTION vela_create_webhook_subscription(
    p_subscription_id uuid,
    p_project_id uuid,
    p_endpoint_url text,
    p_event_types webhook_event_type[],
    p_secret_id uuid,
    p_encryption_key_id text,
    p_encryption_nonce bytea,
    p_encrypted_secret bytea
) RETURNS TABLE (
    id uuid,
    organization_id uuid,
    project_id uuid,
    endpoint_url text,
    event_types text[],
    state text,
    secret_revision integer,
    created_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := vela_current_organization_id();
    v_project_id uuid := vela_current_project_id();
    v_principal_id uuid := vela_current_principal_id();
    v_credential_id uuid := vela_current_credential_id();
BEGIN
    IF p_subscription_id IS NULL OR p_project_id IS NULL OR
       p_endpoint_url IS NULL OR p_event_types IS NULL OR p_secret_id IS NULL OR
       p_encryption_key_id IS NULL OR p_encryption_nonce IS NULL OR
       p_encrypted_secret IS NULL THEN
        RAISE EXCEPTION 'Webhook Subscription parameters are required'
            USING ERRCODE = '22023';
    END IF;
    IF vela_current_request_scope() IS DISTINCT FROM 'webhooks:manage' OR
       v_organization_id IS NULL OR v_project_id IS NULL OR
       v_principal_id IS NULL OR v_credential_id IS NULL THEN
        RAISE EXCEPTION 'invalid webhook management context' USING ERRCODE = '28000';
    END IF;
    IF p_project_id IS DISTINCT FROM v_project_id THEN
        RAISE EXCEPTION 'Webhook Subscription Project is outside request context'
            USING ERRCODE = '42501';
    END IF;

    INSERT INTO webhook_subscriptions (
        id,
        organization_id,
        project_id,
        endpoint_url,
        event_types,
        created_by_principal_id,
        created_by_credential_id
    ) VALUES (
        p_subscription_id,
        v_organization_id,
        v_project_id,
        p_endpoint_url,
        p_event_types,
        v_principal_id,
        v_credential_id
    );
    INSERT INTO webhook_subscription_secrets (
        id,
        organization_id,
        project_id,
        subscription_id,
        revision,
        encryption_key_id,
        encryption_nonce,
        encrypted_secret,
        created_by_principal_id,
        created_by_credential_id
    ) VALUES (
        p_secret_id,
        v_organization_id,
        v_project_id,
        p_subscription_id,
        1,
        p_encryption_key_id,
        p_encryption_nonce,
        p_encrypted_secret,
        v_principal_id,
        v_credential_id
    );

    RETURN QUERY
    SELECT
        subscription.id,
        subscription.organization_id,
        subscription.project_id,
        subscription.endpoint_url,
        subscription.event_types::text[],
        subscription.state::text,
        1,
        subscription.created_at
    FROM webhook_subscriptions AS subscription
    WHERE subscription.id = p_subscription_id;
END
$$;
REVOKE ALL ON FUNCTION vela_create_webhook_subscription(
    uuid, uuid, text, webhook_event_type[], uuid, text, bytea, bytea
) FROM PUBLIC;
ALTER FUNCTION vela_create_webhook_subscription(
    uuid, uuid, text, webhook_event_type[], uuid, text, bytea, bytea
) OWNER TO vela_webhook_owner;

CREATE FUNCTION vela_lock_webhook_secret_rotation(
    p_project_id uuid,
    p_subscription_id uuid
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_subscription_state webhook_subscription_state;
    v_current_revision integer;
BEGIN
    IF p_project_id IS NULL OR p_subscription_id IS NULL THEN
        RAISE EXCEPTION 'Webhook signing-secret rotation lock parameters are required'
            USING ERRCODE = '22023';
    END IF;
    IF vela_current_request_scope() IS DISTINCT FROM 'webhooks:manage' OR
       vela_current_project_id() IS DISTINCT FROM p_project_id THEN
        RAISE EXCEPTION 'invalid webhook management context' USING ERRCODE = '42501';
    END IF;

    SELECT subscription.state
    INTO v_subscription_state
    FROM webhook_subscriptions AS subscription
    WHERE subscription.organization_id = vela_current_organization_id()
      AND subscription.project_id = p_project_id
      AND subscription.id = p_subscription_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'webhook_subscription_not_found',
            MESSAGE = 'Webhook Subscription not found';
    END IF;
    SELECT secret.revision
    INTO v_current_revision
    FROM webhook_subscription_secrets AS secret
    WHERE secret.organization_id = vela_current_organization_id()
      AND secret.project_id = p_project_id
      AND secret.subscription_id = p_subscription_id
      AND secret.valid_until IS NULL
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Webhook Subscription current secret is missing'
            USING ERRCODE = '23514';
    END IF;
    IF v_subscription_state <> 'ACTIVE' THEN
        RAISE EXCEPTION 'disabled Webhook Subscription cannot rotate secrets'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM webhook_subscription_secrets AS previous
        WHERE previous.subscription_id = p_subscription_id
          AND previous.valid_until IS NOT NULL
          AND previous.valid_until > clock_timestamp()
    ) THEN
        RAISE EXCEPTION 'Webhook signing-secret overlap is still active'
            USING ERRCODE = '55000';
    END IF;
    RETURN v_current_revision + 1;
END
$$;
REVOKE ALL ON FUNCTION vela_lock_webhook_secret_rotation(uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_lock_webhook_secret_rotation(uuid, uuid)
    OWNER TO vela_webhook_owner;

CREATE FUNCTION vela_rotate_webhook_secret(
    p_project_id uuid,
    p_subscription_id uuid,
    p_secret_id uuid,
    p_revision integer,
    p_encryption_key_id text,
    p_encryption_nonce bytea,
    p_encrypted_secret bytea
) RETURNS TABLE (
    id uuid,
    organization_id uuid,
    project_id uuid,
    endpoint_url text,
    event_types text[],
    state text,
    secret_revision integer,
    created_at timestamptz,
    previous_secret_valid_until timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_next_revision integer;
    v_now timestamptz := clock_timestamp();
    v_previous_valid_until timestamptz;
BEGIN
    IF p_project_id IS NULL OR p_subscription_id IS NULL OR p_secret_id IS NULL OR
       p_revision IS NULL OR p_encryption_key_id IS NULL OR
       p_encryption_nonce IS NULL OR p_encrypted_secret IS NULL THEN
        RAISE EXCEPTION 'Webhook signing-secret rotation parameters are required'
            USING ERRCODE = '22023';
    END IF;
    v_next_revision := vela_lock_webhook_secret_rotation(
        p_project_id,
        p_subscription_id
    );
    IF p_revision IS DISTINCT FROM v_next_revision THEN
        RAISE EXCEPTION 'Webhook signing-secret revision is stale'
            USING ERRCODE = '40001';
    END IF;

    UPDATE webhook_subscription_secrets AS secret
    SET valid_until = v_now + make_interval(secs => 86400)
    WHERE secret.subscription_id = p_subscription_id
      AND secret.valid_until IS NULL
    RETURNING secret.valid_until INTO v_previous_valid_until;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Webhook Subscription current secret is missing'
            USING ERRCODE = '23514';
    END IF;

    INSERT INTO webhook_subscription_secrets (
        id,
        organization_id,
        project_id,
        subscription_id,
        revision,
        encryption_key_id,
        encryption_nonce,
        encrypted_secret,
        valid_from,
        created_by_principal_id,
        created_by_credential_id
    ) VALUES (
        p_secret_id,
        vela_current_organization_id(),
        p_project_id,
        p_subscription_id,
        p_revision,
        p_encryption_key_id,
        p_encryption_nonce,
        p_encrypted_secret,
        v_now,
        vela_current_principal_id(),
        vela_current_credential_id()
    );

    RETURN QUERY
    SELECT
        subscription.id,
        subscription.organization_id,
        subscription.project_id,
        subscription.endpoint_url,
        subscription.event_types::text[],
        subscription.state::text,
        p_revision,
        subscription.created_at,
        v_previous_valid_until
    FROM webhook_subscriptions AS subscription
    WHERE subscription.id = p_subscription_id;
END
$$;
REVOKE ALL ON FUNCTION vela_rotate_webhook_secret(
    uuid, uuid, uuid, integer, text, bytea, bytea
) FROM PUBLIC;
ALTER FUNCTION vela_rotate_webhook_secret(
    uuid, uuid, uuid, integer, text, bytea, bytea
) OWNER TO vela_webhook_owner;

CREATE FUNCTION vela_disable_webhook_subscription(
    p_project_id uuid,
    p_subscription_id uuid
) RETURNS TABLE (
    id uuid,
    organization_id uuid,
    project_id uuid,
    endpoint_url text,
    event_types text[],
    state text,
    secret_revision integer,
    created_at timestamptz,
    disabled_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_state webhook_subscription_state;
BEGIN
    IF p_project_id IS NULL OR p_subscription_id IS NULL THEN
        RAISE EXCEPTION 'Webhook Subscription disable parameters are required'
            USING ERRCODE = '22023';
    END IF;
    IF vela_current_request_scope() IS DISTINCT FROM 'webhooks:manage' OR
       vela_current_project_id() IS DISTINCT FROM p_project_id THEN
        RAISE EXCEPTION 'invalid webhook management context' USING ERRCODE = '42501';
    END IF;
    SELECT subscription.state
    INTO v_state
    FROM webhook_subscriptions AS subscription
    WHERE subscription.organization_id = vela_current_organization_id()
      AND subscription.project_id = p_project_id
      AND subscription.id = p_subscription_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'webhook_subscription_not_found',
            MESSAGE = 'Webhook Subscription not found';
    END IF;
    IF v_state = 'ACTIVE' THEN
        UPDATE webhook_subscriptions AS subscription
        SET state = 'DISABLED', disabled_at = v_now
        WHERE subscription.id = p_subscription_id;
    END IF;

    UPDATE webhook_deliveries AS delivery
    SET state = 'DEAD_LETTER',
        dead_lettered_at = v_now,
        last_error = 'Webhook Subscription was disabled',
        updated_at = v_now
    WHERE delivery.subscription_id = p_subscription_id
      AND delivery.state = 'PENDING';

    RETURN QUERY
    SELECT
        subscription.id,
        subscription.organization_id,
        subscription.project_id,
        subscription.endpoint_url,
        subscription.event_types::text[],
        subscription.state::text,
        current_secret.revision,
        subscription.created_at,
        subscription.disabled_at
    FROM webhook_subscriptions AS subscription
    JOIN webhook_subscription_secrets AS current_secret
      ON current_secret.subscription_id = subscription.id
     AND current_secret.valid_until IS NULL
    WHERE subscription.id = p_subscription_id;
END
$$;
REVOKE ALL ON FUNCTION vela_disable_webhook_subscription(uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_disable_webhook_subscription(uuid, uuid)
    OWNER TO vela_webhook_owner;

CREATE FUNCTION vela_list_webhook_subscriptions(
    p_project_id uuid,
    p_limit integer
) RETURNS TABLE (
    id uuid,
    organization_id uuid,
    project_id uuid,
    endpoint_url text,
    event_types text[],
    state text,
    secret_revision integer,
    created_at timestamptz,
    disabled_at timestamptz
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_project_id IS NULL OR p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'invalid Webhook Subscription list parameters'
            USING ERRCODE = '22023';
    END IF;
    IF vela_current_request_scope() IS DISTINCT FROM 'webhooks:read' OR
       vela_current_project_id() IS DISTINCT FROM p_project_id THEN
        RAISE EXCEPTION 'invalid webhook read context' USING ERRCODE = '42501';
    END IF;
    RETURN QUERY
    SELECT
        subscription.id,
        subscription.organization_id,
        subscription.project_id,
        subscription.endpoint_url,
        subscription.event_types::text[],
        subscription.state::text,
        current_secret.revision,
        subscription.created_at,
        subscription.disabled_at
    FROM webhook_subscriptions AS subscription
    JOIN webhook_subscription_secrets AS current_secret
      ON current_secret.subscription_id = subscription.id
     AND current_secret.valid_until IS NULL
    WHERE subscription.organization_id = vela_current_organization_id()
      AND subscription.project_id = p_project_id
    ORDER BY subscription.created_at, subscription.id
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_webhook_subscriptions(uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_list_webhook_subscriptions(uuid, integer)
    OWNER TO vela_webhook_owner;

CREATE FUNCTION vela_list_webhook_deliveries(
    p_project_id uuid,
    p_subscription_id uuid,
    p_limit integer
) RETURNS TABLE (
    id uuid,
    event_id uuid,
    event_type text,
    job_id uuid,
    job_version bigint,
    state text,
    generation integer,
    attempts integer,
    retry_deadline_at timestamptz,
    last_attempt_at timestamptz,
    delivered_at timestamptz,
    dead_lettered_at timestamptz,
    last_http_status integer,
    created_at timestamptz,
    updated_at timestamptz
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_project_id IS NULL OR p_subscription_id IS NULL OR p_limit IS NULL OR
       p_limit NOT BETWEEN 1 AND 100 THEN
        RAISE EXCEPTION 'invalid Webhook Delivery list parameters'
            USING ERRCODE = '22023';
    END IF;
    IF vela_current_request_scope() IS DISTINCT FROM 'webhooks:read' OR
       vela_current_project_id() IS DISTINCT FROM p_project_id THEN
        RAISE EXCEPTION 'invalid webhook read context' USING ERRCODE = '42501';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM webhook_subscriptions AS subscription
        WHERE subscription.organization_id = vela_current_organization_id()
          AND subscription.project_id = p_project_id
          AND subscription.id = p_subscription_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'webhook_subscription_not_found',
            MESSAGE = 'Webhook Subscription not found';
    END IF;
    RETURN QUERY
    SELECT
        delivery.id,
        delivery.event_id,
        delivery.event_type,
        delivery.job_id,
        delivery.job_version,
        delivery.state::text,
        delivery.generation,
        delivery.attempts,
        delivery.retry_deadline_at,
        delivery.last_attempt_at,
        delivery.delivered_at,
        delivery.dead_lettered_at,
        delivery.last_http_status,
        delivery.created_at,
        delivery.updated_at
    FROM webhook_deliveries AS delivery
    WHERE delivery.organization_id = vela_current_organization_id()
      AND delivery.project_id = p_project_id
      AND delivery.subscription_id = p_subscription_id
    ORDER BY delivery.created_at DESC, delivery.id DESC
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_webhook_deliveries(uuid, uuid, integer)
    FROM PUBLIC;
ALTER FUNCTION vela_list_webhook_deliveries(uuid, uuid, integer)
    OWNER TO vela_webhook_owner;

CREATE FUNCTION vela_claim_webhook_deliveries(
    p_instance_id text,
    p_claim_seconds integer,
    p_batch_size integer
) RETURNS TABLE (
    delivery_id uuid,
    organization_id uuid,
    project_id uuid,
    subscription_id uuid,
    event_id uuid,
    endpoint_url text,
    payload bytea,
    claimed_at timestamptz,
    claim_token uuid,
    current_secret_id uuid,
    current_secret_revision integer,
    current_encryption_key_id text,
    current_encryption_nonce bytea,
    current_encrypted_secret bytea,
    previous_secret_id uuid,
    previous_secret_revision integer,
    previous_encryption_key_id text,
    previous_encryption_nonce bytea,
    previous_encrypted_secret bytea
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_candidate record;
    v_delivery webhook_deliveries%ROWTYPE;
    v_subscription_state webhook_subscription_state;
    v_endpoint_url text;
    v_current webhook_subscription_secrets%ROWTYPE;
    v_previous webhook_subscription_secrets%ROWTYPE;
    v_claim_token uuid;
    v_attempt_id uuid;
    v_signature_revisions integer[];
BEGIN
    IF p_instance_id IS NULL OR p_claim_seconds IS NULL OR p_batch_size IS NULL OR
       length(p_instance_id) NOT BETWEEN 1 AND 200 OR
       p_claim_seconds NOT BETWEEN 1 AND 3600 OR
       p_batch_size NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION 'invalid Webhook Delivery claim parameters'
            USING ERRCODE = '22023';
    END IF;

    WITH expired AS (
        SELECT delivery.id
        FROM webhook_deliveries AS delivery
        WHERE delivery.state = 'PENDING'
          AND delivery.retry_deadline_at <= v_now
        ORDER BY delivery.retry_deadline_at, delivery.id
        FOR UPDATE SKIP LOCKED
        LIMIT p_batch_size
    )
    UPDATE webhook_deliveries AS delivery
    SET state = 'DEAD_LETTER',
        dead_lettered_at = v_now,
        last_error = 'Webhook Delivery automatic retry window expired',
        updated_at = v_now
    FROM expired
    WHERE delivery.id = expired.id;

    FOR v_candidate IN
        SELECT delivery.id, delivery.subscription_id
        FROM webhook_deliveries AS delivery
        WHERE delivery.state = 'IN_FLIGHT'
          AND delivery.claim_expires_at <= v_now
        ORDER BY delivery.claim_expires_at, delivery.id
        LIMIT p_batch_size
    LOOP
        SELECT subscription.state
        INTO v_subscription_state
        FROM webhook_subscriptions AS subscription
        WHERE subscription.id = v_candidate.subscription_id
        FOR SHARE;

        SELECT delivery.*
        INTO v_delivery
        FROM webhook_deliveries AS delivery
        WHERE delivery.id = v_candidate.id
          AND delivery.state = 'IN_FLIGHT'
          AND delivery.claim_expires_at <= v_now
        FOR UPDATE SKIP LOCKED;
        IF NOT FOUND THEN
            CONTINUE;
        END IF;

        UPDATE webhook_delivery_attempts AS attempt
        SET state = 'ABANDONED',
            completed_at = v_now,
            error = 'Webhook Delivery claim expired before a durable receipt'
        WHERE attempt.delivery_id = v_delivery.id
          AND attempt.claim_token = v_delivery.claim_token
          AND attempt.state = 'STARTED';
        IF NOT FOUND THEN
            RAISE EXCEPTION 'expired Webhook Delivery attempt receipt is missing'
                USING ERRCODE = '23514';
        END IF;

        UPDATE webhook_deliveries AS delivery
        SET state = CASE
                WHEN v_subscription_state = 'ACTIVE'
                    AND v_now < delivery.retry_deadline_at
                THEN 'PENDING'::webhook_delivery_state
                ELSE 'DEAD_LETTER'::webhook_delivery_state
            END,
            available_at = v_now,
            claimed_by = NULL,
            claim_token = NULL,
            claim_expires_at = NULL,
            dead_lettered_at = CASE
                WHEN v_subscription_state = 'ACTIVE'
                    AND v_now < delivery.retry_deadline_at
                THEN NULL
                ELSE v_now
            END,
            last_error = 'Webhook Delivery claim expired before a durable receipt',
            updated_at = v_now
        WHERE delivery.id = v_delivery.id
          AND delivery.claim_token = v_delivery.claim_token;
    END LOOP;

    FOR v_candidate IN
        SELECT delivery.id, delivery.subscription_id
        FROM webhook_deliveries AS delivery
        JOIN webhook_subscriptions AS subscription
          ON subscription.id = delivery.subscription_id
        WHERE delivery.state = 'PENDING'
          AND delivery.available_at <= v_now
          AND delivery.retry_deadline_at > v_now
          AND subscription.state = 'ACTIVE'
        ORDER BY delivery.available_at, delivery.created_at, delivery.id
        LIMIT p_batch_size
    LOOP
        SELECT subscription.state, subscription.endpoint_url
        INTO v_subscription_state, v_endpoint_url
        FROM webhook_subscriptions AS subscription
        WHERE subscription.id = v_candidate.subscription_id
        FOR SHARE;
        IF v_subscription_state IS DISTINCT FROM 'ACTIVE' THEN
            CONTINUE;
        END IF;

        SELECT delivery.*
        INTO v_delivery
        FROM webhook_deliveries AS delivery
        WHERE delivery.id = v_candidate.id
          AND delivery.state = 'PENDING'
          AND delivery.available_at <= v_now
          AND delivery.retry_deadline_at > v_now
        FOR UPDATE SKIP LOCKED;
        IF NOT FOUND THEN
            CONTINUE;
        END IF;

        SELECT secret.*
        INTO v_current
        FROM webhook_subscription_secrets AS secret
        WHERE secret.subscription_id = v_delivery.subscription_id
          AND secret.valid_until IS NULL
        FOR SHARE;
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Webhook Subscription current secret is missing'
                USING ERRCODE = '23514';
        END IF;
        SELECT secret.*
        INTO v_previous
        FROM webhook_subscription_secrets AS secret
        WHERE secret.subscription_id = v_delivery.subscription_id
          AND secret.valid_until IS NOT NULL
          AND secret.valid_until > v_now
        ORDER BY secret.revision DESC
        LIMIT 1
        FOR SHARE;

        v_claim_token := gen_random_uuid();
        v_attempt_id := gen_random_uuid();
        IF v_previous.id IS NULL THEN
            v_signature_revisions := ARRAY[v_current.revision];
        ELSE
            v_signature_revisions := ARRAY[v_current.revision, v_previous.revision];
        END IF;

        UPDATE webhook_deliveries AS delivery
        SET state = 'IN_FLIGHT',
            attempts = delivery.attempts + 1,
            claimed_by = p_instance_id,
            claim_token = v_claim_token,
            claim_expires_at = v_now + make_interval(secs => p_claim_seconds),
            last_attempt_at = v_now,
            last_http_status = NULL,
            last_error = NULL,
            updated_at = v_now
        WHERE delivery.id = v_delivery.id
          AND delivery.state = 'PENDING';
        IF NOT FOUND THEN
            RAISE EXCEPTION 'Webhook Delivery claim changed while locked'
                USING ERRCODE = '40001';
        END IF;

        INSERT INTO webhook_delivery_attempts (
            id,
            organization_id,
            project_id,
            subscription_id,
            delivery_id,
            generation,
            attempt_number,
            claim_token,
            claimed_by,
            claimed_at,
            signature_secret_revisions,
            created_at
        ) VALUES (
            v_attempt_id,
            v_delivery.organization_id,
            v_delivery.project_id,
            v_delivery.subscription_id,
            v_delivery.id,
            v_delivery.generation,
            v_delivery.attempts + 1,
            v_claim_token,
            p_instance_id,
            v_now,
            v_signature_revisions,
            v_now
        );

        delivery_id := v_delivery.id;
        organization_id := v_delivery.organization_id;
        project_id := v_delivery.project_id;
        subscription_id := v_delivery.subscription_id;
        event_id := v_delivery.event_id;
        endpoint_url := v_endpoint_url;
        payload := convert_to(v_delivery.payload::text, 'UTF8');
        claimed_at := v_now;
        claim_token := v_claim_token;
        current_secret_id := v_current.id;
        current_secret_revision := v_current.revision;
        current_encryption_key_id := v_current.encryption_key_id;
        current_encryption_nonce := v_current.encryption_nonce;
        current_encrypted_secret := v_current.encrypted_secret;
        previous_secret_id := v_previous.id;
        previous_secret_revision := v_previous.revision;
        previous_encryption_key_id := v_previous.encryption_key_id;
        previous_encryption_nonce := v_previous.encryption_nonce;
        previous_encrypted_secret := v_previous.encrypted_secret;
        RETURN NEXT;
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION vela_claim_webhook_deliveries(text, integer, integer)
    FROM PUBLIC;
ALTER FUNCTION vela_claim_webhook_deliveries(text, integer, integer)
    OWNER TO vela_webhook_owner;

CREATE FUNCTION vela_mark_webhook_delivered(
    p_delivery_id uuid,
    p_claim_token uuid,
    p_http_status integer
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_delivery_id IS NULL OR p_claim_token IS NULL OR p_http_status IS NULL OR
       p_http_status NOT BETWEEN 200 AND 299 THEN
        RAISE EXCEPTION 'invalid successful Webhook Delivery parameters'
            USING ERRCODE = '22023';
    END IF;
    PERFORM 1
    FROM webhook_deliveries AS delivery
    WHERE delivery.id = p_delivery_id
      AND delivery.state = 'IN_FLIGHT'
      AND delivery.claim_token = p_claim_token
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    UPDATE webhook_delivery_attempts AS attempt
    SET state = 'SUCCEEDED',
        completed_at = v_now,
        http_status = p_http_status,
        error = NULL
    WHERE attempt.delivery_id = p_delivery_id
      AND attempt.claim_token = p_claim_token
      AND attempt.state = 'STARTED';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Webhook Delivery attempt receipt is missing'
            USING ERRCODE = '23514';
    END IF;

    UPDATE webhook_deliveries AS delivery
    SET state = 'DELIVERED',
        claimed_by = NULL,
        claim_token = NULL,
        claim_expires_at = NULL,
        delivered_at = v_now,
        dead_lettered_at = NULL,
        last_http_status = p_http_status,
        last_error = NULL,
        updated_at = v_now
    WHERE delivery.id = p_delivery_id
      AND delivery.claim_token = p_claim_token;
    RETURN true;
END
$$;
REVOKE ALL ON FUNCTION vela_mark_webhook_delivered(uuid, uuid, integer)
    FROM PUBLIC;
ALTER FUNCTION vela_mark_webhook_delivered(uuid, uuid, integer)
    OWNER TO vela_webhook_owner;

CREATE FUNCTION vela_mark_webhook_failed(
    p_delivery_id uuid,
    p_claim_token uuid,
    p_http_status integer,
    p_error text
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_attempts integer;
    v_retry_deadline_at timestamptz;
    v_subscription_id uuid;
    v_subscription_state webhook_subscription_state;
    v_retry_seconds integer;
    v_available_at timestamptz;
    v_error text := left(COALESCE(NULLIF(p_error, ''), 'Webhook Delivery failed'), 2000);
    v_dead_letter boolean;
BEGIN
    IF p_delivery_id IS NULL OR p_claim_token IS NULL OR p_error IS NULL OR
       (p_http_status IS NOT NULL AND p_http_status NOT BETWEEN 100 AND 599) THEN
        RAISE EXCEPTION 'invalid failed Webhook Delivery parameters'
            USING ERRCODE = '22023';
    END IF;
    SELECT delivery.subscription_id
    INTO v_subscription_id
    FROM webhook_deliveries AS delivery
    WHERE delivery.id = p_delivery_id
      AND delivery.state = 'IN_FLIGHT'
      AND delivery.claim_token = p_claim_token;
    IF NOT FOUND THEN
        RETURN false;
    END IF;
    SELECT subscription.state
    INTO v_subscription_state
    FROM webhook_subscriptions AS subscription
    WHERE subscription.id = v_subscription_id
    FOR SHARE;
    SELECT delivery.attempts, delivery.retry_deadline_at
    INTO v_attempts, v_retry_deadline_at
    FROM webhook_deliveries AS delivery
    WHERE delivery.id = p_delivery_id
      AND delivery.state = 'IN_FLIGHT'
      AND delivery.claim_token = p_claim_token
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN false;
    END IF;

    v_retry_seconds := LEAST(
        3600,
        (5 * power(2::numeric, LEAST(v_attempts - 1, 10)))::integer
    );
    v_available_at := v_now + make_interval(secs => v_retry_seconds);
    v_dead_letter := v_subscription_state <> 'ACTIVE' OR
        v_now >= v_retry_deadline_at OR v_available_at >= v_retry_deadline_at;

    UPDATE webhook_delivery_attempts AS attempt
    SET state = 'FAILED',
        completed_at = v_now,
        http_status = p_http_status,
        error = v_error
    WHERE attempt.delivery_id = p_delivery_id
      AND attempt.claim_token = p_claim_token
      AND attempt.state = 'STARTED';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Webhook Delivery attempt receipt is missing'
            USING ERRCODE = '23514';
    END IF;

    UPDATE webhook_deliveries AS delivery
    SET state = CASE
            WHEN v_dead_letter THEN 'DEAD_LETTER'::webhook_delivery_state
            ELSE 'PENDING'::webhook_delivery_state
        END,
        available_at = CASE WHEN v_dead_letter THEN v_now ELSE v_available_at END,
        claimed_by = NULL,
        claim_token = NULL,
        claim_expires_at = NULL,
        dead_lettered_at = CASE WHEN v_dead_letter THEN v_now ELSE NULL END,
        last_http_status = p_http_status,
        last_error = v_error,
        updated_at = v_now
    WHERE delivery.id = p_delivery_id
      AND delivery.claim_token = p_claim_token;
    RETURN true;
END
$$;
REVOKE ALL ON FUNCTION vela_mark_webhook_failed(uuid, uuid, integer, text)
    FROM PUBLIC;
ALTER FUNCTION vela_mark_webhook_failed(uuid, uuid, integer, text)
    OWNER TO vela_webhook_owner;

CREATE FUNCTION vela_replay_webhook_delivery(
    p_project_id uuid,
    p_subscription_id uuid,
    p_delivery_id uuid,
    p_replay_id uuid
) RETURNS TABLE (
    id uuid,
    event_id uuid,
    event_type text,
    job_id uuid,
    job_version bigint,
    state text,
    generation integer,
    attempts integer,
    retry_deadline_at timestamptz,
    created_at timestamptz,
    updated_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_delivery webhook_deliveries%ROWTYPE;
    v_subscription_state webhook_subscription_state;
BEGIN
    IF p_project_id IS NULL OR p_subscription_id IS NULL OR
       p_delivery_id IS NULL OR p_replay_id IS NULL THEN
        RAISE EXCEPTION 'Webhook Delivery replay parameters are required'
            USING ERRCODE = '22023';
    END IF;
    IF vela_current_request_scope() IS DISTINCT FROM 'webhooks:manage' OR
       vela_current_project_id() IS DISTINCT FROM p_project_id THEN
        RAISE EXCEPTION 'invalid webhook management context' USING ERRCODE = '42501';
    END IF;
    SELECT subscription.state
    INTO v_subscription_state
    FROM webhook_subscriptions AS subscription
    WHERE subscription.organization_id = vela_current_organization_id()
      AND subscription.project_id = p_project_id
      AND subscription.id = p_subscription_id
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'webhook_subscription_not_found',
            MESSAGE = 'Webhook Subscription not found';
    END IF;
    IF v_subscription_state <> 'ACTIVE' THEN
        RAISE EXCEPTION 'disabled Webhook Subscription cannot replay Deliveries'
            USING ERRCODE = '55000';
    END IF;
    SELECT delivery.*
    INTO v_delivery
    FROM webhook_deliveries AS delivery
    WHERE delivery.organization_id = vela_current_organization_id()
      AND delivery.project_id = p_project_id
      AND delivery.subscription_id = p_subscription_id
      AND delivery.id = p_delivery_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'webhook_delivery_not_found',
            MESSAGE = 'Webhook Delivery not found';
    END IF;
    IF v_delivery.state NOT IN ('DELIVERED', 'DEAD_LETTER') THEN
        RAISE EXCEPTION 'Webhook Delivery is not terminal'
            USING ERRCODE = '55000';
    END IF;

    INSERT INTO webhook_delivery_replays (
        id,
        organization_id,
        project_id,
        subscription_id,
        delivery_id,
        from_state,
        from_generation,
        to_generation,
        requested_by_principal_id,
        requested_by_credential_id,
        requested_at
    ) VALUES (
        p_replay_id,
        v_delivery.organization_id,
        v_delivery.project_id,
        v_delivery.subscription_id,
        v_delivery.id,
        v_delivery.state,
        v_delivery.generation,
        v_delivery.generation + 1,
        vela_current_principal_id(),
        vela_current_credential_id(),
        v_now
    );

    UPDATE webhook_deliveries AS delivery
    SET state = 'PENDING',
        generation = delivery.generation + 1,
        available_at = v_now,
        retry_window_started_at = v_now,
        retry_deadline_at = v_now + interval '72 hours',
        claimed_by = NULL,
        claim_token = NULL,
        claim_expires_at = NULL,
        delivered_at = NULL,
        dead_lettered_at = NULL,
        last_http_status = NULL,
        last_error = NULL,
        updated_at = v_now
    WHERE delivery.id = v_delivery.id;

    RETURN QUERY
    SELECT
        delivery.id,
        delivery.event_id,
        delivery.event_type,
        delivery.job_id,
        delivery.job_version,
        delivery.state::text,
        delivery.generation,
        delivery.attempts,
        delivery.retry_deadline_at,
        delivery.created_at,
        delivery.updated_at
    FROM webhook_deliveries AS delivery
    WHERE delivery.id = v_delivery.id;
END
$$;
REVOKE ALL ON FUNCTION vela_replay_webhook_delivery(uuid, uuid, uuid, uuid)
    FROM PUBLIC;
ALTER FUNCTION vela_replay_webhook_delivery(uuid, uuid, uuid, uuid)
    OWNER TO vela_webhook_owner;

GRANT USAGE ON SCHEMA public
    TO vela_webhook_request, vela_webhook, vela_webhook_owner;
GRANT SELECT ON vela_private.request_contexts TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_current_organization_id(),
    vela_current_project_id(), vela_current_principal_id(),
    vela_current_request_scope(), vela_current_credential_id()
    TO vela_webhook_owner;
GRANT SELECT, INSERT, UPDATE ON webhook_subscriptions,
    webhook_subscription_secrets, webhook_deliveries,
    webhook_delivery_attempts, webhook_delivery_replays TO vela_webhook_owner;
GRANT EXECUTE ON FUNCTION vela_set_request_context(uuid, bytea, text)
    TO vela_webhook_request;
GRANT EXECUTE ON FUNCTION vela_create_webhook_subscription(
    uuid, uuid, text, webhook_event_type[], uuid, text, bytea, bytea
) TO vela_webhook_request;
GRANT EXECUTE ON FUNCTION vela_lock_webhook_secret_rotation(uuid, uuid),
    vela_rotate_webhook_secret(uuid, uuid, uuid, integer, text, bytea, bytea),
    vela_disable_webhook_subscription(uuid, uuid),
    vela_replay_webhook_delivery(uuid, uuid, uuid, uuid),
    vela_list_webhook_subscriptions(uuid, integer),
    vela_list_webhook_deliveries(uuid, uuid, integer)
    TO vela_webhook_request;
GRANT EXECUTE ON FUNCTION vela_claim_webhook_deliveries(text, integer, integer),
    vela_mark_webhook_delivered(uuid, uuid, integer),
    vela_mark_webhook_failed(uuid, uuid, integer, text) TO vela_webhook;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE
    credentials,
    projects,
    principals,
    outbox_events,
    webhook_subscriptions,
    webhook_subscription_secrets,
    webhook_deliveries,
    webhook_delivery_attempts,
    webhook_delivery_replays
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM webhook_subscriptions) OR
       EXISTS (SELECT 1 FROM webhook_deliveries) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'project_webhook_contract_has_durable_evidence',
            MESSAGE = 'cannot remove Project webhooks after durable evidence exists';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_create_webhook_subscription(
    uuid, uuid, text, webhook_event_type[], uuid, text, bytea, bytea
) FROM vela_webhook_request;
REVOKE EXECUTE ON FUNCTION vela_lock_webhook_secret_rotation(uuid, uuid),
    vela_rotate_webhook_secret(uuid, uuid, uuid, integer, text, bytea, bytea),
    vela_disable_webhook_subscription(uuid, uuid),
    vela_replay_webhook_delivery(uuid, uuid, uuid, uuid),
    vela_list_webhook_subscriptions(uuid, integer),
    vela_list_webhook_deliveries(uuid, uuid, integer)
    FROM vela_webhook_request;
REVOKE EXECUTE ON FUNCTION vela_set_request_context(uuid, bytea, text)
    FROM vela_webhook_request;
DROP FUNCTION vela_replay_webhook_delivery(uuid, uuid, uuid, uuid);
DROP FUNCTION vela_list_webhook_subscriptions(uuid, integer);
DROP FUNCTION vela_list_webhook_deliveries(uuid, uuid, integer);
DROP FUNCTION vela_disable_webhook_subscription(uuid, uuid);
REVOKE EXECUTE ON FUNCTION vela_claim_webhook_deliveries(text, integer, integer),
    vela_mark_webhook_delivered(uuid, uuid, integer),
    vela_mark_webhook_failed(uuid, uuid, integer, text) FROM vela_webhook;
DROP FUNCTION vela_mark_webhook_failed(uuid, uuid, integer, text);
DROP FUNCTION vela_mark_webhook_delivered(uuid, uuid, integer);
DROP FUNCTION vela_claim_webhook_deliveries(text, integer, integer);
DROP FUNCTION vela_rotate_webhook_secret(
    uuid, uuid, uuid, integer, text, bytea, bytea
);
DROP FUNCTION vela_lock_webhook_secret_rotation(uuid, uuid);
DROP FUNCTION vela_create_webhook_subscription(
    uuid, uuid, text, webhook_event_type[], uuid, text, bytea, bytea
);
DROP FUNCTION vela_current_credential_id();
DROP TRIGGER outbox_terminal_webhook_fanout ON outbox_events;
DROP FUNCTION vela_create_terminal_webhook_deliveries();
DROP TRIGGER webhook_delivery_replays_immutable ON webhook_delivery_replays;
DROP TRIGGER webhook_delivery_attempts_truncate_immutable ON webhook_delivery_attempts;
DROP TRIGGER webhook_delivery_attempts_immutable ON webhook_delivery_attempts;
DROP TRIGGER webhook_deliveries_immutable ON webhook_deliveries;
DROP FUNCTION vela_reject_webhook_receipt_mutation();
DROP FUNCTION vela_enforce_webhook_delivery_attempt_immutability();
DROP FUNCTION vela_enforce_webhook_delivery_immutability();
DROP TABLE webhook_delivery_replays;
DROP TABLE webhook_delivery_attempts;
DROP TABLE webhook_deliveries;
ALTER TABLE outbox_events DROP CONSTRAINT outbox_events_webhook_identity_key;
DROP TABLE webhook_subscription_secrets;
DROP TABLE webhook_subscriptions;
DROP FUNCTION vela_enforce_webhook_secret_immutability();
DROP FUNCTION vela_validate_webhook_subscription();
DROP TYPE webhook_event_type;
DROP TYPE webhook_delivery_attempt_state;
DROP TYPE webhook_delivery_state;
DROP TYPE webhook_subscription_state;
-- +goose StatementEnd
