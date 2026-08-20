-- +goose Up
-- +goose StatementBegin
CREATE TYPE principal_kind AS ENUM ('HUMAN', 'SERVICE');
CREATE TYPE catalog_state AS ENUM ('REGISTERED', 'VALIDATING', 'CERTIFIED', 'CANARY', 'ACTIVE', 'DRAINING', 'RETIRED', 'INVALID');
CREATE TYPE job_state AS ENUM ('QUEUED', 'ASSIGNED', 'RUNNING', 'FINALIZING', 'RETRY_WAIT', 'CANCELING', 'SUCCEEDED', 'FAILED', 'CANCELED');
CREATE TYPE credit_reservation_state AS ENUM ('RESERVED', 'CONSUMED', 'RELEASED');

CREATE TABLE customer_organizations (
    id uuid PRIMARY KEY,
    display_name text NOT NULL CHECK (length(display_name) BETWEEN 1 AND 200),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'SUSPENDED', 'CLOSED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (id)
);

CREATE TABLE projects (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    display_name text NOT NULL CHECK (length(display_name) BETWEEN 1 AND 200),
    queued_limit integer NOT NULL CHECK (queued_limit >= 0),
    queued_count integer NOT NULL DEFAULT 0 CHECK (queued_count >= 0 AND queued_count <= queued_limit),
    running_limit integer NOT NULL CHECK (running_limit >= 0),
    running_count integer NOT NULL DEFAULT 0 CHECK (running_count >= 0 AND running_count <= running_limit),
    retry_after_seconds integer NOT NULL DEFAULT 30 CHECK (retry_after_seconds > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, id)
);

CREATE TABLE principals (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    kind principal_kind NOT NULL,
    display_name text NOT NULL CHECK (length(display_name) BETWEEN 1 AND 200),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, id),
    UNIQUE (organization_id, id, kind)
);

CREATE TABLE service_principals (
    principal_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    principal_kind principal_kind NOT NULL DEFAULT 'SERVICE'
        CHECK (principal_kind = 'SERVICE'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, principal_id),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, principal_id, principal_kind)
        REFERENCES principals(organization_id, id, kind)
);

CREATE TABLE credentials (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    principal_id uuid NOT NULL,
    secret_digest bytea NOT NULL CHECK (octet_length(secret_digest) = 32),
    scopes text[] NOT NULL CHECK (cardinality(scopes) > 0),
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_by_principal_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, principal_id)
        REFERENCES service_principals(organization_id, project_id, principal_id),
    FOREIGN KEY (organization_id, created_by_principal_id)
        REFERENCES principals(organization_id, id)
);

CREATE TABLE organization_credit_accounts (
    organization_id uuid PRIMARY KEY REFERENCES customer_organizations(id),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    contract_credit_limit_minor bigint NOT NULL CHECK (contract_credit_limit_minor >= 0),
    reserved_minor bigint NOT NULL DEFAULT 0 CHECK (reserved_minor >= 0),
    unsettled_posted_minor bigint NOT NULL DEFAULT 0 CHECK (unsettled_posted_minor >= 0),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (reserved_minor + unsettled_posted_minor <= contract_credit_limit_minor)
);

CREATE TABLE worker_pools (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL UNIQUE CHECK (length(stable_id) BETWEEN 1 AND 100),
    admission_open boolean NOT NULL DEFAULT true,
    queued_limit integer NOT NULL CHECK (queued_limit >= 0),
    queued_count integer NOT NULL DEFAULT 0 CHECK (queued_count >= 0 AND queued_count <= queued_limit),
    retry_after_seconds integer NOT NULL DEFAULT 30 CHECK (retry_after_seconds > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE model_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    content_hash text NOT NULL CHECK (length(content_hash) >= 16),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE generation_preset_revisions (
    id uuid PRIMARY KEY,
    model_revision_id uuid NOT NULL REFERENCES model_revisions(id),
    stable_id text NOT NULL CHECK (stable_id IN ('quality', 'balanced', 'fast')),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    certified_p95_compute_seconds integer NOT NULL CHECK (certified_p95_compute_seconds > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (model_revision_id, stable_id, revision),
    UNIQUE (id, model_revision_id)
);

CREATE TABLE service_class_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    queue_retry_allowance_seconds integer NOT NULL CHECK (queue_retry_allowance_seconds > 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 3),
    max_total_compute_multiplier_milli integer NOT NULL
        CHECK (max_total_compute_multiplier_milli BETWEEN 1000 AND 2000),
    max_finalization_seconds_per_attempt integer NOT NULL CHECK (max_finalization_seconds_per_attempt > 0),
    retry_backoff_policy jsonb NOT NULL CHECK (jsonb_typeof(retry_backoff_policy) = 'object'),
    retryable_failure_classes text[] NOT NULL CHECK (cardinality(retryable_failure_classes) > 0),
    circuit_breaker_policy jsonb NOT NULL CHECK (jsonb_typeof(circuit_breaker_policy) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE output_specs (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    width integer NOT NULL CHECK (width > 0),
    height integer NOT NULL CHECK (height > 0),
    duration_milliseconds integer NOT NULL CHECK (duration_milliseconds > 0),
    frame_rate_milli integer NOT NULL CHECK (frame_rate_milli > 0),
    codec text NOT NULL CHECK (length(codec) BETWEEN 1 AND 50),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE execution_profile_revisions (
    id uuid PRIMARY KEY,
    model_revision_id uuid NOT NULL REFERENCES model_revisions(id),
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    stable_id text NOT NULL CHECK (length(stable_id) BETWEEN 1 AND 100),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (model_revision_id, stable_id, revision),
    UNIQUE (id, model_revision_id)
);

CREATE TABLE profile_certifications (
    id uuid PRIMARY KEY,
    model_revision_id uuid NOT NULL REFERENCES model_revisions(id),
    generation_preset_revision_id uuid NOT NULL,
    output_spec_id uuid NOT NULL REFERENCES output_specs(id),
    execution_profile_revision_id uuid NOT NULL,
    state catalog_state NOT NULL,
    evidence_digest text NOT NULL CHECK (length(evidence_digest) >= 16),
    certified_at timestamptz NOT NULL,
    invalidated_at timestamptz,
    UNIQUE (generation_preset_revision_id, output_spec_id, execution_profile_revision_id),
    FOREIGN KEY (generation_preset_revision_id, model_revision_id)
        REFERENCES generation_preset_revisions(id, model_revision_id),
    FOREIGN KEY (execution_profile_revision_id, model_revision_id)
        REFERENCES execution_profile_revisions(id, model_revision_id)
);

CREATE TABLE rate_card_revisions (
    id uuid PRIMARY KEY,
    revision integer NOT NULL UNIQUE CHECK (revision > 0),
    state catalog_state NOT NULL,
    effective_at timestamptz NOT NULL,
    expires_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (expires_at IS NULL OR expires_at > effective_at)
);

CREATE TABLE rate_card_lines (
    id uuid PRIMARY KEY,
    rate_card_revision_id uuid NOT NULL REFERENCES rate_card_revisions(id),
    model_revision_id uuid NOT NULL REFERENCES model_revisions(id),
    generation_preset_revision_id uuid NOT NULL,
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    output_spec_id uuid NOT NULL REFERENCES output_specs(id),
    unit_amount_minor bigint NOT NULL CHECK (unit_amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    UNIQUE (
        rate_card_revision_id,
        model_revision_id,
        generation_preset_revision_id,
        service_class_revision_id,
        output_spec_id,
        currency
    ),
    UNIQUE (
        id,
        rate_card_revision_id,
        model_revision_id,
        generation_preset_revision_id,
        service_class_revision_id,
        output_spec_id,
        currency,
        unit_amount_minor
    ),
    FOREIGN KEY (generation_preset_revision_id, model_revision_id)
        REFERENCES generation_preset_revisions(id, model_revision_id)
);

CREATE TABLE jobs (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    created_by_principal_id uuid NOT NULL,
    state job_state NOT NULL DEFAULT 'QUEUED',
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    model_revision_id uuid NOT NULL REFERENCES model_revisions(id),
    generation_preset_revision_id uuid NOT NULL,
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    output_spec_id uuid NOT NULL REFERENCES output_specs(id),
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    request_content jsonb NOT NULL CHECK (jsonb_typeof(request_content) = 'object'),
    request_content_expires_at timestamptz NOT NULL,
    pricing_rate_card_revision_id uuid NOT NULL,
    pricing_rate_line_id uuid NOT NULL,
    pricing_unit_amount_minor bigint NOT NULL CHECK (pricing_unit_amount_minor >= 0),
    pricing_quantity integer NOT NULL CHECK (pricing_quantity > 0),
    pricing_quoted_amount_minor bigint NOT NULL CHECK (pricing_quoted_amount_minor >= 0),
    pricing_currency text NOT NULL CHECK (pricing_currency ~ '^[A-Z]{3}$'),
    execution_max_attempts integer NOT NULL CHECK (execution_max_attempts BETWEEN 1 AND 3),
    execution_max_total_compute_seconds bigint NOT NULL CHECK (execution_max_total_compute_seconds > 0),
    execution_max_finalization_seconds_per_attempt integer NOT NULL
        CHECK (execution_max_finalization_seconds_per_attempt > 0),
    execution_retry_backoff_policy jsonb NOT NULL
        CHECK (jsonb_typeof(execution_retry_backoff_policy) = 'object'),
    execution_retryable_failure_classes text[] NOT NULL
        CHECK (cardinality(execution_retryable_failure_classes) > 0),
    execution_circuit_breaker_policy jsonb NOT NULL
        CHECK (jsonb_typeof(execution_circuit_breaker_policy) = 'object'),
    job_expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, id, pricing_quoted_amount_minor, pricing_currency),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, project_id, created_by_principal_id)
        REFERENCES service_principals(organization_id, project_id, principal_id),
    CHECK (job_expires_at > created_at),
    CHECK (request_content_expires_at > created_at),
    CHECK (pricing_quoted_amount_minor = pricing_unit_amount_minor * pricing_quantity),
    FOREIGN KEY (generation_preset_revision_id, model_revision_id)
        REFERENCES generation_preset_revisions(id, model_revision_id),
    FOREIGN KEY (
        pricing_rate_line_id,
        pricing_rate_card_revision_id,
        model_revision_id,
        generation_preset_revision_id,
        service_class_revision_id,
        output_spec_id,
        pricing_currency,
        pricing_unit_amount_minor
    ) REFERENCES rate_card_lines (
        id,
        rate_card_revision_id,
        model_revision_id,
        generation_preset_revision_id,
        service_class_revision_id,
        output_spec_id,
        currency,
        unit_amount_minor
    )
);

CREATE TABLE credit_reservations (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL UNIQUE,
    amount_minor bigint NOT NULL CHECK (amount_minor >= 0),
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    state credit_reservation_state NOT NULL DEFAULT 'RESERVED',
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, job_id, amount_minor, currency)
        REFERENCES jobs (
            organization_id,
            project_id,
            id,
            pricing_quoted_amount_minor,
            pricing_currency
        )
);

CREATE TABLE retry_runtime_states (
    job_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    attempts_started integer NOT NULL DEFAULT 0 CHECK (attempts_started >= 0),
    compute_seconds_consumed bigint NOT NULL DEFAULT 0 CHECK (compute_seconds_consumed >= 0),
    finalization_seconds_consumed bigint NOT NULL DEFAULT 0 CHECK (finalization_seconds_consumed >= 0),
    finalization_retry_count integer NOT NULL DEFAULT 0 CHECK (finalization_retry_count >= 0),
    next_retry_at timestamptz,
    excluded_workers jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(excluded_workers) = 'array'),
    failure_fingerprints jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (jsonb_typeof(failure_fingerprints) = 'array'),
    circuit_breaker_state jsonb NOT NULL DEFAULT '{}'::jsonb CHECK (jsonb_typeof(circuit_breaker_state) = 'object'),
    last_failure_class text,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, job_id),
    FOREIGN KEY (organization_id, project_id, job_id) REFERENCES jobs(organization_id, project_id, id)
);

CREATE TABLE idempotency_results (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    idempotency_key text NOT NULL CHECK (length(idempotency_key) BETWEEN 1 AND 128),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    job_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (organization_id, project_id, idempotency_key),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, project_id, job_id) REFERENCES jobs(organization_id, project_id, id)
);

CREATE TABLE outbox_events (
    event_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0),
    event_type text NOT NULL,
    schema_version integer NOT NULL CHECK (schema_version > 0),
    payload bytea NOT NULL,
    occurred_at timestamptz NOT NULL,
    available_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    publish_attempts integer NOT NULL DEFAULT 0 CHECK (publish_attempts >= 0),
    claimed_by text,
    claim_token uuid,
    claim_expires_at timestamptz,
    published_at timestamptz,
    broker_stream text,
    broker_sequence bigint CHECK (broker_sequence > 0),
    last_error text,
    UNIQUE (aggregate_type, aggregate_id, aggregate_version, event_type),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, project_id, aggregate_id) REFERENCES jobs(organization_id, project_id, id),
    CHECK (
        (published_at IS NULL AND broker_stream IS NULL AND broker_sequence IS NULL)
        OR
        (published_at IS NOT NULL AND broker_stream IS NOT NULL AND broker_sequence IS NOT NULL)
    ),
    CHECK (
        (claimed_by IS NULL AND claim_token IS NULL AND claim_expires_at IS NULL)
        OR
        (claimed_by IS NOT NULL AND claim_token IS NOT NULL AND claim_expires_at IS NOT NULL)
    )
);

CREATE TABLE inbox_receipts (
    consumer_name text NOT NULL CHECK (length(consumer_name) BETWEEN 1 AND 100),
    event_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    aggregate_type text NOT NULL CHECK (length(aggregate_type) BETWEEN 1 AND 100),
    aggregate_id uuid NOT NULL,
    aggregate_version bigint NOT NULL CHECK (aggregate_version > 0),
    event_type text NOT NULL CHECK (length(event_type) BETWEEN 1 AND 100),
    consumed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (consumer_name, event_id),
    UNIQUE (consumer_name, aggregate_type, aggregate_id, aggregate_version, event_type),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, project_id, aggregate_id)
        REFERENCES jobs(organization_id, project_id, id)
);

CREATE INDEX jobs_project_state_created_idx ON jobs (organization_id, project_id, state, created_at);
CREATE INDEX outbox_events_ready_idx
    ON outbox_events (available_at, claim_expires_at, occurred_at)
    WHERE published_at IS NULL;
CREATE INDEX credentials_active_idx ON credentials (id, expires_at) WHERE revoked_at IS NULL;
CREATE UNIQUE INDEX model_revisions_one_active_idx ON model_revisions (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX generation_preset_revisions_one_active_idx
    ON generation_preset_revisions (model_revision_id, stable_id)
    WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX service_class_revisions_one_active_idx
    ON service_class_revisions (stable_id)
    WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX output_specs_one_active_idx ON output_specs (stable_id) WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX execution_profile_revisions_one_active_idx
    ON execution_profile_revisions (model_revision_id, stable_id)
    WHERE state = 'ACTIVE';

CREATE SCHEMA vela_private AUTHORIZATION vela_internal;
REVOKE ALL ON SCHEMA vela_private FROM PUBLIC;

CREATE TABLE vela_private.request_contexts (
    backend_pid integer PRIMARY KEY,
    transaction_id xid8 NOT NULL,
    credential_id uuid NOT NULL,
    required_scope text NOT NULL CHECK (length(required_scope) BETWEEN 1 AND 100),
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    principal_id uuid NOT NULL,
    established_at timestamptz NOT NULL
);
ALTER TABLE vela_private.request_contexts OWNER TO vela_internal;
REVOKE ALL ON TABLE vela_private.request_contexts FROM PUBLIC, vela_request, vela_auth;

CREATE FUNCTION vela_current_organization_id() RETURNS uuid
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, vela_private
AS $$
    SELECT context.organization_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id_if_assigned()
$$;
REVOKE ALL ON FUNCTION vela_current_organization_id() FROM PUBLIC;
ALTER FUNCTION vela_current_organization_id() OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_current_organization_id() TO vela_request;

CREATE FUNCTION vela_current_project_id() RETURNS uuid
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, vela_private
AS $$
    SELECT context.project_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id_if_assigned()
$$;
REVOKE ALL ON FUNCTION vela_current_project_id() FROM PUBLIC;
ALTER FUNCTION vela_current_project_id() OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_current_project_id() TO vela_request;

CREATE FUNCTION vela_current_principal_id() RETURNS uuid
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, vela_private
AS $$
    SELECT context.principal_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id_if_assigned()
$$;
REVOKE ALL ON FUNCTION vela_current_principal_id() FROM PUBLIC;
ALTER FUNCTION vela_current_principal_id() OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_current_principal_id() TO vela_request;

CREATE FUNCTION vela_current_request_scope() RETURNS text
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, vela_private
AS $$
    SELECT context.required_scope
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id_if_assigned()
$$;
REVOKE ALL ON FUNCTION vela_current_request_scope() FROM PUBLIC;
ALTER FUNCTION vela_current_request_scope() OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_current_request_scope() TO vela_request;

CREATE FUNCTION vela_set_request_context(
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

CREATE FUNCTION vela_authenticate_service_credential(p_credential_id uuid)
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
        c.organization_id,
        c.project_id,
        c.principal_id,
        c.secret_digest,
        c.scopes,
        c.expires_at,
        c.revoked_at
    FROM public.credentials AS c
    WHERE c.id = p_credential_id
      AND c.revoked_at IS NULL
      AND c.expires_at > clock_timestamp()
$$;

REVOKE ALL ON FUNCTION vela_authenticate_service_credential(uuid) FROM PUBLIC;
ALTER FUNCTION vela_authenticate_service_credential(uuid) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_authenticate_service_credential(uuid) TO vela_auth;

CREATE FUNCTION vela_resolve_active_sku(
    p_model text,
    p_generation_preset text,
    p_service_class text,
    p_output_spec text
) RETURNS TABLE (
    model_revision_id uuid,
    generation_preset_revision_id uuid,
    certified_p95_compute_seconds integer,
    service_class_revision_id uuid,
    queue_retry_allowance_seconds integer,
    max_attempts integer,
    max_total_compute_multiplier_milli integer,
    max_finalization_seconds_per_attempt integer,
    retry_backoff_policy jsonb,
    retryable_failure_classes text[],
    circuit_breaker_policy jsonb,
    output_spec_id uuid,
    rate_card_revision_id uuid,
    rate_line_id uuid,
    unit_amount_minor bigint,
    currency text
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        mr.id,
        gpr.id,
        gpr.certified_p95_compute_seconds,
        scr.id,
        scr.queue_retry_allowance_seconds,
        scr.max_attempts,
        scr.max_total_compute_multiplier_milli,
        scr.max_finalization_seconds_per_attempt,
        scr.retry_backoff_policy,
        scr.retryable_failure_classes,
        scr.circuit_breaker_policy,
        os.id,
        rcr.id,
        rcl.id,
        rcl.unit_amount_minor,
        rcl.currency
    FROM public.model_revisions AS mr
    JOIN public.generation_preset_revisions AS gpr ON gpr.model_revision_id = mr.id
    JOIN public.service_class_revisions AS scr ON true
    JOIN public.output_specs AS os ON true
    JOIN public.rate_card_lines AS rcl
      ON rcl.model_revision_id = mr.id
     AND rcl.generation_preset_revision_id = gpr.id
     AND rcl.service_class_revision_id = scr.id
     AND rcl.output_spec_id = os.id
    JOIN public.rate_card_revisions AS rcr ON rcr.id = rcl.rate_card_revision_id
    WHERE mr.stable_id = p_model
      AND public.vela_current_request_scope() = 'jobs:submit'
      AND mr.state = 'ACTIVE'
      AND gpr.stable_id = p_generation_preset
      AND gpr.state = 'ACTIVE'
      AND scr.stable_id = p_service_class
      AND scr.state = 'ACTIVE'
      AND os.stable_id = p_output_spec
      AND os.state = 'ACTIVE'
      AND rcr.state = 'ACTIVE'
      AND rcr.effective_at <= transaction_timestamp()
      AND (rcr.expires_at IS NULL OR rcr.expires_at > transaction_timestamp())
    ORDER BY rcr.effective_at DESC, rcr.revision DESC
    LIMIT 1
    FOR SHARE OF mr, gpr, scr, os, rcl, rcr
$$;

REVOKE ALL ON FUNCTION vela_resolve_active_sku(text, text, text, text) FROM PUBLIC;
ALTER FUNCTION vela_resolve_active_sku(text, text, text, text) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_resolve_active_sku(text, text, text, text) TO vela_request;

CREATE FUNCTION vela_lock_compatible_pool(
    p_model_revision_id uuid,
    p_generation_preset_revision_id uuid,
    p_output_spec_id uuid
) RETURNS TABLE (
    id uuid,
    admission_open boolean,
    queued_count integer,
    queued_limit integer,
    retry_after_seconds integer
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        wp.id,
        wp.admission_open,
        wp.queued_count,
        wp.queued_limit,
        wp.retry_after_seconds
    FROM public.profile_certifications AS pc
    JOIN public.execution_profile_revisions AS epr
      ON epr.id = pc.execution_profile_revision_id
    JOIN public.worker_pools AS wp ON wp.id = epr.worker_pool_id
    WHERE pc.model_revision_id = p_model_revision_id
      AND public.vela_current_request_scope() = 'jobs:submit'
      AND pc.generation_preset_revision_id = p_generation_preset_revision_id
      AND pc.output_spec_id = p_output_spec_id
      AND pc.state = 'ACTIVE'
      AND pc.invalidated_at IS NULL
      AND epr.state = 'ACTIVE'
      AND wp.admission_open
      AND wp.queued_count < wp.queued_limit
    ORDER BY
        CASE
            WHEN wp.queued_limit = 0 THEN 1::numeric
            ELSE wp.queued_count::numeric / wp.queued_limit::numeric
        END,
        wp.stable_id
    LIMIT 1
    FOR SHARE OF pc, epr
    FOR UPDATE OF wp
$$;

REVOKE ALL ON FUNCTION vela_lock_compatible_pool(uuid, uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_lock_compatible_pool(uuid, uuid, uuid) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_lock_compatible_pool(uuid, uuid, uuid) TO vela_request;

ALTER TABLE worker_pools ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_pools FORCE ROW LEVEL SECURITY;
CREATE POLICY worker_pools_request_policy ON worker_pools
    FOR ALL TO vela_request
    USING (vela_current_request_scope() = 'jobs:submit')
    WITH CHECK (vela_current_request_scope() = 'jobs:submit');

ALTER TABLE customer_organizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE customer_organizations FORCE ROW LEVEL SECURITY;
CREATE POLICY customer_organizations_request_policy ON customer_organizations
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND id = vela_current_organization_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND id = vela_current_organization_id()
    );

ALTER TABLE organization_credit_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE organization_credit_accounts FORCE ROW LEVEL SECURITY;
CREATE POLICY organization_credit_accounts_request_policy ON organization_credit_accounts
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
    );

ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects FORCE ROW LEVEL SECURITY;
CREATE POLICY projects_request_policy ON projects
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND id = vela_current_project_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND id = vela_current_project_id()
    );

ALTER TABLE principals ENABLE ROW LEVEL SECURITY;
ALTER TABLE principals FORCE ROW LEVEL SECURITY;
CREATE POLICY principals_request_policy ON principals
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND id = vela_current_principal_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND id = vela_current_principal_id()
    );

ALTER TABLE service_principals ENABLE ROW LEVEL SECURITY;
ALTER TABLE service_principals FORCE ROW LEVEL SECURITY;
CREATE POLICY service_principals_request_policy ON service_principals
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
        AND principal_id = vela_current_principal_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
        AND principal_id = vela_current_principal_id()
    );

ALTER TABLE credentials ENABLE ROW LEVEL SECURITY;
ALTER TABLE credentials FORCE ROW LEVEL SECURITY;
CREATE POLICY credentials_request_policy ON credentials
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
        AND principal_id = vela_current_principal_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
        AND principal_id = vela_current_principal_id()
    );

ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY jobs_request_select_policy ON jobs
    FOR SELECT TO vela_request
    USING (
        vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );
CREATE POLICY jobs_request_insert_policy ON jobs
    FOR INSERT TO vela_request
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
        AND created_by_principal_id = vela_current_principal_id()
    );

ALTER TABLE credit_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE credit_reservations FORCE ROW LEVEL SECURITY;
CREATE POLICY credit_reservations_request_policy ON credit_reservations
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

ALTER TABLE retry_runtime_states ENABLE ROW LEVEL SECURITY;
ALTER TABLE retry_runtime_states FORCE ROW LEVEL SECURITY;
CREATE POLICY retry_runtime_states_request_policy ON retry_runtime_states
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

ALTER TABLE idempotency_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_results FORCE ROW LEVEL SECURITY;
CREATE POLICY idempotency_results_request_policy ON idempotency_results
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_events FORCE ROW LEVEL SECURITY;
CREATE POLICY outbox_events_request_policy ON outbox_events
    FOR ALL TO vela_request
    USING (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    )
    WITH CHECK (
        vela_current_request_scope() = 'jobs:submit'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

ALTER TABLE inbox_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE inbox_receipts FORCE ROW LEVEL SECURITY;

GRANT USAGE ON SCHEMA public TO vela_request, vela_auth, vela_internal;
GRANT SELECT ON worker_pools TO vela_request;
GRANT UPDATE (queued_count) ON worker_pools TO vela_request;
GRANT SELECT ON customer_organizations, projects, principals, service_principals,
    organization_credit_accounts, jobs, credit_reservations, retry_runtime_states, idempotency_results,
    outbox_events TO vela_request;
GRANT INSERT ON jobs, credit_reservations, retry_runtime_states, idempotency_results,
    outbox_events TO vela_request;
GRANT UPDATE (queued_count, running_count) ON projects TO vela_request;
GRANT UPDATE (reserved_minor, version, updated_at) ON organization_credit_accounts TO vela_request;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO vela_internal;

CREATE FUNCTION vela_reject_revision_definition_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF (
        CASE WHEN TG_NARGS = 0 THEN to_jsonb(NEW) ELSE to_jsonb(NEW) - TG_ARGV END
    ) IS DISTINCT FROM (
        CASE WHEN TG_NARGS = 0 THEN to_jsonb(OLD) ELSE to_jsonb(OLD) - TG_ARGV END
    ) THEN
        RAISE EXCEPTION 'immutable revision definition fields cannot be changed on %', TG_TABLE_NAME;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_revision_definition_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_revision_definition_mutation() OWNER TO vela_internal;

CREATE TRIGGER model_revisions_definition_immutable
BEFORE UPDATE ON model_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');

CREATE TRIGGER generation_preset_revisions_definition_immutable
BEFORE UPDATE ON generation_preset_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');

CREATE TRIGGER service_class_revisions_definition_immutable
BEFORE UPDATE ON service_class_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');

CREATE TRIGGER output_specs_definition_immutable
BEFORE UPDATE ON output_specs
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');

CREATE TRIGGER execution_profile_revisions_definition_immutable
BEFORE UPDATE ON execution_profile_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');

CREATE TRIGGER profile_certifications_definition_immutable
BEFORE UPDATE ON profile_certifications
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state', 'invalidated_at');

CREATE TRIGGER rate_card_revisions_definition_immutable
BEFORE UPDATE ON rate_card_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation('state');

CREATE TRIGGER rate_card_lines_immutable
BEFORE UPDATE ON rate_card_lines
FOR EACH ROW EXECUTE FUNCTION vela_reject_revision_definition_mutation();

CREATE FUNCTION vela_assert_job_required_children(p_job_id uuid) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM public.jobs WHERE id = p_job_id)
        AND (
            (SELECT count(*) FROM public.credit_reservations WHERE job_id = p_job_id) <> 1
            OR (SELECT count(*) FROM public.retry_runtime_states WHERE job_id = p_job_id) <> 1
        )
    THEN
        RAISE EXCEPTION 'Job % must have exactly one CreditReservation and RetryRuntimeState', p_job_id
            USING ERRCODE = '23514';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_assert_job_required_children(uuid) FROM PUBLIC;
ALTER FUNCTION vela_assert_job_required_children(uuid) OWNER TO vela_internal;

CREATE FUNCTION vela_enforce_job_required_children() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM public.vela_assert_job_required_children(NEW.id);
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_job_required_children() FROM PUBLIC;
ALTER FUNCTION vela_enforce_job_required_children() OWNER TO vela_internal;

CREATE CONSTRAINT TRIGGER jobs_required_children_complete
AFTER INSERT OR UPDATE ON jobs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_job_required_children();

CREATE FUNCTION vela_enforce_required_child_change() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM public.vela_assert_job_required_children(OLD.job_id);
    IF TG_OP = 'UPDATE' AND NEW.job_id IS DISTINCT FROM OLD.job_id THEN
        PERFORM public.vela_assert_job_required_children(NEW.job_id);
    END IF;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_required_child_change() FROM PUBLIC;
ALTER FUNCTION vela_enforce_required_child_change() OWNER TO vela_internal;

CREATE CONSTRAINT TRIGGER credit_reservations_required_for_job
AFTER DELETE OR UPDATE ON credit_reservations
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_required_child_change();

CREATE CONSTRAINT TRIGGER retry_runtime_states_required_for_job
AFTER DELETE OR UPDATE ON retry_runtime_states
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_required_child_change();

CREATE FUNCTION vela_reject_job_snapshot_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.organization_id IS DISTINCT FROM OLD.organization_id
        OR NEW.project_id IS DISTINCT FROM OLD.project_id
        OR NEW.created_by_principal_id IS DISTINCT FROM OLD.created_by_principal_id
        OR NEW.model_revision_id IS DISTINCT FROM OLD.model_revision_id
        OR NEW.generation_preset_revision_id IS DISTINCT FROM OLD.generation_preset_revision_id
        OR NEW.service_class_revision_id IS DISTINCT FROM OLD.service_class_revision_id
        OR NEW.output_spec_id IS DISTINCT FROM OLD.output_spec_id
        OR NEW.worker_pool_id IS DISTINCT FROM OLD.worker_pool_id
        OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
        OR NEW.request_content IS DISTINCT FROM OLD.request_content
        OR NEW.request_content_expires_at IS DISTINCT FROM OLD.request_content_expires_at
        OR NEW.pricing_rate_card_revision_id IS DISTINCT FROM OLD.pricing_rate_card_revision_id
        OR NEW.pricing_rate_line_id IS DISTINCT FROM OLD.pricing_rate_line_id
        OR NEW.pricing_unit_amount_minor IS DISTINCT FROM OLD.pricing_unit_amount_minor
        OR NEW.pricing_quantity IS DISTINCT FROM OLD.pricing_quantity
        OR NEW.pricing_quoted_amount_minor IS DISTINCT FROM OLD.pricing_quoted_amount_minor
        OR NEW.pricing_currency IS DISTINCT FROM OLD.pricing_currency
        OR NEW.execution_max_attempts IS DISTINCT FROM OLD.execution_max_attempts
        OR NEW.execution_max_total_compute_seconds IS DISTINCT FROM OLD.execution_max_total_compute_seconds
        OR NEW.execution_max_finalization_seconds_per_attempt
            IS DISTINCT FROM OLD.execution_max_finalization_seconds_per_attempt
        OR NEW.execution_retry_backoff_policy IS DISTINCT FROM OLD.execution_retry_backoff_policy
        OR NEW.execution_retryable_failure_classes IS DISTINCT FROM OLD.execution_retryable_failure_classes
        OR NEW.execution_circuit_breaker_policy IS DISTINCT FROM OLD.execution_circuit_breaker_policy
        OR NEW.job_expires_at IS DISTINCT FROM OLD.job_expires_at
    THEN
        RAISE EXCEPTION 'immutable Job snapshot fields cannot be changed';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_job_snapshot_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_job_snapshot_mutation() OWNER TO vela_internal;

CREATE TRIGGER jobs_snapshot_immutable
BEFORE UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_reject_job_snapshot_mutation();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS jobs_snapshot_immutable ON jobs;
DROP TRIGGER IF EXISTS retry_runtime_states_required_for_job ON retry_runtime_states;
DROP TRIGGER IF EXISTS credit_reservations_required_for_job ON credit_reservations;
DROP TRIGGER IF EXISTS jobs_required_children_complete ON jobs;
DROP TRIGGER IF EXISTS rate_card_lines_immutable ON rate_card_lines;
DROP TRIGGER IF EXISTS rate_card_revisions_definition_immutable ON rate_card_revisions;
DROP TRIGGER IF EXISTS profile_certifications_definition_immutable ON profile_certifications;
DROP TRIGGER IF EXISTS execution_profile_revisions_definition_immutable ON execution_profile_revisions;
DROP TRIGGER IF EXISTS output_specs_definition_immutable ON output_specs;
DROP TRIGGER IF EXISTS service_class_revisions_definition_immutable ON service_class_revisions;
DROP TRIGGER IF EXISTS generation_preset_revisions_definition_immutable ON generation_preset_revisions;
DROP TRIGGER IF EXISTS model_revisions_definition_immutable ON model_revisions;
DROP FUNCTION IF EXISTS vela_enforce_required_child_change();
DROP FUNCTION IF EXISTS vela_enforce_job_required_children();
DROP FUNCTION IF EXISTS vela_assert_job_required_children(uuid);
DROP FUNCTION IF EXISTS vela_reject_revision_definition_mutation();
DROP FUNCTION IF EXISTS vela_lock_compatible_pool(uuid, uuid, uuid);
DROP FUNCTION IF EXISTS vela_resolve_active_sku(text, text, text, text);
DROP FUNCTION IF EXISTS vela_set_request_context(uuid, bytea, text);
DROP FUNCTION IF EXISTS vela_authenticate_service_credential(uuid);
DROP TABLE IF EXISTS inbox_receipts;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS idempotency_results;
DROP TABLE IF EXISTS retry_runtime_states;
DROP TABLE IF EXISTS credit_reservations;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS rate_card_lines;
DROP TABLE IF EXISTS rate_card_revisions;
DROP TABLE IF EXISTS profile_certifications;
DROP TABLE IF EXISTS execution_profile_revisions;
DROP TABLE IF EXISTS output_specs;
DROP TABLE IF EXISTS service_class_revisions;
DROP TABLE IF EXISTS generation_preset_revisions;
DROP TABLE IF EXISTS model_revisions;
DROP TABLE IF EXISTS worker_pools;
DROP TABLE IF EXISTS organization_credit_accounts;
DROP TABLE IF EXISTS credentials;
DROP TABLE IF EXISTS service_principals;
DROP TABLE IF EXISTS principals;
DROP TABLE IF EXISTS projects;
DROP TABLE IF EXISTS customer_organizations;
DROP FUNCTION IF EXISTS vela_reject_job_snapshot_mutation();
DROP FUNCTION IF EXISTS vela_current_principal_id();
DROP FUNCTION IF EXISTS vela_current_project_id();
DROP FUNCTION IF EXISTS vela_current_organization_id();
DROP FUNCTION IF EXISTS vela_current_request_scope();
DROP TABLE IF EXISTS vela_private.request_contexts;
DROP SCHEMA IF EXISTS vela_private;
DROP TYPE IF EXISTS credit_reservation_state;
DROP TYPE IF EXISTS job_state;
DROP TYPE IF EXISTS catalog_state;
DROP TYPE IF EXISTS principal_kind;
-- +goose StatementEnd
