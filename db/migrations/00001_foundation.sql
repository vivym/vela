-- +goose Up
-- +goose StatementBegin
CREATE EXTENSION IF NOT EXISTS pgcrypto;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vela_request') THEN
        CREATE ROLE vela_request NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vela_auth') THEN
        CREATE ROLE vela_auth NOLOGIN;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'vela_internal') THEN
        CREATE ROLE vela_internal NOLOGIN BYPASSRLS;
    END IF;
END
$$;

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
    UNIQUE (organization_id, id)
);

CREATE TABLE service_principals (
    principal_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, principal_id),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, principal_id) REFERENCES principals(organization_id, id)
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
    created_by_principal_id uuid,
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
    job_ttl_seconds integer NOT NULL CHECK (job_ttl_seconds > 0),
    max_attempts integer NOT NULL CHECK (max_attempts BETWEEN 1 AND 3),
    max_total_compute_multiplier_milli integer NOT NULL CHECK (max_total_compute_multiplier_milli >= 1000),
    max_finalization_seconds_per_attempt integer NOT NULL CHECK (max_finalization_seconds_per_attempt > 0),
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
    pricing_snapshot jsonb NOT NULL CHECK (jsonb_typeof(pricing_snapshot) = 'object'),
    execution_policy_snapshot jsonb NOT NULL CHECK (jsonb_typeof(execution_policy_snapshot) = 'object'),
    job_expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, created_by_principal_id) REFERENCES principals(organization_id, id),
    CHECK (job_expires_at > created_at),
    CHECK (request_content_expires_at > created_at),
    FOREIGN KEY (generation_preset_revision_id, model_revision_id)
        REFERENCES generation_preset_revisions(id, model_revision_id)
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
    claim_expires_at timestamptz,
    published_at timestamptz,
    broker_stream text,
    broker_sequence bigint,
    last_error text,
    UNIQUE (aggregate_type, aggregate_id, aggregate_version, event_type),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, project_id, aggregate_id) REFERENCES jobs(organization_id, project_id, id),
    CHECK (
        (published_at IS NULL AND broker_stream IS NULL AND broker_sequence IS NULL)
        OR
        (published_at IS NOT NULL AND broker_stream IS NOT NULL AND broker_sequence IS NOT NULL)
    )
);

CREATE INDEX jobs_project_state_created_idx ON jobs (organization_id, project_id, state, created_at);
CREATE INDEX outbox_events_ready_idx ON outbox_events (available_at, occurred_at) WHERE published_at IS NULL;
CREATE INDEX credentials_active_idx ON credentials (id, expires_at) WHERE revoked_at IS NULL;

CREATE FUNCTION vela_current_organization_id() RETURNS uuid
LANGUAGE sql STABLE PARALLEL SAFE
AS $$
    SELECT NULLIF(current_setting('vela.organization_id', true), '')::uuid
$$;

CREATE FUNCTION vela_current_project_id() RETURNS uuid
LANGUAGE sql STABLE PARALLEL SAFE
AS $$
    SELECT NULLIF(current_setting('vela.project_id', true), '')::uuid
$$;

CREATE FUNCTION vela_current_principal_id() RETURNS uuid
LANGUAGE sql STABLE PARALLEL SAFE
AS $$
    SELECT NULLIF(current_setting('vela.principal_id', true), '')::uuid
$$;

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
$$;

REVOKE ALL ON FUNCTION vela_authenticate_service_credential(uuid) FROM PUBLIC;
ALTER FUNCTION vela_authenticate_service_credential(uuid) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_authenticate_service_credential(uuid) TO vela_auth;

ALTER TABLE customer_organizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE customer_organizations FORCE ROW LEVEL SECURITY;
CREATE POLICY customer_organizations_request_policy ON customer_organizations
    FOR ALL TO vela_request
    USING (id = vela_current_organization_id())
    WITH CHECK (id = vela_current_organization_id());

ALTER TABLE organization_credit_accounts ENABLE ROW LEVEL SECURITY;
ALTER TABLE organization_credit_accounts FORCE ROW LEVEL SECURITY;
CREATE POLICY organization_credit_accounts_request_policy ON organization_credit_accounts
    FOR ALL TO vela_request
    USING (organization_id = vela_current_organization_id())
    WITH CHECK (organization_id = vela_current_organization_id());

ALTER TABLE projects ENABLE ROW LEVEL SECURITY;
ALTER TABLE projects FORCE ROW LEVEL SECURITY;
CREATE POLICY projects_request_policy ON projects
    FOR ALL TO vela_request
    USING (
        organization_id = vela_current_organization_id()
        AND id = vela_current_project_id()
    )
    WITH CHECK (
        organization_id = vela_current_organization_id()
        AND id = vela_current_project_id()
    );

ALTER TABLE principals ENABLE ROW LEVEL SECURITY;
ALTER TABLE principals FORCE ROW LEVEL SECURITY;
CREATE POLICY principals_request_policy ON principals
    FOR ALL TO vela_request
    USING (
        organization_id = vela_current_organization_id()
        AND id = vela_current_principal_id()
    )
    WITH CHECK (
        organization_id = vela_current_organization_id()
        AND id = vela_current_principal_id()
    );

ALTER TABLE service_principals ENABLE ROW LEVEL SECURITY;
ALTER TABLE service_principals FORCE ROW LEVEL SECURITY;
CREATE POLICY service_principals_request_policy ON service_principals
    FOR ALL TO vela_request
    USING (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
        AND principal_id = vela_current_principal_id()
    )
    WITH CHECK (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
        AND principal_id = vela_current_principal_id()
    );

ALTER TABLE credentials ENABLE ROW LEVEL SECURITY;
ALTER TABLE credentials FORCE ROW LEVEL SECURITY;
CREATE POLICY credentials_request_policy ON credentials
    FOR ALL TO vela_request
    USING (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
        AND principal_id = vela_current_principal_id()
    )
    WITH CHECK (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
        AND principal_id = vela_current_principal_id()
    );

ALTER TABLE jobs ENABLE ROW LEVEL SECURITY;
ALTER TABLE jobs FORCE ROW LEVEL SECURITY;
CREATE POLICY jobs_request_policy ON jobs
    FOR ALL TO vela_request
    USING (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    )
    WITH CHECK (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

ALTER TABLE credit_reservations ENABLE ROW LEVEL SECURITY;
ALTER TABLE credit_reservations FORCE ROW LEVEL SECURITY;
CREATE POLICY credit_reservations_request_policy ON credit_reservations
    FOR ALL TO vela_request
    USING (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    )
    WITH CHECK (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

ALTER TABLE idempotency_results ENABLE ROW LEVEL SECURITY;
ALTER TABLE idempotency_results FORCE ROW LEVEL SECURITY;
CREATE POLICY idempotency_results_request_policy ON idempotency_results
    FOR ALL TO vela_request
    USING (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    )
    WITH CHECK (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

ALTER TABLE outbox_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE outbox_events FORCE ROW LEVEL SECURITY;
CREATE POLICY outbox_events_request_policy ON outbox_events
    FOR ALL TO vela_request
    USING (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    )
    WITH CHECK (
        organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

GRANT USAGE ON SCHEMA public TO vela_request, vela_auth, vela_internal;
GRANT SELECT ON model_revisions, generation_preset_revisions, service_class_revisions,
    output_specs, execution_profile_revisions, profile_certifications,
    rate_card_revisions, rate_card_lines, worker_pools TO vela_request;
GRANT UPDATE (queued_count) ON worker_pools TO vela_request;
GRANT SELECT ON customer_organizations, projects, principals, service_principals,
    organization_credit_accounts, jobs, credit_reservations, idempotency_results,
    outbox_events TO vela_request;
GRANT INSERT ON jobs, credit_reservations, idempotency_results, outbox_events TO vela_request;
GRANT UPDATE (queued_count, running_count) ON projects TO vela_request;
GRANT UPDATE (reserved_minor, version, updated_at) ON organization_credit_accounts TO vela_request;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO vela_internal;

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
        OR NEW.pricing_snapshot IS DISTINCT FROM OLD.pricing_snapshot
        OR NEW.execution_policy_snapshot IS DISTINCT FROM OLD.execution_policy_snapshot
        OR NEW.job_expires_at IS DISTINCT FROM OLD.job_expires_at
    THEN
        RAISE EXCEPTION 'immutable Job snapshot fields cannot be changed';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER jobs_snapshot_immutable
BEFORE UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_reject_job_snapshot_mutation();
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TRIGGER IF EXISTS jobs_snapshot_immutable ON jobs;
DROP FUNCTION IF EXISTS vela_reject_job_snapshot_mutation();
DROP FUNCTION IF EXISTS vela_authenticate_service_credential(uuid);
DROP FUNCTION IF EXISTS vela_current_principal_id();
DROP FUNCTION IF EXISTS vela_current_project_id();
DROP FUNCTION IF EXISTS vela_current_organization_id();
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS idempotency_results;
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
DROP TYPE IF EXISTS credit_reservation_state;
DROP TYPE IF EXISTS job_state;
DROP TYPE IF EXISTS catalog_state;
DROP TYPE IF EXISTS principal_kind;
DROP ROLE IF EXISTS vela_internal;
DROP ROLE IF EXISTS vela_auth;
DROP ROLE IF EXISTS vela_request;
-- +goose StatementEnd
