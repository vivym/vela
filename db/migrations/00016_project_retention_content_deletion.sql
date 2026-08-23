-- +goose Up
-- +goose StatementBegin
CREATE TYPE content_deletion_source AS ENUM (
    'CUSTOMER', 'RETENTION_REQUEST_CONTENT', 'RETENTION_ARTIFACT'
);
CREATE TYPE content_deletion_state AS ENUM (
    'PENDING', 'IN_PROGRESS', 'RETRY_WAIT', 'COMPLETED'
);
CREATE TYPE content_deletion_target_action AS ENUM (
    'OBJECT_VERSION', 'OBJECT_DISCOVERY', 'MULTIPART_PREFIX'
);
CREATE TYPE content_deletion_target_state AS ENUM (
    'PENDING', 'IN_PROGRESS', 'RETRY_WAIT', 'COMPLETED'
);

CREATE TABLE retention_policy_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL UNIQUE CHECK (length(stable_id) BETWEEN 1 AND 100),
    artifact_retention_days integer NOT NULL CHECK (
        artifact_retention_days IN (7, 30, 90)
    ),
    request_content_retention_days integer NOT NULL CHECK (
        request_content_retention_days = 30
    ),
    incomplete_content_retention_hours integer NOT NULL CHECK (
        incomplete_content_retention_hours = 24
    ),
    scratch_retention_hours integer NOT NULL CHECK (scratch_retention_hours = 24),
    debug_retention_hours integer NOT NULL CHECK (debug_retention_hours = 72),
    metadata_retention_days integer NOT NULL CHECK (metadata_retention_days = 365),
    financial_retention_days integer NOT NULL CHECK (financial_retention_days = 2557),
    state text NOT NULL CHECK (state IN ('ACTIVE', 'RETIRED')),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
ALTER TABLE retention_policy_revisions OWNER TO vela_retention_owner;
REVOKE ALL ON TABLE retention_policy_revisions FROM PUBLIC;

CREATE UNIQUE INDEX retention_policy_revisions_one_active_duration_idx
    ON retention_policy_revisions (artifact_retention_days)
    WHERE state = 'ACTIVE';

INSERT INTO retention_policy_revisions (
    id,
    stable_id,
    artifact_retention_days,
    request_content_retention_days,
    incomplete_content_retention_hours,
    scratch_retention_hours,
    debug_retention_hours,
    metadata_retention_days,
    financial_retention_days,
    state
) VALUES
    (
        '00000000-0000-0000-0000-000000001607',
        'artifact-7d-v1', 7, 30, 24, 24, 72, 365, 2557, 'ACTIVE'
    ),
    (
        '00000000-0000-0000-0000-000000001630',
        'artifact-30d-v1', 30, 30, 24, 24, 72, 365, 2557, 'ACTIVE'
    ),
    (
        '00000000-0000-0000-0000-000000001690',
        'artifact-90d-v1', 90, 30, 24, 24, 72, 365, 2557, 'ACTIVE'
    );

CREATE FUNCTION vela_reject_retention_policy_revision_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Retention Policy revisions are immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_retention_policy_revision_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_retention_policy_revision_mutation() OWNER TO vela_retention_owner;

CREATE TRIGGER retention_policy_revisions_immutable
BEFORE UPDATE OR DELETE ON retention_policy_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_retention_policy_revision_mutation();

CREATE TABLE project_retention_policy_events (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    previous_policy_revision_id uuid NOT NULL,
    selected_policy_revision_id uuid NOT NULL,
    actor_kind principal_kind NOT NULL CHECK (actor_kind = 'HUMAN'),
    actor_principal_id uuid NOT NULL,
    actor_session_id uuid NOT NULL,
    occurred_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id),
    FOREIGN KEY (organization_id, actor_principal_id)
        REFERENCES principals(organization_id, id),
    FOREIGN KEY (previous_policy_revision_id) REFERENCES retention_policy_revisions(id),
    FOREIGN KEY (selected_policy_revision_id) REFERENCES retention_policy_revisions(id),
    CHECK (previous_policy_revision_id <> selected_policy_revision_id)
);
ALTER TABLE project_retention_policy_events OWNER TO vela_retention_owner;
REVOKE ALL ON TABLE project_retention_policy_events FROM PUBLIC;
ALTER TABLE project_retention_policy_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_retention_policy_events FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_reject_project_retention_policy_event_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Project Retention Policy events are immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_project_retention_policy_event_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_project_retention_policy_event_mutation()
    OWNER TO vela_retention_owner;

CREATE TRIGGER project_retention_policy_events_immutable
BEFORE UPDATE OR DELETE ON project_retention_policy_events
FOR EACH ROW EXECUTE FUNCTION vela_reject_project_retention_policy_event_mutation();

ALTER TABLE projects
    ADD COLUMN retention_policy_revision_id uuid NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000001630'::uuid,
    ADD COLUMN retention_artifact_days integer NOT NULL DEFAULT 30
        CHECK (retention_artifact_days IN (7, 30, 90)),
    ADD COLUMN retention_request_content_days integer NOT NULL DEFAULT 30
        CHECK (retention_request_content_days = 30),
    ADD COLUMN retention_incomplete_content_hours integer NOT NULL DEFAULT 24
        CHECK (retention_incomplete_content_hours = 24),
    ADD COLUMN retention_scratch_hours integer NOT NULL DEFAULT 24
        CHECK (retention_scratch_hours = 24),
    ADD COLUMN retention_debug_hours integer NOT NULL DEFAULT 72
        CHECK (retention_debug_hours = 72),
    ADD COLUMN retention_metadata_days integer NOT NULL DEFAULT 365
        CHECK (retention_metadata_days = 365),
    ADD COLUMN retention_financial_days integer NOT NULL DEFAULT 2557
        CHECK (retention_financial_days = 2557),
    ADD CONSTRAINT projects_retention_policy_revision
        FOREIGN KEY (retention_policy_revision_id) REFERENCES retention_policy_revisions(id);

ALTER TABLE jobs
    ADD COLUMN retention_policy_revision_id uuid NOT NULL
        DEFAULT '00000000-0000-0000-0000-000000001630'::uuid,
    ADD COLUMN retention_artifact_days integer NOT NULL DEFAULT 30
        CHECK (retention_artifact_days IN (7, 30, 90)),
    ADD COLUMN retention_request_content_days integer NOT NULL DEFAULT 30
        CHECK (retention_request_content_days = 30),
    ADD COLUMN retention_incomplete_content_hours integer NOT NULL DEFAULT 24
        CHECK (retention_incomplete_content_hours = 24),
    ADD COLUMN retention_scratch_hours integer NOT NULL DEFAULT 24
        CHECK (retention_scratch_hours = 24),
    ADD COLUMN retention_debug_hours integer NOT NULL DEFAULT 72
        CHECK (retention_debug_hours = 72),
    ADD COLUMN retention_metadata_days integer NOT NULL DEFAULT 365
        CHECK (retention_metadata_days = 365),
    ADD COLUMN retention_financial_days integer NOT NULL DEFAULT 2557
        CHECK (retention_financial_days = 2557),
    ADD COLUMN request_content_deleted_at timestamptz,
    ADD CONSTRAINT jobs_retention_policy_revision
        FOREIGN KEY (retention_policy_revision_id) REFERENCES retention_policy_revisions(id),
    ADD CONSTRAINT jobs_request_content_outlives_job CHECK (
        request_content_expires_at > job_expires_at
    ),
    ADD CONSTRAINT jobs_request_content_tombstone CHECK (
        (request_content_deleted_at IS NULL AND request_content <> '{"deleted":true}'::jsonb)
        OR
        (
            request_content_deleted_at IS NOT NULL
            AND request_content = '{"deleted":true}'::jsonb
            AND request_content_deleted_at >= created_at
        )
    );

CREATE OR REPLACE FUNCTION vela_reject_job_snapshot_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
DECLARE
    v_content_tombstoned boolean;
BEGIN
    v_content_tombstoned :=
        current_user = 'vela_retention_owner'
        AND OLD.request_content_deleted_at IS NULL
        AND NEW.request_content_deleted_at IS NOT NULL
        AND NEW.request_content = '{"deleted":true}'::jsonb;

    IF NEW.organization_id IS DISTINCT FROM OLD.organization_id
        OR NEW.project_id IS DISTINCT FROM OLD.project_id
        OR NEW.created_by_principal_id IS DISTINCT FROM OLD.created_by_principal_id
        OR NEW.model_revision_id IS DISTINCT FROM OLD.model_revision_id
        OR NEW.generation_preset_revision_id IS DISTINCT FROM OLD.generation_preset_revision_id
        OR NEW.service_class_revision_id IS DISTINCT FROM OLD.service_class_revision_id
        OR NEW.output_spec_id IS DISTINCT FROM OLD.output_spec_id
        OR NEW.worker_pool_id IS DISTINCT FROM OLD.worker_pool_id
        OR NEW.request_hash IS DISTINCT FROM OLD.request_hash
        OR (NEW.request_content IS DISTINCT FROM OLD.request_content AND NOT v_content_tombstoned)
        OR NEW.request_content_expires_at IS DISTINCT FROM OLD.request_content_expires_at
        OR (
            NEW.request_content_deleted_at IS DISTINCT FROM OLD.request_content_deleted_at
            AND NOT v_content_tombstoned
        )
        OR NEW.retention_policy_revision_id IS DISTINCT FROM OLD.retention_policy_revision_id
        OR NEW.retention_artifact_days IS DISTINCT FROM OLD.retention_artifact_days
        OR NEW.retention_request_content_days IS DISTINCT FROM OLD.retention_request_content_days
        OR NEW.retention_incomplete_content_hours
            IS DISTINCT FROM OLD.retention_incomplete_content_hours
        OR NEW.retention_scratch_hours IS DISTINCT FROM OLD.retention_scratch_hours
        OR NEW.retention_debug_hours IS DISTINCT FROM OLD.retention_debug_hours
        OR NEW.retention_metadata_days IS DISTINCT FROM OLD.retention_metadata_days
        OR NEW.retention_financial_days IS DISTINCT FROM OLD.retention_financial_days
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

CREATE TABLE content_deletion_requests (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    source content_deletion_source NOT NULL,
    idempotency_key text,
    request_hash bytea,
    actor_kind principal_kind,
    actor_principal_id uuid,
    actor_credential_id uuid,
    state content_deletion_state NOT NULL DEFAULT 'PENDING',
    requested_at timestamptz NOT NULL,
    deadline_at timestamptz NOT NULL,
    completed_at timestamptz,
    next_retry_at timestamptz,
    last_error_code text CHECK (
        last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 100
    ),
    last_error_message text CHECK (
        last_error_message IS NULL OR length(last_error_message) BETWEEN 1 AND 500
    ),
    claim_id uuid,
    claim_owner_id text,
    claim_expires_at timestamptz,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, job_id, id),
    UNIQUE (organization_id, project_id, job_id, source),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (organization_id, actor_principal_id)
        REFERENCES principals(organization_id, id),
    CHECK (deadline_at = requested_at + interval '24 hours'),
    CHECK (
        (
            source = 'CUSTOMER'
            AND length(idempotency_key) BETWEEN 1 AND 200
            AND octet_length(request_hash) = 32
            AND actor_kind IS NOT NULL
            AND actor_principal_id IS NOT NULL
            AND actor_credential_id IS NOT NULL
        )
        OR (
            source <> 'CUSTOMER'
            AND idempotency_key IS NULL
            AND request_hash IS NULL
            AND actor_kind IS NULL
            AND actor_principal_id IS NULL
            AND actor_credential_id IS NULL
        )
    ),
    CHECK (
        (state = 'COMPLETED' AND completed_at IS NOT NULL)
        OR (state <> 'COMPLETED' AND completed_at IS NULL)
    ),
    CHECK (
        (claim_id IS NULL AND claim_owner_id IS NULL AND claim_expires_at IS NULL)
        OR (
            claim_id IS NOT NULL
            AND length(claim_owner_id) BETWEEN 1 AND 500
            AND claim_expires_at IS NOT NULL
        )
    ),
    CHECK (
        (last_error_code IS NULL AND last_error_message IS NULL)
        OR (last_error_code IS NOT NULL AND last_error_message IS NOT NULL)
    )
);
ALTER TABLE content_deletion_requests OWNER TO vela_retention_owner;
REVOKE ALL ON TABLE content_deletion_requests FROM PUBLIC;
ALTER TABLE content_deletion_requests ENABLE ROW LEVEL SECURITY;
ALTER TABLE content_deletion_requests FORCE ROW LEVEL SECURITY;
CREATE UNIQUE INDEX content_deletion_requests_customer_idempotency_idx
    ON content_deletion_requests (organization_id, project_id, idempotency_key)
    WHERE source = 'CUSTOMER';

CREATE TABLE content_deletion_targets (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    request_id uuid NOT NULL,
    action content_deletion_target_action NOT NULL,
    artifact_id uuid,
    object_key text NOT NULL CHECK (
        length(object_key) BETWEEN 1 AND 1000
        AND object_key !~ '(^|/)\.\.(/|$)'
    ),
    object_version_id text CHECK (
        object_version_id IS NULL OR length(object_version_id) BETWEEN 1 AND 1000
    ),
    discovered_object_version_id text CHECK (
        discovered_object_version_id IS NULL
        OR length(discovered_object_version_id) BETWEEN 1 AND 1000
    ),
    state content_deletion_target_state NOT NULL DEFAULT 'PENDING',
    attempt_count integer NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    next_retry_at timestamptz,
    last_error_code text CHECK (
        last_error_code IS NULL OR length(last_error_code) BETWEEN 1 AND 100
    ),
    last_error_message text CHECK (
        last_error_message IS NULL OR length(last_error_message) BETWEEN 1 AND 500
    ),
    storage_outcome text CHECK (
        storage_outcome IS NULL
        OR storage_outcome IN (
            'DELETED', 'ALREADY_ABSENT', 'MULTIPART_ABORTED', 'NO_INCOMPLETE_UPLOADS'
        )
    ),
    completed_at timestamptz,
    claim_id uuid,
    claim_owner_id text,
    claim_expires_at timestamptz,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (request_id, id),
    UNIQUE (organization_id, project_id, job_id, request_id, id),
    FOREIGN KEY (organization_id, project_id, job_id, request_id)
        REFERENCES content_deletion_requests(organization_id, project_id, job_id, id),
    FOREIGN KEY (organization_id, project_id, artifact_id)
        REFERENCES artifacts(organization_id, project_id, id),
    CHECK (
        (
            action = 'OBJECT_VERSION'
            AND artifact_id IS NOT NULL
            AND object_version_id IS NOT NULL
            AND discovered_object_version_id IS NULL
        )
        OR (
            action = 'OBJECT_DISCOVERY'
            AND artifact_id IS NOT NULL
            AND object_version_id IS NULL
        )
        OR (
            action = 'MULTIPART_PREFIX'
            AND artifact_id IS NULL
            AND object_version_id IS NULL
            AND discovered_object_version_id IS NULL
            AND right(object_key, 1) = '/'
        )
    ),
    CHECK (
        (state = 'COMPLETED' AND completed_at IS NOT NULL AND storage_outcome IS NOT NULL)
        OR (state <> 'COMPLETED' AND completed_at IS NULL AND storage_outcome IS NULL)
    ),
    CHECK (
        (claim_id IS NULL AND claim_owner_id IS NULL AND claim_expires_at IS NULL)
        OR (
            claim_id IS NOT NULL
            AND length(claim_owner_id) BETWEEN 1 AND 500
            AND claim_expires_at IS NOT NULL
        )
    ),
    CHECK (
        (last_error_code IS NULL AND last_error_message IS NULL)
        OR (last_error_code IS NOT NULL AND last_error_message IS NOT NULL)
    )
);
ALTER TABLE content_deletion_targets OWNER TO vela_retention_owner;
REVOKE ALL ON TABLE content_deletion_targets FROM PUBLIC;
ALTER TABLE content_deletion_targets ENABLE ROW LEVEL SECURITY;
ALTER TABLE content_deletion_targets FORCE ROW LEVEL SECURITY;
CREATE UNIQUE INDEX content_deletion_targets_artifact_idx
    ON content_deletion_targets (request_id, artifact_id)
    WHERE artifact_id IS NOT NULL;
CREATE UNIQUE INDEX content_deletion_targets_multipart_idx
    ON content_deletion_targets (request_id)
    WHERE action = 'MULTIPART_PREFIX';

CREATE TABLE content_deletion_receipts (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    request_id uuid NOT NULL UNIQUE,
    target_count integer NOT NULL CHECK (target_count >= 0),
    completed_target_count integer NOT NULL CHECK (
        completed_target_count = target_count
    ),
    total_attempt_count bigint NOT NULL CHECK (total_attempt_count >= target_count),
    completed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, job_id, request_id, id),
    FOREIGN KEY (organization_id, project_id, job_id, request_id)
        REFERENCES content_deletion_requests(organization_id, project_id, job_id, id)
);
ALTER TABLE content_deletion_receipts OWNER TO vela_retention_owner;
REVOKE ALL ON TABLE content_deletion_receipts FROM PUBLIC;
ALTER TABLE content_deletion_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE content_deletion_receipts FORCE ROW LEVEL SECURITY;

CREATE TABLE content_deletion_receipt_targets (
    receipt_id uuid NOT NULL,
    target_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    request_id uuid NOT NULL,
    action content_deletion_target_action NOT NULL,
    attempt_count integer NOT NULL CHECK (attempt_count > 0),
    target_completed_at timestamptz NOT NULL,
    storage_outcome text NOT NULL CHECK (
        storage_outcome IN (
            'DELETED', 'ALREADY_ABSENT', 'MULTIPART_ABORTED', 'NO_INCOMPLETE_UPLOADS'
        )
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (receipt_id, target_id),
    FOREIGN KEY (organization_id, project_id, job_id, request_id, receipt_id)
        REFERENCES content_deletion_receipts(
            organization_id, project_id, job_id, request_id, id
        ),
    FOREIGN KEY (organization_id, project_id, job_id, request_id, target_id)
        REFERENCES content_deletion_targets(
            organization_id, project_id, job_id, request_id, id
        ),
    CHECK (target_completed_at <= created_at)
);
ALTER TABLE content_deletion_receipt_targets OWNER TO vela_retention_owner;
REVOKE ALL ON TABLE content_deletion_receipt_targets FROM PUBLIC;
ALTER TABLE content_deletion_receipt_targets ENABLE ROW LEVEL SECURITY;
ALTER TABLE content_deletion_receipt_targets FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_reject_content_deletion_receipt_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Content Deletion receipts are immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_content_deletion_receipt_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_content_deletion_receipt_mutation() OWNER TO vela_retention_owner;
CREATE TRIGGER content_deletion_receipts_immutable
BEFORE UPDATE OR DELETE ON content_deletion_receipts
FOR EACH ROW EXECUTE FUNCTION vela_reject_content_deletion_receipt_mutation();
CREATE TRIGGER content_deletion_receipt_targets_immutable
BEFORE UPDATE OR DELETE ON content_deletion_receipt_targets
FOR EACH ROW EXECUTE FUNCTION vela_reject_content_deletion_receipt_mutation();

DO $$
DECLARE
    v_object_constraint text;
    v_verification_constraint text;
    v_retention_constraint text;
BEGIN
    SELECT constraint_entry.conname INTO STRICT v_object_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.artifacts'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%object_version_id%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%uploaded_at%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) NOT LIKE '%verification_id%';
    SELECT constraint_entry.conname INTO STRICT v_verification_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.artifacts'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%verification_id%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%validation_receipt%';
    SELECT constraint_entry.conname INTO STRICT v_retention_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.artifacts'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%retention_expires_at%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%verified_at%';
    EXECUTE format('ALTER TABLE public.artifacts DROP CONSTRAINT %I', v_object_constraint);
    EXECUTE format('ALTER TABLE public.artifacts DROP CONSTRAINT %I', v_verification_constraint);
    EXECUTE format('ALTER TABLE public.artifacts DROP CONSTRAINT %I', v_retention_constraint);
END
$$;
ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_content_lifecycle CHECK (
        (
            state = 'STAGING'
            AND object_version_id IS NULL
            AND size_bytes IS NULL
            AND sha256 IS NULL
            AND content_type IS NULL
            AND uploaded_at IS NULL
        ) OR (
            state IN ('UPLOADED', 'VERIFIED', 'COMMITTED')
            AND length(object_version_id) BETWEEN 1 AND 1000
            AND size_bytes IS NOT NULL
            AND sha256 IS NOT NULL
            AND content_type IS NOT NULL
            AND uploaded_at IS NOT NULL
        ) OR (
            state IN ('EXPIRED', 'DELETED')
            AND (
                (
                    object_version_id IS NULL
                    AND size_bytes IS NULL
                    AND sha256 IS NULL
                    AND content_type IS NULL
                    AND uploaded_at IS NULL
                ) OR (
                    length(object_version_id) BETWEEN 1 AND 1000
                    AND size_bytes IS NOT NULL
                    AND sha256 IS NOT NULL
                    AND content_type IS NOT NULL
                    AND uploaded_at IS NOT NULL
                )
            )
        )
    ),
    ADD CONSTRAINT artifacts_verification_lifecycle CHECK (
        (
            state IN ('STAGING', 'UPLOADED')
            AND verification_id IS NULL
            AND verification_request_hash IS NULL
            AND validation_receipt IS NULL
            AND verified_at IS NULL
        ) OR (
            state IN ('VERIFIED', 'COMMITTED')
            AND verification_id IS NOT NULL
            AND verification_request_hash IS NOT NULL
            AND validation_receipt IS NOT NULL
            AND verified_at IS NOT NULL
        ) OR (
            state IN ('EXPIRED', 'DELETED')
            AND (
                (
                    verification_id IS NULL
                    AND verification_request_hash IS NULL
                    AND validation_receipt IS NULL
                    AND verified_at IS NULL
                ) OR (
                    verification_id IS NOT NULL
                    AND verification_request_hash IS NOT NULL
                    AND validation_receipt IS NOT NULL
                    AND verified_at IS NOT NULL
                )
            )
        )
    ),
    ADD CONSTRAINT artifacts_retention_lifecycle CHECK (
        (state IN ('STAGING', 'UPLOADED', 'VERIFIED') AND retention_expires_at IS NULL)
        OR (
            state = 'COMMITTED'
            AND retention_expires_at IS NOT NULL
            AND retention_expires_at > verified_at
        )
        OR (
            state IN ('EXPIRED', 'DELETED')
            AND (
                retention_expires_at IS NULL
                OR (verified_at IS NOT NULL AND retention_expires_at > verified_at)
            )
        )
    );

DO $$
DECLARE
    v_object_constraint text;
    v_verification_constraint text;
BEGIN
    SELECT constraint_entry.conname INTO STRICT v_object_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.artifact_uploads'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%object_version_id%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%completion_request_hash%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) NOT LIKE '%verification_id%';
    SELECT constraint_entry.conname INTO STRICT v_verification_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.artifact_uploads'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%verification_id%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%validation_receipt%';
    EXECUTE format('ALTER TABLE public.artifact_uploads DROP CONSTRAINT %I', v_object_constraint);
    EXECUTE format(
        'ALTER TABLE public.artifact_uploads DROP CONSTRAINT %I',
        v_verification_constraint
    );
END
$$;
ALTER TABLE artifact_uploads
    ADD CONSTRAINT artifact_uploads_content_lifecycle CHECK (
        (
            state = 'INITIATED'
            AND object_version_id IS NULL
            AND size_bytes IS NULL
            AND sha256 IS NULL
            AND content_type IS NULL
            AND completion_request_hash IS NULL
            AND uploaded_at IS NULL
        ) OR (
            state = 'UPLOADING'
            AND object_version_id IS NULL
            AND uploaded_at IS NULL
            AND (
                (
                    size_bytes IS NULL
                    AND sha256 IS NULL
                    AND content_type IS NULL
                    AND completion_request_hash IS NULL
                ) OR (
                    size_bytes IS NOT NULL
                    AND sha256 IS NOT NULL
                    AND content_type IS NOT NULL
                    AND completion_request_hash IS NOT NULL
                )
            )
        ) OR (
            state IN ('UPLOADED', 'VERIFIED')
            AND length(object_version_id) BETWEEN 1 AND 1000
            AND size_bytes IS NOT NULL
            AND sha256 IS NOT NULL
            AND content_type IS NOT NULL
            AND completion_request_hash IS NOT NULL
            AND uploaded_at IS NOT NULL
        ) OR (
            state IN ('ABORTED', 'EXPIRED')
            AND (
                (
                    object_version_id IS NULL
                    AND size_bytes IS NULL
                    AND sha256 IS NULL
                    AND content_type IS NULL
                    AND completion_request_hash IS NULL
                    AND uploaded_at IS NULL
                ) OR (
                    length(object_version_id) BETWEEN 1 AND 1000
                    AND size_bytes IS NOT NULL
                    AND sha256 IS NOT NULL
                    AND content_type IS NOT NULL
                    AND completion_request_hash IS NOT NULL
                    AND uploaded_at IS NOT NULL
                )
            )
        )
    ),
    ADD CONSTRAINT artifact_uploads_verification_lifecycle CHECK (
        (
            state IN ('INITIATED', 'UPLOADING', 'UPLOADED')
            AND verification_id IS NULL
            AND verification_request_hash IS NULL
            AND validation_receipt IS NULL
            AND verified_at IS NULL
        ) OR (
            state = 'VERIFIED'
            AND verification_id IS NOT NULL
            AND verification_request_hash IS NOT NULL
            AND validation_receipt IS NOT NULL
            AND verified_at IS NOT NULL
        ) OR (
            state IN ('ABORTED', 'EXPIRED')
            AND (
                (
                    verification_id IS NULL
                    AND verification_request_hash IS NULL
                    AND validation_receipt IS NULL
                    AND verified_at IS NULL
                ) OR (
                    verification_id IS NOT NULL
                    AND verification_request_hash IS NOT NULL
                    AND validation_receipt IS NOT NULL
                    AND verified_at IS NOT NULL
                )
            )
        )
    );

CREATE FUNCTION vela_current_request_credential_id() RETURNS uuid
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, vela_private
AS $$
    SELECT context.credential_id
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id_if_assigned()
$$;
REVOKE ALL ON FUNCTION vela_current_request_credential_id() FROM PUBLIC;
ALTER FUNCTION vela_current_request_credential_id() OWNER TO vela_internal;

CREATE FUNCTION vela_current_request_actor_kind() RETURNS principal_kind
LANGUAGE sql STABLE
SECURITY DEFINER
SET search_path = pg_catalog, vela_private
AS $$
    SELECT context.actor_kind
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id_if_assigned()
$$;
REVOKE ALL ON FUNCTION vela_current_request_actor_kind() FROM PUBLIC;
ALTER FUNCTION vela_current_request_actor_kind() OWNER TO vela_internal;

CREATE FUNCTION vela_get_project_retention_policy(
    p_project_id uuid
) RETURNS TABLE (
    project_id uuid,
    policy_revision_id uuid,
    stable_id text,
    artifact_retention_days integer,
    request_content_retention_days integer,
    incomplete_content_retention_hours integer,
    scratch_retention_hours integer,
    debug_retention_hours integer,
    metadata_retention_days integer,
    financial_retention_days integer,
    selected_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        project.id,
        policy.id,
        policy.stable_id,
        policy.artifact_retention_days,
        policy.request_content_retention_days,
        policy.incomplete_content_retention_hours,
        policy.scratch_retention_hours,
        policy.debug_retention_hours,
        policy.metadata_retention_days,
        policy.financial_retention_days,
        coalesce(latest.occurred_at, project.created_at)
    FROM public.projects AS project
    JOIN public.retention_policy_revisions AS policy
      ON policy.id = project.retention_policy_revision_id
    LEFT JOIN LATERAL (
        SELECT event.occurred_at
        FROM public.project_retention_policy_events AS event
        WHERE event.organization_id = project.organization_id
          AND event.project_id = project.id
        ORDER BY event.occurred_at DESC, event.id DESC
        LIMIT 1
    ) AS latest ON true
    WHERE public.vela_current_request_scope() = 'retention_policy:manage'
      AND project.organization_id = public.vela_current_organization_id()
      AND project.id = public.vela_current_project_id()
      AND project.id = p_project_id
$$;
REVOKE ALL ON FUNCTION vela_get_project_retention_policy(uuid) FROM PUBLIC;
ALTER FUNCTION vela_get_project_retention_policy(uuid) OWNER TO vela_retention_owner;

CREATE FUNCTION vela_set_project_retention_policy(
    p_project_id uuid,
    p_artifact_retention_days integer,
    p_event_id uuid
) RETURNS TABLE (
    project_id uuid,
    policy_revision_id uuid,
    stable_id text,
    artifact_retention_days integer,
    request_content_retention_days integer,
    incomplete_content_retention_hours integer,
    scratch_retention_hours integer,
    debug_retention_hours integer,
    metadata_retention_days integer,
    financial_retention_days integer,
    selected_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_project_context_id uuid := public.vela_current_project_id();
    v_principal_id uuid := public.vela_current_principal_id();
    v_credential_id uuid := public.vela_current_request_credential_id();
    v_scope text := public.vela_current_request_scope();
    v_previous_policy_revision_id uuid;
    v_project_created_at timestamptz;
    v_policy public.retention_policy_revisions%ROWTYPE;
    v_actor_kind public.principal_kind := public.vela_current_request_actor_kind();
    v_occurred_at timestamptz := transaction_timestamp();
    v_changed boolean;
BEGIN
    IF v_scope IS DISTINCT FROM 'retention_policy:manage'
        OR v_organization_id IS NULL
        OR v_project_context_id IS DISTINCT FROM p_project_id
        OR v_principal_id IS NULL
        OR v_credential_id IS NULL
    THEN
        RAISE EXCEPTION 'valid Retention Policy request context is required'
            USING ERRCODE = '28000';
    END IF;
    IF p_artifact_retention_days NOT IN (7, 30, 90) OR p_event_id IS NULL THEN
        RAISE EXCEPTION 'invalid Retention Policy selection' USING ERRCODE = '22023';
    END IF;

    SELECT project.retention_policy_revision_id, project.created_at
    INTO v_previous_policy_revision_id, v_project_created_at
    FROM public.projects AS project
    WHERE project.organization_id = v_organization_id
      AND project.id = p_project_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN;
    END IF;

    SELECT policy.* INTO STRICT v_policy
    FROM public.retention_policy_revisions AS policy
    WHERE policy.artifact_retention_days = p_artifact_retention_days
      AND policy.state = 'ACTIVE';

    IF v_actor_kind IS DISTINCT FROM 'HUMAN'::public.principal_kind THEN
        RAISE EXCEPTION 'Human ProjectAdmin authorization is required'
            USING ERRCODE = '42501';
    END IF;

    v_changed := v_previous_policy_revision_id <> v_policy.id;
    IF v_changed THEN
        UPDATE public.projects AS project
        SET retention_policy_revision_id = v_policy.id,
            retention_artifact_days = v_policy.artifact_retention_days,
            retention_request_content_days = v_policy.request_content_retention_days,
            retention_incomplete_content_hours = v_policy.incomplete_content_retention_hours,
            retention_scratch_hours = v_policy.scratch_retention_hours,
            retention_debug_hours = v_policy.debug_retention_hours,
            retention_metadata_days = v_policy.metadata_retention_days,
            retention_financial_days = v_policy.financial_retention_days
        WHERE project.organization_id = v_organization_id
          AND project.id = p_project_id;
        INSERT INTO public.project_retention_policy_events (
            id,
            organization_id,
            project_id,
            previous_policy_revision_id,
            selected_policy_revision_id,
            actor_kind,
            actor_principal_id,
            actor_session_id,
            occurred_at
        ) VALUES (
            p_event_id,
            v_organization_id,
            p_project_id,
            v_previous_policy_revision_id,
            v_policy.id,
            v_actor_kind,
            v_principal_id,
            v_credential_id,
            v_occurred_at
        );
    ELSE
        SELECT coalesce(max(event.occurred_at), v_project_created_at)
        INTO v_occurred_at
        FROM public.project_retention_policy_events AS event
        WHERE event.organization_id = v_organization_id
          AND event.project_id = p_project_id;
    END IF;

    RETURN QUERY SELECT
        p_project_id,
        v_policy.id,
        v_policy.stable_id,
        v_policy.artifact_retention_days,
        v_policy.request_content_retention_days,
        v_policy.incomplete_content_retention_hours,
        v_policy.scratch_retention_hours,
        v_policy.debug_retention_hours,
        v_policy.metadata_retention_days,
        v_policy.financial_retention_days,
        v_occurred_at;
END
$$;
REVOKE ALL ON FUNCTION vela_set_project_retention_policy(uuid, integer, uuid) FROM PUBLIC;
ALTER FUNCTION vela_set_project_retention_policy(uuid, integer, uuid)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_private.vela_cancel_job_for_content_deletion(
    p_job_id uuid,
    p_cancellation_id uuid,
    p_charge_id uuid,
    p_cancel_requested_event_id uuid,
    p_canceling_event_id uuid,
    p_canceled_event_id uuid,
    p_charge_posted_event_id uuid,
    p_invoice_export_event_id uuid
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, vela_private, public
AS $$
DECLARE
    v_transaction_id xid8 := pg_catalog.pg_current_xact_id_if_assigned();
    v_rows bigint;
BEGIN
    IF v_transaction_id IS NULL OR NOT EXISTS (
        SELECT 1
        FROM vela_private.request_contexts AS context
        WHERE context.backend_pid = pg_catalog.pg_backend_pid()
          AND context.transaction_id = v_transaction_id
          AND context.required_scope = 'content_deletion:manage'
          AND context.organization_id IS NOT NULL
          AND context.project_id IS NOT NULL
          AND context.principal_id IS NOT NULL
    ) THEN
        RAISE EXCEPTION 'valid Content Deletion request context is required'
            USING ERRCODE = '28000';
    END IF;

    UPDATE vela_private.request_contexts AS context
    SET required_scope = 'jobs:cancel'
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = v_transaction_id
      AND context.required_scope = 'content_deletion:manage';
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'Content Deletion cancellation context changed'
            USING ERRCODE = '40001';
    END IF;

    PERFORM *
    FROM public.vela_cancel_job(
        p_job_id,
        p_cancellation_id,
        p_charge_id,
        p_cancel_requested_event_id,
        p_canceling_event_id,
        p_canceled_event_id,
        p_charge_posted_event_id,
        p_invoice_export_event_id
    );

    UPDATE vela_private.request_contexts AS context
    SET required_scope = 'content_deletion:manage'
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = v_transaction_id
      AND context.required_scope = 'jobs:cancel';
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION 'Content Deletion request context was not restored'
            USING ERRCODE = '40001';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.vela_cancel_job_for_content_deletion(
    uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) FROM PUBLIC;
ALTER FUNCTION vela_private.vela_cancel_job_for_content_deletion(
    uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) OWNER TO vela_internal;

CREATE FUNCTION vela_get_content_deletion_request(
    p_project_id uuid,
    p_request_id uuid
) RETURNS TABLE (
    request_id uuid,
    project_id uuid,
    job_id uuid,
    request_state content_deletion_state,
    requested_at timestamptz,
    deadline_at timestamptz,
    completed_at timestamptz,
    overdue boolean,
    target_count bigint,
    completed_target_count bigint,
    retrying_target_count bigint,
    last_error_code text,
    last_error_message text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        request.id,
        request.project_id,
        request.job_id,
        request.state,
        request.requested_at,
        request.deadline_at,
        request.completed_at,
        request.state <> 'COMPLETED'
            AND request.deadline_at < clock_timestamp(),
        count(target.id),
        count(target.id) FILTER (WHERE target.state = 'COMPLETED'),
        count(target.id) FILTER (WHERE target.state = 'RETRY_WAIT'),
        request.last_error_code,
        request.last_error_message
    FROM public.content_deletion_requests AS request
    LEFT JOIN public.content_deletion_targets AS target
      ON target.request_id = request.id
    WHERE public.vela_current_request_scope() = 'content_deletion:manage'
      AND request.organization_id = public.vela_current_organization_id()
      AND request.project_id = public.vela_current_project_id()
      AND request.project_id = p_project_id
      AND request.id = p_request_id
    GROUP BY request.id
$$;
REVOKE ALL ON FUNCTION vela_get_content_deletion_request(uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_get_content_deletion_request(uuid, uuid) OWNER TO vela_retention_owner;

CREATE FUNCTION vela_accept_content_deletion_request(
    p_request_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_idempotency_key text,
    p_request_hash bytea,
    p_cancellation_id uuid,
    p_charge_id uuid,
    p_cancel_requested_event_id uuid,
    p_canceling_event_id uuid,
    p_canceled_event_id uuid,
    p_charge_posted_event_id uuid,
    p_invoice_export_event_id uuid
) RETURNS TABLE (
    request_id uuid,
    project_id uuid,
    job_id uuid,
    request_state content_deletion_state,
    requested_at timestamptz,
    deadline_at timestamptz,
    completed_at timestamptz,
    overdue boolean,
    target_count bigint,
    completed_target_count bigint,
    retrying_target_count bigint,
    last_error_code text,
    last_error_message text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_context_project_id uuid := public.vela_current_project_id();
    v_principal_id uuid := public.vela_current_principal_id();
    v_credential_id uuid := public.vela_current_request_credential_id();
    v_actor_kind public.principal_kind := public.vela_current_request_actor_kind();
    v_scope text := public.vela_current_request_scope();
    v_existing public.content_deletion_requests%ROWTYPE;
    v_requested_at timestamptz := transaction_timestamp();
    v_created boolean := false;
BEGIN
    IF v_scope IS DISTINCT FROM 'content_deletion:manage'
        OR v_organization_id IS NULL
        OR v_context_project_id IS DISTINCT FROM p_project_id
        OR v_principal_id IS NULL
        OR v_credential_id IS NULL
        OR v_actor_kind NOT IN ('HUMAN', 'SERVICE')
    THEN
        RAISE EXCEPTION 'valid Content Deletion request context is required'
            USING ERRCODE = '28000';
    END IF;
    IF p_request_id IS NULL OR p_job_id IS NULL
        OR length(p_idempotency_key) NOT BETWEEN 1 AND 200
        OR octet_length(p_request_hash) IS DISTINCT FROM 32
        OR p_cancellation_id IS NULL OR p_charge_id IS NULL
        OR p_cancel_requested_event_id IS NULL OR p_canceling_event_id IS NULL
        OR p_canceled_event_id IS NULL OR p_charge_posted_event_id IS NULL
        OR p_invoice_export_event_id IS NULL
    THEN
        RAISE EXCEPTION 'invalid Content Deletion request' USING ERRCODE = '22023';
    END IF;

    SELECT request.* INTO v_existing
    FROM public.content_deletion_requests AS request
    WHERE request.organization_id = v_organization_id
      AND request.project_id = p_project_id
      AND request.source = 'CUSTOMER'
      AND request.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.job_id IS DISTINCT FROM p_job_id
            OR v_existing.request_hash IS DISTINCT FROM p_request_hash
        THEN
            RAISE EXCEPTION 'Idempotency-Key was already used for another Content Deletion target'
                USING ERRCODE = '23505',
                    CONSTRAINT = 'content_deletion_requests_customer_idempotency_idx';
        END IF;
        RETURN QUERY
        SELECT projection.*
        FROM public.vela_get_content_deletion_request(p_project_id, v_existing.id) AS projection;
        RETURN;
    END IF;

    PERFORM 1
    FROM public.jobs AS job
    WHERE job.organization_id = v_organization_id
      AND job.project_id = p_project_id
      AND job.id = p_job_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job is not visible in this Project' USING ERRCODE = 'P0002';
    END IF;

    PERFORM vela_private.vela_cancel_job_for_content_deletion(
        p_job_id,
        p_cancellation_id,
        p_charge_id,
        p_cancel_requested_event_id,
        p_canceling_event_id,
        p_canceled_event_id,
        p_charge_posted_event_id,
        p_invoice_export_event_id
    );

    BEGIN
        INSERT INTO public.content_deletion_requests (
            id,
            organization_id,
            project_id,
            job_id,
            source,
            idempotency_key,
            request_hash,
            actor_kind,
            actor_principal_id,
            actor_credential_id,
            requested_at,
            deadline_at
        ) VALUES (
            p_request_id,
            v_organization_id,
            p_project_id,
            p_job_id,
            'CUSTOMER',
            p_idempotency_key,
            p_request_hash,
            v_actor_kind,
            v_principal_id,
            v_credential_id,
            v_requested_at,
            v_requested_at + interval '24 hours'
        );
        v_created := true;
    EXCEPTION WHEN unique_violation THEN
        SELECT request.* INTO v_existing
        FROM public.content_deletion_requests AS request
        WHERE request.organization_id = v_organization_id
          AND request.project_id = p_project_id
          AND request.source = 'CUSTOMER'
          AND (
              request.idempotency_key = p_idempotency_key
              OR request.job_id = p_job_id
          );
        IF NOT FOUND OR v_existing.idempotency_key IS DISTINCT FROM p_idempotency_key
            OR v_existing.job_id IS DISTINCT FROM p_job_id
            OR v_existing.request_hash IS DISTINCT FROM p_request_hash
        THEN
            RAISE EXCEPTION 'Content Deletion idempotency conflict'
                USING ERRCODE = '23505',
                    CONSTRAINT = 'content_deletion_requests_customer_idempotency_idx';
        END IF;
    END;

    IF NOT v_created THEN
        RETURN QUERY
        SELECT projection.*
        FROM public.vela_get_content_deletion_request(p_project_id, v_existing.id) AS projection;
        RETURN;
    END IF;

    UPDATE public.jobs AS job
    SET request_content = '{"deleted":true}'::jsonb,
        request_content_deleted_at = v_requested_at,
        updated_at = v_requested_at
    WHERE job.organization_id = v_organization_id
      AND job.project_id = p_project_id
      AND job.id = p_job_id
      AND job.request_content_deleted_at IS NULL;

    UPDATE public.artifact_access_grants AS access_grant
    SET revoked_at = greatest(v_requested_at, access_grant.eligible_at)
    WHERE access_grant.organization_id = v_organization_id
      AND access_grant.project_id = p_project_id
      AND access_grant.job_id = p_job_id
      AND access_grant.revoked_at IS NULL;

    INSERT INTO public.content_deletion_targets (
        id,
        organization_id,
        project_id,
        job_id,
        request_id,
        action,
        artifact_id,
        object_key,
        object_version_id
    )
    SELECT
        gen_random_uuid(),
        artifact.organization_id,
        artifact.project_id,
        artifact.job_id,
        p_request_id,
        CASE
            WHEN artifact.object_version_id IS NULL
            THEN 'OBJECT_DISCOVERY'::public.content_deletion_target_action
            ELSE 'OBJECT_VERSION'::public.content_deletion_target_action
        END,
        artifact.id,
        artifact.object_key,
        artifact.object_version_id
    FROM public.artifacts AS artifact
    WHERE artifact.organization_id = v_organization_id
      AND artifact.project_id = p_project_id
      AND artifact.job_id = p_job_id;

    INSERT INTO public.content_deletion_targets (
        id,
        organization_id,
        project_id,
        job_id,
        request_id,
        action,
        object_key
    ) VALUES (
        gen_random_uuid(),
        v_organization_id,
        p_project_id,
        p_job_id,
        p_request_id,
        'MULTIPART_PREFIX',
        format('artifacts/%s/%s/%s/', v_organization_id, p_project_id, p_job_id)
    );

    RETURN QUERY
    SELECT projection.*
    FROM public.vela_get_content_deletion_request(p_project_id, p_request_id) AS projection;
END
$$;
REVOKE ALL ON FUNCTION vela_accept_content_deletion_request(
    uuid, uuid, uuid, text, bytea, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) FROM PUBLIC;
ALTER FUNCTION vela_accept_content_deletion_request(
    uuid, uuid, uuid, text, bytea, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) OWNER TO vela_retention_owner;

CREATE FUNCTION vela_claim_content_deletion_target(
    p_claim_owner_id text,
    p_claim_id uuid,
    p_claim_seconds integer
) RETURNS TABLE (
    target_id uuid,
    target_action content_deletion_target_action,
    object_key text,
    object_version_id text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_target_id uuid;
    v_claimed_at timestamptz := clock_timestamp();
BEGIN
    IF p_claim_id IS NULL OR p_claim_owner_id IS NULL
        OR length(p_claim_owner_id) NOT BETWEEN 1 AND 500
        OR p_claim_seconds NOT BETWEEN 1 AND 3600
    THEN
        RAISE EXCEPTION 'invalid Content Deletion claim' USING ERRCODE = '22023';
    END IF;

    SELECT target.id INTO v_target_id
    FROM public.content_deletion_targets AS target
    JOIN public.content_deletion_requests AS deletion
      ON deletion.id = target.request_id
    WHERE deletion.state <> 'COMPLETED'
      AND (deletion.next_retry_at IS NULL OR deletion.next_retry_at <= v_claimed_at)
      AND (
          deletion.claim_id IS NULL
          OR deletion.claim_expires_at IS NULL
          OR deletion.claim_expires_at <= v_claimed_at
      )
      AND target.state <> 'COMPLETED'
      AND (target.next_retry_at IS NULL OR target.next_retry_at <= v_claimed_at)
      AND (
          target.claim_id IS NULL
          OR target.claim_expires_at IS NULL
          OR target.claim_expires_at <= v_claimed_at
      )
    ORDER BY deletion.deadline_at, deletion.requested_at, target.created_at, target.id
    FOR UPDATE OF deletion, target SKIP LOCKED
    LIMIT 1;

    IF NOT FOUND THEN
        RETURN;
    END IF;

    UPDATE public.content_deletion_requests AS deletion
    SET state = 'IN_PROGRESS',
        next_retry_at = NULL,
        claim_id = p_claim_id,
        claim_owner_id = p_claim_owner_id,
        claim_expires_at = v_claimed_at + make_interval(secs => p_claim_seconds),
        version = deletion.version + 1,
        updated_at = v_claimed_at
    FROM public.content_deletion_targets AS target
    WHERE target.id = v_target_id
      AND deletion.id = target.request_id;

    UPDATE public.content_deletion_targets AS target
    SET state = 'IN_PROGRESS',
        attempt_count = target.attempt_count + 1,
        next_retry_at = NULL,
        claim_id = p_claim_id,
        claim_owner_id = p_claim_owner_id,
        claim_expires_at = v_claimed_at + make_interval(secs => p_claim_seconds),
        version = target.version + 1,
        updated_at = v_claimed_at
    WHERE target.id = v_target_id;

    RETURN QUERY
    SELECT
        target.id,
        target.action,
        target.object_key,
        target.object_version_id
    FROM public.content_deletion_targets AS target
    WHERE target.id = v_target_id;
END
$$;
REVOKE ALL ON FUNCTION vela_claim_content_deletion_target(text, uuid, integer) FROM PUBLIC;
ALTER FUNCTION vela_claim_content_deletion_target(text, uuid, integer)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_complete_content_deletion_target(
    p_target_id uuid,
    p_claim_id uuid,
    p_receipt_id uuid,
    p_discovered_object_version_id text,
    p_storage_outcome text
) RETURNS TABLE (marked boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_target public.content_deletion_targets%ROWTYPE;
    v_request public.content_deletion_requests%ROWTYPE;
    v_completed_at timestamptz := clock_timestamp();
    v_remaining integer;
BEGIN
    IF p_target_id IS NULL OR p_claim_id IS NULL OR p_receipt_id IS NULL
        OR p_storage_outcome IS NULL OR p_storage_outcome NOT IN (
            'DELETED', 'ALREADY_ABSENT', 'MULTIPART_ABORTED', 'NO_INCOMPLETE_UPLOADS'
        )
    THEN
        RAISE EXCEPTION 'invalid Content Deletion completion' USING ERRCODE = '22023';
    END IF;

    SELECT target.* INTO v_target
    FROM public.content_deletion_targets AS target
    WHERE target.id = p_target_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT false;
        RETURN;
    END IF;
    SELECT deletion.* INTO STRICT v_request
    FROM public.content_deletion_requests AS deletion
    WHERE deletion.id = v_target.request_id
    FOR UPDATE;

    IF v_target.state <> 'IN_PROGRESS'
        OR v_target.claim_id IS DISTINCT FROM p_claim_id
        OR v_target.claim_expires_at IS NULL
        OR v_target.claim_expires_at <= v_completed_at
        OR v_request.claim_id IS DISTINCT FROM p_claim_id
        OR v_request.claim_expires_at IS NULL
        OR v_request.claim_expires_at <= v_completed_at
    THEN
        RETURN QUERY SELECT false;
        RETURN;
    END IF;

    IF (
        v_target.action = 'OBJECT_VERSION'
        AND (
            p_discovered_object_version_id IS NOT NULL
            OR p_storage_outcome NOT IN ('DELETED', 'ALREADY_ABSENT')
        )
    ) OR (
        v_target.action = 'OBJECT_DISCOVERY'
        AND (
            p_storage_outcome NOT IN ('DELETED', 'ALREADY_ABSENT')
            OR (
                p_storage_outcome = 'DELETED'
                AND (
                    p_discovered_object_version_id IS NULL
                    OR length(p_discovered_object_version_id) NOT BETWEEN 1 AND 1000
                )
            )
            OR (
                p_storage_outcome = 'ALREADY_ABSENT'
                AND p_discovered_object_version_id IS NOT NULL
            )
        )
    ) OR (
        v_target.action = 'MULTIPART_PREFIX'
        AND (
            p_discovered_object_version_id IS NOT NULL
            OR p_storage_outcome NOT IN ('MULTIPART_ABORTED', 'NO_INCOMPLETE_UPLOADS')
        )
    ) THEN
        RAISE EXCEPTION 'Content Deletion outcome does not match target action'
            USING ERRCODE = '22023';
    END IF;

    UPDATE public.content_deletion_targets AS target
    SET discovered_object_version_id = CASE
            WHEN target.action = 'OBJECT_DISCOVERY'
            THEN p_discovered_object_version_id
            ELSE target.discovered_object_version_id
        END,
        state = 'COMPLETED',
        next_retry_at = NULL,
        last_error_code = NULL,
        last_error_message = NULL,
        storage_outcome = p_storage_outcome,
        completed_at = v_completed_at,
        claim_id = NULL,
        claim_owner_id = NULL,
        claim_expires_at = NULL,
        version = target.version + 1,
        updated_at = v_completed_at
    WHERE target.id = p_target_id;

    SELECT count(*)::integer INTO v_remaining
    FROM public.content_deletion_targets AS target
    WHERE target.request_id = v_request.id
      AND target.state <> 'COMPLETED';

    IF v_remaining > 0 THEN
        UPDATE public.content_deletion_requests AS deletion
        SET state = 'PENDING',
            next_retry_at = NULL,
            claim_id = NULL,
            claim_owner_id = NULL,
            claim_expires_at = NULL,
            version = deletion.version + 1,
            updated_at = v_completed_at
        WHERE deletion.id = v_request.id;
        RETURN QUERY SELECT true;
        RETURN;
    END IF;

    UPDATE public.artifact_uploads AS upload
    SET state = CASE
            WHEN upload.state IN ('UPLOADED', 'VERIFIED')
            THEN 'EXPIRED'::public.artifact_upload_state
            ELSE 'ABORTED'::public.artifact_upload_state
        END,
        next_retry_at = NULL,
        claim_id = NULL,
        claim_owner_kind = NULL,
        claim_owner_id = NULL,
        claim_expires_at = NULL,
        version = upload.version + 1,
        updated_at = v_completed_at
    WHERE upload.artifact_id IN (
        SELECT target.artifact_id
        FROM public.content_deletion_targets AS target
        WHERE target.request_id = v_request.id
          AND target.artifact_id IS NOT NULL
    )
      AND upload.state NOT IN ('ABORTED', 'EXPIRED');

    UPDATE public.artifacts AS artifact
    SET state = 'DELETED',
        updated_at = v_completed_at
    WHERE artifact.id IN (
        SELECT target.artifact_id
        FROM public.content_deletion_targets AS target
        WHERE target.request_id = v_request.id
          AND target.artifact_id IS NOT NULL
    )
      AND artifact.state <> 'DELETED';

    UPDATE public.content_deletion_requests AS deletion
    SET state = 'COMPLETED',
        completed_at = v_completed_at,
        next_retry_at = NULL,
        last_error_code = NULL,
        last_error_message = NULL,
        claim_id = NULL,
        claim_owner_id = NULL,
        claim_expires_at = NULL,
        version = deletion.version + 1,
        updated_at = v_completed_at
    WHERE deletion.id = v_request.id;

    INSERT INTO public.content_deletion_receipts (
        id,
        organization_id,
        project_id,
        job_id,
        request_id,
        target_count,
        completed_target_count,
        total_attempt_count,
        completed_at
    )
    SELECT
        p_receipt_id,
        v_request.organization_id,
        v_request.project_id,
        v_request.job_id,
        v_request.id,
        count(*)::integer,
        count(*) FILTER (WHERE target.state = 'COMPLETED')::integer,
        sum(target.attempt_count),
        v_completed_at
    FROM public.content_deletion_targets AS target
    WHERE target.request_id = v_request.id;

    INSERT INTO public.content_deletion_receipt_targets (
        receipt_id,
        target_id,
        organization_id,
        project_id,
        job_id,
        request_id,
        action,
        attempt_count,
        target_completed_at,
        storage_outcome,
        created_at
    )
    SELECT
        p_receipt_id,
        target.id,
        target.organization_id,
        target.project_id,
        target.job_id,
        target.request_id,
        target.action,
        target.attempt_count,
        target.completed_at,
        target.storage_outcome,
        v_completed_at
    FROM public.content_deletion_targets AS target
    WHERE target.request_id = v_request.id;

    RETURN QUERY SELECT true;
END
$$;
REVOKE ALL ON FUNCTION vela_complete_content_deletion_target(
    uuid, uuid, uuid, text, text
) FROM PUBLIC;
ALTER FUNCTION vela_complete_content_deletion_target(uuid, uuid, uuid, text, text)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_retry_content_deletion_target(
    p_target_id uuid,
    p_claim_id uuid,
    p_retry_seconds integer,
    p_error_code text
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_target public.content_deletion_targets%ROWTYPE;
    v_retry_at timestamptz := clock_timestamp();
    v_error_message text;
BEGIN
    IF p_target_id IS NULL OR p_claim_id IS NULL
        OR p_retry_seconds NOT BETWEEN 1 AND 86400
        OR p_error_code IS NULL
        OR p_error_code NOT IN ('STORAGE_OPERATION_FAILED', 'STORAGE_IDENTITY_INVALID')
    THEN
        RAISE EXCEPTION 'invalid Content Deletion retry' USING ERRCODE = '22023';
    END IF;
    v_error_message := CASE p_error_code
        WHEN 'STORAGE_OPERATION_FAILED' THEN 'Artifact storage operation failed'
        WHEN 'STORAGE_IDENTITY_INVALID' THEN 'Artifact storage identity was invalid'
    END;

    SELECT target.* INTO v_target
    FROM public.content_deletion_targets AS target
    WHERE target.id = p_target_id
    FOR UPDATE;
    IF NOT FOUND OR v_target.state <> 'IN_PROGRESS'
        OR v_target.claim_id IS DISTINCT FROM p_claim_id
    THEN
        RETURN false;
    END IF;

    UPDATE public.content_deletion_targets AS target
    SET state = 'RETRY_WAIT',
        next_retry_at = v_retry_at + make_interval(secs => p_retry_seconds),
        last_error_code = p_error_code,
        last_error_message = v_error_message,
        claim_id = NULL,
        claim_owner_id = NULL,
        claim_expires_at = NULL,
        version = target.version + 1,
        updated_at = v_retry_at
    WHERE target.id = p_target_id;

    UPDATE public.content_deletion_requests AS deletion
    SET state = 'RETRY_WAIT',
        next_retry_at = v_retry_at + make_interval(secs => p_retry_seconds),
        last_error_code = p_error_code,
        last_error_message = v_error_message,
        claim_id = NULL,
        claim_owner_id = NULL,
        claim_expires_at = NULL,
        version = deletion.version + 1,
        updated_at = v_retry_at
    WHERE deletion.id = v_target.request_id
      AND deletion.claim_id = p_claim_id;

    RETURN true;
END
$$;
REVOKE ALL ON FUNCTION vela_retry_content_deletion_target(
    uuid, uuid, integer, text
) FROM PUBLIC;
ALTER FUNCTION vela_retry_content_deletion_target(uuid, uuid, integer, text)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_enqueue_expired_content_deletions(
    p_limit integer
) RETURNS TABLE (
    request_content_completed integer,
    artifact_requests_created integer
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_job record;
    v_artifact_set record;
    v_request_id uuid;
    v_request_content_completed integer := 0;
    v_artifact_requests_created integer := 0;
BEGIN
    IF p_limit NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION 'invalid retention enqueue limit' USING ERRCODE = '22023';
    END IF;

    FOR v_job IN
        SELECT job.id, job.organization_id, job.project_id
        FROM public.jobs AS job
		WHERE job.request_content_deleted_at IS NULL
          AND job.request_content_expires_at <= v_now
          AND NOT EXISTS (
              SELECT 1
              FROM public.content_deletion_requests AS existing
              WHERE existing.organization_id = job.organization_id
                AND existing.project_id = job.project_id
                AND existing.job_id = job.id
                AND existing.source = 'RETENTION_REQUEST_CONTENT'
          )
        ORDER BY job.request_content_expires_at, job.id
        FOR UPDATE OF job SKIP LOCKED
        LIMIT p_limit
    LOOP
        v_request_id := gen_random_uuid();
        INSERT INTO public.content_deletion_requests (
            id,
            organization_id,
            project_id,
            job_id,
            source,
            state,
            requested_at,
            deadline_at,
            completed_at
        ) VALUES (
            v_request_id,
            v_job.organization_id,
            v_job.project_id,
            v_job.id,
            'RETENTION_REQUEST_CONTENT',
            'COMPLETED',
            v_now,
            v_now + interval '24 hours',
            v_now
        );
        UPDATE public.jobs AS job
        SET request_content = '{"deleted":true}'::jsonb,
            request_content_deleted_at = v_now,
            updated_at = v_now
        WHERE job.id = v_job.id
          AND job.organization_id = v_job.organization_id
          AND job.project_id = v_job.project_id
          AND job.request_content_deleted_at IS NULL;
        INSERT INTO public.content_deletion_receipts (
            id,
            organization_id,
            project_id,
            job_id,
            request_id,
            target_count,
            completed_target_count,
            total_attempt_count,
            completed_at
        ) VALUES (
            gen_random_uuid(),
            v_job.organization_id,
            v_job.project_id,
            v_job.id,
            v_request_id,
            0,
            0,
            0,
            v_now
        );
        v_request_content_completed := v_request_content_completed + 1;
    END LOOP;

    FOR v_artifact_set IN
        SELECT artifact_set.id, artifact_set.organization_id,
            artifact_set.project_id, artifact_set.job_id
        FROM public.artifact_sets AS artifact_set
        JOIN public.jobs AS artifact_job
          ON artifact_job.organization_id = artifact_set.organization_id
         AND artifact_job.project_id = artifact_set.project_id
         AND artifact_job.id = artifact_set.job_id
        WHERE artifact_set.retention_expires_at <= v_now
          AND EXISTS (
              SELECT 1
              FROM public.artifacts AS artifact
              WHERE artifact.organization_id = artifact_set.organization_id
                AND artifact.project_id = artifact_set.project_id
                AND artifact.job_id = artifact_set.job_id
                AND artifact.state = 'COMMITTED'
          )
          AND NOT EXISTS (
              SELECT 1
              FROM public.content_deletion_requests AS existing
              WHERE existing.organization_id = artifact_set.organization_id
                AND existing.project_id = artifact_set.project_id
                AND existing.job_id = artifact_set.job_id
                AND existing.source = 'RETENTION_ARTIFACT'
          )
        ORDER BY artifact_set.retention_expires_at, artifact_set.id
        FOR UPDATE OF artifact_job SKIP LOCKED
        LIMIT p_limit
    LOOP
        v_request_id := gen_random_uuid();
        INSERT INTO public.content_deletion_requests (
            id,
            organization_id,
            project_id,
            job_id,
            source,
            requested_at,
            deadline_at
        ) VALUES (
            v_request_id,
            v_artifact_set.organization_id,
            v_artifact_set.project_id,
            v_artifact_set.job_id,
            'RETENTION_ARTIFACT',
            v_now,
            v_now + interval '24 hours'
        );
        UPDATE public.artifact_access_grants AS access_grant
        SET revoked_at = coalesce(access_grant.revoked_at, v_now)
        WHERE access_grant.organization_id = v_artifact_set.organization_id
          AND access_grant.project_id = v_artifact_set.project_id
          AND access_grant.job_id = v_artifact_set.job_id
          AND access_grant.revoked_at IS NULL;
        INSERT INTO public.content_deletion_targets (
            id,
            organization_id,
            project_id,
            job_id,
            request_id,
            action,
            artifact_id,
            object_key,
            object_version_id
        )
        SELECT
            gen_random_uuid(),
            artifact.organization_id,
            artifact.project_id,
            artifact.job_id,
            v_request_id,
            'OBJECT_VERSION',
            artifact.id,
            artifact.object_key,
            artifact.object_version_id
        FROM public.artifacts AS artifact
        WHERE artifact.organization_id = v_artifact_set.organization_id
          AND artifact.project_id = v_artifact_set.project_id
          AND artifact.job_id = v_artifact_set.job_id
          AND artifact.state = 'COMMITTED';
        INSERT INTO public.content_deletion_targets (
            id,
            organization_id,
            project_id,
            job_id,
            request_id,
            action,
            object_key
        ) VALUES (
            gen_random_uuid(),
            v_artifact_set.organization_id,
            v_artifact_set.project_id,
            v_artifact_set.job_id,
            v_request_id,
            'MULTIPART_PREFIX',
            format(
                'artifacts/%s/%s/%s/',
                v_artifact_set.organization_id,
                v_artifact_set.project_id,
                v_artifact_set.job_id
            )
        );
        v_artifact_requests_created := v_artifact_requests_created + 1;
    END LOOP;

    RETURN QUERY SELECT v_request_content_completed, v_artifact_requests_created;
END
$$;
REVOKE ALL ON FUNCTION vela_enqueue_expired_content_deletions(integer) FROM PUBLIC;
ALTER FUNCTION vela_enqueue_expired_content_deletions(integer)
    OWNER TO vela_retention_owner;

CREATE OR REPLACE FUNCTION vela_project_role_scopes(p_role project_role) RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE p_role
        WHEN 'ProjectAdmin'::public.project_role THEN
            ARRAY[
                'content_deletion:manage',
                'project_members:manage',
                'project_members:read',
                'retention_policy:manage',
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

CREATE OR REPLACE FUNCTION vela_service_credential_scopes_valid(
    p_scopes text[]
) RETURNS boolean
LANGUAGE sql IMMUTABLE
SET search_path = pg_catalog
AS $$
    SELECT p_scopes IS NOT NULL
       AND cardinality(p_scopes) BETWEEN 1 AND 7
       AND NOT EXISTS (
           SELECT 1
           FROM unnest(p_scopes) AS scope
           WHERE scope IS NULL OR scope NOT IN (
               'jobs:submit',
               'jobs:read',
               'jobs:cancel',
               'artifacts:read',
               'content_deletion:manage',
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

GRANT EXECUTE ON FUNCTION vela_set_request_context(uuid, bytea, text)
    TO vela_retention_request;
GRANT EXECUTE ON FUNCTION
    vela_current_organization_id(),
    vela_current_project_id(),
    vela_current_principal_id(),
    vela_current_request_scope(),
    vela_current_request_credential_id(),
    vela_current_request_actor_kind()
    TO vela_retention_owner;
GRANT SELECT (
    id, organization_id, retention_policy_revision_id,
    retention_artifact_days, retention_request_content_days,
    retention_incomplete_content_hours, retention_scratch_hours,
    retention_debug_hours, retention_metadata_days, retention_financial_days,
    created_at
)
    ON projects TO vela_retention_owner;
GRANT UPDATE (
    retention_policy_revision_id, retention_artifact_days,
    retention_request_content_days, retention_incomplete_content_hours,
    retention_scratch_hours, retention_debug_hours, retention_metadata_days,
    retention_financial_days
) ON projects TO vela_retention_owner;
GRANT SELECT (
    id, organization_id, project_id, state,
    request_content_expires_at, request_content_deleted_at
)
    ON jobs TO vela_retention_owner;
GRANT UPDATE (request_content, request_content_deleted_at, updated_at)
    ON jobs TO vela_retention_owner;
GRANT SELECT (
    id, organization_id, project_id, job_id, attempt_id, attempt_fence,
    object_key, object_version_id, size_bytes, sha256, content_type, state
)
    ON artifacts TO vela_retention_owner;
GRANT UPDATE (state, updated_at) ON artifacts TO vela_retention_owner;
GRANT SELECT (artifact_id, state, object_version_id, version)
    ON artifact_uploads TO vela_retention_owner;
GRANT UPDATE (
    state, next_retry_at, claim_id, claim_owner_kind, claim_owner_id,
    claim_expires_at, version, updated_at
) ON artifact_uploads TO vela_retention_owner;
GRANT SELECT (
    organization_id, project_id, job_id, artifact_set_id, eligible_at, revoked_at
)
    ON artifact_access_grants TO vela_retention_owner;
GRANT UPDATE (revoked_at) ON artifact_access_grants TO vela_retention_owner;
GRANT SELECT ON artifact_sets, artifact_set_items, visible_completions, charges, outbox_events
    TO vela_retention_owner;
GRANT EXECUTE ON FUNCTION
    vela_validate_artifact_mutation(),
    vela_validate_artifact_upload_mutation(),
    vela_validate_artifact_access_grant_mutation()
    TO vela_retention_owner;
GRANT USAGE ON SCHEMA vela_private TO vela_retention_owner;
GRANT EXECUTE ON FUNCTION vela_private.vela_cancel_job_for_content_deletion(
    uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) TO vela_retention_owner;
GRANT EXECUTE ON FUNCTION
    vela_get_project_retention_policy(uuid),
    vela_set_project_retention_policy(uuid, integer, uuid),
    vela_get_content_deletion_request(uuid, uuid),
    vela_accept_content_deletion_request(
        uuid, uuid, uuid, text, bytea, uuid, uuid, uuid, uuid, uuid, uuid, uuid
    )
    TO vela_retention_request;
GRANT EXECUTE ON FUNCTION
    vela_claim_content_deletion_target(text, uuid, integer),
    vela_complete_content_deletion_target(uuid, uuid, uuid, text, text),
    vela_retry_content_deletion_target(uuid, uuid, integer, text),
    vela_enqueue_expired_content_deletions(integer)
    TO vela_retention;

GRANT SELECT ON retention_policy_revisions TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE projects, jobs, project_retention_policy_events,
    content_deletion_requests, content_deletion_targets,
    content_deletion_receipts, content_deletion_receipt_targets
    IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM projects
        WHERE retention_policy_revision_id <>
            '00000000-0000-0000-0000-000000001630'::uuid
           OR retention_artifact_days <> 30
           OR retention_request_content_days <> 30
           OR retention_incomplete_content_hours <> 24
           OR retention_scratch_hours <> 24
           OR retention_debug_hours <> 72
           OR retention_metadata_days <> 365
           OR retention_financial_days <> 2557
    ) OR EXISTS (
        SELECT 1
        FROM jobs
        WHERE retention_policy_revision_id <>
                '00000000-0000-0000-0000-000000001630'::uuid
           OR retention_artifact_days <> 30
           OR retention_request_content_days <> 30
           OR retention_incomplete_content_hours <> 24
           OR retention_scratch_hours <> 24
           OR retention_debug_hours <> 72
           OR retention_metadata_days <> 365
           OR retention_financial_days <> 2557
           OR request_content_deleted_at IS NOT NULL
    ) OR EXISTS (SELECT 1 FROM project_retention_policy_events)
      OR EXISTS (SELECT 1 FROM content_deletion_requests)
      OR EXISTS (SELECT 1 FROM content_deletion_targets)
      OR EXISTS (SELECT 1 FROM content_deletion_receipts)
      OR EXISTS (SELECT 1 FROM content_deletion_receipt_targets)
      OR EXISTS (SELECT 1 FROM artifacts WHERE state = 'DELETED')
    THEN
        RAISE EXCEPTION 'cannot remove retention and Content Deletion with durable evidence'
            USING ERRCODE = '55000',
                CONSTRAINT = 'retention_contract_requires_default_empty_evidence';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION vela_service_credential_scopes_valid(
    p_scopes text[]
) RETURNS boolean
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

REVOKE EXECUTE ON FUNCTION
    vela_get_project_retention_policy(uuid),
    vela_set_project_retention_policy(uuid, integer, uuid),
    vela_get_content_deletion_request(uuid, uuid),
    vela_accept_content_deletion_request(
        uuid, uuid, uuid, text, bytea, uuid, uuid, uuid, uuid, uuid, uuid, uuid
    )
    FROM vela_retention_request;
REVOKE EXECUTE ON FUNCTION
    vela_claim_content_deletion_target(text, uuid, integer),
    vela_complete_content_deletion_target(uuid, uuid, uuid, text, text),
    vela_retry_content_deletion_target(uuid, uuid, integer, text),
    vela_enqueue_expired_content_deletions(integer)
    FROM vela_retention;
REVOKE EXECUTE ON FUNCTION
    vela_current_organization_id(),
    vela_current_project_id(),
    vela_current_principal_id(),
    vela_current_request_scope(),
    vela_current_request_credential_id(),
    vela_current_request_actor_kind()
    FROM vela_retention_owner;
REVOKE SELECT (
    id, organization_id, retention_policy_revision_id,
    retention_artifact_days, retention_request_content_days,
    retention_incomplete_content_hours, retention_scratch_hours,
    retention_debug_hours, retention_metadata_days, retention_financial_days,
    created_at
)
    ON projects FROM vela_retention_owner;
REVOKE UPDATE (
    retention_policy_revision_id, retention_artifact_days,
    retention_request_content_days, retention_incomplete_content_hours,
    retention_scratch_hours, retention_debug_hours, retention_metadata_days,
    retention_financial_days
) ON projects FROM vela_retention_owner;
REVOKE SELECT (
    id, organization_id, project_id, state,
    request_content_expires_at, request_content_deleted_at
) ON jobs FROM vela_retention_owner;
REVOKE UPDATE (request_content, request_content_deleted_at, updated_at)
    ON jobs FROM vela_retention_owner;
REVOKE SELECT (
    id, organization_id, project_id, job_id, attempt_id, attempt_fence,
    object_key, object_version_id, size_bytes, sha256, content_type, state
) ON artifacts FROM vela_retention_owner;
REVOKE UPDATE (state, updated_at) ON artifacts FROM vela_retention_owner;
REVOKE SELECT (artifact_id, state, object_version_id, version)
    ON artifact_uploads FROM vela_retention_owner;
REVOKE UPDATE (
    state, next_retry_at, claim_id, claim_owner_kind, claim_owner_id,
    claim_expires_at, version, updated_at
) ON artifact_uploads FROM vela_retention_owner;
REVOKE SELECT (
    organization_id, project_id, job_id, artifact_set_id, eligible_at, revoked_at
)
    ON artifact_access_grants FROM vela_retention_owner;
REVOKE UPDATE (revoked_at) ON artifact_access_grants FROM vela_retention_owner;
REVOKE SELECT ON artifact_sets, artifact_set_items, visible_completions,
    charges, outbox_events FROM vela_retention_owner;
REVOKE EXECUTE ON FUNCTION
    vela_validate_artifact_mutation(),
    vela_validate_artifact_upload_mutation(),
    vela_validate_artifact_access_grant_mutation()
    FROM vela_retention_owner;
REVOKE EXECUTE ON FUNCTION vela_private.vela_cancel_job_for_content_deletion(
    uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
) FROM vela_retention_owner;
REVOKE USAGE ON SCHEMA vela_private FROM vela_retention_owner;
REVOKE EXECUTE ON FUNCTION vela_set_request_context(uuid, bytea, text)
    FROM vela_retention_request;
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

DROP FUNCTION vela_accept_content_deletion_request(
    uuid, uuid, uuid, text, bytea, uuid, uuid, uuid, uuid, uuid, uuid, uuid
);
DROP FUNCTION vela_get_content_deletion_request(uuid, uuid);
DROP FUNCTION vela_enqueue_expired_content_deletions(integer);
DROP FUNCTION vela_retry_content_deletion_target(uuid, uuid, integer, text);
DROP FUNCTION vela_complete_content_deletion_target(uuid, uuid, uuid, text, text);
DROP FUNCTION vela_claim_content_deletion_target(text, uuid, integer);
DROP FUNCTION vela_private.vela_cancel_job_for_content_deletion(
    uuid, uuid, uuid, uuid, uuid, uuid, uuid, uuid
);

DROP TRIGGER content_deletion_receipt_targets_immutable
    ON content_deletion_receipt_targets;
DROP TABLE content_deletion_receipt_targets;
DROP TRIGGER content_deletion_receipts_immutable ON content_deletion_receipts;
DROP FUNCTION vela_reject_content_deletion_receipt_mutation();
DROP TABLE content_deletion_receipts;
DROP TABLE content_deletion_targets;
DROP TABLE content_deletion_requests;
DROP TYPE content_deletion_target_state;
DROP TYPE content_deletion_target_action;
DROP TYPE content_deletion_state;
DROP TYPE content_deletion_source;

ALTER TABLE artifacts
    DROP CONSTRAINT artifacts_content_lifecycle,
    DROP CONSTRAINT artifacts_verification_lifecycle,
    DROP CONSTRAINT artifacts_retention_lifecycle,
    ADD CONSTRAINT artifacts_content_lifecycle_v15 CHECK (
        (
            state = 'STAGING'
            AND object_version_id IS NULL
            AND size_bytes IS NULL
            AND sha256 IS NULL
            AND content_type IS NULL
            AND uploaded_at IS NULL
        ) OR (
            state <> 'STAGING'
            AND length(object_version_id) BETWEEN 1 AND 1000
            AND size_bytes IS NOT NULL
            AND sha256 IS NOT NULL
            AND content_type IS NOT NULL
            AND uploaded_at IS NOT NULL
        )
    ),
    ADD CONSTRAINT artifacts_verification_lifecycle_v15 CHECK (
        (
            state IN ('STAGING', 'UPLOADED')
            AND verification_id IS NULL
            AND verification_request_hash IS NULL
            AND validation_receipt IS NULL
            AND verified_at IS NULL
        ) OR (
            state NOT IN ('STAGING', 'UPLOADED')
            AND verification_id IS NOT NULL
            AND verification_request_hash IS NOT NULL
            AND validation_receipt IS NOT NULL
            AND verified_at IS NOT NULL
        )
    ),
    ADD CONSTRAINT artifacts_retention_lifecycle_v15 CHECK (
        (state <> 'COMMITTED' AND retention_expires_at IS NULL)
        OR (
            state = 'COMMITTED'
            AND retention_expires_at IS NOT NULL
            AND retention_expires_at > verified_at
        )
    );

ALTER TABLE artifact_uploads
    DROP CONSTRAINT artifact_uploads_content_lifecycle,
    DROP CONSTRAINT artifact_uploads_verification_lifecycle,
    ADD CONSTRAINT artifact_uploads_content_lifecycle_v15 CHECK (
        (
            state = 'INITIATED'
            AND object_version_id IS NULL
            AND size_bytes IS NULL
            AND sha256 IS NULL
            AND content_type IS NULL
            AND completion_request_hash IS NULL
            AND uploaded_at IS NULL
        ) OR (
            state = 'UPLOADING'
            AND object_version_id IS NULL
            AND uploaded_at IS NULL
            AND (
                (
                    size_bytes IS NULL
                    AND sha256 IS NULL
                    AND content_type IS NULL
                    AND completion_request_hash IS NULL
                ) OR (
                    size_bytes IS NOT NULL
                    AND sha256 IS NOT NULL
                    AND content_type IS NOT NULL
                    AND completion_request_hash IS NOT NULL
                )
            )
        ) OR (
            state NOT IN ('INITIATED', 'UPLOADING')
            AND length(object_version_id) BETWEEN 1 AND 1000
            AND size_bytes IS NOT NULL
            AND sha256 IS NOT NULL
            AND content_type IS NOT NULL
            AND completion_request_hash IS NOT NULL
            AND uploaded_at IS NOT NULL
        )
    ),
    ADD CONSTRAINT artifact_uploads_verification_lifecycle_v15 CHECK (
        (
            state IN ('INITIATED', 'UPLOADING', 'UPLOADED')
            AND verification_id IS NULL
            AND verification_request_hash IS NULL
            AND validation_receipt IS NULL
            AND verified_at IS NULL
        ) OR (
            state NOT IN ('INITIATED', 'UPLOADING', 'UPLOADED')
            AND verification_id IS NOT NULL
            AND verification_request_hash IS NOT NULL
            AND validation_receipt IS NOT NULL
            AND verified_at IS NOT NULL
        )
    );

DROP FUNCTION vela_set_project_retention_policy(uuid, integer, uuid);
DROP FUNCTION vela_get_project_retention_policy(uuid);
DROP FUNCTION vela_current_request_actor_kind();
DROP FUNCTION vela_current_request_credential_id();
REVOKE SELECT ON retention_policy_revisions FROM vela_internal;
ALTER TABLE jobs DROP CONSTRAINT jobs_request_content_tombstone;
ALTER TABLE jobs DROP CONSTRAINT jobs_request_content_outlives_job;
ALTER TABLE jobs DROP CONSTRAINT jobs_retention_policy_revision;
ALTER TABLE jobs
    DROP COLUMN request_content_deleted_at,
    DROP COLUMN retention_financial_days,
    DROP COLUMN retention_metadata_days,
    DROP COLUMN retention_debug_hours,
    DROP COLUMN retention_scratch_hours,
    DROP COLUMN retention_incomplete_content_hours,
    DROP COLUMN retention_request_content_days,
    DROP COLUMN retention_artifact_days,
    DROP COLUMN retention_policy_revision_id;
CREATE OR REPLACE FUNCTION vela_reject_job_snapshot_mutation() RETURNS trigger
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
DROP TRIGGER project_retention_policy_events_immutable ON project_retention_policy_events;
DROP FUNCTION vela_reject_project_retention_policy_event_mutation();
DROP TABLE project_retention_policy_events;
ALTER TABLE projects DROP CONSTRAINT projects_retention_policy_revision;
ALTER TABLE projects
    DROP COLUMN retention_financial_days,
    DROP COLUMN retention_metadata_days,
    DROP COLUMN retention_debug_hours,
    DROP COLUMN retention_scratch_hours,
    DROP COLUMN retention_incomplete_content_hours,
    DROP COLUMN retention_request_content_days,
    DROP COLUMN retention_artifact_days,
    DROP COLUMN retention_policy_revision_id;
DROP TRIGGER retention_policy_revisions_immutable ON retention_policy_revisions;
DROP FUNCTION vela_reject_retention_policy_revision_mutation();
DROP TABLE retention_policy_revisions;
-- +goose StatementEnd
