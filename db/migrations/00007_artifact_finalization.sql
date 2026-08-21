-- +goose Up
-- +goose StatementBegin
CREATE TYPE artifact_kind AS ENUM ('VIDEO', 'THUMBNAIL');
CREATE TYPE artifact_state AS ENUM (
    'STAGING', 'UPLOADED', 'VERIFIED', 'COMMITTED', 'EXPIRED', 'DELETED'
);
CREATE TYPE artifact_upload_state AS ENUM (
    'INITIATED', 'UPLOADING', 'UPLOADED', 'VERIFIED', 'ABORTED', 'EXPIRED'
);

ALTER TYPE execution_failure_source ADD VALUE 'FINALIZATION_DEADLINE_EXPIRED';
ALTER TYPE execution_failure_source ADD VALUE 'ARTIFACT_RECOVERY_UNRECOVERABLE';

ALTER TABLE execution_failure_decisions
    ADD COLUMN attempt_finalization_seconds bigint NOT NULL DEFAULT 0
        CHECK (attempt_finalization_seconds >= 0),
    ADD COLUMN total_finalization_seconds bigint NOT NULL DEFAULT 0
        CHECK (total_finalization_seconds >= 0);

ALTER TABLE attempt_progress
    DROP CONSTRAINT attempt_progress_execution_phase_check,
    ADD CONSTRAINT attempt_progress_execution_phase_check CHECK (
        execution_phase IN ('PREPARING', 'GENERATING', 'FINALIZING')
    );

ALTER TABLE output_specs
	ADD COLUMN container text NOT NULL DEFAULT 'mp4'
		CHECK (length(container) BETWEEN 1 AND 50),
	ADD COLUMN thumbnail_required boolean NOT NULL DEFAULT true,
	ADD CONSTRAINT output_specs_thumbnail_required CHECK (thumbnail_required);

ALTER TABLE attempts
    ADD CONSTRAINT attempts_finalization_identity
    UNIQUE (organization_id, project_id, id, job_id, fence);

CREATE TABLE artifacts (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    kind artifact_kind NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    object_key text NOT NULL UNIQUE CHECK (
        length(object_key) BETWEEN 1 AND 1000
        AND object_key !~ '(^|/)\.\.(/|$)'
    ),
    expected_content_type text NOT NULL CHECK (
        length(expected_content_type) BETWEEN 1 AND 200
    ),
    object_version_id text,
    size_bytes bigint CHECK (size_bytes > 0),
    sha256 bytea CHECK (octet_length(sha256) = 32),
    content_type text CHECK (length(content_type) BETWEEN 1 AND 200),
    uploaded_at timestamptz,
    verification_id uuid,
    verification_request_hash bytea CHECK (octet_length(verification_request_hash) = 32),
    validation_receipt jsonb CHECK (
        jsonb_typeof(validation_receipt) = 'object'
        AND octet_length(validation_receipt::text) <= 16384
    ),
    verified_at timestamptz,
    retention_expires_at timestamptz,
    state artifact_state NOT NULL DEFAULT 'STAGING',
    expires_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, id
    ),
    UNIQUE (attempt_id, kind, ordinal),
    FOREIGN KEY (
        organization_id, project_id, attempt_id, job_id, attempt_fence
    ) REFERENCES attempts (
        organization_id, project_id, id, job_id, fence
    ),
    CHECK (expires_at > created_at),
    CHECK (
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
    CHECK (
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
    CHECK (
        (state <> 'COMMITTED' AND retention_expires_at IS NULL)
        OR (
            state = 'COMMITTED'
            AND retention_expires_at IS NOT NULL
            AND retention_expires_at > verified_at
        )
    )
);

CREATE TABLE artifact_uploads (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    artifact_id uuid NOT NULL UNIQUE,
    state artifact_upload_state NOT NULL DEFAULT 'INITIATED',
    multipart_upload_id text,
    completed_parts jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (
        jsonb_typeof(completed_parts) = 'array'
    ),
    object_version_id text,
    size_bytes bigint CHECK (size_bytes > 0),
    sha256 bytea CHECK (octet_length(sha256) = 32),
    content_type text CHECK (length(content_type) BETWEEN 1 AND 200),
    completion_request_hash bytea CHECK (octet_length(completion_request_hash) = 32),
    uploaded_at timestamptz,
    verification_id uuid,
    verification_request_hash bytea CHECK (octet_length(verification_request_hash) = 32),
    validation_receipt jsonb CHECK (
        jsonb_typeof(validation_receipt) = 'object'
        AND octet_length(validation_receipt::text) <= 16384
    ),
    verified_at timestamptz,
    retry_count integer NOT NULL DEFAULT 0 CHECK (retry_count >= 0),
    next_retry_at timestamptz,
    claim_id uuid,
    claim_owner_kind lease_owner_kind,
    claim_owner_id text,
    claim_expires_at timestamptz,
    expires_at timestamptz NOT NULL,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, artifact_id
    ) REFERENCES artifacts (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, id
    ),
    CHECK (expires_at > created_at),
    CHECK (multipart_upload_id IS NULL OR length(multipart_upload_id) BETWEEN 1 AND 1000),
    CHECK (
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
    CHECK (
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
    ),
    CHECK (
        (
            claim_id IS NULL
            AND claim_owner_kind IS NULL
            AND claim_owner_id IS NULL
            AND claim_expires_at IS NULL
        ) OR (
            claim_id IS NOT NULL
            AND claim_owner_kind IS NOT NULL
            AND length(claim_owner_id) BETWEEN 1 AND 500
            AND claim_expires_at IS NOT NULL
            AND claim_expires_at <= expires_at
        )
    )
);

ALTER TABLE execution_failure_decisions
    ADD COLUMN artifact_id uuid,
    ADD COLUMN artifact_upload_id uuid,
    ADD COLUMN finalization_failure_code text,
    ADD CONSTRAINT execution_failure_decisions_artifact_finalization_fields CHECK (
        (
            source::text = 'ARTIFACT_RECOVERY_UNRECOVERABLE'
            AND artifact_id IS NOT NULL
            AND artifact_upload_id IS NOT NULL
            AND finalization_failure_code IN (
                'INVALID_COMPLETION_INTENT',
                'MULTIPART_PARTS_MISMATCH',
                'COMPLETED_OBJECT_MISMATCH',
                'VALIDATION_FAILED',
                'RECOVERY_RECEIPT_MISMATCH'
            )
        ) OR (
            source::text <> 'ARTIFACT_RECOVERY_UNRECOVERABLE'
            AND artifact_id IS NULL
            AND artifact_upload_id IS NULL
            AND finalization_failure_code IS NULL
        )
    ),
    ADD CONSTRAINT execution_failure_decisions_artifact_identity
        FOREIGN KEY (organization_id, project_id, artifact_id)
        REFERENCES artifacts(organization_id, project_id, id),
    ADD CONSTRAINT execution_failure_decisions_artifact_upload_identity
        FOREIGN KEY (organization_id, project_id, artifact_upload_id)
        REFERENCES artifact_uploads(organization_id, project_id, id);

CREATE TABLE artifact_sets (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL UNIQUE,
    attempt_id uuid NOT NULL,
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    manifest_sha256 bytea NOT NULL CHECK (octet_length(manifest_sha256) = 32),
    retention_policy_revision text NOT NULL CHECK (
        length(retention_policy_revision) BETWEEN 1 AND 100
    ),
    retention_expires_at timestamptz NOT NULL,
    committed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, job_id, id),
    UNIQUE (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, id
    ),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (
        organization_id, project_id, attempt_id, job_id, attempt_fence
    ) REFERENCES attempts (
        organization_id, project_id, id, job_id, fence
    ),
    CHECK (retention_expires_at > committed_at)
);

CREATE TABLE artifact_set_items (
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    artifact_set_id uuid NOT NULL,
    artifact_id uuid NOT NULL,
    kind artifact_kind NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    object_key text NOT NULL CHECK (length(object_key) BETWEEN 1 AND 1000),
    object_version_id text NOT NULL CHECK (length(object_version_id) BETWEEN 1 AND 1000),
    size_bytes bigint NOT NULL CHECK (size_bytes > 0),
    sha256 bytea NOT NULL CHECK (octet_length(sha256) = 32),
    content_type text NOT NULL CHECK (length(content_type) BETWEEN 1 AND 200),
    validation_receipt jsonb NOT NULL CHECK (
        jsonb_typeof(validation_receipt) = 'object'
        AND octet_length(validation_receipt::text) <= 16384
    ),
    verified_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (artifact_set_id, kind, ordinal),
    UNIQUE (artifact_set_id, artifact_id),
    UNIQUE (organization_id, project_id, artifact_set_id, artifact_id),
    FOREIGN KEY (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, artifact_set_id
    ) REFERENCES artifact_sets (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, id
    ),
    FOREIGN KEY (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, artifact_id
    ) REFERENCES artifacts (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, id
    )
);

ALTER TABLE jobs
    ADD COLUMN result_artifact_set_id uuid,
    ADD CONSTRAINT jobs_result_artifact_set_identity
        FOREIGN KEY (organization_id, project_id, id, result_artifact_set_id)
        REFERENCES artifact_sets(organization_id, project_id, job_id, id);

ALTER TABLE charges
    ADD COLUMN artifact_set_id uuid,
    ADD CONSTRAINT charges_job_identity
        UNIQUE (organization_id, project_id, job_id, id),
    ADD CONSTRAINT charges_artifact_set_identity
        FOREIGN KEY (organization_id, project_id, job_id, artifact_set_id)
        REFERENCES artifact_sets(organization_id, project_id, job_id, id),
    ADD CONSTRAINT charges_visible_completion_identity CHECK (
        (reason = 'VISIBLE_COMPLETION' AND artifact_set_id IS NOT NULL)
        OR (reason = 'CUSTOMER_CANCELLATION' AND artifact_set_id IS NULL)
    );

CREATE TABLE visible_completions (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL UNIQUE,
    attempt_id uuid NOT NULL UNIQUE,
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    authority_lease_id uuid NOT NULL UNIQUE,
    artifact_set_id uuid NOT NULL UNIQUE,
    charge_id uuid NOT NULL UNIQUE,
    candidate_sha256 bytea NOT NULL CHECK (octet_length(candidate_sha256) = 32),
    job_version bigint NOT NULL CHECK (job_version > 0),
    completed_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, job_id, id),
    FOREIGN KEY (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, artifact_set_id
    ) REFERENCES artifact_sets (
        organization_id, project_id, job_id, attempt_id,
        attempt_fence, id
    ),
    FOREIGN KEY (organization_id, project_id, authority_lease_id)
        REFERENCES attempt_leases(organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, job_id, charge_id)
        REFERENCES charges(organization_id, project_id, job_id, id)
);

CREATE TABLE artifact_access_grants (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL UNIQUE,
    artifact_set_id uuid NOT NULL UNIQUE,
    eligible_at timestamptz NOT NULL,
    retention_expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, job_id, artifact_set_id),
    FOREIGN KEY (organization_id, project_id, job_id, artifact_set_id)
        REFERENCES artifact_sets(organization_id, project_id, job_id, id),
    CHECK (retention_expires_at > eligible_at),
    CHECK (revoked_at IS NULL OR revoked_at >= eligible_at)
);

CREATE FUNCTION vela_reject_artifact_publication_history_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Artifact publication history is immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_artifact_publication_history_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_artifact_publication_history_mutation() OWNER TO vela_internal;

CREATE TRIGGER artifact_sets_immutable
BEFORE UPDATE OR DELETE ON artifact_sets
FOR EACH ROW EXECUTE FUNCTION vela_reject_artifact_publication_history_mutation();
CREATE TRIGGER artifact_set_items_immutable
BEFORE UPDATE OR DELETE ON artifact_set_items
FOR EACH ROW EXECUTE FUNCTION vela_reject_artifact_publication_history_mutation();
CREATE TRIGGER visible_completions_immutable
BEFORE UPDATE OR DELETE ON visible_completions
FOR EACH ROW EXECUTE FUNCTION vela_reject_artifact_publication_history_mutation();

CREATE FUNCTION vela_validate_artifact_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.organization_id IS DISTINCT FROM OLD.organization_id
        OR NEW.project_id IS DISTINCT FROM OLD.project_id
        OR NEW.job_id IS DISTINCT FROM OLD.job_id
        OR NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
        OR NEW.attempt_fence IS DISTINCT FROM OLD.attempt_fence
        OR NEW.kind IS DISTINCT FROM OLD.kind
        OR NEW.ordinal IS DISTINCT FROM OLD.ordinal
        OR NEW.object_key IS DISTINCT FROM OLD.object_key
        OR NEW.expected_content_type IS DISTINCT FROM OLD.expected_content_type
        OR NEW.expires_at IS DISTINCT FROM OLD.expires_at
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'immutable Artifact definition fields cannot be changed';
    END IF;
    IF OLD.object_version_id IS NOT NULL
        AND (
            NEW.object_version_id IS DISTINCT FROM OLD.object_version_id
            OR NEW.size_bytes IS DISTINCT FROM OLD.size_bytes
            OR NEW.sha256 IS DISTINCT FROM OLD.sha256
            OR NEW.content_type IS DISTINCT FROM OLD.content_type
            OR NEW.uploaded_at IS DISTINCT FROM OLD.uploaded_at
        )
    THEN
        RAISE EXCEPTION 'immutable Artifact object identity cannot be changed';
    END IF;
    IF OLD.verification_id IS NOT NULL
        AND (
            NEW.verification_id IS DISTINCT FROM OLD.verification_id
            OR NEW.verification_request_hash IS DISTINCT FROM OLD.verification_request_hash
            OR NEW.validation_receipt IS DISTINCT FROM OLD.validation_receipt
            OR NEW.verified_at IS DISTINCT FROM OLD.verified_at
        )
    THEN
        RAISE EXCEPTION 'immutable Artifact verification cannot be changed';
    END IF;
    IF OLD.retention_expires_at IS NOT NULL
        AND NEW.retention_expires_at IS DISTINCT FROM OLD.retention_expires_at
    THEN
        RAISE EXCEPTION 'immutable Artifact retention deadline cannot be changed';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_artifact_mutation() FROM PUBLIC;
ALTER FUNCTION vela_validate_artifact_mutation() OWNER TO vela_internal;

CREATE TRIGGER artifacts_identity_immutable
BEFORE UPDATE ON artifacts
FOR EACH ROW EXECUTE FUNCTION vela_validate_artifact_mutation();

CREATE FUNCTION vela_validate_artifact_upload_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.organization_id IS DISTINCT FROM OLD.organization_id
        OR NEW.project_id IS DISTINCT FROM OLD.project_id
        OR NEW.job_id IS DISTINCT FROM OLD.job_id
        OR NEW.attempt_id IS DISTINCT FROM OLD.attempt_id
        OR NEW.attempt_fence IS DISTINCT FROM OLD.attempt_fence
        OR NEW.artifact_id IS DISTINCT FROM OLD.artifact_id
        OR NEW.expires_at IS DISTINCT FROM OLD.expires_at
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'immutable ArtifactUpload definition fields cannot be changed';
    END IF;
    IF OLD.multipart_upload_id IS NOT NULL
        AND NEW.multipart_upload_id IS DISTINCT FROM OLD.multipart_upload_id
    THEN
        RAISE EXCEPTION 'immutable ArtifactUpload multipart identity cannot be changed';
    END IF;
    IF OLD.object_version_id IS NOT NULL
        AND (
            NEW.completed_parts IS DISTINCT FROM OLD.completed_parts
            OR NEW.object_version_id IS DISTINCT FROM OLD.object_version_id
            OR NEW.size_bytes IS DISTINCT FROM OLD.size_bytes
            OR NEW.sha256 IS DISTINCT FROM OLD.sha256
            OR NEW.content_type IS DISTINCT FROM OLD.content_type
            OR NEW.completion_request_hash IS DISTINCT FROM OLD.completion_request_hash
            OR NEW.uploaded_at IS DISTINCT FROM OLD.uploaded_at
        )
    THEN
        RAISE EXCEPTION 'immutable ArtifactUpload object identity cannot be changed';
    END IF;
    IF OLD.verification_id IS NOT NULL
        AND (
            NEW.verification_id IS DISTINCT FROM OLD.verification_id
            OR NEW.verification_request_hash IS DISTINCT FROM OLD.verification_request_hash
            OR NEW.validation_receipt IS DISTINCT FROM OLD.validation_receipt
            OR NEW.verified_at IS DISTINCT FROM OLD.verified_at
        )
    THEN
        RAISE EXCEPTION 'immutable ArtifactUpload verification cannot be changed';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_artifact_upload_mutation() FROM PUBLIC;
ALTER FUNCTION vela_validate_artifact_upload_mutation() OWNER TO vela_internal;

CREATE TRIGGER artifact_uploads_identity_immutable
BEFORE UPDATE ON artifact_uploads
FOR EACH ROW EXECUTE FUNCTION vela_validate_artifact_upload_mutation();

CREATE FUNCTION vela_validate_artifact_access_grant_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.organization_id IS DISTINCT FROM OLD.organization_id
        OR NEW.project_id IS DISTINCT FROM OLD.project_id
        OR NEW.job_id IS DISTINCT FROM OLD.job_id
        OR NEW.artifact_set_id IS DISTINCT FROM OLD.artifact_set_id
        OR NEW.eligible_at IS DISTINCT FROM OLD.eligible_at
        OR NEW.retention_expires_at IS DISTINCT FROM OLD.retention_expires_at
        OR OLD.revoked_at IS NOT NULL
        OR (NEW.revoked_at IS NOT NULL AND NEW.revoked_at < OLD.eligible_at)
    THEN
        RAISE EXCEPTION 'Artifact access grant identity is immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_artifact_access_grant_mutation() FROM PUBLIC;
ALTER FUNCTION vela_validate_artifact_access_grant_mutation() OWNER TO vela_internal;

CREATE TRIGGER artifact_access_grants_valid
BEFORE UPDATE ON artifact_access_grants
FOR EACH ROW EXECUTE FUNCTION vela_validate_artifact_access_grant_mutation();

CREATE FUNCTION vela_validate_visible_success() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
        AND OLD.state = 'SUCCEEDED'
        AND OLD.result_artifact_set_id IS NULL
        AND NEW.state = OLD.state
        AND NEW.result_artifact_set_id IS NULL
    THEN
        RETURN NEW;
    END IF;
    IF OLD.result_artifact_set_id IS NOT NULL
        AND NEW.result_artifact_set_id IS DISTINCT FROM OLD.result_artifact_set_id
    THEN
        RAISE EXCEPTION 'Job result ArtifactSet is immutable';
    END IF;
    IF NEW.state <> 'SUCCEEDED' AND NEW.result_artifact_set_id IS NOT NULL THEN
        RAISE EXCEPTION 'only a SUCCEEDED Job may reference a result ArtifactSet';
    END IF;
    IF NEW.state = 'SUCCEEDED' THEN
        IF NEW.result_artifact_set_id IS NULL THEN
            RAISE EXCEPTION 'SUCCEEDED Job requires a result ArtifactSet';
        END IF;
        IF NOT EXISTS (
            SELECT 1
            FROM artifact_sets AS artifact_set
            JOIN visible_completions AS completion
              ON completion.artifact_set_id = artifact_set.id
             AND completion.job_id = artifact_set.job_id
            JOIN charges AS charge
              ON charge.id = completion.charge_id
             AND charge.artifact_set_id = artifact_set.id
             AND charge.job_id = artifact_set.job_id
             AND charge.reason = 'VISIBLE_COMPLETION'
            JOIN artifact_access_grants AS access_grant
              ON access_grant.artifact_set_id = artifact_set.id
             AND access_grant.job_id = artifact_set.job_id
             AND access_grant.revoked_at IS NULL
            WHERE artifact_set.id = NEW.result_artifact_set_id
              AND artifact_set.organization_id = NEW.organization_id
              AND artifact_set.project_id = NEW.project_id
              AND artifact_set.job_id = NEW.id
              AND completion.job_version = NEW.version
        ) THEN
            RAISE EXCEPTION 'SUCCEEDED Job requires atomic publication evidence';
        END IF;
        IF EXISTS (
            SELECT 1
            FROM artifacts AS artifact
            JOIN artifact_sets AS artifact_set
              ON artifact_set.id = NEW.result_artifact_set_id
            WHERE artifact.job_id = NEW.id
              AND artifact.attempt_id = artifact_set.attempt_id
              AND artifact.attempt_fence = artifact_set.attempt_fence
              AND (
                  artifact.state <> 'COMMITTED'
                  OR NOT EXISTS (
                      SELECT 1
                      FROM artifact_set_items AS item
                      WHERE item.artifact_set_id = artifact_set.id
                        AND item.artifact_id = artifact.id
                        AND item.object_key = artifact.object_key
                        AND item.object_version_id = artifact.object_version_id
                        AND item.size_bytes = artifact.size_bytes
                        AND item.sha256 = artifact.sha256
                        AND item.content_type = artifact.content_type
                  )
              )
        ) THEN
            RAISE EXCEPTION 'SUCCEEDED Job requires every planned Artifact snapshot';
        END IF;
        IF (
            SELECT count(*)
            FROM outbox_events
            WHERE aggregate_id = NEW.id
              AND aggregate_version = NEW.version
              AND event_type IN (
                  'job.succeeded',
                  'charge.posted',
                  'invoice.export_requested'
              )
        ) <> 3 THEN
            RAISE EXCEPTION 'SUCCEEDED Job requires canonical terminal Outbox events';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_visible_success() FROM PUBLIC;
ALTER FUNCTION vela_validate_visible_success() OWNER TO vela_internal;

CREATE TRIGGER jobs_visible_success_valid
BEFORE INSERT OR UPDATE ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_validate_visible_success();

CREATE FUNCTION vela_set_artifact_request_context(
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

CREATE POLICY jobs_artifact_request_select_policy ON jobs
    FOR SELECT TO vela_artifact_request
    USING (
        vela_current_request_scope() = 'artifacts:read'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

ALTER TABLE artifacts ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifacts FORCE ROW LEVEL SECURITY;
ALTER TABLE artifact_uploads ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_uploads FORCE ROW LEVEL SECURITY;
ALTER TABLE artifact_sets ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_sets FORCE ROW LEVEL SECURITY;
ALTER TABLE artifact_set_items ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_set_items FORCE ROW LEVEL SECURITY;
ALTER TABLE visible_completions ENABLE ROW LEVEL SECURITY;
ALTER TABLE visible_completions FORCE ROW LEVEL SECURITY;
ALTER TABLE artifact_access_grants ENABLE ROW LEVEL SECURITY;
ALTER TABLE artifact_access_grants FORCE ROW LEVEL SECURITY;

CREATE POLICY artifact_sets_request_select_policy ON artifact_sets
    FOR SELECT TO vela_artifact_request
    USING (
        vela_current_request_scope() = 'artifacts:read'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );
CREATE POLICY artifact_set_items_request_select_policy ON artifact_set_items
    FOR SELECT TO vela_artifact_request
    USING (
        vela_current_request_scope() = 'artifacts:read'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );
CREATE POLICY artifact_access_grants_request_select_policy ON artifact_access_grants
    FOR SELECT TO vela_artifact_request
    USING (
        vela_current_request_scope() = 'artifacts:read'
        AND organization_id = vela_current_organization_id()
        AND project_id = vela_current_project_id()
    );

GRANT SELECT, INSERT, UPDATE ON artifacts, artifact_uploads, artifact_access_grants
    TO vela_internal;
GRANT SELECT, INSERT ON artifact_sets, artifact_set_items, visible_completions
    TO vela_internal;
GRANT USAGE ON SCHEMA public TO vela_artifact_request;
GRANT EXECUTE ON FUNCTION vela_current_organization_id(), vela_current_project_id(),
    vela_current_principal_id(), vela_current_request_scope() TO vela_artifact_request;
GRANT SELECT ON jobs, artifact_sets, artifact_set_items, artifact_access_grants
    TO vela_artifact_request;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE
    workers,
    attempt_leases,
    attempts,
    jobs,
    credit_reservations,
    projects,
    worker_pools,
    organization_credit_accounts,
    execution_failure_decisions,
    artifacts,
    artifact_uploads,
    artifact_sets,
    artifact_set_items,
    visible_completions,
    artifact_access_grants,
    job_cancellation_decisions,
    charges,
    cancellation_stop_receipts,
    outbox_events
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM artifacts)
        OR EXISTS (SELECT 1 FROM artifact_uploads)
        OR EXISTS (SELECT 1 FROM artifact_sets)
        OR EXISTS (SELECT 1 FROM artifact_set_items)
        OR EXISTS (SELECT 1 FROM visible_completions)
        OR EXISTS (SELECT 1 FROM artifact_access_grants)
        OR EXISTS (SELECT 1 FROM jobs WHERE state = 'FINALIZING')
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'artifact_finalization_contract_requires_empty_evidence',
            MESSAGE = 'Migration 00007 cannot contract while Artifact or FINALIZING evidence remains';
    END IF;
END
$$;

DROP POLICY jobs_artifact_request_select_policy ON jobs;

DROP TRIGGER IF EXISTS jobs_visible_success_valid ON jobs;
DROP TRIGGER IF EXISTS artifact_access_grants_valid ON artifact_access_grants;
DROP TRIGGER IF EXISTS artifact_uploads_identity_immutable ON artifact_uploads;
DROP TRIGGER IF EXISTS artifacts_identity_immutable ON artifacts;
DROP TRIGGER IF EXISTS visible_completions_immutable ON visible_completions;
DROP TRIGGER IF EXISTS artifact_set_items_immutable ON artifact_set_items;
DROP TRIGGER IF EXISTS artifact_sets_immutable ON artifact_sets;
REVOKE SELECT ON jobs, artifact_sets, artifact_set_items, artifact_access_grants
    FROM vela_artifact_request;
REVOKE EXECUTE ON FUNCTION vela_current_organization_id(), vela_current_project_id(),
    vela_current_principal_id(), vela_current_request_scope() FROM vela_artifact_request;
REVOKE EXECUTE ON FUNCTION vela_set_artifact_request_context(uuid, bytea)
    FROM vela_artifact_request;
REVOKE USAGE ON SCHEMA public FROM vela_artifact_request;
DROP FUNCTION IF EXISTS vela_set_artifact_request_context(uuid, bytea);
DROP FUNCTION IF EXISTS vela_validate_visible_success();
DROP FUNCTION IF EXISTS vela_validate_artifact_access_grant_mutation();
DROP FUNCTION IF EXISTS vela_validate_artifact_upload_mutation();
DROP FUNCTION IF EXISTS vela_validate_artifact_mutation();
DROP FUNCTION IF EXISTS vela_reject_artifact_publication_history_mutation();
ALTER TABLE execution_failure_decisions
    DROP CONSTRAINT IF EXISTS execution_failure_decisions_artifact_upload_identity,
    DROP CONSTRAINT IF EXISTS execution_failure_decisions_artifact_identity,
    DROP CONSTRAINT IF EXISTS execution_failure_decisions_artifact_finalization_fields,
    DROP COLUMN IF EXISTS finalization_failure_code,
    DROP COLUMN IF EXISTS artifact_upload_id,
    DROP COLUMN IF EXISTS artifact_id;
DROP TABLE IF EXISTS artifact_access_grants;
DROP TABLE IF EXISTS visible_completions;
ALTER TABLE charges DROP CONSTRAINT IF EXISTS charges_visible_completion_identity;
ALTER TABLE charges DROP CONSTRAINT IF EXISTS charges_artifact_set_identity;
ALTER TABLE charges DROP CONSTRAINT IF EXISTS charges_job_identity;
ALTER TABLE charges DROP COLUMN IF EXISTS artifact_set_id;
ALTER TABLE jobs DROP CONSTRAINT IF EXISTS jobs_result_artifact_set_identity;
ALTER TABLE jobs DROP COLUMN IF EXISTS result_artifact_set_id;
DROP TABLE IF EXISTS artifact_set_items;
DROP TABLE IF EXISTS artifact_sets;
DROP TABLE IF EXISTS artifact_uploads;
DROP TABLE IF EXISTS artifacts;
ALTER TABLE attempts DROP CONSTRAINT IF EXISTS attempts_finalization_identity;
ALTER TABLE output_specs DROP COLUMN IF EXISTS thumbnail_required;
ALTER TABLE output_specs DROP COLUMN IF EXISTS container;
ALTER TABLE execution_failure_decisions
    DROP COLUMN IF EXISTS total_finalization_seconds,
    DROP COLUMN IF EXISTS attempt_finalization_seconds;
ALTER TABLE attempt_progress
    DROP CONSTRAINT attempt_progress_execution_phase_check,
    ADD CONSTRAINT attempt_progress_execution_phase_check CHECK (
        execution_phase IN ('PREPARING', 'GENERATING')
    );
ALTER TABLE execution_failure_decisions
    DROP CONSTRAINT execution_failure_decisions_check;
ALTER TABLE execution_failure_decisions
    ALTER COLUMN source TYPE text USING source::text;
ALTER TYPE execution_failure_source RENAME TO execution_failure_source_v7;
CREATE TYPE execution_failure_source AS ENUM (
    'WORKER_REPORTED', 'EXECUTION_LEASE_EXPIRED', 'JOB_EXPIRED'
);
ALTER TABLE execution_failure_decisions
    ALTER COLUMN source TYPE execution_failure_source
    USING source::execution_failure_source;
DROP TYPE execution_failure_source_v7;
ALTER TABLE execution_failure_decisions
    ADD CONSTRAINT execution_failure_decisions_check CHECK (
        (
            attempt_id IS NULL
            AND worker_id IS NULL
            AND worker_epoch IS NULL
            AND attempt_fence IS NULL
            AND attempt_state IS NULL
            AND source = 'JOB_EXPIRED'
        )
        OR
        (
            attempt_id IS NOT NULL
            AND worker_id IS NOT NULL
            AND worker_epoch IS NOT NULL
            AND worker_epoch > 0
            AND attempt_fence IS NOT NULL
            AND attempt_fence > 0
            AND attempt_state IN ('FAILED', 'LOST')
        )
    );
DROP TYPE IF EXISTS artifact_upload_state;
DROP TYPE IF EXISTS artifact_state;
DROP TYPE IF EXISTS artifact_kind;
-- +goose StatementEnd
