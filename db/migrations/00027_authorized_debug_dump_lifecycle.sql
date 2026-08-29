-- +goose Up
-- +goose StatementBegin
CREATE TYPE debug_dump_purpose AS ENUM (
    'CUSTOMER_SUPPORT', 'INCIDENT_INVESTIGATION'
);
CREATE TYPE debug_dump_event_action AS ENUM (
    'AUTHORIZED', 'REVOKED',
    'READ_AUTHORIZED', 'READ_DENIED', 'DELIVERED', 'DELIVERY_FAILED',
    'UPLOAD_CLAIMED', 'UPLOADED', 'DELETED'
);
CREATE TYPE debug_dump_state AS ENUM (
    'UPLOADING', 'AVAILABLE', 'DELETED'
);

CREATE TABLE debug_dump_authorizations (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    purpose debug_dump_purpose NOT NULL,
    idempotency_key text NOT NULL CHECK (
        length(idempotency_key) BETWEEN 1 AND 128
        AND idempotency_key ~ '^[A-Za-z0-9._:-]+$'
    ),
    request_hash bytea NOT NULL CHECK (octet_length(request_hash) = 32),
    actor_principal_id uuid NOT NULL,
    actor_session_id uuid NOT NULL,
    authorized_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    revoked_at timestamptz,
    revoked_by_principal_id uuid,
    revoked_by_session_id uuid,
    revocation_idempotency_key text CHECK (
        revocation_idempotency_key IS NULL OR (
            length(revocation_idempotency_key) BETWEEN 1 AND 128
            AND revocation_idempotency_key ~ '^[A-Za-z0-9._:-]+$'
        )
    ),
    revocation_request_hash bytea CHECK (
        revocation_request_hash IS NULL OR octet_length(revocation_request_hash) = 32
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, job_id, id),
    UNIQUE (organization_id, project_id, idempotency_key),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, actor_session_id)
        REFERENCES project_actor_session_attributions(
            organization_id, project_id, actor_session_id
        ),
    FOREIGN KEY (organization_id, actor_principal_id)
        REFERENCES principals(organization_id, id),
    FOREIGN KEY (organization_id, project_id, revoked_by_session_id)
        REFERENCES project_actor_session_attributions(
            organization_id, project_id, actor_session_id
        ),
    FOREIGN KEY (organization_id, revoked_by_principal_id)
        REFERENCES principals(organization_id, id),
    CHECK (expires_at = authorized_at + interval '72 hours'),
    CHECK (
        (
            revoked_at IS NULL
            AND revoked_by_principal_id IS NULL
            AND revoked_by_session_id IS NULL
            AND revocation_idempotency_key IS NULL
            AND revocation_request_hash IS NULL
        )
        OR (
            revoked_at IS NOT NULL
            AND revoked_by_principal_id IS NOT NULL
            AND revoked_by_session_id IS NOT NULL
            AND revocation_idempotency_key IS NOT NULL
            AND revocation_request_hash IS NOT NULL
            AND revoked_at >= authorized_at
        )
    )
);
CREATE INDEX debug_dump_authorizations_job_idx
    ON debug_dump_authorizations (organization_id, project_id, job_id, expires_at DESC);
ALTER TABLE debug_dump_authorizations OWNER TO vela_retention_owner;
REVOKE ALL ON TABLE debug_dump_authorizations FROM PUBLIC;
ALTER TABLE debug_dump_authorizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE debug_dump_authorizations FORCE ROW LEVEL SECURITY;

ALTER TABLE attempts
    ADD COLUMN debug_dump_authorization_id uuid,
    ADD COLUMN debug_dump_authorization_expires_at timestamptz,
    ADD CONSTRAINT attempts_debug_dump_authorization_complete CHECK (
        (debug_dump_authorization_id IS NULL) =
        (debug_dump_authorization_expires_at IS NULL)
    ),
    ADD CONSTRAINT attempts_debug_dump_authorization_fk FOREIGN KEY (
        organization_id, project_id, job_id, debug_dump_authorization_id
    ) REFERENCES debug_dump_authorizations(
        organization_id, project_id, job_id, id
    );

CREATE TABLE debug_dumps (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    authorization_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    worker_id uuid NOT NULL,
    worker_epoch bigint NOT NULL CHECK (worker_epoch > 0),
    object_key text NOT NULL UNIQUE CHECK (
        length(object_key) BETWEEN 1 AND 1000
        AND object_key !~ '[[:cntrl:]]'
    ),
    expected_size_bytes bigint NOT NULL CHECK (
        expected_size_bytes BETWEEN 1 AND 65536
    ),
    expected_sha256 bytea NOT NULL CHECK (octet_length(expected_sha256) = 32),
    expected_content_type text NOT NULL CHECK (
        expected_content_type = 'application/vnd.vela.debug-dump+json'
    ),
    state debug_dump_state NOT NULL DEFAULT 'UPLOADING',
    claim_id uuid,
    claim_expires_at timestamptz,
    multipart_upload_id text CHECK (
        multipart_upload_id IS NULL OR length(multipart_upload_id) BETWEEN 1 AND 1000
    ),
    completed_parts jsonb NOT NULL DEFAULT '[]'::jsonb CHECK (
        jsonb_typeof(completed_parts) = 'array'
    ),
    completion_request_hash bytea CHECK (
        completion_request_hash IS NULL OR octet_length(completion_request_hash) = 32
    ),
    object_version_id text,
    size_bytes bigint,
    sha256 bytea,
    content_type text,
    uploaded_at timestamptz,
    expires_at timestamptz NOT NULL,
    deleted_at timestamptz,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    UNIQUE (organization_id, project_id, id),
    UNIQUE (organization_id, project_id, authorization_id, id),
    UNIQUE (authorization_id, attempt_id),
    FOREIGN KEY (authorization_id, organization_id, project_id, job_id)
        REFERENCES debug_dump_authorizations(id, organization_id, project_id, job_id),
    FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES attempts(organization_id, project_id, id),
    FOREIGN KEY (worker_id) REFERENCES workers(id),
    CHECK ((claim_id IS NULL) = (claim_expires_at IS NULL)),
    CHECK (
        (state = 'UPLOADING' AND object_version_id IS NULL AND size_bytes IS NULL
            AND sha256 IS NULL AND content_type IS NULL AND uploaded_at IS NULL
            AND deleted_at IS NULL)
        OR (state = 'AVAILABLE' AND multipart_upload_id IS NOT NULL
            AND completion_request_hash IS NOT NULL AND object_version_id IS NOT NULL
            AND length(object_version_id) BETWEEN 1 AND 1000
            AND size_bytes = expected_size_bytes AND sha256 = expected_sha256
            AND content_type = expected_content_type AND uploaded_at IS NOT NULL
            AND deleted_at IS NULL)
        OR (state = 'DELETED' AND deleted_at IS NOT NULL AND (
            (object_version_id IS NULL AND size_bytes IS NULL AND sha256 IS NULL
                AND content_type IS NULL AND uploaded_at IS NULL)
            OR (object_version_id IS NOT NULL
                AND size_bytes = expected_size_bytes AND sha256 = expected_sha256
                AND content_type = expected_content_type AND uploaded_at IS NOT NULL)
        ))
    )
);
CREATE INDEX debug_dumps_authorization_idx
    ON debug_dumps (authorization_id, created_at, id);
CREATE INDEX debug_dumps_expiry_idx
    ON debug_dumps (expires_at, id) WHERE state <> 'DELETED';
ALTER TABLE debug_dumps OWNER TO vela_retention_owner;
REVOKE ALL ON TABLE debug_dumps FROM PUBLIC;
ALTER TABLE debug_dumps ENABLE ROW LEVEL SECURITY;
ALTER TABLE debug_dumps FORCE ROW LEVEL SECURITY;
CREATE POLICY debug_dumps_internal_policy ON debug_dumps
    FOR ALL TO vela_internal USING (true) WITH CHECK (true);
CREATE POLICY debug_dumps_retention_owner_policy ON debug_dumps
    FOR ALL TO vela_retention_owner USING (true) WITH CHECK (true);
GRANT SELECT ON debug_dumps TO vela_internal;
GRANT INSERT (
    id, organization_id, project_id, job_id, authorization_id,
    attempt_id, attempt_fence, worker_id, worker_epoch, object_key,
    expected_size_bytes, expected_sha256, expected_content_type,
    expires_at, created_at, updated_at
) ON debug_dumps TO vela_internal;
GRANT UPDATE (
    claim_id, claim_expires_at, multipart_upload_id, completed_parts,
    completion_request_hash, version, updated_at
) ON debug_dumps TO vela_internal;

CREATE TABLE debug_dump_events (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    authorization_id uuid NOT NULL,
    debug_dump_id uuid,
    action debug_dump_event_action NOT NULL,
    outcome_code text NOT NULL CHECK (length(outcome_code) BETWEEN 1 AND 100),
    actor_kind text NOT NULL CHECK (
        actor_kind IN ('HUMAN', 'SERVICE', 'WORKER', 'RETENTION')
    ),
    actor_principal_id uuid,
    actor_session_id uuid,
    worker_id uuid,
    worker_epoch bigint,
    created_at timestamptz NOT NULL,
    FOREIGN KEY (authorization_id, organization_id, project_id, job_id)
        REFERENCES debug_dump_authorizations(id, organization_id, project_id, job_id),
    FOREIGN KEY (organization_id, actor_principal_id)
        REFERENCES principals(organization_id, id),
    FOREIGN KEY (organization_id, project_id, actor_session_id)
        REFERENCES project_actor_session_attributions(
            organization_id, project_id, actor_session_id
        ),
    CHECK (
        (
            actor_kind IN ('HUMAN', 'SERVICE')
            AND actor_principal_id IS NOT NULL
            AND actor_session_id IS NOT NULL
            AND worker_id IS NULL
            AND worker_epoch IS NULL
        ) OR (
            actor_kind = 'WORKER'
            AND actor_principal_id IS NULL
            AND actor_session_id IS NULL
            AND worker_id IS NOT NULL
            AND worker_epoch > 0
        ) OR (
            actor_kind = 'RETENTION'
            AND actor_principal_id IS NULL
            AND actor_session_id IS NULL
            AND worker_id IS NULL
            AND worker_epoch IS NULL
        )
    ),
    CHECK (
        (
            actor_kind = 'HUMAN'
            AND action IN (
                'AUTHORIZED', 'REVOKED', 'READ_AUTHORIZED', 'READ_DENIED',
                'DELIVERED', 'DELIVERY_FAILED'
            )
        ) OR (
            actor_kind = 'SERVICE' AND action = 'REVOKED'
        ) OR (
            actor_kind = 'WORKER'
            AND action IN ('UPLOAD_CLAIMED', 'UPLOADED')
            AND outcome_code = action::text
        ) OR (
            actor_kind = 'RETENTION'
            AND action = 'DELETED'
            AND outcome_code IN ('DELETED', 'ALREADY_ABSENT')
        )
    )
);
CREATE INDEX debug_dump_events_organization_idx
    ON debug_dump_events (organization_id, created_at DESC, id DESC);
CREATE INDEX debug_dump_events_authorization_idx
    ON debug_dump_events (authorization_id, created_at, id);
ALTER TABLE debug_dump_events OWNER TO vela_retention_owner;
REVOKE ALL ON TABLE debug_dump_events FROM PUBLIC;
ALTER TABLE debug_dump_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE debug_dump_events FORCE ROW LEVEL SECURITY;
ALTER TABLE debug_dump_events
    ADD CONSTRAINT debug_dump_events_dump_fk
    FOREIGN KEY (organization_id, project_id, debug_dump_id)
    REFERENCES debug_dumps(organization_id, project_id, id);
CREATE UNIQUE INDEX debug_dump_events_single_claimed_idx
    ON debug_dump_events (debug_dump_id)
    WHERE action = 'UPLOAD_CLAIMED';
CREATE UNIQUE INDEX debug_dump_events_single_uploaded_idx
    ON debug_dump_events (debug_dump_id)
    WHERE action = 'UPLOADED';
CREATE UNIQUE INDEX debug_dump_events_single_deleted_idx
    ON debug_dump_events (debug_dump_id)
    WHERE action = 'DELETED';
CREATE POLICY debug_dump_events_internal_insert_policy ON debug_dump_events
    FOR INSERT TO vela_internal WITH CHECK (actor_kind = 'WORKER');
GRANT INSERT ON debug_dump_events TO vela_internal;

DROP INDEX content_deletion_requests_customer_idempotency_idx;
DO $$
DECLARE
    v_job_source_constraint text;
    v_source_actor_constraint text;
BEGIN
    SELECT constraint_entry.conname INTO STRICT v_job_source_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.content_deletion_requests'::regclass
      AND constraint_entry.contype = 'u'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid)
          = 'UNIQUE (organization_id, project_id, job_id, source)';
    SELECT constraint_entry.conname INTO STRICT v_source_actor_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.content_deletion_requests'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%source%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%CUSTOMER%';
    EXECUTE format(
        'ALTER TABLE public.content_deletion_requests DROP CONSTRAINT %I',
        v_job_source_constraint
    );
    EXECUTE format(
        'ALTER TABLE public.content_deletion_requests DROP CONSTRAINT %I',
        v_source_actor_constraint
    );
END
$$;
ALTER TABLE content_deletion_requests
    ALTER COLUMN source TYPE text USING source::text;
DROP TYPE content_deletion_source;
CREATE TYPE content_deletion_source AS ENUM (
    'CUSTOMER', 'RETENTION_REQUEST_CONTENT', 'RETENTION_ARTIFACT',
    'RETENTION_INCOMPLETE_ARTIFACT', 'RETENTION_DEBUG_DUMP'
);
ALTER TABLE content_deletion_requests
    ALTER COLUMN source TYPE content_deletion_source
    USING source::content_deletion_source;
ALTER TABLE content_deletion_requests
    ADD CONSTRAINT content_deletion_requests_source_actor_check CHECK (
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
    );
CREATE UNIQUE INDEX content_deletion_requests_customer_idempotency_idx
    ON content_deletion_requests (organization_id, project_id, idempotency_key)
    WHERE source = 'CUSTOMER';

ALTER TABLE content_deletion_requests
    ADD COLUMN debug_dump_authorization_id uuid,
    ADD CONSTRAINT content_deletion_requests_debug_authorization_fk FOREIGN KEY (
        debug_dump_authorization_id, organization_id, project_id, job_id
    ) REFERENCES debug_dump_authorizations(id, organization_id, project_id, job_id),
    ADD CONSTRAINT content_deletion_requests_debug_scope_check CHECK (
        (source = 'RETENTION_DEBUG_DUMP') = (debug_dump_authorization_id IS NOT NULL)
    );
CREATE UNIQUE INDEX content_deletion_requests_non_debug_job_source_idx
    ON content_deletion_requests (organization_id, project_id, job_id, source)
    WHERE source <> 'RETENTION_DEBUG_DUMP';
CREATE UNIQUE INDEX content_deletion_requests_debug_authorization_idx
    ON content_deletion_requests (debug_dump_authorization_id)
    WHERE source = 'RETENTION_DEBUG_DUMP';

DO $$
DECLARE
    v_storage_identity_constraint text;
BEGIN
    SELECT constraint_entry.conname INTO STRICT v_storage_identity_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.content_deletion_targets'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%artifact_id IS NOT NULL%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%OBJECT_DISCOVERY%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%MULTIPART_PREFIX%';
    EXECUTE format(
        'ALTER TABLE public.content_deletion_targets DROP CONSTRAINT %I',
        v_storage_identity_constraint
    );
END
$$;
ALTER TABLE content_deletion_targets
    ADD COLUMN debug_dump_authorization_id uuid,
    ADD COLUMN debug_dump_id uuid,
    ADD CONSTRAINT content_deletion_targets_debug_authorization_fk FOREIGN KEY (
        debug_dump_authorization_id, organization_id, project_id, job_id
    ) REFERENCES debug_dump_authorizations(id, organization_id, project_id, job_id),
    ADD CONSTRAINT content_deletion_targets_debug_dump_fk FOREIGN KEY (
        organization_id, project_id, debug_dump_authorization_id, debug_dump_id
    ) REFERENCES debug_dumps(organization_id, project_id, authorization_id, id),
    ADD CONSTRAINT content_deletion_targets_storage_identity_check CHECK (
        (
            action = 'OBJECT_VERSION'
            AND object_version_id IS NOT NULL
            AND discovered_object_version_id IS NULL
            AND (
                (artifact_id IS NOT NULL
                    AND debug_dump_authorization_id IS NULL AND debug_dump_id IS NULL)
                OR (artifact_id IS NULL
                    AND debug_dump_authorization_id IS NOT NULL
                    AND debug_dump_id IS NOT NULL)
            )
        )
        OR (
            action = 'OBJECT_DISCOVERY'
            AND object_version_id IS NULL
            AND (
                (artifact_id IS NOT NULL
                    AND debug_dump_authorization_id IS NULL AND debug_dump_id IS NULL)
                OR (artifact_id IS NULL
                    AND debug_dump_authorization_id IS NOT NULL
                    AND debug_dump_id IS NOT NULL)
            )
        )
        OR (
            action = 'MULTIPART_PREFIX'
            AND artifact_id IS NULL
            AND object_version_id IS NULL
            AND discovered_object_version_id IS NULL
            AND debug_dump_id IS NULL
            AND right(object_key, 1) = '/'
        )
    );
DROP INDEX content_deletion_targets_multipart_idx;
CREATE UNIQUE INDEX content_deletion_targets_debug_dump_idx
    ON content_deletion_targets (request_id, debug_dump_id)
    WHERE debug_dump_id IS NOT NULL;
CREATE UNIQUE INDEX content_deletion_targets_artifact_multipart_idx
    ON content_deletion_targets (request_id)
    WHERE action = 'MULTIPART_PREFIX' AND debug_dump_authorization_id IS NULL;
CREATE UNIQUE INDEX content_deletion_targets_debug_multipart_idx
    ON content_deletion_targets (request_id, debug_dump_authorization_id)
    WHERE action = 'MULTIPART_PREFIX' AND debug_dump_authorization_id IS NOT NULL;

CREATE FUNCTION vela_reject_debug_dump_evidence_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'debug dump evidence is immutable'
        USING ERRCODE = '55000',
            CONSTRAINT = 'debug_dump_evidence_is_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_debug_dump_evidence_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_debug_dump_evidence_mutation() OWNER TO vela_retention_owner;

CREATE FUNCTION vela_enforce_debug_dump_authorization_immutability() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF ROW(
        NEW.id, NEW.organization_id, NEW.project_id, NEW.job_id, NEW.purpose,
        NEW.idempotency_key, NEW.request_hash, NEW.actor_principal_id,
        NEW.actor_session_id, NEW.authorized_at, NEW.expires_at, NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id, OLD.organization_id, OLD.project_id, OLD.job_id, OLD.purpose,
        OLD.idempotency_key, OLD.request_hash, OLD.actor_principal_id,
        OLD.actor_session_id, OLD.authorized_at, OLD.expires_at, OLD.created_at
    ) THEN
        RAISE EXCEPTION 'debug dump authorization identity is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.revoked_at IS NOT NULL AND ROW(
        NEW.revoked_at, NEW.revoked_by_principal_id, NEW.revoked_by_session_id,
        NEW.revocation_idempotency_key, NEW.revocation_request_hash
    ) IS DISTINCT FROM ROW(
        OLD.revoked_at, OLD.revoked_by_principal_id, OLD.revoked_by_session_id,
        OLD.revocation_idempotency_key, OLD.revocation_request_hash
    ) THEN
        RAISE EXCEPTION 'debug dump authorization revocation is permanent'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_debug_dump_authorization_immutability() FROM PUBLIC;
ALTER FUNCTION vela_enforce_debug_dump_authorization_immutability()
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_enforce_debug_dump_lifecycle() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'debug dump lifecycle evidence cannot be deleted'
            USING ERRCODE = '55000',
                CONSTRAINT = 'debug_dump_lifecycle_is_immutable';
    END IF;
    IF ROW(
        NEW.id, NEW.organization_id, NEW.project_id, NEW.job_id,
        NEW.authorization_id, NEW.attempt_id, NEW.attempt_fence,
        NEW.worker_id, NEW.worker_epoch, NEW.object_key,
        NEW.expected_size_bytes, NEW.expected_sha256, NEW.expected_content_type,
        NEW.expires_at, NEW.created_at
    ) IS DISTINCT FROM ROW(
        OLD.id, OLD.organization_id, OLD.project_id, OLD.job_id,
        OLD.authorization_id, OLD.attempt_id, OLD.attempt_fence,
        OLD.worker_id, OLD.worker_epoch, OLD.object_key,
        OLD.expected_size_bytes, OLD.expected_sha256, OLD.expected_content_type,
        OLD.expires_at, OLD.created_at
    ) THEN
        RAISE EXCEPTION 'debug dump identity and expected receipt are immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.multipart_upload_id IS NOT NULL
        AND NEW.multipart_upload_id IS DISTINCT FROM OLD.multipart_upload_id
    THEN
        RAISE EXCEPTION 'debug dump multipart identity is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.completion_request_hash IS NOT NULL AND ROW(
        NEW.completed_parts, NEW.completion_request_hash
    ) IS DISTINCT FROM ROW(
        OLD.completed_parts, OLD.completion_request_hash
    ) THEN
        RAISE EXCEPTION 'debug dump completion intent is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.object_version_id IS NOT NULL AND ROW(
        NEW.object_version_id, NEW.size_bytes, NEW.sha256,
        NEW.content_type, NEW.uploaded_at
    ) IS DISTINCT FROM ROW(
        OLD.object_version_id, OLD.size_bytes, OLD.sha256,
        OLD.content_type, OLD.uploaded_at
    ) THEN
        RAISE EXCEPTION 'debug dump object receipt is immutable'
            USING ERRCODE = '55000';
    END IF;
    IF NOT (
        (OLD.state = 'UPLOADING' AND NEW.state IN ('UPLOADING', 'AVAILABLE', 'DELETED'))
        OR (OLD.state = 'AVAILABLE' AND NEW.state IN ('AVAILABLE', 'DELETED'))
        OR (OLD.state = 'DELETED' AND NEW.state = 'DELETED')
    ) THEN
        RAISE EXCEPTION 'debug dump state transition is invalid'
            USING ERRCODE = '55000';
    END IF;
    IF NEW.state = 'DELETED' AND OLD.state <> 'DELETED'
        AND current_user <> 'vela_retention_owner'
    THEN
        RAISE EXCEPTION 'only retention completion may delete a debug dump'
            USING ERRCODE = '42501';
    END IF;
    IF NEW.version < OLD.version OR NEW.version > OLD.version + 1
        OR NEW.updated_at < OLD.updated_at
    THEN
        RAISE EXCEPTION 'debug dump lifecycle version is invalid'
            USING ERRCODE = '55000';
    END IF;
    IF OLD.state = 'DELETED' AND NEW IS DISTINCT FROM OLD THEN
        RAISE EXCEPTION 'deleted debug dump evidence is immutable'
            USING ERRCODE = '55000';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_debug_dump_lifecycle() FROM PUBLIC;
ALTER FUNCTION vela_enforce_debug_dump_lifecycle() OWNER TO vela_retention_owner;

CREATE TRIGGER debug_dump_authorizations_immutable
BEFORE UPDATE ON debug_dump_authorizations
FOR EACH ROW EXECUTE FUNCTION vela_enforce_debug_dump_authorization_immutability();
CREATE TRIGGER debug_dump_authorizations_no_delete
BEFORE DELETE ON debug_dump_authorizations
FOR EACH ROW EXECUTE FUNCTION vela_reject_debug_dump_evidence_mutation();
CREATE TRIGGER debug_dump_events_immutable
BEFORE UPDATE OR DELETE ON debug_dump_events
FOR EACH ROW EXECUTE FUNCTION vela_reject_debug_dump_evidence_mutation();
CREATE TRIGGER debug_dumps_lifecycle_guard
BEFORE UPDATE OR DELETE ON debug_dumps
FOR EACH ROW EXECUTE FUNCTION vela_enforce_debug_dump_lifecycle();

CREATE FUNCTION vela_record_debug_dump_uploaded(
    p_debug_dump_id uuid,
    p_attempt_id uuid,
    p_attempt_fence bigint,
    p_object_version_id text,
    p_size_bytes bigint,
    p_sha256 bytea,
    p_content_type text,
    p_completed_parts jsonb,
    p_completion_request_hash bytea,
    p_uploaded_at timestamptz
) RETURNS SETOF debug_dumps
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    WITH updated AS (
        UPDATE public.debug_dumps AS dump
        SET state = CASE WHEN dump.state = 'UPLOADING'
                THEN 'AVAILABLE'::public.debug_dump_state ELSE dump.state END,
            object_version_id = p_object_version_id,
            size_bytes = p_size_bytes,
            sha256 = p_sha256,
            content_type = p_content_type,
            completed_parts = p_completed_parts,
            completion_request_hash = p_completion_request_hash,
            uploaded_at = CASE WHEN dump.state = 'UPLOADING'
                THEN p_uploaded_at ELSE dump.uploaded_at END,
            version = CASE WHEN dump.state = 'UPLOADING'
                THEN dump.version + 1 ELSE dump.version END,
            updated_at = CASE WHEN dump.state = 'UPLOADING'
                THEN p_uploaded_at ELSE dump.updated_at END
        WHERE dump.id = p_debug_dump_id
          AND dump.attempt_id = p_attempt_id
          AND dump.attempt_fence = p_attempt_fence
          AND dump.expected_size_bytes = p_size_bytes
          AND dump.expected_sha256 = p_sha256
          AND dump.expected_content_type = p_content_type
          AND dump.completion_request_hash = p_completion_request_hash
          AND dump.completed_parts = p_completed_parts
          AND (
              (dump.state = 'UPLOADING' AND dump.object_version_id IS NULL)
              OR (dump.state = 'AVAILABLE'
                  AND dump.object_version_id = p_object_version_id
                  AND dump.size_bytes = p_size_bytes
                  AND dump.sha256 = p_sha256
                  AND dump.content_type = p_content_type)
          )
        RETURNING dump.*
    ), event AS (
        INSERT INTO public.debug_dump_events (
            id, organization_id, project_id, job_id, authorization_id,
            debug_dump_id, action, outcome_code, actor_kind, worker_id,
            worker_epoch, created_at
        )
        SELECT
            gen_random_uuid(), updated.organization_id, updated.project_id,
            updated.job_id, updated.authorization_id, updated.id,
            'UPLOADED', 'UPLOADED', 'WORKER', updated.worker_id,
            updated.worker_epoch, p_uploaded_at
        FROM updated
        ON CONFLICT DO NOTHING
    )
    SELECT updated.*
    FROM updated;
$$;
REVOKE ALL ON FUNCTION vela_record_debug_dump_uploaded(
    uuid, uuid, bigint, text, bigint, bytea, text, jsonb, bytea, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_record_debug_dump_uploaded(
    uuid, uuid, bigint, text, bigint, bytea, text, jsonb, bytea, timestamptz
) OWNER TO vela_retention_owner;

CREATE FUNCTION vela_private.vela_record_debug_dump_actor_attribution() RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, vela_private, public
AS $$
DECLARE
    v_context vela_private.request_contexts%ROWTYPE;
BEGIN
    SELECT context.* INTO v_context
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id_if_assigned()
      AND context.required_scope = 'debug_dumps:manage'
      AND context.actor_kind = 'HUMAN';
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Human debug dump request context is required'
            USING ERRCODE = '28000';
    END IF;
    INSERT INTO public.project_actor_session_attributions (
        organization_id, project_id, actor_session_id, principal_id,
        actor_kind, first_attributed_at
    ) VALUES (
        v_context.organization_id, v_context.project_id, v_context.credential_id,
        v_context.principal_id, 'HUMAN', v_context.established_at
    )
    ON CONFLICT ON CONSTRAINT project_actor_session_attributions_pkey DO NOTHING;
END
$$;
REVOKE ALL ON FUNCTION vela_private.vela_record_debug_dump_actor_attribution() FROM PUBLIC;
ALTER FUNCTION vela_private.vela_record_debug_dump_actor_attribution() OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_private.vela_record_debug_dump_actor_attribution()
    TO vela_retention_owner;

CREATE FUNCTION vela_private.vela_record_content_deletion_actor_attribution() RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, vela_private, public
AS $$
DECLARE
    v_context vela_private.request_contexts%ROWTYPE;
BEGIN
    SELECT context.* INTO v_context
    FROM vela_private.request_contexts AS context
    WHERE context.backend_pid = pg_catalog.pg_backend_pid()
      AND context.transaction_id = pg_catalog.pg_current_xact_id_if_assigned()
      AND context.required_scope = 'content_deletion:manage'
      AND context.actor_kind IN ('HUMAN', 'SERVICE');
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Content Deletion actor context is required'
            USING ERRCODE = '28000';
    END IF;
    INSERT INTO public.project_actor_session_attributions (
        organization_id, project_id, actor_session_id, principal_id,
        actor_kind, first_attributed_at
    ) VALUES (
        v_context.organization_id, v_context.project_id, v_context.credential_id,
        v_context.principal_id, v_context.actor_kind, v_context.established_at
    )
    ON CONFLICT ON CONSTRAINT project_actor_session_attributions_pkey DO NOTHING;
END
$$;
REVOKE ALL ON FUNCTION vela_private.vela_record_content_deletion_actor_attribution()
    FROM PUBLIC;
ALTER FUNCTION vela_private.vela_record_content_deletion_actor_attribution()
    OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_private.vela_record_content_deletion_actor_attribution()
    TO vela_retention_owner;

CREATE FUNCTION vela_authorize_debug_dump(
    p_authorization_id uuid,
    p_project_id uuid,
    p_job_id uuid,
    p_idempotency_key text,
    p_request_hash bytea,
    p_purpose debug_dump_purpose
) RETURNS TABLE (
    authorization_id uuid,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    purpose text,
    authorized_at timestamptz,
    expires_at timestamptz,
    revoked_at timestamptz,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_context_project_id uuid := public.vela_current_project_id();
    v_principal_id uuid := public.vela_current_principal_id();
    v_session_id uuid := public.vela_current_request_credential_id();
    v_actor_kind public.principal_kind := public.vela_current_request_actor_kind();
    v_scope text := public.vela_current_request_scope();
    v_job_state public.job_state;
    v_retention_debug_hours integer;
    v_existing public.debug_dump_authorizations%ROWTYPE;
    v_now timestamptz := transaction_timestamp();
BEGIN
    IF v_scope IS DISTINCT FROM 'debug_dumps:manage'
        OR v_actor_kind IS DISTINCT FROM 'HUMAN'
        OR v_organization_id IS NULL
        OR v_context_project_id IS DISTINCT FROM p_project_id
        OR v_principal_id IS NULL
        OR v_session_id IS NULL
    THEN
        RAISE EXCEPTION 'Human ProjectAdmin debug dump context is required'
            USING ERRCODE = '28000';
    END IF;
    IF p_authorization_id IS NULL OR p_job_id IS NULL
        OR length(p_idempotency_key) NOT BETWEEN 1 AND 128
        OR p_idempotency_key !~ '^[A-Za-z0-9._:-]+$'
        OR octet_length(p_request_hash) IS DISTINCT FROM 32
        OR p_purpose IS NULL
    THEN
        RAISE EXCEPTION 'invalid debug dump authorization' USING ERRCODE = '22023';
    END IF;

    SELECT candidate.* INTO v_existing
    FROM public.debug_dump_authorizations AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.project_id = p_project_id
      AND candidate.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.job_id IS DISTINCT FROM p_job_id
            OR v_existing.request_hash IS DISTINCT FROM p_request_hash
            OR v_existing.purpose IS DISTINCT FROM p_purpose
        THEN
            RAISE EXCEPTION 'Idempotency-Key was already used for another debug dump authorization'
                USING ERRCODE = '23505',
                    CONSTRAINT = 'debug_dump_authorizations_idempotency_key_key';
        END IF;
        RETURN QUERY SELECT
            v_existing.id, v_existing.organization_id, v_existing.project_id,
            v_existing.job_id, v_existing.purpose::text, v_existing.authorized_at,
            v_existing.expires_at, v_existing.revoked_at, true;
        RETURN;
    END IF;

    SELECT job.state, job.retention_debug_hours
    INTO v_job_state, v_retention_debug_hours
    FROM public.jobs AS job
    WHERE job.organization_id = v_organization_id
      AND job.project_id = p_project_id
      AND job.id = p_job_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Job is not visible in this Project' USING ERRCODE = 'P0002';
    END IF;
    IF v_job_state NOT IN ('QUEUED', 'RETRY_WAIT') OR EXISTS (
        SELECT 1
        FROM public.attempts AS attempt
        WHERE attempt.organization_id = v_organization_id
          AND attempt.project_id = p_project_id
          AND attempt.job_id = p_job_id
          AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
    ) THEN
        RAISE EXCEPTION 'debug dump authorization requires a queued Job without an active Attempt'
            USING ERRCODE = '55000',
                CONSTRAINT = 'debug_dump_authorization_requires_queued_job';
    END IF;
    IF v_retention_debug_hours IS DISTINCT FROM 72 THEN
        RAISE EXCEPTION 'unsupported debug dump retention snapshot'
            USING ERRCODE = '55000';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.debug_dump_authorizations AS candidate
        WHERE candidate.organization_id = v_organization_id
          AND candidate.project_id = p_project_id
          AND candidate.job_id = p_job_id
          AND candidate.revoked_at IS NULL
          AND candidate.expires_at > v_now
    ) THEN
        RAISE EXCEPTION 'Job already has an active debug dump authorization'
            USING ERRCODE = '23505',
                CONSTRAINT = 'debug_dump_authorizations_one_active';
    END IF;

    PERFORM vela_private.vela_record_debug_dump_actor_attribution();

    INSERT INTO public.debug_dump_authorizations (
        id, organization_id, project_id, job_id, purpose, idempotency_key,
        request_hash, actor_principal_id, actor_session_id, authorized_at, expires_at
    ) VALUES (
        p_authorization_id, v_organization_id, p_project_id, p_job_id, p_purpose,
        p_idempotency_key, p_request_hash, v_principal_id, v_session_id, v_now,
        v_now + make_interval(hours => v_retention_debug_hours)
    ) RETURNING * INTO v_existing;

    INSERT INTO public.debug_dump_events (
        id, organization_id, project_id, job_id, authorization_id,
        action, outcome_code, actor_kind, actor_principal_id, actor_session_id, created_at
    ) VALUES (
        gen_random_uuid(), v_organization_id, p_project_id, p_job_id,
        p_authorization_id, 'AUTHORIZED', 'AUTHORIZED', 'HUMAN',
        v_principal_id, v_session_id, v_now
    );

    RETURN QUERY SELECT
        v_existing.id, v_existing.organization_id, v_existing.project_id,
        v_existing.job_id, v_existing.purpose::text, v_existing.authorized_at,
        v_existing.expires_at, v_existing.revoked_at, false;
END
$$;
REVOKE ALL ON FUNCTION vela_authorize_debug_dump(
    uuid, uuid, uuid, text, bytea, debug_dump_purpose
) FROM PUBLIC;
ALTER FUNCTION vela_authorize_debug_dump(
    uuid, uuid, uuid, text, bytea, debug_dump_purpose
) OWNER TO vela_retention_owner;

CREATE FUNCTION vela_get_debug_dump_authorization(
    p_project_id uuid,
    p_job_id uuid,
    p_authorization_id uuid
) RETURNS TABLE (
    authorization_id uuid,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    purpose text,
    authorized_at timestamptz,
    expires_at timestamptz,
    revoked_at timestamptz,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_context_project_id uuid := public.vela_current_project_id();
    v_actor_kind public.principal_kind := public.vela_current_request_actor_kind();
    v_scope text := public.vela_current_request_scope();
BEGIN
    IF v_scope IS DISTINCT FROM 'debug_dumps:manage'
        OR v_actor_kind IS DISTINCT FROM 'HUMAN'
        OR v_organization_id IS NULL
        OR v_context_project_id IS DISTINCT FROM p_project_id
    THEN
        RAISE EXCEPTION 'Human ProjectAdmin debug dump context is required'
            USING ERRCODE = '28000';
    END IF;
    RETURN QUERY SELECT
        candidate.id, candidate.organization_id, candidate.project_id,
        candidate.job_id, candidate.purpose::text, candidate.authorized_at,
        candidate.expires_at, candidate.revoked_at, false
    FROM public.debug_dump_authorizations AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.project_id = p_project_id
      AND candidate.job_id = p_job_id
      AND candidate.id = p_authorization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'debug dump authorization is not visible' USING ERRCODE = 'P0002';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_get_debug_dump_authorization(uuid, uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_get_debug_dump_authorization(uuid, uuid, uuid)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_revoke_debug_dump_authorization(
    p_project_id uuid,
    p_job_id uuid,
    p_authorization_id uuid,
    p_idempotency_key text,
    p_request_hash bytea
) RETURNS TABLE (
    authorization_id uuid,
    organization_id uuid,
    project_id uuid,
    job_id uuid,
    purpose text,
    authorized_at timestamptz,
    expires_at timestamptz,
    revoked_at timestamptz,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_context_project_id uuid := public.vela_current_project_id();
    v_principal_id uuid := public.vela_current_principal_id();
    v_session_id uuid := public.vela_current_request_credential_id();
    v_actor_kind public.principal_kind := public.vela_current_request_actor_kind();
    v_scope text := public.vela_current_request_scope();
    v_existing public.debug_dump_authorizations%ROWTYPE;
    v_now timestamptz := transaction_timestamp();
BEGIN
    IF v_scope IS DISTINCT FROM 'debug_dumps:manage'
        OR v_actor_kind IS DISTINCT FROM 'HUMAN'
        OR v_organization_id IS NULL
        OR v_context_project_id IS DISTINCT FROM p_project_id
        OR v_principal_id IS NULL
        OR v_session_id IS NULL
    THEN
        RAISE EXCEPTION 'Human ProjectAdmin debug dump context is required'
            USING ERRCODE = '28000';
    END IF;
    IF p_job_id IS NULL OR p_authorization_id IS NULL
        OR length(p_idempotency_key) NOT BETWEEN 1 AND 128
        OR p_idempotency_key !~ '^[A-Za-z0-9._:-]+$'
        OR octet_length(p_request_hash) IS DISTINCT FROM 32
    THEN
        RAISE EXCEPTION 'invalid debug dump revocation' USING ERRCODE = '22023';
    END IF;

    SELECT candidate.* INTO v_existing
    FROM public.debug_dump_authorizations AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.project_id = p_project_id
      AND candidate.job_id = p_job_id
      AND candidate.id = p_authorization_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'debug dump authorization is not visible' USING ERRCODE = 'P0002';
    END IF;
    IF v_existing.revoked_at IS NOT NULL THEN
        IF v_existing.revocation_idempotency_key IS DISTINCT FROM p_idempotency_key
            OR v_existing.revocation_request_hash IS DISTINCT FROM p_request_hash
        THEN
            RAISE EXCEPTION 'debug dump authorization was revoked by another request'
                USING ERRCODE = '23505';
        END IF;
        RETURN QUERY SELECT
            v_existing.id, v_existing.organization_id, v_existing.project_id,
            v_existing.job_id, v_existing.purpose::text, v_existing.authorized_at,
            v_existing.expires_at, v_existing.revoked_at, true;
        RETURN;
    END IF;

    PERFORM vela_private.vela_record_debug_dump_actor_attribution();
    UPDATE public.debug_dump_authorizations AS candidate
    SET revoked_at = v_now,
        revoked_by_principal_id = v_principal_id,
        revoked_by_session_id = v_session_id,
        revocation_idempotency_key = p_idempotency_key,
        revocation_request_hash = p_request_hash
    WHERE candidate.id = p_authorization_id
    RETURNING * INTO v_existing;
    INSERT INTO public.debug_dump_events (
        id, organization_id, project_id, job_id, authorization_id,
        action, outcome_code, actor_kind, actor_principal_id, actor_session_id, created_at
    ) VALUES (
        gen_random_uuid(), v_organization_id, p_project_id, p_job_id,
        p_authorization_id, 'REVOKED', 'REVOKED', 'HUMAN',
        v_principal_id, v_session_id, v_now
    );
    RETURN QUERY SELECT
        v_existing.id, v_existing.organization_id, v_existing.project_id,
        v_existing.job_id, v_existing.purpose::text, v_existing.authorized_at,
        v_existing.expires_at, v_existing.revoked_at, false;
END
$$;
REVOKE ALL ON FUNCTION vela_revoke_debug_dump_authorization(
    uuid, uuid, uuid, text, bytea
) FROM PUBLIC;
ALTER FUNCTION vela_revoke_debug_dump_authorization(uuid, uuid, uuid, text, bytea)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_attach_debug_dumps_to_customer_deletion() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_revoked_at timestamptz := NEW.requested_at;
BEGIN
    PERFORM vela_private.vela_record_content_deletion_actor_attribution();

    WITH revoked AS (
        UPDATE public.debug_dump_authorizations AS candidate
        SET revoked_at = v_revoked_at,
            revoked_by_principal_id = NEW.actor_principal_id,
            revoked_by_session_id = NEW.actor_credential_id,
            revocation_idempotency_key = 'content-deletion:' || NEW.id::text,
            revocation_request_hash = pg_catalog.sha256(
                convert_to('vela.debug-dump.content-deletion.v1', 'UTF8')
                || decode('00', 'hex')
                || convert_to(NEW.id::text, 'UTF8')
            )
        WHERE candidate.organization_id = NEW.organization_id
          AND candidate.project_id = NEW.project_id
          AND candidate.job_id = NEW.job_id
          AND candidate.revoked_at IS NULL
        RETURNING candidate.*
    )
    INSERT INTO public.debug_dump_events (
        id, organization_id, project_id, job_id, authorization_id,
        action, outcome_code, actor_kind, actor_principal_id, actor_session_id,
        created_at
    )
    SELECT
        gen_random_uuid(), revoked.organization_id, revoked.project_id, revoked.job_id,
        revoked.id, 'REVOKED', 'CONTENT_DELETION', NEW.actor_kind,
        NEW.actor_principal_id, NEW.actor_credential_id, v_revoked_at
    FROM revoked;

    INSERT INTO public.content_deletion_targets (
        id, organization_id, project_id, job_id, request_id, action,
        debug_dump_authorization_id, debug_dump_id,
        object_key, object_version_id
    )
    SELECT
        gen_random_uuid(), dump.organization_id, dump.project_id, dump.job_id,
        NEW.id,
        CASE WHEN dump.object_version_id IS NULL
            THEN 'OBJECT_DISCOVERY'::public.content_deletion_target_action
            ELSE 'OBJECT_VERSION'::public.content_deletion_target_action END,
        dump.authorization_id, dump.id, dump.object_key, dump.object_version_id
    FROM public.debug_dumps AS dump
    WHERE dump.organization_id = NEW.organization_id
      AND dump.project_id = NEW.project_id
      AND dump.job_id = NEW.job_id
      AND dump.state <> 'DELETED';

    INSERT INTO public.content_deletion_targets (
        id, organization_id, project_id, job_id, request_id, action,
        debug_dump_authorization_id, object_key
    )
    SELECT
        gen_random_uuid(), candidate.organization_id, candidate.project_id,
        candidate.job_id, NEW.id, 'MULTIPART_PREFIX', candidate.id,
        format(
            'debug-dumps/%s/%s/%s/%s/',
            candidate.organization_id,
            candidate.project_id,
            candidate.job_id,
            candidate.id
        )
    FROM public.debug_dump_authorizations AS candidate
    WHERE candidate.organization_id = NEW.organization_id
      AND candidate.project_id = NEW.project_id
      AND candidate.job_id = NEW.job_id;

    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_attach_debug_dumps_to_customer_deletion() FROM PUBLIC;
ALTER FUNCTION vela_attach_debug_dumps_to_customer_deletion()
    OWNER TO vela_retention_owner;
CREATE TRIGGER content_deletion_requests_attach_debug_dumps
AFTER INSERT ON content_deletion_requests
FOR EACH ROW WHEN (NEW.source = 'CUSTOMER')
EXECUTE FUNCTION vela_attach_debug_dumps_to_customer_deletion();

CREATE FUNCTION vela_mark_debug_dump_deleted_from_target() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_deleted_at timestamptz := NEW.completed_at;
BEGIN
    IF NEW.debug_dump_id IS NULL
        OR NEW.state <> 'COMPLETED'
        OR OLD.state = 'COMPLETED'
    THEN
        RETURN NEW;
    END IF;

    WITH deleted AS (
        UPDATE public.debug_dumps AS dump
        SET state = 'DELETED',
            deleted_at = v_deleted_at,
            version = dump.version + 1,
            updated_at = v_deleted_at
        WHERE dump.organization_id = NEW.organization_id
          AND dump.project_id = NEW.project_id
          AND dump.authorization_id = NEW.debug_dump_authorization_id
          AND dump.id = NEW.debug_dump_id
          AND dump.state <> 'DELETED'
        RETURNING dump.*
    )
    INSERT INTO public.debug_dump_events (
        id, organization_id, project_id, job_id, authorization_id, debug_dump_id,
        action, outcome_code, actor_kind, created_at
    )
    SELECT
        gen_random_uuid(), deleted.organization_id, deleted.project_id, deleted.job_id,
        deleted.authorization_id, deleted.id, 'DELETED', NEW.storage_outcome,
        'RETENTION', v_deleted_at
    FROM deleted
    ON CONFLICT DO NOTHING;

    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_mark_debug_dump_deleted_from_target() FROM PUBLIC;
ALTER FUNCTION vela_mark_debug_dump_deleted_from_target()
    OWNER TO vela_retention_owner;
CREATE TRIGGER content_deletion_targets_mark_debug_dump_deleted
AFTER UPDATE OF state ON content_deletion_targets
FOR EACH ROW
EXECUTE FUNCTION vela_mark_debug_dump_deleted_from_target();

CREATE FUNCTION vela_enqueue_expired_debug_dump_deletions(
    p_limit integer
) RETURNS integer
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_authorization record;
    v_request_id uuid;
    v_requests_created integer := 0;
BEGIN
    IF p_limit NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION 'invalid debug dump retention enqueue limit'
            USING ERRCODE = '22023';
    END IF;

    FOR v_authorization IN
        SELECT candidate.id, candidate.organization_id,
            candidate.project_id, candidate.job_id, candidate.expires_at
        FROM public.debug_dump_authorizations AS candidate
        WHERE candidate.expires_at <= v_now
          AND NOT EXISTS (
              SELECT 1
              FROM public.content_deletion_requests AS existing
              WHERE existing.debug_dump_authorization_id = candidate.id
                AND existing.source = 'RETENTION_DEBUG_DUMP'
          )
        ORDER BY candidate.expires_at, candidate.id
        FOR UPDATE OF candidate SKIP LOCKED
        LIMIT p_limit
    LOOP
        v_request_id := gen_random_uuid();
        INSERT INTO public.content_deletion_requests (
            id, organization_id, project_id, job_id, source,
            debug_dump_authorization_id, requested_at, deadline_at
        ) VALUES (
            v_request_id,
            v_authorization.organization_id,
            v_authorization.project_id,
            v_authorization.job_id,
            'RETENTION_DEBUG_DUMP',
            v_authorization.id,
            v_authorization.expires_at,
            v_authorization.expires_at + interval '24 hours'
        );

        INSERT INTO public.content_deletion_targets (
            id, organization_id, project_id, job_id, request_id, action,
            debug_dump_authorization_id, debug_dump_id,
            object_key, object_version_id
        )
        SELECT
            gen_random_uuid(), dump.organization_id, dump.project_id, dump.job_id,
            v_request_id,
            CASE WHEN dump.object_version_id IS NULL
                THEN 'OBJECT_DISCOVERY'::public.content_deletion_target_action
                ELSE 'OBJECT_VERSION'::public.content_deletion_target_action END,
            dump.authorization_id, dump.id, dump.object_key, dump.object_version_id
        FROM public.debug_dumps AS dump
        WHERE dump.authorization_id = v_authorization.id
          AND dump.state <> 'DELETED';

        INSERT INTO public.content_deletion_targets (
            id, organization_id, project_id, job_id, request_id, action,
            debug_dump_authorization_id, object_key
        ) VALUES (
            gen_random_uuid(),
            v_authorization.organization_id,
            v_authorization.project_id,
            v_authorization.job_id,
            v_request_id,
            'MULTIPART_PREFIX',
            v_authorization.id,
            format(
                'debug-dumps/%s/%s/%s/%s/',
                v_authorization.organization_id,
                v_authorization.project_id,
                v_authorization.job_id,
                v_authorization.id
            )
        );
        v_requests_created := v_requests_created + 1;
    END LOOP;

    RETURN v_requests_created;
END
$$;
REVOKE ALL ON FUNCTION vela_enqueue_expired_debug_dump_deletions(integer) FROM PUBLIC;
ALTER FUNCTION vela_enqueue_expired_debug_dump_deletions(integer)
    OWNER TO vela_retention_owner;

ALTER FUNCTION vela_enqueue_expired_content_deletions(integer)
    RENAME TO vela_enqueue_expired_content_deletions_v26;
REVOKE EXECUTE ON FUNCTION vela_enqueue_expired_content_deletions_v26(integer)
    FROM vela_retention;

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
    v_request_content_completed integer;
    v_artifact_requests_created integer;
    v_debug_dump_requests integer;
BEGIN
    -- Keep the two-column migration-26 ABI for the exact N-1 Reconciler. The
    -- second value is an aggregate deletion-request count in current code.
    SELECT expired.request_content_completed, expired.artifact_requests_created
    INTO STRICT v_request_content_completed, v_artifact_requests_created
    FROM public.vela_enqueue_expired_content_deletions_v26(p_limit) AS expired;

    SELECT public.vela_enqueue_expired_debug_dump_deletions(p_limit)
    INTO STRICT v_debug_dump_requests;

    RETURN QUERY SELECT
        v_request_content_completed,
        v_artifact_requests_created + v_debug_dump_requests;
END
$$;
REVOKE ALL ON FUNCTION vela_enqueue_expired_content_deletions(integer) FROM PUBLIC;
ALTER FUNCTION vela_enqueue_expired_content_deletions(integer)
    OWNER TO vela_retention_owner;
GRANT EXECUTE ON FUNCTION vela_enqueue_expired_content_deletions(integer)
    TO vela_retention;

CREATE FUNCTION vela_list_debug_dumps(
    p_project_id uuid,
    p_job_id uuid,
    p_authorization_id uuid
) RETURNS TABLE (
    debug_dump_id uuid,
    authorization_id uuid,
    attempt_id uuid,
    dump_state text,
    size_bytes bigint,
    sha256 bytea,
    content_type text,
    created_at timestamptz,
    uploaded_at timestamptz,
    expires_at timestamptz,
    deleted_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_context_project_id uuid := public.vela_current_project_id();
    v_actor_kind public.principal_kind := public.vela_current_request_actor_kind();
    v_scope text := public.vela_current_request_scope();
BEGIN
    IF v_scope IS DISTINCT FROM 'debug_dumps:manage'
        OR v_actor_kind IS DISTINCT FROM 'HUMAN'
        OR v_organization_id IS NULL
        OR v_context_project_id IS DISTINCT FROM p_project_id
    THEN
        RAISE EXCEPTION 'Human ProjectAdmin debug dump context is required'
            USING ERRCODE = '28000';
    END IF;
    PERFORM 1
    FROM public.debug_dump_authorizations AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.project_id = p_project_id
      AND candidate.job_id = p_job_id
      AND candidate.id = p_authorization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'debug dump authorization is not visible' USING ERRCODE = 'P0002';
    END IF;
    RETURN QUERY SELECT
        dump.id,
        dump.authorization_id,
        dump.attempt_id,
        dump.state::text,
        COALESCE(dump.size_bytes, dump.expected_size_bytes),
        COALESCE(dump.sha256, dump.expected_sha256),
        COALESCE(dump.content_type, dump.expected_content_type),
        dump.created_at,
        dump.uploaded_at,
        dump.expires_at,
        dump.deleted_at
    FROM public.debug_dumps AS dump
    WHERE dump.organization_id = v_organization_id
      AND dump.project_id = p_project_id
      AND dump.job_id = p_job_id
      AND dump.authorization_id = p_authorization_id
    ORDER BY dump.created_at, dump.id;
END
$$;
REVOKE ALL ON FUNCTION vela_list_debug_dumps(uuid, uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_list_debug_dumps(uuid, uuid, uuid) OWNER TO vela_retention_owner;

CREATE FUNCTION vela_authorize_debug_dump_read(
    p_project_id uuid,
    p_job_id uuid,
    p_authorization_id uuid,
    p_debug_dump_id uuid
) RETURNS TABLE (
    authorized boolean,
    object_key text,
    object_version_id text,
    size_bytes bigint,
    sha256 bytea,
    content_type text,
    authorization_expires_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_context_project_id uuid := public.vela_current_project_id();
    v_principal_id uuid := public.vela_current_principal_id();
    v_session_id uuid := public.vela_current_request_credential_id();
    v_actor_kind public.principal_kind := public.vela_current_request_actor_kind();
    v_scope text := public.vela_current_request_scope();
    v_authorization public.debug_dump_authorizations%ROWTYPE;
    v_dump public.debug_dumps%ROWTYPE;
    v_now timestamptz := transaction_timestamp();
    v_outcome text;
BEGIN
    IF v_scope IS DISTINCT FROM 'debug_dumps:manage'
        OR v_actor_kind IS DISTINCT FROM 'HUMAN'
        OR v_organization_id IS NULL
        OR v_context_project_id IS DISTINCT FROM p_project_id
        OR v_principal_id IS NULL
        OR v_session_id IS NULL
    THEN
        RAISE EXCEPTION 'Human ProjectAdmin debug dump context is required'
            USING ERRCODE = '28000';
    END IF;
    SELECT candidate.* INTO v_authorization
    FROM public.debug_dump_authorizations AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.project_id = p_project_id
      AND candidate.job_id = p_job_id
      AND candidate.id = p_authorization_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'debug dump target is not visible' USING ERRCODE = 'P0002';
    END IF;
    SELECT candidate.* INTO v_dump
    FROM public.debug_dumps AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.project_id = p_project_id
      AND candidate.job_id = p_job_id
      AND candidate.authorization_id = p_authorization_id
      AND candidate.id = p_debug_dump_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'debug dump target is not visible' USING ERRCODE = 'P0002';
    END IF;
    PERFORM vela_private.vela_record_debug_dump_actor_attribution();
    v_outcome := CASE
        WHEN v_authorization.revoked_at IS NOT NULL THEN 'AUTHORIZATION_REVOKED'
        WHEN v_authorization.expires_at <= v_now THEN 'AUTHORIZATION_EXPIRED'
        WHEN v_dump.state <> 'AVAILABLE' THEN 'DUMP_UNAVAILABLE'
        ELSE 'READ_AUTHORIZED'
    END;
    INSERT INTO public.debug_dump_events (
        id, organization_id, project_id, job_id, authorization_id, debug_dump_id,
        action, outcome_code, actor_kind, actor_principal_id, actor_session_id, created_at
    ) VALUES (
        gen_random_uuid(), v_organization_id, p_project_id, p_job_id,
        p_authorization_id, p_debug_dump_id,
        CASE WHEN v_outcome = 'READ_AUTHORIZED'
            THEN 'READ_AUTHORIZED'::public.debug_dump_event_action
            ELSE 'READ_DENIED'::public.debug_dump_event_action END,
        v_outcome, 'HUMAN', v_principal_id, v_session_id, v_now
    );
    IF v_outcome <> 'READ_AUTHORIZED' THEN
        RETURN QUERY SELECT false, NULL::text, NULL::text, NULL::bigint,
            NULL::bytea, NULL::text, v_authorization.expires_at;
        RETURN;
    END IF;
    RETURN QUERY SELECT true, v_dump.object_key, v_dump.object_version_id,
        v_dump.size_bytes, v_dump.sha256, v_dump.content_type, v_authorization.expires_at;
END
$$;
REVOKE ALL ON FUNCTION vela_authorize_debug_dump_read(uuid, uuid, uuid, uuid)
    FROM PUBLIC;
ALTER FUNCTION vela_authorize_debug_dump_read(uuid, uuid, uuid, uuid)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_record_debug_dump_delivery(
    p_project_id uuid,
    p_job_id uuid,
    p_authorization_id uuid,
    p_debug_dump_id uuid,
    p_object_version_id text,
    p_sign_succeeded boolean
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_organization_id uuid := public.vela_current_organization_id();
    v_context_project_id uuid := public.vela_current_project_id();
    v_principal_id uuid := public.vela_current_principal_id();
    v_session_id uuid := public.vela_current_request_credential_id();
    v_actor_kind public.principal_kind := public.vela_current_request_actor_kind();
    v_scope text := public.vela_current_request_scope();
    v_authorization public.debug_dump_authorizations%ROWTYPE;
    v_dump public.debug_dumps%ROWTYPE;
    v_now timestamptz := transaction_timestamp();
    v_delivered boolean := false;
    v_outcome text;
BEGIN
    IF v_scope IS DISTINCT FROM 'debug_dumps:manage'
        OR v_actor_kind IS DISTINCT FROM 'HUMAN'
        OR v_organization_id IS NULL
        OR v_context_project_id IS DISTINCT FROM p_project_id
        OR v_principal_id IS NULL
        OR v_session_id IS NULL
        OR p_sign_succeeded IS NULL
    THEN
        RAISE EXCEPTION 'Human ProjectAdmin debug dump context is required'
            USING ERRCODE = '28000';
    END IF;
    SELECT candidate.* INTO v_authorization
    FROM public.debug_dump_authorizations AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.project_id = p_project_id
      AND candidate.job_id = p_job_id
      AND candidate.id = p_authorization_id;
    SELECT candidate.* INTO v_dump
    FROM public.debug_dumps AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.project_id = p_project_id
      AND candidate.job_id = p_job_id
      AND candidate.authorization_id = p_authorization_id
      AND candidate.id = p_debug_dump_id;
    v_delivered := p_sign_succeeded
        AND v_authorization.id IS NOT NULL
        AND v_authorization.revoked_at IS NULL
        AND v_authorization.expires_at > v_now
        AND v_dump.id IS NOT NULL
        AND v_dump.state = 'AVAILABLE'
        AND v_dump.object_version_id = p_object_version_id;
    v_outcome := CASE
        WHEN NOT p_sign_succeeded THEN 'SIGNING_FAILED'
        WHEN v_authorization.id IS NULL OR v_dump.id IS NULL THEN 'TARGET_NOT_VISIBLE'
        WHEN v_authorization.revoked_at IS NOT NULL THEN 'AUTHORIZATION_REVOKED'
        WHEN v_authorization.expires_at <= v_now THEN 'AUTHORIZATION_EXPIRED'
        WHEN v_dump.state <> 'AVAILABLE' THEN 'DUMP_UNAVAILABLE'
        WHEN v_dump.object_version_id IS DISTINCT FROM p_object_version_id
            THEN 'OBJECT_VERSION_CHANGED'
        ELSE 'DELIVERED'
    END;
    IF v_authorization.id IS NOT NULL AND v_dump.id IS NOT NULL THEN
        PERFORM vela_private.vela_record_debug_dump_actor_attribution();
        INSERT INTO public.debug_dump_events (
            id, organization_id, project_id, job_id, authorization_id, debug_dump_id,
            action, outcome_code, actor_kind, actor_principal_id, actor_session_id, created_at
        ) VALUES (
            gen_random_uuid(), v_organization_id, p_project_id, p_job_id,
            p_authorization_id, p_debug_dump_id,
            CASE WHEN v_delivered THEN 'DELIVERED'::public.debug_dump_event_action
                ELSE 'DELIVERY_FAILED'::public.debug_dump_event_action END,
            v_outcome, 'HUMAN', v_principal_id, v_session_id, v_now
        );
    END IF;
    RETURN v_delivered;
END
$$;
REVOKE ALL ON FUNCTION vela_record_debug_dump_delivery(
    uuid, uuid, uuid, uuid, text, boolean
) FROM PUBLIC;
ALTER FUNCTION vela_record_debug_dump_delivery(uuid, uuid, uuid, uuid, text, boolean)
    OWNER TO vela_retention_owner;

CREATE FUNCTION vela_list_organization_audit_events_v3(
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
SET search_path = pg_catalog, public
AS $$
BEGIN
    RETURN QUERY
    SELECT event.*
    FROM (
        SELECT previous.*
        FROM public.vela_list_organization_audit_events_v2(
            p_organization_id,
            p_limit
        ) AS previous
        UNION ALL
        SELECT
            debug_event.id,
            'DEBUG_DUMP'::text,
            debug_event.action::text,
            debug_event.project_id,
            debug_event.actor_principal_id,
            debug_event.actor_session_id,
            CASE WHEN debug_event.debug_dump_id IS NULL
                THEN 'DEBUG_DUMP_AUTHORIZATION'::text
                ELSE 'DEBUG_DUMP'::text END,
            COALESCE(debug_event.debug_dump_id, debug_event.authorization_id),
            debug_event.created_at,
            NULL::text,
            debug_event.outcome_code
        FROM public.debug_dump_events AS debug_event
        WHERE debug_event.organization_id = p_organization_id
    ) AS event
    ORDER BY event.created_at DESC, event.event_id DESC
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_organization_audit_events_v3(uuid, integer)
    FROM PUBLIC;
ALTER FUNCTION vela_list_organization_audit_events_v3(uuid, integer)
    OWNER TO vela_internal;
GRANT SELECT ON debug_dump_events TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_list_organization_audit_events_v3(uuid, integer)
    TO vela_debug_dump_audit_request;

CREATE VIEW vela_internal_debug_dump_authorizations
WITH (security_barrier = true)
AS SELECT
    id AS authorization_id,
    organization_id,
    project_id,
    job_id,
    authorized_at,
    expires_at,
    revoked_at
FROM debug_dump_authorizations;
ALTER VIEW vela_internal_debug_dump_authorizations OWNER TO vela_retention_owner;
REVOKE ALL ON vela_internal_debug_dump_authorizations FROM PUBLIC;

CREATE FUNCTION vela_confirm_debug_dump_authorization_for_assignment(
    p_authorization_id uuid,
    p_assigned_at timestamptz
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_active boolean;
BEGIN
    SELECT candidate.revoked_at IS NULL AND candidate.expires_at > p_assigned_at
    INTO v_active
    FROM public.debug_dump_authorizations AS candidate
    WHERE candidate.id = p_authorization_id
    FOR UPDATE;
    RETURN COALESCE(v_active, false);
END
$$;
REVOKE ALL ON FUNCTION vela_confirm_debug_dump_authorization_for_assignment(
    uuid, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_confirm_debug_dump_authorization_for_assignment(uuid, timestamptz)
    OWNER TO vela_retention_owner;

CREATE OR REPLACE FUNCTION vela_project_role_scopes(p_role project_role) RETURNS text[]
LANGUAGE sql IMMUTABLE STRICT
SET search_path = pg_catalog
AS $$
    SELECT CASE p_role
        WHEN 'ProjectAdmin'::public.project_role THEN
            ARRAY[
                'content_deletion:manage',
                'debug_dumps:manage',
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

GRANT SELECT (retention_debug_hours) ON jobs TO vela_retention_owner;
GRANT SELECT (id, organization_id, project_id, job_id, state) ON attempts
    TO vela_retention_owner;
GRANT SELECT ON vela_internal_debug_dump_authorizations TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_confirm_debug_dump_authorization_for_assignment(
    uuid, timestamptz
) TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_record_debug_dump_uploaded(
    uuid, uuid, bigint, text, bigint, bytea, text, jsonb, bytea, timestamptz
) TO vela_internal;
GRANT USAGE ON SCHEMA public TO vela_debug_dump_request, vela_debug_dump_audit_request;
GRANT USAGE ON TYPE debug_dump_purpose TO vela_debug_dump_request;
GRANT EXECUTE ON FUNCTION vela_set_request_context(uuid, bytea, text)
    TO vela_debug_dump_request;
GRANT EXECUTE ON FUNCTION vela_authorize_debug_dump(
    uuid, uuid, uuid, text, bytea, debug_dump_purpose
) TO vela_debug_dump_request;
GRANT EXECUTE ON FUNCTION vela_get_debug_dump_authorization(uuid, uuid, uuid)
    TO vela_debug_dump_request;
GRANT EXECUTE ON FUNCTION vela_revoke_debug_dump_authorization(
    uuid, uuid, uuid, text, bytea
) TO vela_debug_dump_request;
GRANT EXECUTE ON FUNCTION vela_list_debug_dumps(uuid, uuid, uuid)
    TO vela_debug_dump_request;
GRANT EXECUTE ON FUNCTION vela_authorize_debug_dump_read(uuid, uuid, uuid, uuid)
    TO vela_debug_dump_request;
GRANT EXECUTE ON FUNCTION vela_record_debug_dump_delivery(
    uuid, uuid, uuid, uuid, text, boolean
) TO vela_debug_dump_request;
GRANT EXECUTE ON FUNCTION vela_set_organization_identity_admin_context(uuid, bytea, text)
    TO vela_debug_dump_audit_request;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE content_deletion_targets, content_deletion_requests,
    debug_dump_events, debug_dumps, debug_dump_authorizations
    IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM debug_dump_authorizations)
        OR EXISTS (SELECT 1 FROM debug_dumps)
        OR EXISTS (SELECT 1 FROM debug_dump_events)
    THEN
        RAISE EXCEPTION 'cannot remove authorized debug dump lifecycle with durable evidence'
            USING ERRCODE = '55000',
                CONSTRAINT = 'debug_dump_contract_requires_empty_evidence';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_authorize_debug_dump(
    uuid, uuid, uuid, text, bytea, debug_dump_purpose
) FROM vela_debug_dump_request;
REVOKE EXECUTE ON FUNCTION vela_get_debug_dump_authorization(uuid, uuid, uuid)
    FROM vela_debug_dump_request;
REVOKE EXECUTE ON FUNCTION vela_revoke_debug_dump_authorization(
    uuid, uuid, uuid, text, bytea
) FROM vela_debug_dump_request;
REVOKE EXECUTE ON FUNCTION vela_list_debug_dumps(uuid, uuid, uuid)
    FROM vela_debug_dump_request;
REVOKE EXECUTE ON FUNCTION vela_authorize_debug_dump_read(uuid, uuid, uuid, uuid)
    FROM vela_debug_dump_request;
REVOKE EXECUTE ON FUNCTION vela_record_debug_dump_delivery(
    uuid, uuid, uuid, uuid, text, boolean
) FROM vela_debug_dump_request;
REVOKE EXECUTE ON FUNCTION vela_list_organization_audit_events_v3(uuid, integer)
    FROM vela_debug_dump_audit_request;
REVOKE EXECUTE ON FUNCTION vela_set_request_context(uuid, bytea, text)
    FROM vela_debug_dump_request;
REVOKE EXECUTE ON FUNCTION vela_set_organization_identity_admin_context(uuid, bytea, text)
    FROM vela_debug_dump_audit_request;
REVOKE USAGE ON TYPE debug_dump_purpose FROM vela_debug_dump_request;
REVOKE USAGE ON SCHEMA public FROM vela_debug_dump_request, vela_debug_dump_audit_request;
REVOKE SELECT (id, organization_id, project_id, job_id, state) ON attempts
    FROM vela_retention_owner;
REVOKE SELECT (retention_debug_hours) ON jobs FROM vela_retention_owner;
REVOKE SELECT ON vela_internal_debug_dump_authorizations FROM vela_internal;
REVOKE EXECUTE ON FUNCTION vela_confirm_debug_dump_authorization_for_assignment(
    uuid, timestamptz
) FROM vela_internal;
REVOKE EXECUTE ON FUNCTION vela_record_debug_dump_uploaded(
    uuid, uuid, bigint, text, bigint, bytea, text, jsonb, bytea, timestamptz
) FROM vela_internal;

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

REVOKE EXECUTE ON FUNCTION vela_enqueue_expired_content_deletions(integer)
    FROM vela_retention;
DROP FUNCTION vela_enqueue_expired_content_deletions(integer);
ALTER FUNCTION vela_enqueue_expired_content_deletions_v26(integer)
    RENAME TO vela_enqueue_expired_content_deletions;
GRANT EXECUTE ON FUNCTION vela_enqueue_expired_content_deletions(integer)
    TO vela_retention;
DROP FUNCTION vela_enqueue_expired_debug_dump_deletions(integer);

DROP TRIGGER content_deletion_targets_mark_debug_dump_deleted
    ON content_deletion_targets;
DROP FUNCTION vela_mark_debug_dump_deleted_from_target();
DROP TRIGGER content_deletion_requests_attach_debug_dumps
    ON content_deletion_requests;
DROP FUNCTION vela_attach_debug_dumps_to_customer_deletion();
REVOKE EXECUTE ON FUNCTION vela_private.vela_record_content_deletion_actor_attribution()
    FROM vela_retention_owner;
DROP FUNCTION vela_private.vela_record_content_deletion_actor_attribution();

DROP INDEX content_deletion_targets_debug_multipart_idx;
DROP INDEX content_deletion_targets_artifact_multipart_idx;
DROP INDEX content_deletion_targets_debug_dump_idx;
ALTER TABLE content_deletion_targets
    DROP CONSTRAINT content_deletion_targets_debug_dump_fk,
    DROP CONSTRAINT content_deletion_targets_debug_authorization_fk,
    DROP CONSTRAINT content_deletion_targets_storage_identity_check,
    DROP COLUMN debug_dump_id,
    DROP COLUMN debug_dump_authorization_id,
    ADD CONSTRAINT content_deletion_targets_storage_identity_check CHECK (
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
    );
CREATE UNIQUE INDEX content_deletion_targets_multipart_idx
    ON content_deletion_targets (request_id)
    WHERE action = 'MULTIPART_PREFIX';

DROP INDEX content_deletion_requests_debug_authorization_idx;
DROP INDEX content_deletion_requests_non_debug_job_source_idx;
ALTER TABLE content_deletion_requests
    DROP CONSTRAINT content_deletion_requests_debug_authorization_fk,
    DROP CONSTRAINT content_deletion_requests_debug_scope_check,
    DROP COLUMN debug_dump_authorization_id;

DROP INDEX content_deletion_requests_customer_idempotency_idx;
DO $$
DECLARE
    v_source_constraint text;
BEGIN
    SELECT constraint_entry.conname INTO STRICT v_source_constraint
    FROM pg_catalog.pg_constraint AS constraint_entry
    WHERE constraint_entry.conrelid = 'public.content_deletion_requests'::regclass
      AND constraint_entry.contype = 'c'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%source%'
      AND pg_catalog.pg_get_constraintdef(constraint_entry.oid) LIKE '%CUSTOMER%';
    EXECUTE format(
        'ALTER TABLE public.content_deletion_requests DROP CONSTRAINT %I',
        v_source_constraint
    );
END
$$;
ALTER TABLE content_deletion_requests
    ALTER COLUMN source TYPE text USING source::text;
DROP TYPE content_deletion_source;
CREATE TYPE content_deletion_source AS ENUM (
    'CUSTOMER', 'RETENTION_REQUEST_CONTENT', 'RETENTION_ARTIFACT',
    'RETENTION_INCOMPLETE_ARTIFACT'
);
ALTER TABLE content_deletion_requests
    ALTER COLUMN source TYPE content_deletion_source
    USING source::content_deletion_source;
ALTER TABLE content_deletion_requests
    ADD CONSTRAINT content_deletion_requests_source_actor_check CHECK (
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
    ADD CONSTRAINT content_deletion_requests_job_source_key UNIQUE (
        organization_id, project_id, job_id, source
    );
CREATE UNIQUE INDEX content_deletion_requests_customer_idempotency_idx
    ON content_deletion_requests (organization_id, project_id, idempotency_key)
    WHERE source = 'CUSTOMER';

DROP FUNCTION vela_record_debug_dump_delivery(uuid, uuid, uuid, uuid, text, boolean);
REVOKE SELECT ON debug_dump_events FROM vela_internal;
DROP FUNCTION vela_list_organization_audit_events_v3(uuid, integer);
DROP FUNCTION vela_authorize_debug_dump_read(uuid, uuid, uuid, uuid);
DROP FUNCTION vela_list_debug_dumps(uuid, uuid, uuid);
DROP FUNCTION vela_authorize_debug_dump(
    uuid, uuid, uuid, text, bytea, debug_dump_purpose
);
DROP FUNCTION vela_get_debug_dump_authorization(uuid, uuid, uuid);
DROP FUNCTION vela_revoke_debug_dump_authorization(uuid, uuid, uuid, text, bytea);
DROP FUNCTION vela_confirm_debug_dump_authorization_for_assignment(uuid, timestamptz);
DROP FUNCTION vela_record_debug_dump_uploaded(
    uuid, uuid, bigint, text, bigint, bytea, text, jsonb, bytea, timestamptz
);
DROP VIEW vela_internal_debug_dump_authorizations;
REVOKE EXECUTE ON FUNCTION vela_private.vela_record_debug_dump_actor_attribution()
    FROM vela_retention_owner;
DROP FUNCTION vela_private.vela_record_debug_dump_actor_attribution();
DROP TRIGGER debug_dumps_lifecycle_guard ON debug_dumps;
DROP TRIGGER debug_dump_events_immutable ON debug_dump_events;
DROP TRIGGER debug_dump_authorizations_no_delete ON debug_dump_authorizations;
DROP TRIGGER debug_dump_authorizations_immutable ON debug_dump_authorizations;
DROP FUNCTION vela_enforce_debug_dump_authorization_immutability();
DROP FUNCTION vela_enforce_debug_dump_lifecycle();
DROP FUNCTION vela_reject_debug_dump_evidence_mutation();
ALTER TABLE attempts
    DROP CONSTRAINT attempts_debug_dump_authorization_fk,
    DROP CONSTRAINT attempts_debug_dump_authorization_complete,
    DROP COLUMN debug_dump_authorization_expires_at,
    DROP COLUMN debug_dump_authorization_id;
DROP TABLE debug_dump_events;
DROP POLICY debug_dumps_internal_policy ON debug_dumps;
DROP POLICY debug_dumps_retention_owner_policy ON debug_dumps;
DROP TABLE debug_dumps;
DROP TABLE debug_dump_authorizations;
DROP TYPE debug_dump_state;
DROP TYPE debug_dump_event_action;
DROP TYPE debug_dump_purpose;
-- +goose StatementEnd
