-- +goose Up
-- +goose StatementBegin
CREATE TABLE legacy_h3_contraction_readiness_receipts (
    zero_backlog_receipt_id uuid PRIMARY KEY REFERENCES
        stage_cutover_zero_backlog_receipts(id),
    cutover_revision_id uuid NOT NULL UNIQUE REFERENCES stage_cutover_revisions(id),
    prepared_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    prepared_by text NOT NULL CHECK (
        length(prepared_by) BETWEEN 1 AND 300
        AND btrim(prepared_by) = prepared_by
        AND prepared_by !~ '[[:cntrl:]]'
    ),
    archive_digest bytea NOT NULL CHECK (octet_length(archive_digest) = 32),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32)
);

CREATE TABLE legacy_h3_execution_archive (
    zero_backlog_receipt_id uuid NOT NULL REFERENCES
        stage_cutover_zero_backlog_receipts(id),
    record_kind text NOT NULL CHECK (
        record_kind IN ('JOB', 'ATTEMPT', 'LEASE')
    ),
    record_id uuid NOT NULL,
    parent_id uuid,
    terminal_state text NOT NULL CHECK (
        length(terminal_state) BETWEEN 1 AND 100
    ),
    machine_authority jsonb NOT NULL CHECK (
        jsonb_typeof(machine_authority) = 'object'
    ),
    archived_at timestamptz NOT NULL,
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    PRIMARY KEY (zero_backlog_receipt_id, record_kind, record_id)
);

CREATE TABLE legacy_h3_contraction_v52_function_restore (
    signature text PRIMARY KEY,
    definition text NOT NULL CHECK (length(definition) > 0)
);

INSERT INTO legacy_h3_contraction_v52_function_restore (signature, definition)
SELECT source.signature, pg_get_functiondef(source.signature::regprocedure)
FROM (VALUES
    ('vela_capture_legacy_authority_inventory(uuid,text)'),
    ('vela_current_legacy_authority_inventory_total()')
) AS source(signature);

CREATE OR REPLACE FUNCTION vela_current_legacy_authority_inventory_total()
RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    LOCK TABLE public.jobs, public.attempts, public.attempt_leases,
        public.artifact_uploads, public.outbox_events, public.inbox_receipts
    IN SHARE MODE;
    RETURN (
        SELECT count(*)
        FROM (
            SELECT 1 FROM public.jobs AS job
            WHERE job.execution_authority_kind = 'LEGACY_WORKER'
              AND job.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')
            UNION ALL
            SELECT 1 FROM public.attempts AS attempt
            WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
              AND attempt.state NOT IN (
                  'SUCCEEDED', 'FAILED', 'LOST', 'CANCELED'
              )
            UNION ALL
            SELECT 1
            FROM public.attempt_leases AS lease
            JOIN public.attempts AS attempt ON attempt.id = lease.attempt_id
            WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
              AND lease.phase IN ('EXECUTION', 'FINALIZATION')
              AND lease.revoked_at IS NULL
              AND lease.expires_at > clock_timestamp()
            UNION ALL
            SELECT 1
            FROM public.artifact_uploads AS upload
            JOIN public.attempts AS attempt ON attempt.id = upload.attempt_id
            WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
              AND upload.state IN ('INITIATED', 'UPLOADING', 'UPLOADED')
            UNION ALL
            SELECT 1
            FROM public.outbox_events AS event
            JOIN public.jobs AS job ON job.id = event.aggregate_id
            WHERE job.execution_authority_kind = 'LEGACY_WORKER'
              AND event.published_at IS NULL
            UNION ALL
            SELECT 1
            FROM public.outbox_events AS event
            JOIN public.jobs AS job ON job.id = event.aggregate_id
            WHERE job.execution_authority_kind = 'LEGACY_WORKER'
              AND event.published_at IS NOT NULL
            UNION ALL
            SELECT 1
            FROM public.outbox_events AS event
            JOIN public.jobs AS job ON job.id = event.aggregate_id
            WHERE job.execution_authority_kind = 'LEGACY_WORKER'
              AND event.event_type = 'job.ready'
              AND event.published_at IS NOT NULL
              AND NOT EXISTS (
                  SELECT 1 FROM public.inbox_receipts AS receipt
                  WHERE receipt.consumer_name = 'scheduler'
                    AND receipt.event_id = event.event_id
              )
            UNION ALL
            SELECT 1 FROM public.jobs AS job
            WHERE job.execution_authority_kind = 'LEGACY_WORKER'
              AND job.state = 'RETRY_WAIT'
        ) AS inventory
    );
END
$$;

CREATE OR REPLACE FUNCTION vela_capture_legacy_authority_inventory(
    p_snapshot_id uuid,
    p_observed_by text
) RETURNS legacy_authority_inventory_snapshots
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_existing public.legacy_authority_inventory_snapshots%ROWTYPE;
    v_result public.legacy_authority_inventory_snapshots%ROWTYPE;
    v_cutover_revision_id uuid;
BEGIN
    IF p_snapshot_id IS NULL OR p_observed_by IS NULL
       OR length(p_observed_by) NOT BETWEEN 1 AND 300
       OR btrim(p_observed_by) <> p_observed_by
       OR p_observed_by ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_authority_inventory_request_invalid',
            MESSAGE = 'Legacy authority inventory request is invalid';
    END IF;
    SELECT snapshot.* INTO v_existing
    FROM public.legacy_authority_inventory_snapshots AS snapshot
    WHERE snapshot.id = p_snapshot_id;
    IF FOUND THEN
        IF v_existing.observed_by <> p_observed_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'legacy_authority_inventory_replay_mismatch',
                MESSAGE = 'Legacy authority inventory replay identity changed';
        END IF;
        RETURN v_existing;
    END IF;

    SELECT current_revision_id INTO STRICT v_cutover_revision_id
    FROM public.stage_cutover_control WHERE singleton;

    SELECT
        count(*) FILTER (WHERE source = 'jobs'),
        count(*) FILTER (WHERE source = 'attempts'),
        count(*) FILTER (WHERE source = 'execution_leases'),
        count(*) FILTER (WHERE source = 'finalization_leases'),
        count(*) FILTER (WHERE source = 'uploads'),
        count(*) FILTER (WHERE source = 'unpublished_outbox'),
        count(*) FILTER (WHERE source = 'retained_published_outbox'),
        count(*) FILTER (WHERE source = 'inbox'),
        count(*) FILTER (WHERE source = 'retry')
    INTO
        v_result.nonterminal_jobs,
        v_result.nonterminal_attempts,
        v_result.active_execution_leases,
        v_result.active_finalization_leases,
        v_result.active_artifact_uploads,
        v_result.unpublished_outbox_events,
        v_result.retained_published_outbox_events,
        v_result.scheduler_inbox_backlog,
        v_result.retry_recovery_backlog
    FROM (
        SELECT 'jobs' AS source FROM public.jobs AS job
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND job.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')
        UNION ALL
        SELECT 'attempts' FROM public.attempts AS attempt
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
          AND attempt.state NOT IN (
              'SUCCEEDED', 'FAILED', 'LOST', 'CANCELED'
          )
        UNION ALL
        SELECT 'execution_leases'
        FROM public.attempt_leases AS lease
        JOIN public.attempts AS attempt ON attempt.id = lease.attempt_id
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
          AND lease.phase = 'EXECUTION'
          AND lease.revoked_at IS NULL
          AND lease.expires_at > clock_timestamp()
        UNION ALL
        SELECT 'finalization_leases'
        FROM public.attempt_leases AS lease
        JOIN public.attempts AS attempt ON attempt.id = lease.attempt_id
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
          AND lease.phase = 'FINALIZATION'
          AND lease.revoked_at IS NULL
          AND lease.expires_at > clock_timestamp()
        UNION ALL
        SELECT 'uploads'
        FROM public.artifact_uploads AS upload
        JOIN public.attempts AS attempt ON attempt.id = upload.attempt_id
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
          AND upload.state IN ('INITIATED', 'UPLOADING', 'UPLOADED')
        UNION ALL
        SELECT 'unpublished_outbox'
        FROM public.outbox_events AS event
        JOIN public.jobs AS job ON job.id = event.aggregate_id
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND event.published_at IS NULL
        UNION ALL
        SELECT 'retained_published_outbox'
        FROM public.outbox_events AS event
        JOIN public.jobs AS job ON job.id = event.aggregate_id
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND event.published_at IS NOT NULL
        UNION ALL
        SELECT 'inbox'
        FROM public.outbox_events AS event
        JOIN public.jobs AS job ON job.id = event.aggregate_id
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND event.event_type = 'job.ready'
          AND event.published_at IS NOT NULL
          AND NOT EXISTS (
              SELECT 1 FROM public.inbox_receipts AS receipt
              WHERE receipt.consumer_name = 'scheduler'
                AND receipt.event_id = event.event_id
          )
        UNION ALL
        SELECT 'retry' FROM public.jobs AS job
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND job.state = 'RETRY_WAIT'
    ) AS inventory;

    v_result.id := p_snapshot_id;
    v_result.cutover_revision_id := v_cutover_revision_id;
    v_result.observed_at := clock_timestamp();
    v_result.observed_by := p_observed_by;
    v_result.total_count := v_result.nonterminal_jobs + v_result.nonterminal_attempts
        + v_result.active_execution_leases + v_result.active_finalization_leases
        + v_result.active_artifact_uploads + v_result.unpublished_outbox_events
        + v_result.retained_published_outbox_events
        + v_result.scheduler_inbox_backlog + v_result.retry_recovery_backlog;
    v_result.content_digest := sha256(convert_to(jsonb_build_object(
        'schema_version', 1,
        'id', v_result.id,
        'cutover_revision_id', v_result.cutover_revision_id,
        'observed_at', v_result.observed_at,
        'observed_by', v_result.observed_by,
        'nonterminal_jobs', v_result.nonterminal_jobs,
        'nonterminal_attempts', v_result.nonterminal_attempts,
        'active_execution_leases', v_result.active_execution_leases,
        'active_finalization_leases', v_result.active_finalization_leases,
        'active_artifact_uploads', v_result.active_artifact_uploads,
        'unpublished_outbox_events', v_result.unpublished_outbox_events,
        'retained_published_outbox_events', v_result.retained_published_outbox_events,
        'scheduler_inbox_backlog', v_result.scheduler_inbox_backlog,
        'retry_recovery_backlog', v_result.retry_recovery_backlog,
        'total_count', v_result.total_count
    )::text, 'UTF8'));
    INSERT INTO public.legacy_authority_inventory_snapshots VALUES (v_result.*)
    RETURNING * INTO v_result;
    RETURN v_result;
END
$$;

CREATE TRIGGER legacy_h3_contraction_readiness_receipts_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON legacy_h3_contraction_readiness_receipts
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_cutover_history_mutation();
CREATE TRIGGER legacy_h3_execution_archive_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON legacy_h3_execution_archive
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_cutover_history_mutation();

CREATE FUNCTION vela_guard_prepared_legacy_h3_row() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_authority public.execution_authority_kind;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_authority := OLD.execution_authority_kind;
    ELSE
        v_authority := NEW.execution_authority_kind;
    END IF;
    IF v_authority = 'LEGACY_WORKER'
       AND EXISTS (
           SELECT 1
           FROM public.legacy_h3_contraction_readiness_receipts
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_preparation_frozen',
            MESSAGE = 'Prepared Legacy H3 authority is immutable until release contraction';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;

CREATE FUNCTION vela_guard_prepared_legacy_h3_lease() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_attempt_id uuid;
BEGIN
    IF TG_OP = 'DELETE' THEN
        v_attempt_id := OLD.attempt_id;
    ELSE
        v_attempt_id := NEW.attempt_id;
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.attempts AS attempt
        CROSS JOIN public.legacy_h3_contraction_readiness_receipts AS receipt
        WHERE attempt.id = v_attempt_id
          AND attempt.execution_authority_kind = 'LEGACY_WORKER'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_preparation_frozen',
            MESSAGE = 'Prepared Legacy H3 authority is immutable until release contraction';
    END IF;
    IF TG_OP = 'DELETE' THEN
        RETURN OLD;
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER jobs_freeze_prepared_legacy_h3
BEFORE INSERT OR UPDATE OR DELETE ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_guard_prepared_legacy_h3_row();
CREATE TRIGGER attempts_freeze_prepared_legacy_h3
BEFORE INSERT OR UPDATE OR DELETE ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_guard_prepared_legacy_h3_row();
CREATE TRIGGER attempt_leases_freeze_prepared_legacy_h3
BEFORE INSERT OR UPDATE OR DELETE ON attempt_leases
FOR EACH ROW EXECUTE FUNCTION vela_guard_prepared_legacy_h3_lease();

CREATE FUNCTION vela_prepare_legacy_h3_contraction(
    p_zero_backlog_receipt_id uuid,
    p_prepared_by text
) RETURNS TABLE (
    zero_backlog_receipt_id uuid,
    cutover_revision_id uuid,
    prepared_at timestamptz,
    archive_digest bytea,
    content_digest bytea,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_control public.stage_cutover_control%ROWTYPE;
    v_zero_backlog public.stage_cutover_zero_backlog_receipts%ROWTYPE;
    v_existing public.legacy_h3_contraction_readiness_receipts%ROWTYPE;
    v_live_total bigint;
    v_prepared_at timestamptz;
    v_archive_digest bytea;
    v_content_digest bytea;
BEGIN
    IF p_zero_backlog_receipt_id IS NULL
       OR p_prepared_by IS NULL
       OR length(p_prepared_by) NOT BETWEEN 1 AND 300
       OR btrim(p_prepared_by) <> p_prepared_by
       OR p_prepared_by ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_h3_contraction_preparation_request_invalid',
            MESSAGE = 'Legacy H3 contraction preparation request is invalid';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_zero_backlog_receipt_id::text, 49053)
    );

    SELECT receipt.* INTO v_existing
    FROM public.legacy_h3_contraction_readiness_receipts AS receipt
    WHERE receipt.zero_backlog_receipt_id = p_zero_backlog_receipt_id;
    IF FOUND THEN
        IF v_existing.prepared_by IS DISTINCT FROM p_prepared_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'legacy_h3_contraction_preparation_replay_mismatch',
                MESSAGE = 'Legacy H3 contraction preparation replay changed';
        END IF;
        RETURN QUERY SELECT
            v_existing.zero_backlog_receipt_id,
            v_existing.cutover_revision_id,
            v_existing.prepared_at,
            v_existing.archive_digest,
            v_existing.content_digest,
            true;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.legacy_h3_contraction_readiness_receipts
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_preparation_already_completed',
            MESSAGE = 'Legacy H3 contraction was already prepared with another receipt';
    END IF;

    SELECT control.* INTO STRICT v_control
    FROM public.stage_cutover_control AS control
    WHERE control.singleton
    FOR UPDATE;
    SELECT receipt.* INTO v_zero_backlog
    FROM public.stage_cutover_zero_backlog_receipts AS receipt
    WHERE receipt.id = p_zero_backlog_receipt_id
      AND receipt.cutover_revision_id = v_control.current_revision_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_receipt_required',
            MESSAGE = 'Current sealed zero-backlog receipt is required for Legacy H3 contraction';
    END IF;

    SELECT public.vela_current_legacy_authority_inventory_total()
    INTO v_live_total;
    IF v_live_total <> 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_live_inventory_nonzero',
            MESSAGE = 'Live legacy authority inventory is not zero';
    END IF;

    v_prepared_at := clock_timestamp();
    INSERT INTO public.legacy_h3_execution_archive (
        zero_backlog_receipt_id, record_kind, record_id, parent_id,
        terminal_state, machine_authority, archived_at, content_digest
    )
    SELECT
        p_zero_backlog_receipt_id,
        source.record_kind,
        source.record_id,
        source.parent_id,
        source.terminal_state,
        source.machine_authority,
        v_prepared_at,
        sha256(convert_to(jsonb_build_object(
            'schema_version', 1,
            'zero_backlog_receipt_id', p_zero_backlog_receipt_id,
            'record_kind', source.record_kind,
            'record_id', source.record_id,
            'parent_id', source.parent_id,
            'terminal_state', source.terminal_state,
            'machine_authority', source.machine_authority,
            'archived_at', v_prepared_at
        )::text, 'UTF8'))
    FROM (
        SELECT
            'JOB'::text AS record_kind,
            job.id AS record_id,
            NULL::uuid AS parent_id,
            job.state::text AS terminal_state,
            jsonb_build_object('worker_pool_id', job.worker_pool_id)
                AS machine_authority
        FROM public.jobs AS job
        WHERE job.execution_authority_kind = 'LEGACY_WORKER'
          AND job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
        UNION ALL
        SELECT
            'ATTEMPT'::text,
            attempt.id,
            attempt.job_id,
            attempt.state::text,
            jsonb_build_object(
                'attempt_number', attempt.attempt_number,
                'execution_profile_revision_id',
                    attempt.execution_profile_revision_id,
                'worker_pool_id', attempt.worker_pool_id,
                'worker_id', attempt.worker_id,
                'worker_epoch', attempt.worker_epoch,
                'fence', attempt.fence
            )
        FROM public.attempts AS attempt
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
          AND attempt.state IN ('SUCCEEDED', 'FAILED', 'LOST', 'CANCELED')
        UNION ALL
        SELECT
            'LEASE'::text,
            lease.id,
            lease.attempt_id,
            CASE
                WHEN lease.revoked_at IS NOT NULL THEN 'REVOKED'
                WHEN lease.expires_at <= v_prepared_at THEN 'EXPIRED'
                ELSE 'INACTIVE'
            END,
            jsonb_build_object(
                'organization_id', lease.organization_id,
                'project_id', lease.project_id,
                'worker_id', lease.worker_id,
                'worker_epoch', lease.worker_epoch,
                'fence', lease.fence,
                'phase', lease.phase,
                'owner_kind', lease.owner_kind,
                'owner_id', lease.owner_id,
                'token_digest', lease.token_digest,
                'signing_key_id', lease.signing_key_id,
                'issued_at', lease.issued_at,
                'expires_at', lease.expires_at,
                'token_claim_expires_at', lease.token_claim_expires_at,
                'renewal_protocol_version', lease.renewal_protocol_version,
                'revoked_at', lease.revoked_at
            )
        FROM public.attempt_leases AS lease
        JOIN public.attempts AS attempt ON attempt.id = lease.attempt_id
        WHERE attempt.execution_authority_kind = 'LEGACY_WORKER'
    ) AS source;

    SELECT sha256(convert_to(COALESCE(string_agg(
        archive.record_kind || ':' || archive.record_id::text || ':'
            || encode(archive.content_digest, 'hex'),
        E'\n' ORDER BY archive.record_kind, archive.record_id
    ), ''), 'UTF8'))
    INTO v_archive_digest
    FROM public.legacy_h3_execution_archive AS archive
    WHERE archive.zero_backlog_receipt_id = p_zero_backlog_receipt_id;

    v_content_digest := sha256(convert_to(jsonb_build_object(
        'schema_version', 1,
        'zero_backlog_receipt_id', p_zero_backlog_receipt_id,
        'cutover_revision_id', v_zero_backlog.cutover_revision_id,
        'prepared_at', v_prepared_at,
        'prepared_by', p_prepared_by,
        'archive_digest', encode(v_archive_digest, 'hex')
    )::text, 'UTF8'));
    INSERT INTO public.legacy_h3_contraction_readiness_receipts (
        zero_backlog_receipt_id, cutover_revision_id, prepared_at,
        prepared_by, archive_digest, content_digest
    ) VALUES (
        p_zero_backlog_receipt_id, v_zero_backlog.cutover_revision_id,
        v_prepared_at, p_prepared_by, v_archive_digest, v_content_digest
    ) RETURNING * INTO v_existing;

    RETURN QUERY SELECT
        v_existing.zero_backlog_receipt_id,
        v_existing.cutover_revision_id,
        v_existing.prepared_at,
        v_existing.archive_digest,
        v_existing.content_digest,
        false;
END
$$;

ALTER TABLE legacy_h3_contraction_readiness_receipts
    OWNER TO vela_catalog_promotion_owner;
ALTER TABLE legacy_h3_execution_archive
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_prepare_legacy_h3_contraction(uuid, text)
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_guard_prepared_legacy_h3_row()
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_guard_prepared_legacy_h3_lease()
    OWNER TO vela_catalog_promotion_owner;

REVOKE ALL ON TABLE legacy_h3_contraction_readiness_receipts FROM PUBLIC;
REVOKE ALL ON TABLE legacy_h3_execution_archive FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_prepare_legacy_h3_contraction(uuid, text),
    vela_guard_prepared_legacy_h3_row(),
    vela_guard_prepared_legacy_h3_lease()
FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_prepare_legacy_h3_contraction(uuid, text)
TO vela_catalog_promotion;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE legacy_h3_contraction_readiness_receipts, legacy_h3_execution_archive
IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM legacy_h3_contraction_readiness_receipts)
       OR EXISTS (SELECT 1 FROM legacy_h3_execution_archive) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_rollback_is_unsafe',
            MESSAGE = 'Legacy H3 contraction readiness receipt prevents rollback';
    END IF;
END
$$;
REVOKE EXECUTE ON FUNCTION vela_prepare_legacy_h3_contraction(uuid, text)
FROM vela_catalog_promotion;
DROP FUNCTION vela_prepare_legacy_h3_contraction(uuid, text);
DROP TRIGGER attempt_leases_freeze_prepared_legacy_h3 ON attempt_leases;
DROP TRIGGER attempts_freeze_prepared_legacy_h3 ON attempts;
DROP TRIGGER jobs_freeze_prepared_legacy_h3 ON jobs;
DROP FUNCTION vela_guard_prepared_legacy_h3_lease();
DROP FUNCTION vela_guard_prepared_legacy_h3_row();
DO $$
DECLARE
    v_restore record;
BEGIN
    FOR v_restore IN
        SELECT definition
        FROM legacy_h3_contraction_v52_function_restore
        ORDER BY signature
    LOOP
        EXECUTE v_restore.definition;
    END LOOP;
END
$$;
DROP TRIGGER legacy_h3_contraction_readiness_receipts_immutable
    ON legacy_h3_contraction_readiness_receipts;
DROP TRIGGER legacy_h3_execution_archive_immutable
    ON legacy_h3_execution_archive;
DROP TABLE legacy_h3_contraction_readiness_receipts;
DROP TABLE legacy_h3_execution_archive;
DROP TABLE legacy_h3_contraction_v52_function_restore;
-- +goose StatementEnd
