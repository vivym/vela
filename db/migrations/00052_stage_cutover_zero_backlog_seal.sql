-- +goose Up
-- +goose StatementBegin
CREATE TABLE stage_cutover_external_drain_evidence (
    id uuid PRIMARY KEY,
    cutover_revision_id uuid NOT NULL REFERENCES stage_cutover_revisions(id),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    observed_by text NOT NULL CHECK (
        length(observed_by) BETWEEN 1 AND 300
        AND btrim(observed_by) = observed_by
        AND observed_by !~ '[[:cntrl:]]'
    ),
    release_digest bytea NOT NULL CHECK (octet_length(release_digest) = 32),
    configuration_revision text NOT NULL CHECK (
        length(configuration_revision) BETWEEN 1 AND 300
        AND btrim(configuration_revision) = configuration_revision
        AND configuration_revision !~ '[[:cntrl:]]'
    ),
    configuration_digest bytea NOT NULL CHECK (
        octet_length(configuration_digest) = 32
    ),
    execution_graph_revision_id uuid NOT NULL,
    execution_profile_revision_id uuid NOT NULL,
    connector_set_digest bytea NOT NULL CHECK (
        octet_length(connector_set_digest) = 32
    ),
    launch_manifest_digest bytea NOT NULL REFERENCES
        production_gate_manifests(manifest_digest),
    worker_local_recovery_backlog bigint NOT NULL CHECK (
        worker_local_recovery_backlog >= 0
    ),
    nminusone_deployment_backlog bigint NOT NULL CHECK (
        nminusone_deployment_backlog >= 0
    ),
    external_scheduler_backlog bigint NOT NULL CHECK (
        external_scheduler_backlog >= 0
    ),
    external_event_backlog bigint NOT NULL CHECK (
        external_event_backlog >= 0
    ),
    external_artifact_backlog bigint NOT NULL CHECK (
        external_artifact_backlog >= 0
    ),
    total_count bigint NOT NULL CHECK (total_count >= 0),
    evidence_manifest_digest bytea NOT NULL CHECK (
        octet_length(evidence_manifest_digest) = 32
    ),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    UNIQUE (id, cutover_revision_id),
    FOREIGN KEY (
        cutover_revision_id,
        execution_graph_revision_id,
        execution_profile_revision_id
    ) REFERENCES stage_cutover_revisions (
        id,
        execution_graph_revision_id,
        execution_profile_revision_id
    ),
    CHECK (
        total_count = worker_local_recovery_backlog
            + nminusone_deployment_backlog
            + external_scheduler_backlog
            + external_event_backlog
            + external_artifact_backlog
    )
);

CREATE TABLE stage_cutover_zero_backlog_receipts (
    id uuid PRIMARY KEY,
    cutover_revision_id uuid NOT NULL UNIQUE REFERENCES stage_cutover_revisions(id),
    start_inventory_snapshot_id uuid NOT NULL UNIQUE REFERENCES
        legacy_authority_inventory_snapshots(id),
    end_inventory_snapshot_id uuid NOT NULL UNIQUE REFERENCES
        legacy_authority_inventory_snapshots(id),
    start_external_evidence_id uuid NOT NULL UNIQUE,
    end_external_evidence_id uuid NOT NULL UNIQUE,
    window_started_at timestamptz NOT NULL,
    window_ended_at timestamptz NOT NULL,
    sealed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    sealed_by text NOT NULL CHECK (
        length(sealed_by) BETWEEN 1 AND 300
        AND btrim(sealed_by) = sealed_by
        AND sealed_by !~ '[[:cntrl:]]'
    ),
    release_digest bytea NOT NULL CHECK (octet_length(release_digest) = 32),
    configuration_revision text NOT NULL CHECK (
        length(configuration_revision) BETWEEN 1 AND 300
        AND btrim(configuration_revision) = configuration_revision
        AND configuration_revision !~ '[[:cntrl:]]'
    ),
    configuration_digest bytea NOT NULL CHECK (
        octet_length(configuration_digest) = 32
    ),
    execution_graph_revision_id uuid NOT NULL,
    execution_profile_revision_id uuid NOT NULL,
    connector_set_digest bytea NOT NULL CHECK (
        octet_length(connector_set_digest) = 32
    ),
    launch_manifest_digest bytea NOT NULL REFERENCES
        production_gate_manifests(manifest_digest),
    start_inventory_digest bytea NOT NULL CHECK (
        octet_length(start_inventory_digest) = 32
    ),
    end_inventory_digest bytea NOT NULL CHECK (
        octet_length(end_inventory_digest) = 32
    ),
    start_external_evidence_digest bytea NOT NULL CHECK (
        octet_length(start_external_evidence_digest) = 32
    ),
    end_external_evidence_digest bytea NOT NULL CHECK (
        octet_length(end_external_evidence_digest) = 32
    ),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    FOREIGN KEY (start_external_evidence_id, cutover_revision_id)
        REFERENCES stage_cutover_external_drain_evidence(id, cutover_revision_id),
    FOREIGN KEY (end_external_evidence_id, cutover_revision_id)
        REFERENCES stage_cutover_external_drain_evidence(id, cutover_revision_id),
    FOREIGN KEY (
        cutover_revision_id,
        execution_graph_revision_id,
        execution_profile_revision_id
    ) REFERENCES stage_cutover_revisions (
        id,
        execution_graph_revision_id,
        execution_profile_revision_id
    ),
    CHECK (start_inventory_snapshot_id <> end_inventory_snapshot_id),
    CHECK (start_external_evidence_id <> end_external_evidence_id),
    CHECK (window_ended_at > window_started_at),
    CHECK (sealed_at >= window_ended_at)
);

CREATE TRIGGER stage_cutover_external_drain_evidence_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON stage_cutover_external_drain_evidence
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_cutover_history_mutation();
CREATE TRIGGER stage_cutover_zero_backlog_receipts_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON stage_cutover_zero_backlog_receipts
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_cutover_history_mutation();

CREATE FUNCTION vela_current_legacy_authority_inventory_total() RETURNS bigint
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
              AND attempt.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')
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

CREATE FUNCTION vela_record_stage_cutover_external_drain_evidence(
    p_evidence_id uuid,
    p_worker_local_recovery_backlog bigint,
    p_nminusone_deployment_backlog bigint,
    p_external_scheduler_backlog bigint,
    p_external_event_backlog bigint,
    p_external_artifact_backlog bigint,
    p_evidence_manifest_digest bytea,
    p_observed_by text
) RETURNS TABLE (
    evidence_id uuid,
    total_count bigint,
    content_digest bytea,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_control public.stage_cutover_control%ROWTYPE;
    v_revision public.stage_cutover_revisions%ROWTYPE;
    v_existing public.stage_cutover_external_drain_evidence%ROWTYPE;
    v_result public.stage_cutover_external_drain_evidence%ROWTYPE;
BEGIN
    IF p_evidence_id IS NULL
       OR p_worker_local_recovery_backlog IS NULL
       OR p_worker_local_recovery_backlog < 0
       OR p_nminusone_deployment_backlog IS NULL
       OR p_nminusone_deployment_backlog < 0
       OR p_external_scheduler_backlog IS NULL
       OR p_external_scheduler_backlog < 0
       OR p_external_event_backlog IS NULL
       OR p_external_event_backlog < 0
       OR p_external_artifact_backlog IS NULL
       OR p_external_artifact_backlog < 0
       OR p_evidence_manifest_digest IS NULL
       OR octet_length(p_evidence_manifest_digest) <> 32
       OR p_observed_by IS NULL OR length(p_observed_by) NOT BETWEEN 1 AND 300
       OR btrim(p_observed_by) <> p_observed_by
       OR p_observed_by ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_cutover_external_drain_evidence_invalid',
            MESSAGE = 'Stage cutover external drain evidence is invalid';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_evidence_id::text, 49052)
    );

    SELECT evidence.* INTO v_existing
    FROM public.stage_cutover_external_drain_evidence AS evidence
    WHERE evidence.id = p_evidence_id;
    IF FOUND THEN
        IF v_existing.worker_local_recovery_backlog IS DISTINCT FROM
                p_worker_local_recovery_backlog
           OR v_existing.nminusone_deployment_backlog IS DISTINCT FROM
                p_nminusone_deployment_backlog
           OR v_existing.external_scheduler_backlog IS DISTINCT FROM
                p_external_scheduler_backlog
           OR v_existing.external_event_backlog IS DISTINCT FROM
                p_external_event_backlog
           OR v_existing.external_artifact_backlog IS DISTINCT FROM
                p_external_artifact_backlog
           OR v_existing.evidence_manifest_digest IS DISTINCT FROM
                p_evidence_manifest_digest
           OR v_existing.observed_by IS DISTINCT FROM p_observed_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_cutover_external_drain_evidence_replay_mismatch',
                MESSAGE = 'Stage cutover external drain evidence replay changed';
        END IF;
        RETURN QUERY SELECT
            v_existing.id,
            v_existing.total_count,
            v_existing.content_digest,
            true;
        RETURN;
    END IF;

    SELECT control.* INTO STRICT v_control
    FROM public.stage_cutover_control AS control
    WHERE control.singleton
    FOR SHARE;
    SELECT revision.* INTO STRICT v_revision
    FROM public.stage_cutover_revisions AS revision
    WHERE revision.id = v_control.current_revision_id
    FOR SHARE;
    IF v_revision.scope <> 'PRODUCTION'
       OR v_revision.mode <> 'STAGE_ONLY'
       OR v_revision.stage_cohort_basis_points <> 10000
       OR v_revision.execution_graph_revision_id IS NULL
       OR v_revision.execution_profile_revision_id IS NULL
       OR v_revision.launch_manifest_digest IS NULL
       OR NOT EXISTS (
           SELECT 1
           FROM public.production_gate_manifests AS manifest
           WHERE manifest.manifest_digest = v_revision.launch_manifest_digest
             AND manifest.release_digest = v_revision.release_digest
             AND manifest.configuration_revision = v_revision.configuration_revision
             AND manifest.sealed_at IS NOT NULL
             AND manifest.receipt_count = 9
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_external_drain_requires_production_stage_only',
            MESSAGE = 'External drain evidence requires current evidenced production Stage-only routing';
    END IF;

    v_result.id := p_evidence_id;
    v_result.cutover_revision_id := v_revision.id;
    v_result.observed_at := clock_timestamp();
    v_result.observed_by := p_observed_by;
    v_result.release_digest := v_revision.release_digest;
    v_result.configuration_revision := v_revision.configuration_revision;
    v_result.configuration_digest := v_revision.configuration_digest;
    v_result.execution_graph_revision_id := v_revision.execution_graph_revision_id;
    v_result.execution_profile_revision_id := v_revision.execution_profile_revision_id;
    v_result.connector_set_digest := v_revision.connector_set_digest;
    v_result.launch_manifest_digest := v_revision.launch_manifest_digest;
    v_result.worker_local_recovery_backlog := p_worker_local_recovery_backlog;
    v_result.nminusone_deployment_backlog := p_nminusone_deployment_backlog;
    v_result.external_scheduler_backlog := p_external_scheduler_backlog;
    v_result.external_event_backlog := p_external_event_backlog;
    v_result.external_artifact_backlog := p_external_artifact_backlog;
    v_result.total_count := p_worker_local_recovery_backlog
        + p_nminusone_deployment_backlog
        + p_external_scheduler_backlog
        + p_external_event_backlog
        + p_external_artifact_backlog;
    v_result.evidence_manifest_digest := p_evidence_manifest_digest;
    v_result.content_digest := sha256(convert_to(jsonb_build_object(
        'schema_version', 1,
        'id', v_result.id,
        'cutover_revision_id', v_result.cutover_revision_id,
        'observed_at', v_result.observed_at,
        'observed_by', v_result.observed_by,
        'release_digest', encode(v_result.release_digest, 'hex'),
        'configuration_revision', v_result.configuration_revision,
        'configuration_digest', encode(v_result.configuration_digest, 'hex'),
        'execution_graph_revision_id', v_result.execution_graph_revision_id,
        'execution_profile_revision_id', v_result.execution_profile_revision_id,
        'connector_set_digest', encode(v_result.connector_set_digest, 'hex'),
        'launch_manifest_digest', encode(v_result.launch_manifest_digest, 'hex'),
        'worker_local_recovery_backlog', v_result.worker_local_recovery_backlog,
        'nminusone_deployment_backlog', v_result.nminusone_deployment_backlog,
        'external_scheduler_backlog', v_result.external_scheduler_backlog,
        'external_event_backlog', v_result.external_event_backlog,
        'external_artifact_backlog', v_result.external_artifact_backlog,
        'total_count', v_result.total_count,
        'evidence_manifest_digest', encode(v_result.evidence_manifest_digest, 'hex')
    )::text, 'UTF8'));
    INSERT INTO public.stage_cutover_external_drain_evidence VALUES (v_result.*)
    RETURNING * INTO v_result;
    RETURN QUERY SELECT
        v_result.id,
        v_result.total_count,
        v_result.content_digest,
        false;
END
$$;

CREATE FUNCTION vela_seal_stage_cutover_zero_backlog(
    p_receipt_id uuid,
    p_start_inventory_snapshot_id uuid,
    p_end_inventory_snapshot_id uuid,
    p_start_external_evidence_id uuid,
    p_end_external_evidence_id uuid,
    p_sealed_by text
) RETURNS TABLE (
    receipt_id uuid,
    window_started_at timestamptz,
    window_ended_at timestamptz,
    content_digest bytea,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_control public.stage_cutover_control%ROWTYPE;
    v_revision public.stage_cutover_revisions%ROWTYPE;
    v_start_inventory public.legacy_authority_inventory_snapshots%ROWTYPE;
    v_end_inventory public.legacy_authority_inventory_snapshots%ROWTYPE;
    v_start_external public.stage_cutover_external_drain_evidence%ROWTYPE;
    v_end_external public.stage_cutover_external_drain_evidence%ROWTYPE;
    v_existing public.stage_cutover_zero_backlog_receipts%ROWTYPE;
    v_result public.stage_cutover_zero_backlog_receipts%ROWTYPE;
    v_live_total bigint;
BEGIN
    IF p_receipt_id IS NULL
       OR p_start_inventory_snapshot_id IS NULL
       OR p_end_inventory_snapshot_id IS NULL
       OR p_start_external_evidence_id IS NULL
       OR p_end_external_evidence_id IS NULL
       OR p_start_inventory_snapshot_id = p_end_inventory_snapshot_id
       OR p_start_external_evidence_id = p_end_external_evidence_id
       OR p_sealed_by IS NULL OR length(p_sealed_by) NOT BETWEEN 1 AND 300
       OR btrim(p_sealed_by) <> p_sealed_by
       OR p_sealed_by ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_cutover_zero_backlog_request_invalid',
            MESSAGE = 'Stage cutover zero-backlog seal request is invalid';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_receipt_id::text, 49052)
    );

    SELECT receipt.* INTO v_existing
    FROM public.stage_cutover_zero_backlog_receipts AS receipt
    WHERE receipt.id = p_receipt_id;
    IF FOUND THEN
        IF v_existing.start_inventory_snapshot_id <>
                p_start_inventory_snapshot_id
           OR v_existing.end_inventory_snapshot_id <>
                p_end_inventory_snapshot_id
           OR v_existing.start_external_evidence_id <>
                p_start_external_evidence_id
           OR v_existing.end_external_evidence_id <>
                p_end_external_evidence_id
           OR v_existing.sealed_by <> p_sealed_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_cutover_zero_backlog_replay_mismatch',
                MESSAGE = 'Stage cutover zero-backlog seal replay changed';
        END IF;
        RETURN QUERY SELECT
            v_existing.id,
            v_existing.window_started_at,
            v_existing.window_ended_at,
            v_existing.content_digest,
            true;
        RETURN;
    END IF;

    SELECT control.* INTO STRICT v_control
    FROM public.stage_cutover_control AS control
    WHERE control.singleton
    FOR UPDATE;
    SELECT revision.* INTO STRICT v_revision
    FROM public.stage_cutover_revisions AS revision
    WHERE revision.id = v_control.current_revision_id
    FOR SHARE;
    IF v_revision.scope <> 'PRODUCTION'
       OR v_revision.mode <> 'STAGE_ONLY'
       OR v_revision.stage_cohort_basis_points <> 10000
       OR v_revision.execution_graph_revision_id IS NULL
       OR v_revision.execution_profile_revision_id IS NULL
       OR v_revision.launch_manifest_digest IS NULL
       OR NOT EXISTS (
           SELECT 1
           FROM public.production_gate_manifests AS manifest
           WHERE manifest.manifest_digest = v_revision.launch_manifest_digest
             AND manifest.release_digest = v_revision.release_digest
             AND manifest.configuration_revision = v_revision.configuration_revision
             AND manifest.sealed_at IS NOT NULL
             AND manifest.receipt_count = 9
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_requires_production_stage_only',
            MESSAGE = 'Zero-backlog seal requires current evidenced production Stage-only routing';
    END IF;

    SELECT snapshot.* INTO v_start_inventory
    FROM public.legacy_authority_inventory_snapshots AS snapshot
    WHERE snapshot.id = p_start_inventory_snapshot_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_inventory_missing',
            MESSAGE = 'Start legacy authority inventory is missing';
    END IF;
    SELECT snapshot.* INTO v_end_inventory
    FROM public.legacy_authority_inventory_snapshots AS snapshot
    WHERE snapshot.id = p_end_inventory_snapshot_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_inventory_missing',
            MESSAGE = 'End legacy authority inventory is missing';
    END IF;
    SELECT evidence.* INTO v_start_external
    FROM public.stage_cutover_external_drain_evidence AS evidence
    WHERE evidence.id = p_start_external_evidence_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_external_missing',
            MESSAGE = 'Start external drain evidence is missing';
    END IF;
    SELECT evidence.* INTO v_end_external
    FROM public.stage_cutover_external_drain_evidence AS evidence
    WHERE evidence.id = p_end_external_evidence_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_external_missing',
            MESSAGE = 'End external drain evidence is missing';
    END IF;

    IF v_start_inventory.cutover_revision_id <> v_revision.id
       OR v_end_inventory.cutover_revision_id <> v_revision.id
       OR v_start_external.cutover_revision_id <> v_revision.id
       OR v_end_external.cutover_revision_id <> v_revision.id THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_revision_mismatch',
            MESSAGE = 'Zero-backlog evidence does not bind the current cutover revision';
    END IF;
    IF v_start_inventory.total_count <> 0 OR v_end_inventory.total_count <> 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_inventory_nonzero',
            MESSAGE = 'Legacy database authority inventory is not zero';
    END IF;
    IF v_start_external.total_count <> 0 OR v_end_external.total_count <> 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_external_nonzero',
            MESSAGE = 'External Stage cutover drain evidence is not zero';
    END IF;

    v_result.window_started_at := GREATEST(
        v_start_inventory.observed_at,
        v_start_external.observed_at
    );
    v_result.window_ended_at := LEAST(
        v_end_inventory.observed_at,
        v_end_external.observed_at
    );
    IF v_result.window_started_at < v_revision.activated_at
       OR v_result.window_ended_at <= v_result.window_started_at
       OR EXTRACT(EPOCH FROM (
           v_result.window_ended_at - v_result.window_started_at
       )) < v_revision.minimum_observation_seconds THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_window_too_short',
            MESSAGE = 'Zero-backlog observation window is shorter than the cutover policy';
    END IF;

    SELECT public.vela_current_legacy_authority_inventory_total()
    INTO v_live_total;
    IF v_live_total <> 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_live_inventory_nonzero',
            MESSAGE = 'Live legacy authority inventory changed after the evidence window';
    END IF;

    v_result.id := p_receipt_id;
    v_result.cutover_revision_id := v_revision.id;
    v_result.start_inventory_snapshot_id := v_start_inventory.id;
    v_result.end_inventory_snapshot_id := v_end_inventory.id;
    v_result.start_external_evidence_id := v_start_external.id;
    v_result.end_external_evidence_id := v_end_external.id;
    v_result.sealed_at := clock_timestamp();
    v_result.sealed_by := p_sealed_by;
    v_result.release_digest := v_revision.release_digest;
    v_result.configuration_revision := v_revision.configuration_revision;
    v_result.configuration_digest := v_revision.configuration_digest;
    v_result.execution_graph_revision_id := v_revision.execution_graph_revision_id;
    v_result.execution_profile_revision_id := v_revision.execution_profile_revision_id;
    v_result.connector_set_digest := v_revision.connector_set_digest;
    v_result.launch_manifest_digest := v_revision.launch_manifest_digest;
    v_result.start_inventory_digest := v_start_inventory.content_digest;
    v_result.end_inventory_digest := v_end_inventory.content_digest;
    v_result.start_external_evidence_digest := v_start_external.content_digest;
    v_result.end_external_evidence_digest := v_end_external.content_digest;
    v_result.content_digest := sha256(convert_to(jsonb_build_object(
        'schema_version', 1,
        'id', v_result.id,
        'cutover_revision_id', v_result.cutover_revision_id,
        'start_inventory_snapshot_id', v_result.start_inventory_snapshot_id,
        'end_inventory_snapshot_id', v_result.end_inventory_snapshot_id,
        'start_external_evidence_id', v_result.start_external_evidence_id,
        'end_external_evidence_id', v_result.end_external_evidence_id,
        'window_started_at', v_result.window_started_at,
        'window_ended_at', v_result.window_ended_at,
        'sealed_at', v_result.sealed_at,
        'sealed_by', v_result.sealed_by,
        'release_digest', encode(v_result.release_digest, 'hex'),
        'configuration_revision', v_result.configuration_revision,
        'configuration_digest', encode(v_result.configuration_digest, 'hex'),
        'execution_graph_revision_id', v_result.execution_graph_revision_id,
        'execution_profile_revision_id', v_result.execution_profile_revision_id,
        'connector_set_digest', encode(v_result.connector_set_digest, 'hex'),
        'launch_manifest_digest', encode(v_result.launch_manifest_digest, 'hex'),
        'start_inventory_digest', encode(v_result.start_inventory_digest, 'hex'),
        'end_inventory_digest', encode(v_result.end_inventory_digest, 'hex'),
        'start_external_evidence_digest',
            encode(v_result.start_external_evidence_digest, 'hex'),
        'end_external_evidence_digest',
            encode(v_result.end_external_evidence_digest, 'hex')
    )::text, 'UTF8'));
    INSERT INTO public.stage_cutover_zero_backlog_receipts VALUES (v_result.*)
    RETURNING * INTO v_result;
    RETURN QUERY SELECT
        v_result.id,
        v_result.window_started_at,
        v_result.window_ended_at,
        v_result.content_digest,
        false;
END
$$;

CREATE FUNCTION vela_guard_sealed_stage_cutover_control() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM public.stage_cutover_zero_backlog_receipts AS receipt
        WHERE receipt.cutover_revision_id = OLD.current_revision_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_sealed',
            MESSAGE = 'Sealed zero-backlog authority forbids further cutover mutation';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER stage_cutover_control_zero_backlog_guard
BEFORE UPDATE ON stage_cutover_control
FOR EACH ROW EXECUTE FUNCTION vela_guard_sealed_stage_cutover_control();

CREATE FUNCTION vela_guard_sealed_legacy_job_authority() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_current_revision_id uuid;
BEGIN
    SELECT control.current_revision_id INTO STRICT v_current_revision_id
    FROM public.stage_cutover_control AS control
    WHERE control.singleton
    FOR SHARE;
    IF NEW.execution_authority_kind = 'LEGACY_WORKER'
       AND EXISTS (
           SELECT 1
           FROM public.stage_cutover_zero_backlog_receipts AS receipt
           WHERE receipt.cutover_revision_id = v_current_revision_id
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_legacy_authority_sealed',
            MESSAGE = 'Sealed zero-backlog authority forbids new legacy Jobs';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER jobs_zero_backlog_legacy_authority_guard
BEFORE INSERT OR UPDATE OF execution_authority_kind ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_guard_sealed_legacy_job_authority();

ALTER TABLE stage_cutover_external_drain_evidence
    OWNER TO vela_catalog_promotion_owner;
ALTER TABLE stage_cutover_zero_backlog_receipts
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_current_legacy_authority_inventory_total()
    OWNER TO vela_internal;
ALTER FUNCTION vela_record_stage_cutover_external_drain_evidence(
    uuid, bigint, bigint, bigint, bigint, bigint, bytea, text
) OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_seal_stage_cutover_zero_backlog(
    uuid, uuid, uuid, uuid, uuid, text
) OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_guard_sealed_stage_cutover_control()
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_guard_sealed_legacy_job_authority()
    OWNER TO vela_catalog_promotion_owner;

REVOKE ALL ON stage_cutover_external_drain_evidence,
    stage_cutover_zero_backlog_receipts
FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_current_legacy_authority_inventory_total(),
    vela_record_stage_cutover_external_drain_evidence(
        uuid, bigint, bigint, bigint, bigint, bigint, bytea, text
    ),
    vela_seal_stage_cutover_zero_backlog(uuid, uuid, uuid, uuid, uuid, text),
    vela_guard_sealed_stage_cutover_control(),
    vela_guard_sealed_legacy_job_authority()
FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_current_legacy_authority_inventory_total()
TO vela_catalog_promotion_owner;
GRANT EXECUTE ON FUNCTION vela_record_stage_cutover_external_drain_evidence(
    uuid, bigint, bigint, bigint, bigint, bigint, bytea, text
), vela_seal_stage_cutover_zero_backlog(uuid, uuid, uuid, uuid, uuid, text)
TO vela_catalog_promotion;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE stage_cutover_external_drain_evidence,
    stage_cutover_zero_backlog_receipts
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_cutover_external_drain_evidence)
       OR EXISTS (SELECT 1 FROM stage_cutover_zero_backlog_receipts) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_zero_backlog_rollback_is_unsafe',
            MESSAGE = 'Stage cutover external drain evidence prevents rollback';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_record_stage_cutover_external_drain_evidence(
    uuid, bigint, bigint, bigint, bigint, bigint, bytea, text
), vela_seal_stage_cutover_zero_backlog(uuid, uuid, uuid, uuid, uuid, text)
FROM vela_catalog_promotion;
REVOKE EXECUTE ON FUNCTION vela_current_legacy_authority_inventory_total()
FROM vela_catalog_promotion_owner;
DROP TRIGGER jobs_zero_backlog_legacy_authority_guard ON jobs;
DROP FUNCTION vela_guard_sealed_legacy_job_authority();
DROP TRIGGER stage_cutover_control_zero_backlog_guard
    ON stage_cutover_control;
DROP FUNCTION vela_guard_sealed_stage_cutover_control();
DROP FUNCTION vela_seal_stage_cutover_zero_backlog(
    uuid, uuid, uuid, uuid, uuid, text
);
DROP FUNCTION vela_record_stage_cutover_external_drain_evidence(
    uuid, bigint, bigint, bigint, bigint, bigint, bytea, text
);
DROP FUNCTION vela_current_legacy_authority_inventory_total();
DROP TRIGGER stage_cutover_zero_backlog_receipts_immutable
    ON stage_cutover_zero_backlog_receipts;
DROP TRIGGER stage_cutover_external_drain_evidence_immutable
    ON stage_cutover_external_drain_evidence;
DROP TABLE stage_cutover_zero_backlog_receipts;
DROP TABLE stage_cutover_external_drain_evidence;
-- +goose StatementEnd
