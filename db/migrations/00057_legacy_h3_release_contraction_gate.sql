-- +goose Up
-- +goose StatementBegin
CREATE TABLE legacy_h3_contraction_authorizations (
    zero_backlog_receipt_id uuid PRIMARY KEY REFERENCES
        legacy_h3_contraction_readiness_receipts(zero_backlog_receipt_id),
    cutover_revision_id uuid NOT NULL UNIQUE REFERENCES stage_cutover_revisions(id),
    launch_manifest_digest bytea NOT NULL REFERENCES
        production_gate_manifests(manifest_digest),
    release_digest bytea NOT NULL CHECK (octet_length(release_digest) = 32),
    configuration_revision text NOT NULL CHECK (
        length(configuration_revision) BETWEEN 1 AND 300
        AND btrim(configuration_revision) = configuration_revision
        AND configuration_revision !~ '[[:cntrl:]]'
    ),
    source_revision text NOT NULL CHECK (
        source_revision ~ '^([0-9a-f]{40}|[0-9a-f]{64})$'
    ),
    reachability_evidence_digest bytea NOT NULL CHECK (
        octet_length(reachability_evidence_digest) = 32
    ),
    authorized_at timestamptz NOT NULL,
    authorized_by text NOT NULL CHECK (
        length(authorized_by) BETWEEN 1 AND 300
        AND btrim(authorized_by) = authorized_by
        AND authorized_by !~ '[[:cntrl:]]'
    ),
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32)
);

CREATE TRIGGER legacy_h3_contraction_authorizations_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON legacy_h3_contraction_authorizations
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_cutover_history_mutation();

CREATE FUNCTION vela_authorize_legacy_h3_contraction(
    p_zero_backlog_receipt_id uuid,
    p_launch_manifest_digest bytea,
    p_release_digest bytea,
    p_configuration_revision text,
    p_configuration_manifest bytea,
    p_reachability_evidence bytea,
    p_authorized_by text
) RETURNS TABLE (
    zero_backlog_receipt_id uuid,
    cutover_revision_id uuid,
    launch_manifest_digest bytea,
    release_digest bytea,
    configuration_revision text,
    source_revision text,
    reachability_evidence_digest bytea,
    authorized_at timestamptz,
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
    v_readiness public.legacy_h3_contraction_readiness_receipts%ROWTYPE;
    v_manifest public.production_gate_manifests%ROWTYPE;
    v_existing public.legacy_h3_contraction_authorizations%ROWTYPE;
    v_receipt_count integer;
    v_evidence_key_count integer;
    v_live_total bigint;
    v_configuration jsonb;
    v_evidence jsonb;
    v_expected_checks jsonb;
    v_reachability_evidence_digest bytea;
    v_source_revision text;
    v_authorized_at timestamptz;
    v_content_digest bytea;
BEGIN
    IF p_zero_backlog_receipt_id IS NULL
       OR p_launch_manifest_digest IS NULL
       OR octet_length(p_launch_manifest_digest) <> 32
       OR p_release_digest IS NULL
       OR octet_length(p_release_digest) <> 32
       OR p_configuration_revision IS NULL
       OR length(p_configuration_revision) NOT BETWEEN 1 AND 300
       OR btrim(p_configuration_revision) <> p_configuration_revision
       OR p_configuration_revision ~ '[[:cntrl:]]'
       OR p_configuration_revision !~ '^sha256:[0-9a-f]{64}$'
       OR p_configuration_manifest IS NULL
       OR octet_length(p_configuration_manifest) NOT BETWEEN 1 AND 4194304
       OR p_reachability_evidence IS NULL
       OR octet_length(p_reachability_evidence) NOT BETWEEN 1 AND 1048576
       OR p_authorized_by IS NULL
       OR length(p_authorized_by) NOT BETWEEN 1 AND 300
       OR btrim(p_authorized_by) <> p_authorized_by
       OR p_authorized_by ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_h3_contraction_authorization_request_invalid',
            MESSAGE = 'Legacy H3 contraction authorization request is invalid';
    END IF;

    BEGIN
        v_configuration := convert_from(p_configuration_manifest, 'UTF8')::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_h3_contraction_configuration_manifest_invalid',
            MESSAGE = 'Legacy H3 contraction configuration manifest is invalid';
    END;
    v_source_revision := v_configuration ->> 'source_revision';
    IF jsonb_typeof(v_configuration) <> 'object'
       OR v_configuration -> 'schema_version' IS DISTINCT FROM '2'::jsonb
       OR v_source_revision IS NULL
       OR v_source_revision !~ '^([0-9a-f]{40}|[0-9a-f]{64})$'
       OR p_configuration_revision <>
            'sha256:' || encode(sha256(p_configuration_manifest), 'hex') THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_h3_contraction_configuration_manifest_invalid',
            MESSAGE = 'Configuration manifest does not bind schema v2 and its source revision';
    END IF;

    BEGIN
        v_evidence := convert_from(p_reachability_evidence, 'UTF8')::jsonb;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_h3_contraction_reachability_evidence_invalid',
            MESSAGE = 'Legacy H3 reachability evidence is not valid UTF-8 JSON';
    END;
    IF jsonb_typeof(v_evidence) <> 'object' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_h3_contraction_reachability_evidence_invalid',
            MESSAGE = 'Legacy H3 reachability evidence must be a JSON object';
    END IF;
    v_expected_checks := '[
      {"id":"legacy-worker-assignment-protocol","expected":"ABSENT","passed":true,"matches":null},
      {"id":"legacy-worker-runtime","expected":"ABSENT","passed":true,"matches":null},
      {"id":"legacy-worker-orchestration","expected":"ABSENT","passed":true,"matches":null},
      {"id":"legacy-assignment-sql","expected":"ABSENT","passed":true,"matches":null},
      {"id":"legacy-worker-deployment","expected":"ABSENT","passed":true,"matches":null},
      {"id":"legacy-release-surface","expected":"ABSENT","passed":true,"matches":null},
      {"id":"legacy-machine-h3-tests","expected":"ABSENT","passed":true,"matches":null},
      {"id":"stage-worker-protocol","expected":"PRESENT","passed":true,"matches":["proto/vela/v1/stage_worker_control.proto"]},
      {"id":"stage-worker-runtime","expected":"PRESENT","passed":true,"matches":["cmd/vela-stage-worker-agent/main.go","internal/stageworkeragent/agent.go"]},
      {"id":"stage-scheduler-runtime","expected":"PRESENT","passed":true,"matches":["internal/stagescheduler/service.go"]},
      {"id":"stage-worker-deployment","expected":"PRESENT","passed":true,"matches":["deploy/stage-worker/kustomization.yaml"]},
      {"id":"stage-release-surface","expected":"PRESENT","passed":true,"matches":["bundle:contracted-stage-release"]}
    ]'::jsonb;
    SELECT count(*)::integer INTO v_evidence_key_count
    FROM jsonb_object_keys(v_evidence);
    IF v_evidence_key_count <> 8
       OR v_evidence -> 'schema_version' IS DISTINCT FROM '1'::jsonb
       OR v_evidence -> 'result' IS DISTINCT FROM '"PASS"'::jsonb
       OR v_evidence ->> 'release_digest' IS DISTINCT FROM
            'sha256:' || encode(p_release_digest, 'hex')
       OR v_evidence ->> 'configuration_revision' IS DISTINCT FROM
            p_configuration_revision
       OR v_evidence ->> 'source_revision' IS DISTINCT FROM v_source_revision
       OR v_evidence ->> 'source_revision' !~
            '^([0-9a-f]{40}|[0-9a-f]{64})$'
       OR v_evidence ->> 'observed_by' IS NULL
       OR length(v_evidence ->> 'observed_by') NOT BETWEEN 1 AND 300
       OR btrim(v_evidence ->> 'observed_by') <>
            v_evidence ->> 'observed_by'
       OR v_evidence ->> 'observed_by' ~ '[[:cntrl:]]'
       OR v_evidence ->> 'observed_at' IS NULL
       OR v_evidence ->> 'observed_at' !~ 'Z$'
       OR v_evidence -> 'checks' IS DISTINCT FROM v_expected_checks THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_h3_contraction_reachability_evidence_invalid',
            MESSAGE = 'Legacy H3 reachability evidence does not match the exact PASS contract';
    END IF;
    BEGIN
        PERFORM (v_evidence ->> 'observed_at')::timestamptz;
    EXCEPTION WHEN OTHERS THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'legacy_h3_contraction_reachability_evidence_invalid',
            MESSAGE = 'Legacy H3 reachability observation time is invalid';
    END;
    v_reachability_evidence_digest := sha256(p_reachability_evidence);

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_zero_backlog_receipt_id::text, 49057)
    );

    SELECT record.* INTO v_existing
    FROM public.legacy_h3_contraction_authorizations AS record
    WHERE record.zero_backlog_receipt_id = p_zero_backlog_receipt_id;
    IF FOUND THEN
        IF v_existing.launch_manifest_digest IS DISTINCT FROM p_launch_manifest_digest
           OR v_existing.release_digest IS DISTINCT FROM p_release_digest
           OR v_existing.configuration_revision IS DISTINCT FROM
                p_configuration_revision
           OR v_existing.source_revision IS DISTINCT FROM v_source_revision
           OR v_existing.reachability_evidence_digest IS DISTINCT FROM
                v_reachability_evidence_digest
           OR v_existing.authorized_by IS DISTINCT FROM p_authorized_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'legacy_h3_contraction_authorization_replay_mismatch',
                MESSAGE = 'Legacy H3 contraction authorization replay changed';
        END IF;
        RETURN QUERY SELECT
            v_existing.zero_backlog_receipt_id,
            v_existing.cutover_revision_id,
            v_existing.launch_manifest_digest,
            v_existing.release_digest,
            v_existing.configuration_revision,
            v_existing.source_revision,
            v_existing.reachability_evidence_digest,
            v_existing.authorized_at,
            v_existing.content_digest,
            true;
        RETURN;
    END IF;
    IF EXISTS (SELECT 1 FROM public.legacy_h3_contraction_authorizations) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_authorization_already_completed',
            MESSAGE = 'Legacy H3 contraction was already authorized with another receipt';
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
       OR v_revision.stage_cohort_basis_points <> 10000 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_production_stage_only_required',
            MESSAGE = 'Current cutover revision must be PRODUCTION STAGE_ONLY';
    END IF;

    SELECT readiness.* INTO v_readiness
    FROM public.legacy_h3_contraction_readiness_receipts AS readiness
    WHERE readiness.zero_backlog_receipt_id = p_zero_backlog_receipt_id
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_readiness_required',
            MESSAGE = 'Legacy H3 contraction readiness receipt is required';
    END IF;
    IF v_readiness.cutover_revision_id <> v_revision.id THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_cutover_revision_stale',
            MESSAGE = 'Legacy H3 contraction readiness does not bind the current cutover revision';
    END IF;
    IF v_revision.launch_manifest_digest IS DISTINCT FROM p_launch_manifest_digest THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_launch_manifest_mismatch',
            MESSAGE = 'Legacy H3 contraction manifest does not bind the current cutover revision';
    END IF;
    IF v_revision.release_digest IS DISTINCT FROM p_release_digest
       OR v_revision.configuration_revision IS DISTINCT FROM
            p_configuration_revision THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_release_binding_mismatch',
            MESSAGE = 'Legacy H3 reachability evidence does not bind the current release';
    END IF;

    SELECT manifest.* INTO v_manifest
    FROM public.production_gate_manifests AS manifest
    WHERE manifest.manifest_digest = p_launch_manifest_digest
      AND manifest.sealed_at IS NOT NULL
      AND manifest.receipt_count = 9
    FOR SHARE;
    IF NOT FOUND
       OR v_manifest.release_digest <> v_revision.release_digest
       OR v_manifest.configuration_revision <> v_revision.configuration_revision THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_sealed_release_required',
            MESSAGE = 'A sealed exact-release Production Gate manifest is required';
    END IF;
    SELECT count(*)::integer INTO v_receipt_count
    FROM public.production_gate_receipts AS receipt
    WHERE receipt.manifest_digest = p_launch_manifest_digest
      AND receipt.result = 'PASS'
      AND receipt.release_digest = v_revision.release_digest
      AND receipt.configuration_revision = v_revision.configuration_revision;
    IF v_receipt_count <> 9 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_launch_receipts_incomplete',
            MESSAGE = 'All nine exact-release PASS Launch Receipts are required';
    END IF;

    SELECT public.vela_current_legacy_authority_inventory_total()
    INTO v_live_total;
    IF v_live_total <> 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_authorization_live_inventory_nonzero',
            MESSAGE = 'Live legacy authority inventory is not zero';
    END IF;

    v_authorized_at := clock_timestamp();
    v_content_digest := sha256(convert_to(jsonb_build_object(
        'schema_version', 1,
        'zero_backlog_receipt_id', p_zero_backlog_receipt_id,
        'cutover_revision_id', v_revision.id,
        'launch_manifest_digest', encode(p_launch_manifest_digest, 'hex'),
        'release_digest', encode(v_revision.release_digest, 'hex'),
        'configuration_revision', v_revision.configuration_revision,
        'source_revision', v_source_revision,
        'reachability_evidence_digest',
            encode(v_reachability_evidence_digest, 'hex'),
        'authorized_at', v_authorized_at,
        'authorized_by', p_authorized_by
    )::text, 'UTF8'));
    INSERT INTO public.legacy_h3_contraction_authorizations (
        zero_backlog_receipt_id, cutover_revision_id,
        launch_manifest_digest, release_digest, configuration_revision,
        source_revision, reachability_evidence_digest, authorized_at, authorized_by,
        content_digest
    ) VALUES (
        p_zero_backlog_receipt_id, v_revision.id,
        p_launch_manifest_digest, v_revision.release_digest,
        v_revision.configuration_revision, v_source_revision,
        v_reachability_evidence_digest,
        v_authorized_at, p_authorized_by, v_content_digest
    ) RETURNING * INTO v_existing;

    RETURN QUERY SELECT
        v_existing.zero_backlog_receipt_id,
        v_existing.cutover_revision_id,
        v_existing.launch_manifest_digest,
        v_existing.release_digest,
        v_existing.configuration_revision,
        v_existing.source_revision,
        v_existing.reachability_evidence_digest,
        v_existing.authorized_at,
        v_existing.content_digest,
        false;
END
$$;

ALTER TABLE legacy_h3_contraction_authorizations
    OWNER TO vela_catalog_promotion_owner;
ALTER FUNCTION vela_authorize_legacy_h3_contraction(
    uuid, bytea, bytea, text, bytea, bytea, text
)
    OWNER TO vela_catalog_promotion_owner;

REVOKE ALL ON TABLE legacy_h3_contraction_authorizations FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_authorize_legacy_h3_contraction(
    uuid, bytea, bytea, text, bytea, bytea, text
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_authorize_legacy_h3_contraction(
    uuid, bytea, bytea, text, bytea, bytea, text
) TO vela_catalog_promotion;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE legacy_h3_contraction_authorizations IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM legacy_h3_contraction_authorizations) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'legacy_h3_contraction_authorization_rollback_is_unsafe',
            MESSAGE = 'Legacy H3 contraction authorization prevents rollback';
    END IF;
END
$$;
REVOKE EXECUTE ON FUNCTION vela_authorize_legacy_h3_contraction(
    uuid, bytea, bytea, text, bytea, bytea, text
) FROM vela_catalog_promotion;
DROP FUNCTION vela_authorize_legacy_h3_contraction(
    uuid, bytea, bytea, text, bytea, bytea, text
);
DROP TRIGGER legacy_h3_contraction_authorizations_immutable
    ON legacy_h3_contraction_authorizations;
DROP TABLE legacy_h3_contraction_authorizations;
-- +goose StatementEnd
