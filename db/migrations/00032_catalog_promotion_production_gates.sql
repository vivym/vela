-- +goose Up
-- +goose StatementBegin
CREATE TYPE production_gate AS ENUM (
    'preset-certification',
    'real-h3-soak',
    'state-event-fault-injection',
    'gpu-remediation',
    'organization-isolation-content-safety',
    'data-disaster-recovery',
    'release-rollback',
    'commercial-data-lifecycle',
    'observability-on-call'
);
CREATE TYPE production_gate_result AS ENUM ('PASS', 'FAIL');
CREATE TYPE catalog_evidence_mode AS ENUM ('LEGACY', 'EVIDENCED');

CREATE TABLE production_gate_manifests (
    manifest_digest bytea PRIMARY KEY CHECK (octet_length(manifest_digest) = 32),
    release_digest bytea NOT NULL CHECK (octet_length(release_digest) = 32),
    configuration_revision text NOT NULL CHECK (
        length(configuration_revision) BETWEEN 1 AND 300
        AND btrim(configuration_revision) = configuration_revision
        AND configuration_revision !~ '[[:cntrl:]]'
    ),
    sealed_at timestamptz,
    receipt_count integer NOT NULL DEFAULT 0 CHECK (receipt_count BETWEEN 0 AND 9),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (sealed_at IS NULL AND receipt_count = 0)
        OR (sealed_at IS NOT NULL AND receipt_count = 9)
    )
);

CREATE TABLE production_gate_receipts (
    id uuid PRIMARY KEY,
    manifest_digest bytea NOT NULL REFERENCES production_gate_manifests(manifest_digest),
    schema_version integer NOT NULL CHECK (schema_version = 1),
    gate production_gate NOT NULL,
    release_digest bytea NOT NULL CHECK (octet_length(release_digest) = 32),
    configuration_revision text NOT NULL CHECK (
        length(configuration_revision) BETWEEN 1 AND 300
        AND btrim(configuration_revision) = configuration_revision
        AND configuration_revision !~ '[[:cntrl:]]'
    ),
    validation_environment text NOT NULL CHECK (
        length(validation_environment) BETWEEN 1 AND 500
        AND btrim(validation_environment) = validation_environment
        AND validation_environment !~ '[[:cntrl:]]'
    ),
    result production_gate_result NOT NULL,
    owner_identity text NOT NULL CHECK (
        length(owner_identity) BETWEEN 1 AND 300
        AND btrim(owner_identity) = owner_identity
        AND owner_identity !~ '[[:cntrl:]]'
    ),
    acceptance_threshold text NOT NULL CHECK (
        length(acceptance_threshold) BETWEEN 1 AND 4000
        AND btrim(acceptance_threshold) = acceptance_threshold
        AND acceptance_threshold !~ '[[:cntrl:]]'
    ),
    observed_result text NOT NULL CHECK (
        length(observed_result) BETWEEN 1 AND 4000
        AND btrim(observed_result) = observed_result
        AND observed_result !~ '[[:cntrl:]]'
    ),
    evidence_ref text NOT NULL CHECK (
        length(evidence_ref) BETWEEN 1 AND 2000
        AND btrim(evidence_ref) = evidence_ref
        AND evidence_ref !~ '[[:cntrl:]]'
    ),
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    started_at timestamptz NOT NULL,
    completed_at timestamptz NOT NULL,
    recorded_at timestamptz NOT NULL,
    ingested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (manifest_digest, gate),
    CHECK (started_at <= completed_at AND completed_at <= recorded_at)
);

CREATE TABLE inference_backend_revisions (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL CHECK (
        length(stable_id) BETWEEN 1 AND 100
        AND btrim(stable_id) = stable_id
        AND stable_id !~ '[[:cntrl:]]'
    ),
    revision integer NOT NULL CHECK (revision > 0),
    state catalog_state NOT NULL,
    content_digest bytea NOT NULL CHECK (octet_length(content_digest) = 32),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (stable_id, revision)
);

CREATE TABLE profile_certification_evidence (
    id uuid PRIMARY KEY,
    profile_certification_id uuid NOT NULL UNIQUE REFERENCES profile_certifications(id),
    inference_backend_revision_id uuid NOT NULL REFERENCES inference_backend_revisions(id),
    hardware_driver_baseline text NOT NULL CHECK (
        length(hardware_driver_baseline) BETWEEN 1 AND 300
        AND btrim(hardware_driver_baseline) = hardware_driver_baseline
        AND hardware_driver_baseline !~ '[[:cntrl:]]'
    ),
    benchmark_corpus_revision text NOT NULL CHECK (
        length(benchmark_corpus_revision) BETWEEN 1 AND 300
        AND btrim(benchmark_corpus_revision) = benchmark_corpus_revision
        AND benchmark_corpus_revision !~ '[[:cntrl:]]'
    ),
    quality_threshold_ppm integer NOT NULL CHECK (quality_threshold_ppm BETWEEN 0 AND 1000000),
    quality_observed_ppm integer NOT NULL CHECK (quality_observed_ppm BETWEEN 0 AND 1000000),
    success_rate_threshold_ppm integer NOT NULL CHECK (success_rate_threshold_ppm BETWEEN 0 AND 1000000),
    success_rate_observed_ppm integer NOT NULL CHECK (success_rate_observed_ppm BETWEEN 0 AND 1000000),
    p50_milliseconds bigint NOT NULL CHECK (p50_milliseconds > 0),
    p95_threshold_milliseconds bigint NOT NULL CHECK (p95_threshold_milliseconds > 0),
    p95_observed_milliseconds bigint NOT NULL CHECK (p95_observed_milliseconds > 0),
    cost_threshold_minor bigint NOT NULL CHECK (cost_threshold_minor >= 0),
    cost_observed_minor bigint NOT NULL CHECK (cost_observed_minor >= 0),
    cost_currency text NOT NULL CHECK (cost_currency ~ '^[A-Z]{3}$'),
    confidence_threshold_ppm integer NOT NULL CHECK (confidence_threshold_ppm BETWEEN 0 AND 1000000),
    confidence_observed_ppm integer NOT NULL CHECK (confidence_observed_ppm BETWEEN 0 AND 1000000),
    launch_receipt_id uuid NOT NULL REFERENCES production_gate_receipts(id),
    issued_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (quality_observed_ppm >= quality_threshold_ppm),
    CHECK (success_rate_observed_ppm >= success_rate_threshold_ppm),
    CHECK (p50_milliseconds <= p95_observed_milliseconds),
    CHECK (p95_observed_milliseconds <= p95_threshold_milliseconds),
    CHECK (cost_observed_minor <= cost_threshold_minor),
    CHECK (confidence_observed_ppm >= confidence_threshold_ppm)
);

CREATE TABLE rate_card_release_bindings (
    id uuid PRIMARY KEY,
    rate_card_revision_id uuid NOT NULL UNIQUE REFERENCES rate_card_revisions(id),
    launch_receipt_id uuid NOT NULL REFERENCES production_gate_receipts(id),
    promoted_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE catalog_evidence_protocol_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    mode catalog_evidence_mode NOT NULL DEFAULT 'LEGACY',
    protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version IN (1, 2)),
    launch_receipt_id uuid REFERENCES production_gate_receipts(id),
    transitioned_at timestamptz,
    CHECK (
        (mode = 'LEGACY' AND protocol_version = 1
            AND launch_receipt_id IS NULL AND transitioned_at IS NULL)
        OR (mode = 'EVIDENCED' AND protocol_version = 2
            AND launch_receipt_id IS NOT NULL AND transitioned_at IS NOT NULL)
    )
);
INSERT INTO catalog_evidence_protocol_state (singleton) VALUES (true);

CREATE TABLE catalog_evidence_protocol_transitions (
    protocol_version integer PRIMARY KEY CHECK (protocol_version = 2),
    mode catalog_evidence_mode NOT NULL CHECK (mode = 'EVIDENCED'),
    launch_receipt_id uuid NOT NULL REFERENCES production_gate_receipts(id),
    transitioned_at timestamptz NOT NULL
);

CREATE FUNCTION vela_reject_catalog_evidence_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'catalog_evidence_is_immutable',
        MESSAGE = TG_TABLE_NAME || ' is immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_catalog_evidence_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_catalog_evidence_mutation()
    OWNER TO vela_catalog_promotion_owner;

CREATE TRIGGER production_gate_manifests_immutable
BEFORE DELETE OR TRUNCATE ON production_gate_manifests
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_catalog_evidence_mutation();
CREATE TRIGGER production_gate_receipts_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON production_gate_receipts
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_catalog_evidence_mutation();
CREATE TRIGGER profile_certification_evidence_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON profile_certification_evidence
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_catalog_evidence_mutation();
CREATE TRIGGER rate_card_release_bindings_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON rate_card_release_bindings
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_catalog_evidence_mutation();
CREATE TRIGGER catalog_evidence_protocol_history_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON catalog_evidence_protocol_transitions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_catalog_evidence_mutation();

CREATE FUNCTION vela_enforce_production_gate_manifest_seal() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF OLD.sealed_at IS NOT NULL
        OR NEW.manifest_digest <> OLD.manifest_digest
        OR NEW.release_digest <> OLD.release_digest
        OR NEW.configuration_revision <> OLD.configuration_revision
        OR NEW.created_at <> OLD.created_at
        OR NEW.sealed_at IS NULL
        OR NEW.receipt_count <> 9
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'production_gate_manifest_is_immutable',
            MESSAGE = 'Production Gate manifest permits only one complete seal transition';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_production_gate_manifest_seal() FROM PUBLIC;
ALTER FUNCTION vela_enforce_production_gate_manifest_seal()
    OWNER TO vela_catalog_promotion_owner;
CREATE TRIGGER production_gate_manifests_seal_once
BEFORE UPDATE ON production_gate_manifests
FOR EACH ROW EXECUTE FUNCTION vela_enforce_production_gate_manifest_seal();

CREATE FUNCTION vela_reject_inference_backend_definition_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF (to_jsonb(NEW) - 'state') IS DISTINCT FROM (to_jsonb(OLD) - 'state') THEN
        RAISE EXCEPTION 'immutable InferenceBackendRevision definition fields cannot be changed'
            USING ERRCODE = '55000',
                CONSTRAINT = 'inference_backend_revision_definition_immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_inference_backend_definition_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_inference_backend_definition_mutation()
    OWNER TO vela_catalog_promotion_owner;
CREATE TRIGGER inference_backend_revisions_definition_immutable
BEFORE UPDATE ON inference_backend_revisions
FOR EACH ROW EXECUTE FUNCTION vela_reject_inference_backend_definition_mutation();

CREATE FUNCTION vela_reject_catalog_protocol_state_delete() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'catalog_evidence_protocol_state_required',
        MESSAGE = 'Catalog evidence protocol state cannot be removed';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_catalog_protocol_state_delete() FROM PUBLIC;
ALTER FUNCTION vela_reject_catalog_protocol_state_delete()
    OWNER TO vela_catalog_promotion_owner;
CREATE TRIGGER catalog_evidence_protocol_state_required
BEFORE DELETE OR TRUNCATE ON catalog_evidence_protocol_state
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_catalog_protocol_state_delete();

CREATE FUNCTION vela_enforce_catalog_protocol_state_transition() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.protocol_version <> OLD.protocol_version + 1
        OR OLD.mode <> 'LEGACY'
        OR NEW.mode <> 'EVIDENCED'
        OR NOT EXISTS (
            SELECT 1
            FROM public.catalog_evidence_protocol_transitions AS transition
            WHERE transition.protocol_version = NEW.protocol_version
              AND transition.mode = NEW.mode
              AND transition.launch_receipt_id = NEW.launch_receipt_id
              AND transition.transitioned_at = NEW.transitioned_at
        )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'catalog_evidence_protocol_transition_required',
            MESSAGE = 'Catalog evidence protocol state must follow immutable history';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_catalog_protocol_state_transition() FROM PUBLIC;
ALTER FUNCTION vela_enforce_catalog_protocol_state_transition()
    OWNER TO vela_catalog_promotion_owner;
CREATE TRIGGER catalog_evidence_protocol_enforce_transition
BEFORE UPDATE ON catalog_evidence_protocol_state
FOR EACH ROW EXECUTE FUNCTION vela_enforce_catalog_protocol_state_transition();

CREATE FUNCTION vela_private.require_sealed_preset_receipt(p_receipt_id uuid)
RETURNS production_gate_receipts
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_receipt public.production_gate_receipts%ROWTYPE;
BEGIN
    SELECT receipt.* INTO v_receipt
    FROM public.production_gate_receipts AS receipt
    JOIN public.production_gate_manifests AS manifest
      ON manifest.manifest_digest = receipt.manifest_digest
    WHERE receipt.id = p_receipt_id
      AND receipt.gate = 'preset-certification'
      AND receipt.result = 'PASS'
      AND manifest.sealed_at IS NOT NULL
      AND manifest.receipt_count = 9
    FOR SHARE OF receipt, manifest;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'sealed_preset_certification_receipt_required',
            MESSAGE = 'a sealed PASS preset-certification Launch Receipt is required';
    END IF;
    RETURN v_receipt;
END
$$;
REVOKE ALL ON FUNCTION vela_private.require_sealed_preset_receipt(uuid) FROM PUBLIC;
ALTER FUNCTION vela_private.require_sealed_preset_receipt(uuid)
    OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_record_production_gate_receipt(
    p_receipt_id uuid,
    p_schema_version integer,
    p_gate production_gate,
    p_release_digest bytea,
    p_configuration_revision text,
    p_validation_environment text,
    p_result production_gate_result,
    p_owner_identity text,
    p_acceptance_threshold text,
    p_observed_result text,
    p_evidence_ref text,
    p_evidence_digest bytea,
    p_manifest_digest bytea,
    p_started_at timestamptz,
    p_completed_at timestamptz,
    p_recorded_at timestamptz
) RETURNS TABLE (receipt_id uuid, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_manifest public.production_gate_manifests%ROWTYPE;
    v_existing public.production_gate_receipts%ROWTYPE;
BEGIN
    IF p_receipt_id IS NULL OR p_schema_version <> 1 OR p_gate IS NULL
        OR p_release_digest IS NULL OR octet_length(p_release_digest) <> 32
        OR p_manifest_digest IS NULL OR octet_length(p_manifest_digest) <> 32
        OR p_evidence_digest IS NULL OR octet_length(p_evidence_digest) <> 32
        OR p_configuration_revision IS NULL
        OR length(p_configuration_revision) NOT BETWEEN 1 AND 300
        OR btrim(p_configuration_revision) <> p_configuration_revision
        OR p_configuration_revision ~ '[[:cntrl:]]'
        OR p_validation_environment IS NULL
        OR length(p_validation_environment) NOT BETWEEN 1 AND 500
        OR btrim(p_validation_environment) <> p_validation_environment
        OR p_validation_environment ~ '[[:cntrl:]]'
        OR p_owner_identity IS NULL OR length(p_owner_identity) NOT BETWEEN 1 AND 300
        OR btrim(p_owner_identity) <> p_owner_identity
        OR p_owner_identity ~ '[[:cntrl:]]'
        OR p_acceptance_threshold IS NULL
        OR length(p_acceptance_threshold) NOT BETWEEN 1 AND 4000
        OR btrim(p_acceptance_threshold) <> p_acceptance_threshold
        OR p_acceptance_threshold ~ '[[:cntrl:]]'
        OR p_observed_result IS NULL OR length(p_observed_result) NOT BETWEEN 1 AND 4000
        OR btrim(p_observed_result) <> p_observed_result
        OR p_observed_result ~ '[[:cntrl:]]'
        OR p_evidence_ref IS NULL OR length(p_evidence_ref) NOT BETWEEN 1 AND 2000
        OR btrim(p_evidence_ref) <> p_evidence_ref
        OR p_evidence_ref ~ '[[:cntrl:]]'
        OR p_started_at IS NULL OR p_completed_at IS NULL OR p_recorded_at IS NULL
        OR p_started_at > p_completed_at OR p_completed_at > p_recorded_at
    THEN
        RAISE EXCEPTION 'invalid Production Gate receipt input' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.production_gate_manifests (
        manifest_digest, release_digest, configuration_revision
    ) VALUES (
        p_manifest_digest, p_release_digest, p_configuration_revision
    ) ON CONFLICT (manifest_digest) DO NOTHING;
    SELECT manifest.* INTO STRICT v_manifest
    FROM public.production_gate_manifests AS manifest
    WHERE manifest.manifest_digest = p_manifest_digest
    FOR UPDATE;
    IF v_manifest.release_digest <> p_release_digest
        OR v_manifest.configuration_revision <> p_configuration_revision
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'production_gate_manifest_binding_immutable',
            MESSAGE = 'all receipts in a manifest must bind one release and configuration';
    END IF;

    SELECT receipt.* INTO v_existing
    FROM public.production_gate_receipts AS receipt
    WHERE receipt.id = p_receipt_id
       OR (receipt.manifest_digest = p_manifest_digest AND receipt.gate = p_gate)
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.id <> p_receipt_id
            OR v_existing.manifest_digest <> p_manifest_digest
            OR v_existing.schema_version <> p_schema_version
            OR v_existing.gate <> p_gate
            OR v_existing.release_digest <> p_release_digest
            OR v_existing.configuration_revision <> p_configuration_revision
            OR v_existing.validation_environment <> p_validation_environment
            OR v_existing.result <> p_result
            OR v_existing.owner_identity <> p_owner_identity
            OR v_existing.acceptance_threshold <> p_acceptance_threshold
            OR v_existing.observed_result <> p_observed_result
            OR v_existing.evidence_ref <> p_evidence_ref
            OR v_existing.evidence_digest <> p_evidence_digest
            OR v_existing.started_at <> p_started_at
            OR v_existing.completed_at <> p_completed_at
            OR v_existing.recorded_at <> p_recorded_at
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'production_gate_receipt_idempotency_mismatch',
                MESSAGE = 'Production Gate receipt replay does not match immutable input';
        END IF;
        RETURN QUERY SELECT v_existing.id, true;
        RETURN;
    END IF;
    IF v_manifest.sealed_at IS NOT NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'production_gate_manifest_is_sealed',
            MESSAGE = 'a sealed Production Gate manifest cannot accept another receipt';
    END IF;

    INSERT INTO public.production_gate_receipts (
        id, manifest_digest, schema_version, gate, release_digest,
        configuration_revision, validation_environment, result, owner_identity,
        acceptance_threshold, observed_result, evidence_ref, evidence_digest,
        started_at, completed_at, recorded_at
    ) VALUES (
        p_receipt_id, p_manifest_digest, p_schema_version, p_gate, p_release_digest,
        p_configuration_revision, p_validation_environment, p_result, p_owner_identity,
        p_acceptance_threshold, p_observed_result, p_evidence_ref, p_evidence_digest,
        p_started_at, p_completed_at, p_recorded_at
    );
    RETURN QUERY SELECT p_receipt_id, false;
END
$$;
REVOKE ALL ON FUNCTION vela_record_production_gate_receipt(
    uuid, integer, production_gate, bytea, text, text, production_gate_result,
    text, text, text, text, bytea, bytea, timestamptz, timestamptz, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_record_production_gate_receipt(
    uuid, integer, production_gate, bytea, text, text, production_gate_result,
    text, text, text, text, bytea, bytea, timestamptz, timestamptz, timestamptz
) OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_seal_production_gate_manifest(p_manifest_digest bytea)
RETURNS TABLE (sealed boolean, receipt_count integer)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_manifest public.production_gate_manifests%ROWTYPE;
    v_count integer;
BEGIN
    IF p_manifest_digest IS NULL OR octet_length(p_manifest_digest) <> 32 THEN
        RAISE EXCEPTION 'invalid Production Gate manifest digest' USING ERRCODE = '22023';
    END IF;
    SELECT manifest.* INTO v_manifest
    FROM public.production_gate_manifests AS manifest
    WHERE manifest.manifest_digest = p_manifest_digest
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'production_gate_manifest_not_found',
            MESSAGE = 'Production Gate manifest is not found';
    END IF;
    IF v_manifest.sealed_at IS NOT NULL THEN
        RETURN QUERY SELECT true, v_manifest.receipt_count;
        RETURN;
    END IF;
    SELECT count(*)::integer INTO v_count
    FROM public.production_gate_receipts AS receipt
    WHERE receipt.manifest_digest = p_manifest_digest
      AND receipt.result = 'PASS';
    IF v_count <> 9 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'production_gate_manifest_is_incomplete',
            MESSAGE = 'all nine PASS Production Gate receipts are required';
    END IF;
    UPDATE public.production_gate_manifests
    SET sealed_at = clock_timestamp(), receipt_count = v_count
    WHERE manifest_digest = p_manifest_digest;
    RETURN QUERY SELECT true, v_count;
END
$$;
REVOKE ALL ON FUNCTION vela_seal_production_gate_manifest(bytea) FROM PUBLIC;
ALTER FUNCTION vela_seal_production_gate_manifest(bytea)
    OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_promote_profile_certification(
    p_evidence_id uuid,
    p_profile_certification_id uuid,
    p_inference_backend_revision_id uuid,
    p_hardware_driver_baseline text,
    p_benchmark_corpus_revision text,
    p_quality_threshold_ppm integer,
    p_quality_observed_ppm integer,
    p_success_rate_threshold_ppm integer,
    p_success_rate_observed_ppm integer,
    p_p50_milliseconds bigint,
    p_p95_threshold_milliseconds bigint,
    p_p95_observed_milliseconds bigint,
    p_cost_threshold_minor bigint,
    p_cost_observed_minor bigint,
    p_cost_currency text,
    p_confidence_threshold_ppm integer,
    p_confidence_observed_ppm integer,
    p_launch_receipt_id uuid
) RETURNS TABLE (
    evidence_id uuid,
    replayed boolean,
    certification_state catalog_state
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_receipt public.production_gate_receipts%ROWTYPE;
    v_certification public.profile_certifications%ROWTYPE;
    v_existing public.profile_certification_evidence%ROWTYPE;
    v_backend public.inference_backend_revisions%ROWTYPE;
BEGIN
    IF p_evidence_id IS NULL OR p_profile_certification_id IS NULL
        OR p_inference_backend_revision_id IS NULL
        OR p_hardware_driver_baseline IS NULL
        OR length(p_hardware_driver_baseline) NOT BETWEEN 1 AND 300
        OR btrim(p_hardware_driver_baseline) <> p_hardware_driver_baseline
        OR p_hardware_driver_baseline ~ '[[:cntrl:]]'
        OR p_benchmark_corpus_revision IS NULL
        OR length(p_benchmark_corpus_revision) NOT BETWEEN 1 AND 300
        OR btrim(p_benchmark_corpus_revision) <> p_benchmark_corpus_revision
        OR p_benchmark_corpus_revision ~ '[[:cntrl:]]'
        OR p_quality_threshold_ppm NOT BETWEEN 0 AND 1000000
        OR p_quality_observed_ppm NOT BETWEEN p_quality_threshold_ppm AND 1000000
        OR p_success_rate_threshold_ppm NOT BETWEEN 0 AND 1000000
        OR p_success_rate_observed_ppm NOT BETWEEN p_success_rate_threshold_ppm AND 1000000
        OR p_p50_milliseconds <= 0
        OR p_p95_threshold_milliseconds <= 0
        OR p_p95_observed_milliseconds NOT BETWEEN p_p50_milliseconds AND p_p95_threshold_milliseconds
        OR p_cost_threshold_minor < 0
        OR p_cost_observed_minor NOT BETWEEN 0 AND p_cost_threshold_minor
        OR p_cost_currency IS NULL OR p_cost_currency !~ '^[A-Z]{3}$'
        OR p_confidence_threshold_ppm NOT BETWEEN 0 AND 1000000
        OR p_confidence_observed_ppm NOT BETWEEN p_confidence_threshold_ppm AND 1000000
    THEN
        RAISE EXCEPTION 'invalid ProfileCertification evidence input' USING ERRCODE = '22023';
    END IF;
    v_receipt := vela_private.require_sealed_preset_receipt(p_launch_receipt_id);

    SELECT certification.* INTO v_certification
    FROM public.profile_certifications AS certification
    WHERE certification.id = p_profile_certification_id
    FOR UPDATE;
    IF NOT FOUND OR v_certification.state NOT IN ('CERTIFIED', 'CANARY', 'ACTIVE')
        OR v_certification.invalidated_at IS NOT NULL
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'profile_certification_not_promotable',
            MESSAGE = 'ProfileCertification must be valid and certified or canaried';
    END IF;
    SELECT backend.* INTO v_backend
    FROM public.inference_backend_revisions AS backend
    WHERE backend.id = p_inference_backend_revision_id
    FOR UPDATE;
    IF NOT FOUND OR v_backend.state NOT IN ('CANARY', 'ACTIVE') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'inference_backend_revision_not_promotable',
            MESSAGE = 'InferenceBackendRevision must complete canary before promotion';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM public.model_revisions AS model
        JOIN public.generation_preset_revisions AS preset
          ON preset.id = v_certification.generation_preset_revision_id
         AND preset.model_revision_id = model.id
        JOIN public.output_specs AS output ON output.id = v_certification.output_spec_id
        JOIN public.execution_profile_revisions AS profile
          ON profile.id = v_certification.execution_profile_revision_id
         AND profile.model_revision_id = model.id
        WHERE model.id = v_certification.model_revision_id
          AND model.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
          AND preset.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
          AND output.state IN ('CERTIFIED', 'CANARY', 'ACTIVE')
          AND profile.state IN ('CANARY', 'ACTIVE')
        FOR UPDATE OF model, preset, output, profile
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'profile_certification_dependencies_not_promotable',
            MESSAGE = 'all ProfileCertification revisions must complete certification and canary';
    END IF;

    SELECT evidence.* INTO v_existing
    FROM public.profile_certification_evidence AS evidence
    WHERE evidence.id = p_evidence_id
       OR evidence.profile_certification_id = p_profile_certification_id
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.id <> p_evidence_id
            OR v_existing.profile_certification_id <> p_profile_certification_id
            OR v_existing.inference_backend_revision_id <> p_inference_backend_revision_id
            OR v_existing.hardware_driver_baseline <> p_hardware_driver_baseline
            OR v_existing.benchmark_corpus_revision <> p_benchmark_corpus_revision
            OR v_existing.quality_threshold_ppm <> p_quality_threshold_ppm
            OR v_existing.quality_observed_ppm <> p_quality_observed_ppm
            OR v_existing.success_rate_threshold_ppm <> p_success_rate_threshold_ppm
            OR v_existing.success_rate_observed_ppm <> p_success_rate_observed_ppm
            OR v_existing.p50_milliseconds <> p_p50_milliseconds
            OR v_existing.p95_threshold_milliseconds <> p_p95_threshold_milliseconds
            OR v_existing.p95_observed_milliseconds <> p_p95_observed_milliseconds
            OR v_existing.cost_threshold_minor <> p_cost_threshold_minor
            OR v_existing.cost_observed_minor <> p_cost_observed_minor
            OR v_existing.cost_currency <> p_cost_currency
            OR v_existing.confidence_threshold_ppm <> p_confidence_threshold_ppm
            OR v_existing.confidence_observed_ppm <> p_confidence_observed_ppm
            OR v_existing.launch_receipt_id <> p_launch_receipt_id
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'profile_certification_evidence_idempotency_mismatch',
                MESSAGE = 'ProfileCertification evidence replay does not match immutable input';
        END IF;
        RETURN QUERY SELECT v_existing.id, true, v_certification.state;
        RETURN;
    END IF;

    INSERT INTO public.profile_certification_evidence (
        id, profile_certification_id, inference_backend_revision_id,
        hardware_driver_baseline, benchmark_corpus_revision,
        quality_threshold_ppm, quality_observed_ppm,
        success_rate_threshold_ppm, success_rate_observed_ppm,
        p50_milliseconds, p95_threshold_milliseconds, p95_observed_milliseconds,
        cost_threshold_minor, cost_observed_minor, cost_currency,
        confidence_threshold_ppm, confidence_observed_ppm, launch_receipt_id
    ) VALUES (
        p_evidence_id, p_profile_certification_id, p_inference_backend_revision_id,
        p_hardware_driver_baseline, p_benchmark_corpus_revision,
        p_quality_threshold_ppm, p_quality_observed_ppm,
        p_success_rate_threshold_ppm, p_success_rate_observed_ppm,
        p_p50_milliseconds, p_p95_threshold_milliseconds, p_p95_observed_milliseconds,
        p_cost_threshold_minor, p_cost_observed_minor, p_cost_currency,
        p_confidence_threshold_ppm, p_confidence_observed_ppm, p_launch_receipt_id
    );
    UPDATE public.profile_certifications SET state = 'ACTIVE'
    WHERE id = p_profile_certification_id;
    UPDATE public.model_revisions SET state = 'ACTIVE'
    WHERE id = v_certification.model_revision_id;
    UPDATE public.generation_preset_revisions SET state = 'ACTIVE'
    WHERE id = v_certification.generation_preset_revision_id;
    UPDATE public.output_specs SET state = 'ACTIVE'
    WHERE id = v_certification.output_spec_id;
    UPDATE public.execution_profile_revisions SET state = 'ACTIVE'
    WHERE id = v_certification.execution_profile_revision_id;
    UPDATE public.inference_backend_revisions SET state = 'ACTIVE'
    WHERE id = p_inference_backend_revision_id;
    RETURN QUERY SELECT p_evidence_id, false, 'ACTIVE'::catalog_state;
END
$$;
REVOKE ALL ON FUNCTION vela_promote_profile_certification(
    uuid, uuid, uuid, text, text, integer, integer, integer, integer,
    bigint, bigint, bigint, bigint, bigint, text, integer, integer, uuid
) FROM PUBLIC;
ALTER FUNCTION vela_promote_profile_certification(
    uuid, uuid, uuid, text, text, integer, integer, integer, integer,
    bigint, bigint, bigint, bigint, bigint, text, integer, integer, uuid
) OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_private.rate_card_has_evidenced_coverage(
    p_rate_card_revision_id uuid,
    p_launch_receipt_id uuid
) RETURNS boolean
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT EXISTS (
        SELECT 1 FROM public.rate_card_lines AS present
        WHERE present.rate_card_revision_id = p_rate_card_revision_id
    ) AND NOT EXISTS (
        SELECT 1
        FROM public.rate_card_lines AS line
        WHERE line.rate_card_revision_id = p_rate_card_revision_id
          AND NOT EXISTS (
              SELECT 1
              FROM public.profile_certifications AS certification
              JOIN public.profile_certification_evidence AS evidence
                ON evidence.profile_certification_id = certification.id
              WHERE certification.model_revision_id = line.model_revision_id
                AND certification.generation_preset_revision_id = line.generation_preset_revision_id
                AND certification.output_spec_id = line.output_spec_id
                AND certification.state = 'ACTIVE'
                AND certification.invalidated_at IS NULL
                AND evidence.launch_receipt_id = p_launch_receipt_id
          )
    ) AND NOT EXISTS (
        SELECT 1
        FROM public.rate_card_lines AS line
        JOIN public.generation_preset_revisions AS preset
          ON preset.id = line.generation_preset_revision_id
        WHERE line.rate_card_revision_id = p_rate_card_revision_id
        GROUP BY line.model_revision_id, line.service_class_revision_id,
            line.output_spec_id, line.currency
        HAVING count(*) <> 3
            OR count(DISTINCT preset.stable_id) <> 3
            OR array_agg(DISTINCT preset.stable_id ORDER BY preset.stable_id)
                <> ARRAY['balanced', 'fast', 'quality']::text[]
            OR bool_or(preset.state <> 'ACTIVE')
    )
$$;
REVOKE ALL ON FUNCTION vela_private.rate_card_has_evidenced_coverage(uuid, uuid)
    FROM PUBLIC;
ALTER FUNCTION vela_private.rate_card_has_evidenced_coverage(uuid, uuid)
    OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_promote_rate_card(
    p_binding_id uuid,
    p_rate_card_revision_id uuid,
    p_launch_receipt_id uuid
) RETURNS TABLE (
    binding_id uuid,
    replayed boolean,
    rate_card_state catalog_state
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_receipt public.production_gate_receipts%ROWTYPE;
    v_rate_card public.rate_card_revisions%ROWTYPE;
    v_existing public.rate_card_release_bindings%ROWTYPE;
BEGIN
    IF p_binding_id IS NULL OR p_rate_card_revision_id IS NULL
        OR p_launch_receipt_id IS NULL
    THEN
        RAISE EXCEPTION 'invalid RateCard promotion input' USING ERRCODE = '22023';
    END IF;
    v_receipt := vela_private.require_sealed_preset_receipt(p_launch_receipt_id);
    SELECT rate_card.* INTO v_rate_card
    FROM public.rate_card_revisions AS rate_card
    WHERE rate_card.id = p_rate_card_revision_id
    FOR UPDATE;
    IF NOT FOUND OR v_rate_card.state NOT IN ('CERTIFIED', 'CANARY', 'ACTIVE') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'rate_card_not_promotable',
            MESSAGE = 'RateCardRevision must be certified or canaried before promotion';
    END IF;
    IF NOT vela_private.rate_card_has_evidenced_coverage(
        p_rate_card_revision_id, p_launch_receipt_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'rate_card_requires_three_preset_evidence',
            MESSAGE = 'every saleable OutputSpec requires evidenced quality, balanced, and fast Presets';
    END IF;
    SELECT binding.* INTO v_existing
    FROM public.rate_card_release_bindings AS binding
    WHERE binding.id = p_binding_id
       OR binding.rate_card_revision_id = p_rate_card_revision_id
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.id <> p_binding_id
            OR v_existing.rate_card_revision_id <> p_rate_card_revision_id
            OR v_existing.launch_receipt_id <> p_launch_receipt_id
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '22023',
                CONSTRAINT = 'rate_card_release_binding_idempotency_mismatch',
                MESSAGE = 'RateCard release binding replay does not match immutable input';
        END IF;
        RETURN QUERY SELECT v_existing.id, true, v_rate_card.state;
        RETURN;
    END IF;
    INSERT INTO public.rate_card_release_bindings (
        id, rate_card_revision_id, launch_receipt_id
    ) VALUES (
        p_binding_id, p_rate_card_revision_id, p_launch_receipt_id
    );
    UPDATE public.rate_card_revisions SET state = 'ACTIVE'
    WHERE id = p_rate_card_revision_id;
    UPDATE public.service_class_revisions AS service_class SET state = 'ACTIVE'
    WHERE service_class.id IN (
        SELECT line.service_class_revision_id
        FROM public.rate_card_lines AS line
        WHERE line.rate_card_revision_id = p_rate_card_revision_id
    );
    RETURN QUERY SELECT p_binding_id, false, 'ACTIVE'::catalog_state;
END
$$;
REVOKE ALL ON FUNCTION vela_promote_rate_card(uuid, uuid, uuid) FROM PUBLIC;
ALTER FUNCTION vela_promote_rate_card(uuid, uuid, uuid)
    OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_enable_evidenced_catalog(p_launch_receipt_id uuid)
RETURNS TABLE (
    protocol_version integer,
    mode catalog_evidence_mode,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_receipt public.production_gate_receipts%ROWTYPE;
    v_state public.catalog_evidence_protocol_state%ROWTYPE;
    v_transitioned_at timestamptz;
BEGIN
    v_receipt := vela_private.require_sealed_preset_receipt(p_launch_receipt_id);
    SELECT state.* INTO STRICT v_state
    FROM public.catalog_evidence_protocol_state AS state
    WHERE state.singleton
    FOR UPDATE;
    IF v_state.mode = 'EVIDENCED' THEN
        IF NOT EXISTS (
            SELECT 1
            FROM public.profile_certification_evidence AS evidence
            JOIN public.profile_certifications AS certification
              ON certification.id = evidence.profile_certification_id
            WHERE evidence.launch_receipt_id = p_launch_receipt_id
        ) OR EXISTS (
            SELECT 1
            FROM public.profile_certification_evidence AS evidence
            JOIN public.profile_certifications AS certification
              ON certification.id = evidence.profile_certification_id
            WHERE evidence.launch_receipt_id = p_launch_receipt_id
              AND (
                  certification.state <> 'ACTIVE'
                  OR certification.invalidated_at IS NOT NULL
              )
        ) OR NOT EXISTS (
            SELECT 1
            FROM public.rate_card_release_bindings AS binding
            WHERE binding.launch_receipt_id = p_launch_receipt_id
        ) OR EXISTS (
            SELECT 1
            FROM public.rate_card_release_bindings AS binding
            JOIN public.rate_card_revisions AS rate_card
              ON rate_card.id = binding.rate_card_revision_id
            WHERE binding.launch_receipt_id = p_launch_receipt_id
              AND (
                  rate_card.state <> 'ACTIVE'
                  OR NOT vela_private.rate_card_has_evidenced_coverage(
                      rate_card.id, p_launch_receipt_id
                  )
              )
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'catalog_evidence_is_incomplete',
                MESSAGE = 'the release requires complete ACTIVE Catalog evidence';
        END IF;
        RETURN QUERY SELECT v_state.protocol_version, v_state.mode, true;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.profile_certifications AS certification
        WHERE certification.state = 'ACTIVE'
          AND certification.invalidated_at IS NULL
          AND NOT EXISTS (
              SELECT 1 FROM public.profile_certification_evidence AS evidence
              WHERE evidence.profile_certification_id = certification.id
                AND evidence.launch_receipt_id = p_launch_receipt_id
          )
    ) OR EXISTS (
        SELECT 1 FROM public.rate_card_revisions AS rate_card
        WHERE rate_card.state = 'ACTIVE'
          AND (
              NOT EXISTS (
                  SELECT 1 FROM public.rate_card_release_bindings AS binding
                  WHERE binding.rate_card_revision_id = rate_card.id
                    AND binding.launch_receipt_id = p_launch_receipt_id
              )
              OR NOT vela_private.rate_card_has_evidenced_coverage(
                  rate_card.id, p_launch_receipt_id
              )
          )
    ) OR EXISTS (
        SELECT 1 FROM public.model_revisions AS revision
        WHERE revision.state = 'ACTIVE' AND NOT EXISTS (
            SELECT 1
            FROM public.profile_certification_evidence AS evidence
            JOIN public.profile_certifications AS certification
              ON certification.id = evidence.profile_certification_id
            WHERE certification.model_revision_id = revision.id
              AND certification.state = 'ACTIVE'
              AND certification.invalidated_at IS NULL
              AND evidence.launch_receipt_id = p_launch_receipt_id
        )
    ) OR EXISTS (
        SELECT 1 FROM public.generation_preset_revisions AS revision
        WHERE revision.state = 'ACTIVE' AND NOT EXISTS (
            SELECT 1
            FROM public.profile_certification_evidence AS evidence
            JOIN public.profile_certifications AS certification
              ON certification.id = evidence.profile_certification_id
            WHERE certification.generation_preset_revision_id = revision.id
              AND certification.state = 'ACTIVE'
              AND certification.invalidated_at IS NULL
              AND evidence.launch_receipt_id = p_launch_receipt_id
        )
    ) OR EXISTS (
        SELECT 1 FROM public.output_specs AS revision
        WHERE revision.state = 'ACTIVE' AND NOT EXISTS (
            SELECT 1
            FROM public.profile_certification_evidence AS evidence
            JOIN public.profile_certifications AS certification
              ON certification.id = evidence.profile_certification_id
            WHERE certification.output_spec_id = revision.id
              AND certification.state = 'ACTIVE'
              AND certification.invalidated_at IS NULL
              AND evidence.launch_receipt_id = p_launch_receipt_id
        )
    ) OR EXISTS (
        SELECT 1 FROM public.execution_profile_revisions AS revision
        WHERE revision.state = 'ACTIVE' AND NOT EXISTS (
            SELECT 1
            FROM public.profile_certification_evidence AS evidence
            JOIN public.profile_certifications AS certification
              ON certification.id = evidence.profile_certification_id
            WHERE certification.execution_profile_revision_id = revision.id
              AND certification.state = 'ACTIVE'
              AND certification.invalidated_at IS NULL
              AND evidence.launch_receipt_id = p_launch_receipt_id
        )
    ) OR EXISTS (
        SELECT 1 FROM public.inference_backend_revisions AS revision
        WHERE revision.state = 'ACTIVE' AND NOT EXISTS (
            SELECT 1 FROM public.profile_certification_evidence AS evidence
            JOIN public.profile_certifications AS certification
              ON certification.id = evidence.profile_certification_id
            WHERE evidence.inference_backend_revision_id = revision.id
              AND certification.state = 'ACTIVE'
              AND certification.invalidated_at IS NULL
              AND evidence.launch_receipt_id = p_launch_receipt_id
        )
    ) OR EXISTS (
        SELECT 1 FROM public.service_class_revisions AS revision
        WHERE revision.state = 'ACTIVE' AND NOT EXISTS (
            SELECT 1
            FROM public.rate_card_lines AS line
            JOIN public.rate_card_release_bindings AS binding
              ON binding.rate_card_revision_id = line.rate_card_revision_id
            WHERE line.service_class_revision_id = revision.id
              AND binding.launch_receipt_id = p_launch_receipt_id
        )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'catalog_evidence_is_incomplete',
            MESSAGE = 'every ACTIVE Catalog revision requires exact release evidence';
    END IF;
    v_transitioned_at := clock_timestamp();
    INSERT INTO public.catalog_evidence_protocol_transitions (
        protocol_version, mode, launch_receipt_id, transitioned_at
    ) VALUES (2, 'EVIDENCED', p_launch_receipt_id, v_transitioned_at);
    UPDATE public.catalog_evidence_protocol_state
    SET mode = 'EVIDENCED', protocol_version = 2,
        launch_receipt_id = p_launch_receipt_id,
        transitioned_at = v_transitioned_at
    WHERE singleton;
    RETURN QUERY SELECT 2, 'EVIDENCED'::catalog_evidence_mode, false;
END
$$;
REVOKE ALL ON FUNCTION vela_enable_evidenced_catalog(uuid) FROM PUBLIC;
ALTER FUNCTION vela_enable_evidenced_catalog(uuid)
    OWNER TO vela_catalog_promotion_owner;

CREATE FUNCTION vela_guard_active_catalog_revision() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_mode catalog_evidence_mode;
    v_evidenced boolean := false;
BEGIN
    SELECT state.mode INTO STRICT v_mode
    FROM public.catalog_evidence_protocol_state AS state
    WHERE state.singleton
    FOR UPDATE;
    IF v_mode <> 'EVIDENCED' OR NEW.state <> 'ACTIVE' THEN
        RETURN NEW;
    END IF;
    CASE TG_TABLE_NAME
        WHEN 'model_revisions' THEN
            SELECT EXISTS (
                SELECT 1 FROM public.profile_certification_evidence AS evidence
                JOIN public.profile_certifications AS certification
                  ON certification.id = evidence.profile_certification_id
                WHERE certification.model_revision_id = NEW.id
                  AND certification.state = 'ACTIVE'
                  AND certification.invalidated_at IS NULL
            ) INTO v_evidenced;
        WHEN 'generation_preset_revisions' THEN
            SELECT EXISTS (
                SELECT 1 FROM public.profile_certification_evidence AS evidence
                JOIN public.profile_certifications AS certification
                  ON certification.id = evidence.profile_certification_id
                WHERE certification.generation_preset_revision_id = NEW.id
                  AND certification.state = 'ACTIVE'
                  AND certification.invalidated_at IS NULL
            ) INTO v_evidenced;
        WHEN 'output_specs' THEN
            SELECT EXISTS (
                SELECT 1 FROM public.profile_certification_evidence AS evidence
                JOIN public.profile_certifications AS certification
                  ON certification.id = evidence.profile_certification_id
                WHERE certification.output_spec_id = NEW.id
                  AND certification.state = 'ACTIVE'
                  AND certification.invalidated_at IS NULL
            ) INTO v_evidenced;
        WHEN 'execution_profile_revisions' THEN
            SELECT EXISTS (
                SELECT 1 FROM public.profile_certification_evidence AS evidence
                JOIN public.profile_certifications AS certification
                  ON certification.id = evidence.profile_certification_id
                WHERE certification.execution_profile_revision_id = NEW.id
                  AND certification.state = 'ACTIVE'
                  AND certification.invalidated_at IS NULL
            ) INTO v_evidenced;
        WHEN 'inference_backend_revisions' THEN
            SELECT EXISTS (
                SELECT 1 FROM public.profile_certification_evidence AS evidence
                JOIN public.profile_certifications AS certification
                  ON certification.id = evidence.profile_certification_id
                WHERE evidence.inference_backend_revision_id = NEW.id
                  AND certification.state = 'ACTIVE'
                  AND certification.invalidated_at IS NULL
            ) INTO v_evidenced;
        WHEN 'profile_certifications' THEN
            SELECT EXISTS (
                SELECT 1 FROM public.profile_certification_evidence AS evidence
                WHERE evidence.profile_certification_id = NEW.id
            ) INTO v_evidenced;
        WHEN 'service_class_revisions' THEN
            SELECT EXISTS (
                SELECT 1 FROM public.rate_card_lines AS line
                JOIN public.rate_card_release_bindings AS binding
                  ON binding.rate_card_revision_id = line.rate_card_revision_id
                WHERE line.service_class_revision_id = NEW.id
            ) INTO v_evidenced;
        WHEN 'rate_card_revisions' THEN
            SELECT EXISTS (
                SELECT 1 FROM public.rate_card_release_bindings AS binding
                WHERE binding.rate_card_revision_id = NEW.id
                  AND vela_private.rate_card_has_evidenced_coverage(
                      NEW.id, binding.launch_receipt_id
                  )
            ) INTO v_evidenced;
    END CASE;
    IF NOT v_evidenced THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'catalog_active_revision_requires_evidence',
            MESSAGE = 'ACTIVE Catalog revision requires exact release evidence';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_active_catalog_revision() FROM PUBLIC;
ALTER FUNCTION vela_guard_active_catalog_revision()
    OWNER TO vela_catalog_promotion_owner;

CREATE TRIGGER model_revisions_active_evidence
BEFORE INSERT OR UPDATE OF state ON model_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_catalog_revision();
CREATE TRIGGER generation_preset_revisions_active_evidence
BEFORE INSERT OR UPDATE OF state ON generation_preset_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_catalog_revision();
CREATE TRIGGER output_specs_active_evidence
BEFORE INSERT OR UPDATE OF state ON output_specs
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_catalog_revision();
CREATE TRIGGER execution_profile_revisions_active_evidence
BEFORE INSERT OR UPDATE OF state ON execution_profile_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_catalog_revision();
CREATE TRIGGER inference_backend_revisions_active_evidence
BEFORE INSERT OR UPDATE OF state ON inference_backend_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_catalog_revision();
CREATE TRIGGER profile_certifications_active_evidence
BEFORE INSERT OR UPDATE OF state ON profile_certifications
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_catalog_revision();
CREATE TRIGGER service_class_revisions_active_evidence
BEFORE INSERT OR UPDATE OF state ON service_class_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_catalog_revision();
CREATE TRIGGER rate_card_revisions_active_evidence
BEFORE INSERT OR UPDATE OF state ON rate_card_revisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_catalog_revision();

CREATE FUNCTION vela_guard_active_rate_card_line_mutation() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_rate_card_revision_id uuid;
    v_mode catalog_evidence_mode;
BEGIN
    SELECT state.mode INTO STRICT v_mode
    FROM public.catalog_evidence_protocol_state AS state
    WHERE state.singleton
    FOR UPDATE;
    IF v_mode <> 'EVIDENCED' THEN
        IF TG_OP = 'TRUNCATE' THEN
            RETURN NULL;
        END IF;
        RETURN COALESCE(NEW, OLD);
    END IF;
    IF TG_OP = 'TRUNCATE' THEN
        IF EXISTS (
            SELECT 1 FROM public.rate_card_revisions AS rate_card
            WHERE rate_card.state = 'ACTIVE'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'active_rate_card_lines_are_immutable',
                MESSAGE = 'ACTIVE RateCardRevision lines are immutable';
        END IF;
        RETURN NULL;
    END IF;
    IF TG_OP = 'UPDATE'
        AND OLD.rate_card_revision_id <> NEW.rate_card_revision_id
        AND EXISTS (
            SELECT 1 FROM public.rate_card_revisions AS rate_card
            WHERE rate_card.id = OLD.rate_card_revision_id
              AND rate_card.state = 'ACTIVE'
        )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'active_rate_card_lines_are_immutable',
            MESSAGE = 'ACTIVE RateCardRevision lines are immutable';
    END IF;
    v_rate_card_revision_id := CASE WHEN TG_OP = 'DELETE'
        THEN OLD.rate_card_revision_id ELSE NEW.rate_card_revision_id END;
    IF EXISTS (
        SELECT 1 FROM public.rate_card_revisions AS rate_card
        WHERE rate_card.id = v_rate_card_revision_id AND rate_card.state = 'ACTIVE'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'active_rate_card_lines_are_immutable',
            MESSAGE = 'ACTIVE RateCardRevision lines are immutable';
    END IF;
    RETURN COALESCE(NEW, OLD);
END
$$;
REVOKE ALL ON FUNCTION vela_guard_active_rate_card_line_mutation() FROM PUBLIC;
ALTER FUNCTION vela_guard_active_rate_card_line_mutation()
    OWNER TO vela_catalog_promotion_owner;
CREATE TRIGGER rate_card_lines_active_immutable
BEFORE INSERT OR UPDATE OR DELETE ON rate_card_lines
FOR EACH ROW EXECUTE FUNCTION vela_guard_active_rate_card_line_mutation();
CREATE TRIGGER rate_card_lines_active_truncate
BEFORE TRUNCATE ON rate_card_lines
FOR EACH STATEMENT EXECUTE FUNCTION vela_guard_active_rate_card_line_mutation();

ALTER TABLE production_gate_manifests ENABLE ROW LEVEL SECURITY;
ALTER TABLE production_gate_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE inference_backend_revisions ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_certification_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE rate_card_release_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE catalog_evidence_protocol_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE catalog_evidence_protocol_transitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE production_gate_manifests FORCE ROW LEVEL SECURITY;
ALTER TABLE production_gate_receipts FORCE ROW LEVEL SECURITY;
ALTER TABLE inference_backend_revisions FORCE ROW LEVEL SECURITY;
ALTER TABLE profile_certification_evidence FORCE ROW LEVEL SECURITY;
ALTER TABLE rate_card_release_bindings FORCE ROW LEVEL SECURITY;
ALTER TABLE catalog_evidence_protocol_state FORCE ROW LEVEL SECURITY;
ALTER TABLE catalog_evidence_protocol_transitions FORCE ROW LEVEL SECURITY;

ALTER TABLE production_gate_manifests OWNER TO vela_catalog_promotion_owner;
ALTER TABLE production_gate_receipts OWNER TO vela_catalog_promotion_owner;
ALTER TABLE inference_backend_revisions OWNER TO vela_catalog_promotion_owner;
ALTER TABLE profile_certification_evidence OWNER TO vela_catalog_promotion_owner;
ALTER TABLE rate_card_release_bindings OWNER TO vela_catalog_promotion_owner;
ALTER TABLE catalog_evidence_protocol_state OWNER TO vela_catalog_promotion_owner;
ALTER TABLE catalog_evidence_protocol_transitions OWNER TO vela_catalog_promotion_owner;

GRANT USAGE ON SCHEMA public TO vela_catalog_promotion;
GRANT USAGE ON SCHEMA vela_private TO vela_catalog_promotion_owner;
GRANT SELECT ON model_revisions, generation_preset_revisions,
    service_class_revisions, output_specs, execution_profile_revisions,
    profile_certifications, rate_card_revisions, rate_card_lines
    TO vela_catalog_promotion_owner;
GRANT UPDATE (state) ON model_revisions, generation_preset_revisions,
    service_class_revisions, output_specs, execution_profile_revisions,
    profile_certifications, rate_card_revisions
    TO vela_catalog_promotion_owner;
GRANT EXECUTE ON FUNCTION vela_record_production_gate_receipt(
    uuid, integer, production_gate, bytea, text, text, production_gate_result,
    text, text, text, text, bytea, bytea, timestamptz, timestamptz, timestamptz
) TO vela_catalog_promotion;
GRANT EXECUTE ON FUNCTION vela_seal_production_gate_manifest(bytea)
    TO vela_catalog_promotion;
GRANT EXECUTE ON FUNCTION vela_promote_profile_certification(
    uuid, uuid, uuid, text, text, integer, integer, integer, integer,
    bigint, bigint, bigint, bigint, bigint, text, integer, integer, uuid
) TO vela_catalog_promotion;
GRANT EXECUTE ON FUNCTION vela_promote_rate_card(uuid, uuid, uuid)
    TO vela_catalog_promotion;
GRANT EXECUTE ON FUNCTION vela_enable_evidenced_catalog(uuid)
    TO vela_catalog_promotion;

GRANT SELECT ON inference_backend_revisions,
    profile_certification_evidence,
    rate_card_release_bindings,
    catalog_evidence_protocol_state TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    LOCK TABLE catalog_evidence_protocol_state IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE production_gate_manifests IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE production_gate_receipts IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE profile_certification_evidence IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE rate_card_release_bindings IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE inference_backend_revisions IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (
        SELECT 1 FROM catalog_evidence_protocol_state WHERE mode <> 'LEGACY'
    ) OR EXISTS (
        SELECT 1 FROM production_gate_manifests
    ) OR EXISTS (
        SELECT 1 FROM production_gate_receipts
    ) OR EXISTS (
        SELECT 1 FROM profile_certification_evidence
    ) OR EXISTS (
        SELECT 1 FROM rate_card_release_bindings
    ) OR EXISTS (
        SELECT 1 FROM inference_backend_revisions
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'catalog_promotion_rollback_is_unsafe',
            MESSAGE = 'Catalog promotion evidence or authority exists';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_enable_evidenced_catalog(uuid)
    FROM vela_catalog_promotion;
REVOKE EXECUTE ON FUNCTION vela_promote_rate_card(uuid, uuid, uuid)
    FROM vela_catalog_promotion;
REVOKE EXECUTE ON FUNCTION vela_promote_profile_certification(
    uuid, uuid, uuid, text, text, integer, integer, integer, integer,
    bigint, bigint, bigint, bigint, bigint, text, integer, integer, uuid
) FROM vela_catalog_promotion;
REVOKE EXECUTE ON FUNCTION vela_seal_production_gate_manifest(bytea)
    FROM vela_catalog_promotion;
REVOKE EXECUTE ON FUNCTION vela_record_production_gate_receipt(
    uuid, integer, production_gate, bytea, text, text, production_gate_result,
    text, text, text, text, bytea, bytea, timestamptz, timestamptz, timestamptz
) FROM vela_catalog_promotion;
REVOKE USAGE ON SCHEMA public FROM vela_catalog_promotion;
REVOKE USAGE ON SCHEMA vela_private FROM vela_catalog_promotion_owner;
REVOKE SELECT ON model_revisions, generation_preset_revisions,
    service_class_revisions, output_specs, execution_profile_revisions,
    profile_certifications, rate_card_revisions, rate_card_lines
    FROM vela_catalog_promotion_owner;
REVOKE UPDATE (state) ON model_revisions, generation_preset_revisions,
    service_class_revisions, output_specs, execution_profile_revisions,
    profile_certifications, rate_card_revisions
    FROM vela_catalog_promotion_owner;
REVOKE SELECT ON inference_backend_revisions,
    profile_certification_evidence,
    rate_card_release_bindings,
    catalog_evidence_protocol_state FROM vela_internal;

DROP TRIGGER rate_card_lines_active_truncate ON rate_card_lines;
DROP TRIGGER rate_card_lines_active_immutable ON rate_card_lines;
DROP FUNCTION vela_guard_active_rate_card_line_mutation();
DROP TRIGGER rate_card_revisions_active_evidence ON rate_card_revisions;
DROP TRIGGER service_class_revisions_active_evidence ON service_class_revisions;
DROP TRIGGER profile_certifications_active_evidence ON profile_certifications;
DROP TRIGGER inference_backend_revisions_active_evidence ON inference_backend_revisions;
DROP TRIGGER execution_profile_revisions_active_evidence ON execution_profile_revisions;
DROP TRIGGER output_specs_active_evidence ON output_specs;
DROP TRIGGER generation_preset_revisions_active_evidence ON generation_preset_revisions;
DROP TRIGGER model_revisions_active_evidence ON model_revisions;
DROP FUNCTION vela_guard_active_catalog_revision();

DROP FUNCTION vela_enable_evidenced_catalog(uuid);
DROP FUNCTION vela_promote_rate_card(uuid, uuid, uuid);
DROP FUNCTION vela_private.rate_card_has_evidenced_coverage(uuid, uuid);
DROP FUNCTION vela_promote_profile_certification(
    uuid, uuid, uuid, text, text, integer, integer, integer, integer,
    bigint, bigint, bigint, bigint, bigint, text, integer, integer, uuid
);
DROP FUNCTION vela_seal_production_gate_manifest(bytea);
DROP FUNCTION vela_record_production_gate_receipt(
    uuid, integer, production_gate, bytea, text, text, production_gate_result,
    text, text, text, text, bytea, bytea, timestamptz, timestamptz, timestamptz
);
DROP FUNCTION vela_private.require_sealed_preset_receipt(uuid);

DROP TRIGGER catalog_evidence_protocol_enforce_transition
    ON catalog_evidence_protocol_state;
DROP FUNCTION vela_enforce_catalog_protocol_state_transition();
DROP TRIGGER catalog_evidence_protocol_state_required
    ON catalog_evidence_protocol_state;
DROP FUNCTION vela_reject_catalog_protocol_state_delete();
DROP TRIGGER inference_backend_revisions_definition_immutable
    ON inference_backend_revisions;
DROP FUNCTION vela_reject_inference_backend_definition_mutation();

DROP TRIGGER catalog_evidence_protocol_history_immutable
    ON catalog_evidence_protocol_transitions;
DROP TRIGGER rate_card_release_bindings_immutable ON rate_card_release_bindings;
DROP TRIGGER profile_certification_evidence_immutable ON profile_certification_evidence;
DROP TRIGGER production_gate_receipts_immutable ON production_gate_receipts;
DROP TRIGGER production_gate_manifests_seal_once ON production_gate_manifests;
DROP FUNCTION vela_enforce_production_gate_manifest_seal();
DROP TRIGGER production_gate_manifests_immutable ON production_gate_manifests;
DROP FUNCTION vela_reject_catalog_evidence_mutation();

DROP TABLE catalog_evidence_protocol_transitions;
DROP TABLE catalog_evidence_protocol_state;
DROP TABLE rate_card_release_bindings;
DROP TABLE profile_certification_evidence;
DROP TABLE inference_backend_revisions;
DROP TABLE production_gate_receipts;
DROP TABLE production_gate_manifests;
DROP TYPE catalog_evidence_mode;
DROP TYPE production_gate_result;
DROP TYPE production_gate;
-- +goose StatementEnd
