-- +goose Up
-- +goose StatementBegin
CREATE TYPE slo_measurement_protocol_mode AS ENUM ('LEGACY', 'ENFORCED');
CREATE TYPE slo_job_outcome AS ENUM ('SUCCEEDED', 'FAILED', 'CUSTOMER_CANCELED');
CREATE TYPE slo_measurement_result AS ENUM ('PASS', 'FAIL', 'INSUFFICIENT_DATA');

CREATE TABLE statistical_slo_contract_revisions (
    id uuid PRIMARY KEY,
    target_matrix_revision text NOT NULL CHECK (
        length(target_matrix_revision) BETWEEN 1 AND 200
        AND btrim(target_matrix_revision) = target_matrix_revision
        AND target_matrix_revision !~ '[[:cntrl:]]'
    ),
    model_revision_id uuid NOT NULL REFERENCES model_revisions(id),
    generation_preset_revision_id uuid NOT NULL,
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    output_spec_id uuid NOT NULL REFERENCES output_specs(id),
    generation_count integer NOT NULL CHECK (generation_count BETWEEN 1 AND 16),
    p95_target_milliseconds bigint NOT NULL CHECK (p95_target_milliseconds > 0),
    success_target_ppm integer NOT NULL CHECK (success_target_ppm BETWEEN 1 AND 1000000),
    minimum_sample integer NOT NULL CHECK (minimum_sample BETWEEN 1 AND 1000000),
    confidence_method text NOT NULL DEFAULT 'wilson-one-sided-95-v1'
        CHECK (confidence_method = 'wilson-one-sided-95-v1'),
    one_sided_confidence_ppm integer NOT NULL DEFAULT 950000
        CHECK (one_sided_confidence_ppm = 950000),
    cancellation_policy text NOT NULL DEFAULT 'exclude-customer-cancellation-v1'
        CHECK (cancellation_policy = 'exclude-customer-cancellation-v1'),
    algorithm_revision text NOT NULL DEFAULT 'queued-visible-nearest-rank-wilson-v1'
        CHECK (algorithm_revision = 'queued-visible-nearest-rank-wilson-v1'),
    release_digest bytea NOT NULL CHECK (octet_length(release_digest) = 32),
    configuration_revision text NOT NULL CHECK (
        length(configuration_revision) BETWEEN 1 AND 300
        AND btrim(configuration_revision) = configuration_revision
        AND configuration_revision !~ '[[:cntrl:]]'
    ),
    launch_receipt_id uuid NOT NULL REFERENCES production_gate_receipts(id),
    activated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (
        model_revision_id, generation_preset_revision_id,
        service_class_revision_id, output_spec_id, generation_count
    ),
    UNIQUE (
        id, model_revision_id, generation_preset_revision_id,
        service_class_revision_id, output_spec_id, generation_count
    ),
    FOREIGN KEY (generation_preset_revision_id, model_revision_id)
        REFERENCES generation_preset_revisions(id, model_revision_id)
);

CREATE TABLE slo_measurement_protocol_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    mode slo_measurement_protocol_mode NOT NULL DEFAULT 'LEGACY',
    protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version IN (1, 2)),
    launch_receipt_id uuid REFERENCES production_gate_receipts(id),
    enforced_at timestamptz,
    CHECK (
        (mode = 'LEGACY' AND protocol_version = 1
            AND launch_receipt_id IS NULL AND enforced_at IS NULL)
        OR (mode = 'ENFORCED' AND protocol_version = 2
            AND launch_receipt_id IS NOT NULL AND enforced_at IS NOT NULL)
    )
);
INSERT INTO slo_measurement_protocol_state (singleton) VALUES (true);

CREATE TABLE slo_measurement_protocol_transitions (
    protocol_version integer PRIMARY KEY CHECK (protocol_version = 2),
    mode slo_measurement_protocol_mode NOT NULL CHECK (mode = 'ENFORCED'),
    launch_receipt_id uuid NOT NULL REFERENCES production_gate_receipts(id),
    transitioned_at timestamptz NOT NULL
);

CREATE TABLE job_slo_admissions (
    job_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    contract_revision_id uuid NOT NULL REFERENCES statistical_slo_contract_revisions(id),
    model_revision_id uuid NOT NULL,
    generation_preset_revision_id uuid NOT NULL,
    service_class_revision_id uuid NOT NULL,
    output_spec_id uuid NOT NULL,
    generation_count integer NOT NULL CHECK (generation_count BETWEEN 1 AND 16),
    queued_at timestamptz NOT NULL,
    job_expires_at timestamptz NOT NULL CHECK (job_expires_at > queued_at),
    captured_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, job_id),
    FOREIGN KEY (contract_revision_id, model_revision_id,
        generation_preset_revision_id, service_class_revision_id,
        output_spec_id, generation_count)
        REFERENCES statistical_slo_contract_revisions(
            id, model_revision_id, generation_preset_revision_id,
            service_class_revision_id, output_spec_id, generation_count
        )
);

CREATE TABLE job_slo_outcomes (
    job_id uuid PRIMARY KEY REFERENCES job_slo_admissions(job_id),
    outcome slo_job_outcome NOT NULL,
    terminal_at timestamptz NOT NULL,
    visible_completed_at timestamptz,
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (outcome = 'SUCCEEDED' AND visible_completed_at = terminal_at)
        OR (outcome <> 'SUCCEEDED' AND visible_completed_at IS NULL)
    )
);

CREATE TABLE slo_measurement_reports (
    id uuid PRIMARY KEY,
    contract_revision_id uuid NOT NULL REFERENCES statistical_slo_contract_revisions(id),
    window_start timestamptz NOT NULL,
    window_end timestamptz NOT NULL,
    algorithm_revision text NOT NULL CHECK (
        algorithm_revision = 'queued-visible-nearest-rank-wilson-v1'
    ),
    source_set_digest bytea NOT NULL CHECK (octet_length(source_set_digest) = 32),
    observation_count integer NOT NULL CHECK (observation_count >= 0),
    eligible_count integer NOT NULL CHECK (eligible_count >= 0),
    succeeded_count integer NOT NULL CHECK (succeeded_count >= 0),
    failed_count integer NOT NULL CHECK (failed_count >= 0),
    customer_canceled_count integer NOT NULL CHECK (customer_canceled_count >= 0),
    open_count integer NOT NULL CHECK (open_count >= 0),
    p95_milliseconds bigint CHECK (p95_milliseconds > 0),
    success_observed_ppm integer NOT NULL CHECK (success_observed_ppm BETWEEN 0 AND 1000000),
    success_lower_bound_ppm integer NOT NULL CHECK (success_lower_bound_ppm BETWEEN 0 AND 1000000),
    result slo_measurement_result NOT NULL,
    sealed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (contract_revision_id, window_start, window_end),
    CHECK (
        window_start = date_trunc('month', window_start AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
        AND window_end = window_start + interval '1 month'
    ),
    CHECK (observation_count = eligible_count + customer_canceled_count + open_count),
    CHECK (eligible_count = succeeded_count + failed_count),
    CHECK (
        (succeeded_count = 0 AND p95_milliseconds IS NULL)
        OR (succeeded_count > 0 AND p95_milliseconds IS NOT NULL)
    ),
    CHECK (result = 'INSUFFICIENT_DATA' OR (open_count = 0 AND eligible_count > 0))
);

CREATE FUNCTION vela_reject_slo_evidence_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'statistical_slo_evidence_is_immutable',
        MESSAGE = TG_TABLE_NAME || ' is immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_slo_evidence_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_slo_evidence_mutation() OWNER TO vela_slo_reporting_owner;

CREATE TRIGGER statistical_slo_contracts_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON statistical_slo_contract_revisions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_slo_evidence_mutation();
CREATE TRIGGER slo_protocol_transitions_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON slo_measurement_protocol_transitions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_slo_evidence_mutation();
CREATE TRIGGER job_slo_admissions_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON job_slo_admissions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_slo_evidence_mutation();
CREATE TRIGGER job_slo_outcomes_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON job_slo_outcomes
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_slo_evidence_mutation();
CREATE TRIGGER slo_measurement_reports_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON slo_measurement_reports
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_slo_evidence_mutation();

CREATE FUNCTION vela_enforce_slo_protocol_transition() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    IF OLD.mode <> 'LEGACY' OR NEW.mode <> 'ENFORCED'
       OR OLD.protocol_version <> 1 OR NEW.protocol_version <> 2
       OR NEW.launch_receipt_id IS NULL OR NEW.enforced_at IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_protocol_transition_is_invalid',
            MESSAGE = 'statistical SLO protocol transition is invalid';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_slo_protocol_transition() FROM PUBLIC;
ALTER FUNCTION vela_enforce_slo_protocol_transition() OWNER TO vela_slo_reporting_owner;
CREATE TRIGGER slo_protocol_transition_guard
BEFORE UPDATE ON slo_measurement_protocol_state
FOR EACH ROW EXECUTE FUNCTION vela_enforce_slo_protocol_transition();
CREATE TRIGGER slo_protocol_state_required
BEFORE DELETE OR TRUNCATE ON slo_measurement_protocol_state
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_slo_evidence_mutation();

CREATE FUNCTION vela_private.require_slo_launch_receipt(
    p_launch_receipt_id uuid,
    p_gate production_gate
) RETURNS TABLE (release_digest bytea, configuration_revision text)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    RETURN QUERY
    SELECT receipt.release_digest, receipt.configuration_revision
    FROM public.production_gate_receipts AS receipt
    JOIN public.production_gate_manifests AS manifest
      ON manifest.manifest_digest = receipt.manifest_digest
    WHERE receipt.id = p_launch_receipt_id
      AND receipt.gate = p_gate
      AND receipt.result = 'PASS'
      AND manifest.sealed_at IS NOT NULL
      AND manifest.release_digest = receipt.release_digest
      AND manifest.configuration_revision = receipt.configuration_revision;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_requires_sealed_launch_receipt',
            MESSAGE = 'statistical SLO operation requires a sealed matching Launch Receipt';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.require_slo_launch_receipt(uuid, production_gate)
    FROM PUBLIC;
ALTER FUNCTION vela_private.require_slo_launch_receipt(uuid, production_gate)
    OWNER TO vela_slo_reporting_owner;

CREATE FUNCTION vela_register_slo_contract(
    p_id uuid,
    p_target_matrix_revision text,
    p_model_revision_id uuid,
    p_generation_preset_revision_id uuid,
    p_service_class_revision_id uuid,
    p_output_spec_id uuid,
    p_generation_count integer,
    p_p95_target_milliseconds bigint,
    p_success_target_ppm integer,
    p_minimum_sample integer,
    p_launch_receipt_id uuid
) RETURNS statistical_slo_contract_revisions
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_release_digest bytea;
    v_configuration_revision text;
    v_contract public.statistical_slo_contract_revisions%ROWTYPE;
BEGIN
    SELECT receipt.release_digest, receipt.configuration_revision
    INTO v_release_digest, v_configuration_revision
    FROM vela_private.require_slo_launch_receipt(
        p_launch_receipt_id, 'preset-certification'
    ) AS receipt;
    INSERT INTO public.statistical_slo_contract_revisions (
        id, target_matrix_revision, model_revision_id,
        generation_preset_revision_id, service_class_revision_id,
        output_spec_id, generation_count, p95_target_milliseconds,
        success_target_ppm, minimum_sample, release_digest,
        configuration_revision, launch_receipt_id
    ) VALUES (
        p_id, p_target_matrix_revision, p_model_revision_id,
        p_generation_preset_revision_id, p_service_class_revision_id,
        p_output_spec_id, p_generation_count, p_p95_target_milliseconds,
        p_success_target_ppm, p_minimum_sample, v_release_digest,
        v_configuration_revision, p_launch_receipt_id
    )
    ON CONFLICT (id) DO NOTHING;
    SELECT * INTO v_contract
    FROM public.statistical_slo_contract_revisions WHERE id = p_id;
    IF v_contract.id IS NULL
       OR ROW(v_contract.target_matrix_revision, v_contract.model_revision_id,
            v_contract.generation_preset_revision_id,
            v_contract.service_class_revision_id, v_contract.output_spec_id,
            v_contract.generation_count, v_contract.p95_target_milliseconds,
            v_contract.success_target_ppm, v_contract.minimum_sample,
            v_contract.release_digest, v_contract.configuration_revision,
            v_contract.launch_receipt_id)
          IS DISTINCT FROM ROW(p_target_matrix_revision, p_model_revision_id,
            p_generation_preset_revision_id, p_service_class_revision_id,
            p_output_spec_id, p_generation_count, p_p95_target_milliseconds,
            p_success_target_ppm, p_minimum_sample, v_release_digest,
            v_configuration_revision, p_launch_receipt_id) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_contract_replay_mismatch',
            MESSAGE = 'statistical SLO contract replay does not match immutable evidence';
    END IF;
    RETURN v_contract;
END
$$;
REVOKE ALL ON FUNCTION vela_register_slo_contract(
    uuid, text, uuid, uuid, uuid, uuid, integer, bigint, integer, integer, uuid
) FROM PUBLIC;
ALTER FUNCTION vela_register_slo_contract(
    uuid, text, uuid, uuid, uuid, uuid, integer, bigint, integer, integer, uuid
) OWNER TO vela_slo_reporting_owner;

CREATE FUNCTION vela_private.capture_job_slo_admission() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_contract_id uuid;
    v_mode public.slo_measurement_protocol_mode;
BEGIN
    -- Serialize protocol activation with Job transactions without granting the
    -- reporting owner write privileges on the authoritative jobs table.
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(860037);
    SELECT contract.id INTO v_contract_id
    FROM public.statistical_slo_contract_revisions AS contract
    WHERE contract.model_revision_id = NEW.model_revision_id
      AND contract.generation_preset_revision_id = NEW.generation_preset_revision_id
      AND contract.service_class_revision_id = NEW.service_class_revision_id
      AND contract.output_spec_id = NEW.output_spec_id
      AND contract.generation_count = NEW.pricing_quantity;
    SELECT mode INTO v_mode FROM public.slo_measurement_protocol_state WHERE singleton;
    IF v_contract_id IS NULL THEN
        IF v_mode = 'ENFORCED' THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'admission_requires_statistical_slo_contract',
                MESSAGE = 'Admission requires an exact statistical SLO contract';
        END IF;
        RETURN NEW;
    END IF;
    INSERT INTO public.job_slo_admissions (
        job_id, organization_id, project_id, contract_revision_id,
        model_revision_id, generation_preset_revision_id,
        service_class_revision_id, output_spec_id, generation_count,
        queued_at, job_expires_at
    ) VALUES (
        NEW.id, NEW.organization_id, NEW.project_id, v_contract_id,
        NEW.model_revision_id, NEW.generation_preset_revision_id,
        NEW.service_class_revision_id, NEW.output_spec_id, NEW.pricing_quantity,
        NEW.created_at, NEW.job_expires_at
    ) ON CONFLICT (job_id) DO NOTHING;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_private.capture_job_slo_admission() FROM PUBLIC;
ALTER FUNCTION vela_private.capture_job_slo_admission() OWNER TO vela_slo_reporting_owner;
CREATE TRIGGER jobs_capture_slo_admission
AFTER INSERT ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_private.capture_job_slo_admission();

CREATE FUNCTION vela_private.capture_job_slo_outcome() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_terminal_at timestamptz;
    v_visible_completed_at timestamptz;
    v_outcome public.slo_job_outcome;
    v_existing public.job_slo_outcomes%ROWTYPE;
BEGIN
    IF NEW.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED') THEN
        RETURN NULL;
    END IF;
    PERFORM pg_catalog.pg_advisory_xact_lock_shared(860037);
    IF NOT EXISTS (SELECT 1 FROM public.job_slo_admissions WHERE job_id = NEW.id) THEN
        RETURN NULL;
    END IF;
    SELECT terminal_at INTO v_terminal_at
    FROM public.non_content_job_roots WHERE id = NEW.id;
    IF v_terminal_at IS NULL THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_terminal_clock_is_missing',
            MESSAGE = 'terminal statistical SLO observation requires canonical terminal time';
    END IF;
    IF NEW.state = 'SUCCEEDED' THEN
        v_outcome := 'SUCCEEDED';
        SELECT completed_at INTO v_visible_completed_at
        FROM public.visible_completions WHERE job_id = NEW.id;
        IF v_visible_completed_at IS NULL OR v_visible_completed_at <> v_terminal_at THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'statistical_slo_visible_completion_is_missing',
                MESSAGE = 'successful statistical SLO observation requires canonical Visible Completion';
        END IF;
    ELSIF NEW.state = 'FAILED' THEN
        v_outcome := 'FAILED';
    ELSE
        v_outcome := 'CUSTOMER_CANCELED';
    END IF;
    INSERT INTO public.job_slo_outcomes (
        job_id, outcome, terminal_at, visible_completed_at
    ) VALUES (NEW.id, v_outcome, v_terminal_at, v_visible_completed_at)
    ON CONFLICT (job_id) DO NOTHING;
    SELECT * INTO v_existing FROM public.job_slo_outcomes WHERE job_id = NEW.id;
    IF ROW(v_existing.outcome, v_existing.terminal_at, v_existing.visible_completed_at)
       IS DISTINCT FROM ROW(v_outcome, v_terminal_at, v_visible_completed_at) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_outcome_replay_mismatch',
            MESSAGE = 'statistical SLO outcome replay does not match immutable evidence';
    END IF;
    RETURN NULL;
END
$$;
REVOKE ALL ON FUNCTION vela_private.capture_job_slo_outcome() FROM PUBLIC;
ALTER FUNCTION vela_private.capture_job_slo_outcome() OWNER TO vela_slo_reporting_owner;
CREATE CONSTRAINT TRIGGER jobs_capture_slo_outcome
AFTER INSERT OR UPDATE OF state ON jobs
DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_private.capture_job_slo_outcome();

CREATE FUNCTION vela_enable_slo_measurement(
    p_launch_receipt_id uuid
) RETURNS TABLE (protocol_version integer, mode slo_measurement_protocol_mode, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public, vela_private
AS $$
DECLARE
    v_release_digest bytea;
    v_configuration_revision text;
    v_state public.slo_measurement_protocol_state%ROWTYPE;
    v_transitioned_at timestamptz;
BEGIN
    SELECT receipt.release_digest, receipt.configuration_revision
    INTO v_release_digest, v_configuration_revision
    FROM vela_private.require_slo_launch_receipt(
        p_launch_receipt_id, 'preset-certification'
    ) AS receipt;
    -- Match migration Down's jobs -> protocol-state order. The advisory lock
    -- drains trigger-based Job writers while retaining SELECT-only authority.
    LOCK TABLE public.jobs IN ACCESS SHARE MODE;
    PERFORM pg_catalog.pg_advisory_xact_lock(860037);
    SELECT * INTO v_state FROM public.slo_measurement_protocol_state
    WHERE singleton FOR UPDATE;
    IF v_state.mode = 'ENFORCED' THEN
        IF v_state.launch_receipt_id <> p_launch_receipt_id THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'statistical_slo_protocol_replay_mismatch',
                MESSAGE = 'statistical SLO protocol is already bound to another receipt';
        END IF;
        RETURN QUERY SELECT v_state.protocol_version, v_state.mode, true;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.rate_card_revisions AS rate_card
        JOIN public.rate_card_lines AS line ON line.rate_card_revision_id = rate_card.id
        CROSS JOIN generate_series(1, 16) AS generation(generation_count)
        WHERE rate_card.state = 'ACTIVE'
          AND NOT EXISTS (
              SELECT 1 FROM public.statistical_slo_contract_revisions AS contract
              WHERE contract.model_revision_id = line.model_revision_id
                AND contract.generation_preset_revision_id = line.generation_preset_revision_id
                AND contract.service_class_revision_id = line.service_class_revision_id
                AND contract.output_spec_id = line.output_spec_id
                AND contract.generation_count = generation.generation_count
                AND contract.release_digest = v_release_digest
                AND contract.configuration_revision = v_configuration_revision
          )
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_saleable_coverage_is_incomplete',
            MESSAGE = 'every saleable RateCard line requires exact statistical SLO coverage';
    END IF;
    INSERT INTO public.job_slo_admissions (
        job_id, organization_id, project_id, contract_revision_id,
        model_revision_id, generation_preset_revision_id,
        service_class_revision_id, output_spec_id, generation_count,
        queued_at, job_expires_at
    )
    SELECT job.id, job.organization_id, job.project_id, contract.id,
        job.model_revision_id, job.generation_preset_revision_id,
        job.service_class_revision_id, job.output_spec_id, job.pricing_quantity,
        job.created_at, job.job_expires_at
    FROM public.jobs AS job
    JOIN public.statistical_slo_contract_revisions AS contract
      ON contract.model_revision_id = job.model_revision_id
     AND contract.generation_preset_revision_id = job.generation_preset_revision_id
     AND contract.service_class_revision_id = job.service_class_revision_id
     AND contract.output_spec_id = job.output_spec_id
     AND contract.generation_count = job.pricing_quantity
    ON CONFLICT (job_id) DO NOTHING;
    IF EXISTS (
        SELECT 1 FROM public.jobs AS job
        LEFT JOIN public.job_slo_admissions AS admission ON admission.job_id = job.id
        WHERE admission.job_id IS NULL
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_legacy_backfill_is_incomplete',
            MESSAGE = 'retained Jobs cannot be fully assigned to statistical SLO contracts';
    END IF;
    INSERT INTO public.job_slo_outcomes (job_id, outcome, terminal_at, visible_completed_at)
    SELECT job.id,
        CASE job.state
            WHEN 'SUCCEEDED' THEN 'SUCCEEDED'::public.slo_job_outcome
            WHEN 'FAILED' THEN 'FAILED'::public.slo_job_outcome
            ELSE 'CUSTOMER_CANCELED'::public.slo_job_outcome
        END,
        root.terminal_at,
        completion.completed_at
    FROM public.jobs AS job
    JOIN public.job_slo_admissions AS admission ON admission.job_id = job.id
    JOIN public.non_content_job_roots AS root ON root.id = job.id
    LEFT JOIN public.visible_completions AS completion ON completion.job_id = job.id
    WHERE job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
      AND root.terminal_at IS NOT NULL
    ON CONFLICT (job_id) DO NOTHING;
    IF EXISTS (
        SELECT 1 FROM public.jobs AS job
        JOIN public.job_slo_admissions AS admission ON admission.job_id = job.id
        LEFT JOIN public.job_slo_outcomes AS outcome ON outcome.job_id = job.id
        WHERE job.state IN ('SUCCEEDED', 'FAILED', 'CANCELED')
          AND outcome.job_id IS NULL
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_terminal_backfill_is_incomplete',
            MESSAGE = 'retained terminal Jobs cannot be fully backfilled';
    END IF;
    v_transitioned_at := clock_timestamp();
    UPDATE public.slo_measurement_protocol_state
    SET mode = 'ENFORCED', protocol_version = 2,
        launch_receipt_id = p_launch_receipt_id, enforced_at = v_transitioned_at
    WHERE singleton;
    INSERT INTO public.slo_measurement_protocol_transitions (
        protocol_version, mode, launch_receipt_id, transitioned_at
    ) VALUES (2, 'ENFORCED', p_launch_receipt_id, v_transitioned_at);
    RETURN QUERY SELECT 2, 'ENFORCED'::public.slo_measurement_protocol_mode, false;
END
$$;
REVOKE ALL ON FUNCTION vela_enable_slo_measurement(uuid) FROM PUBLIC;
ALTER FUNCTION vela_enable_slo_measurement(uuid) OWNER TO vela_slo_reporting_owner;

CREATE FUNCTION vela_seal_slo_measurement(
    p_id uuid,
    p_contract_revision_id uuid,
    p_window_start timestamptz,
    p_window_end timestamptz
) RETURNS SETOF slo_measurement_reports
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_contract public.statistical_slo_contract_revisions%ROWTYPE;
    v_observation_count integer;
    v_succeeded_count integer;
    v_failed_count integer;
    v_canceled_count integer;
    v_open_count integer;
    v_eligible_count integer;
    v_p95 bigint;
    v_success_ppm integer := 0;
    v_lower_ppm integer := 0;
    v_result public.slo_measurement_result := 'INSUFFICIENT_DATA';
    v_source_digest bytea;
    v_existing public.slo_measurement_reports%ROWTYPE;
    v_z double precision := 1.6448536269514722;
    v_n double precision;
    v_p double precision;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM public.slo_measurement_protocol_state
        WHERE singleton AND mode = 'ENFORCED'
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_protocol_is_not_enforced',
            MESSAGE = 'statistical SLO reports cannot be sealed before protocol enforcement';
    END IF;
    SELECT * INTO v_contract FROM public.statistical_slo_contract_revisions
    WHERE id = p_contract_revision_id;
    IF v_contract.id IS NULL
       OR p_window_start <> date_trunc('month', p_window_start AT TIME ZONE 'UTC') AT TIME ZONE 'UTC'
       OR p_window_end <> p_window_start + interval '1 month'
       OR p_window_end > clock_timestamp() THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'statistical_slo_measurement_window_is_invalid',
            MESSAGE = 'statistical SLO measurement requires a closed UTC calendar month';
    END IF;
    SELECT count(*)::integer,
        count(*) FILTER (WHERE outcome.outcome = 'SUCCEEDED')::integer,
        count(*) FILTER (WHERE outcome.outcome = 'FAILED')::integer,
        count(*) FILTER (WHERE outcome.outcome = 'CUSTOMER_CANCELED')::integer,
        count(*) FILTER (WHERE outcome.job_id IS NULL)::integer,
        percentile_disc(0.95) WITHIN GROUP (
            ORDER BY floor(extract(epoch FROM
                (outcome.visible_completed_at - admission.queued_at)) * 1000)::bigint
        ) FILTER (WHERE outcome.outcome = 'SUCCEEDED'),
        sha256(convert_to(COALESCE(jsonb_agg(jsonb_build_object(
            'job_id', admission.job_id,
            'queued_at', admission.queued_at,
            'job_expires_at', admission.job_expires_at,
            'outcome', COALESCE(outcome.outcome::text, 'OPEN'),
            'terminal_at', outcome.terminal_at,
            'visible_completed_at', outcome.visible_completed_at
        ) ORDER BY admission.job_id)::text, '[]'), 'UTF8'))
    INTO v_observation_count, v_succeeded_count, v_failed_count,
        v_canceled_count, v_open_count, v_p95, v_source_digest
    FROM public.job_slo_admissions AS admission
    LEFT JOIN public.job_slo_outcomes AS outcome ON outcome.job_id = admission.job_id
    WHERE admission.contract_revision_id = p_contract_revision_id
      AND admission.queued_at >= p_window_start
      AND admission.queued_at < p_window_end;
    v_eligible_count := v_succeeded_count + v_failed_count;
    IF v_eligible_count > 0 THEN
        v_success_ppm := floor(v_succeeded_count::numeric * 1000000 / v_eligible_count)::integer;
        v_n := v_eligible_count::double precision;
        v_p := v_succeeded_count::double precision / v_n;
        v_lower_ppm := floor(GREATEST(0, (
            v_p + (v_z * v_z) / (2 * v_n)
            - v_z * sqrt((v_p * (1 - v_p) + (v_z * v_z) / (4 * v_n)) / v_n)
        ) / (1 + (v_z * v_z) / v_n)) * 1000000)::integer;
    END IF;
    IF v_open_count = 0 AND v_eligible_count >= v_contract.minimum_sample
       AND v_succeeded_count > 0 THEN
        v_result := 'FAIL';
        IF v_p95 <= v_contract.p95_target_milliseconds
           AND v_lower_ppm >= v_contract.success_target_ppm THEN
            v_result := 'PASS';
        END IF;
    END IF;
    INSERT INTO public.slo_measurement_reports (
        id, contract_revision_id, window_start, window_end,
        algorithm_revision, source_set_digest, observation_count,
        eligible_count, succeeded_count, failed_count,
        customer_canceled_count, open_count, p95_milliseconds,
        success_observed_ppm, success_lower_bound_ppm, result
    ) VALUES (
        p_id, p_contract_revision_id, p_window_start, p_window_end,
        v_contract.algorithm_revision, v_source_digest, v_observation_count,
        v_eligible_count, v_succeeded_count, v_failed_count,
        v_canceled_count, v_open_count, v_p95,
        v_success_ppm, v_lower_ppm, v_result
    ) ON CONFLICT (contract_revision_id, window_start, window_end) DO NOTHING;
    SELECT * INTO v_existing FROM public.slo_measurement_reports
    WHERE contract_revision_id = p_contract_revision_id
      AND window_start = p_window_start AND window_end = p_window_end;
    IF v_existing.id <> p_id
       OR ROW(v_existing.source_set_digest, v_existing.observation_count,
            v_existing.eligible_count, v_existing.succeeded_count,
            v_existing.failed_count, v_existing.customer_canceled_count,
            v_existing.open_count, v_existing.p95_milliseconds,
            v_existing.success_observed_ppm,
            v_existing.success_lower_bound_ppm, v_existing.result)
          IS DISTINCT FROM ROW(v_source_digest, v_observation_count,
            v_eligible_count, v_succeeded_count, v_failed_count,
            v_canceled_count, v_open_count, v_p95,
            v_success_ppm, v_lower_ppm, v_result) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_measurement_replay_mismatch',
            MESSAGE = 'statistical SLO measurement replay does not match source observations';
    END IF;
    RETURN NEXT v_existing;
END
$$;
REVOKE ALL ON FUNCTION vela_seal_slo_measurement(
    uuid, uuid, timestamptz, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_seal_slo_measurement(uuid, uuid, timestamptz, timestamptz)
    OWNER TO vela_slo_reporting_owner;

CREATE FUNCTION vela_get_slo_measurement(
    p_id uuid
) RETURNS SETOF slo_measurement_reports
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT * FROM public.slo_measurement_reports WHERE id = p_id
$$;
REVOKE ALL ON FUNCTION vela_get_slo_measurement(uuid) FROM PUBLIC;
ALTER FUNCTION vela_get_slo_measurement(uuid) OWNER TO vela_slo_reporting_owner;

ALTER TABLE statistical_slo_contract_revisions OWNER TO vela_slo_reporting_owner;
ALTER TABLE slo_measurement_protocol_state OWNER TO vela_slo_reporting_owner;
ALTER TABLE slo_measurement_protocol_transitions OWNER TO vela_slo_reporting_owner;
ALTER TABLE job_slo_admissions OWNER TO vela_slo_reporting_owner;
ALTER TABLE job_slo_outcomes OWNER TO vela_slo_reporting_owner;
ALTER TABLE slo_measurement_reports OWNER TO vela_slo_reporting_owner;

ALTER TABLE job_slo_admissions ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_slo_admissions FORCE ROW LEVEL SECURITY;
ALTER TABLE job_slo_outcomes ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_slo_outcomes FORCE ROW LEVEL SECURITY;
CREATE POLICY job_slo_admissions_owner_all ON job_slo_admissions
    FOR ALL TO vela_slo_reporting_owner USING (true) WITH CHECK (true);
CREATE POLICY job_slo_outcomes_owner_all ON job_slo_outcomes
    FOR ALL TO vela_slo_reporting_owner USING (true) WITH CHECK (true);

GRANT USAGE ON SCHEMA public TO vela_slo_reporting;
GRANT USAGE ON SCHEMA vela_private TO vela_slo_reporting_owner;
GRANT USAGE ON TYPE production_gate, production_gate_result,
    slo_measurement_protocol_mode, slo_job_outcome, slo_measurement_result
    TO vela_slo_reporting_owner;
GRANT SELECT ON production_gate_manifests, production_gate_receipts,
    model_revisions, generation_preset_revisions, service_class_revisions,
    output_specs, rate_card_revisions, rate_card_lines, jobs,
    non_content_job_roots, visible_completions TO vela_slo_reporting_owner;
GRANT EXECUTE ON FUNCTION vela_register_slo_contract(
    uuid, text, uuid, uuid, uuid, uuid, integer, bigint, integer, integer, uuid
) TO vela_slo_reporting;
GRANT EXECUTE ON FUNCTION vela_enable_slo_measurement(uuid) TO vela_slo_reporting;
GRANT EXECUTE ON FUNCTION vela_seal_slo_measurement(
    uuid, uuid, timestamptz, timestamptz
) TO vela_slo_reporting;
GRANT EXECUTE ON FUNCTION vela_get_slo_measurement(uuid) TO vela_slo_reporting;
GRANT SELECT ON statistical_slo_contract_revisions,
    slo_measurement_protocol_state, slo_measurement_protocol_transitions,
    slo_measurement_reports TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    -- Serialize with in-flight Job writers before their deferred SLO triggers
    -- can need any table locked below.
    LOCK TABLE jobs IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE slo_measurement_protocol_state IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE statistical_slo_contract_revisions IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE job_slo_admissions IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE job_slo_outcomes IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE slo_measurement_reports IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM slo_measurement_protocol_state WHERE mode <> 'LEGACY')
       OR EXISTS (SELECT 1 FROM statistical_slo_contract_revisions)
       OR EXISTS (SELECT 1 FROM job_slo_admissions)
       OR EXISTS (SELECT 1 FROM job_slo_outcomes)
       OR EXISTS (SELECT 1 FROM slo_measurement_reports) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'statistical_slo_rollback_is_unsafe',
            MESSAGE = 'statistical SLO contracts or evidence exist';
    END IF;
END
$$;

REVOKE SELECT ON statistical_slo_contract_revisions,
    slo_measurement_protocol_state, slo_measurement_protocol_transitions,
    slo_measurement_reports FROM vela_internal;
REVOKE EXECUTE ON FUNCTION vela_get_slo_measurement(uuid) FROM vela_slo_reporting;
REVOKE EXECUTE ON FUNCTION vela_seal_slo_measurement(
    uuid, uuid, timestamptz, timestamptz
) FROM vela_slo_reporting;
REVOKE EXECUTE ON FUNCTION vela_enable_slo_measurement(uuid) FROM vela_slo_reporting;
REVOKE EXECUTE ON FUNCTION vela_register_slo_contract(
    uuid, text, uuid, uuid, uuid, uuid, integer, bigint, integer, integer, uuid
) FROM vela_slo_reporting;
REVOKE SELECT ON production_gate_manifests, production_gate_receipts,
    model_revisions, generation_preset_revisions, service_class_revisions,
    output_specs, rate_card_revisions, rate_card_lines, jobs,
    non_content_job_roots, visible_completions FROM vela_slo_reporting_owner;
REVOKE USAGE ON TYPE production_gate, production_gate_result,
    slo_measurement_protocol_mode, slo_job_outcome, slo_measurement_result
    FROM vela_slo_reporting_owner;
REVOKE USAGE ON SCHEMA vela_private FROM vela_slo_reporting_owner;
REVOKE USAGE ON SCHEMA public FROM vela_slo_reporting;

DROP TRIGGER jobs_capture_slo_outcome ON jobs;
DROP TRIGGER jobs_capture_slo_admission ON jobs;
DROP FUNCTION vela_get_slo_measurement(uuid);
DROP FUNCTION vela_seal_slo_measurement(uuid, uuid, timestamptz, timestamptz);
DROP FUNCTION vela_enable_slo_measurement(uuid);
DROP FUNCTION vela_private.capture_job_slo_outcome();
DROP FUNCTION vela_private.capture_job_slo_admission();
DROP FUNCTION vela_register_slo_contract(
    uuid, text, uuid, uuid, uuid, uuid, integer, bigint, integer, integer, uuid
);
DROP FUNCTION vela_private.require_slo_launch_receipt(uuid, production_gate);

DROP TRIGGER slo_protocol_state_required ON slo_measurement_protocol_state;
DROP TRIGGER slo_protocol_transition_guard ON slo_measurement_protocol_state;
DROP FUNCTION vela_enforce_slo_protocol_transition();
DROP TRIGGER slo_measurement_reports_immutable ON slo_measurement_reports;
DROP TRIGGER job_slo_outcomes_immutable ON job_slo_outcomes;
DROP TRIGGER job_slo_admissions_immutable ON job_slo_admissions;
DROP TRIGGER slo_protocol_transitions_immutable ON slo_measurement_protocol_transitions;
DROP TRIGGER statistical_slo_contracts_immutable ON statistical_slo_contract_revisions;
DROP FUNCTION vela_reject_slo_evidence_mutation();

DROP POLICY job_slo_outcomes_owner_all ON job_slo_outcomes;
DROP POLICY job_slo_admissions_owner_all ON job_slo_admissions;
DROP TABLE slo_measurement_reports;
DROP TABLE job_slo_outcomes;
DROP TABLE job_slo_admissions;
DROP TABLE slo_measurement_protocol_transitions;
DROP TABLE slo_measurement_protocol_state;
DROP TABLE statistical_slo_contract_revisions;
DROP TYPE slo_measurement_result;
DROP TYPE slo_job_outcome;
DROP TYPE slo_measurement_protocol_mode;
-- +goose StatementEnd
