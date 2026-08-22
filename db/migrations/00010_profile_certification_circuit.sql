-- +goose Up
-- +goose StatementBegin
ALTER TABLE service_class_revisions
    ADD COLUMN circuit_fingerprint_window_seconds integer NOT NULL DEFAULT 3600
        CHECK (circuit_fingerprint_window_seconds BETWEEN 1 AND 604800),
    ADD COLUMN circuit_min_distinct_healthy_workers integer NOT NULL DEFAULT 2
        CHECK (circuit_min_distinct_healthy_workers BETWEEN 2 AND 64);

ALTER TABLE jobs
    ADD COLUMN execution_circuit_fingerprint_window_seconds integer NOT NULL DEFAULT 3600
        CHECK (execution_circuit_fingerprint_window_seconds BETWEEN 1 AND 604800),
    ADD COLUMN execution_circuit_min_distinct_healthy_workers integer NOT NULL DEFAULT 2
        CHECK (execution_circuit_min_distinct_healthy_workers BETWEEN 2 AND 64);

CREATE FUNCTION vela_reject_job_circuit_policy_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.execution_circuit_fingerprint_window_seconds IS DISTINCT FROM
           OLD.execution_circuit_fingerprint_window_seconds
       OR NEW.execution_circuit_min_distinct_healthy_workers IS DISTINCT FROM
           OLD.execution_circuit_min_distinct_healthy_workers
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'jobs_circuit_policy_snapshot_immutable',
            MESSAGE = 'Job circuit policy snapshot is immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_job_circuit_policy_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_job_circuit_policy_mutation() OWNER TO vela_internal;
CREATE TRIGGER jobs_circuit_policy_snapshot_immutable
BEFORE UPDATE OF
    execution_circuit_fingerprint_window_seconds,
    execution_circuit_min_distinct_healthy_workers
ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_reject_job_circuit_policy_mutation();

ALTER TABLE attempts ADD COLUMN profile_certification_id uuid;
UPDATE attempts AS attempt
SET profile_certification_id = certification.id
FROM jobs AS job, profile_certifications AS certification
WHERE job.id = attempt.job_id
  AND certification.execution_profile_revision_id = attempt.execution_profile_revision_id
  AND certification.model_revision_id = job.model_revision_id
  AND certification.generation_preset_revision_id = job.generation_preset_revision_id
  AND certification.output_spec_id = job.output_spec_id;
ALTER TABLE attempts
    ALTER COLUMN profile_certification_id SET NOT NULL,
    ADD CONSTRAINT attempts_profile_certification_fk
        FOREIGN KEY (profile_certification_id) REFERENCES profile_certifications(id);

CREATE FUNCTION vela_bind_attempt_profile_certification() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_profile_certification_id uuid;
BEGIN
    SELECT certification.id INTO v_profile_certification_id
    FROM public.jobs AS job
    JOIN public.profile_certifications AS certification
      ON certification.model_revision_id = job.model_revision_id
     AND certification.generation_preset_revision_id = job.generation_preset_revision_id
     AND certification.output_spec_id = job.output_spec_id
     AND certification.execution_profile_revision_id = NEW.execution_profile_revision_id
     AND certification.state = 'ACTIVE'
     AND certification.invalidated_at IS NULL
    WHERE job.id = NEW.job_id
    FOR SHARE OF certification;
    IF NOT FOUND
       OR (
           NEW.profile_certification_id IS NOT NULL
           AND NEW.profile_certification_id <> v_profile_certification_id
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempts_profile_certification_active',
            MESSAGE = 'Attempt requires the exact active ProfileCertification';
    END IF;
    NEW.profile_certification_id := v_profile_certification_id;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_bind_attempt_profile_certification() FROM PUBLIC;
CREATE TRIGGER attempts_bind_profile_certification
BEFORE INSERT ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_bind_attempt_profile_certification();

CREATE FUNCTION vela_reject_attempt_profile_certification_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.profile_certification_id IS DISTINCT FROM OLD.profile_certification_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempts_profile_certification_immutable',
            MESSAGE = 'Attempt ProfileCertification identity is immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_attempt_profile_certification_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_attempt_profile_certification_mutation() OWNER TO vela_internal;
CREATE TRIGGER attempts_profile_certification_immutable
BEFORE UPDATE OF profile_certification_id ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_reject_attempt_profile_certification_mutation();

CREATE TABLE profile_circuit_protocol_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    require_circuit_aggregation boolean NOT NULL DEFAULT false,
    protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version > 0),
    transition_receipt text CHECK (
        transition_receipt IS NULL
        OR (length(transition_receipt) BETWEEN 1 AND 1000 AND btrim(transition_receipt) <> '')
    ),
    transitioned_at timestamptz,
    CHECK (
        (
            protocol_version = 1
            AND NOT require_circuit_aggregation
            AND transition_receipt IS NULL
            AND transitioned_at IS NULL
        )
        OR (
            protocol_version > 1
            AND transition_receipt IS NOT NULL
            AND transitioned_at IS NOT NULL
        )
    )
);
INSERT INTO profile_circuit_protocol_state (singleton) VALUES (true);

CREATE TABLE profile_circuit_protocol_transitions (
    protocol_version integer PRIMARY KEY CHECK (protocol_version > 1),
    require_circuit_aggregation boolean NOT NULL,
    transition_receipt text NOT NULL CHECK (
        length(transition_receipt) BETWEEN 1 AND 1000
        AND btrim(transition_receipt) <> ''
    ),
    transitioned_at timestamptz NOT NULL
);

CREATE FUNCTION vela_reject_profile_circuit_protocol_history_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'profile_circuit_protocol_history_immutable',
        MESSAGE = 'Profile circuit protocol transition history is append-only';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_profile_circuit_protocol_history_mutation() FROM PUBLIC;
CREATE TRIGGER profile_circuit_protocol_history_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON profile_circuit_protocol_transitions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_profile_circuit_protocol_history_mutation();

CREATE FUNCTION vela_validate_profile_circuit_protocol_transition() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_state public.profile_circuit_protocol_state%ROWTYPE;
BEGIN
    SELECT state.* INTO v_state
    FROM public.profile_circuit_protocol_state AS state
    WHERE state.singleton
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'profile_circuit_protocol_state_required',
            MESSAGE = 'Profile circuit protocol state is missing';
    END IF;
    IF NEW.protocol_version <> v_state.protocol_version + 1
       OR NEW.transitioned_at < COALESCE(v_state.transitioned_at, '-infinity'::timestamptz)
       OR NEW.transitioned_at > clock_timestamp()
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'profile_circuit_protocol_history_contiguous',
            MESSAGE = 'Profile circuit protocol transition history must extend current state';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_profile_circuit_protocol_transition() FROM PUBLIC;
CREATE TRIGGER profile_circuit_protocol_validate_transition
BEFORE INSERT ON profile_circuit_protocol_transitions
FOR EACH ROW EXECUTE FUNCTION vela_validate_profile_circuit_protocol_transition();

CREATE FUNCTION vela_enforce_profile_circuit_protocol_state_transition() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.protocol_version <> OLD.protocol_version + 1
       OR NOT EXISTS (
           SELECT 1
           FROM public.profile_circuit_protocol_transitions AS transition
           WHERE transition.protocol_version = NEW.protocol_version
             AND transition.require_circuit_aggregation = NEW.require_circuit_aggregation
             AND transition.transition_receipt = NEW.transition_receipt
             AND transition.transitioned_at = NEW.transitioned_at
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'profile_circuit_protocol_state_transition_required',
            MESSAGE = 'Profile circuit protocol state must follow immutable transition history';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_profile_circuit_protocol_state_transition() FROM PUBLIC;
CREATE TRIGGER profile_circuit_protocol_enforce_transition
BEFORE UPDATE ON profile_circuit_protocol_state
FOR EACH ROW EXECUTE FUNCTION vela_enforce_profile_circuit_protocol_state_transition();

CREATE FUNCTION vela_reject_profile_circuit_protocol_state_delete() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'profile_circuit_protocol_state_required',
        MESSAGE = 'Profile circuit protocol state cannot be removed';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_profile_circuit_protocol_state_delete() FROM PUBLIC;
CREATE TRIGGER profile_circuit_protocol_state_required
BEFORE DELETE OR TRUNCATE ON profile_circuit_protocol_state
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_profile_circuit_protocol_state_delete();

CREATE FUNCTION vela_transition_profile_circuit_protocol(
    p_require_circuit_aggregation boolean,
    p_receipt text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_protocol_version integer;
    v_transitioned_at timestamptz;
BEGIN
    IF p_receipt IS NULL
       OR length(p_receipt) NOT BETWEEN 1 AND 1000
       OR btrim(p_receipt) = ''
    THEN
        RAISE EXCEPTION 'Profile circuit protocol transition receipt is required'
            USING ERRCODE = '22023';
    END IF;

    LOCK TABLE public.execution_failure_decisions IN SHARE ROW EXCLUSIVE MODE;
    SELECT state.protocol_version INTO v_protocol_version
    FROM public.profile_circuit_protocol_state AS state
    WHERE state.singleton
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'profile_circuit_protocol_state_required',
            MESSAGE = 'Profile circuit protocol state is missing';
    END IF;

    v_protocol_version := v_protocol_version + 1;
    v_transitioned_at := clock_timestamp();
    INSERT INTO public.profile_circuit_protocol_transitions (
        protocol_version,
        require_circuit_aggregation,
        transition_receipt,
        transitioned_at
    ) VALUES (
        v_protocol_version,
        p_require_circuit_aggregation,
        p_receipt,
        v_transitioned_at
    );
    UPDATE public.profile_circuit_protocol_state
    SET require_circuit_aggregation = p_require_circuit_aggregation,
        protocol_version = v_protocol_version,
        transition_receipt = p_receipt,
        transitioned_at = v_transitioned_at
    WHERE singleton;
END
$$;
REVOKE ALL ON FUNCTION vela_transition_profile_circuit_protocol(boolean, text) FROM PUBLIC;

CREATE FUNCTION vela_lock_profile_circuit_protocol() RETURNS TABLE (
    require_circuit_aggregation boolean,
    protocol_version integer
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT state.require_circuit_aggregation, state.protocol_version
    FROM public.profile_circuit_protocol_state AS state
    WHERE state.singleton
    FOR SHARE
$$;
REVOKE ALL ON FUNCTION vela_lock_profile_circuit_protocol() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_lock_profile_circuit_protocol() TO vela_internal;

ALTER TABLE execution_failure_decisions
    ADD COLUMN circuit_protocol_version smallint NOT NULL DEFAULT 1
        CHECK (circuit_protocol_version IN (1, 2)),
    ADD COLUMN worker_was_healthy boolean NOT NULL DEFAULT false;

CREATE FUNCTION vela_guard_profile_circuit_failure_writer() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_required boolean;
BEGIN
    SELECT state.require_circuit_aggregation INTO STRICT v_required
    FROM public.profile_circuit_protocol_state AS state
    WHERE state.singleton
    FOR SHARE;
    IF (v_required AND NEW.circuit_protocol_version <> 2)
       OR (NOT v_required AND NEW.circuit_protocol_version <> 1)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'execution_failure_decisions_profile_circuit_protocol',
            MESSAGE = 'failure decision writer does not match the active Profile circuit protocol';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_profile_circuit_failure_writer() FROM PUBLIC;
CREATE TRIGGER execution_failure_decisions_profile_circuit_protocol
BEFORE INSERT ON execution_failure_decisions
FOR EACH ROW EXECUTE FUNCTION vela_guard_profile_circuit_failure_writer();

CREATE TABLE profile_certification_circuit_openings (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    profile_certification_id uuid NOT NULL UNIQUE REFERENCES profile_certifications(id),
    execution_profile_revision_id uuid NOT NULL REFERENCES execution_profile_revisions(id),
    triggering_execution_failure_decision_id uuid NOT NULL UNIQUE
        REFERENCES execution_failure_decisions(id),
    triggering_job_id uuid NOT NULL,
    triggering_attempt_id uuid NOT NULL,
    triggering_worker_id uuid NOT NULL,
    triggering_worker_epoch bigint NOT NULL CHECK (triggering_worker_epoch > 0),
    triggering_attempt_fence bigint NOT NULL CHECK (triggering_attempt_fence > 0),
    failure_class text NOT NULL CHECK (failure_class ~ '^[A-Z][A-Z0-9_]{0,99}$'),
    failure_fingerprint text NOT NULL CHECK (
        failure_fingerprint ~ '^[A-Za-z0-9][A-Za-z0-9._:/-]{0,199}$'
    ),
    inference_backend_revision text NOT NULL
        CHECK (length(inference_backend_revision) BETWEEN 1 AND 200),
    policy_fingerprint_window_seconds integer NOT NULL
        CHECK (policy_fingerprint_window_seconds BETWEEN 1 AND 604800),
    policy_min_distinct_healthy_workers integer NOT NULL
        CHECK (policy_min_distinct_healthy_workers BETWEEN 2 AND 64),
    observed_distinct_healthy_workers integer NOT NULL,
    evidence_window_started_at timestamptz NOT NULL,
    opened_at timestamptz NOT NULL,
    FOREIGN KEY (organization_id, project_id, triggering_job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (
        organization_id, project_id, triggering_attempt_id, triggering_job_id,
        triggering_worker_id, triggering_worker_epoch, triggering_attempt_fence
    ) REFERENCES attempts (
        organization_id, project_id, id, job_id,
        worker_id, worker_epoch, fence
    ),
    CHECK (observed_distinct_healthy_workers >= policy_min_distinct_healthy_workers),
    CHECK (evidence_window_started_at = opened_at
        - policy_fingerprint_window_seconds::bigint * interval '1 second')
);

CREATE FUNCTION vela_validate_profile_certification_circuit_opening() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM public.profile_certifications AS certification
        JOIN public.attempts AS attempt
          ON attempt.id = NEW.triggering_attempt_id
         AND attempt.execution_profile_revision_id = NEW.execution_profile_revision_id
         AND attempt.profile_certification_id = NEW.profile_certification_id
        JOIN public.jobs AS job
          ON job.id = NEW.triggering_job_id
         AND job.organization_id = NEW.organization_id
         AND job.project_id = NEW.project_id
         AND job.id = attempt.job_id
         AND job.model_revision_id = certification.model_revision_id
         AND job.generation_preset_revision_id = certification.generation_preset_revision_id
         AND job.output_spec_id = certification.output_spec_id
        JOIN public.execution_failure_decisions AS decision
          ON decision.id = NEW.triggering_execution_failure_decision_id
         AND decision.organization_id = NEW.organization_id
         AND decision.project_id = NEW.project_id
         AND decision.job_id = NEW.triggering_job_id
         AND decision.attempt_id = NEW.triggering_attempt_id
         AND decision.worker_id = NEW.triggering_worker_id
         AND decision.worker_epoch = NEW.triggering_worker_epoch
         AND decision.attempt_fence = NEW.triggering_attempt_fence
         AND decision.failure_class = NEW.failure_class
         AND decision.failure_fingerprint = NEW.failure_fingerprint
         AND decision.inference_backend_revision = NEW.inference_backend_revision
         AND decision.decided_at = NEW.opened_at
         AND decision.source = 'WORKER_REPORTED'
         AND decision.worker_was_healthy
         AND decision.circuit_protocol_version = 2
        WHERE certification.id = NEW.profile_certification_id
          AND certification.execution_profile_revision_id = NEW.execution_profile_revision_id
          AND certification.state = 'INVALID'
          AND certification.invalidated_at = NEW.opened_at
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'profile_certification_circuit_opening_identity',
            MESSAGE = 'ProfileCertification circuit opening identity is inconsistent';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_profile_certification_circuit_opening() FROM PUBLIC;
CREATE TRIGGER profile_certification_circuit_opening_identity
BEFORE INSERT ON profile_certification_circuit_openings
FOR EACH ROW EXECUTE FUNCTION vela_validate_profile_certification_circuit_opening();

CREATE FUNCTION vela_reject_profile_certification_circuit_opening_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'profile_certification_circuit_openings_immutable',
        MESSAGE = 'ProfileCertification circuit opening receipts are immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_profile_certification_circuit_opening_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_profile_certification_circuit_opening_mutation() OWNER TO vela_internal;
CREATE TRIGGER profile_certification_circuit_openings_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON profile_certification_circuit_openings
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_profile_certification_circuit_opening_mutation();

ALTER TABLE profile_certifications
    ADD CONSTRAINT profile_certifications_invalidation_consistent CHECK (
        (state = 'INVALID' AND invalidated_at IS NOT NULL)
        OR (state <> 'INVALID' AND invalidated_at IS NULL)
    );

CREATE FUNCTION vela_reject_profile_certification_reactivation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF OLD.invalidated_at IS NOT NULL
       AND (
           NEW.state IS DISTINCT FROM OLD.state
           OR NEW.invalidated_at IS DISTINCT FROM OLD.invalidated_at
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'profile_certifications_invalidation_immutable',
            MESSAGE = 'an invalidated ProfileCertification cannot be reactivated or re-timestamped';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_profile_certification_reactivation() FROM PUBLIC;
ALTER FUNCTION vela_reject_profile_certification_reactivation() OWNER TO vela_internal;
CREATE TRIGGER profile_certifications_invalidation_immutable
BEFORE UPDATE OF state, invalidated_at ON profile_certifications
FOR EACH ROW EXECUTE FUNCTION vela_reject_profile_certification_reactivation();

CREATE OR REPLACE FUNCTION vela_lock_compatible_pool(
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
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_candidate record;
    v_admission_open boolean;
    v_queued_count integer;
    v_queued_limit integer;
    v_retry_after_seconds integer;
BEGIN
    FOR v_candidate IN
        SELECT
            wp.id AS pool_id,
            CASE
                WHEN wp.queued_limit = 0 THEN 1::numeric
                ELSE (wp.queued_count - wp.retry_wait_count)::numeric
                    / wp.queued_limit::numeric
            END AS load_ratio,
            wp.stable_id
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
          AND wp.queued_count - wp.retry_wait_count < wp.queued_limit
        GROUP BY
            wp.id,
            wp.queued_count,
            wp.retry_wait_count,
            wp.queued_limit,
            wp.stable_id
        ORDER BY load_ratio, wp.stable_id
    LOOP
        BEGIN
            SELECT
                wp.admission_open,
                wp.queued_count - wp.retry_wait_count,
                wp.queued_limit,
                wp.retry_after_seconds
            INTO
                v_admission_open,
                v_queued_count,
                v_queued_limit,
                v_retry_after_seconds
            FROM public.worker_pools AS wp
            WHERE wp.id = v_candidate.pool_id
              AND wp.admission_open
              AND wp.queued_count - wp.retry_wait_count < wp.queued_limit
            FOR UPDATE;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'P0002',
                    MESSAGE = 'compatible Worker pool candidate changed while waiting';
            END IF;

            PERFORM pc.id
            FROM public.profile_certifications AS pc
            JOIN public.execution_profile_revisions AS epr
              ON epr.id = pc.execution_profile_revision_id
            WHERE pc.model_revision_id = p_model_revision_id
              AND pc.generation_preset_revision_id = p_generation_preset_revision_id
              AND pc.output_spec_id = p_output_spec_id
              AND pc.state = 'ACTIVE'
              AND pc.invalidated_at IS NULL
              AND epr.worker_pool_id = v_candidate.pool_id
              AND epr.state = 'ACTIVE'
            ORDER BY pc.id
            LIMIT 1
            FOR SHARE OF pc, epr;
            IF NOT FOUND THEN
                RAISE EXCEPTION USING
                    ERRCODE = 'P0002',
                    MESSAGE = 'compatible ProfileCertification changed while waiting';
            END IF;

            id := v_candidate.pool_id;
            admission_open := v_admission_open;
            queued_count := v_queued_count;
            queued_limit := v_queued_limit;
            retry_after_seconds := v_retry_after_seconds;
            RETURN NEXT;
            RETURN;
        EXCEPTION WHEN no_data_found THEN
            NULL;
        END;
    END LOOP;
END
$$;

ALTER TABLE profile_circuit_protocol_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_circuit_protocol_state FORCE ROW LEVEL SECURITY;
ALTER TABLE profile_circuit_protocol_transitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_circuit_protocol_transitions FORCE ROW LEVEL SECURITY;
ALTER TABLE profile_certification_circuit_openings ENABLE ROW LEVEL SECURITY;
ALTER TABLE profile_certification_circuit_openings FORCE ROW LEVEL SECURITY;

REVOKE ALL ON profile_circuit_protocol_state,
    profile_circuit_protocol_transitions,
    profile_certification_circuit_openings FROM PUBLIC;
GRANT SELECT, INSERT ON profile_certification_circuit_openings TO vela_internal;

DROP FUNCTION vela_resolve_active_sku(text, text, text, text);
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
    currency text,
    circuit_fingerprint_window_seconds integer,
    circuit_min_distinct_healthy_workers integer
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
        rcl.currency,
        scr.circuit_fingerprint_window_seconds,
        scr.circuit_min_distinct_healthy_workers
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
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE
    execution_failure_decisions,
    profile_certifications,
    service_class_revisions,
    jobs,
    profile_circuit_protocol_state,
    profile_circuit_protocol_transitions,
    profile_certification_circuit_openings
IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM profile_circuit_protocol_transitions)
       OR EXISTS (SELECT 1 FROM profile_certification_circuit_openings)
       OR EXISTS (
           SELECT 1 FROM execution_failure_decisions
           WHERE circuit_protocol_version <> 1 OR worker_was_healthy
       )
       OR EXISTS (
           SELECT 1 FROM service_class_revisions
           WHERE circuit_fingerprint_window_seconds <> 3600
              OR circuit_min_distinct_healthy_workers <> 2
       )
       OR EXISTS (
           SELECT 1 FROM jobs
           WHERE execution_circuit_fingerprint_window_seconds <> 3600
              OR execution_circuit_min_distinct_healthy_workers <> 2
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'profile_certification_circuit_contract_requires_preserved_evidence',
            MESSAGE = 'Migration 00010 cannot contract after protocol transition, custom policy, or durable circuit evidence';
    END IF;
END
$$;

DROP FUNCTION vela_resolve_active_sku(text, text, text, text);
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

CREATE OR REPLACE FUNCTION vela_lock_compatible_pool(
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
        wp.queued_count - wp.retry_wait_count,
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
      AND wp.queued_count - wp.retry_wait_count < wp.queued_limit
    ORDER BY
        CASE
            WHEN wp.queued_limit = 0 THEN 1::numeric
            ELSE (wp.queued_count - wp.retry_wait_count)::numeric / wp.queued_limit::numeric
        END,
        wp.stable_id
    LIMIT 1
    FOR SHARE OF pc, epr
    FOR UPDATE OF wp
$$;

DROP TRIGGER profile_certifications_invalidation_immutable ON profile_certifications;
DROP FUNCTION vela_reject_profile_certification_reactivation();
ALTER TABLE profile_certifications
    DROP CONSTRAINT profile_certifications_invalidation_consistent;

DROP TRIGGER profile_certification_circuit_openings_immutable
    ON profile_certification_circuit_openings;
DROP FUNCTION vela_reject_profile_certification_circuit_opening_mutation();
DROP TRIGGER profile_certification_circuit_opening_identity
    ON profile_certification_circuit_openings;
DROP FUNCTION vela_validate_profile_certification_circuit_opening();
DROP TABLE profile_certification_circuit_openings;

DROP TRIGGER attempts_profile_certification_immutable ON attempts;
DROP FUNCTION vela_reject_attempt_profile_certification_mutation();
DROP TRIGGER attempts_bind_profile_certification ON attempts;
DROP FUNCTION vela_bind_attempt_profile_certification();
ALTER TABLE attempts
    DROP CONSTRAINT attempts_profile_certification_fk,
    DROP COLUMN profile_certification_id;

DROP TRIGGER execution_failure_decisions_profile_circuit_protocol
    ON execution_failure_decisions;
DROP FUNCTION vela_guard_profile_circuit_failure_writer();
ALTER TABLE execution_failure_decisions
    DROP COLUMN worker_was_healthy,
    DROP COLUMN circuit_protocol_version;

DROP FUNCTION vela_lock_profile_circuit_protocol();
DROP FUNCTION vela_transition_profile_circuit_protocol(boolean, text);
DROP TRIGGER profile_circuit_protocol_state_required ON profile_circuit_protocol_state;
DROP FUNCTION vela_reject_profile_circuit_protocol_state_delete();
DROP TRIGGER profile_circuit_protocol_enforce_transition ON profile_circuit_protocol_state;
DROP FUNCTION vela_enforce_profile_circuit_protocol_state_transition();
DROP TRIGGER profile_circuit_protocol_validate_transition
    ON profile_circuit_protocol_transitions;
DROP FUNCTION vela_validate_profile_circuit_protocol_transition();
DROP TRIGGER profile_circuit_protocol_history_immutable
    ON profile_circuit_protocol_transitions;
DROP FUNCTION vela_reject_profile_circuit_protocol_history_mutation();
DROP TABLE profile_circuit_protocol_transitions;
DROP TABLE profile_circuit_protocol_state;

DROP TRIGGER jobs_circuit_policy_snapshot_immutable ON jobs;
DROP FUNCTION vela_reject_job_circuit_policy_mutation();
ALTER TABLE jobs
    DROP COLUMN execution_circuit_min_distinct_healthy_workers,
    DROP COLUMN execution_circuit_fingerprint_window_seconds;
ALTER TABLE service_class_revisions
    DROP COLUMN circuit_min_distinct_healthy_workers,
    DROP COLUMN circuit_fingerprint_window_seconds;
-- +goose StatementEnd
