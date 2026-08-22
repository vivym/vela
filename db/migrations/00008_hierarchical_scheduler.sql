-- +goose Up
-- +goose StatementBegin
CREATE TYPE worker_profile_readiness_state AS ENUM ('WARM', 'PREWARM_ALLOWED');
CREATE TYPE scheduler_lane AS ENUM ('RETRY', 'PROTECTED', 'NORMAL');
CREATE TYPE scheduler_dispatch_state AS ENUM ('CLAIMED', 'COMMITTED', 'ABANDONED');
CREATE TYPE scheduler_candidate AS (
    organization_id uuid,
    service_class_revision_id uuid,
    project_id uuid,
    job_id uuid,
    job_version bigint,
    worker_id uuid,
    worker_epoch bigint,
    execution_profile_revision_id uuid,
    lane scheduler_lane,
    predicted_runtime_seconds bigint,
    predicted_start_at timestamptz,
    predicted_finish_at timestamptz,
    job_order_score bigint,
    worker_score bigint,
    job_created_at timestamptz,
    job_expires_at timestamptz
);

ALTER TABLE service_class_revisions
    ADD COLUMN queue_weight integer NOT NULL DEFAULT 1 CHECK (queue_weight BETWEEN 1 AND 1000),
    ADD COLUMN max_queue_wait_before_protection_seconds integer NOT NULL DEFAULT 1800
        CHECK (max_queue_wait_before_protection_seconds BETWEEN 1 AND 604800),
    ADD COLUMN max_aging_credit_seconds integer NOT NULL DEFAULT 900
        CHECK (max_aging_credit_seconds BETWEEN 0 AND 86400),
    ADD COLUMN max_expiry_urgency_credit_seconds integer NOT NULL DEFAULT 900
        CHECK (max_expiry_urgency_credit_seconds BETWEEN 0 AND 86400),
    ADD COLUMN max_retry_risk_penalty_seconds integer NOT NULL DEFAULT 0
        CHECK (max_retry_risk_penalty_seconds BETWEEN 0 AND 86400);

ALTER TABLE worker_pools
    ADD COLUMN scheduler_quantum_seconds integer NOT NULL DEFAULT 1200
        CHECK (scheduler_quantum_seconds BETWEEN 1 AND 86400),
    ADD COLUMN scheduler_max_deficit_seconds bigint NOT NULL DEFAULT 86400
        CHECK (scheduler_max_deficit_seconds BETWEEN 1 AND 604800),
    ADD COLUMN retry_running_limit integer NOT NULL DEFAULT 1
        CHECK (retry_running_limit >= 0);

CREATE TABLE organization_capacity_shares (
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    weight integer NOT NULL CHECK (weight BETWEEN 1 AND 1000),
    running_limit integer NOT NULL CHECK (running_limit >= 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (worker_pool_id, organization_id)
);

CREATE TABLE project_capacity_shares (
    worker_pool_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    weight integer NOT NULL CHECK (weight BETWEEN 1 AND 1000),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (worker_pool_id, organization_id, project_id),
    FOREIGN KEY (worker_pool_id, organization_id)
        REFERENCES organization_capacity_shares(worker_pool_id, organization_id),
    FOREIGN KEY (organization_id, project_id)
        REFERENCES projects(organization_id, id)
);

CREATE TABLE worker_profile_readiness (
    worker_id uuid NOT NULL,
    worker_epoch bigint NOT NULL,
    execution_profile_revision_id uuid NOT NULL REFERENCES execution_profile_revisions(id),
    readiness worker_profile_readiness_state NOT NULL,
    model_cold_start_penalty_seconds integer NOT NULL DEFAULT 0
        CHECK (model_cold_start_penalty_seconds BETWEEN 0 AND 86400),
    locality_penalty_seconds integer NOT NULL DEFAULT 0
        CHECK (locality_penalty_seconds BETWEEN 0 AND 86400),
    health_risk_penalty_seconds integer NOT NULL DEFAULT 0
        CHECK (health_risk_penalty_seconds BETWEEN 0 AND 86400),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (worker_id, worker_epoch, execution_profile_revision_id),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch)
);

CREATE TABLE job_runtime_predictions (
    job_id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    predicted_runtime_seconds bigint NOT NULL CHECK (predicted_runtime_seconds > 0),
    retry_risk_penalty_seconds integer NOT NULL DEFAULT 0
        CHECK (retry_risk_penalty_seconds BETWEEN 0 AND 86400),
    source text NOT NULL CHECK (length(source) BETWEEN 1 AND 100),
    source_revision text NOT NULL CHECK (length(source_revision) BETWEEN 1 AND 200),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, job_id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id)
);

CREATE TABLE scheduler_organization_deficits (
    worker_pool_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    deficit_seconds numeric NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    last_selected_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (worker_pool_id, organization_id),
    FOREIGN KEY (worker_pool_id, organization_id)
        REFERENCES organization_capacity_shares(worker_pool_id, organization_id)
);

CREATE TABLE scheduler_service_class_deficits (
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    deficit_seconds numeric NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    last_selected_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (worker_pool_id, organization_id, service_class_revision_id)
);

CREATE TABLE scheduler_project_deficits (
    worker_pool_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    project_id uuid NOT NULL,
    deficit_seconds numeric NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    last_selected_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        worker_pool_id, organization_id, service_class_revision_id, project_id
    ),
    FOREIGN KEY (worker_pool_id, organization_id, project_id)
        REFERENCES project_capacity_shares(worker_pool_id, organization_id, project_id)
);

CREATE TABLE scheduler_dispatch_intents (
    id uuid PRIMARY KEY,
    scheduler_id text NOT NULL CHECK (length(scheduler_id) BETWEEN 1 AND 200),
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    organization_id uuid NOT NULL,
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    expected_job_version bigint NOT NULL CHECK (expected_job_version > 0),
    execution_profile_revision_id uuid NOT NULL REFERENCES execution_profile_revisions(id),
    worker_id uuid NOT NULL REFERENCES workers(id),
    worker_epoch bigint NOT NULL CHECK (worker_epoch > 0),
    lane scheduler_lane NOT NULL,
    predicted_runtime_seconds bigint NOT NULL CHECK (predicted_runtime_seconds > 0),
    predicted_start_at timestamptz NOT NULL,
    predicted_finish_at timestamptz NOT NULL,
    job_order_score bigint NOT NULL,
    worker_score bigint NOT NULL CHECK (worker_score >= 0),
    state scheduler_dispatch_state NOT NULL DEFAULT 'CLAIMED',
    claimed_at timestamptz NOT NULL,
    claim_expires_at timestamptz NOT NULL,
    committed_at timestamptz,
    abandoned_at timestamptz,
    abandon_reason text CHECK (abandon_reason IS NULL OR length(abandon_reason) BETWEEN 1 AND 500),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, job_id, id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch),
    CHECK (claim_expires_at > claimed_at),
    CHECK (predicted_finish_at > predicted_start_at),
    CHECK (
        (state = 'CLAIMED' AND committed_at IS NULL AND abandoned_at IS NULL AND abandon_reason IS NULL)
        OR (state = 'COMMITTED' AND committed_at IS NOT NULL AND abandoned_at IS NULL AND abandon_reason IS NULL)
        OR (state = 'ABANDONED' AND committed_at IS NULL AND abandoned_at IS NOT NULL AND abandon_reason IS NOT NULL)
    )
);

CREATE UNIQUE INDEX scheduler_dispatch_intents_one_claimed_job
    ON scheduler_dispatch_intents(job_id) WHERE state = 'CLAIMED';
CREATE UNIQUE INDEX scheduler_dispatch_intents_one_claimed_worker
    ON scheduler_dispatch_intents(worker_id) WHERE state = 'CLAIMED';
CREATE INDEX scheduler_dispatch_intents_expiry_idx
    ON scheduler_dispatch_intents(claim_expires_at, id) WHERE state = 'CLAIMED';

ALTER TABLE attempts
    ADD COLUMN scheduler_dispatch_intent_id uuid UNIQUE
        REFERENCES scheduler_dispatch_intents(id);
CREATE INDEX attempts_active_pool_organization_idx
    ON attempts(worker_pool_id, organization_id)
    WHERE state IN ('ASSIGNED', 'RUNNING', 'FINALIZING');
CREATE INDEX attempts_active_retry_pool_idx
    ON attempts(worker_pool_id)
    WHERE attempt_number > 1
      AND state IN ('ASSIGNED', 'RUNNING', 'FINALIZING');

CREATE TABLE scheduler_dispatch_protocol_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    require_dispatch_intent boolean NOT NULL DEFAULT false,
    protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version > 0),
    transition_receipt text CHECK (
        transition_receipt IS NULL
        OR (length(transition_receipt) BETWEEN 1 AND 1000 AND btrim(transition_receipt) <> '')
    ),
    transitioned_at timestamptz,
    CHECK (
        (
            protocol_version = 1
            AND NOT require_dispatch_intent
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
INSERT INTO scheduler_dispatch_protocol_state (singleton) VALUES (true);

CREATE TABLE scheduler_dispatch_protocol_transitions (
    protocol_version integer PRIMARY KEY CHECK (protocol_version > 1),
    require_dispatch_intent boolean NOT NULL,
    transition_receipt text NOT NULL CHECK (
        length(transition_receipt) BETWEEN 1 AND 1000
        AND btrim(transition_receipt) <> ''
    ),
    transitioned_at timestamptz NOT NULL
);

CREATE FUNCTION vela_reject_scheduler_dispatch_protocol_history_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'scheduler_dispatch_protocol_history_immutable',
        MESSAGE = 'Scheduler dispatch protocol transition history is append-only';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_scheduler_dispatch_protocol_history_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_scheduler_dispatch_protocol_history_mutation() OWNER TO vela_internal;
CREATE TRIGGER scheduler_dispatch_protocol_history_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON scheduler_dispatch_protocol_transitions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_scheduler_dispatch_protocol_history_mutation();

CREATE FUNCTION vela_validate_scheduler_dispatch_protocol_transition() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_state public.scheduler_dispatch_protocol_state%ROWTYPE;
BEGIN
    SELECT state.* INTO v_state
    FROM public.scheduler_dispatch_protocol_state AS state
    WHERE state.singleton
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'scheduler_dispatch_protocol_state_required',
            MESSAGE = 'Scheduler dispatch protocol state is missing';
    END IF;
    IF NEW.protocol_version <> v_state.protocol_version + 1
       OR NEW.transitioned_at < COALESCE(v_state.transitioned_at, '-infinity'::timestamptz)
       OR NEW.transitioned_at > clock_timestamp()
       OR (
           v_state.protocol_version > 1
           AND NOT EXISTS (
               SELECT 1
               FROM public.scheduler_dispatch_protocol_transitions AS current_history
               WHERE current_history.protocol_version = v_state.protocol_version
                 AND current_history.require_dispatch_intent =
                     v_state.require_dispatch_intent
                 AND current_history.transition_receipt = v_state.transition_receipt
                 AND current_history.transitioned_at = v_state.transitioned_at
           )
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'scheduler_dispatch_protocol_history_contiguous',
            MESSAGE = 'Scheduler dispatch protocol transition history must extend current state';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_scheduler_dispatch_protocol_transition() FROM PUBLIC;
CREATE TRIGGER scheduler_dispatch_protocol_validate_transition
BEFORE INSERT ON scheduler_dispatch_protocol_transitions
FOR EACH ROW EXECUTE FUNCTION vela_validate_scheduler_dispatch_protocol_transition();

CREATE FUNCTION vela_reject_scheduler_dispatch_protocol_delete() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'scheduler_dispatch_protocol_state_required',
        MESSAGE = 'Scheduler dispatch protocol state cannot be removed';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_scheduler_dispatch_protocol_delete() FROM PUBLIC;
ALTER FUNCTION vela_reject_scheduler_dispatch_protocol_delete() OWNER TO vela_internal;
CREATE TRIGGER scheduler_dispatch_protocol_reject_delete
BEFORE DELETE OR TRUNCATE ON scheduler_dispatch_protocol_state
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_scheduler_dispatch_protocol_delete();

CREATE FUNCTION vela_enforce_scheduler_dispatch_protocol_state_transition() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF pg_trigger_depth() <> 2
       OR NEW.protocol_version <> OLD.protocol_version + 1
       OR NOT EXISTS (
           SELECT 1
           FROM public.scheduler_dispatch_protocol_transitions AS transition
           WHERE transition.protocol_version = NEW.protocol_version
             AND transition.require_dispatch_intent = NEW.require_dispatch_intent
             AND transition.transition_receipt = NEW.transition_receipt
             AND transition.transitioned_at = NEW.transitioned_at
       ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'scheduler_dispatch_protocol_state_transition_required',
            MESSAGE = 'Scheduler dispatch protocol state must follow immutable transition history';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_scheduler_dispatch_protocol_state_transition() FROM PUBLIC;
CREATE TRIGGER scheduler_dispatch_protocol_enforce_transition
BEFORE UPDATE ON scheduler_dispatch_protocol_state
FOR EACH ROW EXECUTE FUNCTION vela_enforce_scheduler_dispatch_protocol_state_transition();

CREATE FUNCTION vela_apply_scheduler_dispatch_protocol_transition() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    UPDATE public.scheduler_dispatch_protocol_state
    SET require_dispatch_intent = NEW.require_dispatch_intent,
        protocol_version = NEW.protocol_version,
        transition_receipt = NEW.transition_receipt,
        transitioned_at = NEW.transitioned_at
    WHERE singleton
      AND protocol_version = NEW.protocol_version - 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'scheduler_dispatch_protocol_history_contiguous',
            MESSAGE = 'Scheduler dispatch protocol transition history cannot advance current state';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_apply_scheduler_dispatch_protocol_transition() FROM PUBLIC;
CREATE TRIGGER scheduler_dispatch_protocol_apply_transition
AFTER INSERT ON scheduler_dispatch_protocol_transitions
FOR EACH ROW EXECUTE FUNCTION vela_apply_scheduler_dispatch_protocol_transition();

CREATE FUNCTION vela_reject_job_waiting_age_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.created_at IS DISTINCT FROM OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'jobs_scheduler_waiting_age_immutable',
            MESSAGE = 'Job Scheduler waiting age is immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_job_waiting_age_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_job_waiting_age_mutation() OWNER TO vela_internal;
CREATE TRIGGER jobs_reject_waiting_age_mutation
BEFORE UPDATE OF created_at ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_reject_job_waiting_age_mutation();

ALTER TABLE organization_capacity_shares ENABLE ROW LEVEL SECURITY;
ALTER TABLE organization_capacity_shares FORCE ROW LEVEL SECURITY;
ALTER TABLE project_capacity_shares ENABLE ROW LEVEL SECURITY;
ALTER TABLE project_capacity_shares FORCE ROW LEVEL SECURITY;
ALTER TABLE worker_profile_readiness ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_profile_readiness FORCE ROW LEVEL SECURITY;
ALTER TABLE job_runtime_predictions ENABLE ROW LEVEL SECURITY;
ALTER TABLE job_runtime_predictions FORCE ROW LEVEL SECURITY;
ALTER TABLE scheduler_organization_deficits ENABLE ROW LEVEL SECURITY;
ALTER TABLE scheduler_organization_deficits FORCE ROW LEVEL SECURITY;
ALTER TABLE scheduler_service_class_deficits ENABLE ROW LEVEL SECURITY;
ALTER TABLE scheduler_service_class_deficits FORCE ROW LEVEL SECURITY;
ALTER TABLE scheduler_project_deficits ENABLE ROW LEVEL SECURITY;
ALTER TABLE scheduler_project_deficits FORCE ROW LEVEL SECURITY;
ALTER TABLE scheduler_dispatch_intents ENABLE ROW LEVEL SECURITY;
ALTER TABLE scheduler_dispatch_intents FORCE ROW LEVEL SECURITY;
ALTER TABLE scheduler_dispatch_protocol_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE scheduler_dispatch_protocol_state FORCE ROW LEVEL SECURITY;
ALTER TABLE scheduler_dispatch_protocol_transitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE scheduler_dispatch_protocol_transitions FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_scheduler_capacity_timeline(
    p_worker_pool_id uuid,
    p_now timestamptz
) RETURNS TABLE (
    worker_id uuid,
    worker_epoch bigint,
    execution_profile_revision_id uuid,
    available_at timestamptz,
    capacity_state text
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        worker.id,
        worker.epoch,
        readiness.execution_profile_revision_id,
        p_now,
        'READY'::text
    FROM public.workers AS worker
    JOIN public.worker_profile_readiness AS readiness
      ON readiness.worker_id = worker.id
     AND readiness.worker_epoch = worker.epoch
    WHERE worker.worker_pool_id = p_worker_pool_id
      AND worker.lifecycle_state = 'READY'
      AND worker.reachability_condition = 'HEALTHY'
      AND readiness.readiness IN ('WARM', 'PREWARM_ALLOWED')
      AND NOT EXISTS (
          SELECT 1
          FROM public.attempts AS active_attempt
          WHERE active_attempt.worker_id = worker.id
            AND active_attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
      )
      AND NOT EXISTS (
          SELECT 1
          FROM public.scheduler_dispatch_intents AS live_claim
          WHERE live_claim.worker_id = worker.id
            AND live_claim.state = 'CLAIMED'
            AND live_claim.claim_expires_at > p_now
      )

    UNION ALL

    SELECT DISTINCT
        worker.id,
        worker.epoch,
        readiness.execution_profile_revision_id,
        GREATEST(p_now, progress.estimated_finish_at),
        'BUSY'::text
    FROM public.workers AS worker
    JOIN public.worker_profile_readiness AS readiness
      ON readiness.worker_id = worker.id
     AND readiness.worker_epoch = worker.epoch
    JOIN public.attempts AS active_attempt
      ON active_attempt.worker_id = worker.id
     AND active_attempt.worker_epoch = worker.epoch
     AND active_attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
    JOIN public.attempt_progress AS progress
      ON progress.attempt_id = active_attempt.id
     AND progress.worker_id = worker.id
     AND progress.worker_epoch = worker.epoch
    WHERE worker.worker_pool_id = p_worker_pool_id
      AND worker.lifecycle_state = 'BUSY'
      AND worker.reachability_condition = 'HEALTHY'
      AND readiness.readiness IN ('WARM', 'PREWARM_ALLOWED')
      AND progress.progress_valid_until > p_now
      AND progress.estimated_remaining_seconds IS NOT NULL
      AND progress.estimated_finish_at IS NOT NULL
$$;
REVOKE ALL ON FUNCTION vela_scheduler_capacity_timeline(uuid, timestamptz) FROM PUBLIC;
ALTER FUNCTION vela_scheduler_capacity_timeline(uuid, timestamptz) OWNER TO vela_internal;

CREATE FUNCTION vela_scheduler_job_worker_compatibility(
    p_worker_pool_id uuid,
    p_now timestamptz,
    p_job_id uuid
) RETURNS TABLE (
    job_id uuid,
    worker_id uuid,
    worker_epoch bigint,
    execution_profile_revision_id uuid,
    available_at timestamptz,
    capacity_state text,
    worker_score bigint
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        job.id,
        capacity.worker_id,
        capacity.worker_epoch,
        profile.id,
        capacity.available_at,
        capacity.capacity_state,
        (
            readiness.model_cold_start_penalty_seconds
            + readiness.locality_penalty_seconds
            + readiness.health_risk_penalty_seconds
        )::bigint
    FROM public.jobs AS job
    JOIN public.profile_certifications AS certification
      ON certification.model_revision_id = job.model_revision_id
     AND certification.generation_preset_revision_id = job.generation_preset_revision_id
     AND certification.output_spec_id = job.output_spec_id
     AND certification.state = 'ACTIVE'
     AND certification.invalidated_at IS NULL
    JOIN public.execution_profile_revisions AS profile
      ON profile.id = certification.execution_profile_revision_id
     AND profile.worker_pool_id = job.worker_pool_id
     AND profile.state = 'ACTIVE'
    JOIN public.vela_scheduler_capacity_timeline(p_worker_pool_id, p_now) AS capacity
      ON capacity.execution_profile_revision_id = profile.id
    JOIN public.worker_profile_readiness AS readiness
      ON readiness.worker_id = capacity.worker_id
     AND readiness.worker_epoch = capacity.worker_epoch
     AND readiness.execution_profile_revision_id = profile.id
    JOIN public.execution_retry_evidence AS retry_evidence
      ON retry_evidence.job_id = job.id
    WHERE job.worker_pool_id = p_worker_pool_id
      AND job.id = p_job_id
      AND NOT EXISTS (
          SELECT 1
          FROM jsonb_array_elements(retry_evidence.excluded_workers) AS excluded(worker)
          WHERE excluded.worker ->> 'worker_id' = capacity.worker_id::text
            AND (excluded.worker ->> 'expires_at')::timestamptz > p_now
      )
$$;
REVOKE ALL ON FUNCTION vela_scheduler_job_worker_compatibility(uuid, timestamptz, uuid)
    FROM PUBLIC;
ALTER FUNCTION vela_scheduler_job_worker_compatibility(uuid, timestamptz, uuid)
    OWNER TO vela_internal;

CREATE FUNCTION vela_scheduler_projectable_jobs(
    p_worker_pool_id uuid,
    p_now timestamptz
) RETURNS TABLE (
    worker_pool_id uuid,
    job_id uuid,
    job_version bigint,
    organization_id uuid,
    project_id uuid,
    service_class_revision_id uuid,
    execution_profile_revision_ids uuid[],
    predicted_runtime_seconds bigint,
    organization_weight integer,
    service_class_weight integer,
    project_weight integer,
    lane scheduler_lane,
    retry_risk_penalty_seconds bigint,
    aging_credit_seconds bigint,
    max_expiry_urgency_credit_seconds bigint,
    job_created_at timestamptz,
    job_expires_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT
        queued.worker_pool_id,
        queued.id,
        queued.version,
        queued.organization_id,
        queued.project_id,
        queued.service_class_revision_id,
        compatible.execution_profile_revision_ids,
        COALESCE(
            prediction.predicted_runtime_seconds,
            preset.certified_p95_compute_seconds::bigint * queued.pricing_quantity::bigint
        ),
        organization_share.weight,
        service_class.queue_weight,
        project_share.weight,
        CASE
            WHEN queued.state = 'RETRY_WAIT' THEN 'RETRY'::public.scheduler_lane
            WHEN EXTRACT(EPOCH FROM (p_now - queued.created_at)) >=
                 service_class.max_queue_wait_before_protection_seconds
                THEN 'PROTECTED'::public.scheduler_lane
            ELSE 'NORMAL'::public.scheduler_lane
        END,
        LEAST(
            COALESCE(prediction.retry_risk_penalty_seconds, 0),
            service_class.max_retry_risk_penalty_seconds
        )::bigint,
        LEAST(
            service_class.max_aging_credit_seconds::bigint,
            GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (p_now - queued.created_at))))::bigint
        ),
        service_class.max_expiry_urgency_credit_seconds::bigint,
        queued.created_at,
        queued.job_expires_at
    FROM public.jobs AS queued
    JOIN public.worker_pools AS pool ON pool.id = queued.worker_pool_id
    JOIN public.projects AS project
      ON project.organization_id = queued.organization_id
     AND project.id = queued.project_id
    JOIN public.organization_capacity_shares AS organization_share
      ON organization_share.worker_pool_id = queued.worker_pool_id
     AND organization_share.organization_id = queued.organization_id
    JOIN public.project_capacity_shares AS project_share
      ON project_share.worker_pool_id = queued.worker_pool_id
     AND project_share.organization_id = queued.organization_id
     AND project_share.project_id = queued.project_id
    JOIN public.service_class_revisions AS service_class
      ON service_class.id = queued.service_class_revision_id
    JOIN public.generation_preset_revisions AS preset
      ON preset.id = queued.generation_preset_revision_id
    JOIN public.credit_reservations AS credit ON credit.job_id = queued.id
    JOIN public.retry_runtime_states AS retry_state ON retry_state.job_id = queued.id
    CROSS JOIN LATERAL (
        SELECT array_agg(
            DISTINCT compatibility.execution_profile_revision_id
            ORDER BY compatibility.execution_profile_revision_id
        )
            AS execution_profile_revision_ids
        FROM public.vela_scheduler_job_worker_compatibility(
            p_worker_pool_id,
            p_now,
            queued.id
        ) AS compatibility
    ) AS compatible
    LEFT JOIN public.job_runtime_predictions AS prediction ON prediction.job_id = queued.id
    WHERE queued.worker_pool_id = p_worker_pool_id
      AND compatible.execution_profile_revision_ids IS NOT NULL
      AND (
          queued.state = 'QUEUED'
          OR (
              queued.state = 'RETRY_WAIT'
              AND retry_state.next_retry_at IS NOT NULL
              AND retry_state.next_retry_at <= p_now
          )
      )
      AND queued.job_expires_at > p_now
      AND credit.state = 'RESERVED'
      AND retry_state.attempts_started < queued.execution_max_attempts
      AND retry_state.compute_seconds_consumed < queued.execution_max_total_compute_seconds
      AND project.running_count < project.running_limit
      AND (
          SELECT count(*)
          FROM public.attempts AS active_organization_attempt
          WHERE active_organization_attempt.worker_pool_id = queued.worker_pool_id
            AND active_organization_attempt.organization_id = queued.organization_id
            AND active_organization_attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
      ) < organization_share.running_limit
      AND NOT EXISTS (
          SELECT 1
          FROM public.attempts AS active_job_attempt
          WHERE active_job_attempt.job_id = queued.id
            AND active_job_attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
      )
      AND NOT EXISTS (
          SELECT 1
          FROM public.scheduler_dispatch_intents AS live_job_claim
          WHERE live_job_claim.job_id = queued.id
            AND live_job_claim.state = 'CLAIMED'
            AND live_job_claim.claim_expires_at > p_now
      )
      AND (
          queued.state <> 'RETRY_WAIT'
          OR (
              SELECT count(*)
              FROM public.attempts AS active_retry_attempt
              WHERE active_retry_attempt.worker_pool_id = queued.worker_pool_id
                AND active_retry_attempt.attempt_number > 1
                AND active_retry_attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
          ) < pool.retry_running_limit
      )
      AND service_class.state = 'ACTIVE'
      AND preset.state = 'ACTIVE'
      AND (
          NOT pg_has_role(session_user, 'vela_request', 'MEMBER')
          OR EXISTS (
              SELECT 1
              FROM pg_catalog.pg_roles AS login_role
              WHERE login_role.rolname = session_user
                AND (login_role.rolsuper OR login_role.rolbypassrls)
          )
          OR (
              public.vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
              AND queued.organization_id = public.vela_current_organization_id()
              AND queued.project_id = public.vela_current_project_id()
          )
      )
$$;
REVOKE ALL ON FUNCTION vela_scheduler_projectable_jobs(uuid, timestamptz) FROM PUBLIC;
ALTER FUNCTION vela_scheduler_projectable_jobs(uuid, timestamptz) OWNER TO vela_internal;

CREATE FUNCTION vela_scheduler_queue_projection_for_pool(
    p_worker_pool_id uuid,
    p_now timestamptz
) RETURNS TABLE (
    worker_pool_id uuid,
    job_id uuid,
    organization_id uuid,
    project_id uuid,
    service_class_revision_id uuid,
    execution_profile_revision_id uuid,
    projected_worker_id uuid,
    predicted_runtime_seconds bigint,
    predicted_start_at timestamptz,
    predicted_finish_at timestamptz,
    projected_at timestamptz
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_quantum bigint;
    v_max_deficit bigint;
    v_pool_waiting_bound bigint;
    v_projectable_job_count bigint;
    v_credit_scale numeric;
    v_availability_by_worker jsonb;
    v_organization_deficits jsonb;
    v_service_class_deficits jsonb;
    v_project_deficits jsonb;
    v_scheduled_job_ids uuid[] := ARRAY[]::uuid[];
    v_has_remaining boolean;
    v_has_positive boolean;
    v_organization_id uuid;
    v_service_class_revision_id uuid;
    v_project_id uuid;
    v_key text;
    v_old_deficit numeric;
    v_peer record;
    v_candidate record;
    v_profile_id uuid;
BEGIN
    SELECT
        pool.scheduler_quantum_seconds,
        pool.scheduler_max_deficit_seconds,
        pool.queued_limit::bigint + pool.retry_wait_count::bigint
    INTO v_quantum, v_max_deficit, v_pool_waiting_bound
    FROM public.worker_pools AS pool
    WHERE pool.id = p_worker_pool_id;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    v_credit_scale := LEAST(
        1::numeric,
        v_max_deficit::numeric / (1000::numeric * v_quantum::numeric)
    );

    SELECT count(*) INTO v_projectable_job_count
    FROM (
        SELECT 1
        FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now)
        LIMIT v_pool_waiting_bound + 1
    ) AS bounded_projectable_jobs;
    IF v_projectable_job_count > v_pool_waiting_bound THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'scheduler_candidate_snapshot_exceeds_pool_waiting_bound',
            MESSAGE = 'Scheduler candidates exceed the Worker pool waiting bound';
    END IF;

    SELECT jsonb_object_agg(
        capacity.worker_id::text,
        to_jsonb(capacity.available_at)
        ORDER BY capacity.worker_id
    )
    INTO v_availability_by_worker
    FROM (
        SELECT timeline.worker_id, max(timeline.available_at) AS available_at
        FROM public.vela_scheduler_capacity_timeline(p_worker_pool_id, p_now) AS timeline
        GROUP BY timeline.worker_id
    ) AS capacity;
    IF v_availability_by_worker IS NULL THEN
        RETURN;
    END IF;

    SELECT COALESCE(
        jsonb_object_agg(deficit.organization_id::text, to_jsonb(deficit.deficit_seconds)),
        '{}'::jsonb
    ) INTO v_organization_deficits
    FROM public.scheduler_organization_deficits AS deficit
    WHERE deficit.worker_pool_id = p_worker_pool_id;
    SELECT COALESCE(
        jsonb_object_agg(
            concat(deficit.organization_id::text, '/', deficit.service_class_revision_id::text),
            to_jsonb(deficit.deficit_seconds)
        ),
        '{}'::jsonb
    ) INTO v_service_class_deficits
    FROM public.scheduler_service_class_deficits AS deficit
    WHERE deficit.worker_pool_id = p_worker_pool_id;
    SELECT COALESCE(
        jsonb_object_agg(
            concat(
                deficit.organization_id::text, '/',
                deficit.service_class_revision_id::text, '/',
                deficit.project_id::text
            ),
            to_jsonb(deficit.deficit_seconds)
        ),
        '{}'::jsonb
    ) INTO v_project_deficits
    FROM public.scheduler_project_deficits AS deficit
    WHERE deficit.worker_pool_id = p_worker_pool_id;

    LOOP
        SELECT EXISTS (
            SELECT 1
            FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
            WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
        ) INTO v_has_remaining;
        EXIT WHEN NOT v_has_remaining;

        SELECT EXISTS (
            SELECT 1
            FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
            WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
              AND COALESCE(
                  (v_organization_deficits ->> candidate.organization_id::text)::numeric,
                  0
              ) > 0
        ) INTO v_has_positive;
        IF NOT v_has_positive THEN
            FOR v_peer IN
                SELECT DISTINCT candidate.organization_id, candidate.organization_weight
                FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
                WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
            LOOP
                v_key := v_peer.organization_id::text;
                v_old_deficit := COALESCE((v_organization_deficits ->> v_key)::numeric, 0);
                v_organization_deficits := jsonb_set(
                    v_organization_deficits,
                    ARRAY[v_key],
                    to_jsonb(GREATEST(
                        -v_max_deficit::numeric,
                        v_old_deficit
                            + v_peer.organization_weight::numeric
                            * v_quantum::numeric
                            * v_credit_scale
                    )),
                    true
                );
            END LOOP;
        END IF;

        SELECT candidate.organization_id
        INTO v_organization_id
        FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
        WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
        GROUP BY candidate.organization_id
        ORDER BY
            COALESCE(
                (v_organization_deficits ->> candidate.organization_id::text)::numeric,
                0
            ) DESC,
            min(candidate.job_created_at),
            candidate.organization_id
        LIMIT 1;

        SELECT EXISTS (
            SELECT 1
            FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
            WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
              AND candidate.organization_id = v_organization_id
              AND COALESCE(
                  (
                      v_service_class_deficits ->> concat(
                          candidate.organization_id::text, '/',
                          candidate.service_class_revision_id::text
                      )
                  )::numeric,
                  0
              ) > 0
        ) INTO v_has_positive;
        IF NOT v_has_positive THEN
            FOR v_peer IN
                SELECT DISTINCT
                    candidate.organization_id,
                    candidate.service_class_revision_id,
                    candidate.service_class_weight
                FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
                WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
                  AND candidate.organization_id = v_organization_id
            LOOP
                v_key := concat(
                    v_peer.organization_id::text, '/',
                    v_peer.service_class_revision_id::text
                );
                v_old_deficit := COALESCE((v_service_class_deficits ->> v_key)::numeric, 0);
                v_service_class_deficits := jsonb_set(
                    v_service_class_deficits,
                    ARRAY[v_key],
                    to_jsonb(GREATEST(
                        -v_max_deficit::numeric,
                        v_old_deficit
                            + v_peer.service_class_weight::numeric
                            * v_quantum::numeric
                            * v_credit_scale
                    )),
                    true
                );
            END LOOP;
        END IF;

        SELECT candidate.service_class_revision_id
        INTO v_service_class_revision_id
        FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
        WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
          AND candidate.organization_id = v_organization_id
        GROUP BY candidate.organization_id, candidate.service_class_revision_id
        ORDER BY
            COALESCE(
                (
                    v_service_class_deficits ->> concat(
                        candidate.organization_id::text, '/',
                        candidate.service_class_revision_id::text
                    )
                )::numeric,
                0
            ) DESC,
            min(candidate.job_created_at),
            candidate.service_class_revision_id
        LIMIT 1;

        SELECT EXISTS (
            SELECT 1
            FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
            WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
              AND candidate.organization_id = v_organization_id
              AND candidate.service_class_revision_id = v_service_class_revision_id
              AND COALESCE(
                  (
                      v_project_deficits ->> concat(
                          candidate.organization_id::text, '/',
                          candidate.service_class_revision_id::text, '/',
                          candidate.project_id::text
                      )
                  )::numeric,
                  0
              ) > 0
        ) INTO v_has_positive;
        IF NOT v_has_positive THEN
            FOR v_peer IN
                SELECT DISTINCT
                    candidate.organization_id,
                    candidate.service_class_revision_id,
                    candidate.project_id,
                    candidate.project_weight
                FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
                WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
                  AND candidate.organization_id = v_organization_id
                  AND candidate.service_class_revision_id = v_service_class_revision_id
            LOOP
                v_key := concat(
                    v_peer.organization_id::text, '/',
                    v_peer.service_class_revision_id::text, '/',
                    v_peer.project_id::text
                );
                v_old_deficit := COALESCE((v_project_deficits ->> v_key)::numeric, 0);
                v_project_deficits := jsonb_set(
                    v_project_deficits,
                    ARRAY[v_key],
                    to_jsonb(GREATEST(
                        -v_max_deficit::numeric,
                        v_old_deficit
                            + v_peer.project_weight::numeric
                            * v_quantum::numeric
                            * v_credit_scale
                    )),
                    true
                );
            END LOOP;
        END IF;

        SELECT candidate.project_id
        INTO v_project_id
        FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
        WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
          AND candidate.organization_id = v_organization_id
          AND candidate.service_class_revision_id = v_service_class_revision_id
        GROUP BY
            candidate.organization_id,
            candidate.service_class_revision_id,
            candidate.project_id
        ORDER BY
            COALESCE(
                (
                    v_project_deficits ->> concat(
                        candidate.organization_id::text, '/',
                        candidate.service_class_revision_id::text, '/',
                        candidate.project_id::text
                    )
                )::numeric,
                0
            ) DESC,
            min(candidate.job_created_at),
            candidate.project_id
        LIMIT 1;

        SELECT
            candidate.*,
            placement.worker_id AS projected_worker_id,
            placement.predicted_start_at,
            timing.predicted_finish_at,
            ordering.job_order_score
        INTO v_candidate
        FROM public.vela_scheduler_projectable_jobs(p_worker_pool_id, p_now) AS candidate
        CROSS JOIN LATERAL (
            SELECT
                capacity.worker_id,
                GREATEST(
                    capacity.available_at,
                    COALESCE(
                        (
                            v_availability_by_worker ->> capacity.worker_id::text
                        )::timestamptz,
                        capacity.available_at
                    )
                ) AS predicted_start_at
            FROM (
                SELECT
                    compatibility.worker_id,
                    max(compatibility.available_at) AS available_at
                FROM public.vela_scheduler_job_worker_compatibility(
                    p_worker_pool_id,
                    p_now,
                    candidate.job_id
                ) AS compatibility
                GROUP BY compatibility.worker_id
            ) AS capacity
            ORDER BY predicted_start_at, capacity.worker_id
            LIMIT 1
        ) AS placement
        CROSS JOIN LATERAL (
            SELECT placement.predicted_start_at + make_interval(
                secs => candidate.predicted_runtime_seconds::double precision
            ) AS predicted_finish_at
        ) AS timing
        CROSS JOIN LATERAL (
            SELECT
                candidate.predicted_runtime_seconds
                + candidate.retry_risk_penalty_seconds
                - candidate.aging_credit_seconds
                - LEAST(
                    candidate.max_expiry_urgency_credit_seconds,
                    GREATEST(
                        0,
                        candidate.max_expiry_urgency_credit_seconds
                        - FLOOR(EXTRACT(EPOCH FROM (
                            candidate.job_expires_at - timing.predicted_finish_at
                        )))::bigint
                    )
                ) AS job_order_score
        ) AS ordering
        WHERE NOT (candidate.job_id = ANY(v_scheduled_job_ids))
          AND candidate.organization_id = v_organization_id
          AND candidate.service_class_revision_id = v_service_class_revision_id
          AND candidate.project_id = v_project_id
        ORDER BY
            CASE candidate.lane WHEN 'RETRY' THEN 0 WHEN 'PROTECTED' THEN 1 ELSE 2 END,
            CASE WHEN candidate.lane = 'PROTECTED' THEN candidate.job_expires_at END,
            CASE WHEN candidate.lane = 'PROTECTED' THEN candidate.job_created_at END,
            CASE WHEN candidate.lane = 'NORMAL' THEN ordering.job_order_score END,
            candidate.job_created_at,
            candidate.job_id
        LIMIT 1;

        v_key := v_candidate.organization_id::text;
        v_old_deficit := COALESCE((v_organization_deficits ->> v_key)::numeric, 0);
        v_organization_deficits := jsonb_set(
            v_organization_deficits,
            ARRAY[v_key],
            to_jsonb(GREATEST(
                -v_max_deficit::numeric,
                v_old_deficit
                    - v_candidate.predicted_runtime_seconds::numeric * v_credit_scale
            )),
            true
        );
        v_key := concat(
            v_candidate.organization_id::text, '/',
            v_candidate.service_class_revision_id::text
        );
        v_old_deficit := COALESCE((v_service_class_deficits ->> v_key)::numeric, 0);
        v_service_class_deficits := jsonb_set(
            v_service_class_deficits,
            ARRAY[v_key],
            to_jsonb(GREATEST(
                -v_max_deficit::numeric,
                v_old_deficit
                    - v_candidate.predicted_runtime_seconds::numeric * v_credit_scale
            )),
            true
        );
        v_key := concat(
            v_candidate.organization_id::text, '/',
            v_candidate.service_class_revision_id::text, '/',
            v_candidate.project_id::text
        );
        v_old_deficit := COALESCE((v_project_deficits ->> v_key)::numeric, 0);
        v_project_deficits := jsonb_set(
            v_project_deficits,
            ARRAY[v_key],
            to_jsonb(GREATEST(
                -v_max_deficit::numeric,
                v_old_deficit
                    - v_candidate.predicted_runtime_seconds::numeric * v_credit_scale
            )),
            true
        );
        v_availability_by_worker := jsonb_set(
            v_availability_by_worker,
            ARRAY[v_candidate.projected_worker_id::text],
            to_jsonb(v_candidate.predicted_finish_at),
            false
        );
        v_scheduled_job_ids := array_append(v_scheduled_job_ids, v_candidate.job_id);

        worker_pool_id := p_worker_pool_id;
        job_id := v_candidate.job_id;
        organization_id := v_candidate.organization_id;
        project_id := v_candidate.project_id;
        service_class_revision_id := v_candidate.service_class_revision_id;
        projected_worker_id := v_candidate.projected_worker_id;
        predicted_runtime_seconds := v_candidate.predicted_runtime_seconds;
        predicted_start_at := v_candidate.predicted_start_at;
        predicted_finish_at := v_candidate.predicted_finish_at;
        projected_at := p_now;
        FOREACH v_profile_id IN ARRAY v_candidate.execution_profile_revision_ids
        LOOP
            execution_profile_revision_id := v_profile_id;
            RETURN NEXT;
        END LOOP;
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION vela_scheduler_queue_projection_for_pool(uuid, timestamptz) FROM PUBLIC;
ALTER FUNCTION vela_scheduler_queue_projection_for_pool(uuid, timestamptz) OWNER TO vela_internal;

CREATE FUNCTION vela_scheduler_queue_projection() RETURNS TABLE (
    worker_pool_id uuid,
    job_id uuid,
    organization_id uuid,
    project_id uuid,
    service_class_revision_id uuid,
    execution_profile_revision_id uuid,
    projected_worker_id uuid,
    predicted_runtime_seconds bigint,
    predicted_start_at timestamptz,
    predicted_finish_at timestamptz,
    projected_at timestamptz
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker_pool_id uuid;
    v_now timestamptz := clock_timestamp();
BEGIN
    FOR v_worker_pool_id IN
        SELECT pool.id FROM public.worker_pools AS pool ORDER BY pool.id
    LOOP
        RETURN QUERY
        SELECT projection.*
        FROM public.vela_scheduler_queue_projection_for_pool(
            v_worker_pool_id,
            v_now
        ) AS projection;
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION vela_scheduler_queue_projection() FROM PUBLIC;
ALTER FUNCTION vela_scheduler_queue_projection() OWNER TO vela_internal;

CREATE OR REPLACE VIEW vela_request_job_progress
WITH (security_barrier = true) AS
WITH postgres_time AS MATERIALIZED (
    SELECT clock_timestamp() AS observed_at
)
SELECT
    progress.job_id,
    CASE
        WHEN progress.progress_valid_until > postgres_time.observed_at
            THEN progress.phase_progress
        ELSE NULL
    END::double precision AS phase_progress,
    CASE
        WHEN progress.progress_valid_until > postgres_time.observed_at
            THEN progress.estimated_finish_at
        ELSE NULL
    END::timestamptz AS estimated_finish_at,
    progress.progress_updated_at
FROM public.jobs AS job
JOIN public.attempt_progress AS progress
  ON progress.job_id = job.id
 AND progress.fence = job.current_fence
 AND progress.execution_phase = job.execution_phase
CROSS JOIN postgres_time
WHERE public.vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
  AND job.organization_id = public.vela_current_organization_id()
  AND job.project_id = public.vela_current_project_id();
ALTER VIEW vela_request_job_progress OWNER TO vela_internal;

CREATE FUNCTION vela_predict_admission_capacity(
    p_worker_pool_id uuid,
    p_model_revision_id uuid,
    p_generation_preset_revision_id uuid,
    p_service_class_revision_id uuid,
    p_output_spec_id uuid,
    p_generation_count integer
) RETURNS TABLE (
    predicted_queue_wait_seconds bigint,
    predicted_finish_at timestamptz
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    WITH postgres_time AS MATERIALIZED (
        SELECT clock_timestamp() AS observed_at
    ),
    compatible_capacity AS MATERIALIZED (
        SELECT
            capacity.worker_id,
            max(capacity.available_at) AS available_at
        FROM public.vela_scheduler_capacity_timeline(
            p_worker_pool_id,
            (SELECT observed_at FROM postgres_time)
        ) AS capacity
        JOIN public.execution_profile_revisions AS profile
          ON profile.id = capacity.execution_profile_revision_id
         AND profile.worker_pool_id = p_worker_pool_id
         AND profile.model_revision_id = p_model_revision_id
         AND profile.state = 'ACTIVE'
        JOIN public.profile_certifications AS certification
          ON certification.execution_profile_revision_id = profile.id
         AND certification.model_revision_id = p_model_revision_id
         AND certification.generation_preset_revision_id = p_generation_preset_revision_id
         AND certification.output_spec_id = p_output_spec_id
         AND certification.state = 'ACTIVE'
         AND certification.invalidated_at IS NULL
        JOIN public.service_class_revisions AS service_class
          ON service_class.id = p_service_class_revision_id
         AND service_class.state = 'ACTIVE'
        WHERE p_generation_count > 0
        GROUP BY capacity.worker_id
    ),
    worker_tails AS (
        SELECT
            capacity.worker_id,
            GREATEST(
                capacity.available_at,
                COALESCE(max(projection.predicted_finish_at), capacity.available_at)
            ) AS available_at
        FROM compatible_capacity AS capacity
        LEFT JOIN public.vela_scheduler_queue_projection_for_pool(
            p_worker_pool_id,
            (SELECT observed_at FROM postgres_time)
        ) AS projection
          ON projection.projected_worker_id = capacity.worker_id
        GROUP BY capacity.worker_id, capacity.available_at
    ),
    earliest AS (
        SELECT min(worker_tails.available_at) AS available_at
        FROM worker_tails
    ),
    candidate_runtime AS (
        SELECT
            preset.certified_p95_compute_seconds::bigint
                * p_generation_count::bigint AS runtime_seconds
        FROM public.generation_preset_revisions AS preset
        WHERE preset.id = p_generation_preset_revision_id
          AND preset.state = 'ACTIVE'
          AND p_generation_count > 0
    )
    SELECT
        GREATEST(
            0,
            CEIL(EXTRACT(EPOCH FROM (earliest.available_at - postgres_time.observed_at)))
        )::bigint,
        earliest.available_at + candidate_runtime.runtime_seconds * interval '1 second'
    FROM earliest
    CROSS JOIN postgres_time
    CROSS JOIN candidate_runtime
    WHERE earliest.available_at IS NOT NULL
$$;
REVOKE ALL ON FUNCTION vela_predict_admission_capacity(
    uuid, uuid, uuid, uuid, uuid, integer
) FROM PUBLIC;
ALTER FUNCTION vela_predict_admission_capacity(
    uuid, uuid, uuid, uuid, uuid, integer
) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_predict_admission_capacity(
    uuid, uuid, uuid, uuid, uuid, integer
) TO vela_scheduler;

CREATE FUNCTION vela_predict_job_dynamic_eta(
    p_job_id uuid
) RETURNS TABLE (predicted_finish_at timestamptz)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    WITH postgres_time AS MATERIALIZED (
        SELECT clock_timestamp() AS observed_at
    )
    SELECT min(projection.predicted_finish_at)
    FROM public.jobs AS job
    CROSS JOIN postgres_time
    JOIN LATERAL public.vela_scheduler_queue_projection_for_pool(
        job.worker_pool_id,
        postgres_time.observed_at
    ) AS projection ON projection.job_id = job.id
    WHERE job.id = p_job_id
    HAVING min(projection.predicted_finish_at) IS NOT NULL
$$;
REVOKE ALL ON FUNCTION vela_predict_job_dynamic_eta(uuid) FROM PUBLIC;
ALTER FUNCTION vela_predict_job_dynamic_eta(uuid) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_predict_job_dynamic_eta(uuid) TO vela_scheduler;

CREATE FUNCTION vela_scheduler_candidates(
    p_worker_pool_id uuid,
    p_now timestamptz
) RETURNS SETOF scheduler_candidate
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    WITH raw AS (
        SELECT
            candidate.organization_id,
            candidate.service_class_revision_id,
            candidate.project_id,
            candidate.job_id,
            candidate.job_version,
            compatibility.worker_id,
            compatibility.worker_epoch,
            compatibility.execution_profile_revision_id,
            candidate.lane,
            candidate.predicted_runtime_seconds,
            projection.predicted_start_at,
            projection.predicted_finish_at,
            candidate.predicted_runtime_seconds
                + candidate.retry_risk_penalty_seconds
                - candidate.aging_credit_seconds
                - LEAST(
                    candidate.max_expiry_urgency_credit_seconds,
                    GREATEST(
                        0,
                        candidate.max_expiry_urgency_credit_seconds
                            - FLOOR(EXTRACT(EPOCH FROM (
                                candidate.job_expires_at - projection.predicted_finish_at
                            )))::bigint
                    )
                ) AS job_order_score,
            compatibility.worker_score,
            candidate.job_created_at,
            candidate.job_expires_at,
            row_number() OVER (
                PARTITION BY candidate.job_id
                ORDER BY
                    compatibility.worker_score,
                    compatibility.execution_profile_revision_id,
                    compatibility.worker_id
            ) AS worker_rank
        FROM public.vela_scheduler_projectable_jobs(
            p_worker_pool_id,
            p_now
        ) AS candidate
        JOIN LATERAL public.vela_scheduler_job_worker_compatibility(
            p_worker_pool_id,
            p_now,
            candidate.job_id
        ) AS compatibility ON compatibility.capacity_state = 'READY'
        JOIN public.vela_scheduler_queue_projection_for_pool(
            p_worker_pool_id,
            p_now
        ) AS projection
          ON projection.job_id = candidate.job_id
         AND projection.execution_profile_revision_id =
             compatibility.execution_profile_revision_id
    )
    SELECT
        raw.organization_id,
        raw.service_class_revision_id,
        raw.project_id,
        raw.job_id,
        raw.job_version,
        raw.worker_id,
        raw.worker_epoch,
        raw.execution_profile_revision_id,
        raw.lane,
        raw.predicted_runtime_seconds,
        raw.predicted_start_at,
        raw.predicted_finish_at,
        raw.job_order_score,
        raw.worker_score,
        raw.job_created_at,
        raw.job_expires_at
    FROM raw
    WHERE raw.worker_rank = 1
$$;
REVOKE ALL ON FUNCTION vela_scheduler_candidates(uuid, timestamptz) FROM PUBLIC;
ALTER FUNCTION vela_scheduler_candidates(uuid, timestamptz) OWNER TO vela_internal;

CREATE FUNCTION vela_list_schedulable_worker_pools() RETURNS TABLE (worker_pool_id uuid)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT DISTINCT worker.worker_pool_id
    FROM public.workers AS worker
    JOIN public.worker_profile_readiness AS readiness
      ON readiness.worker_id = worker.id AND readiness.worker_epoch = worker.epoch
    WHERE worker.lifecycle_state = 'READY'
      AND worker.reachability_condition = 'HEALTHY'
      AND readiness.readiness IN ('WARM', 'PREWARM_ALLOWED')
    ORDER BY worker.worker_pool_id
$$;
REVOKE ALL ON FUNCTION vela_list_schedulable_worker_pools() FROM PUBLIC;
ALTER FUNCTION vela_list_schedulable_worker_pools() OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_list_schedulable_worker_pools() TO vela_scheduler;

CREATE FUNCTION vela_claim_scheduler_dispatch(
    p_worker_pool_id uuid,
    p_scheduler_id text,
    p_claim_ttl_seconds integer
) RETURNS TABLE (
    intent_id uuid,
    organization_id uuid,
    service_class_revision_id uuid,
    project_id uuid,
    job_id uuid,
    expected_job_version bigint,
    worker_id uuid,
    worker_epoch bigint,
    execution_profile_revision_id uuid,
    lane scheduler_lane,
    predicted_runtime_seconds bigint,
    predicted_start_at timestamptz,
    predicted_finish_at timestamptz,
    job_order_score bigint,
    worker_score bigint,
    claim_expires_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_quantum bigint;
    v_max_deficit bigint;
    v_candidate_limit bigint;
    v_credit_scale numeric;
    v_organization_id uuid;
    v_service_class_revision_id uuid;
    v_project_id uuid;
    v_candidate record;
    v_candidates scheduler_candidate[];
    v_intent_id uuid := gen_random_uuid();
BEGIN
    IF p_scheduler_id IS NULL OR length(p_scheduler_id) NOT BETWEEN 1 AND 200 THEN
        RAISE EXCEPTION 'Scheduler identity is invalid' USING ERRCODE = '22023';
    END IF;
    IF p_claim_ttl_seconds NOT BETWEEN 1 AND 300 THEN
        RAISE EXCEPTION 'Scheduler claim TTL is invalid' USING ERRCODE = '22023';
    END IF;

    PERFORM pg_advisory_xact_lock(hashtextextended(p_worker_pool_id::text, 8617));
    UPDATE public.scheduler_dispatch_intents AS expired_dispatch
    SET state = 'ABANDONED',
        abandoned_at = v_now,
        abandon_reason = 'claim_expired'
    WHERE expired_dispatch.worker_pool_id = p_worker_pool_id
      AND expired_dispatch.state = 'CLAIMED'
      AND expired_dispatch.claim_expires_at <= v_now;

    SELECT
        pool.scheduler_quantum_seconds,
        pool.scheduler_max_deficit_seconds,
        pool.queued_limit::bigint + pool.retry_wait_count::bigint
    INTO v_quantum, v_max_deficit, v_candidate_limit
    FROM public.worker_pools AS pool
    WHERE pool.id = p_worker_pool_id;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    v_credit_scale := LEAST(
        1::numeric,
        v_max_deficit::numeric / (1000::numeric * v_quantum::numeric)
    );

    SELECT array_agg(candidate::public.scheduler_candidate ORDER BY candidate.job_id)
    INTO v_candidates
    FROM (
        SELECT bounded_candidate.*
        FROM public.vela_scheduler_candidates(
            p_worker_pool_id,
            v_now
        ) AS bounded_candidate
        ORDER BY bounded_candidate.job_id
        LIMIT v_candidate_limit + 1
    ) AS candidate;
    IF v_candidates IS NULL THEN
        RETURN;
    END IF;
    IF cardinality(v_candidates) > v_candidate_limit THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'scheduler_candidate_snapshot_exceeds_pool_waiting_bound',
            MESSAGE = 'Scheduler candidate snapshot exceeds the Worker pool waiting bound';
    END IF;

    INSERT INTO public.scheduler_organization_deficits (worker_pool_id, organization_id)
    SELECT DISTINCT p_worker_pool_id, candidate.organization_id
    FROM unnest(v_candidates) AS candidate
    ON CONFLICT DO NOTHING;

    IF NOT EXISTS (
        SELECT 1
        FROM public.scheduler_organization_deficits AS deficit
        WHERE deficit.worker_pool_id = p_worker_pool_id
          AND deficit.deficit_seconds > 0
          AND EXISTS (
              SELECT 1
              FROM unnest(v_candidates) AS candidate
              WHERE candidate.organization_id = deficit.organization_id
          )
    ) THEN
    WITH eligible AS (
        SELECT
            deficit.organization_id,
            deficit.deficit_seconds
                + capacity.weight::numeric * v_quantum::numeric * v_credit_scale AS accrued
        FROM public.scheduler_organization_deficits AS deficit
        JOIN public.organization_capacity_shares AS capacity
          ON capacity.worker_pool_id = deficit.worker_pool_id
         AND capacity.organization_id = deficit.organization_id
        WHERE deficit.worker_pool_id = p_worker_pool_id
          AND EXISTS (
              SELECT 1
              FROM unnest(v_candidates) AS candidate
              WHERE candidate.organization_id = deficit.organization_id
          )
    )
    UPDATE public.scheduler_organization_deficits AS deficit
    SET deficit_seconds = GREATEST(-v_max_deficit, eligible.accrued),
        version = deficit.version + 1,
        updated_at = v_now
    FROM eligible
    WHERE deficit.worker_pool_id = p_worker_pool_id
      AND eligible.organization_id = deficit.organization_id;
    END IF;

    SELECT candidate.organization_id
    INTO v_organization_id
    FROM unnest(v_candidates) AS candidate
    JOIN public.scheduler_organization_deficits AS deficit
      ON deficit.worker_pool_id = p_worker_pool_id
     AND deficit.organization_id = candidate.organization_id
    GROUP BY candidate.organization_id, deficit.deficit_seconds
    ORDER BY deficit.deficit_seconds DESC, min(candidate.job_created_at), candidate.organization_id
    LIMIT 1;
    IF v_organization_id IS NULL THEN
        RETURN;
    END IF;

    INSERT INTO public.scheduler_service_class_deficits (
        worker_pool_id, organization_id, service_class_revision_id
    )
    SELECT DISTINCT
        p_worker_pool_id, candidate.organization_id, candidate.service_class_revision_id
    FROM unnest(v_candidates) AS candidate
    WHERE candidate.organization_id = v_organization_id
    ON CONFLICT DO NOTHING;

    IF NOT EXISTS (
        SELECT 1
        FROM public.scheduler_service_class_deficits AS deficit
        WHERE deficit.worker_pool_id = p_worker_pool_id
          AND deficit.organization_id = v_organization_id
          AND deficit.deficit_seconds > 0
          AND EXISTS (
              SELECT 1
              FROM unnest(v_candidates) AS candidate
              WHERE candidate.organization_id = deficit.organization_id
                AND candidate.service_class_revision_id = deficit.service_class_revision_id
          )
    ) THEN
    WITH eligible AS (
        SELECT
            deficit.service_class_revision_id,
            deficit.deficit_seconds
                + service_class.queue_weight::numeric * v_quantum::numeric * v_credit_scale AS accrued
        FROM public.scheduler_service_class_deficits AS deficit
        JOIN public.service_class_revisions AS service_class
          ON service_class.id = deficit.service_class_revision_id
        WHERE deficit.worker_pool_id = p_worker_pool_id
          AND deficit.organization_id = v_organization_id
          AND EXISTS (
              SELECT 1
              FROM unnest(v_candidates) AS candidate
              WHERE candidate.organization_id = deficit.organization_id
                AND candidate.service_class_revision_id = deficit.service_class_revision_id
          )
    )
    UPDATE public.scheduler_service_class_deficits AS deficit
    SET deficit_seconds = GREATEST(-v_max_deficit, eligible.accrued),
        version = deficit.version + 1,
        updated_at = v_now
    FROM eligible
    WHERE deficit.worker_pool_id = p_worker_pool_id
      AND deficit.organization_id = v_organization_id
      AND eligible.service_class_revision_id = deficit.service_class_revision_id;
    END IF;

    SELECT candidate.service_class_revision_id
    INTO v_service_class_revision_id
    FROM unnest(v_candidates) AS candidate
    JOIN public.scheduler_service_class_deficits AS deficit
      ON deficit.worker_pool_id = p_worker_pool_id
     AND deficit.organization_id = candidate.organization_id
     AND deficit.service_class_revision_id = candidate.service_class_revision_id
    WHERE candidate.organization_id = v_organization_id
    GROUP BY candidate.service_class_revision_id, deficit.deficit_seconds
    ORDER BY deficit.deficit_seconds DESC, min(candidate.job_created_at),
             candidate.service_class_revision_id
    LIMIT 1;

    INSERT INTO public.scheduler_project_deficits (
        worker_pool_id, organization_id, service_class_revision_id, project_id
    )
    SELECT DISTINCT
        p_worker_pool_id,
        candidate.organization_id,
        candidate.service_class_revision_id,
        candidate.project_id
    FROM unnest(v_candidates) AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.service_class_revision_id = v_service_class_revision_id
    ON CONFLICT DO NOTHING;

    IF NOT EXISTS (
        SELECT 1
        FROM public.scheduler_project_deficits AS deficit
        WHERE deficit.worker_pool_id = p_worker_pool_id
          AND deficit.organization_id = v_organization_id
          AND deficit.service_class_revision_id = v_service_class_revision_id
          AND deficit.deficit_seconds > 0
          AND EXISTS (
              SELECT 1
              FROM unnest(v_candidates) AS candidate
              WHERE candidate.organization_id = deficit.organization_id
                AND candidate.service_class_revision_id = deficit.service_class_revision_id
                AND candidate.project_id = deficit.project_id
          )
    ) THEN
    WITH eligible AS (
        SELECT
            deficit.project_id,
            deficit.deficit_seconds
                + capacity.weight::numeric * v_quantum::numeric * v_credit_scale AS accrued
        FROM public.scheduler_project_deficits AS deficit
        JOIN public.project_capacity_shares AS capacity
          ON capacity.worker_pool_id = deficit.worker_pool_id
         AND capacity.organization_id = deficit.organization_id
         AND capacity.project_id = deficit.project_id
        WHERE deficit.worker_pool_id = p_worker_pool_id
          AND deficit.organization_id = v_organization_id
          AND deficit.service_class_revision_id = v_service_class_revision_id
          AND EXISTS (
              SELECT 1
              FROM unnest(v_candidates) AS candidate
              WHERE candidate.organization_id = deficit.organization_id
                AND candidate.service_class_revision_id = deficit.service_class_revision_id
                AND candidate.project_id = deficit.project_id
          )
    )
    UPDATE public.scheduler_project_deficits AS deficit
    SET deficit_seconds = GREATEST(-v_max_deficit, eligible.accrued),
        version = deficit.version + 1,
        updated_at = v_now
    FROM eligible
    WHERE deficit.worker_pool_id = p_worker_pool_id
      AND deficit.organization_id = v_organization_id
      AND deficit.service_class_revision_id = v_service_class_revision_id
      AND eligible.project_id = deficit.project_id;
    END IF;

    SELECT candidate.project_id
    INTO v_project_id
    FROM unnest(v_candidates) AS candidate
    JOIN public.scheduler_project_deficits AS deficit
      ON deficit.worker_pool_id = p_worker_pool_id
     AND deficit.organization_id = candidate.organization_id
     AND deficit.service_class_revision_id = candidate.service_class_revision_id
     AND deficit.project_id = candidate.project_id
    WHERE candidate.organization_id = v_organization_id
      AND candidate.service_class_revision_id = v_service_class_revision_id
    GROUP BY candidate.project_id, deficit.deficit_seconds
    ORDER BY deficit.deficit_seconds DESC, min(candidate.job_created_at), candidate.project_id
    LIMIT 1;

    SELECT candidate.*
    INTO v_candidate
    FROM unnest(v_candidates) AS candidate
    WHERE candidate.organization_id = v_organization_id
      AND candidate.service_class_revision_id = v_service_class_revision_id
      AND candidate.project_id = v_project_id
    ORDER BY
        CASE candidate.lane WHEN 'RETRY' THEN 0 WHEN 'PROTECTED' THEN 1 ELSE 2 END,
        CASE WHEN candidate.lane = 'PROTECTED' THEN candidate.job_expires_at END,
        CASE WHEN candidate.lane = 'PROTECTED' THEN candidate.job_created_at END,
        CASE WHEN candidate.lane = 'NORMAL' THEN candidate.job_order_score END,
        candidate.job_created_at,
        candidate.job_id,
        candidate.worker_score,
        candidate.execution_profile_revision_id,
        candidate.worker_id
    LIMIT 1;
    IF v_candidate.job_id IS NULL THEN
        RETURN;
    END IF;

    UPDATE public.scheduler_organization_deficits AS selected_organization
    SET deficit_seconds = GREATEST(
            -v_max_deficit,
            selected_organization.deficit_seconds
                - v_candidate.predicted_runtime_seconds::numeric * v_credit_scale
        ),
        last_selected_at = v_now,
        version = version + 1,
        updated_at = v_now
    WHERE selected_organization.worker_pool_id = p_worker_pool_id
      AND selected_organization.organization_id = v_organization_id;
    UPDATE public.scheduler_service_class_deficits AS selected_service_class
    SET deficit_seconds = GREATEST(
            -v_max_deficit,
            selected_service_class.deficit_seconds
                - v_candidate.predicted_runtime_seconds::numeric * v_credit_scale
        ),
        last_selected_at = v_now,
        version = version + 1,
        updated_at = v_now
    WHERE selected_service_class.worker_pool_id = p_worker_pool_id
      AND selected_service_class.organization_id = v_organization_id
      AND selected_service_class.service_class_revision_id = v_service_class_revision_id;
    UPDATE public.scheduler_project_deficits AS selected_project
    SET deficit_seconds = GREATEST(
            -v_max_deficit,
            selected_project.deficit_seconds
                - v_candidate.predicted_runtime_seconds::numeric * v_credit_scale
        ),
        last_selected_at = v_now,
        version = version + 1,
        updated_at = v_now
    WHERE selected_project.worker_pool_id = p_worker_pool_id
      AND selected_project.organization_id = v_organization_id
      AND selected_project.service_class_revision_id = v_service_class_revision_id
      AND selected_project.project_id = v_project_id;

    INSERT INTO public.scheduler_dispatch_intents (
        id,
        scheduler_id,
        worker_pool_id,
        organization_id,
        service_class_revision_id,
        project_id,
        job_id,
        expected_job_version,
        execution_profile_revision_id,
        worker_id,
        worker_epoch,
        lane,
        predicted_runtime_seconds,
        predicted_start_at,
        predicted_finish_at,
        job_order_score,
        worker_score,
        claimed_at,
        claim_expires_at
    ) VALUES (
        v_intent_id,
        p_scheduler_id,
        p_worker_pool_id,
        v_candidate.organization_id,
        v_candidate.service_class_revision_id,
        v_candidate.project_id,
        v_candidate.job_id,
        v_candidate.job_version,
        v_candidate.execution_profile_revision_id,
        v_candidate.worker_id,
        v_candidate.worker_epoch,
        v_candidate.lane,
        v_candidate.predicted_runtime_seconds,
        v_candidate.predicted_start_at,
        v_candidate.predicted_finish_at,
        v_candidate.job_order_score,
        v_candidate.worker_score,
        v_now,
        v_now + make_interval(secs => p_claim_ttl_seconds)
    );

    RETURN QUERY SELECT
        v_intent_id,
        v_candidate.organization_id,
        v_candidate.service_class_revision_id,
        v_candidate.project_id,
        v_candidate.job_id,
        v_candidate.job_version,
        v_candidate.worker_id,
        v_candidate.worker_epoch,
        v_candidate.execution_profile_revision_id,
        v_candidate.lane,
        v_candidate.predicted_runtime_seconds,
        v_candidate.predicted_start_at,
        v_candidate.predicted_finish_at,
        v_candidate.job_order_score,
        v_candidate.worker_score,
        v_now + make_interval(secs => p_claim_ttl_seconds);
END
$$;
REVOKE ALL ON FUNCTION vela_claim_scheduler_dispatch(uuid, text, integer) FROM PUBLIC;
ALTER FUNCTION vela_claim_scheduler_dispatch(uuid, text, integer) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_claim_scheduler_dispatch(uuid, text, integer) TO vela_scheduler;

CREATE FUNCTION vela_abandon_scheduler_dispatch(
    p_intent_id uuid,
    p_scheduler_id text,
    p_reason text
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_reason IS NULL OR length(p_reason) NOT BETWEEN 1 AND 500 THEN
        RAISE EXCEPTION 'Scheduler abandon reason is invalid' USING ERRCODE = '22023';
    END IF;
    UPDATE public.scheduler_dispatch_intents
    SET state = 'ABANDONED', abandoned_at = clock_timestamp(), abandon_reason = p_reason
    WHERE id = p_intent_id
      AND scheduler_id = p_scheduler_id
      AND state = 'CLAIMED';
    RETURN FOUND;
END
$$;
REVOKE ALL ON FUNCTION vela_abandon_scheduler_dispatch(uuid, text, text) FROM PUBLIC;
ALTER FUNCTION vela_abandon_scheduler_dispatch(uuid, text, text) OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_abandon_scheduler_dispatch(uuid, text, text) TO vela_scheduler;

CREATE FUNCTION vela_reconcile_expired_scheduler_dispatches() RETURNS bigint
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_rows bigint;
BEGIN
    UPDATE public.scheduler_dispatch_intents
    SET state = 'ABANDONED',
        abandoned_at = clock_timestamp(),
        abandon_reason = 'claim_expired'
    WHERE state = 'CLAIMED' AND claim_expires_at <= clock_timestamp();
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    RETURN v_rows;
END
$$;
REVOKE ALL ON FUNCTION vela_reconcile_expired_scheduler_dispatches() FROM PUBLIC;
ALTER FUNCTION vela_reconcile_expired_scheduler_dispatches() OWNER TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_reconcile_expired_scheduler_dispatches() TO vela_scheduler;

CREATE FUNCTION vela_transition_scheduler_dispatch_protocol(
    p_require_dispatch_intent boolean,
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
    IF (
        p_receipt IS NULL
        OR length(p_receipt) NOT BETWEEN 1 AND 1000
        OR btrim(p_receipt) = ''
    ) THEN
        RAISE EXCEPTION 'Scheduler dispatch protocol transition receipt is required'
            USING ERRCODE = '22023';
    END IF;
    SELECT state.protocol_version INTO v_protocol_version
    FROM public.scheduler_dispatch_protocol_state AS state
    WHERE state.singleton
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'scheduler_dispatch_protocol_state_required',
            MESSAGE = 'Scheduler dispatch protocol state is missing';
    END IF;
    v_protocol_version := v_protocol_version + 1;
    v_transitioned_at := clock_timestamp();
    INSERT INTO public.scheduler_dispatch_protocol_transitions (
        protocol_version,
        require_dispatch_intent,
        transition_receipt,
        transitioned_at
    ) VALUES (
        v_protocol_version,
        p_require_dispatch_intent,
        p_receipt,
        v_transitioned_at
    );
END
$$;
REVOKE ALL ON FUNCTION vela_transition_scheduler_dispatch_protocol(boolean, text) FROM PUBLIC;

CREATE FUNCTION vela_enforce_scheduler_dispatch_attempt() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_required boolean;
    v_rows bigint;
BEGIN
    SELECT require_dispatch_intent INTO STRICT v_required
    FROM public.scheduler_dispatch_protocol_state WHERE singleton
    FOR SHARE;
    IF NEW.scheduler_dispatch_intent_id IS NULL THEN
        IF v_required THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'attempts_scheduler_dispatch_required',
                MESSAGE = 'Assignment requires a live Scheduler dispatch claim';
        END IF;
        RETURN NEW;
    END IF;

    UPDATE public.scheduler_dispatch_intents AS dispatch
    SET state = 'COMMITTED', committed_at = NEW.assigned_at
    FROM public.jobs AS job
    WHERE dispatch.id = NEW.scheduler_dispatch_intent_id
      AND dispatch.state = 'CLAIMED'
      AND dispatch.claim_expires_at > clock_timestamp()
      AND dispatch.organization_id = NEW.organization_id
      AND dispatch.project_id = NEW.project_id
      AND dispatch.job_id = NEW.job_id
	  AND dispatch.service_class_revision_id = job.service_class_revision_id
      AND dispatch.worker_pool_id = NEW.worker_pool_id
      AND dispatch.execution_profile_revision_id = NEW.execution_profile_revision_id
      AND dispatch.worker_id = NEW.worker_id
      AND dispatch.worker_epoch = NEW.worker_epoch
      AND job.id = NEW.job_id
      AND job.version = dispatch.expected_job_version;
    GET DIAGNOSTICS v_rows = ROW_COUNT;
    IF v_rows <> 1 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempts_scheduler_dispatch_exact_claim',
            MESSAGE = 'Assignment does not match a live Scheduler dispatch claim';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_scheduler_dispatch_attempt() FROM PUBLIC;

CREATE TRIGGER attempts_enforce_scheduler_dispatch
BEFORE INSERT ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_enforce_scheduler_dispatch_attempt();

CREATE FUNCTION vela_reject_scheduler_dispatch_attempt_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.scheduler_dispatch_intent_id IS DISTINCT FROM OLD.scheduler_dispatch_intent_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempts_scheduler_dispatch_immutable',
            MESSAGE = 'Attempt Scheduler dispatch identity is immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_scheduler_dispatch_attempt_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_scheduler_dispatch_attempt_mutation() OWNER TO vela_internal;
CREATE TRIGGER attempts_reject_scheduler_dispatch_mutation
BEFORE UPDATE OF scheduler_dispatch_intent_id ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_reject_scheduler_dispatch_attempt_mutation();

GRANT USAGE ON SCHEMA public TO vela_scheduler;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    organization_capacity_shares,
    project_capacity_shares,
    worker_profile_readiness,
    job_runtime_predictions,
    scheduler_organization_deficits,
    scheduler_service_class_deficits,
    scheduler_project_deficits,
    scheduler_dispatch_intents
TO vela_internal;
GRANT SELECT (scheduler_dispatch_intent_id) ON attempts TO vela_internal;
GRANT UPDATE (scheduler_dispatch_intent_id) ON attempts TO vela_internal;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    LOCK TABLE scheduler_dispatch_intents, attempts, organization_capacity_shares,
			project_capacity_shares, worker_profile_readiness, job_runtime_predictions,
			scheduler_dispatch_protocol_state, scheduler_dispatch_protocol_transitions,
			worker_pools, service_class_revisions
        IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM scheduler_dispatch_intents)
       OR EXISTS (SELECT 1 FROM attempts WHERE scheduler_dispatch_intent_id IS NOT NULL)
       OR EXISTS (SELECT 1 FROM organization_capacity_shares)
       OR EXISTS (SELECT 1 FROM project_capacity_shares)
       OR EXISTS (SELECT 1 FROM worker_profile_readiness)
       OR EXISTS (SELECT 1 FROM job_runtime_predictions)
	   OR EXISTS (
		   SELECT 1 FROM worker_pools
		   WHERE scheduler_quantum_seconds <> 1200
		      OR scheduler_max_deficit_seconds <> 86400
		      OR retry_running_limit <> 1
	   )
	   OR EXISTS (
		   SELECT 1 FROM service_class_revisions
		   WHERE queue_weight <> 1
		      OR max_queue_wait_before_protection_seconds <> 1800
		      OR max_aging_credit_seconds <> 900
		      OR max_expiry_urgency_credit_seconds <> 900
		      OR max_retry_risk_penalty_seconds <> 0
	   )
       OR EXISTS (
           SELECT 1 FROM scheduler_dispatch_protocol_state WHERE protocol_version <> 1
       )
       OR EXISTS (
           SELECT 1 FROM scheduler_dispatch_protocol_transitions
       ) THEN
        RAISE EXCEPTION 'Cannot contract Scheduler migration while durable scheduling evidence exists'
			USING ERRCODE = '55000',
				  CONSTRAINT = 'hierarchical_scheduler_contract_requires_empty_evidence';
    END IF;
END
$$;

CREATE OR REPLACE VIEW vela_request_job_progress
WITH (security_barrier = true) AS
WITH postgres_time AS MATERIALIZED (
    SELECT clock_timestamp() AS observed_at
)
SELECT
    progress.job_id,
    CASE
        WHEN progress.progress_valid_until > postgres_time.observed_at
            THEN progress.phase_progress
        ELSE NULL
    END::double precision AS phase_progress,
    CASE
        WHEN progress.progress_valid_until > postgres_time.observed_at
            THEN progress.estimated_finish_at
        ELSE NULL
    END::timestamptz AS estimated_finish_at,
    progress.progress_updated_at
FROM public.jobs AS job
JOIN public.attempt_progress AS progress
  ON progress.job_id = job.id
 AND progress.fence = job.current_fence
 AND progress.execution_phase = job.execution_phase
CROSS JOIN postgres_time
WHERE public.vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
  AND job.organization_id = public.vela_current_organization_id()
  AND job.project_id = public.vela_current_project_id();
ALTER VIEW vela_request_job_progress OWNER TO vela_internal;
REVOKE ALL ON vela_request_job_progress FROM PUBLIC;
GRANT SELECT ON vela_request_job_progress TO vela_request;

DROP INDEX attempts_active_retry_pool_idx;
DROP INDEX attempts_active_pool_organization_idx;
DROP TRIGGER attempts_reject_scheduler_dispatch_mutation ON attempts;
DROP FUNCTION vela_reject_scheduler_dispatch_attempt_mutation();
DROP TRIGGER attempts_enforce_scheduler_dispatch ON attempts;
DROP FUNCTION vela_enforce_scheduler_dispatch_attempt();
DROP FUNCTION vela_transition_scheduler_dispatch_protocol(boolean, text);
DROP FUNCTION vela_reconcile_expired_scheduler_dispatches();
DROP FUNCTION vela_abandon_scheduler_dispatch(uuid, text, text);
DROP FUNCTION vela_claim_scheduler_dispatch(uuid, text, integer);
DROP FUNCTION vela_list_schedulable_worker_pools();
DROP FUNCTION vela_scheduler_candidates(uuid, timestamptz);
DROP FUNCTION vela_predict_job_dynamic_eta(uuid);
DROP FUNCTION vela_predict_admission_capacity(uuid, uuid, uuid, uuid, uuid, integer);
DROP FUNCTION vela_scheduler_queue_projection();
DROP FUNCTION vela_scheduler_queue_projection_for_pool(uuid, timestamptz);
DROP FUNCTION vela_scheduler_projectable_jobs(uuid, timestamptz);
DROP FUNCTION vela_scheduler_job_worker_compatibility(uuid, timestamptz, uuid);
DROP FUNCTION vela_scheduler_capacity_timeline(uuid, timestamptz);
DROP TRIGGER scheduler_dispatch_protocol_apply_transition ON scheduler_dispatch_protocol_transitions;
DROP FUNCTION vela_apply_scheduler_dispatch_protocol_transition();
DROP TRIGGER scheduler_dispatch_protocol_validate_transition ON scheduler_dispatch_protocol_transitions;
DROP FUNCTION vela_validate_scheduler_dispatch_protocol_transition();
DROP TRIGGER scheduler_dispatch_protocol_history_immutable ON scheduler_dispatch_protocol_transitions;
DROP FUNCTION vela_reject_scheduler_dispatch_protocol_history_mutation();
DROP TABLE scheduler_dispatch_protocol_transitions;
DROP TRIGGER scheduler_dispatch_protocol_enforce_transition ON scheduler_dispatch_protocol_state;
DROP FUNCTION vela_enforce_scheduler_dispatch_protocol_state_transition();
DROP TRIGGER scheduler_dispatch_protocol_reject_delete ON scheduler_dispatch_protocol_state;
DROP FUNCTION vela_reject_scheduler_dispatch_protocol_delete();
DROP TRIGGER jobs_reject_waiting_age_mutation ON jobs;
DROP FUNCTION vela_reject_job_waiting_age_mutation();
ALTER TABLE attempts DROP COLUMN scheduler_dispatch_intent_id;
DROP TABLE scheduler_dispatch_protocol_state;
DROP TABLE scheduler_dispatch_intents;
DROP TABLE scheduler_project_deficits;
DROP TABLE scheduler_service_class_deficits;
DROP TABLE scheduler_organization_deficits;
DROP TABLE job_runtime_predictions;
DROP TABLE worker_profile_readiness;
DROP TABLE project_capacity_shares;
DROP TABLE organization_capacity_shares;
ALTER TABLE worker_pools
    DROP COLUMN retry_running_limit,
    DROP COLUMN scheduler_max_deficit_seconds,
    DROP COLUMN scheduler_quantum_seconds;
ALTER TABLE service_class_revisions
    DROP COLUMN max_retry_risk_penalty_seconds,
    DROP COLUMN max_expiry_urgency_credit_seconds,
    DROP COLUMN max_aging_credit_seconds,
    DROP COLUMN max_queue_wait_before_protection_seconds,
    DROP COLUMN queue_weight;
DROP TYPE scheduler_dispatch_state;
DROP TYPE scheduler_candidate;
DROP TYPE scheduler_lane;
DROP TYPE worker_profile_readiness_state;
-- +goose StatementEnd
