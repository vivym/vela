-- +goose Up
-- +goose StatementBegin
CREATE TYPE stage_scheduler_claim_state AS ENUM (
    'CLAIMED', 'COMMITTED', 'ABANDONED', 'EXPIRED'
);

CREATE TABLE stage_ready_queue_entries (
    stage_run_id uuid NOT NULL REFERENCES stage_runs(id),
    capacity_pool_id uuid NOT NULL REFERENCES capacity_pools(id),
    attempt_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    project_id uuid NOT NULL,
    stage_profile_revision_id uuid NOT NULL,
    enqueued_at timestamptz NOT NULL,
    resource_millis bigint NOT NULL CHECK (resource_millis > 0),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (capacity_pool_id, stage_run_id),
    FOREIGN KEY (attempt_id, stage_run_id) REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (capacity_pool_id, stage_profile_revision_id)
        REFERENCES capacity_pools(id, stage_profile_revision_id),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id)
);

CREATE INDEX stage_ready_queue_order_idx
    ON stage_ready_queue_entries (capacity_pool_id, enqueued_at, stage_run_id);

CREATE TABLE stage_capacity_pool_counters (
    capacity_pool_id uuid PRIMARY KEY REFERENCES capacity_pools(id),
    ready_count integer NOT NULL DEFAULT 0 CHECK (ready_count >= 0),
    claimed_count integer NOT NULL DEFAULT 0 CHECK (claimed_count >= 0),
    active_allocation_count integer NOT NULL DEFAULT 0 CHECK (active_allocation_count >= 0),
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE stage_scheduler_organization_deficits (
    capacity_pool_id uuid NOT NULL REFERENCES capacity_pools(id),
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    deficit_millis bigint NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    last_selected_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (capacity_pool_id, organization_id)
);

CREATE TABLE stage_scheduler_service_class_deficits (
    capacity_pool_id uuid NOT NULL REFERENCES capacity_pools(id),
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    deficit_millis bigint NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    last_selected_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (capacity_pool_id, organization_id, service_class_revision_id)
);

CREATE TABLE stage_scheduler_project_deficits (
    capacity_pool_id uuid NOT NULL REFERENCES capacity_pools(id),
    organization_id uuid NOT NULL,
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    project_id uuid NOT NULL,
    deficit_millis bigint NOT NULL DEFAULT 0,
    version bigint NOT NULL DEFAULT 1 CHECK (version > 0),
    last_selected_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (
        capacity_pool_id, organization_id, service_class_revision_id, project_id
    ),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id)
);

CREATE TABLE stage_scheduler_snapshot_traces (
    id uuid PRIMARY KEY,
    algorithm_revision text NOT NULL CHECK (length(algorithm_revision) BETWEEN 1 AND 100),
    evaluated_at timestamptz NOT NULL,
    valid_until timestamptz NOT NULL,
    capacity_pool_id uuid NOT NULL REFERENCES capacity_pools(id),
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    observation_sequence bigint NOT NULL CHECK (observation_sequence > 0),
    snapshot jsonb NOT NULL CHECK (jsonb_typeof(snapshot) = 'object'),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (valid_until > evaluated_at)
);

CREATE INDEX stage_scheduler_snapshot_shadow_idx
    ON stage_scheduler_snapshot_traces (created_at, id);

CREATE TABLE stage_decision_evidence (
    id uuid PRIMARY KEY,
    snapshot_trace_id uuid NOT NULL REFERENCES stage_scheduler_snapshot_traces(id),
    algorithm_revision text NOT NULL CHECK (length(algorithm_revision) BETWEEN 1 AND 100),
    input_digest bytea NOT NULL CHECK (octet_length(input_digest) = 32),
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    capacity_pool_id uuid NOT NULL REFERENCES capacity_pools(id),
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    stage_run_id uuid NOT NULL REFERENCES stage_runs(id),
    attempt_id uuid NOT NULL,
    stage_profile_revision_id uuid NOT NULL REFERENCES stage_profile_revisions(id),
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    service_class_revision_id uuid NOT NULL REFERENCES service_class_revisions(id),
    project_id uuid NOT NULL,
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    stage_fence bigint NOT NULL CHECK (stage_fence > 0),
    stage_version bigint NOT NULL CHECK (stage_version > 0),
    lane scheduler_lane NOT NULL,
    resource_millis bigint NOT NULL CHECK (resource_millis > 0),
    organization_deficit_millis bigint NOT NULL,
    service_class_deficit_millis bigint NOT NULL,
    project_deficit_millis bigint NOT NULL,
    filter_reason_counts jsonb NOT NULL CHECK (jsonb_typeof(filter_reason_counts) = 'object'),
    score_terms jsonb NOT NULL CHECK (jsonb_typeof(score_terms) = 'object'),
    score_total_millis bigint NOT NULL,
    tie_break jsonb NOT NULL CHECK (jsonb_typeof(tie_break) = 'object'),
    decided_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (snapshot_trace_id, evidence_digest),
    FOREIGN KEY (attempt_id, stage_run_id) REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (organization_id, project_id) REFERENCES projects(organization_id, id)
);

CREATE TABLE stage_scheduler_claims (
    id uuid PRIMARY KEY,
    decision_id uuid NOT NULL UNIQUE REFERENCES stage_decision_evidence(id),
    scheduler_id text NOT NULL CHECK (length(scheduler_id) BETWEEN 1 AND 200),
    capacity_pool_id uuid NOT NULL REFERENCES capacity_pools(id),
    worker_instance_id uuid NOT NULL REFERENCES worker_instances(id),
    stage_run_id uuid NOT NULL REFERENCES stage_runs(id),
    stage_attempt_id uuid NOT NULL UNIQUE,
    stage_allocation_id uuid NOT NULL UNIQUE,
    stage_lease_id uuid NOT NULL UNIQUE,
    command_payload jsonb NOT NULL CHECK (jsonb_typeof(command_payload) = 'object'),
    state stage_scheduler_claim_state NOT NULL DEFAULT 'CLAIMED',
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    claim_expires_at timestamptz NOT NULL,
    committed_at timestamptz,
    abandoned_at timestamptz,
    abandon_reason text CHECK (
        abandon_reason IS NULL OR length(abandon_reason) BETWEEN 1 AND 100
    ),
    fairness_accounted boolean NOT NULL DEFAULT false,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (claim_expires_at > claimed_at),
    CHECK (
        (state = 'CLAIMED' AND committed_at IS NULL AND abandoned_at IS NULL
         AND abandon_reason IS NULL AND NOT fairness_accounted)
        OR (state = 'COMMITTED' AND committed_at IS NOT NULL AND abandoned_at IS NULL
            AND abandon_reason IS NULL AND fairness_accounted)
        OR (state IN ('ABANDONED', 'EXPIRED') AND committed_at IS NULL
            AND abandoned_at IS NOT NULL AND abandon_reason IS NOT NULL
            AND NOT fairness_accounted)
    )
);

CREATE UNIQUE INDEX stage_scheduler_claims_one_live_stage_idx
    ON stage_scheduler_claims(stage_run_id) WHERE state = 'CLAIMED';
CREATE INDEX stage_scheduler_claims_expiry_idx
    ON stage_scheduler_claims(claim_expires_at, id) WHERE state = 'CLAIMED';

CREATE TABLE stage_scheduler_shadow_replay_receipts (
    id uuid PRIMARY KEY,
    snapshot_trace_id uuid NOT NULL REFERENCES stage_scheduler_snapshot_traces(id),
    algorithm_revision text NOT NULL CHECK (length(algorithm_revision) BETWEEN 1 AND 100),
    expected_evidence_digest bytea NOT NULL CHECK (octet_length(expected_evidence_digest) = 32),
    replayed_evidence_digest bytea NOT NULL CHECK (octet_length(replayed_evidence_digest) = 32),
    matched boolean NOT NULL,
    replayed_at timestamptz NOT NULL,
    replayed_by text NOT NULL CHECK (length(replayed_by) BETWEEN 1 AND 200),
    UNIQUE (snapshot_trace_id, algorithm_revision, replayed_by)
);

CREATE FUNCTION vela_reject_stage_scheduler_evidence_mutation() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'stage_scheduler_evidence_immutable',
        MESSAGE = TG_TABLE_NAME || ' is append-only';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_stage_scheduler_evidence_mutation() FROM PUBLIC;

CREATE TRIGGER stage_scheduler_snapshot_traces_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON stage_scheduler_snapshot_traces
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_scheduler_evidence_mutation();
CREATE TRIGGER stage_decision_evidence_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON stage_decision_evidence
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_scheduler_evidence_mutation();
CREATE TRIGGER stage_scheduler_shadow_replay_receipts_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON stage_scheduler_shadow_replay_receipts
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_stage_scheduler_evidence_mutation();

CREATE FUNCTION vela_refresh_stage_capacity_pool_counter(p_capacity_pool_id uuid)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_capacity_pool_id IS NULL THEN
        RETURN;
    END IF;
    INSERT INTO public.stage_capacity_pool_counters (
        capacity_pool_id, ready_count, claimed_count, active_allocation_count
    )
    SELECT
        pool.id,
        (SELECT count(*)::integer FROM public.stage_ready_queue_entries AS ready
         WHERE ready.capacity_pool_id = pool.id),
        (SELECT count(*)::integer FROM public.stage_scheduler_claims AS claim
         WHERE claim.capacity_pool_id = pool.id AND claim.state = 'CLAIMED'),
        (SELECT count(*)::integer FROM public.stage_allocations AS allocation
         WHERE allocation.capacity_pool_id = pool.id AND allocation.state = 'ALLOCATED')
    FROM public.capacity_pools AS pool
    WHERE pool.id = p_capacity_pool_id
    ON CONFLICT (capacity_pool_id) DO UPDATE
    SET ready_count = EXCLUDED.ready_count,
        claimed_count = EXCLUDED.claimed_count,
        active_allocation_count = EXCLUDED.active_allocation_count,
        version = public.stage_capacity_pool_counters.version + 1,
        updated_at = clock_timestamp();
END
$$;
REVOKE ALL ON FUNCTION vela_refresh_stage_capacity_pool_counter(uuid) FROM PUBLIC;
ALTER FUNCTION vela_refresh_stage_capacity_pool_counter(uuid)
    OWNER TO vela_stage_scheduler_owner;

CREATE FUNCTION vela_refresh_stage_ready_queue(p_stage_run_id uuid) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_pool_id uuid;
BEGIN
    FOR v_pool_id IN
        SELECT ready.capacity_pool_id
        FROM public.stage_ready_queue_entries AS ready
        WHERE ready.stage_run_id = p_stage_run_id
    LOOP
        DELETE FROM public.stage_ready_queue_entries
        WHERE stage_run_id = p_stage_run_id AND capacity_pool_id = v_pool_id;
        PERFORM public.vela_refresh_stage_capacity_pool_counter(v_pool_id);
    END LOOP;

    INSERT INTO public.stage_ready_queue_entries (
        stage_run_id, capacity_pool_id, attempt_id, organization_id,
        service_class_revision_id, project_id, stage_profile_revision_id,
        enqueued_at, resource_millis
    )
    SELECT
        run.id, pool.id, run.attempt_id, run.organization_id,
        job.service_class_revision_id, run.project_id, pool.stage_profile_revision_id,
        run.updated_at,
        CASE
            WHEN profile.certified_capacity_vector ->> 'resource_millis' ~ '^[1-9][0-9]*$'
                THEN (profile.certified_capacity_vector ->> 'resource_millis')::bigint
            WHEN profile.certified_capacity_vector ->> 'p95_service_millis' ~ '^[1-9][0-9]*$'
                THEN (profile.certified_capacity_vector ->> 'p95_service_millis')::bigint
            ELSE 1000
        END
    FROM public.stage_runs AS run
    JOIN public.attempts AS attempt ON attempt.id = run.attempt_id
    JOIN public.jobs AS job ON job.id = attempt.job_id
    JOIN public.capacity_pools AS pool
      ON pool.stage_profile_revision_id = ANY(run.allowed_stage_profile_revision_ids)
     AND pool.state = 'ACTIVE'
    JOIN public.stage_profile_revisions AS profile
      ON profile.id = pool.stage_profile_revision_id
    WHERE run.id = p_stage_run_id
      AND run.state = 'READY'
      AND attempt.execution_authority_kind = 'STAGE_GRAPH'
      AND attempt.graph_state IN ('QUEUED', 'RUNNING')
      AND attempt.fence = job.current_fence
    ON CONFLICT (capacity_pool_id, stage_run_id) DO NOTHING;

    INSERT INTO public.stage_scheduler_organization_deficits (
        capacity_pool_id, organization_id
    )
    SELECT DISTINCT ready.capacity_pool_id, ready.organization_id
    FROM public.stage_ready_queue_entries AS ready
    WHERE ready.stage_run_id = p_stage_run_id
    ON CONFLICT DO NOTHING;
    INSERT INTO public.stage_scheduler_service_class_deficits (
        capacity_pool_id, organization_id, service_class_revision_id
    )
    SELECT DISTINCT ready.capacity_pool_id, ready.organization_id,
        ready.service_class_revision_id
    FROM public.stage_ready_queue_entries AS ready
    WHERE ready.stage_run_id = p_stage_run_id
    ON CONFLICT DO NOTHING;
    INSERT INTO public.stage_scheduler_project_deficits (
        capacity_pool_id, organization_id, service_class_revision_id, project_id
    )
    SELECT DISTINCT ready.capacity_pool_id, ready.organization_id,
        ready.service_class_revision_id, ready.project_id
    FROM public.stage_ready_queue_entries AS ready
    WHERE ready.stage_run_id = p_stage_run_id
    ON CONFLICT DO NOTHING;

    IF EXISTS (
        SELECT 1
        FROM public.stage_ready_queue_entries AS ready
        JOIN public.capacity_pools AS pool ON pool.id = ready.capacity_pool_id
        WHERE ready.stage_run_id = p_stage_run_id
        GROUP BY pool.id, pool.max_ready_queue_depth
        HAVING count(*) FILTER (WHERE ready.capacity_pool_id = pool.id)
            > pool.max_ready_queue_depth
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '54000',
            CONSTRAINT = 'stage_ready_queue_depth_exceeded',
            MESSAGE = 'Stage READY queue exceeds the CapacityPool bound';
    END IF;

    FOR v_pool_id IN
        SELECT ready.capacity_pool_id
        FROM public.stage_ready_queue_entries AS ready
        WHERE ready.stage_run_id = p_stage_run_id
    LOOP
        PERFORM public.vela_refresh_stage_capacity_pool_counter(v_pool_id);
    END LOOP;
END
$$;
REVOKE ALL ON FUNCTION vela_refresh_stage_ready_queue(uuid) FROM PUBLIC;
ALTER FUNCTION vela_refresh_stage_ready_queue(uuid) OWNER TO vela_stage_scheduler_owner;

CREATE FUNCTION vela_stage_ready_queue_run_trigger() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM public.vela_refresh_stage_ready_queue(COALESCE(NEW.id, OLD.id));
    RETURN COALESCE(NEW, OLD);
END
$$;
REVOKE ALL ON FUNCTION vela_stage_ready_queue_run_trigger() FROM PUBLIC;
ALTER FUNCTION vela_stage_ready_queue_run_trigger() OWNER TO vela_stage_scheduler_owner;

CREATE TRIGGER stage_runs_ready_queue_projection
AFTER INSERT OR UPDATE OF state, fence, version, updated_at ON stage_runs
FOR EACH ROW EXECUTE FUNCTION vela_stage_ready_queue_run_trigger();

CREATE FUNCTION vela_stage_ready_queue_pool_trigger() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_run_id uuid;
BEGIN
    FOR v_run_id IN
        SELECT run.id
        FROM public.stage_runs AS run
        WHERE NEW.stage_profile_revision_id = ANY(run.allowed_stage_profile_revision_ids)
           OR (OLD IS NOT NULL AND OLD.stage_profile_revision_id = ANY(run.allowed_stage_profile_revision_ids))
    LOOP
        PERFORM public.vela_refresh_stage_ready_queue(v_run_id);
    END LOOP;
    PERFORM public.vela_refresh_stage_capacity_pool_counter(NEW.id);
    IF OLD IS NOT NULL AND OLD.id <> NEW.id THEN
        PERFORM public.vela_refresh_stage_capacity_pool_counter(OLD.id);
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_stage_ready_queue_pool_trigger() FROM PUBLIC;
ALTER FUNCTION vela_stage_ready_queue_pool_trigger() OWNER TO vela_stage_scheduler_owner;

CREATE TRIGGER capacity_pools_ready_queue_projection
AFTER INSERT OR UPDATE OF stage_profile_revision_id, state ON capacity_pools
FOR EACH ROW EXECUTE FUNCTION vela_stage_ready_queue_pool_trigger();

CREATE FUNCTION vela_stage_capacity_counter_trigger() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF TG_TABLE_NAME = 'stage_allocations' THEN
        PERFORM public.vela_refresh_stage_capacity_pool_counter(
            COALESCE(NEW.capacity_pool_id, OLD.capacity_pool_id)
        );
    ELSE
        PERFORM public.vela_refresh_stage_capacity_pool_counter(
            COALESCE(NEW.capacity_pool_id, OLD.capacity_pool_id)
        );
    END IF;
    RETURN COALESCE(NEW, OLD);
END
$$;
REVOKE ALL ON FUNCTION vela_stage_capacity_counter_trigger() FROM PUBLIC;
ALTER FUNCTION vela_stage_capacity_counter_trigger() OWNER TO vela_stage_scheduler_owner;

CREATE TRIGGER stage_allocations_capacity_counter
AFTER INSERT OR UPDATE OF state OR DELETE ON stage_allocations
FOR EACH ROW EXECUTE FUNCTION vela_stage_capacity_counter_trigger();
CREATE TRIGGER stage_scheduler_claims_capacity_counter
AFTER INSERT OR UPDATE OF state OR DELETE ON stage_scheduler_claims
FOR EACH ROW EXECUTE FUNCTION vela_stage_capacity_counter_trigger();

CREATE FUNCTION vela_capture_stage_scheduler_snapshot(p_authority jsonb)
RETURNS TABLE (snapshot_id uuid, snapshot jsonb)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_snapshot_id uuid;
    v_pool public.capacity_pools%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_residency public.model_residencies%ROWTYPE;
    v_observation public.capacity_observations%ROWTYPE;
    v_candidates jsonb;
    v_snapshot jsonb;
    v_active integer;
    v_limit integer;
BEGIN
    IF p_authority IS NULL OR jsonb_typeof(p_authority) <> 'object'
       OR (p_authority ->> 'schema_version')::integer <> 1 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'StageScheduler authority is invalid';
    END IF;
    SELECT pool.* INTO v_pool
    FROM public.capacity_pools AS pool
    WHERE pool.id = (p_authority ->> 'capacity_pool_id')::uuid
      AND pool.stage_profile_revision_id =
          (p_authority ->> 'stage_profile_revision_id')::uuid
      AND pool.state = 'ACTIVE';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_capacity_pool_stale',
            MESSAGE = 'StageScheduler CapacityPool authority is stale';
    END IF;
    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = (p_authority ->> 'worker_instance_id')::uuid
      AND worker.capacity_pool_id = v_pool.id
      AND worker.instance_epoch = (p_authority ->> 'worker_instance_epoch')::bigint
      AND worker.lifecycle_state = 'READY'
      AND worker.reachability_state = 'CONNECTED'
      AND worker.device_set_digest = decode(p_authority ->> 'device_set_digest', 'hex')
      AND worker.membership_digest = decode(p_authority ->> 'membership_digest', 'hex');
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_worker_authority_stale',
            MESSAGE = 'StageScheduler WorkerInstance authority is stale';
    END IF;
    SELECT residency.* INTO v_residency
    FROM public.model_residencies AS residency
    JOIN public.stage_profile_revisions AS profile
      ON profile.id = v_pool.stage_profile_revision_id
     AND profile.model_component_revision = residency.model_component_revision
    WHERE residency.id = (p_authority ->> 'model_residency_id')::uuid
      AND residency.worker_instance_id = v_worker.id
      AND residency.worker_instance_epoch = v_worker.instance_epoch
      AND residency.model_runtime_epoch = (p_authority ->> 'model_runtime_epoch')::bigint
      AND residency.state = 'READY';
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_model_residency_stale',
            MESSAGE = 'StageScheduler ModelResidency authority is stale';
    END IF;
    SELECT observation.* INTO v_observation
    FROM public.capacity_observations AS observation
    WHERE observation.worker_instance_id = v_worker.id
      AND observation.worker_instance_epoch = v_worker.instance_epoch
      AND observation.observation_sequence =
          (p_authority ->> 'observation_sequence')::bigint
      AND observation.expires_at > v_now
      AND observation.capacity_vector = p_authority -> 'capacity_vector';
    IF NOT FOUND OR NOT public.vela_worker_instance_authority_matches(
        v_worker.id, v_worker.instance_epoch, v_worker.device_set_digest,
        v_worker.membership_digest, v_residency.id, v_residency.model_runtime_epoch
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_capacity_observation_stale',
            MESSAGE = 'StageScheduler CapacityObservation authority is stale';
    END IF;

    SELECT count(*)::integer INTO v_active
    FROM public.stage_allocations AS allocation
    WHERE allocation.worker_instance_id = v_worker.id
      AND allocation.worker_instance_epoch = v_worker.instance_epoch
      AND allocation.state = 'ALLOCATED';
    v_limit := COALESCE((v_observation.capacity_vector ->> 'concurrency')::integer, 0);
    IF v_limit <= 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'stage_scheduler_capacity_vector_invalid',
            MESSAGE = 'StageScheduler CapacityObservation has no schedulable concurrency';
    END IF;

    SELECT COALESCE(jsonb_agg(candidate.value ORDER BY candidate.stage_run_id), '[]'::jsonb)
    INTO v_candidates
    FROM (
        SELECT ready.stage_run_id, jsonb_build_object(
            'stage_run_id', run.id,
            'attempt_id', run.attempt_id,
            'stage_profile_revision_id', ready.stage_profile_revision_id,
            'organization_id', ready.organization_id,
            'service_class_revision_id', ready.service_class_revision_id,
            'project_id', ready.project_id,
            'lane', CASE
                WHEN run.retry_count > 0 THEN 'RETRY'
                WHEN EXTRACT(EPOCH FROM (v_now - ready.enqueued_at)) >=
                     service_class.max_queue_wait_before_protection_seconds THEN 'PROTECTED'
                ELSE 'NORMAL'
            END,
            'enqueued_at', ready.enqueued_at,
            'attempt_fence', attempt.fence,
            'stage_fence', run.fence,
            'stage_version', run.version,
            'resource_millis', ready.resource_millis,
            'organization_deficit_millis', organization_deficit.deficit_millis,
            'service_class_deficit_millis', service_deficit.deficit_millis,
            'project_deficit_millis', project_deficit.deficit_millis,
            'score', jsonb_build_object(
                'locality_credit_millis', 0,
                'transfer_penalty_millis', 0,
                'load_penalty_millis', 0,
                'predicted_finish_millis', ready.resource_millis * (v_active + 1),
                'critical_path_credit_millis', 0,
                'age_credit_millis', LEAST(
                    service_class.max_aging_credit_seconds::bigint * 1000,
                    GREATEST(0, FLOOR(EXTRACT(EPOCH FROM (v_now - ready.enqueued_at)) * 1000))::bigint
                )
            ),
            'filter_reasons', CASE
                WHEN run.state <> 'READY' THEN jsonb_build_array('STAGE_NOT_READY')
                WHEN EXISTS (
                    SELECT 1 FROM public.stage_scheduler_claims AS live
                    WHERE live.stage_run_id = run.id AND live.state = 'CLAIMED'
                      AND live.claim_expires_at > v_now
                ) THEN jsonb_build_array('STAGE_NOT_READY')
                WHEN v_active >= v_limit THEN jsonb_build_array('CAPACITY_EXHAUSTED')
                ELSE '[]'::jsonb
            END
        ) AS value
        FROM public.stage_ready_queue_entries AS ready
        JOIN public.stage_runs AS run ON run.id = ready.stage_run_id
        JOIN public.attempts AS attempt ON attempt.id = run.attempt_id
        JOIN public.jobs AS job ON job.id = attempt.job_id
        JOIN public.service_class_revisions AS service_class
          ON service_class.id = ready.service_class_revision_id
        JOIN public.stage_scheduler_organization_deficits AS organization_deficit
          ON organization_deficit.capacity_pool_id = ready.capacity_pool_id
         AND organization_deficit.organization_id = ready.organization_id
        JOIN public.stage_scheduler_service_class_deficits AS service_deficit
          ON service_deficit.capacity_pool_id = ready.capacity_pool_id
         AND service_deficit.organization_id = ready.organization_id
         AND service_deficit.service_class_revision_id = ready.service_class_revision_id
        JOIN public.stage_scheduler_project_deficits AS project_deficit
          ON project_deficit.capacity_pool_id = ready.capacity_pool_id
         AND project_deficit.organization_id = ready.organization_id
         AND project_deficit.service_class_revision_id = ready.service_class_revision_id
         AND project_deficit.project_id = ready.project_id
        WHERE ready.capacity_pool_id = v_pool.id
          AND attempt.execution_authority_kind = 'STAGE_GRAPH'
          AND attempt.graph_state IN ('QUEUED', 'RUNNING')
          AND attempt.fence = job.current_fence
    ) AS candidate;

    v_snapshot_id := gen_random_uuid();
    v_snapshot := jsonb_build_object(
        'algorithm_revision', 'stage-filter-fairness-score-pick-v1',
        'evaluated_at', v_now,
        'valid_until', v_observation.expires_at,
        'capacity_pool_id', v_pool.id,
        'worker_instance_id', v_worker.id,
        'worker_instance_epoch', v_worker.instance_epoch,
        'observation_sequence', v_observation.observation_sequence,
        'candidates', v_candidates
    );
    INSERT INTO public.stage_scheduler_snapshot_traces (
        id, algorithm_revision, evaluated_at, valid_until, capacity_pool_id,
        worker_instance_id, worker_instance_epoch, observation_sequence, snapshot
    ) VALUES (
        v_snapshot_id, 'stage-filter-fairness-score-pick-v1', v_now,
        v_observation.expires_at, v_pool.id, v_worker.id, v_worker.instance_epoch,
        v_observation.observation_sequence, v_snapshot
    );
    RETURN QUERY SELECT v_snapshot_id, v_snapshot;
END
$$;
REVOKE ALL ON FUNCTION vela_capture_stage_scheduler_snapshot(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_capture_stage_scheduler_snapshot(jsonb) OWNER TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_capture_stage_scheduler_snapshot(jsonb) TO vela_stage_scheduler;

CREATE FUNCTION vela_claim_stage_scheduler_decision(p_claim jsonb)
RETURNS TABLE (claim_id uuid, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_claim_id uuid := (p_claim ->> 'claim_id')::uuid;
    v_decision_id uuid := (p_claim ->> 'decision_id')::uuid;
    v_snapshot_id uuid := (p_claim ->> 'captured_snapshot_id')::uuid;
    v_stage_run_id uuid := (p_claim #>> '{evidence,selected_stage_run_id}')::uuid;
    v_attempt_id uuid := (p_claim #>> '{evidence,selected_attempt_id}')::uuid;
    v_profile_id uuid := (p_claim #>> '{evidence,selected_stage_profile_revision_id}')::uuid;
    v_existing public.stage_scheduler_claims%ROWTYPE;
    v_trace public.stage_scheduler_snapshot_traces%ROWTYPE;
    v_run public.stage_runs%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_claim IS NULL OR jsonb_typeof(p_claim) <> 'object'
       OR (p_claim ->> 'schema_version')::integer <> 1
       OR jsonb_typeof(p_claim -> 'evidence') <> 'object'
       OR jsonb_typeof(p_claim -> 'command') <> 'object' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'StageScheduler claim is invalid';
    END IF;
    SELECT claim.* INTO v_existing
    FROM public.stage_scheduler_claims AS claim WHERE claim.id = v_claim_id;
    IF FOUND THEN
        IF v_existing.decision_id <> v_decision_id
           OR v_existing.stage_run_id <> v_stage_run_id THEN
            RAISE EXCEPTION USING
                ERRCODE = '23505', CONSTRAINT = 'stage_scheduler_claim_replay_mismatch',
                MESSAGE = 'StageScheduler claim identity was reused with different authority';
        END IF;
        RETURN QUERY SELECT v_existing.id, true;
        RETURN;
    END IF;

    SELECT trace.* INTO v_trace
    FROM public.stage_scheduler_snapshot_traces AS trace
    WHERE trace.id = v_snapshot_id FOR SHARE;
    IF NOT FOUND OR v_trace.valid_until <= v_now
       OR v_trace.capacity_pool_id <> (p_claim #>> '{evidence,capacity_pool_id}')::uuid
       OR v_trace.worker_instance_id <> (p_claim #>> '{evidence,worker_instance_id}')::uuid
       OR v_trace.algorithm_revision <> p_claim #>> '{evidence,algorithm_revision}' THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_scheduler_snapshot_stale',
            MESSAGE = 'StageScheduler snapshot changed before claim';
    END IF;
    PERFORM 1
    FROM public.stage_ready_queue_entries AS ready
    WHERE ready.stage_run_id = v_stage_run_id
      AND ready.capacity_pool_id = v_trace.capacity_pool_id
      AND ready.stage_profile_revision_id = v_profile_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_scheduler_candidate_stale',
            MESSAGE = 'StageScheduler candidate changed before claim';
    END IF;
    SELECT run.* INTO v_run
    FROM public.stage_runs AS run
    WHERE run.id = v_stage_run_id AND run.attempt_id = v_attempt_id;
    IF NOT FOUND OR v_run.state <> 'READY'
       OR v_run.fence <> (p_claim #>> '{evidence,stage_fence}')::bigint
       OR v_run.version <> (p_claim #>> '{evidence,stage_version}')::bigint
       OR NOT v_profile_id = ANY(v_run.allowed_stage_profile_revision_ids)
       OR NOT EXISTS (
           SELECT 1 FROM public.attempts AS attempt
           JOIN public.jobs AS job ON job.id = attempt.job_id
           WHERE attempt.id = v_attempt_id
             AND attempt.fence = (p_claim #>> '{evidence,attempt_fence}')::bigint
             AND attempt.fence = job.current_fence
             AND attempt.execution_authority_kind = 'STAGE_GRAPH'
             AND attempt.graph_state IN ('QUEUED', 'RUNNING')
       )
       THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_scheduler_candidate_stale',
            MESSAGE = 'StageScheduler candidate changed before claim';
    END IF;
    IF (p_claim ->> 'claim_expires_at')::timestamptz <= v_now
       OR (p_claim ->> 'claim_expires_at')::timestamptz > v_trace.valid_until
       OR octet_length(decode(p_claim #>> '{evidence,input_digest}', 'hex')) <> 32
       OR octet_length(decode(p_claim #>> '{evidence,evidence_digest}', 'hex')) <> 32
       OR (p_claim #>> '{command,stage_run_id}')::uuid <> v_stage_run_id
       OR (p_claim #>> '{command,attempt_id}')::uuid <> v_attempt_id
       OR (p_claim #>> '{command,stage_profile_revision_id}')::uuid <> v_profile_id
       OR (p_claim #>> '{command,capacity_pool_id}')::uuid <> v_trace.capacity_pool_id
       OR (p_claim #>> '{command,worker_instance_id}')::uuid <> v_trace.worker_instance_id
       OR (p_claim #>> '{command,worker_instance_epoch}')::bigint <> v_trace.worker_instance_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'stage_scheduler_claim_authority_invalid',
            MESSAGE = 'StageScheduler claim authority is incomplete';
    END IF;

    INSERT INTO public.stage_decision_evidence (
        id, snapshot_trace_id, algorithm_revision, input_digest, evidence_digest,
        capacity_pool_id, worker_instance_id, stage_run_id, attempt_id,
        stage_profile_revision_id, organization_id, service_class_revision_id,
        project_id, attempt_fence, stage_fence, stage_version,
        lane, resource_millis, organization_deficit_millis,
        service_class_deficit_millis, project_deficit_millis,
        filter_reason_counts, score_terms, score_total_millis, tie_break
    ) VALUES (
        v_decision_id, v_snapshot_id, p_claim #>> '{evidence,algorithm_revision}',
        decode(p_claim #>> '{evidence,input_digest}', 'hex'),
        decode(p_claim #>> '{evidence,evidence_digest}', 'hex'),
        v_trace.capacity_pool_id, v_trace.worker_instance_id, v_stage_run_id, v_attempt_id,
        v_profile_id, (p_claim #>> '{evidence,organization_id}')::uuid,
        (p_claim #>> '{evidence,service_class_revision_id}')::uuid,
        (p_claim #>> '{evidence,project_id}')::uuid,
        (p_claim #>> '{evidence,attempt_fence}')::bigint,
        (p_claim #>> '{evidence,stage_fence}')::bigint,
        (p_claim #>> '{evidence,stage_version}')::bigint,
        (p_claim #>> '{evidence,lane}')::public.scheduler_lane,
        (p_claim #>> '{evidence,resource_millis}')::bigint,
        (p_claim #>> '{evidence,organization_deficit_millis}')::bigint,
        (p_claim #>> '{evidence,service_class_deficit_millis}')::bigint,
        (p_claim #>> '{evidence,project_deficit_millis}')::bigint,
        p_claim #> '{evidence,filter_reason_counts}',
        p_claim #> '{evidence,score}',
        (p_claim #>> '{evidence,score_total_millis}')::bigint,
        p_claim #> '{evidence,tie_break}'
    );
    INSERT INTO public.stage_scheduler_claims (
        id, decision_id, scheduler_id, capacity_pool_id, worker_instance_id,
        stage_run_id, stage_attempt_id, stage_allocation_id, stage_lease_id,
        command_payload, claimed_at, claim_expires_at
    ) VALUES (
        v_claim_id, v_decision_id, p_claim ->> 'scheduler_id',
        v_trace.capacity_pool_id, v_trace.worker_instance_id, v_stage_run_id,
        (p_claim #>> '{command,stage_attempt_id}')::uuid,
        (p_claim #>> '{command,stage_allocation_id}')::uuid,
        (p_claim #>> '{command,stage_lease_id}')::uuid,
        p_claim -> 'command', v_now, (p_claim ->> 'claim_expires_at')::timestamptz
    );
    RETURN QUERY SELECT v_claim_id, false;
END
$$;
REVOKE ALL ON FUNCTION vela_claim_stage_scheduler_decision(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_claim_stage_scheduler_decision(jsonb) OWNER TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_claim_stage_scheduler_decision(jsonb) TO vela_stage_scheduler;

CREATE FUNCTION vela_account_stage_scheduler_fairness(p_claim_id uuid, p_selected_at timestamptz)
RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_claim public.stage_scheduler_claims%ROWTYPE;
    v_decision public.stage_decision_evidence%ROWTYPE;
    v_organization_id uuid;
    v_service_class_id uuid;
    v_project_id uuid;
BEGIN
    SELECT claim.* INTO STRICT v_claim
    FROM public.stage_scheduler_claims AS claim WHERE claim.id = p_claim_id FOR UPDATE;
    IF v_claim.fairness_accounted THEN
        RETURN;
    END IF;
    SELECT decision.* INTO STRICT v_decision
    FROM public.stage_decision_evidence AS decision WHERE decision.id = v_claim.decision_id;
    v_organization_id := v_decision.organization_id;
    v_service_class_id := v_decision.service_class_revision_id;
    v_project_id := v_decision.project_id;

    UPDATE public.stage_scheduler_organization_deficits AS deficit
    SET deficit_millis = LEAST(604800000, deficit.deficit_millis + v_decision.resource_millis),
        version = deficit.version + 1, updated_at = p_selected_at
    WHERE deficit.capacity_pool_id = v_claim.capacity_pool_id
      AND EXISTS (
          SELECT 1 FROM public.stage_ready_queue_entries AS ready
          WHERE ready.capacity_pool_id = deficit.capacity_pool_id
            AND ready.organization_id = deficit.organization_id
      );
    UPDATE public.stage_scheduler_organization_deficits
    SET deficit_millis = deficit_millis - v_decision.resource_millis,
        version = version + 1, last_selected_at = p_selected_at, updated_at = p_selected_at
    WHERE capacity_pool_id = v_claim.capacity_pool_id
      AND organization_id = v_organization_id;

    UPDATE public.stage_scheduler_service_class_deficits AS deficit
    SET deficit_millis = LEAST(
            604800000,
            deficit.deficit_millis + v_decision.resource_millis * service_class.queue_weight
        ),
        version = deficit.version + 1, updated_at = p_selected_at
    FROM public.service_class_revisions AS service_class
    WHERE deficit.capacity_pool_id = v_claim.capacity_pool_id
      AND deficit.organization_id = v_organization_id
      AND service_class.id = deficit.service_class_revision_id
      AND EXISTS (
          SELECT 1 FROM public.stage_ready_queue_entries AS ready
          WHERE ready.capacity_pool_id = deficit.capacity_pool_id
            AND ready.organization_id = deficit.organization_id
            AND ready.service_class_revision_id = deficit.service_class_revision_id
      );
    UPDATE public.stage_scheduler_service_class_deficits
    SET deficit_millis = deficit_millis - v_decision.resource_millis,
        version = version + 1, last_selected_at = p_selected_at, updated_at = p_selected_at
    WHERE capacity_pool_id = v_claim.capacity_pool_id
      AND organization_id = v_organization_id
      AND service_class_revision_id = v_service_class_id;

    UPDATE public.stage_scheduler_project_deficits AS deficit
    SET deficit_millis = LEAST(604800000, deficit.deficit_millis + v_decision.resource_millis),
        version = deficit.version + 1, updated_at = p_selected_at
    WHERE deficit.capacity_pool_id = v_claim.capacity_pool_id
      AND deficit.organization_id = v_organization_id
      AND deficit.service_class_revision_id = v_service_class_id
      AND EXISTS (
          SELECT 1 FROM public.stage_ready_queue_entries AS ready
          WHERE ready.capacity_pool_id = deficit.capacity_pool_id
            AND ready.organization_id = deficit.organization_id
            AND ready.service_class_revision_id = deficit.service_class_revision_id
            AND ready.project_id = deficit.project_id
      );
    UPDATE public.stage_scheduler_project_deficits
    SET deficit_millis = deficit_millis - v_decision.resource_millis,
        version = version + 1, last_selected_at = p_selected_at, updated_at = p_selected_at
    WHERE capacity_pool_id = v_claim.capacity_pool_id
      AND organization_id = v_organization_id
      AND service_class_revision_id = v_service_class_id
      AND project_id = v_project_id;
END
$$;
REVOKE ALL ON FUNCTION vela_account_stage_scheduler_fairness(uuid,timestamptz) FROM PUBLIC;
ALTER FUNCTION vela_account_stage_scheduler_fairness(uuid,timestamptz)
    OWNER TO vela_stage_scheduler_owner;

CREATE FUNCTION vela_commit_stage_scheduler_claim(p_claim_id uuid, p_stage_attempt_id uuid)
RETURNS TABLE (claim_id uuid, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_claim public.stage_scheduler_claims%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    SELECT claim.* INTO v_claim
    FROM public.stage_scheduler_claims AS claim WHERE claim.id = p_claim_id FOR UPDATE;
    IF NOT FOUND OR v_claim.stage_attempt_id <> p_stage_attempt_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_scheduler_claim_commit_mismatch',
            MESSAGE = 'StageScheduler claim commit identity is stale';
    END IF;
    IF v_claim.state = 'COMMITTED' THEN
        RETURN QUERY SELECT v_claim.id, true;
        RETURN;
    END IF;
    IF v_claim.state <> 'CLAIMED' OR NOT EXISTS (
        SELECT 1 FROM public.stage_attempts AS attempt
        WHERE attempt.id = p_stage_attempt_id
          AND attempt.stage_run_id = v_claim.stage_run_id
          AND attempt.state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED', 'SUCCEEDED')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_scheduler_claim_commit_stale',
            MESSAGE = 'StageScheduler claim cannot commit without the exact StageAttempt';
    END IF;
    PERFORM public.vela_account_stage_scheduler_fairness(v_claim.id, v_now);
    UPDATE public.stage_scheduler_claims
    SET state = 'COMMITTED', committed_at = v_now, fairness_accounted = true,
        updated_at = v_now
    WHERE id = v_claim.id;
    PERFORM public.vela_refresh_stage_capacity_pool_counter(v_claim.capacity_pool_id);
    RETURN QUERY SELECT v_claim.id, false;
END
$$;
REVOKE ALL ON FUNCTION vela_commit_stage_scheduler_claim(uuid,uuid) FROM PUBLIC;
ALTER FUNCTION vela_commit_stage_scheduler_claim(uuid,uuid) OWNER TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_commit_stage_scheduler_claim(uuid,uuid) TO vela_stage_scheduler;

CREATE FUNCTION vela_abandon_stage_scheduler_claim(p_claim_id uuid, p_reason text)
RETURNS TABLE (claim_id uuid, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_claim public.stage_scheduler_claims%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_reason NOT IN ('ASSIGNMENT_REJECTED', 'SNAPSHOT_STALE', 'SHUTDOWN') THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'StageScheduler abandon reason is unsupported';
    END IF;
    SELECT claim.* INTO v_claim
    FROM public.stage_scheduler_claims AS claim WHERE claim.id = p_claim_id FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = '23503', MESSAGE = 'StageScheduler claim does not exist';
    END IF;
    IF v_claim.state IN ('ABANDONED', 'EXPIRED') THEN
        RETURN QUERY SELECT v_claim.id, true;
        RETURN;
    END IF;
    IF v_claim.state <> 'CLAIMED' OR EXISTS (
        SELECT 1 FROM public.stage_attempts AS attempt
        WHERE attempt.id = v_claim.stage_attempt_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '40001', CONSTRAINT = 'stage_scheduler_claim_abandon_stale',
            MESSAGE = 'StageScheduler claim cannot abandon committed physical authority';
    END IF;
    UPDATE public.stage_scheduler_claims
    SET state = 'ABANDONED', abandoned_at = v_now, abandon_reason = p_reason,
        updated_at = v_now
    WHERE id = v_claim.id;
    PERFORM public.vela_refresh_stage_capacity_pool_counter(v_claim.capacity_pool_id);
    RETURN QUERY SELECT v_claim.id, false;
END
$$;
REVOKE ALL ON FUNCTION vela_abandon_stage_scheduler_claim(uuid,text) FROM PUBLIC;
ALTER FUNCTION vela_abandon_stage_scheduler_claim(uuid,text) OWNER TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_abandon_stage_scheduler_claim(uuid,text) TO vela_stage_scheduler;

CREATE FUNCTION vela_reconcile_expired_stage_scheduler_claims(p_limit integer)
RETURNS TABLE (processed bigint)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_claim public.stage_scheduler_claims%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_processed bigint := 0;
BEGIN
    IF p_limit < 1 OR p_limit > 1000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'StageScheduler reconcile limit is invalid';
    END IF;
    FOR v_claim IN
        SELECT claim.*
        FROM public.stage_scheduler_claims AS claim
        WHERE claim.state = 'CLAIMED' AND claim.claim_expires_at <= v_now
        ORDER BY claim.claim_expires_at, claim.id
        FOR UPDATE SKIP LOCKED
        LIMIT p_limit
    LOOP
        IF EXISTS (
            SELECT 1 FROM public.stage_attempts AS attempt
            WHERE attempt.id = v_claim.stage_attempt_id
              AND attempt.stage_run_id = v_claim.stage_run_id
        ) THEN
            PERFORM public.vela_account_stage_scheduler_fairness(v_claim.id, v_now);
            UPDATE public.stage_scheduler_claims
            SET state = 'COMMITTED', committed_at = v_now, fairness_accounted = true,
                updated_at = v_now
            WHERE id = v_claim.id;
        ELSE
            UPDATE public.stage_scheduler_claims
            SET state = 'EXPIRED', abandoned_at = v_now, abandon_reason = 'CLAIM_EXPIRED',
                updated_at = v_now
            WHERE id = v_claim.id;
        END IF;
        PERFORM public.vela_refresh_stage_capacity_pool_counter(v_claim.capacity_pool_id);
        v_processed := v_processed + 1;
    END LOOP;
    RETURN QUERY SELECT v_processed;
END
$$;
REVOKE ALL ON FUNCTION vela_reconcile_expired_stage_scheduler_claims(integer) FROM PUBLIC;
ALTER FUNCTION vela_reconcile_expired_stage_scheduler_claims(integer)
    OWNER TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_reconcile_expired_stage_scheduler_claims(integer)
    TO vela_stage_scheduler;

CREATE FUNCTION vela_list_stage_scheduler_shadow_snapshots(p_limit integer)
RETURNS TABLE (
    snapshot_id uuid,
    snapshot jsonb,
    expected_evidence_digest bytea
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_limit < 1 OR p_limit > 1000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'StageScheduler shadow limit is invalid';
    END IF;
    RETURN QUERY
    SELECT trace.id, trace.snapshot, decision.evidence_digest
    FROM public.stage_scheduler_snapshot_traces AS trace
    JOIN public.stage_decision_evidence AS decision ON decision.snapshot_trace_id = trace.id
    WHERE NOT EXISTS (
        SELECT 1 FROM public.stage_scheduler_shadow_replay_receipts AS receipt
        WHERE receipt.snapshot_trace_id = trace.id
          AND receipt.algorithm_revision = trace.algorithm_revision
    )
    ORDER BY trace.created_at, trace.id
    LIMIT p_limit;
END
$$;
REVOKE ALL ON FUNCTION vela_list_stage_scheduler_shadow_snapshots(integer) FROM PUBLIC;
ALTER FUNCTION vela_list_stage_scheduler_shadow_snapshots(integer)
    OWNER TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_list_stage_scheduler_shadow_snapshots(integer)
    TO vela_stage_scheduler;

CREATE FUNCTION vela_record_stage_scheduler_shadow_replay(p_receipt jsonb)
RETURNS TABLE (receipt_id uuid, replayed boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_id uuid := (p_receipt ->> 'receipt_id')::uuid;
    v_snapshot_id uuid := (p_receipt ->> 'snapshot_id')::uuid;
    v_existing public.stage_scheduler_shadow_replay_receipts%ROWTYPE;
BEGIN
    SELECT receipt.* INTO v_existing
    FROM public.stage_scheduler_shadow_replay_receipts AS receipt
    WHERE receipt.id = v_id;
    IF FOUND THEN
        IF v_existing.snapshot_trace_id <> v_snapshot_id THEN
            RAISE EXCEPTION USING ERRCODE = '23505', MESSAGE = 'Shadow receipt identity was reused';
        END IF;
        RETURN QUERY SELECT v_existing.id, true;
        RETURN;
    END IF;
    INSERT INTO public.stage_scheduler_shadow_replay_receipts (
        id, snapshot_trace_id, algorithm_revision, expected_evidence_digest,
        replayed_evidence_digest, matched, replayed_at, replayed_by
    ) VALUES (
        v_id, v_snapshot_id, p_receipt ->> 'algorithm_revision',
        decode(p_receipt ->> 'expected_evidence_digest', 'hex'),
        decode(p_receipt ->> 'replayed_evidence_digest', 'hex'),
        (p_receipt ->> 'matched')::boolean,
        (p_receipt ->> 'replayed_at')::timestamptz,
        p_receipt ->> 'replayed_by'
    );
    RETURN QUERY SELECT v_id, false;
END
$$;
REVOKE ALL ON FUNCTION vela_record_stage_scheduler_shadow_replay(jsonb) FROM PUBLIC;
ALTER FUNCTION vela_record_stage_scheduler_shadow_replay(jsonb)
    OWNER TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_record_stage_scheduler_shadow_replay(jsonb)
    TO vela_stage_scheduler;

ALTER TABLE stage_ready_queue_entries ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_ready_queue_entries FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_capacity_pool_counters ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_capacity_pool_counters FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_organization_deficits ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_organization_deficits FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_service_class_deficits ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_service_class_deficits FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_project_deficits ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_project_deficits FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_snapshot_traces ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_snapshot_traces FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_decision_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_decision_evidence FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_claims ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_claims FORCE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_shadow_replay_receipts ENABLE ROW LEVEL SECURITY;
ALTER TABLE stage_scheduler_shadow_replay_receipts FORCE ROW LEVEL SECURITY;

GRANT USAGE ON SCHEMA public TO vela_stage_scheduler, vela_stage_scheduler_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON
    stage_ready_queue_entries, stage_capacity_pool_counters,
    stage_scheduler_organization_deficits, stage_scheduler_service_class_deficits,
    stage_scheduler_project_deficits, stage_scheduler_snapshot_traces,
    stage_decision_evidence, stage_scheduler_claims,
    stage_scheduler_shadow_replay_receipts
TO vela_stage_scheduler_owner;
GRANT SELECT ON
    capacity_pools, worker_instances, model_residencies, capacity_observations,
    stage_runs, attempts, jobs, stage_profile_revisions, service_class_revisions,
    stage_allocations, stage_attempts, projects, customer_organizations
TO vela_stage_scheduler_owner;
GRANT EXECUTE ON FUNCTION vela_worker_instance_authority_matches(
    uuid,bigint,bytea,bytea,uuid,bigint
) TO vela_stage_scheduler_owner;

INSERT INTO stage_capacity_pool_counters (capacity_pool_id)
SELECT id FROM capacity_pools
ON CONFLICT DO NOTHING;
SELECT vela_refresh_stage_ready_queue(id) FROM stage_runs WHERE state = 'READY';
SELECT vela_refresh_stage_capacity_pool_counter(id) FROM capacity_pools;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_scheduler_snapshot_traces)
       OR EXISTS (SELECT 1 FROM stage_decision_evidence)
       OR EXISTS (SELECT 1 FROM stage_scheduler_claims)
       OR EXISTS (SELECT 1 FROM stage_scheduler_shadow_replay_receipts) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_scheduler_rollback_is_unsafe',
            MESSAGE = 'StageScheduler evidence must be empty before rollback';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_record_stage_scheduler_shadow_replay(jsonb)
    FROM vela_stage_scheduler;
DROP FUNCTION vela_record_stage_scheduler_shadow_replay(jsonb);
REVOKE EXECUTE ON FUNCTION vela_list_stage_scheduler_shadow_snapshots(integer)
    FROM vela_stage_scheduler;
DROP FUNCTION vela_list_stage_scheduler_shadow_snapshots(integer);
REVOKE EXECUTE ON FUNCTION vela_reconcile_expired_stage_scheduler_claims(integer)
    FROM vela_stage_scheduler;
DROP FUNCTION vela_reconcile_expired_stage_scheduler_claims(integer);
REVOKE EXECUTE ON FUNCTION vela_abandon_stage_scheduler_claim(uuid,text)
    FROM vela_stage_scheduler;
DROP FUNCTION vela_abandon_stage_scheduler_claim(uuid,text);
REVOKE EXECUTE ON FUNCTION vela_commit_stage_scheduler_claim(uuid,uuid)
    FROM vela_stage_scheduler;
DROP FUNCTION vela_commit_stage_scheduler_claim(uuid,uuid);
DROP FUNCTION vela_account_stage_scheduler_fairness(uuid,timestamptz);
REVOKE EXECUTE ON FUNCTION vela_claim_stage_scheduler_decision(jsonb)
    FROM vela_stage_scheduler;
DROP FUNCTION vela_claim_stage_scheduler_decision(jsonb);
REVOKE EXECUTE ON FUNCTION vela_capture_stage_scheduler_snapshot(jsonb)
    FROM vela_stage_scheduler;
DROP FUNCTION vela_capture_stage_scheduler_snapshot(jsonb);

DROP TRIGGER stage_scheduler_claims_capacity_counter ON stage_scheduler_claims;
DROP TRIGGER stage_allocations_capacity_counter ON stage_allocations;
DROP FUNCTION vela_stage_capacity_counter_trigger();
DROP TRIGGER capacity_pools_ready_queue_projection ON capacity_pools;
DROP FUNCTION vela_stage_ready_queue_pool_trigger();
DROP TRIGGER stage_runs_ready_queue_projection ON stage_runs;
DROP FUNCTION vela_stage_ready_queue_run_trigger();
DROP FUNCTION vela_refresh_stage_ready_queue(uuid);
DROP FUNCTION vela_refresh_stage_capacity_pool_counter(uuid);

DROP TRIGGER stage_scheduler_shadow_replay_receipts_immutable
    ON stage_scheduler_shadow_replay_receipts;
DROP TRIGGER stage_decision_evidence_immutable ON stage_decision_evidence;
DROP TRIGGER stage_scheduler_snapshot_traces_immutable ON stage_scheduler_snapshot_traces;
DROP FUNCTION vela_reject_stage_scheduler_evidence_mutation();

REVOKE SELECT ON
    capacity_pools, worker_instances, model_residencies, capacity_observations,
    stage_runs, attempts, jobs, stage_profile_revisions, service_class_revisions,
    stage_allocations, stage_attempts, projects, customer_organizations
FROM vela_stage_scheduler_owner;
REVOKE USAGE ON SCHEMA public FROM vela_stage_scheduler, vela_stage_scheduler_owner;
REVOKE EXECUTE ON FUNCTION vela_worker_instance_authority_matches(
    uuid,bigint,bytea,bytea,uuid,bigint
) FROM vela_stage_scheduler_owner;

DROP TABLE stage_scheduler_shadow_replay_receipts;
DROP TABLE stage_scheduler_claims;
DROP TABLE stage_decision_evidence;
DROP TABLE stage_scheduler_snapshot_traces;
DROP TABLE stage_scheduler_project_deficits;
DROP TABLE stage_scheduler_service_class_deficits;
DROP TABLE stage_scheduler_organization_deficits;
DROP TABLE stage_capacity_pool_counters;
DROP TABLE stage_ready_queue_entries;
DROP TYPE stage_scheduler_claim_state;
-- +goose StatementEnd
