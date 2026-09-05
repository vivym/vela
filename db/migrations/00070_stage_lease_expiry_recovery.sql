-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_stage_lease_effective_expires_at(p_stage_lease_id uuid)
RETURNS timestamptz
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT GREATEST(lease.expires_at, (
        SELECT max(renewal.expires_at)
        FROM public.stage_authority_renewals AS renewal
        WHERE renewal.stage_lease_id = lease.id
    ))
    FROM public.stage_leases AS lease WHERE lease.id = p_stage_lease_id
$$;
ALTER FUNCTION vela_stage_lease_effective_expires_at(uuid)
    OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_stage_lease_effective_expires_at(uuid) FROM PUBLIC;

CREATE INDEX stage_authority_renewals_effective_expiry_idx
    ON stage_authority_renewals(stage_lease_id, expires_at DESC);

CREATE FUNCTION vela_private.vela_insert_stage_authority_expired_event(
    p_job_id uuid, p_attempt_id uuid, p_failure_class text, p_occurred_at timestamptz
) RETURNS void
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_job public.jobs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_payload bytea;
BEGIN
    SELECT * INTO STRICT v_job FROM public.jobs WHERE id = p_job_id;
    SELECT * INTO STRICT v_attempt FROM public.attempts WHERE id = p_attempt_id;
    v_payload :=
        vela_private.vela_proto_string(1, v_job.organization_id::text)
        || vela_private.vela_proto_string(2, v_job.project_id::text)
        || vela_private.vela_proto_string(3, v_job.id::text)
        || vela_private.vela_proto_string(4, v_attempt.id::text)
        || vela_private.vela_proto_uint(5, v_attempt.attempt_number)
        || vela_private.vela_proto_uint(6, v_attempt.fence)
        || vela_private.vela_proto_uint(7, v_job.current_fence)
        || vela_private.vela_proto_string(8, p_failure_class)
        || vela_private.vela_proto_string(9, 'FAILED')
        || vela_private.vela_proto_bytes(12, vela_private.vela_proto_timestamp(p_occurred_at));
    PERFORM vela_private.vela_insert_canonical_cancellation_event(
        gen_random_uuid(), v_job.organization_id, v_job.project_id,
        v_job.id, v_job.version, 'job.failed', p_occurred_at, 24, v_payload
    );
END
$$;
ALTER FUNCTION vela_private.vela_insert_stage_authority_expired_event(
    uuid, uuid, text, timestamptz
) OWNER TO vela_internal;
REVOKE ALL ON FUNCTION vela_private.vela_insert_stage_authority_expired_event(
    uuid, uuid, text, timestamptz
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_private.vela_insert_stage_authority_expired_event(
    uuid, uuid, text, timestamptz
) TO vela_attempt_coordinator_owner;

CREATE FUNCTION vela_recover_expired_stage_authority(p_stage_run_id uuid)
RETURNS TABLE (
    attempt_id uuid, stage_run_id uuid, stage_state text,
    stage_fence bigint, stage_version bigint, reason text
)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_job public.jobs%ROWTYPE;
    v_attempt public.attempts%ROWTYPE;
    v_run public.stage_runs%ROWTYPE;
    v_physical public.stage_attempts%ROWTYPE;
    v_lease public.stage_leases%ROWTYPE;
    v_materialization public.stage_materialization_leases%ROWTYPE;
    v_stage_budget public.stage_retry_budgets%ROWTYPE;
    v_budget public.attempt_retry_budgets%ROWTYPE;
    v_credit public.credit_reservations%ROWTYPE;
    v_job_id uuid;
    v_now timestamptz := statement_timestamp();
    v_expires_at timestamptz;
    v_compute_end timestamptz;
    v_units bigint;
    v_failure_class text;
    v_retry_allowed boolean;
    v_billable boolean;
    v_rows bigint;
    v_released bigint;
BEGIN
    SELECT attempt.job_id INTO v_job_id
    FROM public.stage_runs AS run
    JOIN public.attempts AS attempt ON attempt.id = run.attempt_id
    WHERE run.id = p_stage_run_id;
    IF NOT FOUND THEN RETURN; END IF;

    -- Renewal, completion, cancellation and assignment serialize on this Job.
    -- NOWAIT makes recovery yield to an in-flight authority transaction.
    SELECT * INTO v_job FROM public.jobs WHERE id = v_job_id FOR UPDATE NOWAIT;
    SELECT attempt.* INTO v_attempt FROM public.attempts AS attempt
    JOIN public.stage_runs AS run ON run.attempt_id = attempt.id
    WHERE run.id = p_stage_run_id FOR UPDATE OF attempt NOWAIT;
    PERFORM run.id FROM public.stage_runs AS run
    WHERE run.attempt_id = v_attempt.id ORDER BY run.id FOR UPDATE NOWAIT;
    PERFORM physical.id FROM public.stage_attempts AS physical
    WHERE physical.attempt_id = v_attempt.id ORDER BY physical.id FOR UPDATE NOWAIT;
    PERFORM lease.id FROM public.stage_leases AS lease
    WHERE lease.attempt_id = v_attempt.id ORDER BY lease.id FOR UPDATE NOWAIT;
    PERFORM allocation.id FROM public.stage_allocations AS allocation
    WHERE allocation.attempt_id = v_attempt.id ORDER BY allocation.id FOR UPDATE NOWAIT;
    PERFORM materialization.id FROM public.stage_materialization_leases AS materialization
    WHERE materialization.attempt_id = v_attempt.id
    ORDER BY materialization.id FOR UPDATE NOWAIT;
    SELECT * INTO v_run FROM public.stage_runs WHERE id = p_stage_run_id;

    IF v_attempt.graph_state IN ('FAILED', 'CANCELED') THEN
        UPDATE public.stage_leases AS lease
        SET state = 'REVOKED', revoked_at = v_now, revoke_reason = 'GRAPH_TERMINAL'
        WHERE lease.stage_run_id = v_run.id AND lease.state = 'ACTIVE';
        UPDATE public.stage_allocations AS allocation
        SET state = 'RELEASED', released_at = v_now, release_reason = 'TERMINAL_AUTHORITY_EXPIRED'
        WHERE allocation.stage_run_id = v_run.id AND allocation.state = 'ALLOCATED'
          AND EXISTS (
              SELECT 1 FROM public.stage_leases AS lease
              WHERE lease.stage_allocation_id = allocation.id
                AND lease.revoke_reason IS DISTINCT FROM 'CUSTOMER_CANCELLATION'
                AND public.vela_stage_lease_effective_expires_at(lease.id) <= v_now
          );
        GET DIAGNOSTICS v_rows = ROW_COUNT;
        UPDATE public.stage_materialization_leases AS materialization
        SET state = 'EXPIRED', revoked_at = v_now, revoke_reason = 'GRAPH_TERMINAL'
        WHERE materialization.stage_run_id = v_run.id AND materialization.state = 'ACTIVE'
          AND materialization.expires_at <= v_now;
        GET DIAGNOSTICS v_released = ROW_COUNT;
        v_rows := v_rows + v_released;
        IF v_rows = 0 THEN RETURN; END IF;
        UPDATE public.stage_attempts AS physical
        SET state = 'CANCELED', ended_at = v_now, updated_at = v_now
        WHERE physical.stage_run_id = v_run.id
          AND physical.state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED');
        RETURN QUERY SELECT v_attempt.id, v_run.id, v_run.state::text,
            v_run.fence, v_run.version, 'TERMINAL_AUTHORITY_EXPIRED'::text;
        RETURN;
    END IF;
    IF v_attempt.graph_state NOT IN ('QUEUED', 'RUNNING')
       OR v_job.state NOT IN ('QUEUED', 'RETRY_WAIT', 'RUNNING')
       OR v_attempt.fence <> v_job.current_fence
       OR v_run.state NOT IN ('ASSIGNED', 'RUNNING', 'MATERIALIZING') THEN
        RETURN;
    END IF;
    SELECT physical.* INTO v_physical FROM public.stage_attempts AS physical
    WHERE physical.stage_run_id = v_run.id
      AND physical.state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED');
    IF NOT FOUND THEN RETURN; END IF;
    SELECT lease.* INTO v_lease FROM public.stage_leases AS lease
    WHERE lease.stage_attempt_id = v_physical.id;
    IF v_run.state = 'MATERIALIZING' THEN
        SELECT materialization.* INTO v_materialization
        FROM public.stage_materialization_leases AS materialization
        WHERE materialization.stage_attempt_id = v_physical.id
          AND (materialization.state = 'ACTIVE'
              OR (materialization.state = 'EXPIRED'
                  AND materialization.revoke_reason = 'RETENTION_EXPIRED'));
        IF NOT FOUND OR v_materialization.expires_at > v_now THEN RETURN; END IF;
        v_expires_at := v_materialization.expires_at;
        v_compute_end := v_materialization.issued_at;
        v_failure_class := 'LOCAL_MATERIALIZATION_SOURCE_LOST';
    ELSE
        v_expires_at := public.vela_stage_lease_effective_expires_at(v_lease.id);
        IF v_lease.state <> 'ACTIVE' OR v_expires_at IS NULL OR v_expires_at > v_now THEN
            RETURN;
        END IF;
        v_compute_end := v_expires_at;
        v_failure_class := 'WORKER_LOST';
    END IF;
    SELECT * INTO STRICT v_budget FROM public.attempt_retry_budgets AS budget
    WHERE budget.attempt_id = v_attempt.id FOR UPDATE NOWAIT;
    SELECT * INTO STRICT v_stage_budget FROM public.stage_retry_budgets AS budget
    WHERE budget.stage_run_id = v_run.id FOR UPDATE NOWAIT;
    v_billable := v_job.billable_started_at IS NOT NULL;
    -- The execution budget is seconds. Charge the bounded interval conservatively;
    -- no physical start means zero compute consumption and no Billable Start.
    v_units := CASE WHEN v_physical.started_at IS NULL THEN 0 ELSE
        LEAST(v_budget.max_resource_units::numeric,
            GREATEST(1::numeric, ceil(extract(epoch FROM v_compute_end - v_physical.started_at))))::bigint
        END;
    v_retry_allowed := v_stage_budget.state = 'ACTIVE' AND v_budget.state = 'ACTIVE'
        AND v_stage_budget.attempts_consumed < v_stage_budget.max_attempts
        AND v_budget.consumed_resource_units::numeric + v_units < v_budget.max_resource_units
        AND v_now + interval '1 second' < v_job.job_expires_at
        AND (v_failure_class = 'LOCAL_MATERIALIZATION_SOURCE_LOST'
             OR v_failure_class = ANY(v_job.execution_retryable_failure_classes));

    UPDATE public.stage_leases AS lease
    SET state = 'EXPIRED', revoked_at = v_now, revoke_reason = 'STAGE_AUTHORITY_EXPIRED'
    WHERE lease.id = v_lease.id AND lease.state = 'ACTIVE';
    UPDATE public.stage_materialization_leases AS materialization
    SET state = 'EXPIRED', revoked_at = v_now, revoke_reason = 'MATERIALIZATION_AUTHORITY_EXPIRED'
    WHERE materialization.id = v_materialization.id AND materialization.state = 'ACTIVE';
    UPDATE public.stage_allocations AS allocation
    SET state = 'RELEASED', released_at = v_now, release_reason = 'STAGE_AUTHORITY_EXPIRED'
    WHERE allocation.id = v_lease.stage_allocation_id AND allocation.state = 'ALLOCATED';
    UPDATE public.stage_attempts AS physical
    SET state = 'LOST', ended_at = v_now, failure_class = v_failure_class,
        failure_fingerprint = sha256(convert_to(v_physical.id::text || ':' || v_expires_at::text, 'UTF8')),
        resource_totals = jsonb_build_object('resource_units', v_units,
            'source', 'AUTHORITY_EXPIRY_UPPER_BOUND'), updated_at = v_now
    WHERE physical.id = v_physical.id;
    UPDATE public.attempt_retry_budgets AS budget
    SET consumed_resource_units = LEAST(budget.max_resource_units::numeric,
            budget.consumed_resource_units::numeric + v_units)::bigint,
        state = CASE WHEN v_retry_allowed THEN 'ACTIVE'::public.stage_retry_budget_state
            WHEN budget.consumed_resource_units::numeric + v_units >= budget.max_resource_units
            THEN 'EXHAUSTED'::public.stage_retry_budget_state
            ELSE 'CANCELED'::public.stage_retry_budget_state END,
        version = budget.version + 1, updated_at = v_now
    WHERE budget.attempt_id = v_attempt.id;

    IF v_retry_allowed THEN
        UPDATE public.stage_runs AS run
        SET state = 'RETRY_WAIT', fence = run.fence + 1,
            retry_count = run.retry_count + 1, next_retry_at = v_now + interval '1 second',
            version = run.version + 1, updated_at = v_now
        WHERE run.id = v_run.id;
    ELSE
        PERFORM project.id FROM public.projects AS project
        WHERE project.id = v_job.project_id FOR UPDATE NOWAIT;
        SELECT * INTO STRICT v_credit FROM public.credit_reservations AS reservation
        WHERE reservation.job_id = v_job.id FOR UPDATE NOWAIT;
        PERFORM account.organization_id FROM public.organization_credit_accounts AS account
        WHERE account.organization_id = v_job.organization_id AND account.currency = v_credit.currency
        FOR UPDATE NOWAIT;
        UPDATE public.stage_leases AS lease
        SET state = 'REVOKED', revoked_at = v_now, revoke_reason = 'GRAPH_TERMINAL'
        WHERE lease.attempt_id = v_attempt.id AND lease.state = 'ACTIVE';
        -- Sibling Workers may still be running under a previously signed renewal.
        UPDATE public.stage_allocations AS allocation
        SET state = 'RELEASED', released_at = v_now, release_reason = 'TERMINAL_AUTHORITY_EXPIRED'
        WHERE allocation.attempt_id = v_attempt.id AND allocation.state = 'ALLOCATED'
          AND EXISTS (
              SELECT 1 FROM public.stage_leases AS lease
              WHERE lease.stage_allocation_id = allocation.id
                AND public.vela_stage_lease_effective_expires_at(lease.id) <= v_now
          );
        UPDATE public.stage_materialization_leases AS materialization
        SET state = 'REVOKED', revoked_at = v_now, revoke_reason = 'GRAPH_TERMINAL'
        WHERE materialization.attempt_id = v_attempt.id AND materialization.state = 'ACTIVE';
        UPDATE public.stage_attempts AS physical
        SET state = 'CANCELED', ended_at = v_now, updated_at = v_now
        WHERE physical.attempt_id = v_attempt.id
          AND physical.state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED');
        UPDATE public.stage_runs AS run
        SET state = CASE WHEN run.id = v_run.id THEN 'FAILED'::public.stage_run_state
                ELSE 'CANCELED'::public.stage_run_state END,
            fence = run.fence + 1, next_retry_at = NULL,
            version = run.version + 1, updated_at = v_now
        WHERE run.attempt_id = v_attempt.id AND run.state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED');
        UPDATE public.stage_retry_budgets AS budget
        SET state = CASE WHEN budget.attempts_consumed = budget.max_attempts
                THEN 'EXHAUSTED'::public.stage_retry_budget_state
                ELSE 'CANCELED'::public.stage_retry_budget_state END,
            version = budget.version + 1, updated_at = v_now
        FROM public.stage_runs AS run
        WHERE run.id = budget.stage_run_id AND run.attempt_id = v_attempt.id AND budget.state = 'ACTIVE';
        UPDATE public.stage_storage_reservations AS reservation
        SET state = 'RELEASED', updated_at = v_now
        WHERE reservation.attempt_id = v_attempt.id AND reservation.state = 'RESERVED';
        UPDATE public.attempts AS attempt
        SET state = 'FAILED', graph_state = 'FAILED', ended_at = v_now, updated_at = v_now
        WHERE attempt.id = v_attempt.id;
        UPDATE public.jobs AS job
        SET state = 'FAILED', version = job.version + 1, updated_at = v_now WHERE job.id = v_job.id;
        UPDATE public.projects AS project
        SET running_count = project.running_count - CASE WHEN v_billable THEN 1 ELSE 0 END,
            queued_count = project.queued_count - CASE WHEN v_billable THEN 0 ELSE 1 END,
            retry_wait_count = project.retry_wait_count - CASE WHEN v_job.state = 'RETRY_WAIT' THEN 1 ELSE 0 END
        WHERE project.id = v_job.project_id;
        UPDATE public.credit_reservations AS reservation
        SET state = 'RELEASED', updated_at = v_now WHERE reservation.id = v_credit.id;
        UPDATE public.organization_credit_accounts AS account
        SET reserved_minor = account.reserved_minor - v_credit.amount_minor,
            version = account.version + 1, updated_at = v_now
        WHERE account.organization_id = v_job.organization_id AND account.currency = v_credit.currency;
        PERFORM vela_private.vela_insert_stage_authority_expired_event(
            v_job.id, v_attempt.id, v_failure_class, v_now
        );
    END IF;
    RETURN QUERY SELECT run.attempt_id, run.id, run.state::text,
        run.fence, run.version, CASE WHEN v_materialization.id IS NULL
            THEN 'STAGE_AUTHORITY_EXPIRED' ELSE 'MATERIALIZATION_AUTHORITY_EXPIRED' END
    FROM public.stage_runs AS run WHERE run.id = v_run.id;
END
$$;
ALTER FUNCTION vela_recover_expired_stage_authority(uuid) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_recover_expired_stage_authority(uuid) FROM PUBLIC;

DO $$
DECLARE v_definition text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_reconcile_stage_graphs(integer)'::regprocedure);
    IF strpos(v_definition, 'AND lease.expires_at <= statement_timestamp()') = 0 THEN
        RAISE EXCEPTION 'Stage cancellation expiration dependency changed';
    END IF;
    EXECUTE replace(v_definition, 'AND lease.expires_at <= statement_timestamp()',
        'AND public.vela_stage_lease_effective_expires_at(lease.id) <= statement_timestamp()');
END
$$;
ALTER FUNCTION vela_reconcile_stage_graphs(integer) RENAME TO vela_reconcile_stage_graphs_v69;
REVOKE ALL ON FUNCTION vela_reconcile_stage_graphs_v69(integer) FROM vela_attempt_coordinator;

CREATE FUNCTION vela_reconcile_stage_graphs(p_limit integer)
RETURNS TABLE (
    attempt_id uuid, stage_run_id uuid, stage_state text,
    stage_fence bigint, stage_version bigint, reason text
)
LANGUAGE plpgsql SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_candidate record;
    v_recovered record;
    v_count integer := 0;
BEGIN
    IF p_limit IS NULL OR p_limit NOT BETWEEN 1 AND 1000 THEN
        RAISE EXCEPTION USING ERRCODE = '22023',
            CONSTRAINT = 'stage_reconcile_limit_invalid',
            MESSAGE = 'Stage graph reconcile limit must be between 1 and 1000';
    END IF;
    FOR v_candidate IN
        SELECT run.id
        FROM (
            SELECT authority.stage_run_id, min(authority.expires_at) AS expires_at
            FROM (
                SELECT lease.stage_run_id,
                    public.vela_stage_lease_effective_expires_at(lease.id) AS expires_at
                FROM public.stage_leases AS lease
                JOIN public.stage_allocations AS allocation ON allocation.id = lease.stage_allocation_id
                WHERE allocation.state = 'ALLOCATED'
                  AND lease.revoke_reason IS DISTINCT FROM 'CUSTOMER_CANCELLATION'
                UNION ALL
                SELECT materialization.stage_run_id, materialization.expires_at
                FROM public.stage_materialization_leases AS materialization
                WHERE materialization.state = 'ACTIVE'
                   OR (materialization.state = 'EXPIRED'
                       AND materialization.revoke_reason = 'RETENTION_EXPIRED'
                       AND EXISTS (SELECT 1 FROM public.stage_runs AS pending
                           JOIN public.stage_attempts AS physical ON physical.stage_run_id = pending.id
                           WHERE pending.id = materialization.stage_run_id
                             AND pending.state = 'MATERIALIZING'
                             AND physical.id = materialization.stage_attempt_id
                             AND physical.state = 'OUTPUT_SEALED'))
            ) AS authority
            WHERE authority.expires_at <= statement_timestamp()
            GROUP BY authority.stage_run_id
        ) AS candidate
        JOIN public.stage_runs AS run ON run.id = candidate.stage_run_id
        JOIN public.attempts AS attempt ON attempt.id = run.attempt_id
        JOIN public.jobs AS job ON job.id = attempt.job_id
        ORDER BY candidate.expires_at, run.id LIMIT p_limit
        FOR UPDATE OF job SKIP LOCKED
    LOOP
        BEGIN
            FOR v_recovered IN SELECT * FROM public.vela_recover_expired_stage_authority(v_candidate.id)
            LOOP
                attempt_id := v_recovered.attempt_id;
                stage_run_id := v_recovered.stage_run_id;
                stage_state := v_recovered.stage_state;
                stage_fence := v_recovered.stage_fence;
                stage_version := v_recovered.stage_version;
                reason := v_recovered.reason;
                RETURN NEXT;
                v_count := v_count + 1;
            END LOOP;
        EXCEPTION WHEN lock_not_available THEN
            CONTINUE;
        END;
    END LOOP;
    IF v_count < p_limit THEN
        RETURN QUERY SELECT * FROM public.vela_reconcile_stage_graphs_v69(p_limit - v_count);
    END IF;
END
$$;
ALTER FUNCTION vela_reconcile_stage_graphs(integer) OWNER TO vela_attempt_coordinator_owner;
REVOKE ALL ON FUNCTION vela_reconcile_stage_graphs(integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_reconcile_stage_graphs(integer) TO vela_attempt_coordinator;

DO $$
DECLARE
    v_definition text;
    v_old text := E'       OR v_committed_at >= v_materialization.expires_at\n';
    v_new text := v_old
        || E'       OR clock_timestamp() >= v_materialization.expires_at\n'
        || E'       OR clock_timestamp() >= v_reservation.expires_at\n';
BEGIN
    v_definition := pg_get_functiondef('public.vela_commit_stage_artifact(jsonb)'::regprocedure);
    IF (length(v_definition) - length(replace(v_definition, v_old, ''))) <> length(v_old) THEN
        RAISE EXCEPTION 'StageArtifact commit authority clock dependency changed';
    END IF;
    EXECUTE replace(v_definition, v_old, v_new);
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
DECLARE v_definition text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_commit_stage_artifact(jsonb)'::regprocedure);
    EXECUTE replace(v_definition,
        E'       OR clock_timestamp() >= v_materialization.expires_at\n'
            || E'       OR clock_timestamp() >= v_reservation.expires_at\n', '');
END
$$;
DROP FUNCTION vela_reconcile_stage_graphs(integer);
ALTER FUNCTION vela_reconcile_stage_graphs_v69(integer) RENAME TO vela_reconcile_stage_graphs;
GRANT EXECUTE ON FUNCTION vela_reconcile_stage_graphs(integer) TO vela_attempt_coordinator;
DO $$
DECLARE v_definition text;
BEGIN
    v_definition := pg_get_functiondef('public.vela_reconcile_stage_graphs(integer)'::regprocedure);
    EXECUTE replace(v_definition,
        'AND public.vela_stage_lease_effective_expires_at(lease.id) <= statement_timestamp()',
        'AND lease.expires_at <= statement_timestamp()');
END
$$;
DROP FUNCTION vela_recover_expired_stage_authority(uuid);
DROP FUNCTION vela_private.vela_insert_stage_authority_expired_event(uuid, uuid, text, timestamptz);
DROP FUNCTION vela_stage_lease_effective_expires_at(uuid);
DROP INDEX stage_authority_renewals_effective_expiry_idx;
-- +goose StatementEnd
