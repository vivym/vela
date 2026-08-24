-- +goose Up
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_claim_remediation_execution(
    p_operation_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_claim_id uuid,
    p_actor_identity text
) RETURNS TABLE (
    operation_id uuid,
    claim_id uuid,
    replayed boolean,
    deadline_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.workers%ROWTYPE;
    v_claim public.remediation_execution_claims%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_operation_id IS NULL OR p_worker_id IS NULL OR p_worker_epoch IS NULL OR p_worker_epoch <= 0
        OR p_claim_id IS NULL
        OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_actor_identity) <> p_actor_identity
    THEN
        RAISE EXCEPTION 'invalid Remediation execution claim' USING ERRCODE = '22023';
    END IF;

    SELECT * INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    SELECT * INTO STRICT v_worker
    FROM public.workers AS worker
    WHERE worker.id = v_operation.worker_id
    FOR UPDATE;
    SELECT * INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;

    IF v_operation.worker_id <> p_worker_id
        OR v_operation.worker_epoch <> p_worker_epoch
        OR v_worker.epoch <> p_worker_epoch
        OR v_operation.node_identity <> v_worker.node_identity
    THEN
        RAISE EXCEPTION 'Remediation execution claim identity or epoch does not match current authority'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_claim_identity_mismatch';
    END IF;
    IF v_operation.state <> 'EXECUTING' THEN
        RAISE EXCEPTION 'Remediation operation is not executing'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_claim_state_invalid';
    END IF;
    IF v_operation.deadline_at <= v_now THEN
        RAISE EXCEPTION 'Remediation execution deadline expired'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_claim_deadline_expired';
    END IF;

    SELECT * INTO v_claim
    FROM public.remediation_execution_claims AS claim
    WHERE claim.operation_id = p_operation_id
    FOR UPDATE;
    IF FOUND THEN
        IF v_claim.claim_id <> p_claim_id
            OR v_claim.worker_id <> p_worker_id
            OR v_claim.worker_epoch <> p_worker_epoch
            OR v_claim.actor_identity <> p_actor_identity
        THEN
            RAISE EXCEPTION 'Remediation operation already has a conflicting execution claim'
                USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_execution_claim_exists';
        END IF;
        RETURN QUERY SELECT v_claim.operation_id, v_claim.claim_id, true, v_operation.deadline_at;
        RETURN;
    END IF;

    INSERT INTO public.remediation_execution_claims (
        operation_id, claim_id, worker_id, worker_epoch, actor_identity, claimed_at
    ) VALUES (
        p_operation_id, p_claim_id, p_worker_id, p_worker_epoch, p_actor_identity, v_now
    );
    RETURN QUERY SELECT p_operation_id, p_claim_id, false, v_operation.deadline_at;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_claim_remediation_execution(
    p_operation_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_claim_id uuid,
    p_actor_identity text
) RETURNS TABLE (
    operation_id uuid,
    claim_id uuid,
    replayed boolean,
    deadline_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.workers%ROWTYPE;
    v_now timestamptz := clock_timestamp();
BEGIN
    IF p_operation_id IS NULL OR p_worker_id IS NULL OR p_worker_epoch IS NULL OR p_worker_epoch <= 0
        OR p_claim_id IS NULL
        OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_actor_identity) <> p_actor_identity
    THEN
        RAISE EXCEPTION 'invalid Remediation execution claim' USING ERRCODE = '22023';
    END IF;

    SELECT * INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    SELECT * INTO STRICT v_worker
    FROM public.workers AS worker
    WHERE worker.id = v_operation.worker_id
    FOR UPDATE;
    SELECT * INTO STRICT v_operation
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;

    IF v_operation.worker_id <> p_worker_id
        OR v_operation.worker_epoch <> p_worker_epoch
        OR v_worker.epoch <> p_worker_epoch
        OR v_operation.node_identity <> v_worker.node_identity
    THEN
        RAISE EXCEPTION 'Remediation execution claim identity or epoch does not match current authority'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_claim_identity_mismatch';
    END IF;
    IF v_operation.state <> 'EXECUTING' THEN
        RAISE EXCEPTION 'Remediation operation is not executing'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_claim_state_invalid';
    END IF;
    IF v_operation.deadline_at <= v_now THEN
        RAISE EXCEPTION 'Remediation execution deadline expired'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_claim_deadline_expired';
    END IF;
    PERFORM 1
    FROM public.remediation_execution_claims AS claim
    WHERE claim.operation_id = p_operation_id
    FOR UPDATE;
    IF FOUND THEN
        RAISE EXCEPTION 'Remediation operation already has an execution claim'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_execution_claim_exists';
    END IF;

    INSERT INTO public.remediation_execution_claims (
        operation_id, claim_id, worker_id, worker_epoch, actor_identity, claimed_at
    ) VALUES (
        p_operation_id, p_claim_id, p_worker_id, p_worker_epoch, p_actor_identity, v_now
    );
    RETURN QUERY SELECT p_operation_id, p_claim_id, false, v_operation.deadline_at;
END
$$;
-- +goose StatementEnd
