-- +goose Up
CREATE TABLE remediation_execution_claims (
    operation_id uuid PRIMARY KEY REFERENCES remediation_operations(id),
    claim_id uuid NOT NULL,
    worker_id uuid NOT NULL,
    worker_epoch bigint NOT NULL CHECK (worker_epoch > 0),
    actor_identity text NOT NULL
        CHECK (length(actor_identity) BETWEEN 1 AND 500 AND btrim(actor_identity) = actor_identity),
    claimed_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

ALTER TABLE remediation_execution_claims ENABLE ROW LEVEL SECURITY;
ALTER TABLE remediation_execution_claims FORCE ROW LEVEL SECURITY;

-- +goose StatementBegin
CREATE FUNCTION vela_claim_remediation_execution(
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

REVOKE ALL ON FUNCTION vela_claim_remediation_execution(uuid, uuid, bigint, uuid, text) FROM PUBLIC;
ALTER FUNCTION vela_claim_remediation_execution(uuid, uuid, bigint, uuid, text)
    OWNER TO vela_remediation_owner;
GRANT EXECUTE ON FUNCTION vela_claim_remediation_execution(uuid, uuid, bigint, uuid, text)
    TO vela_remediation;
GRANT SELECT, INSERT, UPDATE, DELETE ON remediation_execution_claims
    TO vela_remediation_owner;

-- +goose Down
REVOKE ALL ON FUNCTION vela_claim_remediation_execution(uuid, uuid, bigint, uuid, text) FROM PUBLIC;
DROP FUNCTION IF EXISTS vela_claim_remediation_execution(uuid, uuid, bigint, uuid, text);
DROP TABLE IF EXISTS remediation_execution_claims;
