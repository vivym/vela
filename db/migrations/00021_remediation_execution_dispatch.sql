-- +goose Up
-- +goose StatementBegin
CREATE FUNCTION vela_list_executing_remediation(p_limit integer)
RETURNS TABLE (
    operation_id uuid,
    worker_id uuid,
    worker_epoch bigint,
    node_identity text,
    device_identity text,
    failure_class text,
    evidence_digest bytea,
    certification_revision text,
    action_level remediation_action_level,
    idempotency_key text,
    requested_by text,
    state remediation_operation_state,
    requested_at timestamptz,
    deadline_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    result_code text,
    result_detail text,
    postcheck_digest bytea,
    first_approver text,
    second_approver text,
    approved_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_limit IS NULL OR p_limit < 1 OR p_limit > 1000 THEN
        RAISE EXCEPTION 'invalid Remediation execution dispatch limit' USING ERRCODE = '22023';
    END IF;
    RETURN QUERY
    SELECT operation.id, operation.worker_id, operation.worker_epoch,
           operation.node_identity, operation.device_identity, operation.failure_class,
           operation.evidence_digest, operation.certification_revision, operation.action_level,
           operation.idempotency_key, operation.requested_by, operation.state,
           operation.requested_at, operation.deadline_at, operation.started_at,
           operation.finished_at, operation.result_code, operation.result_detail,
           operation.postcheck_digest, operation.first_approver, operation.second_approver,
           operation.approved_at
    FROM public.remediation_operations AS operation
    WHERE operation.state = 'EXECUTING'
    ORDER BY operation.requested_at, operation.id
    LIMIT p_limit;
END
$$;
-- +goose StatementEnd

REVOKE ALL ON FUNCTION vela_list_executing_remediation(integer) FROM PUBLIC;
ALTER FUNCTION vela_list_executing_remediation(integer) OWNER TO vela_remediation_owner;
GRANT EXECUTE ON FUNCTION vela_list_executing_remediation(integer) TO vela_remediation;

-- +goose Down
REVOKE ALL ON FUNCTION vela_list_executing_remediation(integer) FROM PUBLIC;
DROP FUNCTION IF EXISTS vela_list_executing_remediation(integer);
