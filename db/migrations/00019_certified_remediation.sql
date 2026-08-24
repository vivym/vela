-- +goose Up
-- +goose StatementBegin
ALTER TABLE workers
    ADD COLUMN node_identity text NOT NULL DEFAULT '';
UPDATE workers SET node_identity = spiffe_id WHERE node_identity = '';

CREATE FUNCTION vela_default_worker_node_identity() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.node_identity = '' THEN
        NEW.node_identity := NEW.spiffe_id;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_default_worker_node_identity() FROM PUBLIC;
ALTER FUNCTION vela_default_worker_node_identity() OWNER TO vela_internal;

CREATE TRIGGER workers_default_node_identity
BEFORE INSERT ON workers
FOR EACH ROW EXECUTE FUNCTION vela_default_worker_node_identity();

CREATE FUNCTION vela_validate_worker_node_identity() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'UPDATE'
        AND NEW.node_identity IS DISTINCT FROM OLD.node_identity
        AND NEW.epoch <= OLD.epoch
    THEN
        RAISE EXCEPTION 'Worker node identity changes require a new epoch'
            USING ERRCODE = '55000', CONSTRAINT = 'worker_node_identity_epoch_fence';
    END IF;
    IF TG_OP = 'UPDATE'
        AND OLD.lifecycle_state = 'QUARANTINED'
        AND NEW.lifecycle_state IS DISTINCT FROM OLD.lifecycle_state
        AND NEW.epoch <= OLD.epoch
    THEN
        RAISE EXCEPTION 'Quarantined Worker requires a new epoch before reuse'
            USING ERRCODE = '55000', CONSTRAINT = 'worker_quarantine_epoch_fence';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_worker_node_identity() FROM PUBLIC;
ALTER FUNCTION vela_validate_worker_node_identity() OWNER TO vela_internal;

CREATE TRIGGER workers_identity_and_quarantine_fence
BEFORE UPDATE OF node_identity, lifecycle_state, epoch ON workers
FOR EACH ROW EXECUTE FUNCTION vela_validate_worker_node_identity();

CREATE FUNCTION vela_fence_worker_leases_on_quarantine() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF NEW.lifecycle_state = 'QUARANTINED'
        AND OLD.lifecycle_state IS DISTINCT FROM NEW.lifecycle_state
    THEN
        UPDATE public.attempt_leases
        SET revoked_at = clock_timestamp(), updated_at = clock_timestamp()
        WHERE worker_id = NEW.id AND revoked_at IS NULL;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_fence_worker_leases_on_quarantine() FROM PUBLIC;
ALTER FUNCTION vela_fence_worker_leases_on_quarantine() OWNER TO vela_internal;

CREATE TRIGGER workers_quarantine_fences_leases
AFTER UPDATE OF lifecycle_state ON workers
FOR EACH ROW EXECUTE FUNCTION vela_fence_worker_leases_on_quarantine();

ALTER TABLE workers
    ADD CONSTRAINT workers_node_identity_valid
    CHECK (length(node_identity) BETWEEN 1 AND 500 AND btrim(node_identity) = node_identity);
CREATE UNIQUE INDEX workers_node_identity_unique ON workers (node_identity);

CREATE TYPE remediation_action_level AS ENUM (
    'L0_PROCESS_RESTART',
    'L1_CUDA_CLEANUP',
    'L2_GPU_RESET',
    'L3_PCIE_FLR',
    'L4_DRIVER_RELOAD',
    'L5_NODE_REBOOT',
    'L6_BMC_POWER_CYCLE',
    'L7_QUARANTINE'
);

CREATE TYPE remediation_operation_state AS ENUM (
    'REQUESTED',
    'APPROVAL_REQUIRED',
    'EXECUTING',
    'SUCCEEDED',
    'QUARANTINED'
);

CREATE TABLE remediation_operations (
    id uuid PRIMARY KEY,
    worker_id uuid NOT NULL,
    worker_epoch bigint NOT NULL CHECK (worker_epoch > 0),
    node_identity text NOT NULL
        CHECK (length(node_identity) BETWEEN 1 AND 500 AND btrim(node_identity) = node_identity),
    device_identity text NOT NULL
        CHECK (length(device_identity) BETWEEN 1 AND 500 AND btrim(device_identity) = device_identity),
    failure_class text NOT NULL
        CHECK (length(failure_class) BETWEEN 1 AND 200 AND btrim(failure_class) = failure_class),
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    certification_revision text NOT NULL
        CHECK (length(certification_revision) <= 200 AND btrim(certification_revision) = certification_revision),
    action_level remediation_action_level NOT NULL,
    idempotency_key text NOT NULL
        CHECK (length(idempotency_key) BETWEEN 1 AND 200 AND btrim(idempotency_key) = idempotency_key),
    requested_by text NOT NULL
        CHECK (length(requested_by) BETWEEN 1 AND 500 AND btrim(requested_by) = requested_by),
    state remediation_operation_state NOT NULL,
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    deadline_at timestamptz NOT NULL,
    started_at timestamptz,
    finished_at timestamptz,
    result_code text
        CHECK (result_code IS NULL OR (length(result_code) BETWEEN 1 AND 200 AND btrim(result_code) = result_code)),
    result_detail text
        CHECK (result_detail IS NULL OR (length(result_detail) BETWEEN 1 AND 2000 AND btrim(result_detail) = result_detail)),
    postcheck_digest bytea CHECK (postcheck_digest IS NULL OR octet_length(postcheck_digest) = 32),
    first_approver text
        CHECK (first_approver IS NULL OR (length(first_approver) BETWEEN 1 AND 500 AND btrim(first_approver) = first_approver)),
    second_approver text
        CHECK (second_approver IS NULL OR (length(second_approver) BETWEEN 1 AND 500 AND btrim(second_approver) = second_approver)),
    approved_at timestamptz,
    UNIQUE (worker_id, worker_epoch, idempotency_key),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch),
    CHECK (
        (state = 'REQUESTED' AND started_at IS NULL AND finished_at IS NULL)
        OR (state = 'APPROVAL_REQUIRED' AND started_at IS NULL AND finished_at IS NULL)
        OR (state = 'EXECUTING' AND started_at IS NOT NULL AND finished_at IS NULL)
        OR (state IN ('SUCCEEDED', 'QUARANTINED') AND started_at IS NOT NULL AND finished_at IS NOT NULL)
    ),
    CHECK (deadline_at > requested_at),
    CHECK (
        (action_level = 'L6_BMC_POWER_CYCLE' AND state IN ('APPROVAL_REQUIRED', 'REQUESTED', 'EXECUTING', 'SUCCEEDED', 'QUARANTINED'))
        OR action_level <> 'L6_BMC_POWER_CYCLE'
    ),
    CHECK (state <> 'APPROVAL_REQUIRED' OR action_level = 'L6_BMC_POWER_CYCLE'),
    CHECK (second_approver IS NULL OR first_approver IS NOT NULL)
);

CREATE UNIQUE INDEX remediation_operations_one_active_per_worker_epoch
    ON remediation_operations (worker_id, worker_epoch)
    WHERE state IN ('REQUESTED', 'APPROVAL_REQUIRED', 'EXECUTING');

CREATE TABLE remediation_operation_events (
    operation_id uuid NOT NULL REFERENCES remediation_operations(id),
    sequence integer NOT NULL CHECK (sequence > 0),
    from_state remediation_operation_state,
    to_state remediation_operation_state NOT NULL,
    actor_identity text NOT NULL
        CHECK (length(actor_identity) BETWEEN 1 AND 500 AND btrim(actor_identity) = actor_identity),
    result_code text
        CHECK (result_code IS NULL OR (length(result_code) BETWEEN 1 AND 200 AND btrim(result_code) = result_code)),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (operation_id, sequence)
);

ALTER TABLE remediation_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE remediation_operations FORCE ROW LEVEL SECURITY;
ALTER TABLE remediation_operation_events ENABLE ROW LEVEL SECURITY;
ALTER TABLE remediation_operation_events FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_reject_remediation_operation_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Remediation operation identity is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'remediation_operation_identity_is_immutable';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.worker_id IS DISTINCT FROM OLD.worker_id
        OR NEW.worker_epoch IS DISTINCT FROM OLD.worker_epoch
        OR NEW.node_identity IS DISTINCT FROM OLD.node_identity
        OR NEW.device_identity IS DISTINCT FROM OLD.device_identity
        OR NEW.failure_class IS DISTINCT FROM OLD.failure_class
        OR NEW.evidence_digest IS DISTINCT FROM OLD.evidence_digest
        OR NEW.certification_revision IS DISTINCT FROM OLD.certification_revision
        OR NEW.action_level IS DISTINCT FROM OLD.action_level
        OR NEW.idempotency_key IS DISTINCT FROM OLD.idempotency_key
        OR NEW.requested_by IS DISTINCT FROM OLD.requested_by
        OR NEW.requested_at IS DISTINCT FROM OLD.requested_at
        OR (NEW.deadline_at IS DISTINCT FROM OLD.deadline_at
            AND NOT (OLD.state = 'APPROVAL_REQUIRED' AND NEW.state = 'REQUESTED'))
    THEN
        RAISE EXCEPTION 'Remediation operation identity is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'remediation_operation_identity_is_immutable';
    END IF;
    IF OLD.state IN ('SUCCEEDED', 'QUARANTINED') AND NEW.state IS DISTINCT FROM OLD.state THEN
        RAISE EXCEPTION 'terminal Remediation operation state is immutable'
            USING ERRCODE = '55000', CONSTRAINT = 'remediation_operation_terminal_state_is_immutable';
    END IF;
    IF NOT (
        NEW.state = OLD.state
        OR (OLD.state = 'REQUESTED' AND NEW.state IN ('EXECUTING', 'APPROVAL_REQUIRED', 'QUARANTINED'))
        OR (OLD.state = 'APPROVAL_REQUIRED' AND NEW.state IN ('REQUESTED', 'QUARANTINED'))
        OR (OLD.state = 'EXECUTING' AND NEW.state IN ('SUCCEEDED', 'QUARANTINED'))
    ) THEN
        RAISE EXCEPTION 'invalid Remediation operation state transition from % to %', OLD.state, NEW.state
            USING ERRCODE = '23514', CONSTRAINT = 'remediation_operation_state_transition_invalid';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_remediation_operation_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_remediation_operation_mutation() OWNER TO vela_remediation_owner;

CREATE TRIGGER remediation_operations_immutable
BEFORE UPDATE OR DELETE ON remediation_operations
FOR EACH ROW EXECUTE FUNCTION vela_reject_remediation_operation_mutation();

CREATE FUNCTION vela_reject_remediation_event_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Remediation operation events are immutable'
        USING ERRCODE = '55000', CONSTRAINT = 'remediation_operation_event_is_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_remediation_event_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_remediation_event_mutation() OWNER TO vela_remediation_owner;

CREATE TRIGGER remediation_operation_events_immutable
BEFORE UPDATE OR DELETE ON remediation_operation_events
FOR EACH ROW EXECUTE FUNCTION vela_reject_remediation_event_mutation();

CREATE FUNCTION vela_remediation_event(
    p_operation_id uuid,
    p_sequence integer,
    p_from_state remediation_operation_state,
    p_to_state remediation_operation_state,
    p_actor_identity text,
    p_result_code text
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    INSERT INTO public.remediation_operation_events (
        operation_id, sequence, from_state, to_state, actor_identity, result_code
    ) VALUES (
        p_operation_id, p_sequence, p_from_state, p_to_state, p_actor_identity, p_result_code
    );
END
$$;
REVOKE ALL ON FUNCTION vela_remediation_event(uuid, integer, remediation_operation_state, remediation_operation_state, text, text) FROM PUBLIC;
ALTER FUNCTION vela_remediation_event(uuid, integer, remediation_operation_state, remediation_operation_state, text, text)
    OWNER TO vela_remediation_owner;

CREATE FUNCTION vela_request_remediation(
    p_operation_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_node_identity text,
    p_device_identity text,
    p_failure_class text,
    p_evidence_digest bytea,
    p_certification_revision text,
    p_action_level remediation_action_level,
    p_idempotency_key text,
    p_requested_by text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    action_level remediation_action_level,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition,
    requires_approval boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker public.workers%ROWTYPE;
    v_existing public.remediation_operations%ROWTYPE;
    v_state remediation_operation_state;
    v_lifecycle worker_lifecycle_state;
    v_reachability worker_reachability_condition;
    v_now timestamptz := clock_timestamp();
    v_deadline timestamptz;
BEGIN
    IF p_operation_id IS NULL OR p_worker_id IS NULL OR p_worker_epoch IS NULL OR p_worker_epoch <= 0
        OR p_node_identity IS NULL OR length(p_node_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_node_identity) <> p_node_identity
        OR p_device_identity IS NULL OR length(p_device_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_device_identity) <> p_device_identity
        OR p_failure_class IS NULL OR length(p_failure_class) NOT BETWEEN 1 AND 200
        OR btrim(p_failure_class) <> p_failure_class
        OR p_evidence_digest IS NULL OR octet_length(p_evidence_digest) <> 32
        OR p_certification_revision IS NULL OR length(p_certification_revision) > 200
        OR btrim(p_certification_revision) <> p_certification_revision
        OR p_idempotency_key IS NULL OR length(p_idempotency_key) NOT BETWEEN 1 AND 200
        OR btrim(p_idempotency_key) <> p_idempotency_key
        OR p_requested_by IS NULL OR length(p_requested_by) NOT BETWEEN 1 AND 500
        OR btrim(p_requested_by) <> p_requested_by
    THEN
        RAISE EXCEPTION 'invalid Remediation operation request' USING ERRCODE = '22023';
    END IF;
    IF p_action_level = 'L6_BMC_POWER_CYCLE' AND p_certification_revision = '' THEN
        RAISE EXCEPTION 'L6 BMC power cycle requires a certification revision' USING ERRCODE = '22023';
    END IF;
    IF p_action_level <> 'L7_QUARANTINE' AND p_certification_revision = '' THEN
        RAISE EXCEPTION 'automatic Remediation requires a certification revision' USING ERRCODE = '22023';
    END IF;

    SELECT * INTO STRICT v_worker
    FROM public.workers AS worker
    WHERE worker.id = p_worker_id
    FOR UPDATE;
    IF v_worker.epoch <> p_worker_epoch OR v_worker.node_identity <> p_node_identity THEN
        RAISE EXCEPTION 'Remediation Worker identity or epoch does not match current authority'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_worker_identity_mismatch';
    END IF;

    SELECT * INTO v_existing
    FROM public.remediation_operations AS operation
    WHERE operation.worker_id = p_worker_id
      AND operation.worker_epoch = p_worker_epoch
      AND operation.idempotency_key = p_idempotency_key
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.id IS DISTINCT FROM p_operation_id
            OR v_existing.node_identity IS DISTINCT FROM p_node_identity
            OR v_existing.device_identity IS DISTINCT FROM p_device_identity
            OR v_existing.failure_class IS DISTINCT FROM p_failure_class
            OR v_existing.evidence_digest IS DISTINCT FROM p_evidence_digest
            OR v_existing.certification_revision IS DISTINCT FROM p_certification_revision
            OR v_existing.action_level IS DISTINCT FROM p_action_level
            OR v_existing.requested_by IS DISTINCT FROM p_requested_by
        THEN
            RAISE EXCEPTION 'Remediation idempotency key conflicts with committed input'
                USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_idempotency_conflict';
        END IF;
        RETURN QUERY SELECT
            v_existing.id, true, v_existing.state, v_existing.action_level,
            v_worker.lifecycle_state, v_worker.reachability_condition,
            v_existing.state = 'APPROVAL_REQUIRED';
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM public.remediation_operations AS operation
        WHERE operation.worker_id = p_worker_id
          AND operation.worker_epoch = p_worker_epoch
          AND operation.state IN ('REQUESTED', 'APPROVAL_REQUIRED', 'EXECUTING')
    ) THEN
        RAISE EXCEPTION 'Worker already has an active Remediation operation'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_worker_operation_active';
    END IF;

    IF v_worker.lifecycle_state = 'QUARANTINED' THEN
        RAISE EXCEPTION 'Quarantined Worker requires a new epoch before remediation'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_worker_quarantined';
    END IF;

    IF p_action_level = 'L7_QUARANTINE' THEN
        v_state := 'QUARANTINED';
        v_lifecycle := 'QUARANTINED';
        v_reachability := 'OFFLINE';
        v_deadline := v_now + interval '15 minutes';
    ELSIF p_action_level = 'L6_BMC_POWER_CYCLE' THEN
        v_state := 'APPROVAL_REQUIRED';
        v_lifecycle := 'RECOVERING';
        v_reachability := 'OFFLINE';
        v_deadline := v_now + interval '5 minutes';
    ELSIF p_action_level IN ('L0_PROCESS_RESTART', 'L1_CUDA_CLEANUP') THEN
        v_state := 'REQUESTED';
        v_lifecycle := 'DRAINING';
        v_reachability := 'SUSPECT';
        v_deadline := v_now + interval '15 minutes';
    ELSE
        v_state := 'REQUESTED';
        v_lifecycle := 'RECOVERING';
        v_reachability := 'SUSPECT';
        v_deadline := v_now + interval '15 minutes';
    END IF;

    INSERT INTO public.remediation_operations (
        id, worker_id, worker_epoch, node_identity, device_identity, failure_class,
        evidence_digest, certification_revision, action_level, idempotency_key,
        requested_by, state, requested_at, deadline_at, started_at, finished_at, result_code
    ) VALUES (
        p_operation_id, p_worker_id, p_worker_epoch, p_node_identity, p_device_identity,
        p_failure_class, p_evidence_digest, p_certification_revision, p_action_level,
        p_idempotency_key, p_requested_by, v_state, v_now, v_deadline,
        CASE WHEN v_state = 'QUARANTINED' THEN v_now END,
        CASE WHEN v_state = 'QUARANTINED' THEN v_now END,
        CASE WHEN v_state = 'QUARANTINED' THEN 'QUARANTINED_BY_POLICY' END
    );
    UPDATE public.attempt_leases
    SET revoked_at = v_now, updated_at = v_now
    WHERE worker_id = p_worker_id
      AND worker_epoch = p_worker_epoch
      AND revoked_at IS NULL;
    UPDATE public.workers
    SET lifecycle_state = v_lifecycle,
        reachability_condition = v_reachability,
        updated_at = v_now
    WHERE id = p_worker_id;
    PERFORM public.vela_remediation_event(
        p_operation_id, 1, NULL, v_state, p_requested_by,
        CASE WHEN v_state = 'QUARANTINED' THEN 'QUARANTINED_BY_POLICY' END
    );
    RETURN QUERY SELECT
        p_operation_id, false, v_state, p_action_level,
        v_lifecycle, v_reachability, v_state = 'APPROVAL_REQUIRED';
END
$$;

CREATE FUNCTION vela_approve_remediation(
    p_operation_id uuid,
    p_approver_identity text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    approval_count integer,
    requires_approval boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.workers%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_sequence integer;
    v_count integer;
    v_replayed boolean := false;
BEGIN
    IF p_operation_id IS NULL OR p_approver_identity IS NULL
        OR length(p_approver_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_approver_identity) <> p_approver_identity
    THEN
        RAISE EXCEPTION 'invalid Remediation approval' USING ERRCODE = '22023';
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
    IF v_operation.action_level <> 'L6_BMC_POWER_CYCLE' THEN
        RAISE EXCEPTION 'only L6 BMC power cycle operations require approval'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_approval_not_required';
    END IF;
    IF v_operation.state NOT IN ('APPROVAL_REQUIRED', 'REQUESTED') THEN
        IF v_operation.state IN ('EXECUTING', 'SUCCEEDED', 'QUARANTINED') THEN
            SELECT (CASE WHEN operation.first_approver IS NOT NULL THEN 1 ELSE 0 END
                + CASE WHEN operation.second_approver IS NOT NULL THEN 1 ELSE 0 END)
                INTO v_count
            FROM public.remediation_operations AS operation
            WHERE operation.id = p_operation_id;
            RETURN QUERY SELECT p_operation_id, true, v_operation.state, v_count, false;
            RETURN;
        END IF;
        RAISE EXCEPTION 'Remediation operation is not awaiting approval'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_approval_state_invalid';
    END IF;
    IF v_worker.epoch <> v_operation.worker_epoch
        OR v_worker.node_identity <> v_operation.node_identity
        OR v_worker.lifecycle_state = 'QUARANTINED'
    THEN
        RAISE EXCEPTION 'Remediation approval Worker identity or quarantine state is invalid'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_approval_identity_mismatch';
    END IF;
    IF v_operation.deadline_at <= v_now THEN
        UPDATE public.remediation_operations
        SET state = 'QUARANTINED', finished_at = v_now,
            result_code = 'REMEDIATION_DEADLINE_EXPIRED',
            result_detail = 'Remediation approval deadline expired'
        WHERE id = p_operation_id;
        UPDATE public.workers
        SET lifecycle_state = 'QUARANTINED',
            reachability_condition = 'OFFLINE', updated_at = v_now
        WHERE id = v_operation.worker_id;
        SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
        FROM public.remediation_operation_events AS event
        WHERE event.operation_id = p_operation_id;
        PERFORM public.vela_remediation_event(
            p_operation_id, v_sequence, v_operation.state, 'QUARANTINED',
            p_approver_identity, 'REMEDIATION_DEADLINE_EXPIRED'
        );
        RETURN QUERY SELECT p_operation_id, false, 'QUARANTINED'::remediation_operation_state,
            v_operation.action_level, false;
        RETURN;
    END IF;
    IF v_operation.first_approver = p_approver_identity
        OR v_operation.second_approver = p_approver_identity
    THEN
        v_replayed := true;
    ELSIF v_operation.first_approver IS NULL THEN
        UPDATE public.remediation_operations
        SET first_approver = p_approver_identity, approved_at = v_now
        WHERE id = p_operation_id;
        SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
        FROM public.remediation_operation_events AS event
        WHERE event.operation_id = p_operation_id;
        PERFORM public.vela_remediation_event(
            p_operation_id, v_sequence, v_operation.state, v_operation.state,
            p_approver_identity, 'FIRST_APPROVAL_RECORDED'
        );
    ELSIF v_operation.second_approver IS NULL THEN
        UPDATE public.remediation_operations
        SET second_approver = p_approver_identity,
            approved_at = v_now,
            state = 'REQUESTED',
            deadline_at = v_now + interval '15 minutes'
        WHERE id = p_operation_id;
        SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
        FROM public.remediation_operation_events AS event
        WHERE event.operation_id = p_operation_id;
        PERFORM public.vela_remediation_event(
            p_operation_id, v_sequence, v_operation.state, 'REQUESTED',
            p_approver_identity, 'SECOND_APPROVAL_RECORDED'
        );
    ELSE
        RAISE EXCEPTION 'Remediation operation already has two approvals'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_approval_complete';
    END IF;
    SELECT (CASE WHEN operation.first_approver IS NOT NULL THEN 1 ELSE 0 END
        + CASE WHEN operation.second_approver IS NOT NULL THEN 1 ELSE 0 END)
        INTO v_count
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    SELECT operation.state INTO v_operation.state
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id;
    RETURN QUERY SELECT p_operation_id, v_replayed, v_operation.state, v_count,
        v_operation.state = 'APPROVAL_REQUIRED';
END
$$;

CREATE FUNCTION vela_start_remediation(
    p_operation_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_actor_identity text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    action_level remediation_action_level,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.workers%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_sequence integer;
BEGIN
    IF p_operation_id IS NULL OR p_worker_id IS NULL OR p_worker_epoch IS NULL OR p_worker_epoch <= 0
        OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_actor_identity) <> p_actor_identity
    THEN
        RAISE EXCEPTION 'invalid Remediation start' USING ERRCODE = '22023';
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
    IF v_operation.worker_id <> p_worker_id OR v_operation.worker_epoch <> p_worker_epoch
        OR v_worker.epoch <> p_worker_epoch
        OR v_operation.node_identity <> v_worker.node_identity
        OR v_worker.lifecycle_state = 'QUARANTINED'
    THEN
        RAISE EXCEPTION 'Remediation start identity or epoch does not match current authority'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_start_identity_mismatch';
    END IF;
    IF v_operation.state IN ('REQUESTED', 'APPROVAL_REQUIRED', 'EXECUTING')
        AND v_operation.deadline_at <= v_now
    THEN
        UPDATE public.remediation_operations
        SET state = 'QUARANTINED', finished_at = v_now,
            result_code = 'REMEDIATION_DEADLINE_EXPIRED',
            result_detail = 'Remediation execution deadline expired'
        WHERE id = p_operation_id;
        UPDATE public.workers
        SET lifecycle_state = 'QUARANTINED',
            reachability_condition = 'OFFLINE', updated_at = v_now
        WHERE id = v_operation.worker_id;
        SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
        FROM public.remediation_operation_events AS event
        WHERE event.operation_id = p_operation_id;
        PERFORM public.vela_remediation_event(
            p_operation_id, v_sequence, v_operation.state, 'QUARANTINED',
            p_actor_identity, 'REMEDIATION_DEADLINE_EXPIRED'
        );
        RETURN QUERY SELECT p_operation_id, false, 'QUARANTINED'::remediation_operation_state,
            v_operation.action_level, 'QUARANTINED'::worker_lifecycle_state,
            'OFFLINE'::worker_reachability_condition;
        RETURN;
    END IF;
    IF EXISTS (
        SELECT 1 FROM public.attempt_leases AS lease
        WHERE lease.worker_id = p_worker_id
          AND lease.worker_epoch = p_worker_epoch
          AND lease.revoked_at IS NULL
    ) THEN
        RAISE EXCEPTION 'Remediation cannot execute before current Worker Lease is fenced'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_active_lease_not_fenced';
    END IF;
    IF v_operation.state = 'EXECUTING' THEN
        RETURN QUERY SELECT v_operation.id, true, v_operation.state, v_operation.action_level,
            v_worker.lifecycle_state, v_worker.reachability_condition;
        RETURN;
    END IF;
    IF v_operation.state <> 'REQUESTED' THEN
        RAISE EXCEPTION 'Remediation operation is not ready to execute'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_start_state_invalid';
    END IF;
    IF v_operation.action_level = 'L6_BMC_POWER_CYCLE'
        AND (v_operation.first_approver IS NULL OR v_operation.second_approver IS NULL
            OR v_operation.first_approver = v_operation.second_approver)
    THEN
        RAISE EXCEPTION 'L6 BMC power cycle requires two distinct approvals'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_approval_incomplete';
    END IF;
    UPDATE public.remediation_operations
    SET state = 'EXECUTING', started_at = v_now
    WHERE id = p_operation_id;
    SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
    FROM public.remediation_operation_events AS event
    WHERE event.operation_id = p_operation_id;
    PERFORM public.vela_remediation_event(
        p_operation_id, v_sequence, v_operation.state, 'EXECUTING',
        p_actor_identity, 'EXECUTION_STARTED'
    );
    RETURN QUERY SELECT v_operation.id, false, 'EXECUTING'::remediation_operation_state,
        v_operation.action_level, v_worker.lifecycle_state, v_worker.reachability_condition;
END
$$;

CREATE FUNCTION vela_complete_remediation(
    p_operation_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_success boolean,
    p_result_code text,
    p_result_detail text,
    p_postcheck_digest bytea,
    p_actor_identity text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    action_level remediation_action_level,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition,
    result_code text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.workers%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_sequence integer;
    v_final_state remediation_operation_state;
    v_lifecycle worker_lifecycle_state;
    v_reachability worker_reachability_condition;
    v_result_code text := p_result_code;
BEGIN
    IF p_operation_id IS NULL OR p_worker_id IS NULL OR p_worker_epoch IS NULL OR p_worker_epoch <= 0
        OR p_result_code IS NULL OR length(p_result_code) NOT BETWEEN 1 AND 200
        OR btrim(p_result_code) <> p_result_code
        OR p_result_detail IS NULL OR length(p_result_detail) NOT BETWEEN 1 AND 2000
        OR btrim(p_result_detail) <> p_result_detail
        OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_actor_identity) <> p_actor_identity
    THEN
        RAISE EXCEPTION 'invalid Remediation completion' USING ERRCODE = '22023';
    END IF;
    IF p_success AND (p_postcheck_digest IS NULL OR octet_length(p_postcheck_digest) <> 32) THEN
        RAISE EXCEPTION 'successful Remediation completion requires a post-check digest'
            USING ERRCODE = '22023';
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
    IF v_operation.worker_id <> p_worker_id OR v_operation.worker_epoch <> p_worker_epoch
        OR v_worker.epoch <> p_worker_epoch
        OR v_operation.node_identity <> v_worker.node_identity
    THEN
        RAISE EXCEPTION 'Remediation completion identity or epoch does not match current authority'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_completion_identity_mismatch';
    END IF;
    IF v_operation.state IN ('SUCCEEDED', 'QUARANTINED') THEN
        RETURN QUERY SELECT v_operation.id, true, v_operation.state, v_operation.action_level,
            v_worker.lifecycle_state, v_worker.reachability_condition, v_operation.result_code;
        RETURN;
    END IF;
    IF v_operation.state <> 'EXECUTING' THEN
        RAISE EXCEPTION 'Remediation operation is not executing'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_completion_state_invalid';
    END IF;

    IF v_operation.deadline_at <= v_now THEN
        p_success := false;
        v_result_code := 'REMEDIATION_DEADLINE_EXPIRED';
        p_result_detail := 'Remediation execution deadline expired';
    END IF;
    IF p_success AND v_worker.lifecycle_state = 'QUARANTINED' THEN
        p_success := false;
        v_result_code := 'WORKER_ALREADY_QUARANTINED';
        p_result_detail := 'Worker was quarantined before Remediation completion';
    END IF;

    IF p_success AND EXISTS (
        SELECT 1 FROM public.attempts AS attempt
        WHERE attempt.worker_id = p_worker_id
          AND attempt.worker_epoch = p_worker_epoch
          AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
    ) THEN
        p_success := false;
        v_result_code := 'ACTIVE_ATTEMPT_REMAINS';
        p_result_detail := 'Worker still owns an active Attempt after remediation';
    END IF;
    IF p_success AND EXISTS (
        SELECT 1 FROM public.attempt_leases AS lease
        WHERE lease.worker_id = p_worker_id
          AND lease.worker_epoch = p_worker_epoch
          AND lease.revoked_at IS NULL
    ) THEN
        p_success := false;
        v_result_code := 'ACTIVE_ATTEMPT_REMAINS';
        p_result_detail := 'Worker still owns an active Lease after remediation';
    END IF;
    IF p_success THEN
        v_final_state := 'SUCCEEDED';
        v_lifecycle := 'READY';
        v_reachability := 'HEALTHY';
    ELSE
        v_final_state := 'QUARANTINED';
        v_lifecycle := 'QUARANTINED';
        v_reachability := 'OFFLINE';
    END IF;
    UPDATE public.remediation_operations
    SET state = v_final_state,
        finished_at = v_now,
        result_code = v_result_code,
        result_detail = p_result_detail,
        postcheck_digest = p_postcheck_digest
    WHERE id = p_operation_id;
    UPDATE public.workers
    SET lifecycle_state = v_lifecycle,
        reachability_condition = v_reachability,
        updated_at = v_now
    WHERE id = p_worker_id;
    SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
    FROM public.remediation_operation_events AS event
    WHERE event.operation_id = p_operation_id;
    PERFORM public.vela_remediation_event(
        p_operation_id, v_sequence, v_operation.state, v_final_state,
        p_actor_identity, v_result_code
    );
    RETURN QUERY SELECT p_operation_id, false, v_final_state, v_operation.action_level,
        v_lifecycle, v_reachability, v_result_code;
END
$$;

CREATE FUNCTION vela_recover_remediation(
    p_operation_id uuid,
    p_actor_identity text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    state remediation_operation_state,
    action_level remediation_action_level,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition,
    result_code text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_operation public.remediation_operations%ROWTYPE;
    v_worker public.workers%ROWTYPE;
    v_now timestamptz := clock_timestamp();
    v_sequence integer;
BEGIN
    IF p_operation_id IS NULL OR p_actor_identity IS NULL
        OR length(p_actor_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_actor_identity) <> p_actor_identity
    THEN
        RAISE EXCEPTION 'invalid Remediation recovery' USING ERRCODE = '22023';
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
    IF v_operation.state IN ('SUCCEEDED', 'QUARANTINED') THEN
        RETURN QUERY SELECT p_operation_id, true, v_operation.state, v_operation.action_level,
            v_worker.lifecycle_state, v_worker.reachability_condition, v_operation.result_code;
        RETURN;
    END IF;
    IF v_worker.epoch <> v_operation.worker_epoch
        OR v_worker.node_identity <> v_operation.node_identity
    THEN
        RAISE EXCEPTION 'Remediation recovery identity does not match current authority'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_recovery_identity_mismatch';
    END IF;
    IF v_worker.lifecycle_state <> 'QUARANTINED' AND v_operation.deadline_at > v_now THEN
        RAISE EXCEPTION 'Remediation operation deadline has not expired'
            USING ERRCODE = 'P0001', CONSTRAINT = 'remediation_deadline_not_reached';
    END IF;
    UPDATE public.remediation_operations
    SET state = 'QUARANTINED', finished_at = v_now,
        result_code = 'REMEDIATION_DEADLINE_EXPIRED',
        result_detail = 'Remediation operation recovered after deadline or quarantine'
    WHERE id = p_operation_id;
    UPDATE public.workers
    SET lifecycle_state = 'QUARANTINED',
        reachability_condition = 'OFFLINE', updated_at = v_now
    WHERE id = v_operation.worker_id;
    SELECT COALESCE(max(sequence), 0) + 1 INTO v_sequence
    FROM public.remediation_operation_events AS event
    WHERE event.operation_id = p_operation_id;
    PERFORM public.vela_remediation_event(
        p_operation_id, v_sequence, v_operation.state, 'QUARANTINED',
        p_actor_identity, 'REMEDIATION_DEADLINE_EXPIRED'
    );
    RETURN QUERY SELECT p_operation_id, false, 'QUARANTINED'::remediation_operation_state,
        v_operation.action_level, 'QUARANTINED'::worker_lifecycle_state,
        'OFFLINE'::worker_reachability_condition, 'REMEDIATION_DEADLINE_EXPIRED';
END
$$;

CREATE FUNCTION vela_get_remediation_operation(p_operation_id uuid)
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
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT operation.id, operation.worker_id, operation.worker_epoch,
        operation.node_identity, operation.device_identity, operation.failure_class,
        operation.evidence_digest, operation.certification_revision, operation.action_level,
        operation.idempotency_key, operation.requested_by, operation.state,
        operation.requested_at, operation.deadline_at, operation.started_at, operation.finished_at,
        operation.result_code, operation.result_detail, operation.postcheck_digest,
        operation.first_approver, operation.second_approver, operation.approved_at
    FROM public.remediation_operations AS operation
    WHERE operation.id = p_operation_id
$$;

ALTER FUNCTION vela_request_remediation(uuid, uuid, bigint, text, text, text, bytea, text, remediation_action_level, text, text)
    OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_approve_remediation(uuid, text) OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_start_remediation(uuid, uuid, bigint, text) OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_complete_remediation(uuid, uuid, bigint, boolean, text, text, bytea, text)
    OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_recover_remediation(uuid, text) OWNER TO vela_remediation_owner;
ALTER FUNCTION vela_get_remediation_operation(uuid) OWNER TO vela_remediation_owner;

REVOKE ALL ON FUNCTION vela_request_remediation(uuid, uuid, bigint, text, text, text, bytea, text, remediation_action_level, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_approve_remediation(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_start_remediation(uuid, uuid, bigint, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_complete_remediation(uuid, uuid, bigint, boolean, text, text, bytea, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_recover_remediation(uuid, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_get_remediation_operation(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_request_remediation(uuid, uuid, bigint, text, text, text, bytea, text, remediation_action_level, text, text) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_approve_remediation(uuid, text) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_start_remediation(uuid, uuid, bigint, text) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_complete_remediation(uuid, uuid, bigint, boolean, text, text, bytea, text) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_recover_remediation(uuid, text) TO vela_remediation;
GRANT EXECUTE ON FUNCTION vela_get_remediation_operation(uuid) TO vela_remediation;
GRANT SELECT, INSERT, UPDATE, DELETE ON remediation_operations, remediation_operation_events TO vela_remediation_owner;
GRANT SELECT, UPDATE ON workers TO vela_remediation_owner;
GRANT SELECT ON worker_epochs, attempts TO vela_remediation_owner;
GRANT SELECT, UPDATE ON attempt_leases TO vela_remediation_owner;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    -- Serialize the identity-column contraction with any Worker/scheduler transaction
    -- before dropping the remediation surface or changing the Worker relation.
    LOCK TABLE public.workers IN ACCESS EXCLUSIVE MODE;
END
$$;

DROP FUNCTION IF EXISTS vela_get_remediation_operation(uuid);
DROP FUNCTION IF EXISTS vela_recover_remediation(uuid, text);
DROP FUNCTION IF EXISTS vela_complete_remediation(uuid, uuid, bigint, boolean, text, text, bytea, text);
DROP FUNCTION IF EXISTS vela_start_remediation(uuid, uuid, bigint, text);
DROP FUNCTION IF EXISTS vela_approve_remediation(uuid, text);
DROP FUNCTION IF EXISTS vela_request_remediation(uuid, uuid, bigint, text, text, text, bytea, text, remediation_action_level, text, text);
DROP FUNCTION IF EXISTS vela_remediation_event(uuid, integer, remediation_operation_state, remediation_operation_state, text, text);
DROP TRIGGER IF EXISTS remediation_operation_events_immutable ON remediation_operation_events;
DROP TRIGGER IF EXISTS remediation_operations_immutable ON remediation_operations;
DROP FUNCTION IF EXISTS vela_reject_remediation_event_mutation();
DROP FUNCTION IF EXISTS vela_reject_remediation_operation_mutation();
DROP TRIGGER IF EXISTS workers_quarantine_fences_leases ON workers;
DROP FUNCTION IF EXISTS vela_fence_worker_leases_on_quarantine();
DROP TRIGGER IF EXISTS workers_identity_and_quarantine_fence ON workers;
DROP FUNCTION IF EXISTS vela_validate_worker_node_identity();
DROP TABLE IF EXISTS remediation_operation_events;
DROP TABLE IF EXISTS remediation_operations;
DROP TYPE IF EXISTS remediation_operation_state;
DROP TYPE IF EXISTS remediation_action_level;
DROP INDEX IF EXISTS workers_node_identity_unique;
ALTER TABLE workers DROP CONSTRAINT IF EXISTS workers_node_identity_valid;
DROP TRIGGER IF EXISTS workers_default_node_identity ON workers;
DROP FUNCTION IF EXISTS vela_default_worker_node_identity();
ALTER TABLE workers DROP COLUMN IF EXISTS node_identity;
-- +goose StatementEnd
