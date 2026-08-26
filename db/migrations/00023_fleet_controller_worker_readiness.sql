-- +goose Up
-- +goose StatementBegin
CREATE TYPE fleet_capacity_state AS ENUM (
    'ADMITTABLE',
    'SCRATCH_PRESSURED',
    'SCRATCH_CRITICAL',
    'STORAGE_UNAVAILABLE',
    'MULTIPLE_BLOCKERS'
);

CREATE TYPE fleet_scratch_watermark_state AS ENUM ('NORMAL', 'PRESSURED', 'CRITICAL');

CREATE TYPE fleet_readiness_state AS ENUM ('CHECKING', 'READY', 'FAILED', 'EXPIRED');
CREATE TYPE fleet_readiness_check AS ENUM (
    'IDENTITY', 'DEVICE', 'INFERENCE_BACKEND', 'MODEL_WARMUP', 'CANARY'
);
CREATE TYPE fleet_drain_state AS ENUM ('DRAINING', 'COMPLETE', 'EXPIRED');
CREATE TYPE fleet_protected_resource_kind AS ENUM ('POD', 'DAEMONSET', 'WORKER_POOL');
CREATE TYPE fleet_mutation_operation AS ENUM (
    'DELETE', 'PATCH_SELECTOR', 'PATCH_IMAGE', 'REMOVE_FINALIZER'
);

CREATE TABLE fleet_assignment_protocol_state (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    enforced boolean NOT NULL DEFAULT false,
    protocol_version integer NOT NULL DEFAULT 1 CHECK (protocol_version > 0),
    transition_receipt text CHECK (
        transition_receipt IS NULL
        OR (length(transition_receipt) BETWEEN 1 AND 1000 AND btrim(transition_receipt) <> '')
    ),
    legacy_writer_count integer CHECK (legacy_writer_count IS NULL OR legacy_writer_count >= 0),
    transitioned_at timestamptz,
    CHECK (
        (
            protocol_version = 1
            AND NOT enforced
            AND transition_receipt IS NULL
            AND legacy_writer_count IS NULL
            AND transitioned_at IS NULL
        )
        OR (
            protocol_version > 1
            AND transition_receipt IS NOT NULL
            AND legacy_writer_count IS NOT NULL
            AND transitioned_at IS NOT NULL
        )
    )
);
INSERT INTO fleet_assignment_protocol_state (singleton) VALUES (true);

CREATE TABLE fleet_assignment_protocol_transitions (
    protocol_version integer PRIMARY KEY CHECK (protocol_version > 1),
    enforced boolean NOT NULL,
    transition_receipt text NOT NULL CHECK (
        length(transition_receipt) BETWEEN 1 AND 1000
        AND btrim(transition_receipt) <> ''
    ),
    legacy_writer_count integer NOT NULL CHECK (legacy_writer_count >= 0),
    transitioned_at timestamptz NOT NULL,
    CHECK (NOT enforced OR legacy_writer_count = 0)
);

CREATE FUNCTION vela_reject_fleet_assignment_protocol_history_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'fleet_assignment_protocol_history_immutable',
        MESSAGE = 'Fleet Assignment protocol transition history is append-only';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_fleet_assignment_protocol_history_mutation() FROM PUBLIC;
CREATE TRIGGER fleet_assignment_protocol_history_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON fleet_assignment_protocol_transitions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_fleet_assignment_protocol_history_mutation();

CREATE FUNCTION vela_validate_fleet_assignment_protocol_transition() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_state public.fleet_assignment_protocol_state%ROWTYPE;
BEGIN
    SELECT state.* INTO v_state
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'fleet_assignment_protocol_state_required',
            MESSAGE = 'Fleet Assignment protocol state is missing';
    END IF;
    IF NEW.protocol_version <> v_state.protocol_version + 1
       OR NEW.transitioned_at < COALESCE(v_state.transitioned_at, '-infinity'::timestamptz)
       OR NEW.transitioned_at > clock_timestamp()
       OR (NEW.enforced AND NEW.legacy_writer_count <> 0)
       OR (
           v_state.protocol_version > 1
           AND NOT EXISTS (
               SELECT 1
               FROM public.fleet_assignment_protocol_transitions AS current_history
               WHERE current_history.protocol_version = v_state.protocol_version
                 AND current_history.enforced = v_state.enforced
                 AND current_history.transition_receipt = v_state.transition_receipt
                 AND current_history.legacy_writer_count = v_state.legacy_writer_count
                 AND current_history.transitioned_at = v_state.transitioned_at
           )
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'fleet_assignment_protocol_history_contiguous',
            MESSAGE = 'Fleet Assignment protocol transition must extend current state with zero legacy writers';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_validate_fleet_assignment_protocol_transition() FROM PUBLIC;
CREATE TRIGGER fleet_assignment_protocol_validate_transition
BEFORE INSERT ON fleet_assignment_protocol_transitions
FOR EACH ROW EXECUTE FUNCTION vela_validate_fleet_assignment_protocol_transition();

CREATE FUNCTION vela_reject_fleet_assignment_protocol_state_delete() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'fleet_assignment_protocol_state_required',
        MESSAGE = 'Fleet Assignment protocol state cannot be removed';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_fleet_assignment_protocol_state_delete() FROM PUBLIC;
CREATE TRIGGER fleet_assignment_protocol_state_required
BEFORE DELETE OR TRUNCATE ON fleet_assignment_protocol_state
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_fleet_assignment_protocol_state_delete();

CREATE FUNCTION vela_enforce_fleet_assignment_protocol_state_transition() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF pg_trigger_depth() <> 2
       OR NEW.protocol_version <> OLD.protocol_version + 1
       OR NOT EXISTS (
           SELECT 1
           FROM public.fleet_assignment_protocol_transitions AS transition
           WHERE transition.protocol_version = NEW.protocol_version
             AND transition.enforced = NEW.enforced
             AND transition.transition_receipt = NEW.transition_receipt
             AND transition.legacy_writer_count = NEW.legacy_writer_count
             AND transition.transitioned_at = NEW.transitioned_at
       )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'fleet_assignment_protocol_state_transition_required',
            MESSAGE = 'Fleet Assignment protocol state must follow immutable transition history';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_enforce_fleet_assignment_protocol_state_transition() FROM PUBLIC;
CREATE TRIGGER fleet_assignment_protocol_enforce_transition
BEFORE UPDATE ON fleet_assignment_protocol_state
FOR EACH ROW EXECUTE FUNCTION vela_enforce_fleet_assignment_protocol_state_transition();

CREATE FUNCTION vela_apply_fleet_assignment_protocol_transition() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    UPDATE public.fleet_assignment_protocol_state
    SET enforced = NEW.enforced,
        protocol_version = NEW.protocol_version,
        transition_receipt = NEW.transition_receipt,
        legacy_writer_count = NEW.legacy_writer_count,
        transitioned_at = NEW.transitioned_at
    WHERE singleton
      AND protocol_version = NEW.protocol_version - 1;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'fleet_assignment_protocol_history_contiguous',
            MESSAGE = 'Fleet Assignment protocol history cannot advance current state';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_apply_fleet_assignment_protocol_transition() FROM PUBLIC;
CREATE TRIGGER fleet_assignment_protocol_apply_transition
AFTER INSERT ON fleet_assignment_protocol_transitions
FOR EACH ROW EXECUTE FUNCTION vela_apply_fleet_assignment_protocol_transition();

CREATE FUNCTION vela_transition_fleet_assignment_protocol(
    p_enforced boolean,
    p_receipt text,
    p_legacy_writer_count integer
) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_protocol_version integer;
BEGIN
    IF p_receipt IS NULL OR length(p_receipt) NOT BETWEEN 1 AND 1000
       OR btrim(p_receipt) = '' OR p_legacy_writer_count IS NULL
       OR p_legacy_writer_count < 0
    THEN
        RAISE EXCEPTION 'Fleet Assignment protocol transition receipt is invalid'
            USING ERRCODE = '22023';
    END IF;
    IF p_enforced AND p_legacy_writer_count <> 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'fleet_assignment_protocol_legacy_writers_active',
            MESSAGE = 'Fleet Assignment protocol cannot be enforced with active legacy writers';
    END IF;

    LOCK TABLE public.attempts IN SHARE ROW EXCLUSIVE MODE;
    SELECT state.protocol_version INTO v_protocol_version
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'fleet_assignment_protocol_state_required',
            MESSAGE = 'Fleet Assignment protocol state is missing';
    END IF;
    INSERT INTO public.fleet_assignment_protocol_transitions (
        protocol_version, enforced, transition_receipt,
        legacy_writer_count, transitioned_at
    ) VALUES (
        v_protocol_version + 1, p_enforced, p_receipt,
        p_legacy_writer_count, clock_timestamp()
    );
END
$$;
REVOKE ALL ON FUNCTION vela_transition_fleet_assignment_protocol(boolean, text, integer)
    FROM PUBLIC;

CREATE FUNCTION vela_current_fleet_assignment_protocol_version() RETURNS smallint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT CASE WHEN state.enforced THEN 2::smallint ELSE 1::smallint END
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton
    FOR SHARE
$$;
REVOKE ALL ON FUNCTION vela_current_fleet_assignment_protocol_version() FROM PUBLIC;

CREATE FUNCTION vela_require_fleet_protocol_enforced() RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_enforced boolean;
BEGIN
    SELECT state.enforced INTO STRICT v_enforced
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton
    FOR SHARE;
    IF NOT v_enforced THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'fleet_protocol_not_enforced',
            MESSAGE = 'Fleet mutation and readiness protocol is disabled';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_require_fleet_protocol_enforced() FROM PUBLIC;

CREATE TABLE fleet_worker_pod_identity_bindings (
    kubernetes_uid text PRIMARY KEY CHECK (
        length(kubernetes_uid) BETWEEN 1 AND 128
        AND btrim(kubernetes_uid) = kubernetes_uid
    ),
    namespace text NOT NULL CHECK (
        length(namespace) BETWEEN 1 AND 253 AND btrim(namespace) = namespace
    ),
    name text NOT NULL CHECK (
        length(name) BETWEEN 1 AND 253 AND btrim(name) = name
    ),
    worker_id uuid NOT NULL,
    worker_pool_id uuid NOT NULL,
    worker_epoch bigint NOT NULL CHECK (worker_epoch > 0),
    node_identity text NOT NULL CHECK (
        length(node_identity) BETWEEN 1 AND 500 AND btrim(node_identity) = node_identity
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (worker_id, worker_epoch),
    FOREIGN KEY (worker_pool_id, worker_id) REFERENCES workers(worker_pool_id, id),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch)
);
CREATE INDEX fleet_worker_pod_identity_bindings_name_idx
    ON fleet_worker_pod_identity_bindings (namespace, name, created_at DESC);

CREATE FUNCTION vela_resolve_worker_identity(
    p_node_identity text,
    p_worker_pool_id uuid,
    p_kubernetes_uid text,
    p_namespace text,
    p_name text
) RETURNS TABLE (
    worker_id uuid,
    worker_pool_id uuid,
    worker_epoch bigint,
    node_identity text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_worker public.workers%ROWTYPE;
    v_binding public.fleet_worker_pod_identity_bindings%ROWTYPE;
BEGIN
    PERFORM public.vela_require_fleet_protocol_enforced();
    IF p_node_identity IS NULL OR length(p_node_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_node_identity) <> p_node_identity
        OR p_worker_pool_id IS NULL
        OR p_kubernetes_uid IS NULL OR length(p_kubernetes_uid) NOT BETWEEN 1 AND 128
        OR btrim(p_kubernetes_uid) <> p_kubernetes_uid
        OR p_namespace IS NULL OR length(p_namespace) NOT BETWEEN 1 AND 253
        OR btrim(p_namespace) <> p_namespace
        OR p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 253
        OR btrim(p_name) <> p_name
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_worker_identity_request_invalid',
            MESSAGE = 'Fleet Worker identity request is invalid';
    END IF;

    SELECT worker.* INTO v_worker
    FROM public.workers AS worker
    WHERE worker.node_identity = p_node_identity
      AND worker.worker_pool_id = p_worker_pool_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002', CONSTRAINT = 'fleet_worker_identity_not_found',
            MESSAGE = 'Fleet Worker identity does not exist for the exact Node and pool';
    END IF;

    SELECT binding.* INTO v_binding
    FROM public.fleet_worker_pod_identity_bindings AS binding
    WHERE binding.kubernetes_uid = p_kubernetes_uid;
    IF FOUND THEN
        IF v_binding.namespace <> p_namespace OR v_binding.name <> p_name
           OR v_binding.node_identity <> p_node_identity
           OR v_binding.worker_pool_id <> p_worker_pool_id
           OR v_binding.worker_id <> v_worker.id
           OR v_binding.worker_epoch <> v_worker.epoch
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_worker_pod_identity_conflict',
                MESSAGE = 'Fleet Worker Pod identity is stale or conflicting';
        END IF;
        RETURN QUERY SELECT
            v_binding.worker_id,
            v_binding.worker_pool_id,
            v_binding.worker_epoch,
            v_binding.node_identity;
        RETURN;
    END IF;

    UPDATE public.workers AS worker
    SET epoch = worker.epoch + 1,
        lifecycle_state = 'WARMING',
        reachability_condition = 'SUSPECT',
        updated_at = clock_timestamp()
    WHERE worker.id = v_worker.id
    RETURNING worker.* INTO v_worker;

    INSERT INTO public.fleet_worker_pod_identity_bindings (
        kubernetes_uid, namespace, name, worker_id, worker_pool_id,
        worker_epoch, node_identity
    ) VALUES (
        p_kubernetes_uid, p_namespace, p_name, v_worker.id,
        v_worker.worker_pool_id, v_worker.epoch, v_worker.node_identity
    )
    RETURNING * INTO v_binding;

    RETURN QUERY SELECT
        v_binding.worker_id,
        v_binding.worker_pool_id,
        v_binding.worker_epoch,
        v_binding.node_identity;
END
$$;
REVOKE ALL ON FUNCTION vela_resolve_worker_identity(text, uuid, text, text, text)
    FROM PUBLIC;

ALTER TABLE attempts
    ADD COLUMN fleet_protocol_version smallint NOT NULL DEFAULT 1
        CHECK (fleet_protocol_version IN (1, 2));

CREATE FUNCTION vela_guard_fleet_assignment_writer() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_enforced boolean;
BEGIN
    SELECT state.enforced INTO STRICT v_enforced
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton
    FOR SHARE;
    IF (v_enforced AND NEW.fleet_protocol_version <> 2)
       OR (NOT v_enforced AND NEW.fleet_protocol_version <> 1)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'attempts_fleet_assignment_protocol',
            MESSAGE = 'Assignment writer does not match the active Fleet protocol';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_guard_fleet_assignment_writer() FROM PUBLIC;
CREATE TRIGGER attempts_fleet_assignment_protocol
BEFORE INSERT ON attempts
FOR EACH ROW EXECUTE FUNCTION vela_guard_fleet_assignment_writer();

CREATE TABLE worker_pool_capacity_policies (
    worker_pool_id uuid PRIMARY KEY REFERENCES worker_pools(id),
    revision text NOT NULL CHECK (revision ~ '^[0-9a-f]{64}$'),
    worker_high_watermark_bytes bigint NOT NULL CHECK (worker_high_watermark_bytes > 0),
    worker_low_watermark_bytes bigint NOT NULL CHECK (
        worker_low_watermark_bytes >= 0
        AND worker_low_watermark_bytes < worker_high_watermark_bytes
    ),
    worker_critical_free_bytes bigint NOT NULL CHECK (worker_critical_free_bytes >= 0),
    pool_high_watermark_bytes bigint NOT NULL CHECK (pool_high_watermark_bytes > 0),
    pool_low_watermark_bytes bigint NOT NULL CHECK (
        pool_low_watermark_bytes >= 0
        AND pool_low_watermark_bytes < pool_high_watermark_bytes
    ),
    observation_max_age_seconds bigint NOT NULL CHECK (
        observation_max_age_seconds BETWEEN 10 AND 600
    ),
    configured_by text NOT NULL CHECK (
        length(configured_by) BETWEEN 1 AND 500 AND btrim(configured_by) = configured_by
    ),
    configured_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE worker_capacity_conditions (
    worker_id uuid PRIMARY KEY REFERENCES workers(id),
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    worker_epoch bigint NOT NULL CHECK (worker_epoch > 0),
    observation_sequence bigint NOT NULL CHECK (observation_sequence > 0),
    watermark_state fleet_scratch_watermark_state NOT NULL,
    total_bytes bigint NOT NULL CHECK (total_bytes > 0),
    free_bytes bigint NOT NULL CHECK (free_bytes >= 0 AND free_bytes <= total_bytes),
    high_watermark_bytes bigint NOT NULL CHECK (
        high_watermark_bytes > 0 AND high_watermark_bytes < total_bytes
    ),
    low_watermark_bytes bigint NOT NULL CHECK (
        low_watermark_bytes >= 0 AND low_watermark_bytes < high_watermark_bytes
    ),
    critical_free_bytes bigint NOT NULL CHECK (
        critical_free_bytes >= 0 AND critical_free_bytes < total_bytes
    ),
    artifact_store_reachable boolean NOT NULL,
    scratch_latched boolean NOT NULL,
    state fleet_capacity_state NOT NULL,
    assignment_allowed boolean NOT NULL,
    observed_by text NOT NULL CHECK (
        length(observed_by) BETWEEN 1 AND 500 AND btrim(observed_by) = observed_by
    ),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (worker_pool_id, worker_id),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch),
    CHECK (NOT assignment_allowed OR state = 'ADMITTABLE'),
    CHECK (
        watermark_state = CASE
            WHEN free_bytes <= critical_free_bytes THEN 'CRITICAL'::fleet_scratch_watermark_state
            WHEN total_bytes - free_bytes >= high_watermark_bytes
                THEN 'PRESSURED'::fleet_scratch_watermark_state
            ELSE 'NORMAL'::fleet_scratch_watermark_state
        END
    ),
    CHECK (watermark_state = 'NORMAL' OR scratch_latched),
    CHECK (
        state = CASE
            WHEN watermark_state = 'CRITICAL' THEN 'SCRATCH_CRITICAL'::fleet_capacity_state
            WHEN scratch_latched THEN 'SCRATCH_PRESSURED'::fleet_capacity_state
            WHEN NOT artifact_store_reachable THEN 'STORAGE_UNAVAILABLE'::fleet_capacity_state
            ELSE 'ADMITTABLE'::fleet_capacity_state
        END
    )
);

CREATE TABLE worker_pool_capacity_conditions (
    worker_pool_id uuid PRIMARY KEY REFERENCES worker_pools(id),
    total_bytes bigint NOT NULL CHECK (total_bytes > 0),
    used_bytes bigint NOT NULL CHECK (used_bytes >= 0 AND used_bytes <= total_bytes),
    high_watermark_bytes bigint NOT NULL CHECK (high_watermark_bytes > 0),
    low_watermark_bytes bigint NOT NULL CHECK (
        low_watermark_bytes >= 0 AND low_watermark_bytes < high_watermark_bytes
    ),
    observed_worker_count integer NOT NULL CHECK (observed_worker_count > 0),
    oldest_observed_at timestamptz NOT NULL,
    state fleet_capacity_state NOT NULL,
    scratch_latched boolean NOT NULL,
    storage_unavailable boolean NOT NULL,
    assignment_allowed boolean NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (NOT assignment_allowed OR state = 'ADMITTABLE'),
    CHECK (
        state = CASE
            WHEN scratch_latched AND storage_unavailable THEN 'MULTIPLE_BLOCKERS'::fleet_capacity_state
            WHEN scratch_latched THEN 'SCRATCH_PRESSURED'::fleet_capacity_state
            WHEN storage_unavailable THEN 'STORAGE_UNAVAILABLE'::fleet_capacity_state
            ELSE 'ADMITTABLE'::fleet_capacity_state
        END
    )
);

CREATE TABLE worker_readiness_cycles (
    id uuid PRIMARY KEY,
    worker_id uuid NOT NULL REFERENCES workers(id),
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    worker_epoch bigint NOT NULL CHECK (worker_epoch > 0),
    node_identity text NOT NULL CHECK (
        length(node_identity) BETWEEN 1 AND 500 AND btrim(node_identity) = node_identity
    ),
    execution_profile_revision_id uuid NOT NULL REFERENCES execution_profile_revisions(id),
    inference_backend_revision text NOT NULL CHECK (
        length(inference_backend_revision) BETWEEN 1 AND 200
        AND btrim(inference_backend_revision) = inference_backend_revision
    ),
    requested_by text NOT NULL CHECK (
        length(requested_by) BETWEEN 1 AND 500 AND btrim(requested_by) = requested_by
    ),
    state fleet_readiness_state NOT NULL,
    next_check fleet_readiness_check,
    result_code text CHECK (
        result_code IS NULL OR (
            length(result_code) BETWEEN 1 AND 200 AND btrim(result_code) = result_code
        )
    ),
    deadline_at timestamptz NOT NULL,
    started_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (worker_id, worker_epoch, id),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch),
    CHECK (deadline_at > started_at),
    CHECK (
        (state = 'CHECKING' AND next_check IS NOT NULL AND finished_at IS NULL AND result_code IS NULL)
        OR
        (state IN ('READY', 'FAILED', 'EXPIRED') AND next_check IS NULL
            AND finished_at IS NOT NULL AND result_code IS NOT NULL)
    )
);

CREATE UNIQUE INDEX worker_readiness_one_active_cycle
    ON worker_readiness_cycles (worker_id, worker_epoch)
    WHERE state = 'CHECKING';

CREATE TABLE worker_readiness_evidence (
    cycle_id uuid NOT NULL REFERENCES worker_readiness_cycles(id),
    check_kind fleet_readiness_check NOT NULL,
    passed boolean NOT NULL,
    evidence_digest bytea NOT NULL CHECK (octet_length(evidence_digest) = 32),
    observed_by text NOT NULL CHECK (
        length(observed_by) BETWEEN 1 AND 500 AND btrim(observed_by) = observed_by
    ),
    observed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (cycle_id, check_kind)
);

CREATE TABLE worker_drain_operations (
    id uuid PRIMARY KEY,
    worker_id uuid NOT NULL REFERENCES workers(id),
    worker_epoch bigint NOT NULL CHECK (worker_epoch > 0),
    reason text NOT NULL CHECK (
        length(reason) BETWEEN 1 AND 1000 AND btrim(reason) = reason
    ),
    deadline_at timestamptz NOT NULL,
    requested_by text NOT NULL CHECK (
        length(requested_by) BETWEEN 1 AND 500 AND btrim(requested_by) = requested_by
    ),
    state fleet_drain_state NOT NULL,
    result_code text CHECK (
        result_code IS NULL OR (
            length(result_code) BETWEEN 1 AND 200 AND btrim(result_code) = result_code
        )
    ),
    requested_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    finished_at timestamptz,
    finished_by text CHECK (
        finished_by IS NULL OR (
            length(finished_by) BETWEEN 1 AND 500 AND btrim(finished_by) = finished_by
        )
    ),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch),
    CHECK (deadline_at > requested_at),
    CHECK (
        (state = 'DRAINING' AND result_code IS NULL AND finished_at IS NULL AND finished_by IS NULL)
        OR
        (state IN ('COMPLETE', 'EXPIRED') AND result_code IS NOT NULL
            AND finished_at IS NOT NULL AND finished_by IS NOT NULL)
    )
);

CREATE UNIQUE INDEX worker_drain_one_active_operation
    ON worker_drain_operations (worker_id, worker_epoch)
    WHERE state = 'DRAINING';

CREATE TABLE fleet_mutation_authorizations (
    request_uid text PRIMARY KEY CHECK (
        length(request_uid) BETWEEN 1 AND 200 AND btrim(request_uid) = request_uid
    ),
    actor_identity text NOT NULL CHECK (
        length(actor_identity) BETWEEN 1 AND 500 AND btrim(actor_identity) = actor_identity
    ),
    resource_kind fleet_protected_resource_kind NOT NULL,
    operation fleet_mutation_operation NOT NULL,
    kubernetes_uid text NOT NULL CHECK (
        length(kubernetes_uid) BETWEEN 1 AND 200 AND btrim(kubernetes_uid) = kubernetes_uid
    ),
    namespace text NOT NULL CHECK (
        length(namespace) BETWEEN 1 AND 253 AND btrim(namespace) = namespace
    ),
    name text NOT NULL CHECK (
        length(name) BETWEEN 1 AND 253 AND btrim(name) = name
    ),
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    worker_id uuid REFERENCES workers(id),
    worker_epoch bigint CHECK (worker_epoch IS NULL OR worker_epoch > 0),
    drain_operation_ids uuid[] NOT NULL CHECK (cardinality(drain_operation_ids) > 0),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    authorized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (resource_kind = 'POD' AND worker_id IS NOT NULL AND worker_epoch IS NOT NULL
            AND cardinality(drain_operation_ids) = 1)
        OR
        (resource_kind IN ('DAEMONSET', 'WORKER_POOL')
            AND worker_id IS NULL AND worker_epoch IS NULL)
    ),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch)
);

CREATE TABLE fleet_retirement_completions (
    resource_kind fleet_protected_resource_kind NOT NULL,
    kubernetes_uid text NOT NULL CHECK (
        length(kubernetes_uid) BETWEEN 1 AND 200 AND btrim(kubernetes_uid) = kubernetes_uid
    ),
    namespace text NOT NULL CHECK (
        length(namespace) BETWEEN 1 AND 253 AND btrim(namespace) = namespace
    ),
    name text NOT NULL CHECK (
        length(name) BETWEEN 1 AND 253 AND btrim(name) = name
    ),
    worker_pool_id uuid NOT NULL REFERENCES worker_pools(id),
    worker_id uuid REFERENCES workers(id),
    worker_epoch bigint CHECK (worker_epoch IS NULL OR worker_epoch > 0),
    drain_operation_ids uuid[] NOT NULL CHECK (cardinality(drain_operation_ids) > 0),
    observed_by text NOT NULL CHECK (
        length(observed_by) BETWEEN 1 AND 500 AND btrim(observed_by) = observed_by
    ),
    completed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (resource_kind, kubernetes_uid),
    CHECK (
        (resource_kind = 'POD' AND worker_id IS NOT NULL AND worker_epoch IS NOT NULL
            AND cardinality(drain_operation_ids) = 1)
        OR
        (resource_kind IN ('DAEMONSET', 'WORKER_POOL')
            AND worker_id IS NULL AND worker_epoch IS NULL)
    ),
    FOREIGN KEY (worker_id, worker_epoch) REFERENCES worker_epochs(worker_id, epoch)
);

CREATE FUNCTION vela_reject_fleet_mutation_authorization_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'fleet_mutation_authorization_immutable',
        MESSAGE = 'Fleet mutation authorization receipts are append-only';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_fleet_mutation_authorization_mutation() FROM PUBLIC;
CREATE TRIGGER fleet_mutation_authorizations_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON fleet_mutation_authorizations
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_fleet_mutation_authorization_mutation();

CREATE FUNCTION vela_reject_fleet_retirement_completion_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'fleet_retirement_completion_immutable',
        MESSAGE = 'Fleet retirement completion receipts are append-only';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_fleet_retirement_completion_mutation() FROM PUBLIC;
CREATE TRIGGER fleet_retirement_completions_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON fleet_retirement_completions
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_fleet_retirement_completion_mutation();

ALTER TABLE fleet_assignment_protocol_state ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleet_assignment_protocol_state FORCE ROW LEVEL SECURITY;
ALTER TABLE fleet_assignment_protocol_transitions ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleet_assignment_protocol_transitions FORCE ROW LEVEL SECURITY;
ALTER TABLE worker_pool_capacity_policies ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_pool_capacity_policies FORCE ROW LEVEL SECURITY;
ALTER TABLE worker_capacity_conditions ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_capacity_conditions FORCE ROW LEVEL SECURITY;
ALTER TABLE worker_pool_capacity_conditions ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_pool_capacity_conditions FORCE ROW LEVEL SECURITY;
ALTER TABLE worker_readiness_cycles ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_readiness_cycles FORCE ROW LEVEL SECURITY;
ALTER TABLE worker_readiness_evidence ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_readiness_evidence FORCE ROW LEVEL SECURITY;
ALTER TABLE worker_drain_operations ENABLE ROW LEVEL SECURITY;
ALTER TABLE worker_drain_operations FORCE ROW LEVEL SECURITY;
ALTER TABLE fleet_mutation_authorizations ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleet_mutation_authorizations FORCE ROW LEVEL SECURITY;
ALTER TABLE fleet_retirement_completions ENABLE ROW LEVEL SECURITY;
ALTER TABLE fleet_retirement_completions FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_configure_worker_pool_capacity(
    p_worker_pool_id uuid,
    p_revision text,
    p_worker_high_watermark_bytes bigint,
    p_worker_low_watermark_bytes bigint,
    p_worker_critical_free_bytes bigint,
    p_pool_high_watermark_bytes bigint,
    p_pool_low_watermark_bytes bigint,
    p_observation_max_age_seconds bigint,
    p_configured_by text
) RETURNS TABLE (
    worker_pool_id uuid,
    revision text,
    replayed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_existing worker_pool_capacity_policies%ROWTYPE;
BEGIN
    IF p_worker_pool_id IS NULL OR p_revision IS NULL
        OR p_revision !~ '^[0-9a-f]{64}$'
        OR p_worker_high_watermark_bytes <= 0
        OR p_worker_low_watermark_bytes < 0
        OR p_worker_low_watermark_bytes >= p_worker_high_watermark_bytes
        OR p_worker_critical_free_bytes < 0
        OR p_pool_high_watermark_bytes <= 0
        OR p_pool_low_watermark_bytes < 0
        OR p_pool_low_watermark_bytes >= p_pool_high_watermark_bytes
        OR p_observation_max_age_seconds NOT BETWEEN 10 AND 600
        OR p_configured_by IS NULL OR length(p_configured_by) NOT BETWEEN 1 AND 500
        OR btrim(p_configured_by) <> p_configured_by
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_capacity_policy_invalid',
            MESSAGE = 'Fleet capacity policy is invalid';
    END IF;

    PERFORM 1
    FROM public.worker_pools AS worker_pool
    WHERE worker_pool.id = p_worker_pool_id
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002', CONSTRAINT = 'fleet_capacity_policy_pool_missing',
            MESSAGE = 'Worker pool does not exist';
    END IF;

    SELECT policy.* INTO v_existing
    FROM public.worker_pool_capacity_policies AS policy
    WHERE policy.worker_pool_id = p_worker_pool_id
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.revision <> p_revision
            OR v_existing.worker_high_watermark_bytes <> p_worker_high_watermark_bytes
            OR v_existing.worker_low_watermark_bytes <> p_worker_low_watermark_bytes
            OR v_existing.worker_critical_free_bytes <> p_worker_critical_free_bytes
            OR v_existing.pool_high_watermark_bytes <> p_pool_high_watermark_bytes
            OR v_existing.pool_low_watermark_bytes <> p_pool_low_watermark_bytes
            OR v_existing.observation_max_age_seconds <> p_observation_max_age_seconds
            OR v_existing.configured_by <> p_configured_by
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_capacity_policy_conflict',
                MESSAGE = 'Fleet capacity policy conflicts with the immutable WorkerPool revision';
        END IF;
        RETURN QUERY SELECT v_existing.worker_pool_id, v_existing.revision, true;
        RETURN;
    END IF;

    INSERT INTO public.worker_pool_capacity_policies (
        worker_pool_id, revision, worker_high_watermark_bytes,
        worker_low_watermark_bytes, worker_critical_free_bytes,
        pool_high_watermark_bytes, pool_low_watermark_bytes,
        observation_max_age_seconds, configured_by
    ) VALUES (
        p_worker_pool_id, p_revision, p_worker_high_watermark_bytes,
        p_worker_low_watermark_bytes, p_worker_critical_free_bytes,
        p_pool_high_watermark_bytes, p_pool_low_watermark_bytes,
        p_observation_max_age_seconds, p_configured_by
    );
    RETURN QUERY SELECT p_worker_pool_id, p_revision, false;
END
$$;

CREATE FUNCTION vela_observe_worker_capacity(
    p_worker_id uuid,
    p_worker_pool_id uuid,
    p_worker_epoch bigint,
    p_observation_sequence bigint,
    p_observed_at timestamptz,
    p_watermark_state fleet_scratch_watermark_state,
    p_total_bytes bigint,
    p_free_bytes bigint,
    p_high_watermark_bytes bigint,
    p_low_watermark_bytes bigint,
    p_critical_free_bytes bigint,
    p_artifact_store_reachable boolean,
    p_observed_by text
) RETURNS TABLE (
    worker_pool_id uuid,
    replayed boolean,
    worker_state fleet_capacity_state,
    pool_state fleet_capacity_state,
    worker_assignment_allowed boolean,
    pool_readiness_allowed boolean,
    pool_assignment_allowed boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_worker workers%ROWTYPE;
    v_policy worker_pool_capacity_policies%ROWTYPE;
    v_existing worker_capacity_conditions%ROWTYPE;
    v_worker_condition worker_capacity_conditions%ROWTYPE;
    v_pool_condition worker_pool_capacity_conditions%ROWTYPE;
    v_used_bytes bigint;
    v_scratch_latched boolean;
    v_storage_unavailable boolean;
    v_pool_scratch_latched boolean;
    v_pool_storage_unavailable boolean;
    v_pool_has_critical boolean;
    v_pool_all_fresh boolean;
    v_pool_total_bytes bigint;
    v_pool_used_bytes bigint;
    v_pool_observed_worker_count integer;
    v_pool_oldest_observed_at timestamptz;
    v_worker_state fleet_capacity_state;
    v_pool_state fleet_capacity_state;
    v_expected_watermark_state fleet_scratch_watermark_state;
    v_observation_fresh boolean;
    v_pool_assignment_allowed boolean;
    v_replayed boolean := false;
BEGIN
	v_expected_watermark_state := CASE
		WHEN p_free_bytes <= p_critical_free_bytes THEN 'CRITICAL'::fleet_scratch_watermark_state
		WHEN p_total_bytes - p_free_bytes >= p_high_watermark_bytes
			THEN 'PRESSURED'::fleet_scratch_watermark_state
		ELSE 'NORMAL'::fleet_scratch_watermark_state
	END;
    IF p_worker_id IS NULL OR p_worker_pool_id IS NULL OR p_worker_epoch <= 0
        OR p_observation_sequence <= 0 OR p_total_bytes <= 0
        OR p_observed_at IS NULL OR p_observed_at > v_now + interval '30 seconds'
        OR p_free_bytes < 0 OR p_free_bytes > p_total_bytes
        OR p_high_watermark_bytes <= 0 OR p_high_watermark_bytes >= p_total_bytes
        OR p_low_watermark_bytes < 0 OR p_low_watermark_bytes >= p_high_watermark_bytes
        OR p_critical_free_bytes < 0 OR p_critical_free_bytes >= p_total_bytes
		OR p_watermark_state IS DISTINCT FROM v_expected_watermark_state
        OR p_observed_by IS NULL OR length(p_observed_by) NOT BETWEEN 1 AND 500
        OR btrim(p_observed_by) <> p_observed_by
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'fleet_capacity_observation_invalid',
            MESSAGE = 'Fleet capacity observation is invalid';
    END IF;

    SELECT * INTO v_worker
    FROM public.workers
    WHERE id = p_worker_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'fleet_capacity_worker_missing',
            MESSAGE = 'Worker does not exist';
    END IF;
    IF v_worker.worker_pool_id <> p_worker_pool_id OR v_worker.epoch <> p_worker_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'fleet_capacity_worker_identity_conflict',
            MESSAGE = 'Worker pool or epoch does not match current authority';
    END IF;
    PERFORM 1 FROM public.worker_pools WHERE id = p_worker_pool_id FOR UPDATE;

    SELECT policy.* INTO v_policy
    FROM public.worker_pool_capacity_policies AS policy
    WHERE policy.worker_pool_id = p_worker_pool_id
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_capacity_policy_missing',
            MESSAGE = 'Worker pool capacity policy is not configured';
    END IF;
    IF p_high_watermark_bytes <> v_policy.worker_high_watermark_bytes
        OR p_low_watermark_bytes <> v_policy.worker_low_watermark_bytes
        OR p_critical_free_bytes <> v_policy.worker_critical_free_bytes
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_capacity_policy_mismatch',
            MESSAGE = 'Worker capacity thresholds do not match the immutable WorkerPool policy';
    END IF;

    SELECT * INTO v_existing
    FROM public.worker_capacity_conditions
    WHERE worker_id = p_worker_id
    FOR UPDATE;
    IF FOUND AND p_observation_sequence <= v_existing.observation_sequence THEN
        IF p_observation_sequence = v_existing.observation_sequence
            AND p_worker_pool_id = v_existing.worker_pool_id
            AND p_worker_epoch = v_existing.worker_epoch
            AND p_observed_at = v_existing.observed_at
            AND p_watermark_state = v_existing.watermark_state
            AND p_total_bytes = v_existing.total_bytes
            AND p_free_bytes = v_existing.free_bytes
            AND p_high_watermark_bytes = v_existing.high_watermark_bytes
            AND p_low_watermark_bytes = v_existing.low_watermark_bytes
            AND p_critical_free_bytes = v_existing.critical_free_bytes
            AND p_artifact_store_reachable = v_existing.artifact_store_reachable
            AND p_observed_by = v_existing.observed_by
        THEN
            v_replayed := true;
        ELSE
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'fleet_capacity_observation_conflict',
                MESSAGE = 'Capacity observation sequence is stale or conflicting';
        END IF;
    END IF;

    IF NOT v_replayed THEN
        v_observation_fresh := p_observed_at >= v_now
            - (v_policy.observation_max_age_seconds * interval '1 second');
        v_used_bytes := p_total_bytes - p_free_bytes;
        v_scratch_latched := (
            (FOUND AND v_existing.scratch_latched AND v_used_bytes > p_low_watermark_bytes)
            OR p_watermark_state IN ('PRESSURED', 'CRITICAL')
        );
        v_storage_unavailable := NOT p_artifact_store_reachable;
        v_worker_state := CASE
            WHEN p_watermark_state = 'CRITICAL' THEN 'SCRATCH_CRITICAL'::fleet_capacity_state
            WHEN v_scratch_latched THEN 'SCRATCH_PRESSURED'::fleet_capacity_state
            WHEN v_storage_unavailable THEN 'STORAGE_UNAVAILABLE'::fleet_capacity_state
            ELSE 'ADMITTABLE'::fleet_capacity_state
        END;

        INSERT INTO public.worker_capacity_conditions (
            worker_id, worker_pool_id, worker_epoch, observation_sequence,
            watermark_state,
            total_bytes, free_bytes, high_watermark_bytes, low_watermark_bytes,
            critical_free_bytes, artifact_store_reachable, scratch_latched,
            state, assignment_allowed, observed_by, observed_at, updated_at
        ) VALUES (
            p_worker_id, p_worker_pool_id, p_worker_epoch, p_observation_sequence,
            p_watermark_state,
            p_total_bytes, p_free_bytes, p_high_watermark_bytes, p_low_watermark_bytes,
            p_critical_free_bytes, p_artifact_store_reachable, v_scratch_latched,
            v_worker_state, v_worker_state = 'ADMITTABLE' AND v_observation_fresh, p_observed_by,
            p_observed_at, v_now
        )
        ON CONFLICT (worker_id) DO UPDATE SET
            worker_pool_id = EXCLUDED.worker_pool_id,
            worker_epoch = EXCLUDED.worker_epoch,
            observation_sequence = EXCLUDED.observation_sequence,
            watermark_state = EXCLUDED.watermark_state,
            total_bytes = EXCLUDED.total_bytes,
            free_bytes = EXCLUDED.free_bytes,
            high_watermark_bytes = EXCLUDED.high_watermark_bytes,
            low_watermark_bytes = EXCLUDED.low_watermark_bytes,
            critical_free_bytes = EXCLUDED.critical_free_bytes,
            artifact_store_reachable = EXCLUDED.artifact_store_reachable,
            scratch_latched = EXCLUDED.scratch_latched,
            state = EXCLUDED.state,
            assignment_allowed = EXCLUDED.assignment_allowed,
            observed_by = EXCLUDED.observed_by,
            observed_at = EXCLUDED.observed_at,
            updated_at = EXCLUDED.updated_at;
    END IF;

    SELECT condition.* INTO v_pool_condition
    FROM public.worker_pool_capacity_conditions AS condition
    WHERE condition.worker_pool_id = p_worker_pool_id
    FOR UPDATE;
    v_pool_scratch_latched := COALESCE(v_pool_condition.scratch_latched, false);

    SELECT
        sum(condition.total_bytes)::bigint,
        sum(condition.total_bytes - condition.free_bytes)::bigint,
        count(*)::integer,
        min(condition.observed_at),
        COALESCE(bool_or(NOT condition.artifact_store_reachable), false),
        COALESCE(bool_or(condition.state = 'SCRATCH_CRITICAL'), false),
        COALESCE(bool_and(CASE
            WHEN worker.lifecycle_state IN ('READY', 'BUSY')
                AND worker.reachability_condition <> 'OFFLINE'
            THEN condition.observed_at >= v_now
                    - (v_policy.observation_max_age_seconds * interval '1 second')
                AND condition.observed_at <= v_now + interval '30 seconds'
            ELSE true
        END), false)
    INTO v_pool_total_bytes, v_pool_used_bytes, v_pool_observed_worker_count,
        v_pool_oldest_observed_at, v_pool_storage_unavailable,
        v_pool_has_critical, v_pool_all_fresh
    FROM public.worker_capacity_conditions AS condition
    JOIN public.workers AS worker ON worker.id = condition.worker_id
    WHERE worker.worker_pool_id = p_worker_pool_id
      AND condition.worker_epoch = worker.epoch;

    v_pool_scratch_latched := (
        (v_pool_scratch_latched AND v_pool_used_bytes > v_policy.pool_low_watermark_bytes)
        OR v_pool_used_bytes >= v_policy.pool_high_watermark_bytes
        OR v_pool_has_critical
    );

    v_pool_state := CASE
        WHEN v_pool_scratch_latched AND v_pool_storage_unavailable THEN 'MULTIPLE_BLOCKERS'::fleet_capacity_state
        WHEN v_pool_scratch_latched THEN 'SCRATCH_PRESSURED'::fleet_capacity_state
        WHEN v_pool_storage_unavailable THEN 'STORAGE_UNAVAILABLE'::fleet_capacity_state
        ELSE 'ADMITTABLE'::fleet_capacity_state
    END;
    v_pool_assignment_allowed := v_pool_state = 'ADMITTABLE' AND v_pool_all_fresh;
    INSERT INTO public.worker_pool_capacity_conditions (
        worker_pool_id, total_bytes, used_bytes, high_watermark_bytes,
        low_watermark_bytes, observed_worker_count, oldest_observed_at,
        state, scratch_latched, storage_unavailable, assignment_allowed, updated_at
    ) VALUES (
        p_worker_pool_id, v_pool_total_bytes, v_pool_used_bytes,
        v_policy.pool_high_watermark_bytes, v_policy.pool_low_watermark_bytes,
        v_pool_observed_worker_count, v_pool_oldest_observed_at,
        v_pool_state, v_pool_scratch_latched, v_pool_storage_unavailable,
        v_pool_assignment_allowed, v_now
    )
    ON CONFLICT ON CONSTRAINT worker_pool_capacity_conditions_pkey DO UPDATE SET
        total_bytes = EXCLUDED.total_bytes,
        used_bytes = EXCLUDED.used_bytes,
        high_watermark_bytes = EXCLUDED.high_watermark_bytes,
        low_watermark_bytes = EXCLUDED.low_watermark_bytes,
        observed_worker_count = EXCLUDED.observed_worker_count,
        oldest_observed_at = EXCLUDED.oldest_observed_at,
        state = EXCLUDED.state,
        scratch_latched = EXCLUDED.scratch_latched,
        storage_unavailable = EXCLUDED.storage_unavailable,
        assignment_allowed = EXCLUDED.assignment_allowed,
        updated_at = EXCLUDED.updated_at;

    SELECT * INTO STRICT v_worker_condition
    FROM public.worker_capacity_conditions
    WHERE worker_id = p_worker_id;
    SELECT * INTO STRICT v_pool_condition
    FROM public.worker_pool_capacity_conditions AS condition
    WHERE condition.worker_pool_id = p_worker_pool_id;

    RETURN QUERY SELECT
        p_worker_pool_id,
        v_replayed,
        v_worker_condition.state,
        v_pool_condition.state,
		public.vela_worker_capacity_allows_assignment(p_worker_id, p_worker_epoch),
		public.vela_pool_capacity_allows_readiness(p_worker_pool_id),
		public.vela_pool_capacity_allows_assignment(p_worker_pool_id);
END
$$;

CREATE FUNCTION vela_get_worker_pool_capacity(
    p_worker_pool_id uuid
) RETURNS TABLE (
    worker_pool_id uuid,
    pool_state fleet_capacity_state,
    pool_assignment_allowed boolean
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_worker_pool_id IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Worker pool id is required';
    END IF;
    RETURN QUERY
	SELECT condition.worker_pool_id, condition.state,
		public.vela_pool_capacity_allows_assignment(condition.worker_pool_id)
    FROM public.worker_pool_capacity_conditions AS condition
    WHERE condition.worker_pool_id = p_worker_pool_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = 'P0002',
            CONSTRAINT = 'fleet_pool_capacity_missing',
            MESSAGE = 'Worker pool capacity condition does not exist';
    END IF;
END
$$;

CREATE FUNCTION vela_begin_worker_readiness(
    p_cycle_id uuid,
    p_worker_id uuid,
    p_worker_pool_id uuid,
    p_worker_epoch bigint,
    p_node_identity text,
    p_execution_profile_revision_id uuid,
    p_inference_backend_revision text,
    p_requested_by text,
    p_deadline_at timestamptz
) RETURNS TABLE (
    cycle_id uuid,
    replayed boolean,
    readiness_state fleet_readiness_state,
    next_check fleet_readiness_check,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_worker workers%ROWTYPE;
    v_existing worker_readiness_cycles%ROWTYPE;
BEGIN
    PERFORM public.vela_require_fleet_protocol_enforced();
    IF p_cycle_id IS NULL OR p_worker_id IS NULL OR p_worker_pool_id IS NULL
        OR p_worker_epoch <= 0 OR p_execution_profile_revision_id IS NULL
        OR p_node_identity IS NULL OR length(p_node_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_node_identity) <> p_node_identity
        OR p_inference_backend_revision IS NULL
        OR length(p_inference_backend_revision) NOT BETWEEN 1 AND 200
        OR btrim(p_inference_backend_revision) <> p_inference_backend_revision
        OR p_requested_by IS NULL OR length(p_requested_by) NOT BETWEEN 1 AND 500
        OR btrim(p_requested_by) <> p_requested_by
		OR p_deadline_at IS NULL
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_readiness_request_invalid',
            MESSAGE = 'Worker readiness request is invalid';
    END IF;

    SELECT * INTO v_worker
    FROM public.workers
    WHERE id = p_worker_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker does not exist';
    END IF;

    SELECT * INTO v_existing
    FROM public.worker_readiness_cycles AS cycle
    WHERE cycle.id = p_cycle_id
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.worker_id <> p_worker_id
            OR v_existing.worker_pool_id <> p_worker_pool_id
            OR v_existing.worker_epoch <> p_worker_epoch
            OR v_existing.node_identity <> p_node_identity
            OR v_existing.execution_profile_revision_id <> p_execution_profile_revision_id
            OR v_existing.inference_backend_revision <> p_inference_backend_revision
            OR v_existing.requested_by <> p_requested_by
            OR v_existing.deadline_at <> p_deadline_at
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_cycle_conflict',
                MESSAGE = 'Worker readiness cycle identity conflicts with existing evidence';
        END IF;
        RETURN QUERY SELECT
            v_existing.id, true, v_existing.state, v_existing.next_check,
            v_worker.lifecycle_state, v_worker.reachability_condition;
		RETURN;
	END IF;
	IF p_deadline_at <= v_now OR p_deadline_at > v_now + interval '2 hours' THEN
		RAISE EXCEPTION USING
			ERRCODE = '22023', CONSTRAINT = 'fleet_readiness_request_invalid',
			MESSAGE = 'Worker readiness request is invalid';
	END IF;

	IF v_worker.worker_pool_id <> p_worker_pool_id
        OR v_worker.epoch <> p_worker_epoch
        OR v_worker.node_identity <> p_node_identity
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_worker_identity_conflict',
            MESSAGE = 'Worker readiness identity is stale or conflicting';
    END IF;
    IF v_worker.lifecycle_state = 'QUARANTINED' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_quarantine_epoch_fence',
            MESSAGE = 'Quarantined Worker requires a higher epoch before readiness';
    END IF;
    IF v_worker.lifecycle_state = 'BUSY'
        OR EXISTS (
            SELECT 1 FROM public.attempts AS attempt
            WHERE attempt.worker_id = p_worker_id
              AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
        )
        OR EXISTS (
            SELECT 1 FROM public.attempt_leases AS lease
            WHERE lease.worker_id = p_worker_id
              AND lease.revoked_at IS NULL
              AND lease.expires_at > v_now
        )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_active_execution',
            MESSAGE = 'Worker readiness cannot begin with active execution authority';
    END IF;
    PERFORM 1
    FROM public.execution_profile_revisions AS profile
    WHERE profile.id = p_execution_profile_revision_id
      AND profile.worker_pool_id = p_worker_pool_id
      AND profile.state IN ('CANARY', 'ACTIVE')
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_profile_conflict',
            MESSAGE = 'Execution Profile is not eligible for readiness';
    END IF;

    DELETE FROM public.worker_profile_readiness
    WHERE worker_id = p_worker_id AND worker_epoch = p_worker_epoch;
    UPDATE public.workers
    SET lifecycle_state = 'WARMING', reachability_condition = 'SUSPECT', updated_at = v_now
    WHERE id = p_worker_id AND epoch = p_worker_epoch;
    INSERT INTO public.worker_readiness_cycles (
        id, worker_id, worker_pool_id, worker_epoch, node_identity,
        execution_profile_revision_id, inference_backend_revision, requested_by,
        state, next_check, deadline_at, started_at, updated_at
    ) VALUES (
        p_cycle_id, p_worker_id, p_worker_pool_id, p_worker_epoch, p_node_identity,
        p_execution_profile_revision_id, p_inference_backend_revision, p_requested_by,
        'CHECKING', 'IDENTITY', p_deadline_at, v_now, v_now
    );

    RETURN QUERY SELECT
        p_cycle_id, false, 'CHECKING'::fleet_readiness_state,
        'IDENTITY'::fleet_readiness_check, 'WARMING'::worker_lifecycle_state,
        'SUSPECT'::worker_reachability_condition;
END
$$;

CREATE FUNCTION vela_report_worker_readiness(
    p_cycle_id uuid,
    p_check_kind fleet_readiness_check,
    p_passed boolean,
    p_evidence_digest bytea,
    p_observed_by text
) RETURNS TABLE (
    cycle_id uuid,
    replayed boolean,
    readiness_state fleet_readiness_state,
    next_check fleet_readiness_check,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_identity record;
    v_cycle worker_readiness_cycles%ROWTYPE;
    v_worker workers%ROWTYPE;
    v_existing worker_readiness_evidence%ROWTYPE;
    v_next fleet_readiness_check;
BEGIN
    PERFORM public.vela_require_fleet_protocol_enforced();
    IF p_cycle_id IS NULL OR p_check_kind IS NULL
        OR p_evidence_digest IS NULL OR octet_length(p_evidence_digest) <> 32
        OR p_observed_by IS NULL OR length(p_observed_by) NOT BETWEEN 1 AND 500
        OR btrim(p_observed_by) <> p_observed_by
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_readiness_evidence_invalid',
            MESSAGE = 'Worker readiness evidence is invalid';
    END IF;

    SELECT cycle.worker_id INTO v_identity
    FROM public.worker_readiness_cycles AS cycle
    WHERE cycle.id = p_cycle_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker readiness cycle does not exist';
    END IF;
    SELECT * INTO v_worker
    FROM public.workers
    WHERE id = v_identity.worker_id
    FOR UPDATE;
    SELECT * INTO STRICT v_cycle
    FROM public.worker_readiness_cycles AS cycle
    WHERE cycle.id = p_cycle_id
    FOR UPDATE;

    SELECT * INTO v_existing
    FROM public.worker_readiness_evidence AS evidence
    WHERE evidence.cycle_id = p_cycle_id AND evidence.check_kind = p_check_kind
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.passed IS DISTINCT FROM p_passed
            OR v_existing.evidence_digest IS DISTINCT FROM p_evidence_digest
            OR v_existing.observed_by <> p_observed_by
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_evidence_conflict',
                MESSAGE = 'Worker readiness evidence conflicts with existing report';
        END IF;
        RETURN QUERY SELECT
            v_cycle.id, true, v_cycle.state, v_cycle.next_check,
            v_worker.lifecycle_state, v_worker.reachability_condition;
        RETURN;
    END IF;

    IF v_cycle.state <> 'CHECKING' OR v_cycle.next_check <> p_check_kind THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_check_order',
            MESSAGE = 'Worker readiness check is out of order or terminal';
    END IF;
    IF v_cycle.deadline_at <= v_now THEN
        INSERT INTO public.worker_readiness_evidence (
            cycle_id, check_kind, passed, evidence_digest, observed_by, observed_at
        ) VALUES (
            p_cycle_id, p_check_kind, p_passed, p_evidence_digest, p_observed_by, v_now
        );
        UPDATE public.worker_readiness_cycles
        SET state = 'EXPIRED', next_check = NULL, result_code = 'READINESS_DEADLINE_EXPIRED',
            finished_at = v_now, updated_at = v_now
        WHERE id = p_cycle_id;
        UPDATE public.workers
        SET lifecycle_state = 'DRAINING', reachability_condition = 'SUSPECT', updated_at = v_now
        WHERE id = v_cycle.worker_id AND epoch = v_cycle.worker_epoch;
        RETURN QUERY SELECT
            v_cycle.id, false, 'EXPIRED'::fleet_readiness_state,
            NULL::fleet_readiness_check, 'DRAINING'::worker_lifecycle_state,
            'SUSPECT'::worker_reachability_condition;
        RETURN;
    END IF;
    IF v_worker.worker_pool_id <> v_cycle.worker_pool_id
        OR v_worker.epoch <> v_cycle.worker_epoch
        OR v_worker.node_identity <> v_cycle.node_identity
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_worker_changed',
            MESSAGE = 'Worker identity changed during readiness';
    END IF;
    IF v_worker.lifecycle_state = 'BUSY'
        OR EXISTS (
            SELECT 1 FROM public.attempts AS attempt
            WHERE attempt.worker_id = v_cycle.worker_id
              AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
        )
        OR EXISTS (
            SELECT 1 FROM public.attempt_leases AS lease
            WHERE lease.worker_id = v_cycle.worker_id
              AND lease.revoked_at IS NULL
              AND lease.expires_at > v_now
        )
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_active_execution',
            MESSAGE = 'Worker readiness cannot advance with active execution authority';
    END IF;
    IF NOT public.vela_worker_capacity_allows_assignment(v_cycle.worker_id, v_cycle.worker_epoch)
        OR NOT public.vela_pool_capacity_allows_readiness(v_cycle.worker_pool_id)
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_capacity_blocked',
            MESSAGE = 'Worker readiness is blocked by capacity state';
    END IF;
    PERFORM 1
    FROM public.execution_profile_revisions AS profile
    WHERE profile.id = v_cycle.execution_profile_revision_id
      AND profile.worker_pool_id = v_cycle.worker_pool_id
      AND profile.state IN ('CANARY', 'ACTIVE')
    FOR SHARE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_profile_conflict',
            MESSAGE = 'Execution Profile is no longer eligible for readiness';
    END IF;

    INSERT INTO public.worker_readiness_evidence (
        cycle_id, check_kind, passed, evidence_digest, observed_by, observed_at
    ) VALUES (
        p_cycle_id, p_check_kind, p_passed, p_evidence_digest, p_observed_by, v_now
    );

    IF NOT p_passed THEN
        UPDATE public.worker_readiness_cycles
        SET state = 'FAILED', next_check = NULL,
            result_code = 'READINESS_' || p_check_kind::text || '_FAILED',
            finished_at = v_now, updated_at = v_now
        WHERE id = p_cycle_id;
        UPDATE public.workers
        SET lifecycle_state = 'DRAINING', reachability_condition = 'SUSPECT', updated_at = v_now
        WHERE id = v_cycle.worker_id AND epoch = v_cycle.worker_epoch;
        RETURN QUERY SELECT
            v_cycle.id, false, 'FAILED'::fleet_readiness_state,
            NULL::fleet_readiness_check, 'DRAINING'::worker_lifecycle_state,
            'SUSPECT'::worker_reachability_condition;
        RETURN;
    END IF;

    v_next := CASE p_check_kind
        WHEN 'IDENTITY' THEN 'DEVICE'::fleet_readiness_check
        WHEN 'DEVICE' THEN 'INFERENCE_BACKEND'::fleet_readiness_check
        WHEN 'INFERENCE_BACKEND' THEN 'MODEL_WARMUP'::fleet_readiness_check
        WHEN 'MODEL_WARMUP' THEN 'CANARY'::fleet_readiness_check
        ELSE NULL
    END;
    IF v_next IS NOT NULL THEN
        UPDATE public.worker_readiness_cycles
        SET next_check = v_next, updated_at = v_now
        WHERE id = p_cycle_id;
        RETURN QUERY SELECT
            v_cycle.id, false, 'CHECKING'::fleet_readiness_state, v_next,
            v_worker.lifecycle_state, v_worker.reachability_condition;
        RETURN;
    END IF;

    IF NOT public.vela_worker_capacity_allows_assignment(v_cycle.worker_id, v_cycle.worker_epoch)
        OR NOT public.vela_pool_capacity_allows_readiness(v_cycle.worker_pool_id)
        OR EXISTS (
            SELECT 1 FROM public.attempts AS attempt
            WHERE attempt.worker_id = v_cycle.worker_id
              AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
        )
        OR EXISTS (
            SELECT 1 FROM public.attempt_leases AS lease
            WHERE lease.worker_id = v_cycle.worker_id
              AND lease.revoked_at IS NULL
              AND lease.expires_at > v_now
        )
        OR NOT EXISTS (
            SELECT 1
            FROM public.execution_profile_revisions AS profile
            WHERE profile.id = v_cycle.execution_profile_revision_id
              AND profile.worker_pool_id = v_cycle.worker_pool_id
              AND profile.state IN ('CANARY', 'ACTIVE')
        )
        OR (SELECT count(*) FROM public.worker_readiness_evidence AS evidence
            WHERE evidence.cycle_id = p_cycle_id AND evidence.passed) <> 5
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_final_authority_changed',
            MESSAGE = 'Worker readiness final authority no longer satisfies promotion';
    END IF;

    INSERT INTO public.worker_profile_readiness (
        worker_id, worker_epoch, execution_profile_revision_id, readiness, updated_at
    ) VALUES (
        v_cycle.worker_id, v_cycle.worker_epoch,
        v_cycle.execution_profile_revision_id, 'WARM', v_now
    )
    ON CONFLICT (worker_id, worker_epoch, execution_profile_revision_id) DO UPDATE
    SET readiness = 'WARM', updated_at = EXCLUDED.updated_at;
    UPDATE public.workers
    SET lifecycle_state = 'READY', reachability_condition = 'HEALTHY', updated_at = v_now
    WHERE id = v_cycle.worker_id AND epoch = v_cycle.worker_epoch;
    UPDATE public.worker_readiness_cycles
    SET state = 'READY', next_check = NULL, result_code = 'READINESS_PASSED',
        finished_at = v_now, updated_at = v_now
    WHERE id = p_cycle_id;
    RETURN QUERY SELECT
        v_cycle.id, false, 'READY'::fleet_readiness_state,
        NULL::fleet_readiness_check, 'READY'::worker_lifecycle_state,
        'HEALTHY'::worker_reachability_condition;
END
$$;

CREATE FUNCTION vela_get_worker_readiness(
    p_cycle_id uuid
) RETURNS TABLE (
    cycle_id uuid,
    readiness_state fleet_readiness_state,
    next_check fleet_readiness_check,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    RETURN QUERY
    SELECT cycle.id, cycle.state, cycle.next_check,
        worker.lifecycle_state, worker.reachability_condition
    FROM public.worker_readiness_cycles AS cycle
    JOIN public.workers AS worker ON worker.id = cycle.worker_id
    WHERE cycle.id = p_cycle_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker readiness cycle does not exist';
    END IF;
END
$$;

CREATE FUNCTION vela_get_worker_readiness_work(
    p_worker_id uuid,
    p_worker_epoch bigint
) RETURNS TABLE (
    cycle_id uuid,
    next_check fleet_readiness_check,
    worker_id uuid,
    worker_pool_id uuid,
    worker_epoch bigint,
    node_identity text,
    execution_profile_revision_id uuid,
    inference_backend_revision text,
    deadline_at timestamptz
)
LANGUAGE plpgsql
VOLATILE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_enforced boolean;
    v_now timestamptz;
    v_worker public.workers%ROWTYPE;
    v_cycle public.worker_readiness_cycles%ROWTYPE;
BEGIN
    IF p_worker_id IS NULL OR p_worker_epoch <= 0 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_readiness_work_identity_invalid',
            MESSAGE = 'Worker readiness work identity is invalid';
    END IF;
    SELECT state.enforced INTO STRICT v_enforced
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton;
    IF NOT v_enforced THEN
        RETURN;
    END IF;
    SELECT worker.* INTO v_worker
    FROM public.workers AS worker
    WHERE worker.id = p_worker_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker does not exist';
    END IF;
    IF v_worker.epoch <> p_worker_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_readiness_work_epoch_conflict',
            MESSAGE = 'Worker readiness work epoch is stale';
    END IF;
    SELECT cycle.* INTO v_cycle
    FROM public.worker_readiness_cycles AS cycle
    WHERE cycle.worker_id = p_worker_id
      AND cycle.worker_epoch = p_worker_epoch
      AND cycle.state = 'CHECKING'
    ORDER BY cycle.started_at DESC
    LIMIT 1
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN;
    END IF;
    v_now := clock_timestamp();
    IF v_cycle.deadline_at <= v_now THEN
        UPDATE public.worker_readiness_cycles
        SET state = 'EXPIRED', next_check = NULL,
            result_code = 'READINESS_DEADLINE_EXPIRED',
            finished_at = v_now, updated_at = v_now
        WHERE id = v_cycle.id;
        UPDATE public.workers
        SET lifecycle_state = 'DRAINING', reachability_condition = 'SUSPECT',
            updated_at = v_now
        WHERE id = v_cycle.worker_id AND epoch = v_cycle.worker_epoch;
        RETURN;
    END IF;
    RETURN QUERY
    SELECT v_cycle.id, v_cycle.next_check, v_cycle.worker_id, v_cycle.worker_pool_id,
        v_cycle.worker_epoch, v_cycle.node_identity,
        v_cycle.execution_profile_revision_id, v_cycle.inference_backend_revision,
        v_cycle.deadline_at;
END
$$;

CREATE FUNCTION vela_request_worker_drain(
    p_operation_id uuid,
    p_worker_id uuid,
    p_expected_epoch bigint,
    p_reason text,
    p_deadline_at timestamptz,
    p_requested_by text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    drain_state fleet_drain_state,
    worker_id uuid,
    worker_epoch bigint,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition,
    deadline_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_worker workers%ROWTYPE;
    v_existing worker_drain_operations%ROWTYPE;
    v_state fleet_drain_state;
    v_active_authority boolean;
BEGIN
    PERFORM public.vela_require_fleet_protocol_enforced();
    IF p_operation_id IS NULL OR p_worker_id IS NULL OR p_expected_epoch <= 0
        OR p_reason IS NULL OR length(p_reason) NOT BETWEEN 1 AND 1000
        OR btrim(p_reason) <> p_reason
		OR p_deadline_at IS NULL
        OR p_requested_by IS NULL OR length(p_requested_by) NOT BETWEEN 1 AND 500
        OR btrim(p_requested_by) <> p_requested_by
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_drain_request_invalid',
            MESSAGE = 'Worker drain request is invalid';
    END IF;

    SELECT * INTO v_existing
    FROM public.worker_drain_operations AS operation
    WHERE operation.id = p_operation_id;
    IF FOUND AND v_existing.worker_id <> p_worker_id THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_drain_operation_conflict',
            MESSAGE = 'Worker drain operation identity conflicts with existing request';
    END IF;

    SELECT * INTO v_worker
    FROM public.workers
    WHERE id = p_worker_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker does not exist';
    END IF;

    SELECT * INTO v_existing
    FROM public.worker_drain_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;
    IF FOUND THEN
        IF v_existing.worker_id <> p_worker_id
            OR v_existing.worker_epoch <> p_expected_epoch
            OR v_existing.reason <> p_reason
            OR v_existing.deadline_at <> p_deadline_at
            OR v_existing.requested_by <> p_requested_by
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_drain_operation_conflict',
                MESSAGE = 'Worker drain operation identity conflicts with existing request';
        END IF;
        RETURN QUERY SELECT
            v_existing.id, true, v_existing.state, v_existing.worker_id,
            v_existing.worker_epoch, v_worker.lifecycle_state,
            v_worker.reachability_condition, v_existing.deadline_at;
		RETURN;
	END IF;
	IF p_deadline_at <= v_now OR p_deadline_at > v_now + interval '24 hours' THEN
		RAISE EXCEPTION USING
			ERRCODE = '22023', CONSTRAINT = 'fleet_drain_request_invalid',
			MESSAGE = 'Worker drain request is invalid';
	END IF;

	IF v_worker.epoch <> p_expected_epoch THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_drain_worker_epoch_conflict',
            MESSAGE = 'Worker drain epoch is stale or conflicting';
    END IF;
    IF v_worker.lifecycle_state NOT IN ('READY', 'BUSY', 'WARMING') THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_drain_worker_state_conflict',
            MESSAGE = 'Worker lifecycle is not eligible for routine drain';
    END IF;

    SELECT
        v_worker.lifecycle_state = 'BUSY'
        OR EXISTS (
            SELECT 1 FROM public.attempts AS attempt
            WHERE attempt.worker_id = p_worker_id
              AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
        )
        OR EXISTS (
            SELECT 1 FROM public.attempt_leases AS lease
            WHERE lease.worker_id = p_worker_id
              AND lease.revoked_at IS NULL
              AND lease.expires_at > v_now
        )
    INTO v_active_authority;
    v_state := CASE
        WHEN v_active_authority THEN 'DRAINING'::fleet_drain_state
        ELSE 'COMPLETE'::fleet_drain_state
    END;

    UPDATE public.workers
    SET lifecycle_state = 'DRAINING', updated_at = v_now
    WHERE id = p_worker_id AND epoch = p_expected_epoch;
    INSERT INTO public.worker_drain_operations (
        id, worker_id, worker_epoch, reason, deadline_at, requested_by,
        state, result_code, requested_at, finished_at, finished_by, updated_at
    ) VALUES (
        p_operation_id, p_worker_id, p_expected_epoch, p_reason, p_deadline_at,
        p_requested_by, v_state,
        CASE WHEN v_state = 'COMPLETE' THEN 'DRAIN_COMPLETE' ELSE NULL END,
        v_now, CASE WHEN v_state = 'COMPLETE' THEN v_now ELSE NULL END,
        CASE WHEN v_state = 'COMPLETE' THEN p_requested_by ELSE NULL END, v_now
    );

    RETURN QUERY SELECT
        p_operation_id, false, v_state, p_worker_id, p_expected_epoch,
        'DRAINING'::worker_lifecycle_state, v_worker.reachability_condition,
        p_deadline_at;
END
$$;

CREATE FUNCTION vela_reconcile_worker_drain(
    p_operation_id uuid,
    p_reconciled_by text
) RETURNS TABLE (
    operation_id uuid,
    replayed boolean,
    drain_state fleet_drain_state,
    worker_id uuid,
    worker_epoch bigint,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition,
    deadline_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_worker_id uuid;
    v_worker workers%ROWTYPE;
    v_operation worker_drain_operations%ROWTYPE;
    v_active_authority boolean;
BEGIN
    PERFORM public.vela_require_fleet_protocol_enforced();
    IF p_operation_id IS NULL OR p_reconciled_by IS NULL
        OR length(p_reconciled_by) NOT BETWEEN 1 AND 500
        OR btrim(p_reconciled_by) <> p_reconciled_by
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_drain_reconcile_invalid',
            MESSAGE = 'Worker drain reconciliation request is invalid';
    END IF;

    SELECT operation.worker_id INTO v_worker_id
    FROM public.worker_drain_operations AS operation
    WHERE operation.id = p_operation_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker drain operation does not exist';
    END IF;
    SELECT * INTO STRICT v_worker
    FROM public.workers
    WHERE id = v_worker_id
    FOR UPDATE;
    SELECT * INTO STRICT v_operation
    FROM public.worker_drain_operations AS operation
    WHERE operation.id = p_operation_id
    FOR UPDATE;

    IF v_operation.state <> 'DRAINING' THEN
        RETURN QUERY SELECT
            v_operation.id, true, v_operation.state, v_operation.worker_id,
            v_operation.worker_epoch, v_worker.lifecycle_state,
            v_worker.reachability_condition, v_operation.deadline_at;
        RETURN;
    END IF;
    IF v_worker.epoch <> v_operation.worker_epoch
        OR v_worker.lifecycle_state <> 'DRAINING'
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_drain_worker_authority_changed',
            MESSAGE = 'Worker drain authority is stale or conflicting';
    END IF;

    IF v_operation.deadline_at <= v_now THEN
        UPDATE public.worker_drain_operations
        SET state = 'EXPIRED', result_code = 'DRAIN_DEADLINE_EXPIRED',
            finished_at = v_now, finished_by = p_reconciled_by, updated_at = v_now
        WHERE id = p_operation_id;
        RETURN QUERY SELECT
            v_operation.id, false, 'EXPIRED'::fleet_drain_state,
            v_operation.worker_id, v_operation.worker_epoch, v_worker.lifecycle_state,
            v_worker.reachability_condition, v_operation.deadline_at;
        RETURN;
    END IF;

    SELECT
        EXISTS (
            SELECT 1 FROM public.attempts AS attempt
            WHERE attempt.worker_id = v_operation.worker_id
              AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
        )
        OR EXISTS (
            SELECT 1 FROM public.attempt_leases AS lease
            WHERE lease.worker_id = v_operation.worker_id
              AND lease.revoked_at IS NULL
              AND lease.expires_at > v_now
        )
    INTO v_active_authority;
    IF v_active_authority THEN
        RETURN QUERY SELECT
            v_operation.id, false, v_operation.state, v_operation.worker_id,
            v_operation.worker_epoch, v_worker.lifecycle_state,
            v_worker.reachability_condition, v_operation.deadline_at;
        RETURN;
    END IF;

    UPDATE public.worker_drain_operations
    SET state = 'COMPLETE', result_code = 'DRAIN_COMPLETE',
        finished_at = v_now, finished_by = p_reconciled_by, updated_at = v_now
    WHERE id = p_operation_id;
    RETURN QUERY SELECT
        v_operation.id, false, 'COMPLETE'::fleet_drain_state,
        v_operation.worker_id, v_operation.worker_epoch, v_worker.lifecycle_state,
        v_worker.reachability_condition, v_operation.deadline_at;
END
$$;

CREATE FUNCTION vela_get_worker_drain(
    p_operation_id uuid
) RETURNS TABLE (
    operation_id uuid,
    drain_state fleet_drain_state,
    worker_id uuid,
    worker_epoch bigint,
    worker_lifecycle_state worker_lifecycle_state,
    worker_reachability_condition worker_reachability_condition,
    deadline_at timestamptz
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_operation_id IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Drain operation id is required';
    END IF;
    RETURN QUERY
    SELECT operation.id, operation.state, operation.worker_id, operation.worker_epoch,
        worker.lifecycle_state, worker.reachability_condition, operation.deadline_at
    FROM public.worker_drain_operations AS operation
    JOIN public.workers AS worker ON worker.id = operation.worker_id
    WHERE operation.id = p_operation_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker drain operation does not exist';
    END IF;
END
$$;

CREATE FUNCTION vela_authorize_fleet_mutation(
    p_request_uid text,
    p_actor_identity text,
    p_resource_kind fleet_protected_resource_kind,
    p_operation fleet_mutation_operation,
    p_kubernetes_uid text,
    p_namespace text,
    p_name text,
    p_worker_pool_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_drain_operation_ids uuid[],
    p_request_digest bytea
) RETURNS TABLE (
    request_uid text,
    replayed boolean,
    authorized boolean
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_now timestamptz := clock_timestamp();
    v_existing fleet_mutation_authorizations%ROWTYPE;
    v_worker workers%ROWTYPE;
    v_drain worker_drain_operations%ROWTYPE;
    v_pool_worker_count integer;
BEGIN
    PERFORM public.vela_require_fleet_protocol_enforced();
    IF p_request_uid IS NULL OR length(p_request_uid) NOT BETWEEN 1 AND 200
        OR btrim(p_request_uid) <> p_request_uid
        OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
        OR btrim(p_actor_identity) <> p_actor_identity
        OR p_resource_kind IS NULL OR p_operation IS NULL
        OR p_kubernetes_uid IS NULL OR length(p_kubernetes_uid) NOT BETWEEN 1 AND 200
        OR btrim(p_kubernetes_uid) <> p_kubernetes_uid
        OR p_namespace IS NULL OR length(p_namespace) NOT BETWEEN 1 AND 253
        OR btrim(p_namespace) <> p_namespace
        OR p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 253
        OR btrim(p_name) <> p_name
        OR p_worker_pool_id IS NULL OR p_drain_operation_ids IS NULL
        OR cardinality(p_drain_operation_ids) = 0
        OR p_request_digest IS NULL OR octet_length(p_request_digest) <> 32
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_mutation_authorization_invalid',
            MESSAGE = 'Fleet mutation authorization request is invalid';
    END IF;

    SELECT * INTO v_existing
    FROM public.fleet_mutation_authorizations AS receipt
    WHERE receipt.request_uid = p_request_uid;
    IF FOUND THEN
        IF v_existing.actor_identity <> p_actor_identity
            OR v_existing.resource_kind <> p_resource_kind
            OR v_existing.operation <> p_operation
            OR v_existing.kubernetes_uid <> p_kubernetes_uid
            OR v_existing.namespace <> p_namespace
            OR v_existing.name <> p_name
            OR v_existing.worker_pool_id <> p_worker_pool_id
            OR v_existing.worker_id IS DISTINCT FROM p_worker_id
            OR v_existing.worker_epoch IS DISTINCT FROM p_worker_epoch
            OR v_existing.drain_operation_ids IS DISTINCT FROM p_drain_operation_ids
            OR v_existing.request_digest IS DISTINCT FROM p_request_digest
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_authorization_conflict',
                MESSAGE = 'Fleet mutation request UID conflicts with existing authorization';
        END IF;
        RETURN QUERY SELECT p_request_uid, true, true;
        RETURN;
    END IF;

    IF cardinality(p_drain_operation_ids) <> (
        SELECT count(DISTINCT operation_id)
        FROM unnest(p_drain_operation_ids) AS operation_id
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_duplicate_drain',
            MESSAGE = 'Fleet mutation DrainOperation set contains duplicates';
    END IF;

    IF p_resource_kind = 'POD' THEN
        IF p_worker_id IS NULL OR p_worker_epoch IS NULL
            OR cardinality(p_drain_operation_ids) <> 1
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_worker_identity_missing',
                MESSAGE = 'Protected Pod mutation requires exact Worker identity';
        END IF;

        SELECT * INTO v_worker
        FROM public.workers
        WHERE id = p_worker_id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker does not exist';
        END IF;
        IF v_worker.worker_pool_id <> p_worker_pool_id
            OR v_worker.epoch <> p_worker_epoch
            OR v_worker.lifecycle_state <> 'DRAINING'
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_worker_authority_conflict',
                MESSAGE = 'Worker mutation authority is stale or conflicting';
        END IF;

        SELECT * INTO v_drain
        FROM public.worker_drain_operations AS operation
        WHERE operation.id = p_drain_operation_ids[1]
        FOR UPDATE;
        IF NOT FOUND OR v_drain.state <> 'COMPLETE'
            OR v_drain.worker_id <> p_worker_id OR v_drain.worker_epoch <> p_worker_epoch
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_drain_incomplete',
                MESSAGE = 'Completed exact-epoch DrainOperation is required';
        END IF;
        IF EXISTS (
            SELECT 1 FROM public.attempts AS attempt
            WHERE attempt.worker_id = p_worker_id
              AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
        ) OR EXISTS (
            SELECT 1 FROM public.attempt_leases AS lease
            WHERE lease.worker_id = p_worker_id
              AND lease.revoked_at IS NULL
              AND lease.expires_at > v_now
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_active_execution',
                MESSAGE = 'Worker mutation cannot proceed with active execution authority';
        END IF;
    ELSE
        IF p_resource_kind NOT IN ('DAEMONSET', 'WORKER_POOL')
            OR p_worker_id IS NOT NULL OR p_worker_epoch IS NOT NULL
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_resource_not_supported',
                MESSAGE = 'Fleet mutation resource is not eligible for this authorization';
        END IF;
        PERFORM 1 FROM public.worker_pools WHERE id = p_worker_pool_id FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker pool does not exist';
        END IF;
        PERFORM 1
        FROM public.workers AS worker
        WHERE worker.worker_pool_id = p_worker_pool_id
        ORDER BY worker.id
        FOR UPDATE;
        IF NOT FOUND THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_pool_empty',
                MESSAGE = 'Protected pool mutation requires at least one Worker';
        END IF;
        SELECT count(*) INTO v_pool_worker_count
        FROM public.workers AS worker
        WHERE worker.worker_pool_id = p_worker_pool_id;
        IF cardinality(p_drain_operation_ids) <> v_pool_worker_count
            OR EXISTS (
                SELECT 1
                FROM public.workers AS worker
                WHERE worker.worker_pool_id = p_worker_pool_id
                  AND (
                      worker.lifecycle_state <> 'DRAINING'
                      OR NOT EXISTS (
                          SELECT 1
                          FROM public.worker_drain_operations AS operation
                          WHERE operation.id = ANY(p_drain_operation_ids)
                            AND operation.worker_id = worker.id
                            AND operation.worker_epoch = worker.epoch
                            AND operation.state = 'COMPLETE'
                      )
                  )
            )
            OR EXISTS (
                SELECT 1
                FROM unnest(p_drain_operation_ids) AS supplied(operation_id)
                LEFT JOIN public.worker_drain_operations AS operation
                  ON operation.id = supplied.operation_id
                LEFT JOIN public.workers AS worker
                  ON worker.id = operation.worker_id
                 AND worker.epoch = operation.worker_epoch
                 AND worker.worker_pool_id = p_worker_pool_id
                WHERE operation.id IS NULL OR operation.state <> 'COMPLETE'
                   OR worker.id IS NULL
            )
        THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_pool_drain_incomplete',
                MESSAGE = 'Complete current-epoch pool DrainOperation set is required';
        END IF;
        IF EXISTS (
            SELECT 1 FROM public.attempts AS attempt
            WHERE attempt.worker_pool_id = p_worker_pool_id
              AND attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
        ) OR EXISTS (
            SELECT 1
            FROM public.attempt_leases AS lease
            JOIN public.workers AS worker ON worker.id = lease.worker_id
            WHERE worker.worker_pool_id = p_worker_pool_id
              AND lease.revoked_at IS NULL
              AND lease.expires_at > v_now
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000', CONSTRAINT = 'fleet_mutation_active_execution',
                MESSAGE = 'Pool mutation cannot proceed with active execution authority';
        END IF;
    END IF;

    INSERT INTO public.fleet_mutation_authorizations (
        request_uid, actor_identity, resource_kind, operation, kubernetes_uid,
        namespace, name, worker_pool_id, worker_id, worker_epoch,
        drain_operation_ids, request_digest, authorized_at
    ) VALUES (
        p_request_uid, p_actor_identity, p_resource_kind, p_operation, p_kubernetes_uid,
        p_namespace, p_name, p_worker_pool_id, p_worker_id, p_worker_epoch,
        p_drain_operation_ids, p_request_digest, v_now
    );
    RETURN QUERY SELECT p_request_uid, false, true;
END
$$;

CREATE FUNCTION vela_has_fleet_retirement_authorization(
    p_resource_kind fleet_protected_resource_kind,
    p_kubernetes_uid text,
    p_namespace text,
    p_name text,
    p_worker_pool_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_drain_operation_ids uuid[]
) RETURNS boolean
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_authorized boolean;
BEGIN
    PERFORM public.vela_require_fleet_protocol_enforced();
    IF p_resource_kind IS NULL
        OR p_kubernetes_uid IS NULL OR length(p_kubernetes_uid) NOT BETWEEN 1 AND 200
        OR btrim(p_kubernetes_uid) <> p_kubernetes_uid
        OR p_namespace IS NULL OR length(p_namespace) NOT BETWEEN 1 AND 253
        OR btrim(p_namespace) <> p_namespace
        OR p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 253
        OR btrim(p_name) <> p_name
        OR p_worker_pool_id IS NULL OR p_drain_operation_ids IS NULL
        OR cardinality(p_drain_operation_ids) NOT BETWEEN 1 AND 4096
        OR cardinality(p_drain_operation_ids) <> (
            SELECT count(DISTINCT operation_id)
            FROM unnest(p_drain_operation_ids) AS operation_id
        )
        OR (
            p_resource_kind = 'POD' AND (
                p_worker_id IS NULL OR p_worker_epoch IS NULL OR p_worker_epoch <= 0
                OR cardinality(p_drain_operation_ids) <> 1
            )
        )
        OR (
            p_resource_kind IN ('DAEMONSET', 'WORKER_POOL') AND (
                p_worker_id IS NOT NULL OR p_worker_epoch IS NOT NULL
            )
        )
        OR p_resource_kind NOT IN ('POD', 'DAEMONSET', 'WORKER_POOL')
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_retirement_authorization_invalid',
            MESSAGE = 'Fleet retirement authorization request is invalid';
    END IF;

    SELECT count(DISTINCT receipt.operation) = 2 INTO v_authorized
    FROM public.fleet_mutation_authorizations AS receipt
    WHERE receipt.resource_kind = p_resource_kind
      AND receipt.operation IN ('DELETE', 'REMOVE_FINALIZER')
      AND receipt.kubernetes_uid = p_kubernetes_uid
      AND receipt.namespace = p_namespace
      AND receipt.name = p_name
      AND receipt.worker_pool_id = p_worker_pool_id
      AND receipt.worker_id IS NOT DISTINCT FROM p_worker_id
      AND receipt.worker_epoch IS NOT DISTINCT FROM p_worker_epoch
      AND cardinality(receipt.drain_operation_ids) = cardinality(p_drain_operation_ids)
      AND receipt.drain_operation_ids @> p_drain_operation_ids
      AND p_drain_operation_ids @> receipt.drain_operation_ids;
    RETURN v_authorized;
END
$$;

CREATE FUNCTION vela_record_fleet_retirement_completion(
    p_resource_kind fleet_protected_resource_kind,
    p_kubernetes_uid text,
    p_namespace text,
    p_name text,
    p_worker_pool_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_drain_operation_ids uuid[],
    p_observed_by text
) RETURNS TABLE (
    replayed boolean,
    recorded_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_drain_operation_ids uuid[];
    v_existing public.fleet_retirement_completions%ROWTYPE;
    v_completed_at timestamptz;
BEGIN
    PERFORM public.vela_require_fleet_protocol_enforced();
    IF p_observed_by IS NULL OR length(p_observed_by) NOT BETWEEN 1 AND 500
        OR btrim(p_observed_by) <> p_observed_by
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_retirement_completion_observer_invalid',
            MESSAGE = 'Fleet retirement completion observer is invalid';
    END IF;
    IF NOT public.vela_has_fleet_retirement_authorization(
        p_resource_kind, p_kubernetes_uid, p_namespace, p_name,
        p_worker_pool_id, p_worker_id, p_worker_epoch, p_drain_operation_ids
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_retirement_completion_unauthorized',
            MESSAGE = 'Fleet retirement completion requires complete mutation authorization';
    END IF;
    SELECT array_agg(drain.operation_id ORDER BY drain.operation_id)
    INTO v_drain_operation_ids
    FROM unnest(p_drain_operation_ids) AS drain(operation_id);

    INSERT INTO public.fleet_retirement_completions (
        resource_kind, kubernetes_uid, namespace, name, worker_pool_id,
        worker_id, worker_epoch, drain_operation_ids, observed_by
    ) VALUES (
        p_resource_kind, p_kubernetes_uid, p_namespace, p_name, p_worker_pool_id,
        p_worker_id, p_worker_epoch, v_drain_operation_ids, p_observed_by
    )
    ON CONFLICT (resource_kind, kubernetes_uid) DO NOTHING
    RETURNING fleet_retirement_completions.completed_at INTO v_completed_at;
    IF FOUND THEN
        RETURN QUERY SELECT false, v_completed_at;
        RETURN;
    END IF;

    SELECT receipt.* INTO STRICT v_existing
    FROM public.fleet_retirement_completions AS receipt
    WHERE receipt.resource_kind = p_resource_kind
      AND receipt.kubernetes_uid = p_kubernetes_uid;
    IF v_existing.namespace <> p_namespace OR v_existing.name <> p_name
        OR v_existing.worker_pool_id <> p_worker_pool_id
        OR v_existing.worker_id IS DISTINCT FROM p_worker_id
        OR v_existing.worker_epoch IS DISTINCT FROM p_worker_epoch
        OR v_existing.drain_operation_ids <> v_drain_operation_ids
        OR v_existing.observed_by <> p_observed_by
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000', CONSTRAINT = 'fleet_retirement_completion_conflict',
            MESSAGE = 'Fleet retirement completion conflicts with an existing receipt';
    END IF;
    RETURN QUERY SELECT true, v_existing.completed_at;
END
$$;

CREATE FUNCTION vela_has_fleet_retirement_completion(
    p_resource_kind fleet_protected_resource_kind,
    p_kubernetes_uid text,
    p_namespace text,
    p_name text,
    p_worker_pool_id uuid,
    p_worker_id uuid,
    p_worker_epoch bigint,
    p_drain_operation_ids uuid[]
) RETURNS boolean
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM public.vela_require_fleet_protocol_enforced();
    IF p_resource_kind IS NULL
        OR p_kubernetes_uid IS NULL OR length(p_kubernetes_uid) NOT BETWEEN 1 AND 200
        OR btrim(p_kubernetes_uid) <> p_kubernetes_uid
        OR p_namespace IS NULL OR length(p_namespace) NOT BETWEEN 1 AND 253
        OR btrim(p_namespace) <> p_namespace
        OR p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 253
        OR btrim(p_name) <> p_name
        OR p_worker_pool_id IS NULL OR p_drain_operation_ids IS NULL
        OR cardinality(p_drain_operation_ids) NOT BETWEEN 1 AND 4096
        OR cardinality(p_drain_operation_ids) <> (
            SELECT count(DISTINCT operation_id)
            FROM unnest(p_drain_operation_ids) AS operation_id
        )
        OR (
            p_resource_kind = 'POD' AND (
                p_worker_id IS NULL OR p_worker_epoch IS NULL OR p_worker_epoch <= 0
                OR cardinality(p_drain_operation_ids) <> 1
            )
        )
        OR (
            p_resource_kind IN ('DAEMONSET', 'WORKER_POOL') AND (
                p_worker_id IS NOT NULL OR p_worker_epoch IS NOT NULL
            )
        )
        OR p_resource_kind NOT IN ('POD', 'DAEMONSET', 'WORKER_POOL')
    THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023', CONSTRAINT = 'fleet_retirement_completion_lookup_invalid',
            MESSAGE = 'Fleet retirement completion lookup is invalid';
    END IF;

    RETURN EXISTS (
        SELECT 1
        FROM public.fleet_retirement_completions AS receipt
        WHERE receipt.resource_kind = p_resource_kind
          AND receipt.kubernetes_uid = p_kubernetes_uid
          AND receipt.namespace = p_namespace
          AND receipt.name = p_name
          AND receipt.worker_pool_id = p_worker_pool_id
          AND receipt.worker_id IS NOT DISTINCT FROM p_worker_id
          AND receipt.worker_epoch IS NOT DISTINCT FROM p_worker_epoch
          AND cardinality(receipt.drain_operation_ids) = cardinality(p_drain_operation_ids)
          AND receipt.drain_operation_ids @> p_drain_operation_ids
          AND p_drain_operation_ids @> receipt.drain_operation_ids
    );
END
$$;

CREATE FUNCTION vela_worker_capacity_allows_assignment(
    p_worker_id uuid,
    p_worker_epoch bigint
) RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT CASE
        WHEN NOT state.enforced THEN true
        ELSE COALESCE((
            SELECT condition.assignment_allowed
            FROM public.worker_capacity_conditions AS condition
            JOIN public.worker_pool_capacity_policies AS policy
              ON policy.worker_pool_id = condition.worker_pool_id
            WHERE condition.worker_id = p_worker_id
              AND condition.worker_epoch = p_worker_epoch
              AND condition.observed_at >= statement_timestamp()
                  - (policy.observation_max_age_seconds * interval '1 second')
              AND condition.observed_at <= statement_timestamp() + interval '30 seconds'
        ), false)
    END
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton
$$;

CREATE FUNCTION vela_pool_capacity_allows_assignment(
    p_worker_pool_id uuid
) RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT CASE
        WHEN NOT state.enforced THEN true
        ELSE COALESCE((
            SELECT condition.assignment_allowed
            FROM public.worker_pool_capacity_conditions AS condition
            JOIN public.worker_pool_capacity_policies AS policy
              ON policy.worker_pool_id = condition.worker_pool_id
            WHERE condition.worker_pool_id = p_worker_pool_id
              AND EXISTS (
                SELECT 1
                FROM public.workers AS worker
                JOIN public.worker_capacity_conditions AS worker_condition
                  ON worker_condition.worker_id = worker.id
                 AND worker_condition.worker_epoch = worker.epoch
                WHERE worker.worker_pool_id = p_worker_pool_id
                  AND worker.lifecycle_state IN ('READY', 'BUSY')
                  AND worker.reachability_condition <> 'OFFLINE'
                  AND worker_condition.observed_at >= statement_timestamp()
                      - (policy.observation_max_age_seconds * interval '1 second')
                  AND worker_condition.observed_at <= statement_timestamp() + interval '30 seconds'
              )
              AND NOT EXISTS (
                SELECT 1
                FROM public.workers AS worker
                LEFT JOIN public.worker_capacity_conditions AS worker_condition
                  ON worker_condition.worker_id = worker.id
                 AND worker_condition.worker_epoch = worker.epoch
                WHERE worker.worker_pool_id = p_worker_pool_id
                  AND worker.lifecycle_state IN ('READY', 'BUSY')
                  AND worker.reachability_condition <> 'OFFLINE'
                  AND (
                    worker_condition.worker_id IS NULL
                    OR worker_condition.observed_at < statement_timestamp()
                        - (policy.observation_max_age_seconds * interval '1 second')
                    OR worker_condition.observed_at > statement_timestamp() + interval '30 seconds'
                  )
              )
        ), false)
    END
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton
$$;

CREATE FUNCTION vela_pool_capacity_allows_readiness(
    p_worker_pool_id uuid
) RETURNS boolean
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT CASE
        WHEN NOT state.enforced THEN true
        ELSE COALESCE((
            SELECT condition.state = 'ADMITTABLE'
              AND NOT EXISTS (
                SELECT 1
                FROM public.workers AS worker
                LEFT JOIN public.worker_capacity_conditions AS worker_condition
                  ON worker_condition.worker_id = worker.id
                 AND worker_condition.worker_epoch = worker.epoch
                WHERE worker.worker_pool_id = p_worker_pool_id
                  AND worker.lifecycle_state IN ('READY', 'BUSY')
                  AND worker.reachability_condition <> 'OFFLINE'
                  AND (
                    worker_condition.worker_id IS NULL
                    OR worker_condition.observed_at < statement_timestamp()
                        - (policy.observation_max_age_seconds * interval '1 second')
                    OR worker_condition.observed_at > statement_timestamp() + interval '30 seconds'
                  )
              )
            FROM public.worker_pool_capacity_conditions AS condition
            JOIN public.worker_pool_capacity_policies AS policy
              ON policy.worker_pool_id = condition.worker_pool_id
            WHERE condition.worker_pool_id = p_worker_pool_id
        ), false)
    END
    FROM public.fleet_assignment_protocol_state AS state
    WHERE state.singleton
$$;

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
          AND public.vela_pool_capacity_allows_assignment(wp.id)
          AND (
              NOT (
                  SELECT protocol.enforced
                  FROM public.fleet_assignment_protocol_state AS protocol
                  WHERE protocol.singleton
              )
              OR EXISTS (
                  SELECT 1
                  FROM public.vela_scheduler_capacity_timeline(
                      wp.id,
                      statement_timestamp()
                  ) AS capacity
                  WHERE capacity.execution_profile_revision_id = epr.id
              )
          )
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
              AND public.vela_pool_capacity_allows_assignment(wp.id)
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
              AND (
                  NOT (
                      SELECT protocol.enforced
                      FROM public.fleet_assignment_protocol_state AS protocol
                      WHERE protocol.singleton
                  )
                  OR EXISTS (
                      SELECT 1
                      FROM public.vela_scheduler_capacity_timeline(
                          v_candidate.pool_id,
                          statement_timestamp()
                      ) AS capacity
                      WHERE capacity.execution_profile_revision_id = epr.id
                  )
              )
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

CREATE OR REPLACE FUNCTION vela_scheduler_capacity_timeline(
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
      AND public.vela_pool_capacity_allows_assignment(worker.worker_pool_id)
      AND public.vela_worker_capacity_allows_assignment(worker.id, worker.epoch)
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
      AND public.vela_pool_capacity_allows_assignment(worker.worker_pool_id)
      AND public.vela_worker_capacity_allows_assignment(worker.id, worker.epoch)
      AND progress.progress_valid_until > p_now
      AND progress.estimated_remaining_seconds IS NOT NULL
      AND progress.estimated_finish_at IS NOT NULL
$$;

CREATE OR REPLACE FUNCTION vela_complete_remediation(
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
        v_lifecycle := 'WARMING';
        v_reachability := 'SUSPECT';
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

CREATE OR REPLACE FUNCTION vela_list_schedulable_worker_pools()
RETURNS TABLE (worker_pool_id uuid)
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
      AND public.vela_pool_capacity_allows_assignment(worker.worker_pool_id)
      AND public.vela_worker_capacity_allows_assignment(worker.id, worker.epoch)
    ORDER BY worker.worker_pool_id
$$;

ALTER FUNCTION vela_worker_capacity_allows_assignment(uuid, bigint) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_pool_capacity_allows_assignment(uuid) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_pool_capacity_allows_readiness(uuid) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_reject_fleet_assignment_protocol_history_mutation()
    OWNER TO vela_internal;
ALTER FUNCTION vela_validate_fleet_assignment_protocol_transition()
    OWNER TO vela_internal;
ALTER FUNCTION vela_reject_fleet_assignment_protocol_state_delete()
    OWNER TO vela_internal;
ALTER FUNCTION vela_enforce_fleet_assignment_protocol_state_transition()
    OWNER TO vela_internal;
ALTER FUNCTION vela_apply_fleet_assignment_protocol_transition()
    OWNER TO vela_internal;
ALTER FUNCTION vela_transition_fleet_assignment_protocol(boolean, text, integer)
    OWNER TO vela_internal;
ALTER FUNCTION vela_current_fleet_assignment_protocol_version()
    OWNER TO vela_internal;
ALTER FUNCTION vela_require_fleet_protocol_enforced() OWNER TO vela_internal;
ALTER FUNCTION vela_resolve_worker_identity(text, uuid, text, text, text)
    OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_guard_fleet_assignment_writer() OWNER TO vela_internal;
ALTER FUNCTION vela_reject_fleet_mutation_authorization_mutation()
    OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_reject_fleet_retirement_completion_mutation()
    OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_begin_worker_readiness(
    uuid, uuid, uuid, bigint, text, uuid, text, text, timestamptz
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_report_worker_readiness(
    uuid, fleet_readiness_check, boolean, bytea, text
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_get_worker_readiness(uuid) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_get_worker_readiness_work(uuid, bigint) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_request_worker_drain(
    uuid, uuid, bigint, text, timestamptz, text
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_reconcile_worker_drain(uuid, text) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_get_worker_drain(uuid) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_authorize_fleet_mutation(
    text, text, fleet_protected_resource_kind, fleet_mutation_operation,
    text, text, text, uuid, uuid, bigint, uuid[], bytea
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_has_fleet_retirement_authorization(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_record_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[], text
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_has_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
) OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION vela_worker_capacity_allows_assignment(uuid, bigint) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_pool_capacity_allows_assignment(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_pool_capacity_allows_readiness(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_worker_capacity_allows_assignment(uuid, bigint) TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_pool_capacity_allows_assignment(uuid) TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_current_fleet_assignment_protocol_version() TO vela_internal;
GRANT EXECUTE ON FUNCTION vela_require_fleet_protocol_enforced() TO vela_fleet_owner;
GRANT EXECUTE ON FUNCTION vela_resolve_worker_identity(text, uuid, text, text, text)
    TO vela_fleet;
REVOKE ALL ON FUNCTION vela_begin_worker_readiness(
    uuid, uuid, uuid, bigint, text, uuid, text, text, timestamptz
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_report_worker_readiness(
    uuid, fleet_readiness_check, boolean, bytea, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_get_worker_readiness(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_get_worker_readiness_work(uuid, bigint) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_begin_worker_readiness(
    uuid, uuid, uuid, bigint, text, uuid, text, text, timestamptz
) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_report_worker_readiness(
    uuid, fleet_readiness_check, boolean, bytea, text
) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_get_worker_readiness(uuid) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_get_worker_readiness_work(uuid, bigint) TO vela_fleet;
REVOKE ALL ON FUNCTION vela_request_worker_drain(
    uuid, uuid, bigint, text, timestamptz, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_get_worker_drain(uuid) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_reconcile_worker_drain(uuid, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_request_worker_drain(
    uuid, uuid, bigint, text, timestamptz, text
) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_get_worker_drain(uuid) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_reconcile_worker_drain(uuid, text) TO vela_fleet;
REVOKE ALL ON FUNCTION vela_authorize_fleet_mutation(
    text, text, fleet_protected_resource_kind, fleet_mutation_operation,
    text, text, text, uuid, uuid, bigint, uuid[], bytea
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_authorize_fleet_mutation(
    text, text, fleet_protected_resource_kind, fleet_mutation_operation,
    text, text, text, uuid, uuid, bigint, uuid[], bytea
) TO vela_fleet;
REVOKE ALL ON FUNCTION vela_has_fleet_retirement_authorization(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_has_fleet_retirement_authorization(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
) TO vela_fleet;
REVOKE ALL ON FUNCTION vela_record_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[], text
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_record_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[], text
) TO vela_fleet;
REVOKE ALL ON FUNCTION vela_has_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_has_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
) TO vela_fleet;

ALTER FUNCTION vela_observe_worker_capacity(
	uuid, uuid, bigint, bigint, timestamptz, fleet_scratch_watermark_state,
	bigint, bigint, bigint, bigint, bigint, boolean, text
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_configure_worker_pool_capacity(
    uuid, text, bigint, bigint, bigint, bigint, bigint, bigint, text
) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_get_worker_pool_capacity(uuid) OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION vela_observe_worker_capacity(
	uuid, uuid, bigint, bigint, timestamptz, fleet_scratch_watermark_state,
	bigint, bigint, bigint, bigint, bigint, boolean, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_configure_worker_pool_capacity(
    uuid, text, bigint, bigint, bigint, bigint, bigint, bigint, text
) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_get_worker_pool_capacity(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_observe_worker_capacity(
	uuid, uuid, bigint, bigint, timestamptz, fleet_scratch_watermark_state,
	bigint, bigint, bigint, bigint, bigint, boolean, text
) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_configure_worker_pool_capacity(
    uuid, text, bigint, bigint, bigint, bigint, bigint, bigint, text
) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_get_worker_pool_capacity(uuid) TO vela_fleet;

GRANT SELECT, INSERT, UPDATE ON worker_capacity_conditions TO vela_fleet_owner;
GRANT SELECT, INSERT, UPDATE ON worker_pool_capacity_conditions TO vela_fleet_owner;
GRANT SELECT, INSERT, UPDATE ON worker_pool_capacity_policies TO vela_fleet_owner;
GRANT SELECT, INSERT, UPDATE ON worker_readiness_cycles TO vela_fleet_owner;
GRANT SELECT, INSERT ON worker_readiness_evidence TO vela_fleet_owner;
-- The evidence replay check takes a row lock without mutating immutable evidence.
GRANT UPDATE (cycle_id) ON worker_readiness_evidence TO vela_fleet_owner;
GRANT SELECT, INSERT, UPDATE ON worker_drain_operations TO vela_fleet_owner;
GRANT SELECT, INSERT ON fleet_mutation_authorizations TO vela_fleet_owner;
GRANT SELECT, INSERT ON fleet_retirement_completions TO vela_fleet_owner;
GRANT SELECT, INSERT ON fleet_worker_pod_identity_bindings TO vela_fleet_owner;
GRANT SELECT, INSERT, UPDATE ON fleet_assignment_protocol_state TO vela_internal;
GRANT SELECT, INSERT, UPDATE ON fleet_assignment_protocol_transitions TO vela_internal;
GRANT SELECT ON fleet_assignment_protocol_state TO vela_fleet_owner;
GRANT SELECT, UPDATE ON workers TO vela_fleet_owner;
GRANT SELECT ON worker_pools TO vela_fleet_owner;
-- PostgreSQL locking clauses require UPDATE on at least one column even when
-- the SECURITY DEFINER functions never mutate the locked catalog row.
GRANT UPDATE (id) ON worker_pools TO vela_fleet_owner;
GRANT SELECT, INSERT ON worker_epochs TO vela_fleet_owner;
GRANT SELECT ON attempts, attempt_leases TO vela_fleet_owner;
GRANT SELECT ON execution_profile_revisions TO vela_fleet_owner;
GRANT UPDATE (id) ON execution_profile_revisions TO vela_fleet_owner;
GRANT SELECT, INSERT, UPDATE, DELETE ON worker_profile_readiness TO vela_fleet_owner;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    LOCK TABLE public.attempts IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.fleet_assignment_protocol_state IN ACCESS EXCLUSIVE MODE;
    LOCK TABLE public.fleet_assignment_protocol_transitions IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.worker_pool_capacity_policies IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.worker_capacity_conditions IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.worker_pool_capacity_conditions IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.worker_readiness_cycles IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.worker_readiness_evidence IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.worker_drain_operations IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.fleet_mutation_authorizations IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.fleet_retirement_completions IN SHARE ROW EXCLUSIVE MODE;
    LOCK TABLE public.fleet_worker_pod_identity_bindings IN SHARE ROW EXCLUSIVE MODE;

    IF EXISTS (
        SELECT 1 FROM public.attempts AS attempt
        WHERE attempt.fleet_protocol_version = 2
    ) OR EXISTS (
        SELECT 1 FROM public.fleet_assignment_protocol_transitions
    ) OR EXISTS (
        SELECT 1 FROM public.worker_pool_capacity_policies
    ) OR EXISTS (
        SELECT 1 FROM public.worker_capacity_conditions
    ) OR EXISTS (
        SELECT 1 FROM public.worker_pool_capacity_conditions
    ) OR EXISTS (
        SELECT 1 FROM public.fleet_worker_pod_identity_bindings
    ) OR EXISTS (
        SELECT 1 FROM public.worker_readiness_cycles
    ) OR EXISTS (
        SELECT 1 FROM public.worker_readiness_evidence
    ) OR EXISTS (
        SELECT 1 FROM public.worker_drain_operations
    ) OR EXISTS (
        SELECT 1 FROM public.fleet_mutation_authorizations
    ) OR EXISTS (
        SELECT 1 FROM public.fleet_retirement_completions
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'fleet_schema_contraction_has_references',
            MESSAGE = 'Fleet schema contraction requires zero protocol references and durable evidence';
    END IF;
END
$$;

CREATE OR REPLACE FUNCTION vela_complete_remediation(
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

CREATE OR REPLACE FUNCTION vela_list_schedulable_worker_pools()
RETURNS TABLE (worker_pool_id uuid)
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

CREATE OR REPLACE FUNCTION vela_scheduler_capacity_timeline(
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
          SELECT 1 FROM public.attempts AS active_attempt
          WHERE active_attempt.worker_id = worker.id
            AND active_attempt.state IN ('ASSIGNED', 'RUNNING', 'FINALIZING')
      )
      AND NOT EXISTS (
          SELECT 1 FROM public.scheduler_dispatch_intents AS live_claim
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

DROP FUNCTION IF EXISTS vela_get_worker_readiness(uuid);
DROP FUNCTION IF EXISTS vela_get_worker_readiness_work(uuid, bigint);
DROP FUNCTION IF EXISTS vela_report_worker_readiness(
    uuid, fleet_readiness_check, boolean, bytea, text
);
DROP FUNCTION IF EXISTS vela_begin_worker_readiness(
    uuid, uuid, uuid, bigint, text, uuid, text, text, timestamptz
);
DROP FUNCTION IF EXISTS vela_get_worker_drain(uuid);
DROP FUNCTION IF EXISTS vela_reconcile_worker_drain(uuid, text);
DROP FUNCTION IF EXISTS vela_request_worker_drain(
    uuid, uuid, bigint, text, timestamptz, text
);
DROP FUNCTION IF EXISTS vela_authorize_fleet_mutation(
    text, text, fleet_protected_resource_kind, fleet_mutation_operation,
    text, text, text, uuid, uuid, bigint, uuid[], bytea
);
DROP FUNCTION IF EXISTS vela_record_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[], text
);
DROP FUNCTION IF EXISTS vela_has_fleet_retirement_completion(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
);
DROP FUNCTION IF EXISTS vela_has_fleet_retirement_authorization(
    fleet_protected_resource_kind, text, text, text, uuid, uuid, bigint, uuid[]
);
DROP TRIGGER IF EXISTS attempts_fleet_assignment_protocol ON attempts;
DROP FUNCTION IF EXISTS vela_guard_fleet_assignment_writer();
ALTER TABLE attempts DROP COLUMN IF EXISTS fleet_protocol_version;
DROP TRIGGER IF EXISTS fleet_assignment_protocol_apply_transition
    ON fleet_assignment_protocol_transitions;
DROP TRIGGER IF EXISTS fleet_assignment_protocol_enforce_transition
    ON fleet_assignment_protocol_state;
DROP TRIGGER IF EXISTS fleet_assignment_protocol_state_required
    ON fleet_assignment_protocol_state;
DROP TRIGGER IF EXISTS fleet_assignment_protocol_validate_transition
    ON fleet_assignment_protocol_transitions;
DROP TRIGGER IF EXISTS fleet_assignment_protocol_history_immutable
    ON fleet_assignment_protocol_transitions;
DROP FUNCTION IF EXISTS vela_current_fleet_assignment_protocol_version();
DROP FUNCTION IF EXISTS vela_resolve_worker_identity(text, uuid, text, text, text);
DROP FUNCTION IF EXISTS vela_require_fleet_protocol_enforced();
DROP FUNCTION IF EXISTS vela_transition_fleet_assignment_protocol(boolean, text, integer);
DROP FUNCTION IF EXISTS vela_apply_fleet_assignment_protocol_transition();
DROP FUNCTION IF EXISTS vela_enforce_fleet_assignment_protocol_state_transition();
DROP FUNCTION IF EXISTS vela_reject_fleet_assignment_protocol_state_delete();
DROP FUNCTION IF EXISTS vela_validate_fleet_assignment_protocol_transition();
DROP FUNCTION IF EXISTS vela_reject_fleet_assignment_protocol_history_mutation();
DROP FUNCTION IF EXISTS vela_pool_capacity_allows_readiness(uuid);
DROP FUNCTION IF EXISTS vela_pool_capacity_allows_assignment(uuid);
DROP FUNCTION IF EXISTS vela_worker_capacity_allows_assignment(uuid, bigint);
DROP FUNCTION IF EXISTS vela_get_worker_pool_capacity(uuid);
DROP FUNCTION IF EXISTS vela_configure_worker_pool_capacity(
    uuid, text, bigint, bigint, bigint, bigint, bigint, bigint, text
);
DROP FUNCTION IF EXISTS vela_observe_worker_capacity(
	uuid, uuid, bigint, bigint, timestamptz, fleet_scratch_watermark_state,
	bigint, bigint, bigint, bigint, bigint, boolean, text
);
DROP TABLE IF EXISTS worker_pool_capacity_conditions;
DROP TABLE IF EXISTS worker_capacity_conditions;
DROP TABLE IF EXISTS worker_pool_capacity_policies;
DROP TABLE IF EXISTS fleet_worker_pod_identity_bindings;
DROP TABLE IF EXISTS worker_readiness_evidence;
DROP TABLE IF EXISTS worker_readiness_cycles;
DROP TABLE IF EXISTS worker_drain_operations;
DROP TRIGGER IF EXISTS fleet_retirement_completions_immutable
    ON fleet_retirement_completions;
DROP FUNCTION IF EXISTS vela_reject_fleet_retirement_completion_mutation();
DROP TABLE IF EXISTS fleet_retirement_completions;
DROP TRIGGER IF EXISTS fleet_mutation_authorizations_immutable
    ON fleet_mutation_authorizations;
DROP FUNCTION IF EXISTS vela_reject_fleet_mutation_authorization_mutation();
DROP TABLE IF EXISTS fleet_mutation_authorizations;
DROP TABLE IF EXISTS fleet_assignment_protocol_transitions;
DROP TABLE IF EXISTS fleet_assignment_protocol_state;
DROP TYPE IF EXISTS fleet_mutation_operation;
DROP TYPE IF EXISTS fleet_protected_resource_kind;
DROP TYPE IF EXISTS fleet_drain_state;
DROP TYPE IF EXISTS fleet_readiness_check;
DROP TYPE IF EXISTS fleet_readiness_state;
DROP TYPE IF EXISTS fleet_scratch_watermark_state;
DROP TYPE IF EXISTS fleet_capacity_state;
-- +goose StatementEnd
