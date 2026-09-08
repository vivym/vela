-- +goose Up
-- This is an immutable first-use reservation, not a Node grant or readiness.
-- No release/replacement exists until independent process termination and
-- journal reconciliation can be proved. Lost replies must never mint Fresh.
CREATE TABLE runtime_startup_reservations (
    request_id uuid PRIMARY KEY CHECK (request_id <> '00000000-0000-0000-0000-000000000000'),
    bootstrap_request_id uuid NOT NULL UNIQUE REFERENCES worker_bootstrap_receipts(request_id),
    node_identity text NOT NULL,
    actor_identity text NOT NULL,
    runtime_journal_id uuid NOT NULL UNIQUE,
    runtime_scope bytea NOT NULL CHECK (octet_length(runtime_scope) = 32),
    incarnation_id uuid NOT NULL UNIQUE,
    launch_digest bytea NOT NULL CHECK (octet_length(launch_digest) = 32),
    owner_observation_digest bytea NOT NULL CHECK (octet_length(owner_observation_digest) = 32),
    epochs jsonb NOT NULL CHECK (jsonb_typeof(epochs) = 'array' AND jsonb_array_length(epochs) BETWEEN 1 AND 64),
    reserved_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER runtime_startup_reservations_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON runtime_startup_reservations
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_worker_bootstrap_mutation();
CREATE CONSTRAINT TRIGGER runtime_startup_reservations_require_synchronous_quorum
AFTER INSERT ON runtime_startup_reservations DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();
CREATE TRIGGER runtime_startup_recovery_admission_gate
BEFORE INSERT ON runtime_startup_reservations
FOR EACH ROW EXECUTE FUNCTION vela_require_admission_open();

-- +goose StatementBegin
CREATE FUNCTION vela_reserve_runtime_startup(
    p_request uuid, p_bootstrap uuid, p_node text, p_actor text,
    p_journal uuid, p_scope bytea, p_incarnation uuid, p_launch bytea,
    p_owner bytea, p_epochs jsonb
) RETURNS TABLE (fresh boolean, reserved_at timestamptz)
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_claim public.worker_bootstrap_claims%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_bundle public.worker_bundles%ROWTYPE;
    v_existing public.runtime_startup_reservations%ROWTYPE;
    v_payload jsonb;
    v_launch jsonb;
    v_epochs jsonb;
    v_time timestamptz;
BEGIN
    IF p_request IS NULL OR p_request = '00000000-0000-0000-0000-000000000000'
       OR p_bootstrap IS NULL OR p_journal IS NULL OR p_incarnation IS NULL
       OR p_incarnation::text !~ '^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
       OR p_node IS NULL OR length(p_node) NOT BETWEEN 1 AND 253 OR btrim(p_node) <> p_node
       OR p_actor IS NULL OR length(p_actor) NOT BETWEEN 1 AND 500 OR btrim(p_actor) <> p_actor
       OR p_scope IS NULL OR octet_length(p_scope) <> 32 OR p_scope = decode(repeat('00',32),'hex')
       OR p_launch IS NULL OR octet_length(p_launch) <> 32 OR p_launch = decode(repeat('00',32),'hex')
       OR p_owner IS NULL OR octet_length(p_owner) <> 32 OR p_owner = decode(repeat('00',32),'hex')
       OR p_epochs IS NULL OR jsonb_typeof(p_epochs) IS DISTINCT FROM 'array' THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Runtime startup reservation is invalid';
    END IF;
    SELECT * INTO v_claim FROM public.worker_bootstrap_claims
    WHERE request_id = p_bootstrap AND node_identity = p_node AND actor_identity = p_actor;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Runtime startup bootstrap does not exist';
    END IF;
    -- Same Worker-before-claim order as bootstrap observation, fencing and receipts.
    SELECT * INTO STRICT v_worker FROM public.worker_instances WHERE id = v_claim.worker_instance_id FOR UPDATE;
    PERFORM 1 FROM public.worker_bootstrap_claims WHERE request_id = p_bootstrap FOR UPDATE;
    IF NOT EXISTS (SELECT 1 FROM public.worker_bootstrap_receipts
        WHERE request_id = p_bootstrap AND runtime_journal_id = p_journal AND runtime_scope = p_scope)
       OR EXISTS (SELECT 1 FROM public.worker_bootstrap_abandonments WHERE request_id = p_bootstrap) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Runtime startup requires the exact retained journal pair';
    END IF;

    SELECT * INTO v_existing FROM public.runtime_startup_reservations WHERE request_id = p_request;
    IF FOUND THEN
        IF v_existing.bootstrap_request_id <> p_bootstrap OR v_existing.node_identity <> p_node
           OR v_existing.actor_identity <> p_actor OR v_existing.runtime_journal_id <> p_journal
           OR v_existing.runtime_scope <> p_scope OR v_existing.incarnation_id <> p_incarnation
           OR v_existing.launch_digest <> p_launch OR v_existing.owner_observation_digest <> p_owner
           OR v_existing.epochs <> p_epochs THEN
            RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Runtime startup request identity was reused';
        END IF;
        RETURN QUERY SELECT false, v_existing.reserved_at;
        RETURN;
    END IF;
    -- First use only, before any observed/registered Runtime authority. A replay
    -- above remains history even after fencing; a fresh reservation cannot.
    IF v_worker.lifecycle_state <> 'PROVISIONING' OR v_worker.instance_epoch <> v_claim.worker_instance_epoch
       OR v_worker.worker_bundle_id <> v_claim.worker_bundle_id
       OR v_worker.observed_at IS NOT NULL OR v_worker.device_set_id IS NOT NULL
       OR v_worker.control_session_epoch <> 1 OR v_worker.control_session_id IS NOT NULL
       OR EXISTS (SELECT 1 FROM public.worker_instance_epochs WHERE worker_instance_id = v_worker.id)
       OR EXISTS (SELECT 1 FROM public.worker_members WHERE worker_instance_id = v_worker.id)
       OR EXISTS (SELECT 1 FROM public.model_residencies WHERE worker_instance_id = v_worker.id)
       OR EXISTS (SELECT 1 FROM public.active_device_bindings WHERE worker_instance_id = v_worker.id) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Runtime startup requires unobserved first-use authority';
    END IF;
    SELECT * INTO STRICT v_bundle FROM public.worker_bundles WHERE id = v_claim.worker_bundle_id FOR SHARE;
    SELECT convert_from(manifest, 'UTF8')::jsonb INTO v_payload
    FROM public.worker_bootstrap_manifests WHERE worker_bundle_id = v_bundle.id AND layout_digest = v_bundle.layout_digest;
    IF NOT FOUND OR v_bundle.residency_plan_revision_id IS DISTINCT FROM v_worker.residency_plan_revision_id
       OR v_bundle.lifecycle_state NOT IN ('APPLYING', 'READY') THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Runtime startup approved bundle changed';
    END IF;
    SELECT entry INTO STRICT v_launch FROM jsonb_array_elements(v_payload #> '{bundle,worker_instances}') AS entry
    WHERE entry ->> 'id' = v_worker.id::text;
    IF EXISTS (SELECT 1 FROM jsonb_array_elements(v_launch -> 'model_runtimes') AS runtime
        WHERE (runtime ->> 'model_runtime_epoch_floor')::numeric NOT BETWEEN 1 AND 9223372036854775806
           OR runtime ->> 'model_runtime_epoch_floor' IS NULL) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Runtime startup epoch floor is exhausted or invalid';
    END IF;
    SELECT jsonb_agg(jsonb_build_object(
        'model_residency_id', runtime ->> 'model_residency_id',
        'model_runtime_identity', runtime ->> 'runtime_identity',
        'stage_profile_revision_id', runtime ->> 'stage_profile_revision_id',
        'model_runtime_epoch', (runtime ->> 'model_runtime_epoch_floor')::bigint + 1
    ) ORDER BY runtime ->> 'model_residency_id') INTO v_epochs
    FROM jsonb_array_elements(v_launch -> 'model_runtimes') AS runtime;
    IF v_epochs IS NULL OR jsonb_array_length(v_epochs) NOT BETWEEN 1 AND 64
       OR v_epochs IS DISTINCT FROM p_epochs THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Runtime startup epoch vector differs from approved bundle';
    END IF;
    INSERT INTO public.runtime_startup_reservations(request_id, bootstrap_request_id, node_identity, actor_identity,
        runtime_journal_id, runtime_scope, incarnation_id, launch_digest, owner_observation_digest, epochs)
    VALUES (p_request, p_bootstrap, p_node, p_actor, p_journal, p_scope, p_incarnation, p_launch, p_owner, v_epochs)
    RETURNING runtime_startup_reservations.reserved_at INTO v_time;
    RETURN QUERY SELECT true, v_time;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION vela_lookup_runtime_startup(p_request uuid, p_node text, p_actor text)
RETURNS SETOF runtime_startup_reservations
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT * FROM public.runtime_startup_reservations
    WHERE request_id = p_request AND node_identity = p_node AND actor_identity = p_actor;
$$;
-- +goose StatementEnd

ALTER FUNCTION vela_recovery_inventory() RENAME TO vela_recovery_inventory_v94;
REVOKE ALL ON FUNCTION vela_recovery_inventory_v94() FROM vela_recovery;
-- +goose StatementBegin
CREATE FUNCTION vela_recovery_inventory() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT public.vela_recovery_inventory_v94() || jsonb_build_object(
        'runtime_startup_reservations', (SELECT count(*) FROM public.runtime_startup_reservations));
$$;
-- +goose StatementEnd
ALTER TABLE runtime_startup_reservations OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_reserve_runtime_startup(uuid,uuid,text,text,uuid,bytea,uuid,bytea,bytea,jsonb) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_lookup_runtime_startup(uuid,text,text) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_recovery_inventory() OWNER TO vela_recovery_owner;
REVOKE ALL ON TABLE runtime_startup_reservations FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_reserve_runtime_startup(uuid,uuid,text,text,uuid,bytea,uuid,bytea,bytea,jsonb) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_lookup_runtime_startup(uuid,text,text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_recovery_inventory() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_reserve_runtime_startup(uuid,uuid,text,text,uuid,bytea,uuid,bytea,bytea,jsonb) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_lookup_runtime_startup(uuid,text,text) TO vela_fleet;
GRANT SELECT ON runtime_startup_reservations TO vela_recovery_owner;
GRANT EXECUTE ON FUNCTION vela_recovery_inventory() TO vela_recovery;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM runtime_startup_reservations) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Runtime startup reservation history prohibits rollback';
    END IF;
END $$;
-- +goose StatementEnd
DROP FUNCTION vela_recovery_inventory();
ALTER FUNCTION vela_recovery_inventory_v94() RENAME TO vela_recovery_inventory;
GRANT EXECUTE ON FUNCTION vela_recovery_inventory() TO vela_recovery;
DROP FUNCTION vela_lookup_runtime_startup(uuid,text,text);
DROP FUNCTION vela_reserve_runtime_startup(uuid,uuid,text,text,uuid,bytea,uuid,bytea,bytea,jsonb);
DROP TABLE runtime_startup_reservations;
