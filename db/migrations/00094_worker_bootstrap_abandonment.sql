-- +goose Up
CREATE TABLE worker_bootstrap_abandonments (
    request_id uuid PRIMARY KEY REFERENCES worker_bootstrap_claims(request_id),
    fenced_instance_epoch bigint NOT NULL CHECK (fenced_instance_epoch > 1),
    abandoned_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER worker_bootstrap_abandonments_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON worker_bootstrap_abandonments
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_worker_bootstrap_mutation();
CREATE CONSTRAINT TRIGGER worker_bootstrap_abandonments_require_synchronous_quorum
AFTER INSERT ON worker_bootstrap_abandonments DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();

-- +goose StatementBegin
CREATE FUNCTION vela_abandon_worker_bootstrap(p_request_id uuid, p_node text, p_actor text)
RETURNS timestamptz
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_claim public.worker_bootstrap_claims%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_abandoned_at timestamptz;
BEGIN
    IF p_request_id IS NULL OR p_request_id = '00000000-0000-0000-0000-000000000000'
       OR p_node IS NULL OR length(p_node) NOT BETWEEN 1 AND 253
       OR p_actor IS NULL OR length(p_actor) NOT BETWEEN 1 AND 500 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Worker bootstrap abandonment is invalid';
    END IF;
    SELECT * INTO v_claim FROM public.worker_bootstrap_claims
    WHERE request_id = p_request_id AND node_identity = p_node AND actor_identity = p_actor;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker bootstrap claim does not exist';
    END IF;
    -- Match the Worker-before-claim order used by first use and receipt recording.
    SELECT * INTO STRICT v_worker FROM public.worker_instances WHERE id = v_claim.worker_instance_id FOR UPDATE;
    PERFORM 1 FROM public.worker_bootstrap_claims WHERE request_id = p_request_id FOR UPDATE;
    SELECT abandoned_at INTO v_abandoned_at FROM public.worker_bootstrap_abandonments WHERE request_id = p_request_id;
    IF FOUND THEN
        RETURN v_abandoned_at;
    END IF;
    IF EXISTS (SELECT 1 FROM public.worker_bootstrap_receipts WHERE request_id = p_request_id) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Recorded Worker bootstrap cannot be abandoned';
    END IF;
    IF NOT ((v_worker.lifecycle_state = 'PROVISIONING' AND v_worker.instance_epoch = v_claim.worker_instance_epoch
             AND v_worker.observed_at IS NULL AND v_worker.device_set_id IS NULL
             AND v_worker.control_session_epoch = 1 AND v_worker.control_session_id IS NULL)
         OR (v_worker.lifecycle_state = 'FENCED' AND v_worker.instance_epoch = v_claim.worker_instance_epoch + 1))
       OR EXISTS (SELECT 1 FROM public.worker_instance_epochs WHERE worker_instance_id = v_worker.id)
       OR EXISTS (SELECT 1 FROM public.worker_members WHERE worker_instance_id = v_worker.id)
       OR EXISTS (SELECT 1 FROM public.model_residencies WHERE worker_instance_id = v_worker.id)
       OR EXISTS (SELECT 1 FROM public.active_device_bindings WHERE worker_instance_id = v_worker.id) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap abandonment requires unobserved first-use authority';
    END IF;
    PERFORM public.vela_fence_worker_instance(v_worker.id, v_claim.worker_instance_epoch, 'BOOTSTRAP_ABANDONED', p_actor);
    INSERT INTO public.worker_bootstrap_abandonments(request_id, fenced_instance_epoch)
    VALUES (p_request_id, v_claim.worker_instance_epoch + 1) RETURNING abandoned_at INTO v_abandoned_at;
    RETURN v_abandoned_at;
END
$$;
-- +goose StatementEnd

-- Preserve the original immutable receipt implementation behind a locked guard.
ALTER FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text)
    RENAME TO vela_record_worker_bootstrap_receipt_v93;
REVOKE ALL ON FUNCTION vela_record_worker_bootstrap_receipt_v93(uuid, uuid, bytea, uuid, bytea, text) FROM vela_fleet;
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_record_worker_bootstrap_receipt(
    p_request_id uuid, p_worker_journal_id uuid, p_worker_scope bytea,
    p_runtime_journal_id uuid, p_runtime_scope bytea, p_actor text
) RETURNS timestamptz
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_claim public.worker_bootstrap_claims%ROWTYPE;
BEGIN
    IF p_request_id IS NULL OR p_worker_journal_id IS NULL OR p_runtime_journal_id IS NULL
       OR p_worker_scope IS NULL OR octet_length(p_worker_scope) <> 32
       OR p_runtime_scope IS NULL OR octet_length(p_runtime_scope) <> 32 OR p_actor IS NULL THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Worker bootstrap receipt is invalid';
    END IF;
    SELECT * INTO v_claim FROM public.worker_bootstrap_claims WHERE request_id = p_request_id;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Worker bootstrap claim does not exist';
    END IF;
    PERFORM 1 FROM public.worker_instances WHERE id = v_claim.worker_instance_id FOR UPDATE;
    PERFORM 1 FROM public.worker_bootstrap_claims WHERE request_id = p_request_id FOR UPDATE;
    IF EXISTS (SELECT 1 FROM public.worker_bootstrap_abandonments WHERE request_id = p_request_id) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Abandoned Worker bootstrap cannot record a journal pair';
    END IF;
    RETURN public.vela_record_worker_bootstrap_receipt_v93(p_request_id, p_worker_journal_id, p_worker_scope,
        p_runtime_journal_id, p_runtime_scope, p_actor);
END
$$;
-- +goose StatementEnd

-- Keep the original history API available for old readers, without granting new authority.
-- +goose StatementBegin
CREATE FUNCTION vela_lookup_worker_bootstrap_v2(p_request_id uuid, p_node text, p_actor text)
RETURNS TABLE (
    request_id uuid, worker_instance_id uuid, worker_instance_epoch bigint,
    worker_member_id uuid, worker_member_epoch bigint, node_identity text,
    bundle_digest bytea, claimed_at timestamptz, actor_identity text,
    worker_journal_id uuid, worker_scope bytea,
    runtime_journal_id uuid, runtime_scope bytea, recorded_at timestamptz,
    fenced_instance_epoch bigint, abandoned_at timestamptz
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT history.*, abandonment.fenced_instance_epoch, abandonment.abandoned_at
    FROM public.vela_lookup_worker_bootstrap(p_request_id, p_node, p_actor) AS history
    LEFT JOIN public.worker_bootstrap_abandonments AS abandonment USING (request_id);
$$;
-- +goose StatementEnd

-- Database-only quiescence may exclude permanently rejected completion authority.
ALTER FUNCTION vela_recovery_inventory() RENAME TO vela_recovery_inventory_v93;
REVOKE ALL ON FUNCTION vela_recovery_inventory_v93() FROM vela_recovery;
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_recovery_inventory() RETURNS jsonb
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT public.vela_recovery_inventory_v93() || jsonb_build_object(
        'worker_bootstrap_claims', (SELECT count(*) FROM public.worker_bootstrap_claims AS claim
            WHERE NOT EXISTS (SELECT 1 FROM public.worker_bootstrap_receipts WHERE request_id = claim.request_id)
              AND NOT EXISTS (SELECT 1 FROM public.worker_bootstrap_abandonments WHERE request_id = claim.request_id))
    );
$$;
-- +goose StatementEnd

ALTER TABLE worker_bootstrap_abandonments OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_abandon_worker_bootstrap(uuid, text, text) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_lookup_worker_bootstrap_v2(uuid, text, text) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_recovery_inventory() OWNER TO vela_recovery_owner;
REVOKE ALL ON TABLE worker_bootstrap_abandonments FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_abandon_worker_bootstrap(uuid, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_lookup_worker_bootstrap_v2(uuid, text, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_recovery_inventory() FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_abandon_worker_bootstrap(uuid, text, text) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_lookup_worker_bootstrap_v2(uuid, text, text) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text) TO vela_fleet;
GRANT SELECT ON worker_bootstrap_abandonments TO vela_recovery_owner;
GRANT EXECUTE ON FUNCTION vela_recovery_inventory() TO vela_recovery;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM worker_bootstrap_abandonments) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Worker bootstrap abandonment history prohibits rollback';
    END IF;
END $$;
-- +goose StatementEnd
DROP FUNCTION vela_recovery_inventory();
ALTER FUNCTION vela_recovery_inventory_v93() RENAME TO vela_recovery_inventory;
GRANT EXECUTE ON FUNCTION vela_recovery_inventory() TO vela_recovery;
DROP FUNCTION vela_lookup_worker_bootstrap_v2(uuid, text, text);
DROP FUNCTION vela_abandon_worker_bootstrap(uuid, text, text);
DROP FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text);
ALTER FUNCTION vela_record_worker_bootstrap_receipt_v93(uuid, uuid, bytea, uuid, bytea, text)
    RENAME TO vela_record_worker_bootstrap_receipt;
GRANT EXECUTE ON FUNCTION vela_record_worker_bootstrap_receipt(uuid, uuid, bytea, uuid, bytea, text) TO vela_fleet;
DROP TABLE worker_bootstrap_abandonments;
