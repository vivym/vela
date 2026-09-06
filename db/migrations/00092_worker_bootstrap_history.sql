-- +goose Up
-- History inspection must never call the first-use mutation to discover state.
-- Both the authenticated node and original actor scope the immutable claim.
-- +goose StatementBegin
CREATE FUNCTION vela_lookup_worker_bootstrap(p_request_id uuid, p_node text, p_actor text)
RETURNS TABLE (
    request_id uuid, worker_instance_id uuid, worker_instance_epoch bigint,
    worker_member_id uuid, worker_member_epoch bigint, node_identity text,
    bundle_digest bytea, claimed_at timestamptz, actor_identity text,
    worker_journal_id uuid, worker_scope bytea,
    runtime_journal_id uuid, runtime_scope bytea, recorded_at timestamptz
)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT claim.request_id, claim.worker_instance_id, claim.worker_instance_epoch,
           claim.worker_member_id, claim.worker_member_epoch, claim.node_identity,
           manifest.layout_digest, claim.claimed_at, claim.actor_identity,
           receipt.worker_journal_id, receipt.worker_scope,
           receipt.runtime_journal_id, receipt.runtime_scope, receipt.recorded_at
    FROM public.worker_bootstrap_claims AS claim
    JOIN public.worker_bootstrap_manifests AS manifest USING (worker_bundle_id)
    LEFT JOIN public.worker_bootstrap_receipts AS receipt USING (request_id)
    WHERE claim.request_id = p_request_id AND claim.node_identity = p_node
      AND claim.actor_identity = p_actor;
$$;
-- +goose StatementEnd
ALTER FUNCTION vela_lookup_worker_bootstrap(uuid, text, text) OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION vela_lookup_worker_bootstrap(uuid, text, text) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_lookup_worker_bootstrap(uuid, text, text) TO vela_fleet;

-- +goose Down
DROP FUNCTION vela_lookup_worker_bootstrap(uuid, text, text);
