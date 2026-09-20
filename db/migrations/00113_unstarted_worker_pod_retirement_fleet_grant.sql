-- +goose Up
-- The Fleet control-plane login owns the Pod-mutation authority surface.
-- Keep the append-only history readable only through the existing internal
-- path, while granting the SECURITY DEFINER entry point to vela_fleet just
-- like the established WorkerInstance Pod-mutation function.
GRANT EXECUTE ON FUNCTION vela_authorize_unstarted_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text,
    uuid, bigint, uuid, uuid, uuid, text, bytea
) TO vela_fleet;
REVOKE EXECUTE ON FUNCTION vela_authorize_unstarted_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text,
    uuid, bigint, uuid, uuid, uuid, text, bytea
) FROM vela_internal;

-- +goose Down
REVOKE EXECUTE ON FUNCTION vela_authorize_unstarted_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text,
    uuid, bigint, uuid, uuid, uuid, text, bytea
) FROM vela_fleet;
GRANT EXECUTE ON FUNCTION vela_authorize_unstarted_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text,
    uuid, bigint, uuid, uuid, uuid, text, bytea
) TO vela_internal;
