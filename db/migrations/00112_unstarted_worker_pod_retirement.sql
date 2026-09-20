-- +goose Up
-- A Worker Pod can be created before its node agent ever reaches the
-- bootstrap RPC. Such a Pod still needs an append-only, exact retirement
-- authority after the rollout is withdrawn and the Worker is fenced.
CREATE TABLE worker_unstarted_pod_mutation_authorizations (
    request_uid text PRIMARY KEY CHECK (
        length(request_uid) BETWEEN 1 AND 200 AND btrim(request_uid) = request_uid
    ),
    actor_identity text NOT NULL CHECK (
        length(actor_identity) BETWEEN 1 AND 500 AND btrim(actor_identity) = actor_identity
    ),
    operation fleet_mutation_operation NOT NULL CHECK (operation IN ('DELETE', 'REMOVE_FINALIZER')),
    kubernetes_uid text NOT NULL CHECK (
        length(kubernetes_uid) BETWEEN 1 AND 200 AND btrim(kubernetes_uid) = kubernetes_uid
    ),
    namespace text NOT NULL CHECK (length(namespace) BETWEEN 1 AND 253 AND btrim(namespace) = namespace),
    name text NOT NULL CHECK (length(name) BETWEEN 1 AND 253 AND btrim(name) = name),
    worker_instance_id uuid NOT NULL,
    worker_instance_epoch bigint NOT NULL CHECK (worker_instance_epoch > 0),
    residency_plan_revision_id uuid NOT NULL REFERENCES residency_plan_revisions(id),
    worker_bundle_id uuid NOT NULL REFERENCES worker_bundles(id),
    worker_member_id uuid NOT NULL,
    worker_member_key text NOT NULL CHECK (length(worker_member_key) BETWEEN 1 AND 200 AND btrim(worker_member_key) = worker_member_key),
    request_digest bytea NOT NULL CHECK (octet_length(request_digest) = 32),
    authorized_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    FOREIGN KEY (worker_instance_id, worker_instance_epoch)
        REFERENCES worker_instance_epochs(worker_instance_id, epoch)
);

ALTER TABLE worker_unstarted_pod_mutation_authorizations OWNER TO vela_fleet_owner;
REVOKE ALL ON worker_unstarted_pod_mutation_authorizations FROM PUBLIC;
GRANT SELECT ON worker_unstarted_pod_mutation_authorizations TO vela_internal;
CREATE TRIGGER worker_unstarted_pod_mutation_authorizations_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON worker_unstarted_pod_mutation_authorizations
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_worker_instance_pod_mutation_authorization_mutation();

-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_authorize_unstarted_worker_instance_pod_mutation(
    p_request_uid text,
    p_actor_identity text,
    p_operation fleet_mutation_operation,
    p_kubernetes_uid text,
    p_namespace text,
    p_name text,
    p_worker_instance_id uuid,
    p_worker_instance_epoch bigint,
    p_residency_plan_revision_id uuid,
    p_worker_bundle_id uuid,
    p_worker_member_id uuid,
    p_worker_member_key text,
    p_request_digest bytea
) RETURNS TABLE (request_uid text, replayed boolean, authorized boolean)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_existing public.worker_unstarted_pod_mutation_authorizations%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_member public.worker_members%ROWTYPE;
BEGIN
    IF p_request_uid IS NULL OR length(p_request_uid) NOT BETWEEN 1 AND 200 OR btrim(p_request_uid) <> p_request_uid
       OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500 OR btrim(p_actor_identity) <> p_actor_identity
       OR p_operation IS NULL OR p_operation NOT IN ('DELETE', 'REMOVE_FINALIZER')
       OR p_kubernetes_uid IS NULL OR length(p_kubernetes_uid) NOT BETWEEN 1 AND 200 OR btrim(p_kubernetes_uid) <> p_kubernetes_uid
       OR p_namespace IS NULL OR length(p_namespace) NOT BETWEEN 1 AND 253 OR btrim(p_namespace) <> p_namespace
       OR p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 253 OR btrim(p_name) <> p_name
       OR p_worker_instance_id IS NULL OR p_worker_instance_epoch <= 0
       OR p_residency_plan_revision_id IS NULL OR p_worker_bundle_id IS NULL OR p_worker_member_id IS NULL
       OR p_worker_member_key IS NULL OR length(p_worker_member_key) NOT BETWEEN 1 AND 200 OR btrim(p_worker_member_key) <> p_worker_member_key
       OR octet_length(p_request_digest) <> 32 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', CONSTRAINT = 'unstarted_worker_pod_mutation_authorization_invalid',
            MESSAGE = 'Unstarted Worker Pod mutation authorization request is invalid';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(pg_catalog.hashtextextended(p_request_uid, 49035));
    SELECT receipt.* INTO v_existing
    FROM public.worker_unstarted_pod_mutation_authorizations AS receipt
    WHERE receipt.request_uid = p_request_uid;
    IF FOUND THEN
        IF v_existing.actor_identity <> p_actor_identity OR v_existing.operation <> p_operation
           OR v_existing.kubernetes_uid <> p_kubernetes_uid OR v_existing.namespace <> p_namespace
           OR v_existing.name <> p_name OR v_existing.worker_instance_id <> p_worker_instance_id
           OR v_existing.worker_instance_epoch <> p_worker_instance_epoch
           OR v_existing.residency_plan_revision_id <> p_residency_plan_revision_id
           OR v_existing.worker_bundle_id <> p_worker_bundle_id OR v_existing.worker_member_id <> p_worker_member_id
           OR v_existing.worker_member_key <> p_worker_member_key
           OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
            RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'unstarted_worker_pod_mutation_authorization_conflict',
                MESSAGE = 'Unstarted Worker Pod mutation request UID conflicts with existing authorization';
        END IF;
        RETURN QUERY SELECT p_request_uid, true, true;
        RETURN;
    END IF;

    SELECT worker.* INTO v_worker FROM public.worker_instances AS worker
    WHERE worker.id = p_worker_instance_id FOR UPDATE;
    SELECT member.* INTO v_member FROM public.worker_members AS member
    WHERE member.worker_instance_id = p_worker_instance_id AND member.id = p_worker_member_id FOR UPDATE;

    -- Already observed Workers continue through the established authority,
    -- including the receipt-backed pre-readiness path.
    IF v_member.id IS NOT NULL OR EXISTS (
        SELECT 1 FROM public.worker_bootstrap_claims AS claim
        WHERE claim.worker_instance_id = p_worker_instance_id
          AND claim.worker_instance_epoch = p_worker_instance_epoch
          AND claim.worker_member_id = p_worker_member_id
          AND claim.worker_bundle_id = p_worker_bundle_id
    ) THEN
        RETURN QUERY SELECT * FROM public.vela_authorize_worker_instance_pod_mutation(
            p_request_uid, p_actor_identity, p_operation, p_kubernetes_uid, p_namespace, p_name,
            p_worker_instance_id, p_worker_instance_epoch, p_residency_plan_revision_id,
            p_worker_bundle_id, p_worker_member_id, p_request_digest
        );
        RETURN;
    END IF;

    IF v_worker.id IS NULL OR v_worker.residency_plan_revision_id IS DISTINCT FROM p_residency_plan_revision_id
       OR v_worker.worker_bundle_id IS DISTINCT FROM p_worker_bundle_id
       OR v_worker.lifecycle_state <> 'FENCED'
       OR v_worker.instance_epoch <> p_worker_instance_epoch + 1
       OR EXISTS (SELECT 1 FROM public.worker_members WHERE worker_instance_id = p_worker_instance_id)
       OR EXISTS (SELECT 1 FROM public.model_residencies WHERE worker_instance_id = p_worker_instance_id)
       OR p_name <> 'wi-' || pg_catalog.replace(p_worker_instance_id::text, '-', '') || '-' ||
            pg_catalog.encode(pg_catalog.substr(pg_catalog.sha256(pg_catalog.convert_to(p_worker_member_key, 'UTF8')), 1, 4), 'hex') THEN
        RAISE EXCEPTION USING ERRCODE = '55000', CONSTRAINT = 'unstarted_worker_pod_mutation_authority_conflict',
            MESSAGE = 'Unstarted Worker Pod mutation authority is stale or conflicting';
    END IF;

    INSERT INTO public.worker_unstarted_pod_mutation_authorizations (
        request_uid, actor_identity, operation, kubernetes_uid, namespace, name,
        worker_instance_id, worker_instance_epoch, residency_plan_revision_id,
        worker_bundle_id, worker_member_id, worker_member_key, request_digest
    ) VALUES (
        p_request_uid, p_actor_identity, p_operation, p_kubernetes_uid, p_namespace, p_name,
        p_worker_instance_id, p_worker_instance_epoch, p_residency_plan_revision_id,
        p_worker_bundle_id, p_worker_member_id, p_worker_member_key, p_request_digest
    );
    RETURN QUERY SELECT p_request_uid, false, true;
END
$$;
-- +goose StatementEnd

ALTER FUNCTION vela_authorize_unstarted_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text, uuid, bigint, uuid, uuid, uuid, text, bytea
) OWNER TO vela_fleet_owner;
REVOKE ALL ON FUNCTION vela_authorize_unstarted_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text, uuid, bigint, uuid, uuid, uuid, text, bytea
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_authorize_unstarted_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text, uuid, bigint, uuid, uuid, uuid, text, bytea
) TO vela_internal;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    LOCK TABLE worker_unstarted_pod_mutation_authorizations IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM worker_unstarted_pod_mutation_authorizations) THEN
        RAISE EXCEPTION 'Cannot discard unstarted Worker Pod retirement history';
    END IF;
END $$;
-- +goose StatementEnd
DROP FUNCTION vela_authorize_unstarted_worker_instance_pod_mutation(
    text, text, fleet_mutation_operation, text, text, text, uuid, bigint, uuid, uuid, uuid, text, bytea
);
DROP TABLE worker_unstarted_pod_mutation_authorizations;
