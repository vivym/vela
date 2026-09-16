-- +goose Up
-- Keep observed-member foreign keys intact; pre-readiness cleanup has its own
-- append-only receipt bound to an actual bootstrap claim and signed manifest.
ALTER TABLE worker_bootstrap_claims ADD CONSTRAINT worker_bootstrap_claims_retirement_identity
    UNIQUE (request_id, worker_instance_id, worker_instance_epoch, worker_member_id, worker_bundle_id);
CREATE TABLE worker_bootstrap_pod_mutation_authorizations (
    LIKE worker_instance_pod_mutation_authorizations INCLUDING ALL,
    bootstrap_request_id uuid NOT NULL,
    FOREIGN KEY (bootstrap_request_id, worker_instance_id, worker_instance_epoch, worker_member_id, worker_bundle_id)
        REFERENCES worker_bootstrap_claims(request_id, worker_instance_id, worker_instance_epoch, worker_member_id, worker_bundle_id),
    FOREIGN KEY (residency_plan_revision_id) REFERENCES residency_plan_revisions(id),
    FOREIGN KEY (worker_bundle_id) REFERENCES worker_bootstrap_manifests(worker_bundle_id)
);
ALTER TABLE worker_bootstrap_pod_mutation_authorizations OWNER TO vela_fleet_owner;
REVOKE ALL ON worker_bootstrap_pod_mutation_authorizations FROM PUBLIC;
GRANT SELECT ON worker_bootstrap_pod_mutation_authorizations TO vela_internal;
CREATE TRIGGER worker_bootstrap_pod_mutation_authorizations_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON worker_bootstrap_pod_mutation_authorizations
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_worker_instance_pod_mutation_authorization_mutation();
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_authorize_worker_instance_pod_mutation(
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
    v_existing public.worker_instance_pod_mutation_authorizations%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_member public.worker_members%ROWTYPE;
    v_member_key text;
    v_bootstrap_request_id uuid;
BEGIN
    IF p_request_uid IS NULL OR length(p_request_uid) NOT BETWEEN 1 AND 200
       OR btrim(p_request_uid) <> p_request_uid
       OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_actor_identity) <> p_actor_identity
       OR p_operation IS NULL OR p_operation NOT IN ('DELETE', 'REMOVE_FINALIZER')
       OR p_kubernetes_uid IS NULL OR length(p_kubernetes_uid) NOT BETWEEN 1 AND 200
       OR btrim(p_kubernetes_uid) <> p_kubernetes_uid
       OR p_namespace IS NULL OR length(p_namespace) NOT BETWEEN 1 AND 253
       OR btrim(p_namespace) <> p_namespace
       OR p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 253
       OR btrim(p_name) <> p_name
       OR p_worker_instance_id IS NULL OR p_worker_instance_epoch <= 0
       OR p_residency_plan_revision_id IS NULL OR p_worker_bundle_id IS NULL
       OR p_worker_member_id IS NULL OR octet_length(p_request_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_pod_mutation_authorization_invalid',
            MESSAGE = 'WorkerInstance Pod mutation authorization request is invalid';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_request_uid, 49035)
    );
    SELECT receipt.* INTO v_existing
    FROM public.worker_instance_pod_mutation_authorizations AS receipt
    WHERE receipt.request_uid = p_request_uid;
    IF NOT FOUND THEN
        SELECT receipt.request_uid, receipt.actor_identity, receipt.operation, receipt.kubernetes_uid, receipt.namespace, receipt.name, receipt.worker_instance_id, receipt.worker_instance_epoch, receipt.residency_plan_revision_id, receipt.worker_bundle_id, receipt.worker_member_id, receipt.request_digest, receipt.authorized_at INTO v_existing
        FROM public.worker_bootstrap_pod_mutation_authorizations AS receipt
        WHERE receipt.request_uid = p_request_uid;
    END IF;
    IF v_existing.request_uid IS NOT NULL THEN
        IF v_existing.actor_identity <> p_actor_identity
           OR v_existing.operation <> p_operation
           OR v_existing.kubernetes_uid <> p_kubernetes_uid
           OR v_existing.namespace <> p_namespace
           OR v_existing.name <> p_name
           OR v_existing.worker_instance_id <> p_worker_instance_id
           OR v_existing.worker_instance_epoch <> p_worker_instance_epoch
           OR v_existing.residency_plan_revision_id <> p_residency_plan_revision_id
           OR v_existing.worker_bundle_id <> p_worker_bundle_id
           OR v_existing.worker_member_id <> p_worker_member_id
           OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'worker_instance_pod_mutation_authorization_conflict',
                MESSAGE = 'WorkerInstance Pod mutation request UID conflicts with existing authorization';
        END IF;
        RETURN QUERY SELECT p_request_uid, true, true;
        RETURN;
    END IF;

    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = p_worker_instance_id
    FOR UPDATE;
    SELECT member.* INTO v_member
    FROM public.worker_members AS member
    WHERE member.worker_instance_id = p_worker_instance_id
      AND member.id = p_worker_member_id
    FOR UPDATE;
    v_member_key := v_member.member_key;
    IF v_member.id IS NULL AND v_worker.lifecycle_state = 'FENCED'
       AND v_worker.instance_epoch = p_worker_instance_epoch + 1
       AND NOT EXISTS (SELECT 1 FROM public.worker_members WHERE worker_instance_id = p_worker_instance_id)
       AND NOT EXISTS (SELECT 1 FROM public.model_residencies WHERE worker_instance_id = p_worker_instance_id) THEN
        -- A pre-readiness member is a declaration in the immutable bootstrap
        -- manifest, never an invented observed worker_members row.
        SELECT claim.request_id, member.value ->> 'key'
        INTO v_bootstrap_request_id, v_member_key
        FROM public.worker_bootstrap_claims AS claim
        JOIN public.worker_bootstrap_receipts AS receipt USING (request_id)
        JOIN public.worker_bootstrap_manifests AS published USING (worker_bundle_id)
        CROSS JOIN LATERAL (SELECT convert_from(published.manifest, 'UTF8')::jsonb -> 'bundle' AS value) AS manifest
        CROSS JOIN LATERAL jsonb_array_elements(manifest.value -> 'worker_instances') AS worker
        CROSS JOIN LATERAL jsonb_array_elements(worker.value -> 'members') AS member
        WHERE claim.worker_instance_id = p_worker_instance_id
          AND claim.worker_instance_epoch = p_worker_instance_epoch
          AND claim.worker_member_id = p_worker_member_id
          AND claim.worker_bundle_id = p_worker_bundle_id
          AND convert_from(published.manifest, 'UTF8')::jsonb ->> 'schema' = 'vela.worker-bundle-actuation/v2'
          AND manifest.value ->> 'namespace' = p_namespace
          AND manifest.value ->> 'plan_revision_id' = p_residency_plan_revision_id::text
          AND manifest.value ->> 'worker_bundle_id' = p_worker_bundle_id::text
          AND manifest.value ->> 'runtime_launch_protocol' = 'kubernetes-pidfd-v1'
          AND worker.value ->> 'id' = p_worker_instance_id::text
          AND (worker.value ->> 'instance_epoch')::bigint = p_worker_instance_epoch
          AND member.value ->> 'id' = p_worker_member_id::text
          AND (member.value ->> 'member_epoch')::bigint = claim.worker_member_epoch
          AND member.value ->> 'node_identity' = claim.node_identity;
    END IF;
    IF v_worker.id IS NULL OR v_member_key IS NULL
       OR v_worker.residency_plan_revision_id IS DISTINCT FROM p_residency_plan_revision_id
       OR v_worker.worker_bundle_id IS DISTINCT FROM p_worker_bundle_id
       OR (v_bootstrap_request_id IS NULL AND (
           v_member.worker_instance_epoch <> p_worker_instance_epoch
           OR v_member.worker_bundle_id IS DISTINCT FROM p_worker_bundle_id))
       OR p_name <> 'wi-'
            || pg_catalog.replace(p_worker_instance_id::text, '-', '')
            || '-'
            || pg_catalog.encode(
                pg_catalog.substr(
                    pg_catalog.sha256(
                        pg_catalog.convert_to(v_member_key, 'UTF8')
                    ),
                    1,
                    4
                ),
                'hex'
            ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_pod_mutation_authority_conflict',
            MESSAGE = 'WorkerInstance Pod mutation authority is stale or conflicting';
    END IF;

    IF v_worker.lifecycle_state = 'DRAINING'
       AND v_worker.instance_epoch = p_worker_instance_epoch THEN
        IF NOT EXISTS (
            SELECT 1
            FROM public.worker_instance_drain_operations AS drain
            WHERE drain.worker_instance_id = p_worker_instance_id
              AND drain.worker_instance_epoch = p_worker_instance_epoch
        ) OR NOT EXISTS (
            SELECT 1
            FROM public.model_residencies AS residency
            WHERE residency.worker_instance_id = p_worker_instance_id
              AND residency.worker_instance_epoch = p_worker_instance_epoch
        ) OR EXISTS (
            SELECT 1
            FROM public.model_residencies AS residency
            WHERE residency.worker_instance_id = p_worker_instance_id
              AND residency.worker_instance_epoch = p_worker_instance_epoch
              AND residency.state <> 'RELEASED'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'worker_instance_pod_mutation_release_incomplete',
                MESSAGE = 'WorkerInstance Pod mutation requires drain and complete residency release';
        END IF;
    ELSIF NOT (
        v_worker.lifecycle_state = 'FENCED'
        AND v_worker.instance_epoch = p_worker_instance_epoch + 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_pod_mutation_lifecycle_conflict',
            MESSAGE = 'WorkerInstance Pod mutation requires released drain or fenced old epoch';
    END IF;

    IF v_bootstrap_request_id IS NOT NULL THEN
        INSERT INTO public.worker_bootstrap_pod_mutation_authorizations (
            request_uid, actor_identity, operation, kubernetes_uid, namespace, name,
            worker_instance_id, worker_instance_epoch, residency_plan_revision_id,
            worker_bundle_id, worker_member_id, request_digest, bootstrap_request_id
        ) VALUES (
            p_request_uid, p_actor_identity, p_operation, p_kubernetes_uid,
            p_namespace, p_name, p_worker_instance_id, p_worker_instance_epoch,
            p_residency_plan_revision_id, p_worker_bundle_id, p_worker_member_id,
            p_request_digest, v_bootstrap_request_id
        );
    ELSE
        INSERT INTO public.worker_instance_pod_mutation_authorizations (
            request_uid, actor_identity, operation, kubernetes_uid, namespace, name,
            worker_instance_id, worker_instance_epoch, residency_plan_revision_id,
            worker_bundle_id, worker_member_id, request_digest
        ) VALUES (
            p_request_uid, p_actor_identity, p_operation, p_kubernetes_uid,
            p_namespace, p_name, p_worker_instance_id, p_worker_instance_epoch,
            p_residency_plan_revision_id, p_worker_bundle_id, p_worker_member_id,
            p_request_digest
        );
    END IF;
    RETURN QUERY SELECT p_request_uid, false, true;
END
$$;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    LOCK TABLE worker_bootstrap_pod_mutation_authorizations IN ACCESS EXCLUSIVE MODE;
    IF EXISTS (SELECT 1 FROM worker_bootstrap_pod_mutation_authorizations) THEN
        RAISE EXCEPTION 'Cannot discard pre-readiness Pod retirement history';
    END IF;
END $$;
-- +goose StatementEnd
-- +goose StatementBegin
CREATE OR REPLACE FUNCTION vela_authorize_worker_instance_pod_mutation(
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
    v_existing public.worker_instance_pod_mutation_authorizations%ROWTYPE;
    v_worker public.worker_instances%ROWTYPE;
    v_member public.worker_members%ROWTYPE;
BEGIN
    IF p_request_uid IS NULL OR length(p_request_uid) NOT BETWEEN 1 AND 200
       OR btrim(p_request_uid) <> p_request_uid
       OR p_actor_identity IS NULL OR length(p_actor_identity) NOT BETWEEN 1 AND 500
       OR btrim(p_actor_identity) <> p_actor_identity
       OR p_operation IS NULL OR p_operation NOT IN ('DELETE', 'REMOVE_FINALIZER')
       OR p_kubernetes_uid IS NULL OR length(p_kubernetes_uid) NOT BETWEEN 1 AND 200
       OR btrim(p_kubernetes_uid) <> p_kubernetes_uid
       OR p_namespace IS NULL OR length(p_namespace) NOT BETWEEN 1 AND 253
       OR btrim(p_namespace) <> p_namespace
       OR p_name IS NULL OR length(p_name) NOT BETWEEN 1 AND 253
       OR btrim(p_name) <> p_name
       OR p_worker_instance_id IS NULL OR p_worker_instance_epoch <= 0
       OR p_residency_plan_revision_id IS NULL OR p_worker_bundle_id IS NULL
       OR p_worker_member_id IS NULL OR octet_length(p_request_digest) <> 32 THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'worker_instance_pod_mutation_authorization_invalid',
            MESSAGE = 'WorkerInstance Pod mutation authorization request is invalid';
    END IF;

    PERFORM pg_catalog.pg_advisory_xact_lock(
        pg_catalog.hashtextextended(p_request_uid, 49035)
    );
    SELECT receipt.* INTO v_existing
    FROM public.worker_instance_pod_mutation_authorizations AS receipt
    WHERE receipt.request_uid = p_request_uid;
    IF FOUND THEN
        IF v_existing.actor_identity <> p_actor_identity
           OR v_existing.operation <> p_operation
           OR v_existing.kubernetes_uid <> p_kubernetes_uid
           OR v_existing.namespace <> p_namespace
           OR v_existing.name <> p_name
           OR v_existing.worker_instance_id <> p_worker_instance_id
           OR v_existing.worker_instance_epoch <> p_worker_instance_epoch
           OR v_existing.residency_plan_revision_id <> p_residency_plan_revision_id
           OR v_existing.worker_bundle_id <> p_worker_bundle_id
           OR v_existing.worker_member_id <> p_worker_member_id
           OR v_existing.request_digest IS DISTINCT FROM p_request_digest THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'worker_instance_pod_mutation_authorization_conflict',
                MESSAGE = 'WorkerInstance Pod mutation request UID conflicts with existing authorization';
        END IF;
        RETURN QUERY SELECT p_request_uid, true, true;
        RETURN;
    END IF;

    SELECT worker.* INTO v_worker
    FROM public.worker_instances AS worker
    WHERE worker.id = p_worker_instance_id
    FOR UPDATE;
    SELECT member.* INTO v_member
    FROM public.worker_members AS member
    WHERE member.worker_instance_id = p_worker_instance_id
      AND member.id = p_worker_member_id
    FOR UPDATE;
    IF v_worker.id IS NULL OR v_member.id IS NULL
       OR v_worker.residency_plan_revision_id IS DISTINCT FROM p_residency_plan_revision_id
       OR v_worker.worker_bundle_id IS DISTINCT FROM p_worker_bundle_id
       OR v_member.worker_instance_epoch <> p_worker_instance_epoch
       OR v_member.worker_bundle_id IS DISTINCT FROM p_worker_bundle_id
       OR p_name <> 'wi-'
            || pg_catalog.replace(p_worker_instance_id::text, '-', '')
            || '-'
            || pg_catalog.encode(
                pg_catalog.substr(
                    pg_catalog.sha256(
                        pg_catalog.convert_to(v_member.member_key, 'UTF8')
                    ),
                    1,
                    4
                ),
                'hex'
            ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_pod_mutation_authority_conflict',
            MESSAGE = 'WorkerInstance Pod mutation authority is stale or conflicting';
    END IF;

    IF v_worker.lifecycle_state = 'DRAINING'
       AND v_worker.instance_epoch = p_worker_instance_epoch THEN
        IF NOT EXISTS (
            SELECT 1
            FROM public.worker_instance_drain_operations AS drain
            WHERE drain.worker_instance_id = p_worker_instance_id
              AND drain.worker_instance_epoch = p_worker_instance_epoch
        ) OR NOT EXISTS (
            SELECT 1
            FROM public.model_residencies AS residency
            WHERE residency.worker_instance_id = p_worker_instance_id
              AND residency.worker_instance_epoch = p_worker_instance_epoch
        ) OR EXISTS (
            SELECT 1
            FROM public.model_residencies AS residency
            WHERE residency.worker_instance_id = p_worker_instance_id
              AND residency.worker_instance_epoch = p_worker_instance_epoch
              AND residency.state <> 'RELEASED'
        ) THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'worker_instance_pod_mutation_release_incomplete',
                MESSAGE = 'WorkerInstance Pod mutation requires drain and complete residency release';
        END IF;
    ELSIF NOT (
        v_worker.lifecycle_state = 'FENCED'
        AND v_worker.instance_epoch = p_worker_instance_epoch + 1
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'worker_instance_pod_mutation_lifecycle_conflict',
            MESSAGE = 'WorkerInstance Pod mutation requires released drain or fenced old epoch';
    END IF;

    INSERT INTO public.worker_instance_pod_mutation_authorizations (
        request_uid, actor_identity, operation, kubernetes_uid, namespace, name,
        worker_instance_id, worker_instance_epoch, residency_plan_revision_id,
        worker_bundle_id, worker_member_id, request_digest
    ) VALUES (
        p_request_uid, p_actor_identity, p_operation, p_kubernetes_uid,
        p_namespace, p_name, p_worker_instance_id, p_worker_instance_epoch,
        p_residency_plan_revision_id, p_worker_bundle_id, p_worker_member_id,
        p_request_digest
    );
    RETURN QUERY SELECT p_request_uid, false, true;
END
$$;
-- +goose StatementEnd
DROP TABLE worker_bootstrap_pod_mutation_authorizations;
ALTER TABLE worker_bootstrap_claims DROP CONSTRAINT worker_bootstrap_claims_retirement_identity;
