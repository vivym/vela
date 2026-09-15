-- +goose Up
-- The reservation is immutable, but the signed authorization is produced
-- after the reservation's database-assigned reserved_at is known. Keep that
-- exact wire in a separate immutable record so a lost response or Fleet
-- restart can return the same bytes without minting a second permission.
CREATE TABLE runtime_startup_authorizations (
    request_id uuid PRIMARY KEY REFERENCES runtime_startup_reservations(request_id),
    authorization_bytes bytea NOT NULL CHECK (octet_length(authorization_bytes) BETWEEN 1 AND 65536),
    recorded_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TRIGGER runtime_startup_authorizations_immutable
BEFORE UPDATE OR DELETE OR TRUNCATE ON runtime_startup_authorizations
FOR EACH STATEMENT EXECUTE FUNCTION vela_reject_worker_bootstrap_mutation();
CREATE CONSTRAINT TRIGGER runtime_startup_authorizations_require_synchronous_quorum
AFTER INSERT ON runtime_startup_authorizations DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW EXECUTE FUNCTION vela_enforce_synchronous_quorum();

-- +goose StatementBegin
CREATE FUNCTION vela_store_runtime_startup_authorization(
    p_request uuid, p_authorization bytea
) RETURNS bytea
LANGUAGE plpgsql SECURITY DEFINER SET search_path = pg_catalog, public AS $$
DECLARE
    v_authorization bytea;
BEGIN
    IF p_request IS NULL OR p_request = '00000000-0000-0000-0000-000000000000'
       OR p_authorization IS NULL OR octet_length(p_authorization) NOT BETWEEN 1 AND 65536 THEN
        RAISE EXCEPTION USING ERRCODE = '22023', MESSAGE = 'Runtime startup authorization is invalid';
    END IF;
    INSERT INTO public.runtime_startup_authorizations(request_id, authorization_bytes)
    VALUES (p_request, p_authorization)
    ON CONFLICT (request_id) DO NOTHING;
    SELECT authorization_bytes INTO v_authorization
    FROM public.runtime_startup_authorizations
    WHERE request_id = p_request;
    IF NOT FOUND THEN
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'Runtime startup reservation authorization does not exist';
    END IF;
    RETURN v_authorization;
END
$$;
-- +goose StatementEnd

-- +goose StatementBegin
CREATE FUNCTION vela_get_runtime_startup_authorization(
    p_request uuid
) RETURNS bytea
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = pg_catalog, public AS $$
    SELECT authorization_bytes FROM public.runtime_startup_authorizations
    WHERE request_id = p_request;
$$;
-- +goose StatementEnd

ALTER TABLE runtime_startup_authorizations OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_store_runtime_startup_authorization(uuid,bytea) OWNER TO vela_fleet_owner;
ALTER FUNCTION vela_get_runtime_startup_authorization(uuid) OWNER TO vela_fleet_owner;
REVOKE ALL ON TABLE runtime_startup_authorizations FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_store_runtime_startup_authorization(uuid,bytea) FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_get_runtime_startup_authorization(uuid) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION vela_store_runtime_startup_authorization(uuid,bytea) TO vela_fleet;
GRANT EXECUTE ON FUNCTION vela_get_runtime_startup_authorization(uuid) TO vela_fleet;
GRANT SELECT ON runtime_startup_authorizations TO vela_recovery_owner;

-- +goose Down
-- +goose StatementBegin
DO $$ BEGIN
    IF EXISTS (SELECT 1 FROM runtime_startup_authorizations) THEN
        RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'Runtime startup authorization history prohibits rollback';
    END IF;
END $$;
-- +goose StatementEnd
DROP FUNCTION vela_store_runtime_startup_authorization(uuid,bytea);
DROP FUNCTION vela_get_runtime_startup_authorization(uuid);
DROP TABLE runtime_startup_authorizations;
