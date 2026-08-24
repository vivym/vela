-- +goose Up
-- +goose StatementBegin
CREATE TYPE finance_reconciliation_kind AS ENUM (
    'SETTLEMENT_POSTED',
    'CREDIT_ADJUSTMENT_POSTED',
    'CONTRACT_CREDIT_LIMIT_CHANGED'
);

CREATE TABLE finance_reconciliation_principals (
    id uuid PRIMARY KEY,
    stable_id text NOT NULL UNIQUE
        CHECK (length(stable_id) BETWEEN 1 AND 200 AND btrim(stable_id) = stable_id),
    tls_uri_identity text NOT NULL UNIQUE
        CHECK (
            length(tls_uri_identity) BETWEEN 1 AND 500
            AND btrim(tls_uri_identity) = tls_uri_identity
            AND tls_uri_identity !~ '[[:cntrl:][:space:]]'
        ),
    status text NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE', 'DISABLED')),
    disabled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    CHECK (
        (status = 'ACTIVE' AND disabled_at IS NULL)
        OR (status = 'DISABLED' AND disabled_at IS NOT NULL)
    )
);

CREATE TABLE finance_reconciliation_database_bindings (
    database_role name PRIMARY KEY,
    principal_id uuid NOT NULL REFERENCES finance_reconciliation_principals(id),
    disabled_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE finance_reconciliation_cursors (
    principal_id uuid PRIMARY KEY REFERENCES finance_reconciliation_principals(id),
    last_sequence bigint NOT NULL DEFAULT 0 CHECK (last_sequence >= 0),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp()
);

CREATE TABLE finance_reconciliation_records (
    id uuid PRIMARY KEY,
    principal_id uuid NOT NULL REFERENCES finance_reconciliation_principals(id),
    idempotency_key text NOT NULL,
    source_sequence bigint NOT NULL CHECK (source_sequence > 0),
    organization_id uuid NOT NULL REFERENCES customer_organizations(id),
    kind finance_reconciliation_kind NOT NULL,
    currency text NOT NULL CHECK (currency ~ '^[A-Z]{3}$'),
    settlement_minor bigint,
    credit_adjustment_minor bigint,
    contract_credit_limit_minor bigint,
    external_reference text NOT NULL,
    effective_at timestamptz NOT NULL,
    contract_credit_limit_minor_before bigint NOT NULL CHECK (contract_credit_limit_minor_before >= 0),
    contract_credit_limit_minor_after bigint NOT NULL CHECK (contract_credit_limit_minor_after >= 0),
    reserved_minor_at_posting bigint NOT NULL CHECK (reserved_minor_at_posting >= 0),
    unsettled_posted_minor_before bigint NOT NULL CHECK (unsettled_posted_minor_before >= 0),
    unsettled_posted_minor_after bigint NOT NULL CHECK (unsettled_posted_minor_after >= 0),
    account_version_before bigint NOT NULL CHECK (account_version_before > 0),
    account_version_after bigint NOT NULL CHECK (account_version_after > account_version_before),
    posted_at timestamptz NOT NULL,
    UNIQUE (principal_id, idempotency_key),
    UNIQUE (principal_id, source_sequence),
    UNIQUE (principal_id, external_reference),
    CHECK (length(idempotency_key) BETWEEN 1 AND 500 AND btrim(idempotency_key) = idempotency_key),
    CHECK (idempotency_key !~ '[[:cntrl:]]'),
    CHECK (
        length(external_reference) BETWEEN 1 AND 500
        AND btrim(external_reference) = external_reference
        AND external_reference !~ '[[:cntrl:]]'
    ),
    CHECK (
        (kind = 'SETTLEMENT_POSTED'
            AND settlement_minor > 0
            AND credit_adjustment_minor IS NULL
            AND contract_credit_limit_minor IS NULL)
        OR (kind = 'CREDIT_ADJUSTMENT_POSTED'
            AND settlement_minor IS NULL
            AND credit_adjustment_minor <> 0
            AND contract_credit_limit_minor IS NULL)
        OR (kind = 'CONTRACT_CREDIT_LIMIT_CHANGED'
            AND settlement_minor IS NULL
            AND credit_adjustment_minor IS NULL
            AND contract_credit_limit_minor >= 0)
    ),
    CHECK (
        reserved_minor_at_posting::numeric + unsettled_posted_minor_after::numeric
        <= contract_credit_limit_minor_after::numeric
    )
);

CREATE INDEX finance_reconciliation_records_organization_idx
    ON finance_reconciliation_records (organization_id, posted_at DESC, id DESC);

ALTER TABLE finance_reconciliation_principals ENABLE ROW LEVEL SECURITY;
ALTER TABLE finance_reconciliation_principals FORCE ROW LEVEL SECURITY;
ALTER TABLE finance_reconciliation_database_bindings ENABLE ROW LEVEL SECURITY;
ALTER TABLE finance_reconciliation_database_bindings FORCE ROW LEVEL SECURITY;
ALTER TABLE finance_reconciliation_cursors ENABLE ROW LEVEL SECURITY;
ALTER TABLE finance_reconciliation_cursors FORCE ROW LEVEL SECURITY;
ALTER TABLE finance_reconciliation_records ENABLE ROW LEVEL SECURITY;
ALTER TABLE finance_reconciliation_records FORCE ROW LEVEL SECURITY;

CREATE FUNCTION vela_reject_finance_reconciliation_principal_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        RAISE EXCEPTION 'Finance Reconciliation Principal identity is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'finance_reconciliation_principal_identity_is_immutable';
    END IF;
    IF NEW.id IS DISTINCT FROM OLD.id
        OR NEW.stable_id IS DISTINCT FROM OLD.stable_id
        OR NEW.tls_uri_identity IS DISTINCT FROM OLD.tls_uri_identity
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
    THEN
        RAISE EXCEPTION 'Finance Reconciliation Principal identity is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'finance_reconciliation_principal_identity_is_immutable';
    END IF;
    IF NEW.status IS DISTINCT FROM OLD.status OR NEW.disabled_at IS DISTINCT FROM OLD.disabled_at THEN
        IF NOT (
            OLD.status = 'ACTIVE'
            AND OLD.disabled_at IS NULL
            AND NEW.status = 'DISABLED'
            AND NEW.disabled_at IS NOT NULL
        ) THEN
            RAISE EXCEPTION 'Finance Reconciliation Principal disablement is permanent'
                USING ERRCODE = '55000',
                    CONSTRAINT = 'finance_reconciliation_principal_disablement_is_permanent';
        END IF;
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_finance_reconciliation_principal_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_finance_reconciliation_principal_mutation()
    OWNER TO vela_finance_reconciliation_owner;

CREATE TRIGGER finance_reconciliation_principals_immutable
BEFORE UPDATE OR DELETE ON finance_reconciliation_principals
FOR EACH ROW EXECUTE FUNCTION vela_reject_finance_reconciliation_principal_mutation();

CREATE FUNCTION vela_reject_finance_reconciliation_binding_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF TG_OP = 'DELETE'
        OR NEW.database_role IS DISTINCT FROM OLD.database_role
        OR NEW.principal_id IS DISTINCT FROM OLD.principal_id
        OR NEW.created_at IS DISTINCT FROM OLD.created_at
        OR (
            NEW.disabled_at IS DISTINCT FROM OLD.disabled_at
            AND NOT (OLD.disabled_at IS NULL AND NEW.disabled_at IS NOT NULL)
        )
    THEN
        RAISE EXCEPTION 'Finance Reconciliation database binding is immutable'
            USING ERRCODE = '55000',
                CONSTRAINT = 'finance_reconciliation_database_binding_is_immutable';
    END IF;
    RETURN NEW;
END
$$;
REVOKE ALL ON FUNCTION vela_reject_finance_reconciliation_binding_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_finance_reconciliation_binding_mutation()
    OWNER TO vela_finance_reconciliation_owner;

CREATE TRIGGER finance_reconciliation_database_bindings_immutable
BEFORE UPDATE OR DELETE ON finance_reconciliation_database_bindings
FOR EACH ROW EXECUTE FUNCTION vela_reject_finance_reconciliation_binding_mutation();

CREATE FUNCTION vela_reject_finance_reconciliation_record_mutation() RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'Finance Reconciliation records are immutable'
        USING ERRCODE = '55000',
            CONSTRAINT = 'finance_reconciliation_record_is_immutable';
END
$$;
REVOKE ALL ON FUNCTION vela_reject_finance_reconciliation_record_mutation() FROM PUBLIC;
ALTER FUNCTION vela_reject_finance_reconciliation_record_mutation()
    OWNER TO vela_finance_reconciliation_owner;

CREATE TRIGGER finance_reconciliation_records_immutable
BEFORE UPDATE OR DELETE ON finance_reconciliation_records
FOR EACH ROW EXECUTE FUNCTION vela_reject_finance_reconciliation_record_mutation();

CREATE FUNCTION vela_private.require_finance_reconciliation_identity()
RETURNS TABLE (
    principal_id uuid,
    stable_id text,
    tls_uri_identity text
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    RETURN QUERY
    SELECT principal.id, principal.stable_id, principal.tls_uri_identity
    FROM public.finance_reconciliation_database_bindings AS binding
    JOIN public.finance_reconciliation_principals AS principal
      ON principal.id = binding.principal_id
    WHERE binding.database_role = session_user::name
      AND binding.disabled_at IS NULL
      AND principal.status = 'ACTIVE'
      AND principal.disabled_at IS NULL;

    IF NOT FOUND THEN
        RAISE EXCEPTION 'active Finance Reconciliation Principal binding is required'
            USING ERRCODE = '28000';
    END IF;
END
$$;
REVOKE ALL ON FUNCTION vela_private.require_finance_reconciliation_identity() FROM PUBLIC;
ALTER FUNCTION vela_private.require_finance_reconciliation_identity()
    OWNER TO vela_finance_reconciliation_owner;

CREATE FUNCTION vela_get_finance_reconciliation_identity()
RETURNS TABLE (
    principal_id uuid,
    stable_id text,
    tls_uri_identity text
)
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
    SELECT identity.principal_id, identity.stable_id, identity.tls_uri_identity
    FROM vela_private.require_finance_reconciliation_identity() AS identity
$$;
REVOKE ALL ON FUNCTION vela_get_finance_reconciliation_identity() FROM PUBLIC;
ALTER FUNCTION vela_get_finance_reconciliation_identity()
    OWNER TO vela_finance_reconciliation_owner;

CREATE FUNCTION vela_apply_finance_reconciliation(
    p_record_id uuid,
    p_idempotency_key text,
    p_source_sequence bigint,
    p_organization_id uuid,
    p_kind finance_reconciliation_kind,
    p_currency text,
    p_settlement_minor bigint,
    p_credit_adjustment_minor bigint,
    p_contract_credit_limit_minor bigint,
    p_external_reference text,
    p_effective_at timestamptz
) RETURNS TABLE (
    record_id uuid,
    replayed boolean,
    organization_id uuid,
    kind finance_reconciliation_kind,
    currency text,
    contract_credit_limit_minor bigint,
    unsettled_posted_minor bigint,
    account_version bigint,
    posted_at timestamptz
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_identity record;
    v_existing public.finance_reconciliation_records%ROWTYPE;
    v_account public.organization_credit_accounts%ROWTYPE;
    v_last_sequence bigint;
    v_limit_after bigint;
    v_unsettled_after bigint;
    v_unsettled_after_numeric numeric;
    v_posted_at timestamptz;
BEGIN
    SELECT * INTO STRICT v_identity
    FROM vela_private.require_finance_reconciliation_identity();

    IF p_record_id IS NULL
        OR p_source_sequence IS NULL OR p_source_sequence <= 0
        OR p_organization_id IS NULL
        OR p_kind IS NULL
        OR p_currency IS NULL OR p_currency !~ '^[A-Z]{3}$'
        OR p_idempotency_key IS NULL
        OR length(p_idempotency_key) NOT BETWEEN 1 AND 500
        OR btrim(p_idempotency_key) <> p_idempotency_key
        OR p_idempotency_key ~ '[[:cntrl:]]'
        OR p_external_reference IS NULL
        OR length(p_external_reference) NOT BETWEEN 1 AND 500
        OR btrim(p_external_reference) <> p_external_reference
        OR p_external_reference ~ '[[:cntrl:]]'
        OR p_effective_at IS NULL
    THEN
        RAISE EXCEPTION 'invalid Finance Reconciliation input' USING ERRCODE = '22023';
    END IF;

    INSERT INTO public.finance_reconciliation_cursors (principal_id)
    VALUES (v_identity.principal_id)
    ON CONFLICT (principal_id) DO NOTHING;

    SELECT cursor.last_sequence INTO STRICT v_last_sequence
    FROM public.finance_reconciliation_cursors AS cursor
    WHERE cursor.principal_id = v_identity.principal_id
        FOR UPDATE;

    SELECT * INTO v_existing
    FROM public.finance_reconciliation_records AS reconciliation
    WHERE reconciliation.principal_id = v_identity.principal_id
      AND reconciliation.idempotency_key = p_idempotency_key;
    IF FOUND THEN
        IF v_existing.source_sequence IS DISTINCT FROM p_source_sequence
            OR v_existing.organization_id IS DISTINCT FROM p_organization_id
            OR v_existing.kind IS DISTINCT FROM p_kind
            OR v_existing.currency IS DISTINCT FROM p_currency
            OR v_existing.settlement_minor IS DISTINCT FROM p_settlement_minor
            OR v_existing.credit_adjustment_minor IS DISTINCT FROM p_credit_adjustment_minor
            OR v_existing.contract_credit_limit_minor IS DISTINCT FROM p_contract_credit_limit_minor
            OR v_existing.external_reference IS DISTINCT FROM p_external_reference
            OR v_existing.effective_at IS DISTINCT FROM p_effective_at
        THEN
            RAISE EXCEPTION 'Finance Reconciliation idempotency key conflicts with committed input'
                USING ERRCODE = 'P0001',
                    CONSTRAINT = 'finance_reconciliation_idempotency_conflict';
        END IF;
        RETURN QUERY SELECT
            v_existing.id,
            true,
            v_existing.organization_id,
            v_existing.kind,
            v_existing.currency,
            v_existing.contract_credit_limit_minor_after,
            v_existing.unsettled_posted_minor_after,
            v_existing.account_version_after,
            v_existing.posted_at;
        RETURN;
    END IF;

    IF v_last_sequence = 9223372036854775807 THEN
        RAISE EXCEPTION 'Finance Reconciliation source sequence cannot advance'
            USING ERRCODE = 'P0001',
                CONSTRAINT = 'finance_reconciliation_sequence_exhausted';
    END IF;

    IF p_source_sequence <> v_last_sequence + 1 THEN
        RAISE EXCEPTION 'Finance Reconciliation source sequence is out of order'
            USING ERRCODE = 'P0001',
                CONSTRAINT = 'finance_reconciliation_sequence_out_of_order';
    END IF;
    IF EXISTS (
        SELECT 1
        FROM public.finance_reconciliation_records AS reconciliation
        WHERE reconciliation.principal_id = v_identity.principal_id
          AND reconciliation.external_reference = p_external_reference
    ) THEN
        RAISE EXCEPTION 'Finance Reconciliation external reference conflicts with committed input'
            USING ERRCODE = 'P0001',
                CONSTRAINT = 'finance_reconciliation_external_reference_conflict';
    END IF;

    SELECT * INTO v_account
    FROM public.organization_credit_accounts AS account
    WHERE account.organization_id = p_organization_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'Customer Organization credit account is not found'
            USING ERRCODE = 'P0002';
    END IF;
    IF v_account.currency <> p_currency THEN
        RAISE EXCEPTION 'Finance Reconciliation currency does not match credit account'
            USING ERRCODE = '22023';
    END IF;
    IF v_account.version = 9223372036854775807 THEN
        RAISE EXCEPTION 'credit account version cannot advance'
            USING ERRCODE = 'P0001',
                CONSTRAINT = 'finance_reconciliation_account_version_exhausted';
    END IF;

    v_limit_after := v_account.contract_credit_limit_minor;
    v_unsettled_after := v_account.unsettled_posted_minor;

    IF p_kind = 'SETTLEMENT_POSTED'
        AND p_settlement_minor IS NOT NULL AND p_settlement_minor > 0
        AND p_credit_adjustment_minor IS NULL
        AND p_contract_credit_limit_minor IS NULL
    THEN
        IF p_settlement_minor > v_account.unsettled_posted_minor THEN
            RAISE EXCEPTION 'settlement exceeds unsettled posted credit'
                USING ERRCODE = 'P0001',
                    CONSTRAINT = 'finance_reconciliation_over_application';
        END IF;
        v_unsettled_after := v_account.unsettled_posted_minor - p_settlement_minor;
    ELSIF p_kind = 'CREDIT_ADJUSTMENT_POSTED'
        AND p_settlement_minor IS NULL
        AND p_credit_adjustment_minor IS NOT NULL AND p_credit_adjustment_minor <> 0
        AND p_contract_credit_limit_minor IS NULL
    THEN
        v_unsettled_after_numeric :=
            v_account.unsettled_posted_minor::numeric - p_credit_adjustment_minor::numeric;
        IF v_unsettled_after_numeric < 0 THEN
            RAISE EXCEPTION 'credit adjustment exceeds unsettled posted credit'
                USING ERRCODE = 'P0001',
                    CONSTRAINT = 'finance_reconciliation_over_application';
        END IF;
        IF v_unsettled_after_numeric > 9223372036854775807::numeric
            OR v_account.reserved_minor::numeric + v_unsettled_after_numeric
                > v_account.contract_credit_limit_minor::numeric
        THEN
            RAISE EXCEPTION 'credit adjustment exceeds Contract Credit Limit'
                USING ERRCODE = 'P0001',
                    CONSTRAINT = 'finance_reconciliation_credit_limit_exceeded';
        END IF;
        v_unsettled_after := v_unsettled_after_numeric::bigint;
    ELSIF p_kind = 'CONTRACT_CREDIT_LIMIT_CHANGED'
        AND p_settlement_minor IS NULL
        AND p_credit_adjustment_minor IS NULL
        AND p_contract_credit_limit_minor IS NOT NULL
        AND p_contract_credit_limit_minor >= 0
    THEN
        IF v_account.reserved_minor::numeric + v_account.unsettled_posted_minor::numeric
            > p_contract_credit_limit_minor::numeric
        THEN
            RAISE EXCEPTION 'Contract Credit Limit is below committed credit usage'
                USING ERRCODE = 'P0001',
                    CONSTRAINT = 'finance_reconciliation_credit_limit_exceeded';
        END IF;
        v_limit_after := p_contract_credit_limit_minor;
    ELSE
        RAISE EXCEPTION 'unsupported Finance Reconciliation kind or amount shape'
            USING ERRCODE = '22023';
    END IF;
    v_posted_at := clock_timestamp();

    INSERT INTO public.finance_reconciliation_records (
        id,
        principal_id,
        idempotency_key,
        source_sequence,
        organization_id,
        kind,
        currency,
        settlement_minor,
        credit_adjustment_minor,
        contract_credit_limit_minor,
        external_reference,
        effective_at,
        contract_credit_limit_minor_before,
        contract_credit_limit_minor_after,
        reserved_minor_at_posting,
        unsettled_posted_minor_before,
        unsettled_posted_minor_after,
        account_version_before,
        account_version_after,
        posted_at
    ) VALUES (
        p_record_id,
        v_identity.principal_id,
        p_idempotency_key,
        p_source_sequence,
        p_organization_id,
        p_kind,
        p_currency,
        p_settlement_minor,
        p_credit_adjustment_minor,
        p_contract_credit_limit_minor,
        p_external_reference,
        p_effective_at,
        v_account.contract_credit_limit_minor,
        v_limit_after,
        v_account.reserved_minor,
        v_account.unsettled_posted_minor,
        v_unsettled_after,
        v_account.version,
        v_account.version + 1,
        v_posted_at
    );

    UPDATE public.organization_credit_accounts AS account
    SET contract_credit_limit_minor = v_limit_after,
        unsettled_posted_minor = v_unsettled_after,
        version = v_account.version + 1,
        updated_at = v_posted_at
    WHERE account.organization_id = p_organization_id;

    UPDATE public.finance_reconciliation_cursors AS cursor
    SET last_sequence = p_source_sequence,
        updated_at = v_posted_at
    WHERE cursor.principal_id = v_identity.principal_id;

    RETURN QUERY SELECT
        p_record_id,
        false,
        p_organization_id,
        p_kind,
        p_currency,
        v_limit_after,
        v_unsettled_after,
        v_account.version + 1,
        v_posted_at;
END
$$;
REVOKE ALL ON FUNCTION vela_apply_finance_reconciliation(
    uuid, text, bigint, uuid, finance_reconciliation_kind, text,
    bigint, bigint, bigint, text, timestamptz
) FROM PUBLIC;
ALTER FUNCTION vela_apply_finance_reconciliation(
    uuid, text, bigint, uuid, finance_reconciliation_kind, text,
    bigint, bigint, bigint, text, timestamptz
) OWNER TO vela_finance_reconciliation_owner;

GRANT USAGE ON SCHEMA public, vela_private TO vela_finance_reconciliation_owner;
GRANT USAGE ON TYPE finance_reconciliation_kind TO vela_finance_reconciliation_owner;
GRANT SELECT ON finance_reconciliation_principals,
    finance_reconciliation_database_bindings,
    finance_reconciliation_cursors,
    finance_reconciliation_records,
    organization_credit_accounts
TO vela_finance_reconciliation_owner;
GRANT INSERT ON finance_reconciliation_cursors,
    finance_reconciliation_records TO vela_finance_reconciliation_owner;
GRANT UPDATE ON finance_reconciliation_cursors TO vela_finance_reconciliation_owner;
GRANT UPDATE (
    contract_credit_limit_minor,
    unsettled_posted_minor,
    version,
    updated_at
) ON organization_credit_accounts TO vela_finance_reconciliation_owner;
GRANT EXECUTE ON FUNCTION vela_private.require_finance_reconciliation_identity()
    TO vela_finance_reconciliation_owner;

GRANT USAGE ON SCHEMA public TO vela_finance_reconciliation;
GRANT EXECUTE ON FUNCTION vela_get_finance_reconciliation_identity()
    TO vela_finance_reconciliation;
GRANT EXECUTE ON FUNCTION vela_apply_finance_reconciliation(
    uuid, text, bigint, uuid, finance_reconciliation_kind, text,
    bigint, bigint, bigint, text, timestamptz
) TO vela_finance_reconciliation;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
LOCK TABLE finance_reconciliation_records,
    finance_reconciliation_cursors,
    finance_reconciliation_database_bindings,
    finance_reconciliation_principals IN ACCESS EXCLUSIVE MODE;

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM finance_reconciliation_records)
        OR EXISTS (SELECT 1 FROM finance_reconciliation_cursors)
        OR EXISTS (SELECT 1 FROM finance_reconciliation_database_bindings)
        OR EXISTS (SELECT 1 FROM finance_reconciliation_principals)
    THEN
        RAISE EXCEPTION 'cannot remove Finance Reconciliation with durable evidence or provisioning'
            USING ERRCODE = '55000',
                CONSTRAINT = 'finance_reconciliation_contract_has_durable_evidence';
    END IF;
END
$$;

REVOKE EXECUTE ON FUNCTION vela_apply_finance_reconciliation(
    uuid, text, bigint, uuid, finance_reconciliation_kind, text,
    bigint, bigint, bigint, text, timestamptz
) FROM vela_finance_reconciliation;
REVOKE EXECUTE ON FUNCTION vela_get_finance_reconciliation_identity()
    FROM vela_finance_reconciliation;
REVOKE USAGE ON SCHEMA public FROM vela_finance_reconciliation;

DROP FUNCTION vela_apply_finance_reconciliation(
    uuid, text, bigint, uuid, finance_reconciliation_kind, text,
    bigint, bigint, bigint, text, timestamptz
);
DROP FUNCTION vela_get_finance_reconciliation_identity();
DROP FUNCTION vela_private.require_finance_reconciliation_identity();
DROP TRIGGER finance_reconciliation_records_immutable
    ON finance_reconciliation_records;
DROP FUNCTION vela_reject_finance_reconciliation_record_mutation();
DROP TRIGGER finance_reconciliation_database_bindings_immutable
    ON finance_reconciliation_database_bindings;
DROP FUNCTION vela_reject_finance_reconciliation_binding_mutation();
DROP TRIGGER finance_reconciliation_principals_immutable
    ON finance_reconciliation_principals;
DROP FUNCTION vela_reject_finance_reconciliation_principal_mutation();

REVOKE UPDATE (
    contract_credit_limit_minor,
    unsettled_posted_minor,
    version,
    updated_at
) ON organization_credit_accounts FROM vela_finance_reconciliation_owner;
REVOKE UPDATE ON finance_reconciliation_cursors FROM vela_finance_reconciliation_owner;
REVOKE INSERT ON finance_reconciliation_cursors,
    finance_reconciliation_records FROM vela_finance_reconciliation_owner;
REVOKE SELECT ON finance_reconciliation_principals,
    finance_reconciliation_database_bindings,
    finance_reconciliation_cursors,
    finance_reconciliation_records,
    organization_credit_accounts
FROM vela_finance_reconciliation_owner;
REVOKE USAGE ON TYPE finance_reconciliation_kind FROM vela_finance_reconciliation_owner;
REVOKE USAGE ON SCHEMA public, vela_private FROM vela_finance_reconciliation_owner;

DROP TABLE finance_reconciliation_records;
DROP TABLE finance_reconciliation_cursors;
DROP TABLE finance_reconciliation_database_bindings;
DROP TABLE finance_reconciliation_principals;
DROP TYPE finance_reconciliation_kind;
-- +goose StatementEnd
