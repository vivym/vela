-- +goose Up
-- Optional diagnostic context is committed with admission and survives Outbox
-- retries. It is not an identity, authorization, or business idempotency key.
ALTER TABLE jobs ADD COLUMN origin_trace_parent text
    CONSTRAINT jobs_origin_trace_parent_valid CHECK (
        origin_trace_parent IS NULL OR (
            octet_length(origin_trace_parent) = 55
            AND origin_trace_parent ~ '^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$'
            AND substring(origin_trace_parent FROM 4 FOR 32) <> repeat('0', 32)
            AND substring(origin_trace_parent FROM 37 FOR 16) <> repeat('0', 16)
        )
    );

-- +goose StatementBegin
CREATE FUNCTION vela_preserve_job_origin_trace() RETURNS trigger
LANGUAGE plpgsql SET search_path = pg_catalog AS $$
BEGIN
    IF NEW.origin_trace_parent IS DISTINCT FROM OLD.origin_trace_parent THEN
        RAISE EXCEPTION USING ERRCODE = '23514',
            MESSAGE = 'Job origin trace cannot be replaced';
    END IF;
    RETURN NEW;
END
$$;
-- +goose StatementEnd
REVOKE ALL ON FUNCTION vela_preserve_job_origin_trace() FROM PUBLIC;
CREATE TRIGGER jobs_origin_trace_immutable
BEFORE UPDATE OF origin_trace_parent ON jobs
FOR EACH ROW EXECUTE FUNCTION vela_preserve_job_origin_trace();

-- +goose Down
DROP TRIGGER jobs_origin_trace_immutable ON jobs;
DROP FUNCTION vela_preserve_job_origin_trace();
ALTER TABLE jobs DROP COLUMN origin_trace_parent;
