-- +goose Up
-- +goose StatementBegin
CREATE TYPE stage_graph_finalization_claim_state AS ENUM (
    'ACTIVE', 'EXPIRED', 'COMPLETED'
);

CREATE TABLE stage_graph_finalization_claims (
    id uuid PRIMARY KEY,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    job_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    attempt_fence bigint NOT NULL CHECK (attempt_fence > 0),
    final_stage_run_id uuid NOT NULL,
    final_stage_artifact_id uuid NOT NULL,
    exact_object_version text NOT NULL CHECK (
        length(exact_object_version) BETWEEN 1 AND 1000
    ),
    owner_id text NOT NULL CHECK (length(owner_id) BETWEEN 1 AND 500),
    token_digest bytea NOT NULL CHECK (octet_length(token_digest) = 32),
    signing_key_id text NOT NULL CHECK (length(signing_key_id) BETWEEN 1 AND 100),
    state stage_graph_finalization_claim_state NOT NULL DEFAULT 'ACTIVE',
    issued_at timestamptz NOT NULL,
    expires_at timestamptz NOT NULL,
    expired_at timestamptz,
    completed_at timestamptz,
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    UNIQUE (organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, job_id)
        REFERENCES jobs(organization_id, project_id, id),
    FOREIGN KEY (organization_id, project_id, attempt_id)
        REFERENCES attempts(organization_id, project_id, id),
    FOREIGN KEY (attempt_id, final_stage_run_id)
        REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (final_stage_run_id, final_stage_artifact_id)
        REFERENCES stage_artifacts(producer_stage_run_id, id),
    CHECK (expires_at > issued_at),
    CHECK (
        (state = 'ACTIVE' AND expired_at IS NULL AND completed_at IS NULL)
        OR (state = 'EXPIRED' AND expired_at IS NOT NULL AND completed_at IS NULL)
        OR (state = 'COMPLETED' AND expired_at IS NULL AND completed_at IS NOT NULL)
    )
);

CREATE UNIQUE INDEX stage_graph_finalization_one_active_per_attempt_idx
    ON stage_graph_finalization_claims(attempt_id)
    WHERE state = 'ACTIVE';
CREATE UNIQUE INDEX stage_graph_finalization_one_active_per_owner_idx
    ON stage_graph_finalization_claims(owner_id)
    WHERE state = 'ACTIVE';

CREATE FUNCTION vela_validate_stage_graph_finalization_claim_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.id <> OLD.id
       OR NEW.organization_id <> OLD.organization_id
       OR NEW.project_id <> OLD.project_id
       OR NEW.job_id <> OLD.job_id
       OR NEW.attempt_id <> OLD.attempt_id
       OR NEW.attempt_fence <> OLD.attempt_fence
       OR NEW.final_stage_run_id <> OLD.final_stage_run_id
       OR NEW.final_stage_artifact_id <> OLD.final_stage_artifact_id
       OR NEW.exact_object_version <> OLD.exact_object_version
       OR NEW.owner_id <> OLD.owner_id
       OR NEW.token_digest <> OLD.token_digest
       OR NEW.signing_key_id <> OLD.signing_key_id
       OR NEW.issued_at <> OLD.issued_at
       OR NEW.expires_at > OLD.expires_at
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'stage_graph_finalization_claim_identity_immutable',
            MESSAGE = 'Stage graph finalization claim identity is immutable';
    END IF;
    IF OLD.state <> NEW.state AND NOT (
        OLD.state = 'ACTIVE' AND NEW.state IN ('EXPIRED', 'COMPLETED')
    ) THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            CONSTRAINT = 'stage_graph_finalization_claim_state_transition',
            MESSAGE = 'Stage graph finalization claim state transition is invalid';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER stage_graph_finalization_claim_mutation_valid
BEFORE UPDATE ON stage_graph_finalization_claims
FOR EACH ROW EXECUTE FUNCTION vela_validate_stage_graph_finalization_claim_mutation();

ALTER TABLE stage_graph_finalization_claims OWNER TO vela_internal;
ALTER FUNCTION vela_validate_stage_graph_finalization_claim_mutation()
    OWNER TO vela_internal;
REVOKE ALL ON stage_graph_finalization_claims FROM PUBLIC;
GRANT SELECT, INSERT, UPDATE ON stage_graph_finalization_claims TO vela_internal;
GRANT SELECT ON stage_runs, stage_dependencies, stage_artifacts TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_graph_finalization_claims) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_finalization_rollback_is_unsafe',
            MESSAGE = 'Migration 00043 cannot contract while Finalizer handoff evidence remains';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS stage_graph_finalization_claim_mutation_valid
    ON stage_graph_finalization_claims;
DROP FUNCTION IF EXISTS vela_validate_stage_graph_finalization_claim_mutation();
DROP TABLE IF EXISTS stage_graph_finalization_claims;
REVOKE SELECT ON stage_runs, stage_dependencies, stage_artifacts FROM vela_internal;
DROP TYPE IF EXISTS stage_graph_finalization_claim_state;
-- +goose StatementEnd
