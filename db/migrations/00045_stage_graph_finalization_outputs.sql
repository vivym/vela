-- +goose Up
-- +goose StatementBegin
ALTER TABLE stage_graph_finalization_claims
    ADD COLUMN output_set_digest bytea CHECK (
        output_set_digest IS NULL OR octet_length(output_set_digest) = 32
    );

CREATE FUNCTION vela_reject_stage_graph_finalization_output_set_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION USING
        ERRCODE = '55000',
        CONSTRAINT = 'stage_graph_finalization_output_set_immutable',
        MESSAGE = 'Stage graph finalization output-set evidence is immutable';
END
$$;

CREATE TRIGGER stage_graph_finalization_output_set_immutable
BEFORE UPDATE OF output_set_digest ON stage_graph_finalization_claims
FOR EACH ROW
WHEN (OLD.output_set_digest IS DISTINCT FROM NEW.output_set_digest)
EXECUTE FUNCTION vela_reject_stage_graph_finalization_output_set_mutation();

CREATE TABLE stage_graph_finalization_claim_outputs (
    claim_id uuid NOT NULL,
    organization_id uuid NOT NULL,
    project_id uuid NOT NULL,
    attempt_id uuid NOT NULL,
    output_key text NOT NULL CHECK (
        length(output_key) BETWEEN 1 AND 100
        AND output_key ~ '^[a-z][a-z0-9_-]*$'
    ),
    artifact_kind artifact_kind NOT NULL,
    ordinal integer NOT NULL CHECK (ordinal >= 0),
    stage_run_id uuid NOT NULL,
    stage_artifact_id uuid NOT NULL,
    stage_interface_revision_id uuid NOT NULL,
    exact_object_version text NOT NULL CHECK (
        length(exact_object_version) BETWEEN 1 AND 1000
    ),
    created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
    PRIMARY KEY (claim_id, output_key),
    UNIQUE (claim_id, artifact_kind, ordinal),
    UNIQUE (claim_id, stage_artifact_id),
    FOREIGN KEY (organization_id, project_id, claim_id)
        REFERENCES stage_graph_finalization_claims(organization_id, project_id, id),
    FOREIGN KEY (attempt_id, stage_run_id)
        REFERENCES stage_runs(attempt_id, id),
    FOREIGN KEY (stage_run_id, stage_artifact_id)
        REFERENCES stage_artifacts(producer_stage_run_id, id),
    FOREIGN KEY (stage_interface_revision_id)
        REFERENCES stage_interface_revisions(id)
);

CREATE TRIGGER stage_graph_finalization_claim_outputs_immutable
BEFORE UPDATE OR DELETE ON stage_graph_finalization_claim_outputs
FOR EACH ROW EXECUTE FUNCTION vela_reject_stage_graph_finalization_output_set_mutation();

ALTER TABLE stage_graph_finalization_claim_outputs OWNER TO vela_internal;
ALTER FUNCTION vela_reject_stage_graph_finalization_output_set_mutation()
    OWNER TO vela_internal;
REVOKE ALL ON stage_graph_finalization_claim_outputs FROM PUBLIC;
REVOKE ALL ON FUNCTION vela_reject_stage_graph_finalization_output_set_mutation()
    FROM PUBLIC;
GRANT SELECT, INSERT ON stage_graph_finalization_claim_outputs TO vela_internal;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM stage_graph_finalization_claim_outputs) THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_graph_finalization_outputs_rollback_is_unsafe',
            MESSAGE = 'Migration 00045 cannot contract while output-set evidence remains';
    END IF;
END
$$;

DROP TRIGGER IF EXISTS stage_graph_finalization_claim_outputs_immutable
    ON stage_graph_finalization_claim_outputs;
DROP TABLE IF EXISTS stage_graph_finalization_claim_outputs;
DROP TRIGGER IF EXISTS stage_graph_finalization_output_set_immutable
    ON stage_graph_finalization_claims;
DROP FUNCTION IF EXISTS vela_reject_stage_graph_finalization_output_set_mutation();
ALTER TABLE stage_graph_finalization_claims DROP COLUMN output_set_digest;
-- +goose StatementEnd
