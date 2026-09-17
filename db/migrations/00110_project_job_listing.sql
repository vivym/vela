-- +goose Up
GRANT SELECT (id, stable_id) ON model_revisions TO vela_request;
CREATE INDEX jobs_project_created_id_idx
    ON jobs (organization_id, project_id, created_at DESC, id DESC);
CREATE INDEX jobs_project_active_created_id_idx
    ON jobs (organization_id, project_id, created_at DESC, id DESC)
    WHERE state IN ('QUEUED', 'ASSIGNED', 'RUNNING', 'FINALIZING', 'RETRY_WAIT', 'CANCELING');

-- +goose Down
DROP INDEX jobs_project_active_created_id_idx;
DROP INDEX jobs_project_created_id_idx;
REVOKE SELECT (id, stable_id) ON model_revisions FROM vela_request;
