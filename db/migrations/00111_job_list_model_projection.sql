-- +goose Up
-- Model names are projected only for authorized Jobs. The request login must
-- retain its exact privilege boundary; it cannot read catalog tables directly.
REVOKE SELECT (id, stable_id) ON model_revisions FROM vela_request;
CREATE OR REPLACE VIEW vela_request_job_runtime
WITH (security_barrier = true) AS
SELECT r.job_id, r.attempts_started, r.next_retry_at, m.stable_id AS model
FROM retry_runtime_states AS r
JOIN jobs AS j ON j.id = r.job_id
    AND j.organization_id = r.organization_id AND j.project_id = r.project_id
JOIN model_revisions AS m ON m.id = j.model_revision_id
WHERE vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
  AND r.organization_id = vela_current_organization_id()
  AND r.project_id = vela_current_project_id();

-- +goose Down
-- Do not restore the unsafe direct catalog grant from migration 110.
DROP VIEW vela_request_job_runtime;
CREATE VIEW vela_request_job_runtime
WITH (security_barrier = true) AS
SELECT job_id, attempts_started, next_retry_at
FROM retry_runtime_states
WHERE vela_current_request_scope() IN ('jobs:submit', 'jobs:read')
  AND organization_id = vela_current_organization_id()
  AND project_id = vela_current_project_id();
REVOKE ALL ON vela_request_job_runtime FROM PUBLIC;
GRANT SELECT ON vela_request_job_runtime TO vela_request;
