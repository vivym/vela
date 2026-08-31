-- +goose Up
-- +goose StatementBegin
GRANT USAGE ON SCHEMA public TO vela_h3_campaign_evidence;

GRANT SELECT ON
    artifact_sets,
    attempts,
    charges,
    compute_nodes,
    devices,
    jobs,
    stage_allocations,
    stage_artifact_inputs,
    stage_artifact_pins,
    stage_artifacts,
    stage_attempts,
    stage_cache_entries,
    stage_cache_references,
    stage_dependencies,
    stage_run_output_bindings,
    stage_runs,
    transfer_tickets,
    visible_completions,
    worker_instances,
    worker_member_devices,
    worker_members
TO vela_h3_campaign_evidence;

CREATE POLICY h3_campaign_evidence_read ON artifact_sets
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON attempts
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON charges
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON compute_nodes
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON devices
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON jobs
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_allocations
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_artifact_inputs
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_artifact_pins
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_artifacts
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_attempts
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_cache_entries
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_cache_references
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_dependencies
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_run_output_bindings
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON stage_runs
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON transfer_tickets
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON visible_completions
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON worker_instances
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON worker_member_devices
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
CREATE POLICY h3_campaign_evidence_read ON worker_members
    FOR SELECT TO vela_h3_campaign_evidence USING (true);
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP POLICY h3_campaign_evidence_read ON worker_members;
DROP POLICY h3_campaign_evidence_read ON worker_member_devices;
DROP POLICY h3_campaign_evidence_read ON worker_instances;
DROP POLICY h3_campaign_evidence_read ON visible_completions;
DROP POLICY h3_campaign_evidence_read ON transfer_tickets;
DROP POLICY h3_campaign_evidence_read ON stage_runs;
DROP POLICY h3_campaign_evidence_read ON stage_run_output_bindings;
DROP POLICY h3_campaign_evidence_read ON stage_dependencies;
DROP POLICY h3_campaign_evidence_read ON stage_cache_references;
DROP POLICY h3_campaign_evidence_read ON stage_cache_entries;
DROP POLICY h3_campaign_evidence_read ON stage_attempts;
DROP POLICY h3_campaign_evidence_read ON stage_artifacts;
DROP POLICY h3_campaign_evidence_read ON stage_artifact_pins;
DROP POLICY h3_campaign_evidence_read ON stage_artifact_inputs;
DROP POLICY h3_campaign_evidence_read ON stage_allocations;
DROP POLICY h3_campaign_evidence_read ON jobs;
DROP POLICY h3_campaign_evidence_read ON devices;
DROP POLICY h3_campaign_evidence_read ON compute_nodes;
DROP POLICY h3_campaign_evidence_read ON charges;
DROP POLICY h3_campaign_evidence_read ON attempts;
DROP POLICY h3_campaign_evidence_read ON artifact_sets;

REVOKE SELECT ON
    artifact_sets,
    attempts,
    charges,
    compute_nodes,
    devices,
    jobs,
    stage_allocations,
    stage_artifact_inputs,
    stage_artifact_pins,
    stage_artifacts,
    stage_attempts,
    stage_cache_entries,
    stage_cache_references,
    stage_dependencies,
    stage_run_output_bindings,
    stage_runs,
    transfer_tickets,
    visible_completions,
    worker_instances,
    worker_member_devices,
    worker_members
FROM vela_h3_campaign_evidence;
REVOKE USAGE ON SCHEMA public FROM vela_h3_campaign_evidence;
-- +goose StatementEnd
