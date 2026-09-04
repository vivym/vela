SELECT count(*)
FROM stage_cutover_control AS control
JOIN stage_cutover_revisions AS revision
  ON revision.id = control.current_revision_id
JOIN execution_graph_revisions AS graph
  ON graph.id = revision.execution_graph_revision_id
 AND graph.model_revision_id = revision.model_revision_id
JOIN execution_profile_revisions AS profile
  ON profile.id = revision.execution_profile_revision_id
 AND profile.execution_graph_revision_id = graph.id
 AND profile.model_revision_id = graph.model_revision_id
JOIN stage_cutover_internal_projects AS binding
  ON binding.cutover_revision_id = revision.id
 AND binding.organization_id = '84000000-0000-0000-0000-000000000001'
 AND binding.project_id = '84000000-0000-0000-0000-000000000002'
WHERE control.singleton
  AND revision.id = '84000000-0000-0000-0000-000000000701'
  AND revision.scope = 'INTERNAL'
  AND revision.mode = 'STAGE_ONLY'
  AND revision.stage_cohort_basis_points = 10000
  AND revision.model_revision_id = '84000000-0000-0000-0000-000000000004'
  AND revision.execution_graph_revision_id = '84000000-0000-0000-0000-000000000501'
  AND revision.execution_profile_revision_id = '84000000-0000-0000-0000-000000000006'
  AND revision.reserved_storage_bytes > 0
  AND graph.state = 'ACTIVE'
  AND profile.state = 'ACTIVE';
