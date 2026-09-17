-- User-authorized queue tuning, 2026-09-17. Run as platform DBA in database app.
-- PostgreSQL is the durable authority for mutable queue bounds. Existing signed
-- ResidencyPlans remain historical bootstrap inputs; replay does not reset pools.
-- No Job, execution/profile identity, credit, runtime, or model route is changed.
-- Fail on drift. Rollback requires queue drain; never remove accepted work.
\set ON_ERROR_STOP on
BEGIN;
SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '30s';
DO $queue_tuning$
DECLARE
    v_pool_ids uuid[] := ARRAY[
    '1897eadf-e7fa-5202-a1a6-b9f2d19aefb9'::uuid,
    '44667230-d304-59fb-8904-b532670125e0'::uuid,
    '6a3f4470-efa2-51c1-8058-785b0b8587b5'::uuid,
    '86f09634-b032-5553-a91d-cde54aa1ab71'::uuid,
    '9c067716-aa83-56c5-a6ae-326ef4923bdf'::uuid,
    'b9d37710-3c1f-5956-98bc-72d056f72bb3'::uuid,
    'bc3fa694-b650-55d2-b286-5de7b4305492'::uuid,
    'e3efefc3-72c5-5fe4-8152-da18f0131e15'::uuid,
    'e40935a4-11da-53b0-86d7-7423db5a7872'::uuid,
    'f130d310-e2f2-5d21-9b78-dee099c245c2'::uuid,
    'fb6983bd-a32a-5dd1-88d0-e6509c354950'::uuid
    ];
    v_count integer;
BEGIN
    PERFORM r.model_revision_id FROM model_stage_routes r
    JOIN model_revisions m ON m.id=r.model_revision_id
    WHERE m.stable_id IN ('minimax-h3','minimax-h3-live-validation')
    ORDER BY r.model_revision_id FOR SHARE OF r;
    SELECT count(*) INTO v_count FROM model_stage_routes r
    JOIN model_revisions m ON m.id=r.model_revision_id
    JOIN (VALUES
    ('minimax-h3', '9f979515-1341-5cd2-99eb-479ee595069c'::uuid),
    ('minimax-h3-live-validation', '9eee563b-c970-5ef7-a594-3745967f5a12'::uuid)
    ) expected(model,cutover) ON expected.model=m.stable_id AND expected.cutover=r.cutover_revision_id;
    IF v_count <> 2 THEN RAISE EXCEPTION 'Model routes changed; re-inventory before queue tuning'; END IF;
    PERFORM id FROM projects WHERE id='62275ddc-ae83-4ca1-b80c-313161264836' FOR UPDATE;
    PERFORM id FROM capacity_pools WHERE id=ANY(v_pool_ids) ORDER BY id FOR UPDATE;
    IF EXISTS (
      (SELECT DISTINCT p.id FROM model_stage_routes r
       JOIN model_revisions m ON m.id=r.model_revision_id
       JOIN stage_cutover_revisions c ON c.id=r.cutover_revision_id
       JOIN execution_profile_stage_options o ON o.execution_profile_revision_id=c.execution_profile_revision_id
       JOIN capacity_pools p ON p.stage_profile_revision_id=o.stage_profile_revision_id AND p.state='ACTIVE'
       WHERE m.stable_id IN ('minimax-h3','minimax-h3-live-validation')
       EXCEPT SELECT unnest(v_pool_ids))
      UNION ALL
      (SELECT unnest(v_pool_ids) EXCEPT
       SELECT DISTINCT p.id FROM model_stage_routes r
       JOIN model_revisions m ON m.id=r.model_revision_id
       JOIN stage_cutover_revisions c ON c.id=r.cutover_revision_id
       JOIN execution_profile_stage_options o ON o.execution_profile_revision_id=c.execution_profile_revision_id
       JOIN capacity_pools p ON p.stage_profile_revision_id=o.stage_profile_revision_id AND p.state='ACTIVE'
       WHERE m.stable_id IN ('minimax-h3','minimax-h3-live-validation'))
    ) THEN RAISE EXCEPTION 'Routed pool set changed; re-inventory before queue tuning'; END IF;
    IF EXISTS (SELECT 1 FROM stage_ready_queue_entries WHERE capacity_pool_id=ANY(v_pool_ids)
               GROUP BY capacity_pool_id HAVING count(*)>32) THEN
      RAISE EXCEPTION 'Queue must drain below target before reducing capacity';
    END IF;
    UPDATE projects SET queued_limit=10
    WHERE id='62275ddc-ae83-4ca1-b80c-313161264836'
      AND organization_id='40f37967-d2e0-4028-a6ab-1c75eb243289'
      AND queued_limit=64 AND running_limit=8
      AND queued_count-retry_wait_count<=10;
    GET DIAGNOSTICS v_count=ROW_COUNT;
    IF v_count <> 1 THEN RAISE EXCEPTION 'Project settings changed or queue is above target'; END IF;
    UPDATE capacity_pools SET max_ready_queue_depth=32
    WHERE id=ANY(v_pool_ids) AND state='ACTIVE' AND max_ready_queue_depth=128;
    GET DIAGNOSTICS v_count=ROW_COUNT;
    IF v_count <> 11 THEN RAISE EXCEPTION 'Pool settings changed'; END IF;
END
$queue_tuning$;
COMMIT;
