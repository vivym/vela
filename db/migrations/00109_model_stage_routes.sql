-- +goose Up
-- +goose StatementBegin
-- Serialize pointer migration with the versioned activation transaction.
LOCK TABLE stage_cutover_control IN ACCESS EXCLUSIVE MODE;
-- Activation still appends one versioned, approved cutover revision. Each model
-- keeps its last activated route when another model is published. A global
-- non-STAGE_ONLY activation clears all routes; reopening requires explicit
-- activation of each model, never implicit resurrection of old authorizations.
ALTER TABLE stage_cutover_revisions
    ADD CONSTRAINT stage_cutover_revision_model UNIQUE (id, model_revision_id);
CREATE TABLE model_stage_routes (
    model_revision_id uuid PRIMARY KEY REFERENCES model_revisions(id),
    cutover_revision_id uuid NOT NULL UNIQUE,
    FOREIGN KEY (cutover_revision_id, model_revision_id)
        REFERENCES stage_cutover_revisions(id, model_revision_id)
);
INSERT INTO model_stage_routes (model_revision_id, cutover_revision_id)
SELECT revision.model_revision_id, revision.id
FROM stage_cutover_control AS control
JOIN stage_cutover_revisions AS revision ON revision.id = control.current_revision_id
WHERE control.singleton AND revision.mode = 'STAGE_ONLY';
GRANT SELECT, INSERT, UPDATE, DELETE ON model_stage_routes TO vela_catalog_promotion_owner;
REVOKE ALL ON model_stage_routes FROM PUBLIC;
CREATE TRIGGER model_stage_routes_writer
BEFORE INSERT OR UPDATE OR DELETE OR TRUNCATE ON model_stage_routes
FOR EACH STATEMENT EXECUTE FUNCTION vela_guard_stage_cutover_control_writer();

CREATE FUNCTION vela_sync_model_stage_route() RETURNS trigger
LANGUAGE plpgsql
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_revision public.stage_cutover_revisions%ROWTYPE;
BEGIN
    SELECT * INTO STRICT v_revision FROM public.stage_cutover_revisions
    WHERE id = NEW.current_revision_id;
    IF v_revision.mode = 'STAGE_ONLY' THEN
        INSERT INTO public.model_stage_routes (model_revision_id, cutover_revision_id)
        VALUES (v_revision.model_revision_id, v_revision.id)
        ON CONFLICT (model_revision_id) DO UPDATE
        SET cutover_revision_id = EXCLUDED.cutover_revision_id;
    ELSE
        DELETE FROM public.model_stage_routes;
    END IF;
    RETURN NEW;
END
$$;
ALTER FUNCTION vela_sync_model_stage_route() OWNER TO vela_catalog_promotion_owner;
REVOKE ALL ON FUNCTION vela_sync_model_stage_route() FROM PUBLIC;
CREATE TRIGGER stage_cutover_model_route
AFTER UPDATE ON stage_cutover_control
FOR EACH ROW EXECUTE FUNCTION vela_sync_model_stage_route();

CREATE OR REPLACE FUNCTION vela_resolve_stage_job_execution_route(
    p_organization_id uuid,
    p_project_id uuid,
    p_model_revision_id uuid
) RETURNS TABLE (
    stage_cutover_revision_id uuid,
    execution_graph_revision_id uuid,
    execution_profile_revision_id uuid,
    reserved_storage_bytes bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_organization_id IS NULL OR p_project_id IS NULL
       OR p_model_revision_id IS NULL
       OR p_organization_id IS DISTINCT FROM public.vela_current_organization_id()
       OR p_project_id IS DISTINCT FROM public.vela_current_project_id() THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'stage_route_request_scope_mismatch',
            MESSAGE = 'Stage route must match the authenticated request scope';
    END IF;
    RETURN QUERY
    SELECT revision.id, revision.execution_graph_revision_id,
        revision.execution_profile_revision_id, revision.reserved_storage_bytes
    FROM public.stage_cutover_control AS control
    JOIN public.stage_cutover_revisions AS global_revision
      ON global_revision.id = control.current_revision_id
     AND global_revision.mode = 'STAGE_ONLY'
    JOIN public.model_stage_routes AS route
      ON route.model_revision_id = p_model_revision_id
    JOIN public.stage_cutover_revisions AS revision
      ON revision.id = route.cutover_revision_id
     AND revision.model_revision_id = route.model_revision_id
    JOIN public.execution_graph_revisions AS graph
      ON graph.id = revision.execution_graph_revision_id
     AND graph.model_revision_id = revision.model_revision_id
     AND graph.state = 'ACTIVE'
    JOIN public.execution_profile_revisions AS profile
      ON profile.id = revision.execution_profile_revision_id
     AND profile.execution_graph_revision_id = graph.id
     AND profile.model_revision_id = graph.model_revision_id
     AND profile.state = 'ACTIVE'
    WHERE control.singleton
      AND revision.mode = 'STAGE_ONLY'
      AND revision.model_revision_id = p_model_revision_id
      AND revision.reserved_storage_bytes > 0
      AND (
          revision.scope = 'PRODUCTION'
          OR EXISTS (
              SELECT 1
              FROM public.stage_cutover_internal_projects AS binding
              WHERE binding.cutover_revision_id = revision.id
                AND binding.organization_id = p_organization_id
                AND binding.project_id = p_project_id
          )
      )
    FOR SHARE OF control, route, revision, graph, profile;
END
$$;
CREATE OR REPLACE FUNCTION vela_authorize_stage_cutover_internal_project(
    p_cutover_revision_id uuid,
    p_organization_id uuid,
    p_project_id uuid,
    p_authorized_by text
) RETURNS stage_cutover_internal_projects
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_revision public.stage_cutover_revisions%ROWTYPE;
    v_result public.stage_cutover_internal_projects%ROWTYPE;
BEGIN
    IF p_cutover_revision_id IS NULL OR p_organization_id IS NULL
       OR p_project_id IS NULL OR p_authorized_by IS NULL
       OR length(p_authorized_by) NOT BETWEEN 1 AND 300
       OR btrim(p_authorized_by) <> p_authorized_by
       OR p_authorized_by ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_cutover_internal_project_invalid',
            MESSAGE = 'Internal Stage cutover project authorization is invalid';
    END IF;
    SELECT revision.* INTO STRICT v_revision
    FROM public.stage_cutover_control AS control
    JOIN public.stage_cutover_revisions AS global_revision
      ON global_revision.id = control.current_revision_id
     AND global_revision.mode = 'STAGE_ONLY'
    JOIN public.model_stage_routes AS route ON true
    JOIN public.stage_cutover_revisions AS revision
      ON revision.id = route.cutover_revision_id
     AND revision.model_revision_id = route.model_revision_id
    WHERE control.singleton
      AND revision.id = p_cutover_revision_id
    FOR SHARE OF control, route, revision;
    IF v_revision.scope <> 'INTERNAL' OR v_revision.mode = 'LEGACY_ONLY' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_internal_project_scope_mismatch',
            MESSAGE = 'Only the current INTERNAL Stage revision accepts project authorization';
    END IF;
    INSERT INTO public.stage_cutover_internal_projects (
        cutover_revision_id, organization_id, project_id, authorized_by
    ) VALUES (
        p_cutover_revision_id, p_organization_id, p_project_id, p_authorized_by
    ) ON CONFLICT (cutover_revision_id, organization_id, project_id) DO NOTHING
    RETURNING * INTO v_result;
    IF NOT FOUND THEN
        SELECT binding.* INTO STRICT v_result
        FROM public.stage_cutover_internal_projects AS binding
        WHERE binding.cutover_revision_id = p_cutover_revision_id
          AND binding.organization_id = p_organization_id
          AND binding.project_id = p_project_id;
        IF v_result.authorized_by <> p_authorized_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_cutover_internal_project_replay_mismatch',
                MESSAGE = 'Internal Stage cutover project authorization replay changed';
        END IF;
    END IF;
    RETURN v_result;
END
$$;

-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
-- Serialize pointer migration with the versioned activation transaction.
LOCK TABLE stage_cutover_control IN ACCESS EXCLUSIVE MODE;
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM model_stage_routes AS route
        WHERE route.cutover_revision_id IS DISTINCT FROM (
            SELECT current_revision_id FROM stage_cutover_control WHERE singleton
        )
    ) THEN
        RAISE EXCEPTION 'cannot downgrade while additional model routes are active';
    END IF;
END
$$;
CREATE OR REPLACE FUNCTION vela_resolve_stage_job_execution_route(
    p_organization_id uuid,
    p_project_id uuid,
    p_model_revision_id uuid
) RETURNS TABLE (
    stage_cutover_revision_id uuid,
    execution_graph_revision_id uuid,
    execution_profile_revision_id uuid,
    reserved_storage_bytes bigint
)
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF p_organization_id IS NULL OR p_project_id IS NULL
       OR p_model_revision_id IS NULL
       OR p_organization_id IS DISTINCT FROM public.vela_current_organization_id()
       OR p_project_id IS DISTINCT FROM public.vela_current_project_id() THEN
        RAISE EXCEPTION USING
            ERRCODE = '42501',
            CONSTRAINT = 'stage_route_request_scope_mismatch',
            MESSAGE = 'Stage route must match the authenticated request scope';
    END IF;
    RETURN QUERY
    SELECT revision.id, revision.execution_graph_revision_id,
        revision.execution_profile_revision_id, revision.reserved_storage_bytes
    FROM public.stage_cutover_control AS control
    JOIN public.stage_cutover_revisions AS revision
      ON revision.id = control.current_revision_id
    JOIN public.execution_graph_revisions AS graph
      ON graph.id = revision.execution_graph_revision_id
     AND graph.model_revision_id = revision.model_revision_id
     AND graph.state = 'ACTIVE'
    JOIN public.execution_profile_revisions AS profile
      ON profile.id = revision.execution_profile_revision_id
     AND profile.execution_graph_revision_id = graph.id
     AND profile.model_revision_id = graph.model_revision_id
     AND profile.state = 'ACTIVE'
    WHERE control.singleton
      AND revision.mode = 'STAGE_ONLY'
      AND revision.model_revision_id = p_model_revision_id
      AND revision.reserved_storage_bytes > 0
      AND (
          revision.scope = 'PRODUCTION'
          OR EXISTS (
              SELECT 1
              FROM public.stage_cutover_internal_projects AS binding
              WHERE binding.cutover_revision_id = revision.id
                AND binding.organization_id = p_organization_id
                AND binding.project_id = p_project_id
          )
      )
    FOR SHARE OF control, revision, graph, profile;
END
$$;
CREATE OR REPLACE FUNCTION vela_authorize_stage_cutover_internal_project(
    p_cutover_revision_id uuid,
    p_organization_id uuid,
    p_project_id uuid,
    p_authorized_by text
) RETURNS stage_cutover_internal_projects
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    v_revision public.stage_cutover_revisions%ROWTYPE;
    v_result public.stage_cutover_internal_projects%ROWTYPE;
BEGIN
    IF p_cutover_revision_id IS NULL OR p_organization_id IS NULL
       OR p_project_id IS NULL OR p_authorized_by IS NULL
       OR length(p_authorized_by) NOT BETWEEN 1 AND 300
       OR btrim(p_authorized_by) <> p_authorized_by
       OR p_authorized_by ~ '[[:cntrl:]]' THEN
        RAISE EXCEPTION USING
            ERRCODE = '22023',
            CONSTRAINT = 'stage_cutover_internal_project_invalid',
            MESSAGE = 'Internal Stage cutover project authorization is invalid';
    END IF;
    SELECT revision.* INTO STRICT v_revision
    FROM public.stage_cutover_control AS control
    JOIN public.stage_cutover_revisions AS revision
      ON revision.id = control.current_revision_id
    WHERE control.singleton
      AND revision.id = p_cutover_revision_id
    FOR SHARE OF control, revision;
    IF v_revision.scope <> 'INTERNAL' OR v_revision.mode = 'LEGACY_ONLY' THEN
        RAISE EXCEPTION USING
            ERRCODE = '55000',
            CONSTRAINT = 'stage_cutover_internal_project_scope_mismatch',
            MESSAGE = 'Only the current INTERNAL Stage revision accepts project authorization';
    END IF;
    INSERT INTO public.stage_cutover_internal_projects (
        cutover_revision_id, organization_id, project_id, authorized_by
    ) VALUES (
        p_cutover_revision_id, p_organization_id, p_project_id, p_authorized_by
    ) ON CONFLICT (cutover_revision_id, organization_id, project_id) DO NOTHING
    RETURNING * INTO v_result;
    IF NOT FOUND THEN
        SELECT binding.* INTO STRICT v_result
        FROM public.stage_cutover_internal_projects AS binding
        WHERE binding.cutover_revision_id = p_cutover_revision_id
          AND binding.organization_id = p_organization_id
          AND binding.project_id = p_project_id;
        IF v_result.authorized_by <> p_authorized_by THEN
            RAISE EXCEPTION USING
                ERRCODE = '55000',
                CONSTRAINT = 'stage_cutover_internal_project_replay_mismatch',
                MESSAGE = 'Internal Stage cutover project authorization replay changed';
        END IF;
    END IF;
    RETURN v_result;
END
$$;

DROP TRIGGER stage_cutover_model_route ON stage_cutover_control;
DROP FUNCTION vela_sync_model_stage_route();
DROP TABLE model_stage_routes;
ALTER TABLE stage_cutover_revisions DROP CONSTRAINT stage_cutover_revision_model;
-- +goose StatementEnd
