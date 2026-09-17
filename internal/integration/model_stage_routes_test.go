//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
)

func TestModelStageRoutesPreservePreviousModelAdmission(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 107)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	// Upgrade must retain an already published model.
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatal(err)
	}
	graph, profile := cloneRouteModelFixture(t, database.Admin)
	secondCutover := uuid.New()
	activateStageCutoverRevision(t, database, secondCutover, 3, uuid.MustParse(stageCutoverRevisionID), graph, profile, 2<<30, "second-model-route")
	server := admissionServerForDatabase(t, database)
	for _, model := range []string{"minimax-h3", "minimax-h3-independent"} {
		body := []byte(fmt.Sprintf(`{"model":%q,"generation_preset":"balanced","service_class":"standard","output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"a quiet lake"}`, model))
		result := submitJob(t, server.URL, "model-route-"+model, body)
		if result.StatusCode != http.StatusAccepted {
			t.Fatalf("model %s status=%d body=%s", model, result.StatusCode, result.Body)
		}
	}

	promotion := stageCutoverPromotionPool(t, database)
	// A prior model remains administrable, but grants stay attached to its exact revision.
	authorizeInternalCutoverProject(t, promotion, uuid.MustParse(stageCutoverRevisionID))
	if _, err := promotion.Exec(context.Background(), `DELETE FROM model_stage_routes`); err == nil {
		t.Fatal("promotion login directly deleted model routes")
	}
	if _, err := database.Admin.Exec(`UPDATE model_stage_routes SET cutover_revision_id=cutover_revision_id`); err == nil {
		t.Fatal("direct administrative writer bypassed activation function")
	}
	if err := goose.DownTo(database.Admin, migrations, 107); err == nil || !strings.Contains(err.Error(), "additional model routes") {
		t.Fatalf("unsafe downgrade: %v", err)
	}
	// Newer independent migrations may have rolled back before the route guard rejected Down.
	// Restore the current HTTP read projection before making further API requests.
	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatal(err)
	}
	// A new revision of an existing model replaces only that model's pointer.
	replacement := uuid.New()
	activateStageCutoverRevision(t, database, replacement, 4, secondCutover, uuid.MustParse(stageGraphID), uuid.MustParse(graphExecutionProfileID), 3<<30, "replace-first-model")
	var count int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM model_stage_routes WHERE cutover_revision_id IN ($1,$2)`, replacement, secondCutover).Scan(&count); err != nil || count != 2 {
		t.Fatalf("replacement lost or duplicated a route: count=%d err=%v", count, err)
	}

	// New revisions do not inherit even the same model's prior project grant.
	unapproved := uuid.New()
	var connectorDigest []byte
	if err := database.Admin.QueryRow(`SELECT vela_execution_profile_connector_set_digest($1,$2)`, graphExecutionProfileID, stageGraphID).Scan(&connectorDigest); err != nil {
		t.Fatal(err)
	}
	if _, err := promotion.Exec(context.Background(), `SELECT vela_activate_stage_cutover(
 $1,5,$2,'INTERNAL','STAGE_ONLY',10000,$3,$4,2147483648,1,
 decode(repeat('a1',32),'hex'),'unapproved-replacement',sha256('unapproved-replacement'::bytea),
 $5,NULL,'integration','verify exact grant boundary')`,
		unapproved, replacement, stageGraphID, graphExecutionProfileID, connectorDigest); err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		model  string
		status int
	}{{"minimax-h3", http.StatusServiceUnavailable}, {"minimax-h3-independent", http.StatusAccepted}} {
		body := []byte(fmt.Sprintf(`{"model":%q,"generation_preset":"balanced","service_class":"standard","output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"a quiet lake"}`, item.model))
		result := submitJob(t, server.URL, "unapproved-route-"+item.model, body)
		if result.StatusCode != item.status {
			t.Fatalf("model %s authorization status=%d body=%s", item.model, result.StatusCode, result.Body)
		}
	}
	if _, err := promotion.Exec(context.Background(), `SELECT vela_authorize_stage_cutover_internal_project($1,$2,$3,'integration-catalog-promotion')`, replacement, testOrganizationID, testProjectID); err == nil {
		t.Fatal("authorized a superseded route revision")
	}
	// The global maintenance switch closes every model and does not resurrect old routes.
	maintenance := uuid.New()
	if _, err := promotion.Exec(context.Background(), `SELECT vela_activate_stage_cutover(
 $1,6,$2,'INTERNAL','LEGACY_ONLY',0,NULL,NULL,0,1,decode(repeat('a1',32),'hex'),
 'maintenance',sha256('maintenance'::bytea),sha256(''::bytea),NULL,'integration','global maintenance')`, maintenance, unapproved); err != nil {
		t.Fatal(err)
	}
	if err := database.Admin.QueryRow(`SELECT count(*) FROM model_stage_routes`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("maintenance retained routes: %d %v", count, err)
	}
	activateStageCutoverRevision(t, database, uuid.New(), 7, maintenance, graph, profile, 2<<30, "reopen-second-model")
	if err := database.Admin.QueryRow(`SELECT count(*) FROM model_stage_routes`).Scan(&count); err != nil || count != 1 {
		t.Fatalf("reopening resurrected routes: %d %v", count, err)
	}
	if err := goose.DownTo(database.Admin, migrations, 107); err != nil {
		t.Fatal(err)
	}
	if err := goose.UpTo(database.Admin, migrations, 109); err != nil {
		t.Fatal(err)
	}
}

// Clone only catalog fixture facts; real deployment requires independent qualification.
func cloneRouteModelFixture(t *testing.T, db *sql.DB) (uuid.UUID, uuid.UUID) {
	t.Helper()
	const model = "00000000-0000-0000-0000-000000000010"
	graph, profile, newModel := uuid.New(), uuid.New(), uuid.New()
	replacements := map[string]string{model: newModel.String(), stageGraphID: graph.String(), graphExecutionProfileID: profile.String()}
	type fixtureRows struct {
		table string
		rows  []map[string]any
	}
	specs := []struct{ table, where string }{
		{"model_revisions", "id='" + model + "'"},
		{"generation_preset_revisions", "model_revision_id='" + model + "'"},
		{"execution_graph_revisions", "id='" + stageGraphID + "'"},
		{"execution_graph_stages", "execution_graph_revision_id='" + stageGraphID + "'"},
		{"execution_graph_edges", "execution_graph_revision_id='" + stageGraphID + "'"},
		{"execution_graph_inputs", "execution_graph_revision_id='" + stageGraphID + "'"},
		{"execution_graph_outputs", "execution_graph_revision_id='" + stageGraphID + "'"},
		{"execution_profile_revisions", "id='" + graphExecutionProfileID + "'"},
		{"execution_profile_stage_options", "execution_profile_revision_id='" + graphExecutionProfileID + "'"},
		{"execution_profile_connector_options", "execution_profile_revision_id='" + graphExecutionProfileID + "'"},
		{"profile_certifications", "execution_profile_revision_id='" + graphExecutionProfileID + "'"},
		{"rate_card_lines", "model_revision_id='" + model + "'"},
	}
	var all []fixtureRows
	for _, spec := range specs {
		rows, err := db.Query("SELECT row_to_json(r) FROM " + spec.table + " r WHERE " + spec.where)
		if err != nil {
			t.Fatal(err)
		}
		group := fixtureRows{table: spec.table}
		for rows.Next() {
			var wire []byte
			if err := rows.Scan(&wire); err != nil {
				t.Fatal(err)
			}
			var row map[string]any
			if err := json.Unmarshal(wire, &row); err != nil {
				t.Fatal(err)
			}
			group.rows = append(group.rows, row)
			if id, ok := row["id"].(string); ok && replacements[id] == "" {
				replacements[id] = uuid.NewString()
			}
		}
		if err := rows.Err(); err != nil {
			t.Fatal(err)
		}
		rows.Close()
		all = append(all, group)
	}
	for _, group := range all {
		for _, row := range group.rows {
			if group.table == "model_revisions" {
				row["stable_id"] = "minimax-h3-independent"
			}
			if group.table == "execution_graph_revisions" || group.table == "execution_profile_revisions" {
				row["stable_id"] = row["stable_id"].(string) + "-independent"
				row["state"] = "CERTIFIED"
			}
			wire, err := json.Marshal(row)
			if err != nil {
				t.Fatal(err)
			}
			for before, after := range replacements {
				wire = bytes.ReplaceAll(wire, []byte(before), []byte(after))
			}
			if _, err := db.Exec("INSERT INTO "+group.table+" SELECT (jsonb_populate_record(NULL::"+group.table+",$1::jsonb)).*", string(wire)); err != nil {
				t.Fatalf("clone %s: %v", group.table, err)
			}
		}
	}
	if _, err := db.Exec(`UPDATE execution_graph_revisions SET content_digest=vela_execution_graph_content_digest(id) WHERE id=$1;`, graph); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`SELECT * FROM vela_activate_execution_graph($1,vela_execution_graph_content_digest($1))`, graph); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE execution_profile_revisions SET state='ACTIVE' WHERE id=$1`, profile); err != nil {
		t.Fatal(err)
	}
	return graph, profile
}

func TestModelStageRoutesDowngradeSerializesWithActivation(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	graph, profile := cloneRouteModelFixture(t, database.Admin)
	promotion := stageCutoverPromotionPool(t, database)
	var connectorDigest []byte
	if err := database.Admin.QueryRow(`SELECT vela_execution_profile_connector_set_digest($1,$2)`, profile, graph).Scan(&connectorDigest); err != nil {
		t.Fatal(err)
	}
	tx, err := promotion.Begin(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err := tx.Exec(context.Background(), `SELECT vela_activate_stage_cutover(
 $1,3,$2,'INTERNAL','STAGE_ONLY',10000,$3,$4,2147483648,1,
 decode(repeat('a1',32),'hex'),'concurrent-model',sha256('concurrent-model'::bytea),
 $5,NULL,'integration','concurrent publication')`, uuid.New(), stageCutoverRevisionID, graph, profile, connectorDigest); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		done <- goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 107)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := database.Admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_locks WHERE relation='stage_cutover_control'::regclass AND NOT granted)`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("downgrade did not wait for concurrent activation")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if err := tx.Commit(context.Background()); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "additional model routes") {
			t.Fatalf("downgrade lost concurrent publication: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("downgrade did not finish")
	}
	var routes int
	if err := database.Admin.QueryRow(`SELECT count(*) FROM model_stage_routes`).Scan(&routes); err != nil || routes != 2 {
		t.Fatalf("published routes=%d error=%v", routes, err)
	}
}
