//go:build integration

package integration_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/h3stage"
	"github.com/vivym/vela/internal/stageartifact"
)

func TestLabExactCacheEvidenceRequiresPhysicalSourceAndExactTargetBindings(t *testing.T) {
	fixture := runH3CampaignEvidenceFixture(t)
	query, err := os.ReadFile(filepath.Join(repositoryRoot(t), "deploy", "lab-v2", "exact-cache-evidence.sql"))
	if err != nil {
		t.Fatal(err)
	}
	type cacheEvidence struct {
		SourceReady  bool `json:"source_ready"`
		Equivalent   bool `json:"equivalent_requests"`
		TargetReused bool `json:"target_reused"`
		Production   bool `json:"production_gate_evidence"`
	}
	capture := func(source, target uuid.UUID) cacheEvidence {
		t.Helper()
		var payload []byte
		if err := fixture.database.Admin.QueryRow(string(query), source, target).Scan(&payload); err != nil {
			t.Fatalf("capture lab cache evidence: %v", err)
		}
		var evidence cacheEvidence
		if err := json.Unmarshal(payload, &evidence); err != nil {
			t.Fatal(err)
		}
		return evidence
	}
	if evidence := capture(fixture.sourceJobID, fixture.cacheJobID); evidence.SourceReady || !evidence.TargetReused {
		t.Fatalf("unfinished source must not pass campaign: %#v", evidence)
	}
	completeLabCacheSource(t, fixture)
	if evidence := capture(fixture.sourceJobID, fixture.cacheJobID); !evidence.SourceReady || !evidence.Equivalent || !evidence.TargetReused || evidence.Production {
		t.Fatalf("committed exact-cache campaign = %#v", evidence)
	}
	if evidence := capture(fixture.sourceJobID, fixture.sameNodeJobID); evidence.TargetReused {
		t.Fatal("same request executed physically must not count as cache reuse")
	}
	if evidence := capture(fixture.cacheJobID, fixture.cacheJobID); evidence.Equivalent || evidence.SourceReady {
		t.Fatal("one cached Job must not count as a two-Job miss/hit campaign")
	}
	if evidence := capture(uuid.New(), fixture.cacheJobID); evidence.SourceReady || evidence.TargetReused || evidence.Equivalent {
		t.Fatal("unrelated source identity must not count as cache provenance")
	}
}

func completeLabCacheSource(t *testing.T, fixture h3CampaignEvidenceFixture) {
	t.Helper()
	database := fixture.database
	registry, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
	if err != nil {
		t.Fatal(err)
	}
	coordinator, err := attemptcoordinator.NewService(newRolePool(t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password"))
	if err != nil {
		t.Fatal(err)
	}
	artifacts, err := stageartifact.NewPostgresRepository(newRolePool(t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password"))
	if err != nil {
		t.Fatal(err)
	}
	var attemptID, runID uuid.UUID
	var version int64
	if err := database.Admin.QueryRow(`
		SELECT run.attempt_id, run.id, run.version FROM stage_runs AS run
		JOIN attempts AS attempt ON attempt.id = run.attempt_id
		WHERE attempt.job_id = $1 AND run.stage_key = 'vae'
	`, fixture.sourceJobID).Scan(&attemptID, &runID, &version); err != nil {
		t.Fatal(err)
	}
	stage := h3IntegrationStage{
		key: "vae", stage: h3stage.StageVAEDecoder,
		profileStableID: h3stage.VAESingleGPUProfile,
		profileID:       h3VAEStageProfileID, workerProfileID: h3VAEWorkerProfileID,
		component: "h3-vae-v1", outputPort: "video",
		outputInterface: "49000000-0000-0000-0000-000000000013",
		nodeIdentity:    "lab-cache-source-vae", contentType: "video/mp4",
		residencyPlanID: uuid.MustParse(campaignResidencyPlanID),
	}
	assignment := assignH3IntegrationStage(t, database, coordinator, registry, attemptID, runID, version, stage, 0xf4)
	authority := signedAssignedStageAuthority(t, database, jobResponse{JobID: fixture.sourceJobID.String()}, assignment, version+1)
	_ = startH3IntegrationStage(t, database, assignment, authority)
	_ = materializeH3IntegrationStage(t, artifacts, fixture.objectStore, attemptID, runID, assignment, stage,
		[]byte("lab cache source VAE output"), []byte(`{"kind":"video","campaign":"lab-cache-source"}`))
	completeH3CampaignGraph(t, visibleCompletionService(t, database.DSN, fixture.objectStore), fixture.sourceJobID)
}
