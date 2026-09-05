//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagecache"
)

func TestStageCacheHitsFeedPhysicalDownstreamAssignmentsAndLineage(t *testing.T) {
	fixture := newPinnedStageCacheFixture(t, "cached-downstream-assignment")
	database := fixture.database
	if _, err := database.Admin.Exec(`
		UPDATE credentials SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel'] WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatal(err)
	}
	artifacts, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatal(err)
	}
	if response := cancelJob(t, fixture.serverURL, testProjectID, fixture.sourceJobID.String(),
		testBearerCredential()); response.StatusCode != http.StatusOK {
		t.Fatalf("cancel completed cache-source work: %s", response.Body)
	}
	stages := h3IntegrationStages([]string{"cached-encoder", "physical-dit", "physical-vae"}, nil)
	var firstAttemptID uuid.UUID
	if err := database.Admin.QueryRow(`SELECT attempt_id FROM stage_runs WHERE id = $1`, fixture.targetRunID).
		Scan(&firstAttemptID); err != nil {
		t.Fatal(err)
	}
	firstArtifact := executeCachedInputConsumer(t, fixture, artifacts, firstAttemptID,
		stages[1], fixture.hit.ArtifactID, []byte("pinned exact encoder conditioning"), 0xe1)
	var equivalenceID uuid.UUID
	if err := database.Admin.QueryRow(`SELECT result_equivalence_revision_id FROM stage_profile_revisions WHERE id = $1`,
		ditStageProfileID).Scan(&equivalenceID); err != nil {
		t.Fatal(err)
	}
	ditKey := sha256.Sum256([]byte("cached-downstream-assignment/dit"))
	ditEntryID := uuid.New()
	now := time.Now().UTC()
	if _, err := fixture.cache.Admit(context.Background(), stagecache.AdmitCommand{
		CommandID: uuid.New(), EntryID: ditEntryID, ArtifactID: firstArtifact.ID,
		CachePolicyRevisionID:  uuid.MustParse(h3CachePolicyID),
		StageProfileRevisionID: uuid.MustParse(ditStageProfileID), ResultEquivalenceRevisionID: equivalenceID,
		Scope: stagecache.ScopeProject, StageKey: "dit", CacheKeyDigest: ditKey,
		ExpectedSavedComputeMinor: 10000, CarryCostMinor: 10,
		AdmittedAt: now, ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("admit physically computed DiT output: %v", err)
	}
	if response := cancelJob(t, fixture.serverURL, testProjectID, fixture.targetJobID.String(),
		testBearerCredential()); response.StatusCode != http.StatusOK {
		t.Fatalf("cancel first target after DiT: %s", response.Body)
	}
	_, secondAttemptID := instantiateH3IntegrationGraph(t, database, fixture.serverURL, "cached-downstream-second-target")
	for _, hit := range []struct {
		stageKey string
		profile  string
		entry    uuid.UUID
		key      stagecache.Digest
		version  int64
	}{
		{"encoder", encoderStageProfileID, fixture.entryID, fixture.cacheKey, 1},
		{"dit", ditStageProfileID, ditEntryID, ditKey, 2},
	} {
		var runID uuid.UUID
		if err := database.Admin.QueryRow(`SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = $2`,
			secondAttemptID, hit.stageKey).Scan(&runID); err != nil {
			t.Fatal(err)
		}
		if _, err := fixture.cache.Hit(context.Background(), stagecache.HitCommand{
			CommandID: uuid.New(), EntryID: hit.entry, PinID: uuid.New(), AttemptID: secondAttemptID,
			StageRunID: runID, StageProfileRevisionID: uuid.MustParse(hit.profile),
			ExpectedOrganizationID: uuid.MustParse(testOrganizationID), ExpectedProjectID: uuid.MustParse(testProjectID),
			ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: hit.version,
			ProgressReceiptID: uuid.New(), CacheKeyDigest: hit.key, HitAt: time.Now().UTC(),
		}); err != nil {
			t.Fatalf("hit second target %s: %v", hit.stageKey, err)
		}
	}
	finalArtifact := executeCachedInputConsumer(t, fixture, artifacts, secondAttemptID,
		stages[2], firstArtifact.ID, []byte("physical-dit output after cached input"), 0xe2)
	var physicalCachedStages int
	var graphState string
	if err := database.Admin.QueryRow(`
		SELECT attempt.graph_state::text,
		       (SELECT count(*) FROM stage_attempts AS physical
		        JOIN stage_runs AS run ON run.id = physical.stage_run_id
		        WHERE run.attempt_id = attempt.id AND run.stage_key IN ('encoder', 'dit'))
		FROM attempts AS attempt WHERE attempt.id = $1
	`, secondAttemptID).Scan(&graphState, &physicalCachedStages); err != nil {
		t.Fatal(err)
	}
	if graphState != "FINALIZING" || physicalCachedStages != 0 || finalArtifact.ID == uuid.Nil {
		t.Fatalf("hybrid cache graph=%s cached physical count=%d artifact=%s", graphState, physicalCachedStages, finalArtifact.ID)
	}
}

func executeCachedInputConsumer(t *testing.T, fixture pinnedStageCacheFixture,
	artifacts *stageartifact.PostgresRepository, attemptID uuid.UUID,
	stage h3IntegrationStage, inputArtifactID uuid.UUID, inputPayload []byte, identityByte byte,
) stageartifact.Artifact {
	t.Helper()
	database := fixture.database
	var runID uuid.UUID
	if err := database.Admin.QueryRow(`SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = $2`,
		attemptID, stage.key).Scan(&runID); err != nil {
		t.Fatal(err)
	}
	worker := newH3AssignmentWorkerFixture(t, database, fixture.coordinator, runID, stage, identityByte)
	backend := newPostgresAssignmentTestBackend(t, worker)
	command := stageWorkerAcquireCommand(worker)
	request := stageWorkerAcquireRequest(worker)
	result, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || result.Assignment == nil {
		t.Fatalf("actual cached-input %s assignment=%#v error=%v", stage.key, result, err)
	}
	assignment := result.Assignment
	if assignment.GetAuthority().GetStageRunId() != runID.String() {
		t.Fatalf("%s acquired wrong StageRun %s", stage.key, assignment.GetAuthority().GetStageRunId())
	}
	inputs := assignment.GetExecutionSpec().GetInputs()
	tickets := assignment.GetInputTransferTickets()
	if len(inputs) != 1 || len(tickets) != 1 || inputs[0].GetStageArtifactId() != inputArtifactID.String() {
		t.Fatalf("%s cache input/ticket bindings=%#v/%#v", stage.key, inputs, tickets)
	}
	replayed, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || replayed.Assignment == nil ||
		!bytes.Equal(replayed.Assignment.GetInputTransferTickets()[0].GetTransferTicket(), tickets[0].GetTransferTicket()) {
		t.Fatalf("%s durable assignment replay=%#v error=%v", stage.key, replayed, err)
	}
	keys := map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}
	ticketSigner, err := stageartifact.NewTransferTicketKeyringSigner("stage-authority-key-v1", keys)
	if err != nil {
		t.Fatal(err)
	}
	ticket := stageartifact.SignedTransferTicket{Token: tickets[0].GetTransferTicket()}
	claims, err := ticketSigner.Verify(ticket, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	connector, err := stageartifact.NewObjectStorePullConnector(fixture.objectStore, artifacts, ticketSigner, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	destination := stageartifact.NewMemoryTransferTarget()
	if _, err := connector.Pull(context.Background(), ticket, claims.Destination, destination); err != nil {
		t.Fatalf("pull %s actual assignment input: %v", stage.key, err)
	}
	if !bytes.Equal(destination.Bytes(), inputPayload) {
		t.Fatalf("%s input bytes=%q want=%q", stage.key, destination.Bytes(), inputPayload)
	}
	validator, err := stageauthority.NewValidator(keys, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := validator.ValidateEnvelope(assignment.GetAuthority())
	if err != nil {
		t.Fatal(err)
	}
	authority := verified.Authority
	physical := attemptcoordinator.AssignStageCommand{
		AttemptID: attemptID, StageRunID: runID, ExpectedStageVersion: authority.GetStageVersion() - 1,
		StageAttemptID:         uuid.MustParse(authority.GetStageAttemptId()),
		StageAllocationID:      uuid.MustParse(authority.GetStageAllocationId()),
		StageLeaseID:           uuid.MustParse(authority.GetStageLeaseId()),
		WorkerInstanceID:       uuid.MustParse(authority.GetWorkerInstanceId()),
		WorkerInstanceEpoch:    authority.GetWorkerInstanceEpoch(),
		ModelResidencyID:       uuid.MustParse(authority.GetModelResidencyId()),
		ModelRuntimeEpoch:      authority.GetModelRuntimeBarrierGeneration(),
		StageProfileRevisionID: uuid.MustParse(authority.GetStageProfileRevisionId()),
		IssuedAt:               authority.GetIssuedAt().AsTime(),
	}
	startH3IntegrationStage(t, database, physical, verified)
	artifact := materializeH3IntegrationStage(t, artifacts, fixture.objectStore, attemptID, runID,
		physical, stage, []byte("physical-"+stage.key+" output after cached input"), []byte(`{"cache_input":true}`))
	var lineage uuid.UUID
	if err := database.Admin.QueryRow(`SELECT input_stage_artifact_id FROM stage_artifact_inputs WHERE stage_artifact_id = $1`,
		artifact.ID).Scan(&lineage); err != nil || lineage != inputArtifactID {
		t.Fatalf("%s committed input lineage=%s want=%s error=%v", stage.key, lineage, inputArtifactID, err)
	}
	return artifact
}

func TestStageCacheDownstreamBindingMigrationRoundTrip(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 72); err != nil {
		t.Fatalf("rollback cache downstream binding: %v", err)
	}
	if err := goose.Up(database.Admin, migrations); err != nil {
		t.Fatalf("reapply cache downstream binding: %v", err)
	}
}
