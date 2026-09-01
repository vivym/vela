//go:build integration

package integration_test

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/cpumedia"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/h3stage"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stagefinalization"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

const (
	cpuMediaGraphID             = "49700000-0000-0000-0000-000000000001"
	cpuMediaExecutionProfileID  = "49700000-0000-0000-0000-000000000070"
	cpuEncodeWorkerProfileID    = "49700000-0000-0000-0000-000000000021"
	cpuMuxWorkerProfileID       = "49700000-0000-0000-0000-000000000022"
	cpuThumbnailWorkerProfileID = "49700000-0000-0000-0000-000000000023"
	cpuEncodeStageProfileID     = "49700000-0000-0000-0000-000000000041"
	cpuMuxStageProfileID        = "49700000-0000-0000-0000-000000000042"
	cpuThumbnailStageProfileID  = "49700000-0000-0000-0000-000000000043"
)

type cpuMediaGraphOutcome struct {
	database  testDatabase
	jobID     uuid.UUID
	attemptID uuid.UUID
	video     stageartifact.Artifact
	thumbnail stageartifact.Artifact
}

func TestCPUMediaStageWorkersProduceAtomicVisibleCompletionWithoutLegacyLease(t *testing.T) {
	outcome := runCPUMediaH3Graph(t)
	service := visibleCompletionService(t, outcome.database.DSN)
	finalizer := stagefinalization.AuthenticatedFinalizer{
		ID: "spiffe://vela.internal/finalizer/h3-cpu-media",
	}
	claim, err := service.ClaimNextStageGraphFinalization(context.Background(), finalizer)
	if err != nil {
		t.Fatalf("claim CPU media graph finalization: %v", err)
	}
	if claim.Decision != stagefinalization.StageGraphFinalizationGranted ||
		len(claim.Sources) != 2 ||
		claim.Sources[0].ArtifactKind != stagefinalization.ArtifactKindThumbnail ||
		claim.Sources[0].StageArtifactID != outcome.thumbnail.ID ||
		claim.Sources[1].ArtifactKind != stagefinalization.ArtifactKindVideo ||
		claim.Sources[1].StageArtifactID != outcome.video.ID {
		t.Fatalf("CPU media graph finalization claim = %#v", claim)
	}
	completionID := uuid.New()
	completed, err := service.CompleteStageGraphVisibleCompletion(
		context.Background(), finalizer, claim.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{
			CompletionID: completionID, ExpectedJobVersion: claim.JobVersion,
		},
	)
	if err != nil {
		t.Fatalf("complete CPU media graph Visible Completion: %v", err)
	}
	if completed.Decision != stagefinalization.VisibleCompletionCommitted ||
		completed.CompletionID != completionID || completed.ArtifactSetID == uuid.Nil ||
		completed.ChargeID == uuid.Nil || len(completed.Artifacts) != 2 {
		t.Fatalf("CPU media graph Visible Completion = %#v", completed)
	}

	replayed, err := service.CompleteStageGraphVisibleCompletion(
		context.Background(), finalizer, claim.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{
			CompletionID: completionID, ExpectedJobVersion: claim.JobVersion,
		},
	)
	if err != nil || replayed.Decision != stagefinalization.VisibleCompletionCommitted ||
		replayed.ArtifactSetID != completed.ArtifactSetID || replayed.ChargeID != completed.ChargeID {
		t.Fatalf("replay CPU media graph Visible Completion = %#v error=%v", replayed, err)
	}

	var (
		jobState, attemptState, graphState, claimState string
		authorityLeaseID, authorityClaimID             *uuid.UUID
		sets, charges, grants, completions             int
		sourcedArtifacts                               int
	)
	if err := outcome.database.Admin.QueryRow(`
		SELECT job.state::text, attempt.state::text, attempt.graph_state::text,
		       claim.state::text,
		       (SELECT count(*) FROM artifact_sets WHERE attempt_id = attempt.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id),
		       (SELECT count(*) FROM artifact_access_grants WHERE job_id = job.id),
		       (SELECT count(*) FROM visible_completions WHERE job_id = job.id),
		       (SELECT authority_lease_id FROM visible_completions WHERE job_id = job.id),
		       (SELECT authority_stage_graph_finalization_claim_id
		          FROM visible_completions WHERE job_id = job.id),
		       (SELECT count(*) FROM artifacts
		          WHERE job_id = job.id AND source_stage_artifact_id IS NOT NULL)
		FROM attempts AS attempt
		JOIN jobs AS job ON job.id = attempt.job_id
		JOIN stage_graph_finalization_claims AS claim ON claim.attempt_id = attempt.id
		WHERE attempt.id = $1
	`, outcome.attemptID).Scan(
		&jobState, &attemptState, &graphState, &claimState,
		&sets, &charges, &grants, &completions,
		&authorityLeaseID, &authorityClaimID, &sourcedArtifacts,
	); err != nil {
		t.Fatalf("inspect CPU media Visible Completion: %v", err)
	}
	if jobState != "SUCCEEDED" || attemptState != "SUCCEEDED" || graphState != "SUCCEEDED" ||
		claimState != "COMPLETED" || sets != 1 ||
		charges != 1 || grants != 1 || completions != 1 || authorityLeaseID != nil ||
		authorityClaimID == nil || *authorityClaimID != claim.ClaimID || sourcedArtifacts != 2 {
		t.Fatalf(
			"CPU media completion state job=%s attempt=%s graph=%s claim=%s sets=%d charges=%d grants=%d completions=%d authority=%v/%v sourced=%d",
			jobState, attemptState, graphState, claimState,
			sets, charges, grants, completions, authorityLeaseID, authorityClaimID,
			sourcedArtifacts,
		)
	}
	if _, err := outcome.database.Admin.Exec(`
		UPDATE artifacts
		SET source_stage_artifact_id = NULL
		WHERE attempt_id = $1 AND source_stage_artifact_id IS NOT NULL
	`, outcome.attemptID); err == nil {
		t.Fatal("StageArtifact provenance mutation succeeded")
	} else {
		assertPostgresConstraint(t, err, "artifact_source_stage_artifact_immutable")
	}
}

func TestStageGraphVisibleCompletionRejectsInspectionMismatchAtomically(t *testing.T) {
	outcome := runCPUMediaH3Graph(t)
	service := stageGraphVisibleCompletionService(t, outcome.database.DSN,
		artifactInspectorFunc(func(
			_ context.Context,
			request stagefinalization.ArtifactInspectionRequest,
		) (stagefinalization.ArtifactInspection, error) {
			inspection := validInspectionForRequest(request)
			inspection.SHA256[0] ^= 0xff
			return inspection, nil
		}),
	)
	finalizer := stagefinalization.AuthenticatedFinalizer{
		ID: "spiffe://vela.internal/finalizer/h3-inspection-mismatch",
	}
	claim, err := service.ClaimNextStageGraphFinalization(context.Background(), finalizer)
	if err != nil || claim.Decision != stagefinalization.StageGraphFinalizationGranted {
		t.Fatalf("claim mismatched Stage graph finalization = %#v error=%v", claim, err)
	}
	_, err = service.CompleteStageGraphVisibleCompletion(
		context.Background(), finalizer, claim.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{
			CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion,
		},
	)
	if err == nil || !strings.Contains(err.Error(), "inspection mismatch") {
		t.Fatalf("mismatched Stage graph inspection error = %v", err)
	}
	assertNoStageGraphVisibleCompletionWrites(t, outcome)
}

func TestStageGraphVisibleCompletionRejectsExpiredClaim(t *testing.T) {
	outcome := runCPUMediaH3Graph(t)
	service := visibleCompletionService(t, outcome.database.DSN)
	finalizer := stagefinalization.AuthenticatedFinalizer{
		ID: "spiffe://vela.internal/finalizer/h3-expired-claim",
	}
	claim, err := service.ClaimNextStageGraphFinalization(context.Background(), finalizer)
	if err != nil || claim.Decision != stagefinalization.StageGraphFinalizationGranted {
		t.Fatalf("claim expiring Stage graph finalization = %#v error=%v", claim, err)
	}
	if _, err := outcome.database.Admin.Exec(`
		UPDATE stage_graph_finalization_claims
		SET expires_at = issued_at + interval '1 microsecond'
		WHERE id = $1
	`, claim.ClaimID); err != nil {
		t.Fatalf("expire Stage graph finalization claim: %v", err)
	}
	result, err := service.CompleteStageGraphVisibleCompletion(
		context.Background(), finalizer, claim.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{
			CompletionID: uuid.New(), ExpectedJobVersion: claim.JobVersion,
		},
	)
	if err != nil || result.Decision != stagefinalization.VisibleCompletionRejectedStaleLease {
		t.Fatalf("expired Stage graph Visible Completion = %#v error=%v", result, err)
	}
	assertNoStageGraphVisibleCompletionWrites(t, outcome)
}

func TestStageGraphVisibleCompletionMigrationRoundTripAndEvidenceRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 46)
		if err := goose.DownTo(database.Admin, migrations, 45); err != nil {
			t.Fatalf("migrate empty Stage graph Visible Completion down: %v", err)
		}
		var removed bool
		if err := database.Admin.QueryRow(`
			SELECT NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'visible_completions'
				  AND column_name = 'authority_stage_graph_finalization_claim_id'
			) AND NOT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public' AND table_name = 'artifacts'
				  AND column_name = 'source_stage_artifact_id'
			)
		`).Scan(&removed); err != nil || !removed {
			t.Fatalf("inspect Stage graph Visible Completion rollback = %t error=%v", removed, err)
		}
		if err := goose.UpTo(database.Admin, migrations, 46); err != nil {
			t.Fatalf("migrate Stage graph Visible Completion back up: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 46 {
			t.Fatalf("Stage graph Visible Completion version after Up = %d error=%v", version, err)
		}
	})

}

func stageGraphVisibleCompletionService(
	t *testing.T,
	dsn string,
	inspector stagefinalization.ArtifactInspector,
) *stagefinalization.Service {
	t.Helper()
	service, err := stagefinalization.NewService(
		context.Background(),
		newRolePool(t, dsn, "vela_internal_login", "vela-internal-password"),
		stagefinalization.Config{
			LeaseTTL: 2 * time.Minute, ActiveLeaseKeyID: "lease-key-v1",
			LeaseKeys: map[string][]byte{
				"lease-key-v1": []byte("0123456789abcdef0123456789abcdef"),
			},
			ArtifactInspector: inspector,
		},
	)
	if err != nil {
		t.Fatalf("create Stage graph Visible Completion coordinator: %v", err)
	}
	return service
}

func assertNoStageGraphVisibleCompletionWrites(t *testing.T, outcome cpuMediaGraphOutcome) {
	t.Helper()
	var jobState, attemptState, claimState string
	var artifacts, sets, charges, grants, completions int
	if err := outcome.database.Admin.QueryRow(`
		SELECT job.state::text, attempt.state::text, claim.state::text,
		       (SELECT count(*) FROM artifacts WHERE job_id = job.id),
		       (SELECT count(*) FROM artifact_sets WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id),
		       (SELECT count(*) FROM artifact_access_grants WHERE job_id = job.id),
		       (SELECT count(*) FROM visible_completions WHERE job_id = job.id)
		FROM attempts AS attempt
		JOIN jobs AS job ON job.id = attempt.job_id
		JOIN stage_graph_finalization_claims AS claim ON claim.attempt_id = attempt.id
		WHERE attempt.id = $1
	`, outcome.attemptID).Scan(
		&jobState, &attemptState, &claimState,
		&artifacts, &sets, &charges, &grants, &completions,
	); err != nil {
		t.Fatalf("inspect rejected Stage graph Visible Completion: %v", err)
	}
	if jobState != "FINALIZING" || attemptState != "FINALIZING" || claimState != "ACTIVE" ||
		artifacts != 0 || sets != 0 || charges != 0 || grants != 0 || completions != 0 {
		t.Fatalf(
			"rejected Stage graph completion state=%s/%s/%s writes artifacts/sets/charges/grants/completions=%d/%d/%d/%d/%d",
			jobState, attemptState, claimState, artifacts, sets, charges, grants, completions,
		)
	}
}

func runCPUMediaH3Graph(t *testing.T) cpuMediaGraphOutcome {
	t.Helper()
	return runCPUMediaH3GraphWithKey(t, "cpu-media-stage-graph")
}

func runCPUMediaH3GraphWithKey(t *testing.T, idempotencyKey string) cpuMediaGraphOutcome {
	t.Helper()
	database, coordinator, serverURL := newH3IntegrationEnvironment(t)
	seedCPUMediaExecutionGraph(t, database)
	seedCPUMediaAdmissionCapacityPath(t, database)
	activateStageCutoverRevision(
		t, database, uuid.MustParse("49700000-0000-0000-0000-000000000049"), 3,
		uuid.MustParse(stageCutoverRevisionID), uuid.MustParse(cpuMediaGraphID),
		uuid.MustParse(cpuMediaExecutionProfileID), 4<<30,
		"integration-cpu-media-stage-cutover",
	)
	accepted := submitJob(t, serverURL, idempotencyKey, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"cpu media graph"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit CPU media Job status = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode CPU media Job: %v", err)
	}
	jobID, err := uuid.Parse(job.JobID)
	if err != nil {
		t.Fatalf("parse CPU media Job ID: %v", err)
	}
	instantiation := readStageGraphInstantiation(t, database.Admin, job.JobID)
	attemptID := instantiation.AttemptID

	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("construct CPU media Worker Registry: %v", err)
	}
	stageArtifacts, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct CPU media StageArtifact repository: %v", err)
	}
	objectStore := artifactstore.NewLocal()
	transferSigner, err := stageartifact.NewTransferTicketSigner(
		"stage-transfer-key-v1", []byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("construct CPU media TransferTicket signer: %v", err)
	}
	transferIssuer, err := stageartifact.NewTransferTicketIssuer(stageArtifacts, transferSigner)
	if err != nil {
		t.Fatalf("construct CPU media TransferTicket issuer: %v", err)
	}

	stages := map[string]h3IntegrationStage{
		"encoder": {
			key: "encoder", stage: h3stage.StageEncoder,
			profileStableID: h3stage.EncoderSingleGPUProfile,
			profileID:       encoderStageProfileID, workerProfileID: encoderWorkerProfileID,
			component: "h3-encoder-v1", outputPort: "conditioning",
			outputInterface: "49000000-0000-0000-0000-000000000011",
			nodeIdentity:    "encoder-node-01",
		},
		"dit": {
			key: "dit", stage: h3stage.StageDiT,
			profileStableID: h3stage.DiTSingleGPUProfile,
			profileID:       ditStageProfileID, workerProfileID: ditWorkerProfileID,
			component: "h3-dit-v1", outputPort: "latent",
			outputInterface: "49000000-0000-0000-0000-000000000012",
			nodeIdentity:    "dit-node-01",
		},
		"vae": {
			key: "vae", stage: h3stage.StageVAEDecoder,
			profileStableID: h3stage.VAESingleGPUProfile,
			profileID:       h3VAEStageProfileID, workerProfileID: h3VAEWorkerProfileID,
			component: "h3-vae-v1", outputPort: "video",
			outputInterface: "49000000-0000-0000-0000-000000000013",
			nodeIdentity:    "vae-node-01",
		},
		"encode": cpuIntegrationStage(t, cpumedia.EncodeProfile, cpuEncodeStageProfileID,
			cpuEncodeWorkerProfileID, "encoded", "49700000-0000-0000-0000-000000000014"),
		"mux": cpuIntegrationStage(t, cpumedia.MuxProfile, cpuMuxStageProfileID,
			cpuMuxWorkerProfileID, "video", "49700000-0000-0000-0000-000000000015"),
		"thumbnail": cpuIntegrationStage(t, cpumedia.ThumbnailProfile,
			cpuThumbnailStageProfileID, cpuThumbnailWorkerProfileID,
			"thumbnail", "49700000-0000-0000-0000-000000000016"),
	}
	stages["mux"] = withContentType(stages["mux"], "video/mp4")
	stages["thumbnail"] = withContentType(stages["thumbnail"], "image/webp")

	artifacts := make(map[string]stageartifact.Artifact, len(stages))
	payloads := make(map[string][]byte, len(stages))
	order := []string{"encoder", "dit", "vae", "encode", "thumbnail", "mux"}
	inputs := map[string]string{
		"dit": "encoder", "vae": "dit", "encode": "vae", "thumbnail": "vae", "mux": "encode",
	}
	connectors := map[string]uuid.UUID{
		"dit":       uuid.MustParse("49700000-0000-0000-0000-000000000050"),
		"vae":       uuid.MustParse("49700000-0000-0000-0000-000000000051"),
		"encode":    uuid.MustParse("49700000-0000-0000-0000-000000000052"),
		"mux":       uuid.MustParse("49700000-0000-0000-0000-000000000053"),
		"thumbnail": uuid.MustParse("49700000-0000-0000-0000-000000000054"),
	}
	for index, key := range order {
		stage := stages[key]
		var stageRunID uuid.UUID
		var stageVersion int64
		if err := database.Admin.QueryRow(`
			SELECT id, version FROM stage_runs WHERE attempt_id = $1 AND stage_key = $2
		`, attemptID, key).Scan(&stageRunID, &stageVersion); err != nil {
			t.Fatalf("read CPU graph %s StageRun: %v", key, err)
		}
		assignment := assignH3IntegrationStage(
			t, database, coordinator, registry, attemptID, stageRunID,
			stageVersion, stage, byte(0xd0+index),
		)
		var input *velav1.StageInputArtifact
		if sourceKey := inputs[key]; sourceKey != "" {
			source := artifacts[sourceKey]
			input = &velav1.StageInputArtifact{
				StageArtifactId: source.ID.String(), ObjectVersion: source.ObjectVersion,
				Sha256: source.SHA256[:], SizeBytes: source.SizeBytes,
				StageInterfaceRevisionId: stages[sourceKey].outputInterface,
			}
			pullH3IntegrationInput(
				t, database, stageArtifacts, objectStore, transferSigner, transferIssuer,
				stageRunID, assignment, input, payloads[sourceKey], connectors[key],
			)
		}
		payload := sha256.Sum256([]byte(fmt.Sprintf("cpu-media/%s/%x", key, index)))
		manifest := []byte(fmt.Sprintf(`{"kind":"%s","sha256":"%x"}`, stage.outputPort, payload))
		var backend modelruntime.Backend
		if stage.resourceClass == "CPU" {
			backend = cpuMediaIntegrationBackend(t, stage, payload[:], manifest)
		} else {
			backend = h3IntegrationBackend(t, stage, payload[:], manifest)
		}
		authority := signedAssignedStageAuthority(t, database, job, assignment, stageVersion+1)
		spec := &velav1.StageExecutionSpec{
			ParametersJson:             []byte(fmt.Sprintf(`{"stage":"%s"}`, key)),
			ExpectedOutputManifestJson: manifest,
		}
		if input != nil {
			spec.Inputs = []*velav1.StageInputArtifact{input}
		}
		if err := backend.Prepare(context.Background(), authority, spec); err != nil {
			t.Fatalf("prepare CPU graph %s: %v", key, err)
		}
		if err := backend.Start(context.Background(), authority); err != nil {
			t.Fatalf("start CPU graph %s: %v", key, err)
		}
		started := startH3IntegrationStage(t, database, assignment, authority)
		sealed, err := backend.Seal(context.Background(), started)
		if err != nil {
			t.Fatalf("seal CPU graph %s: %v", key, err)
		}
		artifact := materializeH3IntegrationStage(
			t, stageArtifacts, objectStore, attemptID, stageRunID, assignment,
			stage, payload[:], sealed.OutputManifestJSON,
		)
		artifacts[key] = artifact
		payloads[key] = append([]byte(nil), payload[:]...)
	}
	return cpuMediaGraphOutcome{
		database: database, jobID: jobID, attemptID: attemptID,
		video: artifacts["mux"], thumbnail: artifacts["thumbnail"],
	}
}

func seedCPUMediaAdmissionCapacityPath(t *testing.T, database testDatabase) {
	t.Helper()
	type stageCapacity struct {
		key             string
		resourceClass   string
		profileID       string
		workerProfileID string
		component       string
		poolID          uuid.UUID
		workerID        uuid.UUID
		identityByte    byte
		capacity        map[string]int64
	}
	stages := []stageCapacity{
		{
			key: "encoder", resourceClass: "GPU", profileID: encoderStageProfileID,
			workerProfileID: encoderWorkerProfileID, component: "h3-encoder-v1",
			poolID:       uuid.MustParse("49700000-0000-0000-0000-000000000081"),
			workerID:     uuid.MustParse("49700000-0000-0000-0000-000000000084"),
			identityByte: 0x81, capacity: map[string]int64{"concurrency": 1},
		},
		{
			key: "dit", resourceClass: "GPU", profileID: ditStageProfileID,
			workerProfileID: ditWorkerProfileID, component: "h3-dit-v1",
			poolID:       uuid.MustParse("49700000-0000-0000-0000-000000000082"),
			workerID:     uuid.MustParse("49700000-0000-0000-0000-000000000085"),
			identityByte: 0x82, capacity: map[string]int64{"concurrency": 1},
		},
		{
			key: "vae", resourceClass: "GPU", profileID: h3VAEStageProfileID,
			workerProfileID: h3VAEWorkerProfileID, component: "h3-vae-v1",
			poolID:       uuid.MustParse("49700000-0000-0000-0000-000000000083"),
			workerID:     uuid.MustParse("49700000-0000-0000-0000-000000000086"),
			identityByte: 0x83, capacity: map[string]int64{"concurrency": 1},
		},
		{
			key: "encode", resourceClass: "CPU", profileID: cpuEncodeStageProfileID,
			workerProfileID: cpuEncodeWorkerProfileID, component: "ffmpeg-encode-v1",
			poolID:       uuid.MustParse("49700000-0000-0000-0000-000000000091"),
			workerID:     uuid.MustParse("49700000-0000-0000-0000-000000000094"),
			identityByte: 0x94,
			capacity: map[string]int64{
				"cpu_milli": 4000, "memory_bytes": 8 << 30,
				"scratch_bytes": 128 << 30, "concurrency": 1,
			},
		},
		{
			key: "mux", resourceClass: "CPU", profileID: cpuMuxStageProfileID,
			workerProfileID: cpuMuxWorkerProfileID, component: "ffmpeg-mux-v1",
			poolID:       uuid.MustParse("49700000-0000-0000-0000-000000000092"),
			workerID:     uuid.MustParse("49700000-0000-0000-0000-000000000095"),
			identityByte: 0x95,
			capacity: map[string]int64{
				"cpu_milli": 2000, "memory_bytes": 4 << 30,
				"scratch_bytes": 128 << 30, "concurrency": 1,
			},
		},
		{
			key: "thumbnail", resourceClass: "CPU", profileID: cpuThumbnailStageProfileID,
			workerProfileID: cpuThumbnailWorkerProfileID, component: "ffmpeg-thumbnail-v1",
			poolID:       uuid.MustParse("49700000-0000-0000-0000-000000000093"),
			workerID:     uuid.MustParse("49700000-0000-0000-0000-000000000096"),
			identityByte: 0x96,
			capacity: map[string]int64{
				"cpu_milli": 2000, "memory_bytes": 4 << 30,
				"scratch_bytes": 32 << 30, "concurrency": 1,
			},
		},
	}
	bundleID := uuid.MustParse("49700000-0000-0000-0000-000000000090")
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_bundles (
			id, stable_id, plan_revision, desired_generation, observed_generation,
			lifecycle_state, layout_digest, approved_by
		) VALUES (
			$1, 'h3-cpu-media-admission-capacity', 'integration-ready-path-v1',
			1, 0, 'APPLYING', decode(repeat('97', 32), 'hex'), 'integration-fixture'
		)
	`, bundleID); err != nil {
		t.Fatalf("seed CPU media Admission WorkerBundle: %v", err)
	}
	for _, stage := range stages {
		if _, err := database.Admin.Exec(`
			INSERT INTO capacity_pools (
				id, stable_id, stage_profile_revision_id, resource_class,
				security_class, region, max_ready_queue_depth, state
			) VALUES ($1, $2, $3, $4, 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE')
		`, stage.poolID, "h3-cpu-media-admission-"+stage.key, stage.profileID,
			stage.resourceClass); err != nil {
			t.Fatalf("seed %s CPU media Admission CapacityPool: %v", stage.key, err)
		}
		if _, err := database.Admin.Exec(`
			INSERT INTO worker_instances (
				id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
				lifecycle_state, reachability_state, instance_epoch,
				control_session_epoch, desired_member_count, desired_device_count
			) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
		`, stage.workerID, stage.workerProfileID, stage.poolID, bundleID); err != nil {
			t.Fatalf("seed %s CPU media Admission WorkerInstance: %v", stage.key, err)
		}
	}
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("construct CPU media Admission Worker Registry: %v", err)
	}
	for _, stage := range stages {
		evidence := workerRegistryEvidenceValue(t, stage.workerID, stage.identityByte)
		nodeID := uuid.NewSHA1(stage.workerID, []byte("cpu-media-admission-node"))
		deviceID := uuid.NewSHA1(stage.workerID, []byte("cpu-media-admission-device"))
		evidence.DeviceSet.Devices[0].ID = deviceID
		evidence.DeviceSet.Devices[0].ComputeNodeID = nodeID
		evidence.DeviceSet.Devices[0].NodeIdentity = "h3-cpu-media-admission-" + stage.key
		if stage.resourceClass == "CPU" {
			evidence.DeviceSet.Devices[0].Kind = "CPU"
			evidence.DeviceSet.Devices[0].GPUUUID = ""
			evidence.DeviceSet.Devices[0].PCIBDF = ""
		} else {
			evidence.DeviceSet.Devices[0].GPUUUID = "GPU-" + stage.workerID.String()
			evidence.DeviceSet.Devices[0].PCIBDF = fmt.Sprintf(
				"0000:%02x:00.0", stage.identityByte,
			)
		}
		evidence.Members[0].ComputeNodeID = nodeID
		evidence.Members[0].DeviceIDs = []uuid.UUID{deviceID}
		evidence.Residencies[0].ModelComponentRevision = stage.component
		evidence.Residencies[0].RuntimeIdentity = stage.key + "@sha256:cpu-media-admission"
		evidence.Capacity.Vector = stage.capacity
		evidence.ObservedBy = "node-agent/h3-cpu-media-admission-" + stage.key
		if _, err := registry.Observe(context.Background(), evidence); err != nil {
			t.Fatalf("observe %s CPU media Admission READY capacity: %v", stage.key, err)
		}
	}
}

func cpuIntegrationStage(
	t *testing.T,
	stableID string,
	profileID string,
	workerProfileID string,
	outputPort string,
	outputInterface string,
) h3IntegrationStage {
	t.Helper()
	profiles, err := cpumedia.ProductionProfiles()
	if err != nil {
		t.Fatalf("load CPU media profiles: %v", err)
	}
	for _, profile := range profiles {
		if profile.StableID != stableID {
			continue
		}
		return h3IntegrationStage{
			key: string(profile.Stage), profileStableID: stableID,
			profileID: profileID, workerProfileID: workerProfileID,
			component: profile.RuntimeAdapterRevision, outputPort: outputPort,
			outputInterface: outputInterface, nodeIdentity: "cpu-node-01",
			resourceClass: "CPU", capacityVector: map[string]int64{
				"cpu_milli":     profile.CapacityLimits.CPUMilli,
				"memory_bytes":  profile.CapacityLimits.MemoryBytes,
				"scratch_bytes": profile.CapacityLimits.ScratchBytes,
				"concurrency":   profile.CapacityLimits.Concurrency,
			},
		}
	}
	t.Fatalf("missing CPU media profile %s", stableID)
	return h3IntegrationStage{}
}

func withContentType(stage h3IntegrationStage, contentType string) h3IntegrationStage {
	stage.contentType = contentType
	return stage
}

func h3IntegrationBackend(
	t *testing.T,
	stage h3IntegrationStage,
	payload []byte,
	manifest []byte,
) modelruntime.Backend {
	t.Helper()
	driver := &h3IntegrationDriver{
		residency: h3stage.Residency{
			Stage: stage.stage, StageProfileRevisionID: stage.profileID,
			ModelRevision: stage.component, BackendRevision: "sglang@sha256:h3-integration",
			PlacementEvidence: stage.nodeIdentity, Warm: true,
		},
		payload: append([]byte(nil), payload...), manifest: append([]byte(nil), manifest...),
	}
	adapter, err := h3stage.NewAdapter(h3stage.AdapterConfig{
		ProfileStableID:        stage.profileStableID,
		StageProfileRevisionID: stage.profileID,
		Driver:                 driver,
	})
	if err != nil {
		t.Fatalf("construct %s H3 adapter: %v", stage.key, err)
	}
	return adapter
}

func cpuMediaIntegrationBackend(
	t *testing.T,
	stage h3IntegrationStage,
	payload []byte,
	manifest []byte,
) modelruntime.Backend {
	t.Helper()
	driver := &cpuMediaIntegrationDriver{
		resources: cpumedia.ResourceVector{
			CPUMilli:     stage.capacityVector["cpu_milli"],
			MemoryBytes:  stage.capacityVector["memory_bytes"],
			ScratchBytes: stage.capacityVector["scratch_bytes"],
			Concurrency:  stage.capacityVector["concurrency"],
		},
		payload: append([]byte(nil), payload...), manifest: append([]byte(nil), manifest...),
	}
	adapter, err := cpumedia.NewAdapter(cpumedia.AdapterConfig{
		ProfileStableID:        stage.profileStableID,
		StageProfileRevisionID: stage.profileID,
		Driver:                 driver,
	})
	if err != nil {
		t.Fatalf("construct %s CPU media adapter: %v", stage.key, err)
	}
	return adapter
}

type cpuMediaIntegrationDriver struct {
	resources cpumedia.ResourceVector
	payload   []byte
	manifest  []byte
	prepared  bool
	started   bool
}

func (driver *cpuMediaIntegrationDriver) Resources() cpumedia.ResourceVector {
	return driver.resources
}

func (driver *cpuMediaIntegrationDriver) Probe(
	context.Context,
	velav1.ModelRuntimeReadinessCheck,
) (modelruntime.ProbeResult, error) {
	return modelruntime.ProbeResult{Ready: true}, nil
}

func (driver *cpuMediaIntegrationDriver) Prepare(
	_ context.Context,
	_ cpumedia.ExecutionIdentity,
	spec *velav1.StageExecutionSpec,
) error {
	if spec == nil || !json.Valid(spec.GetParametersJson()) {
		return errors.New("invalid CPU media integration spec")
	}
	driver.prepared = true
	return nil
}

func (driver *cpuMediaIntegrationDriver) Start(
	context.Context,
	cpumedia.ExecutionIdentity,
) error {
	if !driver.prepared {
		return errors.New("CPU media integration stage is not prepared")
	}
	driver.started = true
	return nil
}

func (driver *cpuMediaIntegrationDriver) Cancel(
	context.Context,
	cpumedia.ExecutionIdentity,
	velav1.ModelRuntimeCancelReason,
) error {
	driver.started = false
	return nil
}

func (driver *cpuMediaIntegrationDriver) Status(
	context.Context,
	cpumedia.ExecutionIdentity,
) (modelruntime.BackendStatus, error) {
	return modelruntime.BackendStatus{
		State: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY,
	}, nil
}

func (driver *cpuMediaIntegrationDriver) Seal(
	context.Context,
	cpumedia.ExecutionIdentity,
) (modelruntime.SealedOutput, error) {
	if !driver.started {
		return modelruntime.SealedOutput{}, errors.New("CPU media integration stage is not started")
	}
	return modelruntime.SealedOutput{
		OutputManifestJSON: append([]byte(nil), driver.manifest...),
		TotalSizeBytes:     int64(len(driver.payload)),
	}, nil
}

func seedCPUMediaExecutionGraph(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO stage_interface_revisions (
			id, stable_id, revision, state, payload_kind, dtype, layout,
			shape_contract, serialization, max_bytes, digest_algorithm,
			schema_digest, content_digest
		) VALUES
			('49700000-0000-0000-0000-000000000014', 'h3-encoded-video', 1, 'CERTIFIED',
			 'video', '', 'encoded-packets', '{}', 'packet-bundle', 8589934592, 'sha256',
			 decode(repeat('14', 32), 'hex'), decode(repeat('24', 32), 'hex')),
			('49700000-0000-0000-0000-000000000015', 'h3-final-video', 1, 'CERTIFIED',
			 'video', '', 'mp4', '{}', 'mp4', 8589934592, 'sha256',
			 decode(repeat('15', 32), 'hex'), decode(repeat('25', 32), 'hex')),
			('49700000-0000-0000-0000-000000000016', 'h3-thumbnail', 1, 'CERTIFIED',
			 'image', '', 'webp', '{}', 'webp', 16777216, 'sha256',
			 decode(repeat('16', 32), 'hex'), decode(repeat('26', 32), 'hex'));

		INSERT INTO stage_result_equivalence_revisions (
			id, stable_id, revision, state, exact_contract, evidence_receipt_ref,
			evidence_digest, content_digest
		) VALUES
			('49700000-0000-0000-0000-000000000031', 'h3-encode-exact', 1, 'CERTIFIED',
			 '{"mode":"BITWISE"}', 'receipt://h3/cpu/encode',
			 decode(repeat('31', 32), 'hex'), decode(repeat('41', 32), 'hex')),
			('49700000-0000-0000-0000-000000000032', 'h3-mux-exact', 1, 'CERTIFIED',
			 '{"mode":"BITWISE"}', 'receipt://h3/cpu/mux',
			 decode(repeat('32', 32), 'hex'), decode(repeat('42', 32), 'hex')),
			('49700000-0000-0000-0000-000000000033', 'h3-thumbnail-exact', 1, 'CERTIFIED',
			 '{"mode":"BITWISE"}', 'receipt://h3/cpu/thumbnail',
			 decode(repeat('33', 32), 'hex'), decode(repeat('43', 32), 'hex'));

		INSERT INTO worker_profile_revisions (
			id, stable_id, revision, state, device_count, member_count,
			device_set_shape, resident_model_revisions, capacity_limits,
			readiness_checks, content_digest
		) VALUES
			('49700000-0000-0000-0000-000000000021', 'h3-cpu-encode-worker', 1, 'CERTIFIED', 1, 1,
			 '{"kind":"cpu-slot"}', '["ffmpeg-encode-v1"]',
			 '{"cpu_milli":4000,"memory_bytes":8589934592,"scratch_bytes":137438953472,"concurrency":1}',
			 '{"process":true}', decode(repeat('51', 32), 'hex')),
			('49700000-0000-0000-0000-000000000022', 'h3-cpu-mux-worker', 1, 'CERTIFIED', 1, 1,
			 '{"kind":"cpu-slot"}', '["ffmpeg-mux-v1"]',
			 '{"cpu_milli":2000,"memory_bytes":4294967296,"scratch_bytes":137438953472,"concurrency":1}',
			 '{"process":true}', decode(repeat('52', 32), 'hex')),
			('49700000-0000-0000-0000-000000000023', 'h3-cpu-thumbnail-worker', 1, 'CERTIFIED', 1, 1,
			 '{"kind":"cpu-slot"}', '["ffmpeg-thumbnail-v1"]',
			 '{"cpu_milli":2000,"memory_bytes":4294967296,"scratch_bytes":34359738368,"concurrency":1}',
			 '{"process":true}', decode(repeat('53', 32), 'hex'));

		INSERT INTO stage_definition_revisions (
			id, stable_id, revision, state, stage_kind, input_ports, output_ports,
			required_input_ports, required_output_ports, resource_class, retry_class,
			cache_policy_revision_id, checkpoint_policy_revision_id, public_phase,
			content_digest
		) VALUES
			('49700000-0000-0000-0000-000000000034', 'h3-cpu-encode', 1, 'CERTIFIED',
			 'ENCODE', '{"frames":"49000000-0000-0000-0000-000000000013"}',
			 '{"encoded":"49700000-0000-0000-0000-000000000014"}',
			 ARRAY['frames'], ARRAY['encoded'], 'CPU', 'STAGE_RETRY', NULL,
			 '49000000-0000-0000-0000-000000000021', 'FINALIZING', decode(repeat('61', 32), 'hex')),
			('49700000-0000-0000-0000-000000000035', 'h3-cpu-mux', 1, 'CERTIFIED',
			 'MUX', '{"encoded":"49700000-0000-0000-0000-000000000014"}',
			 '{"video":"49700000-0000-0000-0000-000000000015"}',
			 ARRAY['encoded'], ARRAY['video'], 'CPU', 'STAGE_RETRY', NULL,
			 '49000000-0000-0000-0000-000000000021', 'FINALIZING', decode(repeat('62', 32), 'hex')),
			('49700000-0000-0000-0000-000000000036', 'h3-cpu-thumbnail', 1, 'CERTIFIED',
			 'THUMBNAIL', '{"frames":"49000000-0000-0000-0000-000000000013"}',
			 '{"thumbnail":"49700000-0000-0000-0000-000000000016"}',
			 ARRAY['frames'], ARRAY['thumbnail'], 'CPU', 'STAGE_RETRY', NULL,
			 '49000000-0000-0000-0000-000000000021', 'FINALIZING', decode(repeat('63', 32), 'hex'));

		INSERT INTO stage_profile_revisions (
			id, stable_id, revision, state, stage_definition_revision_id,
			model_component_revision, runtime_image_digest, worker_profile_revision_id,
			result_equivalence_revision_id, certified_capacity_vector, content_digest
		) VALUES
			('49700000-0000-0000-0000-000000000041', 'h3-cpu-encode', 1, 'CERTIFIED',
			 '49700000-0000-0000-0000-000000000034', 'ffmpeg-encode-v1',
			 'sha256:1111111111111111111111111111111111111111111111111111111111111111',
			 '49700000-0000-0000-0000-000000000021', '49700000-0000-0000-0000-000000000031',
			 '{"cpu_milli":4000,"memory_bytes":8589934592,"scratch_bytes":137438953472,"concurrency":1}',
			 decode(repeat('71', 32), 'hex')),
			('49700000-0000-0000-0000-000000000042', 'h3-cpu-mux', 1, 'CERTIFIED',
			 '49700000-0000-0000-0000-000000000035', 'ffmpeg-mux-v1',
			 'sha256:1111111111111111111111111111111111111111111111111111111111111111',
			 '49700000-0000-0000-0000-000000000022', '49700000-0000-0000-0000-000000000032',
			 '{"cpu_milli":2000,"memory_bytes":4294967296,"scratch_bytes":137438953472,"concurrency":1}',
			 decode(repeat('72', 32), 'hex')),
			('49700000-0000-0000-0000-000000000043', 'h3-cpu-thumbnail', 1, 'CERTIFIED',
			 '49700000-0000-0000-0000-000000000036', 'ffmpeg-thumbnail-v1',
			 'sha256:1111111111111111111111111111111111111111111111111111111111111111',
			 '49700000-0000-0000-0000-000000000023', '49700000-0000-0000-0000-000000000033',
			 '{"cpu_milli":2000,"memory_bytes":4294967296,"scratch_bytes":34359738368,"concurrency":1}',
			 decode(repeat('73', 32), 'hex'));

		INSERT INTO connector_revisions (
			id, stable_id, revision, state, source_interface_revision_id,
			destination_interface_revision_id, transport, durable_fallback,
			topology_policy, integrity_policy, security_policy, limits, content_digest
		) VALUES
			('49700000-0000-0000-0000-000000000050', 'h3-cpu-conditioning-l2', 1, 'CERTIFIED',
			 '49000000-0000-0000-0000-000000000011', '49000000-0000-0000-0000-000000000011',
			 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('81', 32), 'hex')),
			('49700000-0000-0000-0000-000000000051', 'h3-cpu-latent-l2', 1, 'CERTIFIED',
			 '49000000-0000-0000-0000-000000000012', '49000000-0000-0000-0000-000000000012',
			 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('82', 32), 'hex')),
			('49700000-0000-0000-0000-000000000052', 'h3-frames-to-encode-l2', 1, 'CERTIFIED',
			 '49000000-0000-0000-0000-000000000013', '49000000-0000-0000-0000-000000000013',
			 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('83', 32), 'hex')),
			('49700000-0000-0000-0000-000000000053', 'h3-encoded-to-mux-l2', 1, 'CERTIFIED',
			 '49700000-0000-0000-0000-000000000014', '49700000-0000-0000-0000-000000000014',
			 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('84', 32), 'hex')),
			('49700000-0000-0000-0000-000000000054', 'h3-frames-to-thumbnail-l2', 1, 'CERTIFIED',
			 '49000000-0000-0000-0000-000000000013', '49000000-0000-0000-0000-000000000013',
			 'OBJECT_STORE', true, '{}', '{}', '{}', '{}', decode(repeat('85', 32), 'hex'));

		INSERT INTO execution_graph_revisions (
			id, model_revision_id, stable_id, revision, schema_version, state,
			final_output_contract, public_phase_map, content_digest
		) VALUES (
			'49700000-0000-0000-0000-000000000001',
			'00000000-0000-0000-0000-000000000010', 'minimax-h3-cpu-media-graph', 1, 1,
			'CERTIFIED', '{"output_key":"video"}',
			'{"encoder":"PREPARING","dit":"GENERATING","vae":"GENERATING","encode":"FINALIZING","mux":"FINALIZING","thumbnail":"FINALIZING"}',
			decode(repeat('91', 32), 'hex')
		);
		INSERT INTO execution_graph_stages (
			execution_graph_revision_id, stage_key, stage_definition_revision_id, required, max_fan_out
		) VALUES
			('49700000-0000-0000-0000-000000000001', 'encoder', '49000000-0000-0000-0000-000000000030', true, 1),
			('49700000-0000-0000-0000-000000000001', 'dit', '49000000-0000-0000-0000-000000000031', true, 1),
			('49700000-0000-0000-0000-000000000001', 'vae', '49000000-0000-0000-0000-000000000032', true, 2),
			('49700000-0000-0000-0000-000000000001', 'encode', '49700000-0000-0000-0000-000000000034', true, 1),
			('49700000-0000-0000-0000-000000000001', 'mux', '49700000-0000-0000-0000-000000000035', true, 1),
			('49700000-0000-0000-0000-000000000001', 'thumbnail', '49700000-0000-0000-0000-000000000036', true, 1);
		INSERT INTO execution_graph_edges (
			id, execution_graph_revision_id, source_stage_key, source_port,
			destination_stage_key, destination_port, buffer_class
		) VALUES
			('49700000-0000-0000-0000-000000000060', '49700000-0000-0000-0000-000000000001', 'encoder', 'conditioning', 'dit', 'conditioning', 'L2_DURABLE'),
			('49700000-0000-0000-0000-000000000061', '49700000-0000-0000-0000-000000000001', 'dit', 'latent', 'vae', 'latent', 'L2_DURABLE'),
			('49700000-0000-0000-0000-000000000062', '49700000-0000-0000-0000-000000000001', 'vae', 'video', 'encode', 'frames', 'L2_DURABLE'),
			('49700000-0000-0000-0000-000000000063', '49700000-0000-0000-0000-000000000001', 'encode', 'encoded', 'mux', 'encoded', 'L2_DURABLE'),
			('49700000-0000-0000-0000-000000000064', '49700000-0000-0000-0000-000000000001', 'vae', 'video', 'thumbnail', 'frames', 'L2_DURABLE');
		INSERT INTO execution_graph_inputs (
			execution_graph_revision_id, input_key, interface_revision_id,
			destination_stage_key, destination_port
		) VALUES (
			'49700000-0000-0000-0000-000000000001', 'request',
			'49000000-0000-0000-0000-000000000010', 'encoder', 'request'
		);
		INSERT INTO execution_graph_outputs (
			execution_graph_revision_id, output_key, interface_revision_id,
			source_stage_key, source_port, required
		) VALUES
			('49700000-0000-0000-0000-000000000001', 'video',
			 '49700000-0000-0000-0000-000000000015', 'mux', 'video', true),
			('49700000-0000-0000-0000-000000000001', 'thumbnail',
			 '49700000-0000-0000-0000-000000000016', 'thumbnail', 'thumbnail', true);

		INSERT INTO execution_profile_revisions (
			id, model_revision_id, execution_graph_revision_id,
			stable_id, revision, state
		) VALUES (
			'49700000-0000-0000-0000-000000000070',
			'00000000-0000-0000-0000-000000000010',
			'49700000-0000-0000-0000-000000000001', 'h3-cpu-media-stage-graph', 1, 'CERTIFIED'
		);
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id, preference, eligibility_metadata
		) VALUES
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', 'encoder', '49000000-0000-0000-0000-000000000030', '49300000-0000-0000-0000-000000000031', 0, '{}'),
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', 'dit', '49000000-0000-0000-0000-000000000031', '49300000-0000-0000-0000-000000000033', 0, '{}'),
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', 'vae', '49000000-0000-0000-0000-000000000032', '49300000-0000-0000-0000-000000000035', 0, '{}'),
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', 'encode', '49700000-0000-0000-0000-000000000034', '49700000-0000-0000-0000-000000000041', 0, '{}'),
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', 'mux', '49700000-0000-0000-0000-000000000035', '49700000-0000-0000-0000-000000000042', 0, '{}'),
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', 'thumbnail', '49700000-0000-0000-0000-000000000036', '49700000-0000-0000-0000-000000000043', 0, '{}');
		INSERT INTO execution_profile_connector_options (
			execution_profile_revision_id, execution_graph_revision_id,
			execution_graph_edge_id, connector_revision_id, required_topology_policy, preference
		) VALUES
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', '49700000-0000-0000-0000-000000000060', '49700000-0000-0000-0000-000000000050', '{}', 0),
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', '49700000-0000-0000-0000-000000000061', '49700000-0000-0000-0000-000000000051', '{}', 0),
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', '49700000-0000-0000-0000-000000000062', '49700000-0000-0000-0000-000000000052', '{}', 0),
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', '49700000-0000-0000-0000-000000000063', '49700000-0000-0000-0000-000000000053', '{}', 0),
			('49700000-0000-0000-0000-000000000070', '49700000-0000-0000-0000-000000000001', '49700000-0000-0000-0000-000000000064', '49700000-0000-0000-0000-000000000054', '{}', 0);

		UPDATE execution_graph_revisions
		SET content_digest = vela_execution_graph_content_digest(id)
		WHERE id = '49700000-0000-0000-0000-000000000001';
		UPDATE execution_profile_revisions
		SET state = 'ACTIVE'
		WHERE id = '49700000-0000-0000-0000-000000000070';
	`); err != nil {
		t.Fatalf("seed CPU media ExecutionGraph: %v", err)
	}
	activation := newRolePool(
		t, database.DSN,
		"vela_stage_catalog_activation_login", "vela-stage-catalog-activation-password",
	)
	var graphDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT content_digest FROM execution_graph_revisions WHERE id = $1
	`, cpuMediaGraphID).Scan(&graphDigest); err != nil {
		t.Fatalf("read CPU media graph digest: %v", err)
	}
	var state string
	var order []string
	if err := activation.QueryRow(context.Background(), `
		SELECT state::text, topological_order FROM vela_activate_execution_graph($1, $2)
	`, cpuMediaGraphID, graphDigest).Scan(&state, &order); err != nil {
		t.Fatalf("activate CPU media ExecutionGraph: %v", err)
	}
	if state != "ACTIVE" || len(order) != 6 {
		t.Fatalf("CPU media graph activation = %s/%v", state, order)
	}
}

var _ cpumedia.Driver = (*cpuMediaIntegrationDriver)(nil)
