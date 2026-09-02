//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/h3stage"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagefinalization"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	h3VAEWorkerProfileID = "49300000-0000-0000-0000-000000000034"
	h3VAEStageProfileID  = "49300000-0000-0000-0000-000000000035"
)

type h3IntegrationStage struct {
	key             string
	stage           h3stage.Stage
	profileStableID string
	profileID       string
	workerProfileID string
	component       string
	outputPort      string
	outputInterface string
	nodeIdentity    string
	resourceClass   string
	capacityVector  map[string]int64
	contentType     string
	residencyPlanID uuid.UUID
}

type splitH3GraphOutcome struct {
	database      testDatabase
	jobID         uuid.UUID
	attemptID     uuid.UUID
	finalArtifact stageartifact.Artifact
}

type runningH3DiT struct {
	attemptID       uuid.UUID
	encoderArtifact stageartifact.Artifact
	stageRunID      uuid.UUID
	assignment      attemptcoordinator.AssignStageCommand
}

func TestSplitH3StageGraphProducesExactOutputOnSameAndCrossNodePlacements(t *testing.T) {
	placements := map[string][]string{
		"same-node":  {"h3-node-01", "h3-node-01", "h3-node-01"},
		"cross-node": {"encoder-node-01", "dit-node-09", "vae-node-03"},
	}
	var reference [sha256.Size]byte
	for name, nodes := range placements {
		t.Run(name, func(t *testing.T) {
			outcome := runSplitH3StageGraph(t, nodes)
			got := outcome.finalArtifact.SHA256
			var consumedTransfers, releasedCredits int
			if err := outcome.database.Admin.QueryRow(`
				SELECT
					count(*) FILTER (WHERE ticket.state = 'CONSUMED'),
					(SELECT count(*) FROM edge_buffer_credits WHERE state = 'RELEASED')
				FROM transfer_tickets AS ticket
			`).Scan(&consumedTransfers, &releasedCredits); err != nil {
				t.Fatalf("inspect split H3 transfers: %v", err)
			}
			if consumedTransfers != 2 || releasedCredits != 2 {
				t.Fatalf(
					"split H3 transfers consumed=%d released_credits=%d, want 2/2",
					consumedTransfers, releasedCredits,
				)
			}
			if reference == ([sha256.Size]byte{}) {
				reference = got
				return
			}
			if got != reference {
				t.Fatalf("final VAE StageArtifact digest = %x, want %x", got, reference)
			}
		})
	}
}

func TestStageGraphFinalizerReclaimsExactVAEArtifactWithoutRerunningVAE(t *testing.T) {
	outcome := runSplitH3StageGraph(
		t,
		[]string{"encoder-node-01", "dit-node-09", "vae-node-03"},
	)
	service := visibleCompletionService(t, outcome.database.DSN)
	firstFinalizer := stagefinalization.AuthenticatedFinalizer{
		ID: "spiffe://vela.internal/finalizer/h3-primary",
	}
	first, err := service.ClaimNextStageGraphFinalization(
		context.Background(), firstFinalizer,
	)
	if err != nil {
		t.Fatalf("claim Stage graph finalization: %v", err)
	}
	if first.Decision != stagefinalization.StageGraphFinalizationGranted ||
		first.ClaimID == uuid.Nil || first.Credentials.Token == "" ||
		first.Credentials.AttemptID != outcome.attemptID ||
		len(first.Sources) != 1 || first.Sources[0].OutputKey != "video" ||
		first.Sources[0].ArtifactKind != stagefinalization.ArtifactKindVideo ||
		first.Sources[0].Ordinal != 0 ||
		first.Source.StageArtifactID != outcome.finalArtifact.ID ||
		first.Source.ObjectKey != outcome.finalArtifact.ObjectKey ||
		first.Source.ObjectVersion != outcome.finalArtifact.ObjectVersion ||
		first.Source.SHA256 != outcome.finalArtifact.SHA256 ||
		first.Source.SizeBytes != outcome.finalArtifact.SizeBytes {
		t.Fatalf("Stage graph finalization claim = %#v", first)
	}
	var claimOutputs int
	if err := outcome.database.Admin.QueryRow(`
		SELECT count(*)
		FROM stage_graph_finalization_claim_outputs
		WHERE claim_id = $1
	`, first.ClaimID).Scan(&claimOutputs); err != nil {
		t.Fatalf("count Stage graph finalization claim outputs: %v", err)
	}
	if claimOutputs != 1 {
		t.Fatalf("Stage graph finalization claim output count = %d, want 1", claimOutputs)
	}
	incomplete, err := service.CompleteStageGraphVisibleCompletion(
		context.Background(), firstFinalizer, first.Credentials,
		stagefinalization.StageGraphVisibleCompletionCandidate{
			CompletionID: uuid.New(), ExpectedJobVersion: first.JobVersion,
		},
	)
	if err != nil {
		t.Fatalf("reject incomplete Stage graph Visible Completion: %v", err)
	}
	if incomplete.Decision != stagefinalization.VisibleCompletionIncompleteArtifact {
		t.Fatalf("incomplete Stage graph Visible Completion = %#v", incomplete)
	}
	var artifactSets, charges int
	if err := outcome.database.Admin.QueryRow(`
		SELECT (SELECT count(*) FROM artifact_sets WHERE attempt_id = attempt.id),
		       (SELECT count(*) FROM charges WHERE job_id = attempt.job_id)
		FROM attempts AS attempt
		WHERE attempt.id = $1
	`, outcome.attemptID).Scan(&artifactSets, &charges); err != nil {
		t.Fatalf("inspect incomplete Stage graph Finalizer state: %v", err)
	}
	if artifactSets != 0 || charges != 0 {
		t.Fatalf(
			"incomplete Stage graph Finalizer sets=%d charges=%d",
			artifactSets, charges,
		)
	}

	replayed, err := service.ClaimNextStageGraphFinalization(
		context.Background(), firstFinalizer,
	)
	if err != nil {
		t.Fatalf("replay Stage graph finalization claim: %v", err)
	}
	if !reflect.DeepEqual(replayed, first) {
		t.Fatalf("replayed Stage graph finalization claim = %#v, want %#v", replayed, first)
	}

	if _, err := outcome.database.Admin.Exec(`
		UPDATE stage_graph_finalization_claims
		SET expires_at = clock_timestamp()
		WHERE id = $1
	`, first.ClaimID); err != nil {
		t.Fatalf("expire Stage graph finalization claim: %v", err)
	}
	second, err := service.ClaimNextStageGraphFinalization(
		context.Background(), stagefinalization.AuthenticatedFinalizer{
			ID: "spiffe://vela.internal/finalizer/h3-recovery",
		},
	)
	if err != nil {
		t.Fatalf("reclaim Stage graph finalization: %v", err)
	}
	if second.Decision != stagefinalization.StageGraphFinalizationGranted ||
		second.ClaimID == first.ClaimID || second.Credentials.Token == "" ||
		second.Source != first.Source {
		t.Fatalf("reclaimed Stage graph finalization = %#v, first %#v", second, first)
	}

	var vaeAttempts int
	if err := outcome.database.Admin.QueryRow(`
		SELECT count(*)
		FROM stage_attempts AS physical
		JOIN stage_runs AS run ON run.id = physical.stage_run_id
		WHERE run.attempt_id = $1 AND run.stage_key = 'vae'
	`, outcome.attemptID).Scan(&vaeAttempts); err != nil {
		t.Fatalf("count VAE StageAttempts: %v", err)
	}
	if vaeAttempts != 1 {
		t.Fatalf("VAE StageAttempt count after Finalizer recovery = %d, want 1", vaeAttempts)
	}
}

func TestOneOfSevenSameNodeDiTFailuresDoesNotFenceUnrelatedWorkers(t *testing.T) {
	database, coordinator, serverURL := newH3IntegrationEnvironment(t)
	if _, err := database.Admin.Exec(`
		UPDATE projects SET running_limit = 7 WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("set seven-DiT Project concurrency: %v", err)
	}
	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("construct seven-DiT Worker Registry: %v", err)
	}
	stageArtifacts, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct seven-DiT StageArtifact repository: %v", err)
	}
	objectStore := artifactstore.NewLocal()
	encoderStage := h3IntegrationStage{
		key: "encoder", stage: h3stage.StageEncoder,
		profileStableID: h3stage.EncoderSingleGPUProfile,
		profileID:       encoderStageProfileID,
		workerProfileID: encoderWorkerProfileID,
		component:       "h3-encoder-v1", outputPort: "conditioning",
		outputInterface: "49000000-0000-0000-0000-000000000011",
		nodeIdentity:    "h3-node-01",
	}
	ditStage := h3IntegrationStage{
		key: "dit", stage: h3stage.StageDiT,
		profileStableID: h3stage.DiTSingleGPUProfile,
		profileID:       ditStageProfileID,
		workerProfileID: ditWorkerProfileID,
		component:       "h3-dit-v1", outputPort: "latent",
		outputInterface: "49000000-0000-0000-0000-000000000012",
		nodeIdentity:    "h3-node-01",
	}
	running := make([]runningH3DiT, 0, 7)
	for index := 0; index < 7; index++ {
		job, attemptID := instantiateH3IntegrationGraph(
			t, database, serverURL, fmt.Sprintf("seven-dit-%d", index),
		)
		var encoderRunID, ditRunID uuid.UUID
		if err := database.Admin.QueryRow(`
			SELECT
				(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
				(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'dit')
		`, attemptID).Scan(&encoderRunID, &ditRunID); err != nil {
			t.Fatalf("read graph %d Encoder/DiT StageRuns: %v", index, err)
		}
		encoderAssignment := assignH3IntegrationStage(
			t, database, coordinator, registry, attemptID, encoderRunID, 1,
			encoderStage, byte(0xa0+index),
		)
		encoderAuthority := signedAssignedStageAuthority(t, database, job, encoderAssignment, 2)
		_ = startH3IntegrationStage(t, database, encoderAssignment, encoderAuthority)
		payload := []byte(fmt.Sprintf("committed conditioning for DiT worker %d", index))
		manifest := []byte(fmt.Sprintf(`{"kind":"conditioning","worker":%d}`, index))
		encoderArtifact := materializeH3IntegrationStage(
			t, stageArtifacts, objectStore, attemptID, encoderRunID,
			encoderAssignment, encoderStage, payload, manifest,
		)

		ditAssignment := assignH3IntegrationStage(
			t, database, coordinator, registry, attemptID, ditRunID, 2,
			ditStage, byte(0xb0+index),
		)
		ditAuthority := signedAssignedStageAuthority(t, database, job, ditAssignment, 3)
		_ = startH3IntegrationStage(t, database, ditAssignment, ditAuthority)
		running = append(running, runningH3DiT{
			attemptID: attemptID, encoderArtifact: encoderArtifact,
			stageRunID: ditRunID, assignment: ditAssignment,
		})
	}

	failing := running[0]
	failedAt := failing.assignment.IssuedAt.Add(10 * time.Millisecond)
	decision, err := coordinator.Apply(context.Background(), attemptcoordinator.FailStageCommand{
		CommandID: uuid.New(), AttemptID: failing.attemptID,
		StageRunID: failing.stageRunID, StageAttemptID: failing.assignment.StageAttemptID,
		StageLeaseID:         failing.assignment.StageLeaseID,
		ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 4,
		FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: bytesOf(0xde, 32),
		ConsumedResourceUnits: 1, FailedAt: failedAt, RetryAt: failedAt.Add(time.Millisecond),
	})
	if err != nil {
		t.Fatalf("fail one of seven DiT StageAttempts: %v", err)
	}
	if decision.State != "RETRY_WAIT" || decision.StageFence != 2 ||
		decision.StageVersion != 5 {
		t.Fatalf("failed DiT decision = %#v", decision)
	}
	var failedArtifactState string
	if err := database.Admin.QueryRow(`
		SELECT state::text FROM stage_artifacts WHERE id = $1
	`, failing.encoderArtifact.ID).Scan(&failedArtifactState); err != nil {
		t.Fatalf("read failed Job Encoder Artifact: %v", err)
	}
	if failedArtifactState != "COMMITTED" {
		t.Fatalf("failed Job Encoder Artifact state = %s, want COMMITTED", failedArtifactState)
	}

	seenWorkers := map[uuid.UUID]struct{}{failing.assignment.WorkerInstanceID: {}}
	for index, active := range running[1:] {
		if _, duplicate := seenWorkers[active.assignment.WorkerInstanceID]; duplicate {
			t.Fatalf("DiT WorkerInstance %s was shared", active.assignment.WorkerInstanceID)
		}
		seenWorkers[active.assignment.WorkerInstanceID] = struct{}{}
		var runState, leaseState, reachability string
		var runFence, runVersion, workerEpoch int64
		if err := database.Admin.QueryRow(`
			SELECT run.state::text, run.fence, run.version,
			       lease.state::text, worker.instance_epoch, worker.reachability_state::text
			FROM stage_runs AS run
			JOIN stage_leases AS lease ON lease.id = $2
			JOIN worker_instances AS worker ON worker.id = $3
			WHERE run.id = $1
		`, active.stageRunID, active.assignment.StageLeaseID,
			active.assignment.WorkerInstanceID).Scan(
			&runState, &runFence, &runVersion, &leaseState, &workerEpoch, &reachability,
		); err != nil {
			t.Fatalf("read unrelated DiT worker %d: %v", index+1, err)
		}
		if runState != "RUNNING" || runFence != 1 || runVersion != 4 ||
			leaseState != "ACTIVE" || workerEpoch != 1 || reachability != "CONNECTED" {
			t.Fatalf(
				"unrelated DiT worker %d run=%s fence=%d version=%d lease=%s epoch=%d reachability=%s",
				index+1, runState, runFence, runVersion, leaseState, workerEpoch, reachability,
			)
		}
	}
	if len(seenWorkers) != 7 {
		t.Fatalf("independent DiT WorkerInstance count = %d, want 7", len(seenWorkers))
	}
}

func TestStageGraphFinalizationMigrationRoundTripAndDurableClaimRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 43)
		if err := goose.DownTo(database.Admin, migrations, 42); err != nil {
			t.Fatalf("migrate empty Stage graph finalization down: %v", err)
		}
		var removed bool
		if err := database.Admin.QueryRow(`
			SELECT to_regclass('stage_graph_finalization_claims') IS NULL
		`).Scan(&removed); err != nil {
			t.Fatalf("inspect Stage graph finalization rollback: %v", err)
		}
		if !removed {
			t.Fatal("Stage graph finalization claim table survived empty Down")
		}
		if err := goose.UpTo(database.Admin, migrations, 43); err != nil {
			t.Fatalf("migrate Stage graph finalization back up: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 43 {
			t.Fatalf("Stage graph finalization version after Up = %d error=%v", version, err)
		}
	})

}

func runSplitH3StageGraph(t *testing.T, nodes []string) splitH3GraphOutcome {
	t.Helper()
	if len(nodes) != 3 {
		t.Fatalf("H3 placement nodes = %v, want Encoder/DiT/VAE", nodes)
	}
	database, coordinator, serverURL := newH3IntegrationEnvironment(t)
	seedWorkerRegistryPlan(t, database.Admin)
	return runSplitH3StageGraphInEnvironment(
		t, database, coordinator, serverURL, nodes,
		"split-h3-"+nodes[0]+nodes[1]+nodes[2],
	)
}

func runSplitH3StageGraphInEnvironment(
	t *testing.T,
	database testDatabase,
	coordinator *attemptcoordinator.Service,
	serverURL string,
	nodes []string,
	idempotencyKey string,
) splitH3GraphOutcome {
	return runSplitH3StageGraphInEnvironmentWithContentTypes(
		t, database, coordinator, serverURL, nodes, idempotencyKey, nil, 0xc0, uuid.Nil,
	)
}

func runSplitH3StageGraphInEnvironmentWithContentTypes(
	t *testing.T,
	database testDatabase,
	coordinator *attemptcoordinator.Service,
	serverURL string,
	nodes []string,
	idempotencyKey string,
	contentTypes map[string]string,
	identityBase byte,
	residencyPlanID uuid.UUID,
) splitH3GraphOutcome {
	t.Helper()
	if len(nodes) != 3 {
		t.Fatalf("H3 placement nodes = %v, want Encoder/DiT/VAE", nodes)
	}
	job, attemptID := instantiateH3IntegrationGraph(
		t, database, serverURL, idempotencyKey,
	)
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_fleet_login", "vela-fleet-password",
	))
	if err != nil {
		t.Fatalf("construct split H3 Worker Registry: %v", err)
	}
	stageArtifacts, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct split H3 StageArtifact repository: %v", err)
	}
	objectStore := artifactstore.NewLocal()
	transferSigner, err := stageartifact.NewTransferTicketSigner(
		"stage-transfer-key-v1", []byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("construct split H3 TransferTicket signer: %v", err)
	}
	transferIssuer, err := stageartifact.NewTransferTicketIssuer(stageArtifacts, transferSigner)
	if err != nil {
		t.Fatalf("construct split H3 TransferTicket issuer: %v", err)
	}

	stages := h3IntegrationStages(nodes, contentTypes)
	for index := range stages {
		stages[index].residencyPlanID = residencyPlanID
	}

	root := sha256.Sum256([]byte("certified-h3-input-v1"))
	stageInput := root[:]
	var inputArtifact *velav1.StageInputArtifact
	var inputPayload []byte
	var finalArtifact stageartifact.Artifact
	for index, stage := range stages {
		var stageRunID uuid.UUID
		var stageVersion int64
		if err := database.Admin.QueryRow(`
			SELECT id, version
			FROM stage_runs
			WHERE attempt_id = $1 AND stage_key = $2
		`, attemptID, stage.key).Scan(&stageRunID, &stageVersion); err != nil {
			t.Fatalf("read %s StageRun: %v", stage.key, err)
		}
		if (index == 0 && stageVersion != 1) || (index > 0 && stageVersion != 2) {
			t.Fatalf("%s ready StageRun version = %d", stage.key, stageVersion)
		}
		assignment := assignH3IntegrationStage(
			t, database, coordinator, registry, attemptID, stageRunID, stageVersion,
			stage, identityBase+byte(index),
		)
		if inputArtifact != nil {
			connectorID := uuid.MustParse("49000000-0000-0000-0000-000000000050")
			if index == 2 {
				connectorID = uuid.MustParse("49000000-0000-0000-0000-000000000051")
			}
			pullH3IntegrationInput(
				t, database, stageArtifacts, objectStore, transferSigner,
				transferIssuer, stageRunID, assignment, inputArtifact,
				inputPayload, connectorID,
			)
		}
		spec := &velav1.StageExecutionSpec{
			ParametersJson: []byte(fmt.Sprintf(
				`{"certified_input":"%s","stage":"%s"}`,
				hex.EncodeToString(stageInput), stage.key,
			)),
			ExpectedOutputManifestJson: []byte(fmt.Sprintf(
				`{"interface_revision_id":"%s","output_port":"%s"}`,
				stage.outputInterface, stage.outputPort,
			)),
		}
		if inputArtifact != nil {
			spec.Inputs = []*velav1.StageInputArtifact{inputArtifact}
		}
		payloadDigest := sha256.Sum256(append(
			append([]byte(stage.key+"\x00"), stageInput...), byte(index),
		))
		payload := payloadDigest[:]
		driver := &h3IntegrationDriver{
			residency: h3stage.Residency{
				Stage: stage.stage, StageProfileRevisionID: stage.profileID,
				ModelRevision: stage.component, BackendRevision: "sglang@sha256:h3-integration",
				PlacementEvidence: stage.nodeIdentity, Warm: true,
			},
			payload: append([]byte(nil), payload...),
			manifest: []byte(fmt.Sprintf(
				`{"kind":"%s","sha256":"%x"}`, stage.outputPort, payloadDigest,
			)),
		}
		adapter, err := h3stage.NewAdapter(h3stage.AdapterConfig{
			ProfileStableID: stage.profileStableID, StageProfileRevisionID: stage.profileID,
			Driver: driver,
		})
		if err != nil {
			t.Fatalf("construct %s runtime Adapter: %v", stage.key, err)
		}
		assigned := signedAssignedStageAuthority(t, database, job, assignment, stageVersion+1)
		if err := adapter.Prepare(context.Background(), assigned, spec); err != nil {
			t.Fatalf("prepare %s runtime: %v", stage.key, err)
		}
		if err := adapter.Start(context.Background(), assigned); err != nil {
			t.Fatalf("start %s runtime: %v", stage.key, err)
		}
		started := startH3IntegrationStage(t, database, assignment, assigned)
		sealedOutput, err := adapter.Seal(context.Background(), started)
		if err != nil {
			t.Fatalf("seal %s runtime output: %v", stage.key, err)
		}
		if !bytes.Equal(sealedOutput.OutputManifestJSON, driver.manifest) ||
			sealedOutput.TotalSizeBytes != int64(len(payload)) {
			t.Fatalf("%s sealed runtime output = %#v", stage.key, sealedOutput)
		}

		artifact := materializeH3IntegrationStage(
			t, stageArtifacts, objectStore, attemptID, stageRunID, assignment,
			stage, payload, sealedOutput.OutputManifestJSON,
		)
		finalArtifact = artifact
		stageInput = artifact.SHA256[:]
		inputPayload = append([]byte(nil), payload...)
		inputArtifact = &velav1.StageInputArtifact{
			StageArtifactId: artifact.ID.String(), ObjectVersion: artifact.ObjectVersion,
			Sha256: artifact.SHA256[:], SizeBytes: artifact.SizeBytes,
			StageInterfaceRevisionId: stage.outputInterface,
		}
	}

	var jobState, attemptState, graphState, vaeState string
	var winningArtifact uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.state::text, attempt.graph_state::text,
		       vae.state::text, vae.winner_stage_artifact_id
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN stage_runs AS vae ON vae.attempt_id = attempt.id AND vae.stage_key = 'vae'
		WHERE attempt.id = $1
	`, attemptID).Scan(
		&jobState, &attemptState, &graphState, &vaeState, &winningArtifact,
	); err != nil {
		t.Fatalf("read completed split H3 graph: %v", err)
	}
	if jobState != "FINALIZING" || attemptState != "FINALIZING" ||
		graphState != "FINALIZING" || vaeState != "SUCCEEDED" ||
		winningArtifact != finalArtifact.ID {
		t.Fatalf(
			"split H3 terminal handoff = job/attempt/graph/vae %s/%s/%s/%s artifact %s",
			jobState, attemptState, graphState, vaeState, winningArtifact,
		)
	}
	return splitH3GraphOutcome{
		database: database, jobID: uuid.MustParse(job.JobID),
		attemptID: attemptID, finalArtifact: finalArtifact,
	}
}

func h3IntegrationStages(nodes []string, contentTypes map[string]string) []h3IntegrationStage {
	stages := []h3IntegrationStage{
		{
			key: "encoder", stage: h3stage.StageEncoder,
			profileStableID: h3stage.EncoderSingleGPUProfile,
			profileID:       encoderStageProfileID,
			workerProfileID: encoderWorkerProfileID,
			component:       "h3-encoder-v1", outputPort: "conditioning",
			outputInterface: "49000000-0000-0000-0000-000000000011",
			nodeIdentity:    nodes[0], contentType: contentTypes["encoder"],
		},
		{
			key: "dit", stage: h3stage.StageDiT,
			profileStableID: h3stage.DiTSingleGPUProfile,
			profileID:       ditStageProfileID,
			workerProfileID: ditWorkerProfileID,
			component:       "h3-dit-v1", outputPort: "latent",
			outputInterface: "49000000-0000-0000-0000-000000000012",
			nodeIdentity:    nodes[1], contentType: contentTypes["dit"],
		},
		{
			key: "vae", stage: h3stage.StageVAEDecoder,
			profileStableID: h3stage.VAESingleGPUProfile,
			profileID:       h3VAEStageProfileID,
			workerProfileID: h3VAEWorkerProfileID,
			component:       "h3-vae-v1", outputPort: "video",
			outputInterface: "49000000-0000-0000-0000-000000000013",
			nodeIdentity:    nodes[2], contentType: "video/mp4",
		},
	}
	if contentTypes["vae"] != "" {
		stages[2].contentType = contentTypes["vae"]
	}
	return stages
}

func assignH3IntegrationStage(
	t *testing.T,
	database testDatabase,
	coordinator *attemptcoordinator.Service,
	registry *fleet.Service,
	attemptID uuid.UUID,
	stageRunID uuid.UUID,
	expectedVersion int64,
	stage h3IntegrationStage,
	identityByte byte,
) attemptcoordinator.AssignStageCommand {
	t.Helper()
	poolID := uuid.New()
	workerID := uuid.New()
	resourceClass := stage.resourceClass
	if resourceClass == "" {
		resourceClass = "GPU"
	}
	capacityVector := stage.capacityVector
	if capacityVector == nil {
		capacityVector = map[string]int64{"concurrency": 1}
	}
	var residencyPlanID *uuid.UUID
	if stage.residencyPlanID != uuid.Nil {
		residencyPlanID = &stage.residencyPlanID
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state,
			residency_plan_revision_id
		) VALUES ($1, $2, $3, $4, 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE', $5)
	`, poolID, "h3-integration-"+stage.key+"-"+workerID.String(), stage.profileID,
		resourceClass, residencyPlanID); err != nil {
		t.Fatalf("seed %s CapacityPool: %v", stage.key, err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count,
			residency_plan_revision_id
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1, $5)
	`, workerID, stage.workerProfileID, poolID, workerRegistryBundleID, residencyPlanID); err != nil {
		t.Fatalf("seed %s WorkerInstance: %v", stage.key, err)
	}
	evidence := workerRegistryEvidenceValue(t, workerID, identityByte)
	nodeID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte("vela/integration/"+stage.nodeIdentity))
	deviceID := uuid.NewSHA1(workerID, []byte("device-0"))
	evidence.DeviceSet.Devices[0].ID = deviceID
	evidence.DeviceSet.Devices[0].ComputeNodeID = nodeID
	evidence.DeviceSet.Devices[0].NodeIdentity = stage.nodeIdentity
	if resourceClass == "CPU" {
		evidence.DeviceSet.Devices[0].Kind = "CPU"
		evidence.DeviceSet.Devices[0].GPUUUID = ""
		evidence.DeviceSet.Devices[0].PCIBDF = ""
	} else {
		evidence.DeviceSet.Devices[0].GPUUUID = "GPU-" + workerID.String()
		evidence.DeviceSet.Devices[0].PCIBDF = fmt.Sprintf("0000:%02x:00.0", identityByte)
	}
	evidence.Members[0].ComputeNodeID = nodeID
	evidence.Members[0].DeviceIDs = []uuid.UUID{deviceID}
	evidence.Residencies[0].ModelComponentRevision = stage.component
	evidence.Residencies[0].RuntimeIdentity = stage.key + "@sha256:h3-integration"
	evidence.Capacity.Vector = maps.Clone(capacityVector)
	evidence.ObservedBy = "node-agent/" + stage.nodeIdentity
	spiffeID := "spiffe://vela/worker/" + workerID.String()
	spiffeDigest := sha256.Sum256([]byte(spiffeID))
	evidence.Members[0].IdentityDigest = hex.EncodeToString(spiffeDigest[:])
	if _, err := registry.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe %s WorkerInstance: %v", stage.key, err)
	}
	authority := workerAuthority(t, evidence)
	issuedAt := time.Now().UTC().Truncate(time.Millisecond)
	leaseTokenDigest := sha256.Sum256(bytes.Repeat([]byte{0xb3}, 32))
	assignment := attemptcoordinator.AssignStageCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: stageRunID,
		ExpectedAttemptFence: 1, ExpectedStageFence: 1,
		ExpectedStageVersion: expectedVersion,
		StageAttemptID:       uuid.New(), StageAllocationID: uuid.New(), StageLeaseID: uuid.New(),
		StageProfileRevisionID: uuid.MustParse(stage.profileID), CapacityPoolID: poolID,
		WorkerInstanceID: workerID, WorkerInstanceEpoch: authority.InstanceEpoch,
		ObservationSequence: evidence.Capacity.Sequence,
		DeviceSetDigest:     authority.DeviceSetDigest, MembershipDigest: authority.MembershipDigest,
		ModelResidencyID: authority.ModelResidencyID, ModelRuntimeEpoch: authority.ModelRuntimeEpoch,
		CapacityVector: maps.Clone(capacityVector), TokenDigest: leaseTokenDigest[:],
		SigningKeyID: "stage-authority-key-v1", ExecutionNonce: bytesOf(identityByte, 32),
		IssuedAt: issuedAt, ExpiresAt: issuedAt.Add(5 * time.Minute),
		LocalDeadlineAt: issuedAt.Add(4 * time.Minute),
	}
	decision, err := coordinator.Apply(context.Background(), assignment)
	if err != nil {
		t.Fatalf("assign %s StageRun: %v", stage.key, err)
	}
	if decision.State != "ASSIGNED" || decision.StageVersion != expectedVersion+1 {
		t.Fatalf("%s Assignment decision = %#v", stage.key, decision)
	}
	return assignment
}

func newH3IntegrationGraphFixture(
	t *testing.T,
	idempotencyKey string,
) (testDatabase, *attemptcoordinator.Service, jobResponse, uuid.UUID) {
	t.Helper()
	database, coordinator, serverURL := newH3IntegrationEnvironment(t)
	job, attemptID := instantiateH3IntegrationGraph(
		t, database, serverURL, idempotencyKey,
	)
	return database, coordinator, job, attemptID
}

func newH3IntegrationEnvironment(
	t *testing.T,
) (testDatabase, *attemptcoordinator.Service, string) {
	return newH3IntegrationEnvironmentWithCatalogSetup(t, nil)
}

func newH3IntegrationEnvironmentWithCatalogSetup(
	t *testing.T,
	setup func(*testing.T, testDatabase),
) (testDatabase, *attemptcoordinator.Service, string) {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedEncoderAssignmentProfile(t, database)
	seedDiTAssignmentProfile(t, database)
	seedVAEIntegrationProfile(t, database)
	if setup != nil {
		setup(t, database)
	}
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	coordinator, err := attemptcoordinator.NewService(newRolePool(
		t, database.DSN,
		"vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct split H3 AttemptCoordinator: %v", err)
	}
	return database, coordinator, server.URL
}

func instantiateH3IntegrationGraph(
	t *testing.T,
	database testDatabase,
	serverURL string,
	idempotencyKey string,
) (jobResponse, uuid.UUID) {
	t.Helper()
	accepted := submitJob(t, serverURL, idempotencyKey, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"certified split H3 stage graph",
		"h3":{"seed":17}
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit split H3 Job status = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode split H3 Job: %v", err)
	}
	instantiation := readStageGraphInstantiation(t, database.Admin, job.JobID)
	return job, instantiation.AttemptID
}

func seedVAEIntegrationProfile(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_profile_revisions (
			id, stable_id, revision, state, device_count, member_count,
			device_set_shape, resident_model_revisions, capacity_limits,
			readiness_checks, content_digest
		) VALUES (
			$1, 'h3-vae-dedicated', 1, 'CERTIFIED', 1, 1,
			'{"kind":"single-gpu"}', '["h3-vae-v1"]',
			'{"concurrency":1}', '{"warmup":true}', decode(repeat('77', 32), 'hex')
		)
	`, h3VAEWorkerProfileID); err != nil {
		t.Fatalf("seed VAE WorkerProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO stage_profile_revisions (
			id, stable_id, revision, state, stage_definition_revision_id,
			model_component_revision, runtime_image_digest, worker_profile_revision_id,
			result_equivalence_revision_id, certified_capacity_vector, content_digest
		) VALUES (
			$1, 'h3-vae-dedicated', 1, 'CERTIFIED',
			'49000000-0000-0000-0000-000000000032', 'h3-vae-v1',
			'sha256:5555555555555555555555555555555555555555555555555555555555555555',
			$2, '49000000-0000-0000-0000-000000000025',
			'{"concurrency":1}', decode(repeat('78', 32), 'hex')
		)
	`, h3VAEStageProfileID, h3VAEWorkerProfileID); err != nil {
		t.Fatalf("seed VAE StageProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id,
			preference, eligibility_metadata
		) VALUES (
			$1, $2, 'vae', '49000000-0000-0000-0000-000000000032',
			$3, -1, '{"dedicated_model":true}'
		)
	`, graphExecutionProfileID, stageGraphID, h3VAEStageProfileID); err != nil {
		t.Fatalf("seed VAE StageProfile option: %v", err)
	}
}

func startH3IntegrationStage(
	t *testing.T,
	database testDatabase,
	assignment attemptcoordinator.AssignStageCommand,
	assigned stageauthority.Verified,
) stageauthority.Verified {
	t.Helper()
	keys := map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("construct StageAuthority signer: %v", err)
	}
	controlNow := assignment.IssuedAt.Add(10 * time.Millisecond)
	backend, err := stageworkercontrol.NewPostgresExecutionBackend(
		newRolePool(
			t, database.DSN,
			"vela_stage_worker_control_login", "vela-stage-worker-control-password",
		),
		signer,
		stageworkercontrol.PostgresExecutionConfig{
			ActiveSigningKeyID: "stage-authority-key-v1", AuthorityTTL: 2 * time.Minute,
			LocalDeadlineTTL: 90 * time.Second, MaxClockSkew: time.Second,
			Now: func() time.Time { return controlNow },
		},
	)
	if err != nil {
		t.Fatalf("construct Stage Worker execution backend: %v", err)
	}
	result, err := backend.StartStage(
		context.Background(),
		stageworkercontrol.CommandContext{
			CommandID: uuid.New(),
			Identity: stageworkertransport.Identity{
				SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String(),
			},
			ControlSessionEpoch: 1,
		},
		&velav1.StartStageRequest{
			Authority: assigned.Authority,
			StartedAt: timestamppb.New(assignment.IssuedAt.Add(5 * time.Millisecond)),
		},
		stageworkercontrol.VerifiedAuthorities{Stage: &assigned},
	)
	if err != nil {
		t.Fatalf("persist Stage Worker start: %v", err)
	}
	if result.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED ||
		result.RenewedAuthority == nil {
		t.Fatalf("Stage Worker start result = %#v", result)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return controlNow })
	if err != nil {
		t.Fatalf("construct renewed StageAuthority validator: %v", err)
	}
	verified, err := validator.ValidateEnvelope(result.RenewedAuthority)
	if err != nil {
		t.Fatalf("validate renewed StageAuthority: %v", err)
	}
	return verified
}

func materializeH3IntegrationStage(
	t *testing.T,
	repository *stageartifact.PostgresRepository,
	objectStore artifactstore.VersionedStore,
	attemptID uuid.UUID,
	stageRunID uuid.UUID,
	assignment attemptcoordinator.AssignStageCommand,
	stage h3IntegrationStage,
	payload []byte,
	manifest []byte,
) stageartifact.Artifact {
	t.Helper()
	digest := sha256.Sum256(payload)
	manifestDigest := sha256.Sum256(manifest)
	lineageDigest := sha256.Sum256([]byte("lineage/" + attemptID.String() + "/" + stage.key))
	tokenDigest := sha256.Sum256([]byte("materialization-authority/" + assignment.StageLeaseID.String()))
	artifactID := uuid.New()
	leaseID := uuid.New()
	now := assignment.IssuedAt.Add(20 * time.Millisecond)
	contentType := stage.contentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	objectKey := fmt.Sprintf("artifacts/stage/%s/%s/%s.bin", attemptID, stage.key, artifactID)
	lease, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: stageRunID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1, ExpectedStageFence: 1,
		ExpectedStageVersion: assignment.ExpectedStageVersion + 2,
		OutputPort:           stage.outputPort, LocalReceiptID: stage.key + "-local-receipt-v1",
		LocalReceiptDigest: manifestDigest, ManifestSHA256: manifestDigest, SHA256: digest,
		LineageDigest: lineageDigest, TokenDigest: tokenDigest, SizeBytes: int64(len(payload)),
		ArtifactID: artifactID, MaterializationLeaseID: leaseID, ObjectKey: objectKey,
		ContentType: contentType, SealedAt: now,
		LeaseExpiresAt: now.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("seal %s StageArtifact: %v", stage.key, err)
	}
	object, err := objectStore.PutIfAbsent(
		context.Background(), lease.ObjectKey, lease.ContentType,
		bytes.NewReader(payload), int64(len(payload)), digest,
	)
	if err != nil {
		t.Fatalf("publish %s StageArtifact exact object: %v", stage.key, err)
	}
	artifact, err := repository.Commit(context.Background(), stageartifact.CommitCommand{
		CommandID: uuid.New(), ProgressReceiptID: uuid.New(), MaterializationLeaseID: lease.ID,
		ArtifactID: artifactID, ObjectKey: lease.ObjectKey,
		ObjectVersion: object.VersionID, SHA256: digest,
		SizeBytes: int64(len(payload)), TokenDigest: tokenDigest, CommittedAt: now.Add(time.Millisecond),
	})
	if err != nil {
		t.Fatalf("commit %s StageArtifact: %v", stage.key, err)
	}
	return artifact
}

func pullH3IntegrationInput(
	t *testing.T,
	database testDatabase,
	repository *stageartifact.PostgresRepository,
	objectStore artifactstore.VersionedStore,
	signer *stageartifact.TransferTicketSigner,
	issuer *stageartifact.TransferTicketIssuer,
	destinationStageRunID uuid.UUID,
	assignment attemptcoordinator.AssignStageCommand,
	input *velav1.StageInputArtifact,
	wantPayload []byte,
	connectorID uuid.UUID,
) {
	t.Helper()
	artifactID, err := uuid.Parse(input.GetStageArtifactId())
	if err != nil {
		t.Fatalf("parse upstream StageArtifact identity: %v", err)
	}
	var pinID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id
		FROM stage_artifact_pins
		WHERE stage_artifact_id = $1
		  AND owner_stage_run_id = $2
		  AND state = 'ACTIVE'
	`, artifactID, destinationStageRunID).Scan(&pinID); err != nil {
		t.Fatalf("read downstream StageArtifact pin: %v", err)
	}
	destination := stageartifact.TransferDestination{
		WorkerInstanceID:    assignment.WorkerInstanceID,
		WorkerInstanceEpoch: assignment.WorkerInstanceEpoch,
		ModelResidencyID:    assignment.ModelResidencyID,
		ModelRuntimeEpoch:   assignment.ModelRuntimeEpoch,
		ConnectorRevisionID: connectorID,
	}
	now := assignment.IssuedAt.Add(time.Millisecond)
	ticketID := uuid.New()
	ticket, err := issuer.Issue(context.Background(), stageartifact.IssueTransferRequest{
		CommandID: uuid.New(), TicketID: ticketID, ArtifactID: artifactID,
		PinID: pinID, Destination: destination, IssuedAt: now,
		ExpiresAt: now.Add(5 * time.Minute),
	})
	if err != nil {
		t.Fatalf("issue downstream TransferTicket: %v", err)
	}
	connector, err := stageartifact.NewObjectStorePullConnector(
		objectStore, repository, signer, func() time.Time { return now.Add(time.Millisecond) },
	)
	if err != nil {
		t.Fatalf("construct object-store pull Connector: %v", err)
	}
	wrongDestination := destination
	wrongDestination.WorkerInstanceID = uuid.New()
	if _, err := connector.Pull(
		context.Background(), ticket, wrongDestination, stageartifact.NewMemoryTransferTarget(),
	); !errors.Is(err, stageartifact.ErrTransferTicketDestinationMismatch) {
		t.Fatalf("wrong destination TransferTicket error = %v", err)
	}
	for replay := 0; replay < 2; replay++ {
		target := stageartifact.NewMemoryTransferTarget()
		receipt, err := connector.Pull(context.Background(), ticket, destination, target)
		if err != nil {
			t.Fatalf("pull downstream StageArtifact replay=%d: %v", replay, err)
		}
		if receipt.ArtifactID != artifactID || receipt.SHA256 != sha256.Sum256(wantPayload) ||
			!bytes.Equal(target.Bytes(), wantPayload) {
			t.Fatalf(
				"pulled downstream StageArtifact replay=%d receipt=%#v bytes=%x",
				replay, receipt, target.Bytes(),
			)
		}
	}
}

type h3IntegrationDriver struct {
	residency h3stage.Residency
	payload   []byte
	manifest  []byte
	prepared  bool
	started   bool
}

func (driver *h3IntegrationDriver) Residency() h3stage.Residency { return driver.residency }

func (driver *h3IntegrationDriver) Probe(
	context.Context,
	velav1.ModelRuntimeReadinessCheck,
) (modelruntime.ProbeResult, error) {
	return modelruntime.ProbeResult{Ready: driver.residency.Warm}, nil
}

func (driver *h3IntegrationDriver) Prepare(
	_ context.Context,
	_ h3stage.ExecutionIdentity,
	spec *velav1.StageExecutionSpec,
) error {
	if spec == nil || !json.Valid(spec.GetParametersJson()) {
		return fmt.Errorf("invalid H3 integration execution spec")
	}
	driver.prepared = true
	return nil
}

func (driver *h3IntegrationDriver) Start(context.Context, h3stage.ExecutionIdentity) error {
	if !driver.prepared {
		return fmt.Errorf("H3 integration runtime was not prepared")
	}
	driver.started = true
	return nil
}

func (driver *h3IntegrationDriver) Cancel(
	context.Context,
	h3stage.ExecutionIdentity,
	velav1.ModelRuntimeCancelReason,
) error {
	driver.started = false
	return nil
}

func (driver *h3IntegrationDriver) Status(
	context.Context,
	h3stage.ExecutionIdentity,
) (modelruntime.BackendStatus, error) {
	return modelruntime.BackendStatus{
		State:        velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY,
		BackendStage: string(driver.residency.Stage),
	}, nil
}

func (driver *h3IntegrationDriver) Seal(
	context.Context,
	h3stage.ExecutionIdentity,
) (modelruntime.SealedOutput, error) {
	if !driver.started {
		return modelruntime.SealedOutput{}, fmt.Errorf("H3 integration runtime was not started")
	}
	return modelruntime.SealedOutput{
		OutputManifestJSON: append([]byte(nil), driver.manifest...),
		TotalSizeBytes:     int64(len(driver.payload)),
	}, nil
}
