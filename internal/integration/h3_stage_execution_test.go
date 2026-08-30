//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/h3stage"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	"github.com/vivym/vela/internal/workercontrol"
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
}

type splitH3GraphOutcome struct {
	database      testDatabase
	attemptID     uuid.UUID
	finalArtifact stageartifact.Artifact
}

func TestSplitH3StageGraphProducesExactOutputOnSameAndCrossNodePlacements(t *testing.T) {
	placements := map[string][]string{
		"same-node":  {"h3-node-01", "h3-node-01", "h3-node-01"},
		"cross-node": {"encoder-node-01", "dit-node-09", "vae-node-03"},
	}
	var reference [sha256.Size]byte
	for name, nodes := range placements {
		t.Run(name, func(t *testing.T) {
			got := runSplitH3StageGraph(t, nodes).finalArtifact.SHA256
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
	firstFinalizer := workercontrol.AuthenticatedFinalizer{
		ID: "spiffe://vela.internal/finalizer/h3-primary",
	}
	first, err := service.ClaimNextStageGraphFinalization(
		context.Background(), firstFinalizer,
	)
	if err != nil {
		t.Fatalf("claim Stage graph finalization: %v", err)
	}
	if first.Decision != workercontrol.StageGraphFinalizationGranted ||
		first.ClaimID == uuid.Nil || first.Credentials.Token == "" ||
		first.Credentials.AttemptID != outcome.attemptID ||
		first.Source.StageArtifactID != outcome.finalArtifact.ID ||
		first.Source.ObjectKey != outcome.finalArtifact.ObjectKey ||
		first.Source.ObjectVersion != outcome.finalArtifact.ObjectVersion ||
		first.Source.SHA256 != outcome.finalArtifact.SHA256 ||
		first.Source.SizeBytes != outcome.finalArtifact.SizeBytes {
		t.Fatalf("Stage graph finalization claim = %#v", first)
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
		context.Background(), workercontrol.AuthenticatedFinalizer{
			ID: "spiffe://vela.internal/finalizer/h3-recovery",
		},
	)
	if err != nil {
		t.Fatalf("reclaim Stage graph finalization: %v", err)
	}
	if second.Decision != workercontrol.StageGraphFinalizationGranted ||
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

func TestStageGraphFinalizationMigrationRoundTripAndDurableClaimRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
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
		if err := goose.Up(database.Admin, migrations); err != nil {
			t.Fatalf("migrate Stage graph finalization back up: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 43 {
			t.Fatalf("Stage graph finalization version after Up = %d error=%v", version, err)
		}
	})

	t.Run("durable claim refuses Down", func(t *testing.T) {
		outcome := runSplitH3StageGraph(
			t,
			[]string{"encoder-node-01", "dit-node-09", "vae-node-03"},
		)
		service := visibleCompletionService(t, outcome.database.DSN)
		claim, err := service.ClaimNextStageGraphFinalization(
			context.Background(), workercontrol.AuthenticatedFinalizer{
				ID: "spiffe://vela.internal/finalizer/migration-guard",
			},
		)
		if err != nil || claim.Decision != workercontrol.StageGraphFinalizationGranted {
			t.Fatalf("seed durable Stage graph finalization claim = %#v error=%v", claim, err)
		}
		err = goose.DownTo(outcome.database.Admin, migrations, 42)
		assertPostgresConstraint(t, err, "stage_graph_finalization_rollback_is_unsafe")
		version, versionErr := goose.GetDBVersion(outcome.database.Admin)
		if versionErr != nil || version != 43 {
			t.Fatalf(
				"Stage graph finalization version after refused Down = %d error=%v",
				version, versionErr,
			)
		}
	})
}

func runSplitH3StageGraph(t *testing.T, nodes []string) splitH3GraphOutcome {
	t.Helper()
	if len(nodes) != 3 {
		t.Fatalf("H3 placement nodes = %v, want Encoder/DiT/VAE", nodes)
	}
	database, coordinator, job, attemptID := newH3IntegrationGraphFixture(
		t, "split-h3-"+nodes[0]+nodes[1]+nodes[2],
	)
	seedWorkerRegistryPlan(t, database.Admin)
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
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

	stages := []h3IntegrationStage{
		{
			key: "encoder", stage: h3stage.StageEncoder,
			profileStableID: h3stage.EncoderSingleGPUProfile,
			profileID:       encoderStageProfileID,
			workerProfileID: encoderWorkerProfileID,
			component:       "h3-encoder-v1", outputPort: "conditioning",
			outputInterface: "49000000-0000-0000-0000-000000000011",
			nodeIdentity:    nodes[0],
		},
		{
			key: "dit", stage: h3stage.StageDiT,
			profileStableID: h3stage.DiTSingleGPUProfile,
			profileID:       ditStageProfileID,
			workerProfileID: ditWorkerProfileID,
			component:       "h3-dit-v1", outputPort: "latent",
			outputInterface: "49000000-0000-0000-0000-000000000012",
			nodeIdentity:    nodes[1],
		},
		{
			key: "vae", stage: h3stage.StageVAEDecoder,
			profileStableID: h3stage.VAESingleGPUProfile,
			profileID:       h3VAEStageProfileID,
			workerProfileID: h3VAEWorkerProfileID,
			component:       "h3-vae-v1", outputPort: "video",
			outputInterface: "49000000-0000-0000-0000-000000000013",
			nodeIdentity:    nodes[2],
		},
	}

	root := sha256.Sum256([]byte("certified-h3-input-v1"))
	stageInput := root[:]
	var inputArtifact *velav1.StageInputArtifact
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
			stage, byte(0xc0+index),
		)
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
			t, stageArtifacts, attemptID, stageRunID, assignment,
			stage, payload, sealedOutput.OutputManifestJSON,
		)
		finalArtifact = artifact
		stageInput = artifact.SHA256[:]
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
		database: database, attemptID: attemptID, finalArtifact: finalArtifact,
	}
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
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES ($1, $2, $3, 'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE')
	`, poolID, "h3-integration-"+stage.key+"-"+workerID.String(), stage.profileID); err != nil {
		t.Fatalf("seed %s CapacityPool: %v", stage.key, err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
	`, workerID, stage.workerProfileID, poolID, workerRegistryBundleID); err != nil {
		t.Fatalf("seed %s WorkerInstance: %v", stage.key, err)
	}
	evidence := workerRegistryEvidenceValue(t, workerID, identityByte)
	nodeID := uuid.NewSHA1(uuid.NameSpaceDNS, []byte("vela/integration/"+stage.nodeIdentity))
	deviceID := uuid.NewSHA1(workerID, []byte("device-0"))
	evidence.DeviceSet.Devices[0].ID = deviceID
	evidence.DeviceSet.Devices[0].ComputeNodeID = nodeID
	evidence.DeviceSet.Devices[0].NodeIdentity = stage.nodeIdentity
	evidence.DeviceSet.Devices[0].GPUUUID = "GPU-" + workerID.String()
	evidence.DeviceSet.Devices[0].PCIBDF = fmt.Sprintf("0000:%02x:00.0", identityByte)
	evidence.Members[0].ComputeNodeID = nodeID
	evidence.Members[0].DeviceIDs = []uuid.UUID{deviceID}
	evidence.Residencies[0].ModelComponentRevision = stage.component
	evidence.Residencies[0].RuntimeIdentity = stage.key + "@sha256:h3-integration"
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
		CapacityVector: map[string]int64{"concurrency": 1}, TokenDigest: leaseTokenDigest[:],
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
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedEncoderAssignmentProfile(t, database)
	seedDiTAssignmentProfile(t, database)
	seedVAEIntegrationProfile(t, database)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, idempotencyKey, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"certified split H3 stage graph"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit split H3 Job status = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode split H3 Job: %v", err)
	}
	coordinator, err := attemptcoordinator.NewService(newRolePool(
		t, database.DSN,
		"vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct split H3 AttemptCoordinator: %v", err)
	}
	attemptID := uuid.New()
	if _, err := coordinator.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
		CommandID: uuid.New(), JobID: uuid.MustParse(job.JobID),
		ExpectedJobVersion: 1, ExpectedJobFence: 0,
		ExecutionGraphSnapshotID: uuid.New(), ExecutionGraphRevisionID: uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID), AttemptID: attemptID,
		StorageReservationID: uuid.New(), ReservedStorageBytes: 2 << 30,
	}); err != nil {
		t.Fatalf("instantiate split H3 graph: %v", err)
	}
	return database, coordinator, job, attemptID
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
	objectKey := fmt.Sprintf(
		"artifacts/stage/%s/%s/%s.bin", attemptID, stage.key, artifactID,
	)
	lease, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: stageRunID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1, ExpectedStageFence: 1,
		ExpectedStageVersion: assignment.ExpectedStageVersion + 2,
		OutputPort:           stage.outputPort, LocalReceiptID: stage.key + "-local-receipt-v1",
		LocalReceiptDigest: manifestDigest, ManifestSHA256: manifestDigest, SHA256: digest,
		LineageDigest: lineageDigest, TokenDigest: tokenDigest, SizeBytes: int64(len(payload)),
		ArtifactID: artifactID, MaterializationLeaseID: leaseID, ObjectKey: objectKey,
		ContentType: "application/octet-stream", SealedAt: now,
		LeaseExpiresAt: now.Add(30 * time.Minute),
	})
	if err != nil {
		t.Fatalf("seal %s StageArtifact: %v", stage.key, err)
	}
	artifact, err := repository.Commit(context.Background(), stageartifact.CommitCommand{
		CommandID: uuid.New(), ProgressReceiptID: uuid.New(), MaterializationLeaseID: lease.ID,
		ArtifactID: artifactID, ObjectKey: lease.ObjectKey,
		ObjectVersion: "l2-" + stage.key + "-exact-v1", SHA256: digest,
		SizeBytes: int64(len(payload)), TokenDigest: tokenDigest, CommittedAt: now.Add(time.Millisecond),
	})
	if err != nil {
		t.Fatalf("commit %s StageArtifact: %v", stage.key, err)
	}
	return artifact
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
