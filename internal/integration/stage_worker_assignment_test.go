//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/artifactstore"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagescheduler"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestEncoderAssignmentBindsFrozenH3ParametersAndRootMaterial(t *testing.T) {
	const digestHex = "d7a8fbb307d7809469ca9abcb0082e4f8d5651e46d3cdb762d02d0bf37c9e592"
	fixture := newStageSchedulerFixtureWithRequest(
		t,
		"h3-root-material",
		[]byte(`{
			"model":"minimax-h3",
			"generation_preset":"balanced",
			"service_class":"standard",
			"output_spec":"video-1080p-5s-24fps",
			"generation_count":1,
			"prompt":"animate the reference",
			"h3":{
				"task":"ref2va",
				"seed":17,
				"conditions":[{
					"role":"reference",
					"type":"image",
					"uri":"vela://uploads/reference-frame",
					"download_url":"https://objects.example.test/reference?signature=worker-only",
					"sha256":"`+digestHex+`",
					"size_bytes":4096
				}],
				"target":{"short_edge":768,"aspect_ratio":"16:9","duration_seconds":5},
				"sampling":{"num_inference_steps":30,"quality":"lossless"}
			}
		}`),
	)
	result, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(),
		stageWorkerAcquireCommand(fixture),
		stageWorkerAcquireRequest(fixture),
	)
	if err != nil || result.Assignment == nil {
		t.Fatalf("Acquire Encoder = %#v error=%v", result, err)
	}
	assignment := result.Assignment
	if _, err := stageassignment.Validate(assignment); err != nil {
		t.Fatalf("validate Encoder Assignment: %v", err)
	}
	var parameters struct {
		SchemaRevision   int `json:"schema_revision"`
		CanonicalRequest struct {
			Task       string `json:"task"`
			Prompt     string `json:"prompt"`
			Seed       int64  `json:"seed"`
			Conditions []struct {
				URI string `json:"uri"`
			} `json:"conditions"`
		} `json:"canonical_request"`
		Sampling struct {
			NumInferenceSteps int    `json:"num_inference_steps"`
			Quality           string `json:"quality"`
		} `json:"sampling"`
	}
	if err := json.Unmarshal(assignment.GetExecutionSpec().GetParametersJson(), &parameters); err != nil {
		t.Fatalf("decode fast-h3 parameters: %v", err)
	}
	if parameters.SchemaRevision != 1 || parameters.CanonicalRequest.Task != "ref2va" ||
		parameters.CanonicalRequest.Prompt != "animate the reference" ||
		parameters.CanonicalRequest.Seed != 17 || len(parameters.CanonicalRequest.Conditions) != 1 ||
		parameters.CanonicalRequest.Conditions[0].URI != "vela://uploads/reference-frame" ||
		parameters.Sampling.NumInferenceSteps != 30 || parameters.Sampling.Quality != "lossless" {
		t.Fatalf("fast-h3 parameters = %#v", parameters)
	}
	if bytes.Contains(assignment.GetExecutionSpec().GetParametersJson(), []byte("download_url")) ||
		bytes.Contains(assignment.GetExecutionSpec().GetParametersJson(), []byte("generation_preset")) {
		t.Fatalf("backend parameters expose non-fast-h3 fields: %s", assignment.GetExecutionSpec().GetParametersJson())
	}
	digest, _ := hex.DecodeString(digestHex)
	rootInputs := assignment.GetExecutionSpec().GetRootInputs()
	fetches := assignment.GetRootInputFetches()
	if len(rootInputs) != 1 || rootInputs[0].GetConditionIndex() != 0 ||
		rootInputs[0].GetUri() != "vela://uploads/reference-frame" ||
		!bytes.Equal(rootInputs[0].GetSha256(), digest) || rootInputs[0].GetSizeBytes() != 4096 ||
		len(fetches) != 1 || fetches[0].GetDownloadUrl() !=
		"https://objects.example.test/reference?signature=worker-only" ||
		!bytes.Equal(fetches[0].GetSha256(), digest) {
		t.Fatalf("root inputs/fetches = %#v / %#v", rootInputs, fetches)
	}
}

func TestPostgresAssignmentBackendReplaysExactAssignment(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-assignment-replay")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command := stageWorkerAcquireCommand(fixture)
	request := stageWorkerAcquireRequest(fixture)

	first, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || first.Assignment == nil || first.Command != nil || first.RetryAfter != 0 {
		t.Fatalf("first AcquireStage = %#v error=%v", first, err)
	}
	if _, err := stageassignment.Validate(first.Assignment); err != nil {
		t.Fatalf("validate first StageAssignment: %v", err)
	}
	if first.Assignment.GetAuthority().GetModelRuntimeBarrierGeneration() !=
		request.GetModelRuntimeEpoch() {
		t.Fatalf(
			"StageAssignment barrier generation = %d, want %d",
			first.Assignment.GetAuthority().GetModelRuntimeBarrierGeneration(),
			request.GetModelRuntimeEpoch(),
		)
	}
	members := first.Assignment.GetAuthority().GetMembers()
	identityDigest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	if len(members) != 1 || !bytes.Equal(members[0].GetIdentityDigest(), identityDigest[:]) {
		t.Fatalf("StageAssignment signed member identity digest = %#v", members)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)},
		func() time.Time { return first.Assignment.GetAuthority().GetIssuedAt().AsTime().Add(time.Millisecond) },
	)
	if err != nil {
		t.Fatalf("construct StageAuthority validator: %v", err)
	}
	verified, err := validator.ValidateEnvelope(first.Assignment.GetAuthority())
	if err != nil {
		t.Fatalf("verify StageAssignment authority: %v", err)
	}
	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(newRolePool(
		t, fixture.database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	))
	if err != nil {
		t.Fatalf("construct StageAuthority durable authorizer: %v", err)
	}
	active, err := authorizer.IsActive(
		context.Background(), command.Identity, command.ControlSessionEpoch,
		stageworkercontrol.OperationStartStage, verified,
	)
	if err != nil || !active {
		t.Fatalf("authorize StageAssignment barrier = %t error=%v", active, err)
	}
	firstWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(first.Assignment)
	if err != nil {
		t.Fatalf("marshal first StageAssignment: %v", err)
	}

	second, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || second.Assignment == nil || second.Command != nil || second.RetryAfter != 0 {
		t.Fatalf("replayed AcquireStage = %#v error=%v", second, err)
	}
	secondWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(second.Assignment)
	if err != nil {
		t.Fatalf("marshal replayed StageAssignment: %v", err)
	}
	if !bytes.Equal(firstWire, secondWire) {
		t.Fatal("replayed StageAssignment wire changed")
	}

	var attempts, leases, intents, results int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_leases WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_worker_acquire_intents WHERE command_id = $2),
			(SELECT count(*) FROM stage_worker_acquire_results WHERE command_id = $2)
	`, fixture.stageRunID, command.CommandID).Scan(
		&attempts, &leases, &intents, &results,
	); err != nil {
		t.Fatalf("read durable StageAssignment replay evidence: %v", err)
	}
	if attempts != 1 || leases != 1 || intents != 1 || results != 1 {
		t.Fatalf(
			"durable Assignment rows attempts=%d leases=%d intents=%d results=%d",
			attempts, leases, intents, results,
		)
	}
}

func TestPostgresAssignmentBackendAssignsCertifiedConnectorInput(t *testing.T) {
	database, coordinator, _, attemptID := newH3IntegrationGraphFixture(
		t, "stage-worker-certified-connector-input",
	)
	seedWorkerRegistryPlan(t, database.Admin)
	stages := h3IntegrationStages(
		[]string{"encoder-node-01", "dit-node-09", "vae-node-03"}, nil,
	)
	var encoderRunID, ditRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'dit')
	`, attemptID).Scan(&encoderRunID, &ditRunID); err != nil {
		t.Fatalf("read Encoder/DiT StageRuns: %v", err)
	}

	encoderFixture := newH3AssignmentWorkerFixture(
		t, database, coordinator, encoderRunID, stages[0], 0xd1,
	)
	encoderResult, err := newPostgresAssignmentTestBackend(t, encoderFixture).AcquireStage(
		context.Background(),
		stageWorkerAcquireCommand(encoderFixture),
		stageWorkerAcquireRequest(encoderFixture),
	)
	if err != nil || encoderResult.Assignment == nil {
		t.Fatalf("Acquire Encoder = %#v error=%v", encoderResult, err)
	}
	validator, err := stageauthority.NewValidator(
		map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)},
		func() time.Time {
			return encoderResult.Assignment.GetAuthority().GetIssuedAt().AsTime().Add(time.Millisecond)
		},
	)
	if err != nil {
		t.Fatalf("construct Encoder StageAuthority validator: %v", err)
	}
	verified, err := validator.ValidateEnvelope(encoderResult.Assignment.GetAuthority())
	if err != nil {
		t.Fatalf("verify Encoder StageAssignment authority: %v", err)
	}
	authority := verified.Authority
	encoderAssignment := attemptcoordinator.AssignStageCommand{
		AttemptID:              uuid.MustParse(authority.GetAttemptId()),
		StageRunID:             uuid.MustParse(authority.GetStageRunId()),
		ExpectedStageVersion:   authority.GetStageVersion() - 1,
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
	started := startH3IntegrationStage(t, database, encoderAssignment, verified)
	if started.Authority.GetStageVersion() != authority.GetStageVersion()+1 {
		t.Fatalf("started Encoder StageVersion = %d", started.Authority.GetStageVersion())
	}
	artifactRepository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct Encoder StageArtifact repository: %v", err)
	}
	encoderArtifact := materializeH3IntegrationStage(
		t, artifactRepository, artifactstore.NewLocal(), attemptID, encoderRunID,
		encoderAssignment, stages[0], []byte("certified connector input"),
		[]byte(`{"kind":"conditioning","schema_version":1}`),
	)

	ditFixture := newH3AssignmentWorkerFixture(
		t, database, coordinator, ditRunID, stages[1], 0xd2,
	)
	ditResult, err := newPostgresAssignmentTestBackend(t, ditFixture).AcquireStage(
		context.Background(),
		stageWorkerAcquireCommand(ditFixture),
		stageWorkerAcquireRequest(ditFixture),
	)
	if err != nil || ditResult.Assignment == nil {
		t.Fatalf("Acquire DiT with CERTIFIED Connector = %#v error=%v", ditResult, err)
	}
	inputs := ditResult.Assignment.GetExecutionSpec().GetInputs()
	tickets := ditResult.Assignment.GetInputTransferTickets()
	if len(inputs) != 1 || len(tickets) != 1 ||
		inputs[0].GetStageArtifactId() != encoderArtifact.ID.String() ||
		inputs[0].GetObjectVersion() != encoderArtifact.ObjectVersion ||
		tickets[0].GetStageArtifactId() != encoderArtifact.ID.String() ||
		tickets[0].GetObjectVersion() != encoderArtifact.ObjectVersion {
		t.Fatalf("DiT certified Connector inputs/tickets = %#v / %#v", inputs, tickets)
	}
	var connectorState string
	if err := database.Admin.QueryRow(`
		SELECT connector.state::text
		FROM transfer_tickets AS ticket
		JOIN connector_revisions AS connector ON connector.id = ticket.connector_revision_id
		WHERE ticket.stage_artifact_id = $1
	`, encoderArtifact.ID).Scan(&connectorState); err != nil {
		t.Fatalf("read DiT TransferTicket Connector: %v", err)
	}
	if connectorState != "CERTIFIED" {
		t.Fatalf("DiT TransferTicket Connector state = %s, want CERTIFIED", connectorState)
	}
}

func TestPostgresAssignmentBackendReplaysNoWorkAndRejectsChangedRequest(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-no-work-replay")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	request := stageWorkerAcquireRequest(fixture)
	if result, err := backend.AcquireStage(
		context.Background(), stageWorkerAcquireCommand(fixture), request,
	); err != nil || result.Assignment == nil {
		t.Fatalf("consume READY StageRun = %#v error=%v", result, err)
	}
	command := stageWorkerAcquireCommand(fixture)
	first, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || first.Assignment != nil || first.Command != nil ||
		first.RetryAfter != 250*time.Millisecond {
		t.Fatalf("first NoWork = %#v error=%v", first, err)
	}
	second, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || second != first {
		t.Fatalf("replayed NoWork = %#v want %#v error=%v", second, first, err)
	}
	changed := proto.Clone(request).(*velav1.AcquireStageRequest)
	changed.ModelRuntimeEpoch++
	_, err = backend.AcquireStage(context.Background(), command, changed)
	assertPostgresConstraint(t, err, "stage_worker_acquire_replay_mismatch")

	var kind string
	var retry int64
	if err := fixture.database.Admin.QueryRow(`
		SELECT result_kind::text, retry_after_ms
		FROM stage_worker_acquire_results WHERE command_id = $1
	`, command.CommandID).Scan(&kind, &retry); err != nil {
		t.Fatalf("read durable NoWork result: %v", err)
	}
	if kind != "NO_WORK" || retry != 250 {
		t.Fatalf("durable NoWork kind=%s retry=%d", kind, retry)
	}
}

func TestPostgresAssignmentBackendDoesNotAssignFirstStageAtProjectRunningLimit(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-project-capacity")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE projects
		SET running_count = running_limit
		WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("fill Project running capacity: %v", err)
	}

	result, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(),
		stageWorkerAcquireCommand(fixture),
		stageWorkerAcquireRequest(fixture),
	)
	if err != nil || result.Assignment != nil || result.Command != nil ||
		result.RetryAfter != 250*time.Millisecond {
		t.Fatalf("AcquireStage at Project limit = %#v error=%v", result, err)
	}

	var attempts, allocations int
	var filterReason string
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_allocations WHERE stage_run_id = $1),
			COALESCE((
				SELECT candidate #>> '{filter_reasons,0}'
				FROM stage_scheduler_snapshot_traces AS trace,
				     jsonb_array_elements(trace.snapshot -> 'candidates') AS candidate
				WHERE (candidate ->> 'stage_run_id')::uuid = $1
				ORDER BY trace.evaluated_at DESC
				LIMIT 1
			), '')
	`, fixture.stageRunID).Scan(&attempts, &allocations, &filterReason); err != nil {
		t.Fatalf("read Project-capacity scheduling evidence: %v", err)
	}
	if attempts != 0 || allocations != 0 ||
		filterReason != string(stagescheduler.FilterProjectCapacityExhausted) {
		t.Fatalf(
			"Project-capacity evidence attempts=%d allocations=%d reason=%q",
			attempts, allocations, filterReason,
		)
	}
}

func TestPostgresAssignmentBackendPersistsForgedAndStaleAuthorityDecisions(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-authority-rejection")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	request := stageWorkerAcquireRequest(fixture)
	forged := stageWorkerAcquireCommand(fixture)
	forged.Identity.SPIFFEID += "/forged"
	rejected, err := backend.AcquireStage(context.Background(), forged, request)
	if err != nil || rejected.Command == nil ||
		rejected.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED {
		t.Fatalf("forged identity result = %#v error=%v", rejected, err)
	}
	stale := stageWorkerAcquireCommand(fixture)
	stale.ControlSessionEpoch++
	staleResult, err := backend.AcquireStage(context.Background(), stale, request)
	if err != nil || staleResult.Command == nil ||
		staleResult.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
		t.Fatalf("stale session result = %#v error=%v", staleResult, err)
	}
	valid, err := backend.AcquireStage(
		context.Background(), stageWorkerAcquireCommand(fixture), request,
	)
	if err != nil || valid.Assignment == nil {
		t.Fatalf("valid authority after rejections = %#v error=%v", valid, err)
	}
	var attempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1
	`, fixture.stageRunID).Scan(&attempts); err != nil {
		t.Fatalf("count StageAttempts after rejected acquires: %v", err)
	}
	if attempts != 1 {
		t.Fatalf("StageAttempts after rejected acquires = %d, want 1", attempts)
	}
}

func TestPostgresAssignmentBackendRecoversCrashAfterSchedulerClaim(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-claim-crash")
	crashingScheduler, err := stagescheduler.NewService(
		fixture.repository,
		panickingStageCoordinator{},
		stagescheduler.Config{
			SchedulerID:      "stage-worker-control/claim-crash",
			ClaimTTL:         30 * time.Second,
			LeaseTTL:         time.Minute,
			LocalDeadlineTTL: 50 * time.Second,
			SigningKeyID:     "stage-authority-key-v1",
		},
	)
	if err != nil {
		t.Fatalf("construct crashing StageScheduler: %v", err)
	}
	backend := newPostgresAssignmentTestBackendWithScheduler(t, fixture, crashingScheduler)
	command := stageWorkerAcquireCommand(fixture)
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("Stage Worker acquire did not stop after scheduler claim")
			}
		}()
		_, _ = backend.AcquireStage(
			context.Background(), command, stageWorkerAcquireRequest(fixture),
		)
	}()

	recovered, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(), command, stageWorkerAcquireRequest(fixture),
	)
	if err != nil || recovered.Assignment == nil {
		t.Fatalf("recover acquire after scheduler claim = %#v error=%v", recovered, err)
	}
	assertSingleDurableAssignment(t, fixture, command.CommandID)
}

func TestPostgresAssignmentBackendRecoversAfterCommittedSchedulerBeforeResult(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stage-worker-result-crash")
	normalScheduler := newStageSchedulerTestService(t, fixture)
	ctx, cancel := context.WithCancel(context.Background())
	backend := newPostgresAssignmentTestBackendWithScheduler(
		t, fixture, cancelAfterAssignmentScheduler{delegate: normalScheduler, cancel: cancel},
	)
	command := stageWorkerAcquireCommand(fixture)
	first, err := backend.AcquireStage(ctx, command, stageWorkerAcquireRequest(fixture))
	if err == nil || first != (stageworkercontrol.AcquireResult{}) {
		t.Fatalf("acquire canceled after scheduler commit = %#v error=%v", first, err)
	}
	var results int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*) FROM stage_worker_acquire_results WHERE command_id = $1
	`, command.CommandID).Scan(&results); err != nil {
		t.Fatalf("count pre-recovery acquire results: %v", err)
	}
	if results != 0 {
		t.Fatalf("pre-recovery acquire results = %d, want 0", results)
	}

	recovered, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(), command, stageWorkerAcquireRequest(fixture),
	)
	if err != nil || recovered.Assignment == nil {
		t.Fatalf("recover acquire before result persistence = %#v error=%v", recovered, err)
	}
	assertSingleDurableAssignment(t, fixture, command.CommandID)
}

func TestStageWorkerAssignmentMigrationEmptyDownUp(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 42)
		if err := goose.DownTo(database.Admin, migrations, 41); err != nil {
			t.Fatalf("migrate empty Stage Worker assignment down: %v", err)
		}
		var intents, completeFunction bool
		if err := database.Admin.QueryRow(`
			SELECT to_regclass('stage_worker_acquire_intents') IS NOT NULL,
			       to_regprocedure('vela_complete_stage_worker_acquire(jsonb)') IS NOT NULL
		`).Scan(&intents, &completeFunction); err != nil {
			t.Fatalf("inspect schema 41 assignment surface: %v", err)
		}
		if intents || completeFunction {
			t.Fatal("Stage Worker assignment surface survived empty Down")
		}
		if err := goose.UpTo(database.Admin, migrations, 42); err != nil {
			t.Fatalf("migrate Stage Worker assignment back up: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 42 {
			t.Fatalf("Stage Worker assignment version after Up = %d error=%v", version, err)
		}
	})
}

func TestStageAssignmentConnectorStatesMigrationEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 67)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	assertConnectorStates := func(wantExpanded bool) {
		t.Helper()
		var definition string
		if err := database.Admin.QueryRow(`
			SELECT pg_get_functiondef(
				'public.vela_read_stage_assignment_execution_v1(uuid,uuid)'::regprocedure
			)
		`).Scan(&definition); err != nil {
			t.Fatalf("read StageAssignment execution definition: %v", err)
		}
		expanded := strings.Contains(
			definition,
			"revision.state IN ('CERTIFIED', 'CANARY', 'ACTIVE', 'DRAINING')",
		)
		if expanded != wantExpanded {
			t.Fatalf("StageAssignment Connector states expanded=%t want=%t", expanded, wantExpanded)
		}
	}

	assertConnectorStates(false)
	if err := goose.UpTo(database.Admin, migrations, 68); err != nil {
		t.Fatalf("expand StageAssignment Connector states migration: %v", err)
	}
	assertConnectorStates(true)
	if err := goose.DownTo(database.Admin, migrations, 67); err != nil {
		t.Fatalf("contract StageAssignment Connector states migration: %v", err)
	}
	assertConnectorStates(false)
	if err := goose.UpTo(database.Admin, migrations, 68); err != nil {
		t.Fatalf("re-expand StageAssignment Connector states migration: %v", err)
	}
	assertConnectorStates(true)
}

func newPostgresAssignmentTestBackend(
	t *testing.T,
	fixture stageSchedulerFixture,
) *stageworkercontrol.PostgresAssignmentBackend {
	t.Helper()
	return newPostgresAssignmentTestBackendWithScheduler(
		t, fixture, newStageSchedulerTestService(t, fixture),
	)
}

func newStageSchedulerTestService(
	t *testing.T,
	fixture stageSchedulerFixture,
) *stagescheduler.Service {
	t.Helper()
	scheduling, err := stagescheduler.NewService(
		fixture.repository,
		fixture.coordinator,
		stagescheduler.Config{
			SchedulerID:      "stage-worker-control/integration",
			ClaimTTL:         30 * time.Second,
			LeaseTTL:         time.Minute,
			LocalDeadlineTTL: 50 * time.Second,
			SigningKeyID:     "stage-authority-key-v1",
		},
	)
	if err != nil {
		t.Fatalf("construct StageScheduler: %v", err)
	}
	return scheduling
}

func newPostgresAssignmentTestBackendWithScheduler(
	t *testing.T,
	fixture stageSchedulerFixture,
	scheduling stageworkercontrol.AssignmentScheduler,
) *stageworkercontrol.PostgresAssignmentBackend {
	t.Helper()
	authoritySigner, err := stageauthority.NewSigner(map[string][]byte{
		"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32),
	})
	if err != nil {
		t.Fatalf("construct StageAuthority signer: %v", err)
	}
	transferSigner, err := stageartifact.NewTransferTicketKeyringSigner(
		"stage-authority-key-v1",
		map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)},
	)
	if err != nil {
		t.Fatalf("construct TransferTicket signer: %v", err)
	}
	artifactRepository, err := stageartifact.NewPostgresRepository(newRolePool(
		t, fixture.database.DSN,
		"vela_stage_artifact_login", "vela-stage-artifact-password",
	))
	if err != nil {
		t.Fatalf("construct StageArtifact repository: %v", err)
	}
	transferIssuer, err := stageartifact.NewTransferTicketIssuer(
		artifactRepository, transferSigner,
	)
	if err != nil {
		t.Fatalf("construct TransferTicket issuer: %v", err)
	}
	backend, err := stageworkercontrol.NewPostgresAssignmentBackend(
		stageworkercontrol.PostgresAssignmentConfig{
			Pool: newRolePool(
				t, fixture.database.DSN,
				"vela_stage_worker_control_login", "vela-stage-worker-control-password",
			),
			Scheduler:          scheduling,
			AuthoritySigner:    authoritySigner,
			TransferTickets:    transferIssuer,
			IdentityKey:        bytes.Repeat([]byte{0x9c}, 32),
			NoWorkRetry:        250 * time.Millisecond,
			MemberStartTimeout: 30 * time.Second,
			TransferTicketTTL:  30 * time.Second,
		},
	)
	if err != nil {
		t.Fatalf("construct PostgresAssignmentBackend: %v", err)
	}
	return backend
}

type cancelAfterAssignmentScheduler struct {
	delegate stageworkercontrol.AssignmentScheduler
	cancel   context.CancelFunc
}

func (scheduler cancelAfterAssignmentScheduler) AcquireIdentified(
	ctx context.Context,
	authority stagescheduler.WorkerAuthority,
	observation stagescheduler.CapacityObservation,
	identity stagescheduler.AssignmentIdentity,
) (stagescheduler.Assignment, bool, error) {
	assignment, assigned, err := scheduler.delegate.AcquireIdentified(
		ctx, authority, observation, identity,
	)
	scheduler.cancel()
	return assignment, assigned, err
}

func assertSingleDurableAssignment(
	t *testing.T,
	fixture stageSchedulerFixture,
	commandID uuid.UUID,
) {
	t.Helper()
	var attempts, leases, results int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_leases WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_worker_acquire_results WHERE command_id = $2)
	`, fixture.stageRunID, commandID).Scan(&attempts, &leases, &results); err != nil {
		t.Fatalf("read recovered durable Assignment: %v", err)
	}
	if attempts != 1 || leases != 1 || results != 1 {
		t.Fatalf(
			"recovered durable Assignment attempts=%d leases=%d results=%d",
			attempts, leases, results,
		)
	}
}

func stageWorkerAcquireCommand(
	fixture stageSchedulerFixture,
) stageworkercontrol.CommandContext {
	return stageworkercontrol.CommandContext{
		CommandID: uuid.New(),
		Identity: stageworkertransport.Identity{
			SPIFFEID: "spiffe://vela/worker/" + fixture.authority.WorkerInstanceID.String(),
		},
		ControlSessionEpoch: 1,
	}
}

func stageWorkerAcquireRequest(fixture stageSchedulerFixture) *velav1.AcquireStageRequest {
	return &velav1.AcquireStageRequest{
		WorkerInstanceId:            fixture.authority.WorkerInstanceID.String(),
		WorkerInstanceEpoch:         fixture.authority.WorkerInstanceEpoch,
		CapacityObservationSequence: fixture.observation.Sequence,
		ModelResidencyId:            fixture.authority.ModelResidencyID.String(),
		ModelRuntimeEpoch:           fixture.authority.ModelRuntimeEpoch,
		StageProfileRevisionId:      fixture.authority.StageProfileRevisionID.String(),
	}
}
