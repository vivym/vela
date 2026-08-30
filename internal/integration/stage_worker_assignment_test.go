//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagescheduler"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

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

func TestStageWorkerAssignmentMigrationRoundTripAndDurableEvidenceRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
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

	t.Run("durable evidence refuses Down", func(t *testing.T) {
		fixture := newStageSchedulerFixture(t, "stage-worker-migration-refusal")
		backend := newPostgresAssignmentTestBackend(t, fixture)
		command := stageWorkerAcquireCommand(fixture)
		command.Identity.SPIFFEID += "/forged"
		if result, err := backend.AcquireStage(
			context.Background(), command, stageWorkerAcquireRequest(fixture),
		); err != nil || result.Command == nil {
			t.Fatalf("create durable acquire evidence = %#v error=%v", result, err)
		}
		err := goose.DownTo(fixture.database.Admin, migrations, 41)
		assertPostgresConstraint(t, err, "stage_worker_assignment_rollback_is_unsafe")
	})
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
