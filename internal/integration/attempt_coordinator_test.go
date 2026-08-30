//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/fleet"
)

const (
	graphExecutionProfileID = "49000000-0000-0000-0000-000000000070"
	graphSnapshotID         = "49300000-0000-0000-0000-000000000001"
	graphAttemptID          = "49300000-0000-0000-0000-000000000002"
	graphReservationID      = "49300000-0000-0000-0000-000000000003"
	encoderWorkerProfileID  = "49300000-0000-0000-0000-000000000030"
	encoderStageProfileID   = "49300000-0000-0000-0000-000000000031"
	ditWorkerProfileID      = "49300000-0000-0000-0000-000000000032"
	ditStageProfileID       = "49300000-0000-0000-0000-000000000033"
)

func TestAttemptCoordinatorInstantiateIsDurableAndIdempotent(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-coordinator-instantiate", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"stage graph authority"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode graph Job: %v", err)
	}

	coordinatorPool := newRolePool(
		t,
		database.DSN,
		"vela_attempt_coordinator_login",
		"vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct AttemptCoordinator: %v", err)
	}
	command := attemptcoordinator.InstantiateCommand{
		CommandID:                  uuid.MustParse("49300000-0000-0000-0000-000000000004"),
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExpectedJobFence:           0,
		ExecutionGraphSnapshotID:   uuid.MustParse(graphSnapshotID),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  uuid.MustParse(graphAttemptID),
		StorageReservationID:       uuid.MustParse(graphReservationID),
		ReservedStorageBytes:       2 << 30,
	}
	first, err := coordinator.Instantiate(context.Background(), command)
	if err != nil {
		t.Fatalf("instantiate Stage graph: %v", err)
	}
	replay, err := coordinator.Instantiate(context.Background(), command)
	if err != nil {
		t.Fatalf("replay Stage graph instantiation: %v", err)
	}
	if first.AttemptID != command.AttemptID || first.SnapshotID != command.ExecutionGraphSnapshotID ||
		first.AttemptFence != 1 || first.StageRunCount != 3 || first.Replayed {
		t.Fatalf("first AttemptHandle = %#v", first)
	}
	if replay.AttemptID != first.AttemptID || replay.SnapshotID != first.SnapshotID ||
		replay.AttemptFence != first.AttemptFence || replay.StageRunCount != 3 || !replay.Replayed {
		t.Fatalf("replayed AttemptHandle = %#v, first=%#v", replay, first)
	}

	rows, err := database.Admin.Query(`
		SELECT stage_key, state::text
		FROM stage_runs
		WHERE attempt_id = $1
		ORDER BY stage_key
	`, command.AttemptID)
	if err != nil {
		t.Fatalf("read instantiated StageRuns: %v", err)
	}
	defer rows.Close()
	states := map[string]string{}
	for rows.Next() {
		var key, state string
		if err := rows.Scan(&key, &state); err != nil {
			t.Fatalf("scan StageRun: %v", err)
		}
		states[key] = state
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate StageRuns: %v", err)
	}
	if states["encoder"] != "READY" || states["dit"] != "BLOCKED" || states["vae"] != "BLOCKED" {
		t.Fatalf("instantiated StageRun states = %#v", states)
	}

	var snapshotCount, attemptCount, dependencyCount, reservationCount int
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM execution_graph_snapshots WHERE id = $1),
			(SELECT count(*) FROM attempts WHERE id = $2),
			(SELECT count(*) FROM stage_dependencies WHERE attempt_id = $2),
			(SELECT count(*) FROM stage_storage_reservations WHERE attempt_id = $2)
	`, command.ExecutionGraphSnapshotID, command.AttemptID).Scan(
		&snapshotCount, &attemptCount, &dependencyCount, &reservationCount,
	); err != nil {
		t.Fatalf("read graph authority counts: %v", err)
	}
	if snapshotCount != 1 || attemptCount != 1 || dependencyCount != 2 || reservationCount != 1 {
		t.Fatalf(
			"graph authority counts snapshot=%d attempt=%d dependencies=%d reservation=%d",
			snapshotCount, attemptCount, dependencyCount, reservationCount,
		)
	}
}

func TestAttemptCoordinatorPhysicalStageLifecycleIsIdempotent(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedEncoderAssignmentProfile(t, database)
	activateH3StageGraph(t, database)
	seedWorkerRegistryPlan(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-coordinator-assign", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"assign encoder stage"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode graph Job: %v", err)
	}
	coordinatorPool := newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct AttemptCoordinator: %v", err)
	}
	_, err = coordinator.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
		CommandID:                  uuid.MustParse("49300000-0000-0000-0000-000000000014"),
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExpectedJobFence:           0,
		ExecutionGraphSnapshotID:   uuid.MustParse("49300000-0000-0000-0000-000000000011"),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  uuid.MustParse("49300000-0000-0000-0000-000000000012"),
		StorageReservationID:       uuid.MustParse("49300000-0000-0000-0000-000000000013"),
		ReservedStorageBytes:       2 << 30,
	})
	if err != nil {
		t.Fatalf("instantiate assign fixture: %v", err)
	}
	var encoderRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, "49300000-0000-0000-0000-000000000012").Scan(&encoderRunID); err != nil {
		t.Fatalf("read Encoder StageRun: %v", err)
	}

	const encoderPoolID = "49300000-0000-0000-0000-000000000020"
	workerID := uuid.MustParse("49300000-0000-0000-0000-000000000021")
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES ($1, 'h3-encoder-shanghai', $2, 'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE')
	`, encoderPoolID, encoderStageProfileID); err != nil {
		t.Fatalf("seed Encoder CapacityPool: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
	`, workerID, encoderWorkerProfileID, encoderPoolID, workerRegistryBundleID); err != nil {
		t.Fatalf("seed Encoder WorkerInstance: %v", err)
	}
	evidence := workerRegistryEvidenceValue(t, workerID, 0x31)
	evidence.Residencies[0].ModelComponentRevision = "h3-encoder-v1"
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	registry, err := fleet.NewService(fleetPool)
	if err != nil {
		t.Fatalf("construct Worker Registry: %v", err)
	}
	if _, err := registry.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe Encoder WorkerInstance: %v", err)
	}
	authority := workerAuthority(t, evidence)
	now := time.Now().UTC().Truncate(time.Millisecond)
	assign := attemptcoordinator.AssignStageCommand{
		CommandID:              uuid.MustParse("49300000-0000-0000-0000-000000000022"),
		AttemptID:              uuid.MustParse("49300000-0000-0000-0000-000000000012"),
		StageRunID:             encoderRunID,
		ExpectedAttemptFence:   1,
		ExpectedStageFence:     1,
		ExpectedStageVersion:   1,
		StageAttemptID:         uuid.MustParse("49300000-0000-0000-0000-000000000023"),
		StageAllocationID:      uuid.MustParse("49300000-0000-0000-0000-000000000024"),
		StageLeaseID:           uuid.MustParse("49300000-0000-0000-0000-000000000025"),
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		CapacityPoolID:         uuid.MustParse(encoderPoolID),
		WorkerInstanceID:       workerID,
		WorkerInstanceEpoch:    1,
		DeviceSetDigest:        authority.DeviceSetDigest,
		MembershipDigest:       authority.MembershipDigest,
		ModelResidencyID:       authority.ModelResidencyID,
		ModelRuntimeEpoch:      1,
		CapacityVector:         map[string]int64{"concurrency": 1},
		TokenDigest:            bytesOf(0x71, 32),
		SigningKeyID:           "stage-authority-key-v1",
		ExecutionNonce:         bytesOf(0x72, 32),
		IssuedAt:               now,
		ExpiresAt:              now.Add(time.Minute),
		LocalDeadlineAt:        now.Add(50 * time.Second),
	}
	first, err := coordinator.Apply(context.Background(), assign)
	if err != nil {
		t.Fatalf("assign Encoder StageRun: %v", err)
	}
	replay, err := coordinator.Apply(context.Background(), assign)
	if err != nil {
		t.Fatalf("replay Encoder assignment: %v", err)
	}
	if first.StageAttemptID != assign.StageAttemptID || first.State != "ASSIGNED" ||
		first.StageFence != 1 || first.StageVersion != 2 || first.Replayed {
		t.Fatalf("first StageDecision = %#v", first)
	}
	if replay.StageAttemptID != first.StageAttemptID || replay.State != first.State ||
		replay.StageVersion != first.StageVersion || !replay.Replayed {
		t.Fatalf("replayed StageDecision = %#v, first=%#v", replay, first)
	}
	var activeAttempts int
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM stage_attempts
		WHERE stage_run_id = $1 AND state IN ('ASSIGNED', 'RUNNING', 'OUTPUT_SEALED')
	`, encoderRunID).Scan(&activeAttempts); err != nil {
		t.Fatalf("count active StageAttempts: %v", err)
	}
	if activeAttempts != 1 {
		t.Fatalf("active StageAttempt count = %d, want 1", activeAttempts)
	}

	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin active StageAttempt invariant transaction: %v", err)
	}
	if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
		t.Fatalf("assume AttemptCoordinator owner role: %v", err)
	}
	_, err = tx.Exec(`
		INSERT INTO stage_attempts (
			id, organization_id, project_id, attempt_id, stage_run_id,
			physical_attempt_number, state, selected_stage_profile_revision_id, assigned_at
		) VALUES (
			$1, $2, $3, $4, $5, 2, 'ASSIGNED', $6, clock_timestamp()
		)
	`, uuid.New(), testOrganizationID, testProjectID, assign.AttemptID,
		encoderRunID, assign.StageProfileRevisionID)
	assertPostgresConstraint(t, err, "stage_attempts_one_active_per_stage_run_idx")
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback active StageAttempt invariant transaction: %v", err)
	}

	for _, test := range []struct {
		name       string
		statement  string
		constraint string
		target     uuid.UUID
		code       string
		message    string
	}{
		{
			name: "Graph Attempt fence",
			statement: `
				UPDATE attempts
				SET fence = fence + 1
				WHERE id = $1
			`,
			target:  assign.AttemptID,
			code:    "P0001",
			message: "immutable Attempt identity fields cannot be changed",
		},
		{
			name: "Graph Attempt state transition",
			statement: `
				UPDATE attempts
				SET state = 'FINALIZING', graph_state = 'FINALIZING'
				WHERE id = $1
			`,
			target:  assign.AttemptID,
			code:    "23514",
			message: "invalid Attempt state transition",
		},
		{
			name: "StageRun state transition",
			statement: `
				UPDATE stage_runs
				SET state = 'BLOCKED', version = version + 1
				WHERE id = $1
			`,
			constraint: "stage_run_transition_invalid",
			target:     encoderRunID,
		},
		{
			name: "StageAttempt identity",
			statement: `
				UPDATE stage_attempts
				SET physical_attempt_number = physical_attempt_number + 1
				WHERE id = $1
			`,
			constraint: "stage_attempt_identity_immutable",
			target:     assign.StageAttemptID,
		},
		{
			name: "StageAllocation identity",
			statement: `
				UPDATE stage_allocations
				SET model_runtime_epoch = model_runtime_epoch + 1
				WHERE stage_attempt_id = $1
			`,
			constraint: "stage_allocation_identity_immutable",
			target:     assign.StageAttemptID,
		},
		{
			name: "StageLease identity",
			statement: `
				UPDATE stage_leases
				SET token_digest = decode(repeat('ab', 32), 'hex')
				WHERE stage_attempt_id = $1
			`,
			constraint: "stage_lease_identity_immutable",
			target:     assign.StageAttemptID,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := database.Admin.Begin()
			if err != nil {
				t.Fatalf("begin rejected authority mutation: %v", err)
			}
			defer func() { _ = tx.Rollback() }()
			if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
				t.Fatalf("assume AttemptCoordinator owner role: %v", err)
			}
			_, err = tx.Exec(test.statement, test.target)
			if test.constraint != "" {
				assertPostgresConstraint(t, err, test.constraint)
				return
			}
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) || postgresError.Code != test.code ||
				!strings.Contains(postgresError.Message, test.message) {
				t.Fatalf(
					"PostgreSQL error = %v, want SQLSTATE %s containing %q",
					err, test.code, test.message,
				)
			}
		})
	}

	start := attemptcoordinator.StartStageCommand{
		CommandID:            uuid.MustParse("49300000-0000-0000-0000-000000000026"),
		AttemptID:            assign.AttemptID,
		StageRunID:           encoderRunID,
		StageAttemptID:       assign.StageAttemptID,
		StageLeaseID:         assign.StageLeaseID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 2,
		StartedAt:            now.Add(time.Second),
	}
	started, err := coordinator.Apply(context.Background(), start)
	if err != nil {
		t.Fatalf("start Encoder StageAttempt: %v", err)
	}
	startReplay, err := coordinator.Apply(context.Background(), start)
	if err != nil {
		t.Fatalf("replay Encoder start: %v", err)
	}
	if started.StageAttemptID != assign.StageAttemptID || started.State != "RUNNING" ||
		started.StageFence != 1 || started.StageVersion != 3 || started.Replayed {
		t.Fatalf("first Start StageDecision = %#v", started)
	}
	if startReplay.StageAttemptID != started.StageAttemptID ||
		startReplay.StageVersion != started.StageVersion || !startReplay.Replayed {
		t.Fatalf("replayed Start StageDecision = %#v, first=%#v", startReplay, started)
	}
	var jobState, graphState, runState string
	var billableStartedAt, attemptStartedAt time.Time
	var startCommandCount int
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, job.billable_started_at,
		       attempt.graph_state::text, attempt.started_at,
		       run.state::text,
		       (SELECT count(*) FROM attempt_coordinator_commands
		        WHERE attempt_id = attempt.id AND command_kind = 'START')
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN stage_runs AS run ON run.attempt_id = attempt.id AND run.id = $2
		WHERE job.id = $1
	`, job.JobID, encoderRunID).Scan(
		&jobState, &billableStartedAt, &graphState, &attemptStartedAt,
		&runState, &startCommandCount,
	); err != nil {
		t.Fatalf("read first-progress authority: %v", err)
	}
	if jobState != "RUNNING" || graphState != "RUNNING" || runState != "RUNNING" ||
		!billableStartedAt.Equal(start.StartedAt) || !attemptStartedAt.Equal(start.StartedAt) ||
		startCommandCount != 1 {
		t.Fatalf(
			"first progress job=%s billable=%s graph=%s attemptStart=%s run=%s commands=%d",
			jobState, billableStartedAt, graphState, attemptStartedAt, runState, startCommandCount,
		)
	}

	complete := attemptcoordinator.CompleteStageCommand{
		CommandID:            uuid.MustParse("49300000-0000-0000-0000-000000000027"),
		AttemptID:            assign.AttemptID,
		StageRunID:           encoderRunID,
		StageAttemptID:       assign.StageAttemptID,
		StageLeaseID:         assign.StageLeaseID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 3,
		ProgressReceiptID:    uuid.MustParse("49300000-0000-0000-0000-000000000028"),
		OutputIdentity:       "stage-output/encoder/exact-version-1",
		OutputDigest:         bytesOf(0x79, 32),
		CompletedAt:          now.Add(2 * time.Second),
	}
	completed, err := coordinator.Apply(context.Background(), complete)
	if err != nil {
		t.Fatalf("complete Encoder StageAttempt: %v", err)
	}
	completeReplay, err := coordinator.Apply(context.Background(), complete)
	if err != nil {
		t.Fatalf("replay Encoder completion: %v", err)
	}
	if completed.StageAttemptID != assign.StageAttemptID || completed.State != "SUCCEEDED" ||
		completed.StageFence != 1 || completed.StageVersion != 4 || completed.Replayed {
		t.Fatalf("first Complete StageDecision = %#v", completed)
	}
	if completeReplay.StageAttemptID != completed.StageAttemptID ||
		completeReplay.StageVersion != completed.StageVersion || !completeReplay.Replayed {
		t.Fatalf("replayed Complete StageDecision = %#v, first=%#v", completeReplay, completed)
	}
	var encoderState, ditState, physicalState, allocationState, leaseState, progressKind string
	var winnerAttemptID, winnerOutputID, dependencyReceipt uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT encoder.state::text, dit.state::text, physical.state::text,
		       allocation.state::text, lease.state::text, receipt.progress_kind::text,
		       encoder.winner_stage_attempt_id, encoder.winner_output_identity,
		       dependency.satisfied_progress_receipt_id
		FROM stage_runs AS encoder
		JOIN stage_runs AS dit ON dit.attempt_id = encoder.attempt_id AND dit.stage_key = 'dit'
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_allocations AS allocation ON allocation.stage_attempt_id = physical.id
		JOIN stage_leases AS lease ON lease.stage_attempt_id = physical.id
		JOIN stage_progress_receipts AS receipt ON receipt.stage_run_id = encoder.id
		JOIN stage_dependencies AS dependency
		  ON dependency.source_stage_run_id = encoder.id
		 AND dependency.destination_stage_run_id = dit.id
		WHERE encoder.id = $1
	`, encoderRunID, assign.StageAttemptID).Scan(
		&encoderState, &ditState, &physicalState, &allocationState, &leaseState,
		&progressKind, &winnerAttemptID, &winnerOutputID, &dependencyReceipt,
	); err != nil {
		t.Fatalf("read physical Stage completion: %v", err)
	}
	if encoderState != "SUCCEEDED" || ditState != "READY" || physicalState != "SUCCEEDED" ||
		allocationState != "RELEASED" || leaseState != "REVOKED" ||
		progressKind != "PHYSICAL_OUTPUT" || winnerAttemptID != assign.StageAttemptID ||
		winnerOutputID != complete.ProgressReceiptID || dependencyReceipt != complete.ProgressReceiptID {
		t.Fatalf(
			"physical completion encoder=%s dit=%s attempt=%s allocation=%s lease=%s kind=%s winners=%s/%s dependency=%s",
			encoderState, ditState, physicalState, allocationState, leaseState,
			progressKind, winnerAttemptID, winnerOutputID, dependencyReceipt,
		)
	}
}

func TestAttemptCoordinatorCacheProgressAndStageRetryPreserveUpstreamIdentity(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedDiTAssignmentProfile(t, database)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "attempt-coordinator-cache-advance", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"exact cached conditioning"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit cache graph Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode cache graph Job: %v", err)
	}
	coordinatorPool := newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct AttemptCoordinator: %v", err)
	}
	attemptID := uuid.MustParse("49300000-0000-0000-0000-000000000042")
	_, err = coordinator.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
		CommandID:                  uuid.MustParse("49300000-0000-0000-0000-000000000044"),
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExpectedJobFence:           0,
		ExecutionGraphSnapshotID:   uuid.MustParse("49300000-0000-0000-0000-000000000041"),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  attemptID,
		StorageReservationID:       uuid.MustParse("49300000-0000-0000-0000-000000000043"),
		ReservedStorageBytes:       2 << 30,
	})
	if err != nil {
		t.Fatalf("instantiate exact cache fixture: %v", err)
	}
	var encoderRunID, ditRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'dit')
	`, attemptID).Scan(&encoderRunID, &ditRunID); err != nil {
		t.Fatalf("read cache fixture StageRuns: %v", err)
	}
	advancedAt := time.Now().UTC().Truncate(time.Millisecond)
	advance := attemptcoordinator.ExactCacheAdvanceCommand{
		CommandID:            uuid.MustParse("49300000-0000-0000-0000-000000000045"),
		AttemptID:            attemptID,
		StageRunID:           encoderRunID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 1,
		ProgressReceiptID:    uuid.MustParse("49300000-0000-0000-0000-000000000046"),
		CacheSourceIdentity:  "shadow-cache-entry/encoder/exact-v1",
		OutputDigest:         bytesOf(0x81, 32),
		AdvancedAt:           advancedAt,
	}
	first, err := coordinator.Apply(context.Background(), advance)
	if err != nil {
		t.Fatalf("advance exact cache hit: %v", err)
	}
	replay, err := coordinator.Apply(context.Background(), advance)
	if err != nil {
		t.Fatalf("replay exact cache advance: %v", err)
	}
	if first.StageRunID != encoderRunID || first.StageAttemptID != uuid.Nil ||
		first.State != "SUCCEEDED" || first.StageFence != 1 || first.StageVersion != 2 ||
		first.Replayed {
		t.Fatalf("first exact-cache StageDecision = %#v", first)
	}
	if replay.StageRunID != first.StageRunID || replay.StageAttemptID != uuid.Nil ||
		replay.StageVersion != first.StageVersion || !replay.Replayed {
		t.Fatalf("replayed exact-cache StageDecision = %#v, first=%#v", replay, first)
	}
	var jobState, graphState, encoderState, ditState, progressKind string
	var billableStartedAt time.Time
	var dependencyReceipt uuid.UUID
	var progressCount int
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, job.billable_started_at, attempt.graph_state::text,
		       encoder.state::text, dit.state::text, receipt.progress_kind::text,
		       dependency.satisfied_progress_receipt_id,
		       (SELECT count(*) FROM stage_progress_receipts WHERE stage_run_id = encoder.id)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN stage_runs AS encoder ON encoder.attempt_id = attempt.id AND encoder.id = $2
		JOIN stage_runs AS dit ON dit.attempt_id = attempt.id AND dit.id = $3
		JOIN stage_progress_receipts AS receipt ON receipt.stage_run_id = encoder.id
		JOIN stage_dependencies AS dependency
		  ON dependency.source_stage_run_id = encoder.id
		 AND dependency.destination_stage_run_id = dit.id
		WHERE job.id = $1
	`, job.JobID, encoderRunID, ditRunID).Scan(
		&jobState, &billableStartedAt, &graphState, &encoderState, &ditState,
		&progressKind, &dependencyReceipt, &progressCount,
	); err != nil {
		t.Fatalf("read exact cache graph advancement: %v", err)
	}
	if jobState != "RUNNING" || graphState != "RUNNING" || encoderState != "SUCCEEDED" ||
		ditState != "READY" || progressKind != "EXACT_CACHE" ||
		dependencyReceipt != advance.ProgressReceiptID || progressCount != 1 ||
		!billableStartedAt.Equal(advancedAt) {
		t.Fatalf(
			"cache advance job=%s billable=%s graph=%s encoder=%s dit=%s kind=%s receipt=%s count=%d",
			jobState, billableStartedAt, graphState, encoderState, ditState,
			progressKind, dependencyReceipt, progressCount,
		)
	}

	seedWorkerRegistryPlan(t, database.Admin)
	const ditPoolID = "49300000-0000-0000-0000-000000000050"
	ditWorkerID := uuid.MustParse("49300000-0000-0000-0000-000000000051")
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES ($1, 'h3-dit-retry-shanghai', $2, 'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE')
	`, ditPoolID, ditStageProfileID); err != nil {
		t.Fatalf("seed DiT retry CapacityPool: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
	`, ditWorkerID, ditWorkerProfileID, ditPoolID, workerRegistryBundleID); err != nil {
		t.Fatalf("seed DiT retry WorkerInstance: %v", err)
	}
	ditEvidence := workerRegistryEvidenceValue(t, ditWorkerID, 0x41)
	ditEvidence.Residencies[0].ModelComponentRevision = "h3-dit-v1"
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	registry, err := fleet.NewService(fleetPool)
	if err != nil {
		t.Fatalf("construct Worker Registry for DiT retry: %v", err)
	}
	if _, err := registry.Observe(context.Background(), ditEvidence); err != nil {
		t.Fatalf("observe DiT retry WorkerInstance: %v", err)
	}
	ditAuthority := workerAuthority(t, ditEvidence)
	assign := attemptcoordinator.AssignStageCommand{
		CommandID:              uuid.MustParse("49300000-0000-0000-0000-000000000052"),
		AttemptID:              attemptID,
		StageRunID:             ditRunID,
		ExpectedAttemptFence:   1,
		ExpectedStageFence:     1,
		ExpectedStageVersion:   2,
		StageAttemptID:         uuid.MustParse("49300000-0000-0000-0000-000000000053"),
		StageAllocationID:      uuid.MustParse("49300000-0000-0000-0000-000000000054"),
		StageLeaseID:           uuid.MustParse("49300000-0000-0000-0000-000000000055"),
		StageProfileRevisionID: uuid.MustParse(ditStageProfileID),
		CapacityPoolID:         uuid.MustParse(ditPoolID),
		WorkerInstanceID:       ditWorkerID,
		WorkerInstanceEpoch:    1,
		DeviceSetDigest:        ditAuthority.DeviceSetDigest,
		MembershipDigest:       ditAuthority.MembershipDigest,
		ModelResidencyID:       ditAuthority.ModelResidencyID,
		ModelRuntimeEpoch:      1,
		CapacityVector:         map[string]int64{"concurrency": 1},
		TokenDigest:            bytesOf(0x82, 32),
		SigningKeyID:           "stage-authority-key-v1",
		ExecutionNonce:         bytesOf(0x83, 32),
		IssuedAt:               advancedAt.Add(time.Millisecond),
		ExpiresAt:              advancedAt.Add(time.Minute),
		LocalDeadlineAt:        advancedAt.Add(50 * time.Second),
	}
	assigned, err := coordinator.Apply(context.Background(), assign)
	if err != nil {
		t.Fatalf("assign DiT retry StageRun: %v", err)
	}
	if assigned.StageVersion != 3 {
		t.Fatalf("assigned DiT StageDecision = %#v", assigned)
	}
	start := attemptcoordinator.StartStageCommand{
		CommandID:            uuid.MustParse("49300000-0000-0000-0000-000000000056"),
		AttemptID:            attemptID,
		StageRunID:           ditRunID,
		StageAttemptID:       assign.StageAttemptID,
		StageLeaseID:         assign.StageLeaseID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 3,
		StartedAt:            advancedAt.Add(2 * time.Millisecond),
	}
	started, err := coordinator.Apply(context.Background(), start)
	if err != nil {
		t.Fatalf("start DiT retry StageAttempt: %v", err)
	}
	if started.StageVersion != 4 {
		t.Fatalf("started DiT StageDecision = %#v", started)
	}
	fail := attemptcoordinator.FailStageCommand{
		CommandID:             uuid.MustParse("49300000-0000-0000-0000-000000000057"),
		AttemptID:             attemptID,
		StageRunID:            ditRunID,
		StageAttemptID:        assign.StageAttemptID,
		StageLeaseID:          assign.StageLeaseID,
		ExpectedAttemptFence:  1,
		ExpectedStageFence:    1,
		ExpectedStageVersion:  4,
		FailureClass:          "TRANSIENT_BACKEND",
		FailureFingerprint:    bytesOf(0x84, 32),
		ConsumedResourceUnits: 15,
		FailedAt:              advancedAt.Add(3 * time.Millisecond),
		RetryAt:               advancedAt.Add(4 * time.Millisecond),
	}
	failed, err := coordinator.Apply(context.Background(), fail)
	if err != nil {
		t.Fatalf("fail DiT StageAttempt for retry: %v", err)
	}
	if failed.StageAttemptID != assign.StageAttemptID || failed.State != "RETRY_WAIT" ||
		failed.StageFence != 2 || failed.StageVersion != 5 || failed.Replayed {
		t.Fatalf("failed DiT StageDecision = %#v", failed)
	}
	var ditFence int64
	var ditRetryCount int
	var consumedUnits int64
	var retryPhysicalState, retryAllocationState, retryLeaseState string
	if err := database.Admin.QueryRow(`
		SELECT dit.state::text, dit.fence, dit.retry_count,
		       physical.state::text, allocation.state::text, lease.state::text,
		       dependency.satisfied_progress_receipt_id,
		       budget.consumed_resource_units
		FROM stage_runs AS dit
		JOIN stage_attempts AS physical ON physical.id = $2
		JOIN stage_allocations AS allocation ON allocation.stage_attempt_id = physical.id
		JOIN stage_leases AS lease ON lease.stage_attempt_id = physical.id
		JOIN stage_dependencies AS dependency ON dependency.destination_stage_run_id = dit.id
		JOIN attempt_retry_budgets AS budget ON budget.attempt_id = dit.attempt_id
		WHERE dit.id = $1
	`, ditRunID, assign.StageAttemptID).Scan(
		&ditState, &ditFence, &ditRetryCount, &retryPhysicalState, &retryAllocationState,
		&retryLeaseState, &dependencyReceipt, &consumedUnits,
	); err != nil {
		t.Fatalf("read DiT retry authority: %v", err)
	}
	if ditState != "RETRY_WAIT" || ditFence != 2 || ditRetryCount != 1 ||
		retryPhysicalState != "FAILED" || retryAllocationState != "RELEASED" ||
		retryLeaseState != "REVOKED" || dependencyReceipt != advance.ProgressReceiptID ||
		consumedUnits != fail.ConsumedResourceUnits {
		t.Fatalf(
			"DiT retry state=%s fence=%d retries=%d physical=%s allocation=%s lease=%s dependency=%s consumed=%d",
			ditState, ditFence, ditRetryCount, retryPhysicalState, retryAllocationState,
			retryLeaseState, dependencyReceipt, consumedUnits,
		)
	}

	reconciled, err := coordinator.Reconcile(context.Background(), 10)
	if err != nil {
		t.Fatalf("reconcile due DiT retry: %v", err)
	}
	if len(reconciled) != 1 || reconciled[0].AttemptID != attemptID ||
		reconciled[0].StageRunID != ditRunID || reconciled[0].State != "READY" ||
		reconciled[0].StageFence != 2 || reconciled[0].StageVersion != 6 ||
		reconciled[0].Reason != "RETRY_DUE" {
		t.Fatalf("reconciled DiT retry = %#v", reconciled)
	}
	replayed, err := coordinator.Reconcile(context.Background(), 10)
	if err != nil {
		t.Fatalf("replay due DiT retry reconciliation: %v", err)
	}
	if len(replayed) != 0 {
		t.Fatalf("replayed DiT retry reconciliation = %#v", replayed)
	}
}

func TestStageGraphRunningCancellationPostsChargeAndFencesLateProgress(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Stage graph cancellation scope: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "cancel-stage-graph-running", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"cancel a running durable stage graph"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit Stage graph cancellation Job status = %d, body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode Stage graph cancellation Job: %v", err)
	}
	coordinatorPool := newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct cancellation AttemptCoordinator: %v", err)
	}
	attemptID := uuid.MustParse("49400000-0000-0000-0000-000000000001")
	_, err = coordinator.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
		CommandID:                  uuid.MustParse("49400000-0000-0000-0000-000000000002"),
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExpectedJobFence:           0,
		ExecutionGraphSnapshotID:   uuid.MustParse("49400000-0000-0000-0000-000000000003"),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  attemptID,
		StorageReservationID:       uuid.MustParse("49400000-0000-0000-0000-000000000004"),
		ReservedStorageBytes:       2 << 30,
	})
	if err != nil {
		t.Fatalf("instantiate cancellable Stage graph: %v", err)
	}
	var encoderRunID, ditRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'dit')
	`, attemptID).Scan(&encoderRunID, &ditRunID); err != nil {
		t.Fatalf("read cancellable StageRuns: %v", err)
	}
	advancedAt := time.Now().UTC().Truncate(time.Millisecond)
	_, err = coordinator.Apply(context.Background(), attemptcoordinator.ExactCacheAdvanceCommand{
		CommandID:            uuid.MustParse("49400000-0000-0000-0000-000000000005"),
		AttemptID:            attemptID,
		StageRunID:           encoderRunID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 1,
		ProgressReceiptID:    uuid.MustParse("49400000-0000-0000-0000-000000000006"),
		CacheSourceIdentity:  "stage-cancel/cache/encoder/exact-v1",
		OutputDigest:         bytesOf(0x91, 32),
		AdvancedAt:           advancedAt,
	})
	if err != nil {
		t.Fatalf("start cancellable graph with exact cache progress: %v", err)
	}

	first := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if first.StatusCode != http.StatusOK {
		t.Fatalf("cancel Stage graph status = %d, body=%s", first.StatusCode, first.Body)
	}
	replay := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if replay.StatusCode != http.StatusOK {
		t.Fatalf("replay Stage graph cancellation status = %d, body=%s", replay.StatusCode, replay.Body)
	}
	var firstResult, replayResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode Stage graph cancellation: %v", err)
	}
	if err := json.Unmarshal(replay.Body, &replayResult); err != nil {
		t.Fatalf("decode replayed Stage graph cancellation: %v", err)
	}
	if firstResult.CancellationID == "" ||
		firstResult.CancellationID != replayResult.CancellationID ||
		firstResult.Decision != "CANCELED" || replayResult.Decision != "CANCELED" ||
		firstResult.State != "CANCELED" || replayResult.State != "CANCELED" ||
		firstResult.JobVersion != 5 || replayResult.JobVersion != 5 ||
		!firstResult.Billable || !replayResult.Billable ||
		firstResult.Charge == nil || replayResult.Charge == nil ||
		firstResult.Charge.Amount != 1250 || replayResult.Charge.Amount != 1250 ||
		firstResult.Charge.Currency != "CNY" || replayResult.Charge.Currency != "CNY" ||
		firstResult.Charge.Reason != "CUSTOMER_CANCELLATION" ||
		replayResult.Charge.ChargeID != firstResult.Charge.ChargeID {
		t.Fatalf("Stage graph first/replayed cancellation = %#v / %#v", firstResult, replayResult)
	}

	_, err = coordinator.Apply(context.Background(), attemptcoordinator.ExactCacheAdvanceCommand{
		CommandID:            uuid.MustParse("49400000-0000-0000-0000-000000000007"),
		AttemptID:            attemptID,
		StageRunID:           ditRunID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 2,
		ProgressReceiptID:    uuid.MustParse("49400000-0000-0000-0000-000000000008"),
		CacheSourceIdentity:  "stage-cancel/cache/dit/late-v1",
		OutputDigest:         bytesOf(0x92, 32),
		AdvancedAt:           advancedAt.Add(time.Second),
	})
	if err == nil {
		t.Fatal("late Stage progress succeeded after Customer Cancellation fenced the graph")
	}
}

func TestStageGraphQueuedCancellationReleasesGraphAuthorityWithoutCharge(t *testing.T) {
	database, serverURL, coordinator, job, attemptID, _, _ :=
		newStageGraphCancellationFixture(t, "cancel-stage-graph-queued")

	first := cancelJob(t, serverURL, testProjectID, job.JobID, testBearerCredential())
	replay := cancelJob(t, serverURL, testProjectID, job.JobID, testBearerCredential())
	if first.StatusCode != http.StatusOK || replay.StatusCode != http.StatusOK {
		t.Fatalf(
			"queued Stage graph cancellation statuses = %d/%d bodies=%s/%s",
			first.StatusCode, replay.StatusCode, first.Body, replay.Body,
		)
	}
	var firstResult, replayResult cancelResponse
	if err := json.Unmarshal(first.Body, &firstResult); err != nil {
		t.Fatalf("decode queued Stage graph cancellation: %v", err)
	}
	if err := json.Unmarshal(replay.Body, &replayResult); err != nil {
		t.Fatalf("decode replayed queued Stage graph cancellation: %v", err)
	}
	if firstResult.CancellationID == "" ||
		firstResult.CancellationID != replayResult.CancellationID ||
		firstResult.Decision != "CANCELED" || replayResult.Decision != "CANCELED" ||
		firstResult.State != "CANCELED" || replayResult.State != "CANCELED" ||
		firstResult.JobVersion != 3 || replayResult.JobVersion != 3 ||
		firstResult.Billable || replayResult.Billable ||
		firstResult.Charge != nil || replayResult.Charge != nil {
		t.Fatalf("queued Stage graph first/replayed cancellation = %#v / %#v", firstResult, replayResult)
	}

	var jobState, attemptState, storageState, reservationState string
	var activeRuns, activeLeases, activeAllocations, decisions, charges int64
	var projectQueued, poolQueued, reservedMinor int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.graph_state::text, storage.state::text,
		       credit_reservation.state::text,
		       (SELECT count(*) FROM stage_runs
		        WHERE attempt_id = attempt.id AND state NOT IN ('SUCCEEDED', 'FAILED', 'CANCELED')),
		       (SELECT count(*) FROM stage_leases
		        WHERE attempt_id = attempt.id AND state = 'ACTIVE'),
		       (SELECT count(*) FROM stage_allocations
		        WHERE attempt_id = attempt.id AND state = 'ALLOCATED'),
		       (SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id),
		       project.queued_count, pool.queued_count, account.reserved_minor
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $1
		JOIN stage_storage_reservations AS storage ON storage.attempt_id = attempt.id
		JOIN credit_reservations AS credit_reservation ON credit_reservation.job_id = job.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN worker_pools AS pool ON pool.id = job.worker_pool_id
		JOIN organization_credit_accounts AS account
		  ON account.organization_id = job.organization_id
		WHERE job.id = $2
	`, attemptID, job.JobID).Scan(
		&jobState, &attemptState, &storageState, &reservationState,
		&activeRuns, &activeLeases, &activeAllocations, &decisions, &charges,
		&projectQueued, &poolQueued, &reservedMinor,
	); err != nil {
		t.Fatalf("read queued Stage graph cancellation authority: %v", err)
	}
	if jobState != "CANCELED" || attemptState != "CANCELED" ||
		storageState != "RELEASED" || reservationState != "RELEASED" ||
		activeRuns != 0 || activeLeases != 0 || activeAllocations != 0 ||
		decisions != 1 || charges != 0 || projectQueued != 0 || poolQueued != 0 ||
		reservedMinor != 0 {
		t.Fatalf(
			"queued Stage graph cancellation = job/attempt/storage/credit %s/%s/%s/%s active %d/%d/%d decisions/charges %d/%d counters %d/%d/%d",
			jobState, attemptState, storageState, reservationState,
			activeRuns, activeLeases, activeAllocations, decisions, charges,
			projectQueued, poolQueued, reservedMinor,
		)
	}
	if reconciled, err := coordinator.Reconcile(context.Background(), 10); err != nil || len(reconciled) != 0 {
		t.Fatalf("reconcile canceled queued Stage graph = %#v error=%v", reconciled, err)
	}
}

func TestStageGraphCancellationFailsClosedOnQueueCounterDrift(t *testing.T) {
	database, serverURL, _, job, attemptID, _, _ :=
		newStageGraphCancellationFixture(t, "cancel-stage-graph-counter-drift")
	if _, err := database.Admin.Exec(`
		UPDATE projects SET queued_count = 0 WHERE id = $1
	`, testProjectID); err != nil {
		t.Fatalf("inject Stage graph Project counter drift: %v", err)
	}

	result := cancelJob(t, serverURL, testProjectID, job.JobID, testBearerCredential())
	if result.StatusCode != http.StatusInternalServerError {
		t.Fatalf(
			"Stage graph cancellation with counter drift status = %d body=%s, want 500",
			result.StatusCode, result.Body,
		)
	}
	var jobState, attemptState, reservationState string
	var decisions, charges, canceledRuns int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, attempt.graph_state::text, reservation.state::text,
		       (SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id),
		       (SELECT count(*) FROM stage_runs
		        WHERE attempt_id = attempt.id AND state = 'CANCELED')
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $1
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		WHERE job.id = $2
	`, attemptID, job.JobID).Scan(
		&jobState, &attemptState, &reservationState, &decisions, &charges, &canceledRuns,
	); err != nil {
		t.Fatalf("read rejected Stage graph cancellation effects: %v", err)
	}
	if jobState != "QUEUED" || attemptState != "QUEUED" ||
		reservationState != "RESERVED" || decisions != 0 || charges != 0 ||
		canceledRuns != 0 {
		t.Fatalf(
			"rejected Stage graph cancellation = job/attempt/reservation %s/%s/%s decisions/charges/runs %d/%d/%d",
			jobState, attemptState, reservationState, decisions, charges, canceledRuns,
		)
	}
}

func TestStageGraphCancellationAndFirstProgressProduceOneTerminalBillingOutcome(t *testing.T) {
	database, serverURL, coordinator, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "cancel-stage-graph-first-progress-race")
	advancedAt := time.Now().UTC().Truncate(time.Millisecond)
	advance := attemptcoordinator.ExactCacheAdvanceCommand{
		CommandID:            uuid.New(),
		AttemptID:            attemptID,
		StageRunID:           encoderRunID,
		ExpectedAttemptFence: 1,
		ExpectedStageFence:   1,
		ExpectedStageVersion: 1,
		ProgressReceiptID:    uuid.New(),
		CacheSourceIdentity:  "stage-cancel/cache/encoder/race-v1",
		OutputDigest:         bytesOf(0x93, 32),
		AdvancedAt:           advancedAt,
	}
	type advanceCall struct {
		decision attemptcoordinator.StageDecision
		err      error
	}
	type cancelCall struct {
		result httpResult
		err    error
	}
	start := make(chan struct{})
	advanceResult := make(chan advanceCall, 1)
	cancelResult := make(chan cancelCall, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	go func() {
		<-start
		decision, err := coordinator.Apply(ctx, advance)
		advanceResult <- advanceCall{decision: decision, err: err}
	}()
	go func() {
		<-start
		result, err := doCancelJob(
			serverURL, testProjectID, job.JobID, testBearerCredential(),
		)
		cancelResult <- cancelCall{result: result, err: err}
	}()
	close(start)
	advanced := <-advanceResult
	canceled := <-cancelResult
	if canceled.err != nil || canceled.result.StatusCode != http.StatusOK {
		t.Fatalf(
			"concurrent Stage graph cancellation = status %d body=%s error=%v",
			canceled.result.StatusCode, canceled.result.Body, canceled.err,
		)
	}
	var response cancelResponse
	if err := json.Unmarshal(canceled.result.Body, &response); err != nil {
		t.Fatalf("decode Stage graph progress race cancellation: %v", err)
	}

	var jobState, attemptState, reservationState string
	var jobVersion, decisionCount, chargeCount, receiptCount int64
	var reservedMinor, unsettledPostedMinor int64
	if err := database.Admin.QueryRow(`
		SELECT job.state::text, job.version, attempt.graph_state::text,
		       reservation.state::text, account.reserved_minor,
		       account.unsettled_posted_minor,
		       (SELECT count(*) FROM job_cancellation_decisions WHERE job_id = job.id),
		       (SELECT count(*) FROM charges WHERE job_id = job.id),
		       (SELECT count(*) FROM stage_progress_receipts WHERE attempt_id = attempt.id)
		FROM jobs AS job
		JOIN attempts AS attempt ON attempt.id = $1
		JOIN credit_reservations AS reservation ON reservation.job_id = job.id
		JOIN organization_credit_accounts AS account
		  ON account.organization_id = job.organization_id
		WHERE job.id = $2
	`, attemptID, job.JobID).Scan(
		&jobState, &jobVersion, &attemptState, &reservationState,
		&reservedMinor, &unsettledPostedMinor,
		&decisionCount, &chargeCount, &receiptCount,
	); err != nil {
		t.Fatalf("read Stage graph progress race result: %v", err)
	}
	if jobState != "CANCELED" || attemptState != "CANCELED" || decisionCount != 1 {
		t.Fatalf(
			"Stage graph progress race authority = job %s attempt %s decisions %d",
			jobState, attemptState, decisionCount,
		)
	}
	if advanced.err == nil {
		if advanced.decision.State != "SUCCEEDED" || response.JobVersion != 5 ||
			jobVersion != 5 || !response.Billable || response.Charge == nil ||
			response.Charge.Amount != 1250 || reservationState != "CONSUMED" ||
			reservedMinor != 0 || unsettledPostedMinor != 1250 ||
			chargeCount != 1 || receiptCount != 1 {
			t.Fatalf(
				"progress-winning cancellation = advance %#v response %#v jobVersion %d reservation %s credit %d/%d charges/receipts %d/%d",
				advanced.decision, response, jobVersion, reservationState,
				reservedMinor, unsettledPostedMinor, chargeCount, receiptCount,
			)
		}
	} else if response.JobVersion != 3 || jobVersion != 3 || response.Billable ||
		response.Charge != nil || reservationState != "RELEASED" ||
		reservedMinor != 0 || unsettledPostedMinor != 0 ||
		chargeCount != 0 || receiptCount != 0 {
		t.Fatalf(
			"cancellation-winning progress race = error %v response %#v jobVersion %d reservation %s credit %d/%d charges/receipts %d/%d",
			advanced.err, response, jobVersion, reservationState,
			reservedMinor, unsettledPostedMinor, chargeCount, receiptCount,
		)
	}
}

func TestStageAttemptAuthorityMigrationRoundTripAndDurableAuthorityRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up restores legacy cancellation surface", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 35); err != nil {
			t.Fatalf("migrate empty Stage Attempt authority down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 35 {
			t.Fatalf("Stage Attempt authority version after Down = %d error=%v", version, err)
		}
		var currentCancellation, legacyCancellation, stageRuns bool
		var stageAwareLegacyTriggers int
		if err := database.Admin.QueryRow(`
			SELECT
				to_regprocedure(
				  'vela_cancel_job(uuid,uuid,uuid,uuid,uuid,uuid,uuid,uuid)'
				) IS NOT NULL,
				to_regprocedure(
				  'vela_cancel_legacy_job(uuid,uuid,uuid,uuid,uuid,uuid,uuid,uuid)'
				) IS NOT NULL,
				to_regclass('stage_runs') IS NOT NULL,
				(SELECT count(*)
				 FROM unnest(ARRAY[
				   'vela_bind_attempt_profile_certification()'::regprocedure,
				   'vela_enforce_scheduler_dispatch_attempt()'::regprocedure,
				   'vela_guard_fleet_assignment_writer()'::regprocedure,
				   'vela_validate_job_state_transition()'::regprocedure,
				   'vela_reject_billable_start_mutation()'::regprocedure
				 ]) AS function_id
				 WHERE pg_get_functiondef(function_id) LIKE '%execution_authority_kind%')
		`).Scan(
			&currentCancellation, &legacyCancellation, &stageRuns, &stageAwareLegacyTriggers,
		); err != nil {
			t.Fatalf("inspect schema 35 Stage Attempt rollback surface: %v", err)
		}
		if !currentCancellation || legacyCancellation || stageRuns || stageAwareLegacyTriggers != 0 {
			t.Fatalf(
				"schema 35 rollback surface = current/legacy/stageRuns %t/%t/%t stage-aware-triggers=%d",
				currentCancellation, legacyCancellation, stageRuns, stageAwareLegacyTriggers,
			)
		}
		if err := goose.UpTo(database.Admin, migrations, 36); err != nil {
			t.Fatalf("migrate Stage Attempt authority up again: %v", err)
		}
		version, err = goose.GetDBVersion(database.Admin)
		if err != nil || version != 36 {
			t.Fatalf("Stage Attempt authority version after Down Up = %d error=%v", version, err)
		}
	})

	t.Run("durable Stage graph authority refuses Down", func(t *testing.T) {
		database, _, _, _, attemptID, _, _ :=
			newStageGraphCancellationFixture(t, "stage-attempt-migration-refusal")
		err := goose.DownTo(database.Admin, migrations, 35)
		assertPostgresConstraint(t, err, "stage_attempt_authority_rollback_is_unsafe")
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 36 {
			t.Fatalf(
				"Stage Attempt authority version after refusal = %d error=%v",
				version, versionErr,
			)
		}
		var attempts, runs int64
		if err := database.Admin.QueryRow(`
			SELECT
				(SELECT count(*) FROM attempts WHERE id = $1),
				(SELECT count(*) FROM stage_runs WHERE attempt_id = $1)
		`, attemptID).Scan(&attempts, &runs); err != nil {
			t.Fatalf("read durable Stage graph authority after refusal: %v", err)
		}
		if attempts != 1 || runs != 3 {
			t.Fatalf("durable Stage graph authority after refusal = attempts/runs %d/%d", attempts, runs)
		}
	})
}

func newStageGraphCancellationFixture(
	t *testing.T,
	idempotencyKey string,
) (testDatabase, string, *attemptcoordinator.Service, jobResponse, uuid.UUID, uuid.UUID, uuid.UUID) {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	if _, err := database.Admin.Exec(`
		UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel']
		WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatalf("grant Stage graph cancellation scope: %v", err)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, idempotencyKey, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"race durable stage graph authority"
	}`))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("submit cancellable Stage graph status = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode cancellable Stage graph Job: %v", err)
	}
	coordinatorPool := newRolePool(
		t, database.DSN, "vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("construct cancellable AttemptCoordinator: %v", err)
	}
	attemptID := uuid.New()
	_, err = coordinator.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
		CommandID:                  uuid.New(),
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExpectedJobFence:           0,
		ExecutionGraphSnapshotID:   uuid.New(),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  attemptID,
		StorageReservationID:       uuid.New(),
		ReservedStorageBytes:       2 << 30,
	})
	if err != nil {
		t.Fatalf("instantiate cancellable Stage graph: %v", err)
	}
	var encoderRunID, ditRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
			(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'dit')
	`, attemptID).Scan(&encoderRunID, &ditRunID); err != nil {
		t.Fatalf("read cancellable StageRuns: %v", err)
	}
	return database, server.URL, coordinator, job, attemptID, encoderRunID, ditRunID
}

func bytesOf(value byte, count int) []byte {
	result := make([]byte, count)
	for index := range result {
		result[index] = value
	}
	return result
}

func seedEncoderAssignmentProfile(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_profile_revisions (
			id, stable_id, revision, state, device_count, member_count,
			device_set_shape, resident_model_revisions, capacity_limits,
			readiness_checks, content_digest
		) VALUES (
			$1, 'h3-encoder-dedicated', 1, 'CERTIFIED', 1, 1,
			'{"kind":"single-gpu"}', '["h3-encoder-v1"]',
			'{"concurrency":1}', '{"warmup":true}', decode(repeat('73', 32), 'hex')
		)
	`, encoderWorkerProfileID); err != nil {
		t.Fatalf("seed Encoder WorkerProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO stage_profile_revisions (
			id, stable_id, revision, state, stage_definition_revision_id,
			model_component_revision, runtime_image_digest, worker_profile_revision_id,
			result_equivalence_revision_id, certified_capacity_vector, content_digest
		) VALUES (
			$1, 'h3-encoder-dedicated', 1, 'CERTIFIED',
			'49000000-0000-0000-0000-000000000030', 'h3-encoder-v1',
			'sha256:3333333333333333333333333333333333333333333333333333333333333333',
			$2, '49000000-0000-0000-0000-000000000023',
			'{"concurrency":1}', decode(repeat('74', 32), 'hex')
		)
	`, encoderStageProfileID, encoderWorkerProfileID); err != nil {
		t.Fatalf("seed Encoder StageProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id,
			preference, eligibility_metadata
		) VALUES (
			$1, $2, 'encoder', '49000000-0000-0000-0000-000000000030',
			$3, -1, '{"dedicated_model":true}'
		)
	`, graphExecutionProfileID, stageGraphID, encoderStageProfileID); err != nil {
		t.Fatalf("seed Encoder StageProfile option: %v", err)
	}
}

func seedDiTAssignmentProfile(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_profile_revisions (
			id, stable_id, revision, state, device_count, member_count,
			device_set_shape, resident_model_revisions, capacity_limits,
			readiness_checks, content_digest
		) VALUES (
			$1, 'h3-dit-dedicated', 1, 'CERTIFIED', 1, 1,
			'{"kind":"single-gpu"}', '["h3-dit-v1"]',
			'{"concurrency":1}', '{"warmup":true}', decode(repeat('75', 32), 'hex')
		)
	`, ditWorkerProfileID); err != nil {
		t.Fatalf("seed DiT WorkerProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO stage_profile_revisions (
			id, stable_id, revision, state, stage_definition_revision_id,
			model_component_revision, runtime_image_digest, worker_profile_revision_id,
			result_equivalence_revision_id, certified_capacity_vector, content_digest
		) VALUES (
			$1, 'h3-dit-dedicated', 1, 'CERTIFIED',
			'49000000-0000-0000-0000-000000000031', 'h3-dit-v1',
			'sha256:4444444444444444444444444444444444444444444444444444444444444444',
			$2, '49000000-0000-0000-0000-000000000024',
			'{"concurrency":1}', decode(repeat('76', 32), 'hex')
		)
	`, ditStageProfileID, ditWorkerProfileID); err != nil {
		t.Fatalf("seed DiT StageProfile: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_profile_stage_options (
			execution_profile_revision_id, execution_graph_revision_id, stage_key,
			stage_definition_revision_id, stage_profile_revision_id,
			preference, eligibility_metadata
		) VALUES (
			$1, $2, 'dit', '49000000-0000-0000-0000-000000000031',
			$3, -1, '{"dedicated_model":true}'
		)
	`, graphExecutionProfileID, stageGraphID, ditStageProfileID); err != nil {
		t.Fatalf("seed DiT StageProfile option: %v", err)
	}
}

func activateH3StageGraph(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`
		UPDATE connector_revisions
		SET state = 'CERTIFIED'
		WHERE stable_id IN ('h3-conditioning-l2', 'h3-latent-l2')
	`); err != nil {
		t.Fatalf("certify H3 graph connectors: %v", err)
	}
	if _, err := database.Admin.Exec(`
		UPDATE execution_profile_revisions
		SET state = 'ACTIVE'
		WHERE id = $1
	`, graphExecutionProfileID); err != nil {
		t.Fatalf("certify H3 graph profile: %v", err)
	}
	activation := newRolePool(
		t,
		database.DSN,
		"vela_stage_catalog_activation_login",
		"vela-stage-catalog-activation-password",
	)
	var graphDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT content_digest FROM execution_graph_revisions WHERE id = $1
	`, stageGraphID).Scan(&graphDigest); err != nil {
		t.Fatalf("read H3 graph digest: %v", err)
	}
	var state string
	var order []string
	if err := activation.QueryRow(context.Background(), `
		SELECT state::text, topological_order
		FROM vela_activate_execution_graph($1, $2)
	`, stageGraphID, graphDigest).Scan(&state, &order); err != nil {
		t.Fatalf("activate H3 Stage graph: %v", err)
	}
	if state != "ACTIVE" || len(order) != 3 {
		t.Fatalf("activated H3 Stage graph state/order = %s/%v", state, order)
	}
}
