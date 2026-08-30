//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/stagescheduler"
)

func TestStageSchedulerAcquirePersistsDecisionAndAssignsExactlyOnce(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedEncoderAssignmentProfile(t, database)
	activateH3StageGraph(t, database)
	seedWorkerRegistryPlan(t, database.Admin)

	poolID := uuid.MustParse("49400000-0000-0000-0000-000000000001")
	workerID := uuid.MustParse("49400000-0000-0000-0000-000000000002")
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES ($1, 'h3-encoder-stage-scheduler', $2, 'GPU', 'INTERNAL',
			'cn-shanghai', 1024, 'ACTIVE')
	`, poolID, encoderStageProfileID); err != nil {
		t.Fatalf("seed StageScheduler CapacityPool: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
	`, workerID, encoderWorkerProfileID, poolID, workerRegistryBundleID); err != nil {
		t.Fatalf("seed StageScheduler WorkerInstance: %v", err)
	}
	evidence := workerRegistryEvidenceValue(t, workerID, 0xc1)
	evidence.Residencies[0].ModelComponentRevision = "h3-encoder-v1"
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	))
	if err != nil {
		t.Fatalf("construct StageScheduler Worker Registry: %v", err)
	}
	if _, err := registry.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe StageScheduler WorkerInstance: %v", err)
	}

	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "stage-scheduler-acquire", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"schedule encoder independently"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit StageScheduler Job status = %d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode StageScheduler Job: %v", err)
	}
	coordinator, err := attemptcoordinator.NewService(newRolePool(
		t,
		database.DSN,
		"vela_attempt_coordinator_login",
		"vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct StageScheduler AttemptCoordinator: %v", err)
	}
	attemptID := uuid.MustParse("49400000-0000-0000-0000-000000000003")
	if _, err := coordinator.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
		CommandID:                  uuid.MustParse("49400000-0000-0000-0000-000000000004"),
		JobID:                      uuid.MustParse(job.JobID),
		ExpectedJobVersion:         1,
		ExpectedJobFence:           0,
		ExecutionGraphSnapshotID:   uuid.MustParse("49400000-0000-0000-0000-000000000005"),
		ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
		ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
		AttemptID:                  attemptID,
		StorageReservationID:       uuid.MustParse("49400000-0000-0000-0000-000000000006"),
		ReservedStorageBytes:       2 << 30,
	}); err != nil {
		t.Fatalf("instantiate StageScheduler graph: %v", err)
	}
	var functionOwner string
	var securityDefiner, ownerCanReadPool bool
	if err := database.Admin.QueryRow(`
		SELECT owner.rolname, function.prosecdef,
			has_table_privilege(owner.rolname, 'capacity_pools', 'SELECT')
		FROM pg_proc AS function
		JOIN pg_roles AS owner ON owner.oid = function.proowner
		WHERE function.oid = 'vela_capture_stage_scheduler_snapshot(jsonb)'::regprocedure
	`).Scan(&functionOwner, &securityDefiner, &ownerCanReadPool); err != nil {
		t.Fatalf("inspect StageScheduler database authority: %v", err)
	}
	if functionOwner != "vela_stage_scheduler_owner" || !securityDefiner || !ownerCanReadPool {
		t.Fatalf(
			"StageScheduler authority owner=%s security_definer=%t pool_read=%t",
			functionOwner,
			securityDefiner,
			ownerCanReadPool,
		)
	}
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin StageScheduler owner authority check: %v", err)
	}
	if _, err := tx.Exec(`SET LOCAL ROLE vela_stage_scheduler_owner`); err != nil {
		t.Fatalf("assume StageScheduler owner role: %v", err)
	}
	var visiblePools int
	if err := tx.QueryRow(`SELECT count(*) FROM capacity_pools`).Scan(&visiblePools); err != nil {
		t.Fatalf("StageScheduler owner cannot read CapacityPools: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback StageScheduler owner authority check: %v", err)
	}

	repository, err := stagescheduler.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_scheduler_login", "vela-stage-scheduler-password",
	))
	if err != nil {
		t.Fatalf("construct StageScheduler repository: %v", err)
	}
	scheduling, err := stagescheduler.NewService(repository, coordinator, stagescheduler.Config{
		SchedulerID:      "stage-scheduler/integration",
		ClaimTTL:         30 * time.Second,
		LeaseTTL:         time.Minute,
		LocalDeadlineTTL: 50 * time.Second,
		SigningKeyID:     "stage-authority-key-v1",
	})
	if err != nil {
		t.Fatalf("construct StageScheduler service: %v", err)
	}
	worker := workerAuthority(t, evidence)
	assignment, ok, err := scheduling.Acquire(context.Background(), stagescheduler.WorkerAuthority{
		CapacityPoolID:         poolID,
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		WorkerInstanceID:       worker.WorkerInstanceID,
		WorkerInstanceEpoch:    worker.InstanceEpoch,
		DeviceSetDigest:        worker.DeviceSetDigest,
		MembershipDigest:       worker.MembershipDigest,
		ModelResidencyID:       worker.ModelResidencyID,
		ModelRuntimeEpoch:      worker.ModelRuntimeEpoch,
		CapacityVector:         evidence.Capacity.Vector,
	}, stagescheduler.CapacityObservation{Sequence: evidence.Capacity.Sequence})
	if err != nil || !ok {
		var postgresError *pgconn.PgError
		if errors.As(err, &postgresError) {
			t.Fatalf(
				"Acquire StageScheduler assignment ok=%t error=%v table=%s where=%s routine=%s",
				ok,
				err,
				postgresError.TableName,
				postgresError.Where,
				postgresError.Routine,
			)
		}
		t.Fatalf("Acquire StageScheduler assignment ok=%t error=%v", ok, err)
	}

	var stageState, claimState string
	var attempts, decisions, queueDepth, readyCount, claimedCount, activeCount int
	if err := database.Admin.QueryRow(`
		SELECT
			run.state::text,
			claim.state::text,
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = run.id),
			(SELECT count(*) FROM stage_decision_evidence WHERE stage_run_id = run.id),
			(SELECT count(*) FROM stage_ready_queue_entries WHERE stage_run_id = run.id),
			counter.ready_count,
			counter.claimed_count,
			counter.active_allocation_count
		FROM stage_runs AS run
		JOIN stage_scheduler_claims AS claim ON claim.stage_run_id = run.id
		JOIN stage_capacity_pool_counters AS counter ON counter.capacity_pool_id = claim.capacity_pool_id
		WHERE run.attempt_id = $1 AND run.stage_key = 'encoder'
	`, attemptID).Scan(
		&stageState,
		&claimState,
		&attempts,
		&decisions,
		&queueDepth,
		&readyCount,
		&claimedCount,
		&activeCount,
	); err != nil {
		t.Fatalf("read StageScheduler durable result: %v", err)
	}
	if stageState != "ASSIGNED" || claimState != "COMMITTED" || attempts != 1 ||
		decisions != 1 || queueDepth != 0 || readyCount != 0 || claimedCount != 0 || activeCount != 1 {
		t.Fatalf(
			"StageScheduler result state=%s claim=%s attempts=%d decisions=%d queue=%d counters=(%d,%d,%d)",
			stageState,
			claimState,
			attempts,
			decisions,
			queueDepth,
			readyCount,
			claimedCount,
			activeCount,
		)
	}
	if assignment.StageRunID == uuid.Nil || assignment.StageAttemptID == uuid.Nil ||
		assignment.StageLeaseID == uuid.Nil {
		t.Fatalf("StageScheduler Assignment = %#v", assignment)
	}

	second, ok, err := scheduling.Acquire(context.Background(), stagescheduler.WorkerAuthority{
		CapacityPoolID:         poolID,
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
		WorkerInstanceID:       worker.WorkerInstanceID,
		WorkerInstanceEpoch:    worker.InstanceEpoch,
		DeviceSetDigest:        worker.DeviceSetDigest,
		MembershipDigest:       worker.MembershipDigest,
		ModelResidencyID:       worker.ModelResidencyID,
		ModelRuntimeEpoch:      worker.ModelRuntimeEpoch,
		CapacityVector:         evidence.Capacity.Vector,
	}, stagescheduler.CapacityObservation{Sequence: evidence.Capacity.Sequence})
	if err != nil || ok || second != (stagescheduler.Assignment{}) {
		t.Fatalf("second Acquire = %#v ok=%t error=%v, want NoWork", second, ok, err)
	}
}

func TestStageSchedulerExpiredClaimRecoversWithoutDoubleAssignment(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "claim-crash")
	crashing, err := stagescheduler.NewService(
		fixture.repository,
		panickingStageCoordinator{},
		stagescheduler.Config{
			SchedulerID:      "stage-scheduler/crash",
			ClaimTTL:         30 * time.Second,
			LeaseTTL:         time.Minute,
			LocalDeadlineTTL: 50 * time.Second,
			SigningKeyID:     "stage-authority-key-v1",
		},
	)
	if err != nil {
		t.Fatalf("construct crashing StageScheduler: %v", err)
	}
	func() {
		defer func() {
			if recovered := recover(); recovered == nil {
				t.Fatal("StageScheduler crash fixture did not panic after claim")
			}
		}()
		_, _, _ = crashing.Acquire(
			context.Background(), fixture.authority, fixture.observation,
		)
	}()

	var crashedClaimID uuid.UUID
	var crashedState string
	if err := fixture.database.Admin.QueryRow(`
		SELECT id, state::text
		FROM stage_scheduler_claims
		WHERE stage_run_id = $1
	`, fixture.stageRunID).Scan(&crashedClaimID, &crashedState); err != nil {
		t.Fatalf("read crashed StageScheduler claim: %v", err)
	}
	if crashedState != "CLAIMED" {
		t.Fatalf("crashed StageScheduler claim state = %s, want CLAIMED", crashedState)
	}
	if _, err := fixture.database.Admin.Exec(`
		UPDATE stage_scheduler_claims
		SET claimed_at = clock_timestamp() - interval '2 seconds',
			claim_expires_at = clock_timestamp() - interval '1 second',
			updated_at = clock_timestamp()
		WHERE id = $1
	`, crashedClaimID); err != nil {
		t.Fatalf("expire crashed StageScheduler claim: %v", err)
	}
	processed, err := fixture.repository.ReconcileExpired(context.Background(), 10)
	if err != nil || processed != 1 {
		t.Fatalf("ReconcileExpired() processed=%d error=%v", processed, err)
	}
	var expiredState string
	var queued, attempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT claim.state::text,
			(SELECT count(*) FROM stage_ready_queue_entries WHERE stage_run_id = $2),
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = $2)
		FROM stage_scheduler_claims AS claim
		WHERE claim.id = $1
	`, crashedClaimID, fixture.stageRunID).Scan(&expiredState, &queued, &attempts); err != nil {
		t.Fatalf("read expired StageScheduler claim result: %v", err)
	}
	if expiredState != "EXPIRED" || queued != 1 || attempts != 0 {
		t.Fatalf(
			"expired claim state=%s queue=%d attempts=%d, want EXPIRED/1/0",
			expiredState,
			queued,
			attempts,
		)
	}

	scheduling, err := stagescheduler.NewService(
		fixture.repository,
		fixture.coordinator,
		stagescheduler.Config{
			SchedulerID:      "stage-scheduler/recovered",
			ClaimTTL:         30 * time.Second,
			LeaseTTL:         time.Minute,
			LocalDeadlineTTL: 50 * time.Second,
			SigningKeyID:     "stage-authority-key-v1",
		},
	)
	if err != nil {
		t.Fatalf("construct recovered StageScheduler: %v", err)
	}
	if _, ok, err := scheduling.Acquire(
		context.Background(), fixture.authority, fixture.observation,
	); err != nil || !ok {
		t.Fatalf("recovered Acquire() ok=%t error=%v", ok, err)
	}
	var committedClaims, expiredClaims, finalAttempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE state = 'COMMITTED'),
			count(*) FILTER (WHERE state = 'EXPIRED'),
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1)
		FROM stage_scheduler_claims
		WHERE stage_run_id = $1
	`, fixture.stageRunID).Scan(&committedClaims, &expiredClaims, &finalAttempts); err != nil {
		t.Fatalf("read recovered StageScheduler authority: %v", err)
	}
	if committedClaims != 1 || expiredClaims != 1 || finalAttempts != 1 {
		t.Fatalf(
			"recovered claims committed=%d expired=%d attempts=%d",
			committedClaims,
			expiredClaims,
			finalAttempts,
		)
	}
}

type stageSchedulerFixture struct {
	database    testDatabase
	repository  *stagescheduler.PostgresRepository
	coordinator *attemptcoordinator.Service
	authority   stagescheduler.WorkerAuthority
	observation stagescheduler.CapacityObservation
	stageRunID  uuid.UUID
}

func newStageSchedulerFixture(t *testing.T, suffix string) stageSchedulerFixture {
	t.Helper()
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedEncoderAssignmentProfile(t, database)
	activateH3StageGraph(t, database)
	seedWorkerRegistryPlan(t, database.Admin)
	poolID := uuid.New()
	workerID := uuid.New()
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_pools (
			id, stable_id, stage_profile_revision_id, resource_class,
			security_class, region, max_ready_queue_depth, state
		) VALUES ($1, $2, $3, 'GPU', 'INTERNAL', 'cn-shanghai', 1024, 'ACTIVE')
	`, poolID, "h3-encoder-stage-scheduler-"+suffix, encoderStageProfileID); err != nil {
		t.Fatalf("seed %s CapacityPool: %v", suffix, err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 1, 1)
	`, workerID, encoderWorkerProfileID, poolID, workerRegistryBundleID); err != nil {
		t.Fatalf("seed %s WorkerInstance: %v", suffix, err)
	}
	evidence := workerRegistryEvidenceValue(t, workerID, 0xc2)
	evidence.Residencies[0].ModelComponentRevision = "h3-encoder-v1"
	registry, err := fleet.NewService(newRolePool(
		t, database.DSN, "vela_internal_login", "vela-internal-password",
	))
	if err != nil {
		t.Fatalf("construct %s Worker Registry: %v", suffix, err)
	}
	if _, err := registry.Observe(context.Background(), evidence); err != nil {
		t.Fatalf("observe %s WorkerInstance: %v", suffix, err)
	}
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "stage-scheduler-"+suffix, []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"recover stage scheduling claim"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit %s Job status=%d body=%s", suffix, accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode %s Job: %v", suffix, err)
	}
	coordinator, err := attemptcoordinator.NewService(newRolePool(
		t,
		database.DSN,
		"vela_attempt_coordinator_login",
		"vela-attempt-coordinator-password",
	))
	if err != nil {
		t.Fatalf("construct %s AttemptCoordinator: %v", suffix, err)
	}
	attemptID := uuid.New()
	if _, err := coordinator.Instantiate(context.Background(), attemptcoordinator.InstantiateCommand{
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
	}); err != nil {
		t.Fatalf("instantiate %s graph: %v", suffix, err)
	}
	var stageRunID uuid.UUID
	if err := database.Admin.QueryRow(`
		SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'
	`, attemptID).Scan(&stageRunID); err != nil {
		t.Fatalf("read %s Encoder StageRun: %v", suffix, err)
	}
	repository, err := stagescheduler.NewPostgresRepository(newRolePool(
		t, database.DSN, "vela_stage_scheduler_login", "vela-stage-scheduler-password",
	))
	if err != nil {
		t.Fatalf("construct %s StageScheduler repository: %v", suffix, err)
	}
	worker := workerAuthority(t, evidence)
	return stageSchedulerFixture{
		database:    database,
		repository:  repository,
		coordinator: coordinator,
		authority: stagescheduler.WorkerAuthority{
			CapacityPoolID:         poolID,
			StageProfileRevisionID: uuid.MustParse(encoderStageProfileID),
			WorkerInstanceID:       worker.WorkerInstanceID,
			WorkerInstanceEpoch:    worker.InstanceEpoch,
			DeviceSetDigest:        worker.DeviceSetDigest,
			MembershipDigest:       worker.MembershipDigest,
			ModelResidencyID:       worker.ModelResidencyID,
			ModelRuntimeEpoch:      worker.ModelRuntimeEpoch,
			CapacityVector:         evidence.Capacity.Vector,
		},
		observation: stagescheduler.CapacityObservation{Sequence: evidence.Capacity.Sequence},
		stageRunID:  stageRunID,
	}
}

type panickingStageCoordinator struct{}

func (panickingStageCoordinator) Apply(
	context.Context,
	attemptcoordinator.StageCommand,
) (attemptcoordinator.StageDecision, error) {
	panic("simulated StageScheduler process crash")
}
