//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	veladb "github.com/vivym/vela/internal/database"
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

func TestStageSchedulerShadowReplayPersistsMatchedReceipt(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "shadow-replay")
	scheduling, err := stagescheduler.NewService(
		fixture.repository,
		fixture.coordinator,
		stagescheduler.Config{
			SchedulerID:      "stage-scheduler/shadow-replay",
			ClaimTTL:         30 * time.Second,
			LeaseTTL:         time.Minute,
			LocalDeadlineTTL: 50 * time.Second,
			SigningKeyID:     "stage-authority-key-v1",
		},
	)
	if err != nil {
		t.Fatalf("construct shadow StageScheduler: %v", err)
	}
	if _, ok, err := scheduling.Acquire(
		context.Background(), fixture.authority, fixture.observation,
	); err != nil || !ok {
		t.Fatalf("Acquire shadow fixture ok=%t error=%v", ok, err)
	}

	summary, err := scheduling.ReplayShadow(context.Background(), 10)
	if err != nil {
		t.Fatalf("ReplayShadow: %v", err)
	}
	if summary.Processed != 1 || summary.Matched != 1 || summary.Diverged != 0 {
		t.Fatalf("ReplayShadow summary = %#v", summary)
	}
	var receiptID, snapshotID uuid.UUID
	var algorithmRevision, replayedBy string
	var replayedAt time.Time
	var matched bool
	var receiptCount int
	var expectedDigest, replayedDigest []byte
	if err := fixture.database.Admin.QueryRow(`
		SELECT receipt.id,
			receipt.snapshot_trace_id,
			receipt.algorithm_revision,
			receipt.expected_evidence_digest,
			receipt.replayed_evidence_digest,
			receipt.matched,
			receipt.replayed_at,
			receipt.replayed_by,
			(SELECT count(*) FROM stage_scheduler_shadow_replay_receipts)
		FROM stage_scheduler_shadow_replay_receipts AS receipt
		JOIN stage_scheduler_snapshot_traces AS trace
			ON trace.id = receipt.snapshot_trace_id
		WHERE trace.worker_instance_id = $1
	`, fixture.authority.WorkerInstanceID).Scan(
		&receiptID,
		&snapshotID,
		&algorithmRevision,
		&expectedDigest,
		&replayedDigest,
		&matched,
		&replayedAt,
		&replayedBy,
		&receiptCount,
	); err != nil {
		t.Fatalf("read StageScheduler shadow receipt: %v", err)
	}
	if !matched || !bytes.Equal(expectedDigest, replayedDigest) || receiptCount != 1 {
		t.Fatalf(
			"shadow receipt matched=%t digest_equal=%t count=%d",
			matched,
			bytes.Equal(expectedDigest, replayedDigest),
			receiptCount,
		)
	}
	var expectedDigestArray, replayedDigestArray [32]byte
	copy(expectedDigestArray[:], expectedDigest)
	copy(replayedDigestArray[:], replayedDigest)
	receipt := stagescheduler.ShadowReplayReceipt{
		ID:                     receiptID,
		SnapshotID:             snapshotID,
		AlgorithmRevision:      algorithmRevision,
		ExpectedEvidenceDigest: expectedDigestArray,
		ReplayedEvidenceDigest: replayedDigestArray,
		Matched:                matched,
		ReplayedAt:             replayedAt,
		ReplayedBy:             replayedBy,
	}
	if err := fixture.repository.RecordShadowReplay(context.Background(), receipt); err != nil {
		t.Fatalf("replay identical StageScheduler shadow receipt: %v", err)
	}
	receipt.ReplayedEvidenceDigest[0] ^= 0xff
	err = fixture.repository.RecordShadowReplay(context.Background(), receipt)
	assertPostgresConstraint(t, err, "stage_scheduler_shadow_receipt_identity_reused")

	second, err := scheduling.ReplayShadow(context.Background(), 10)
	if err != nil || second != (stagescheduler.ShadowReplaySummary{}) {
		t.Fatalf("second ReplayShadow = %#v error=%v, want no pending snapshots", second, err)
	}
}

func TestStageSchedulerStaleWorkerEvidenceFailsClosed(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "stale-evidence")
	scheduling, err := stagescheduler.NewService(
		fixture.repository,
		fixture.coordinator,
		stagescheduler.Config{
			SchedulerID:      "stage-scheduler/stale-evidence",
			ClaimTTL:         30 * time.Second,
			LeaseTTL:         time.Minute,
			LocalDeadlineTTL: 50 * time.Second,
			SigningKeyID:     "stage-authority-key-v1",
		},
	)
	if err != nil {
		t.Fatalf("construct stale-evidence StageScheduler: %v", err)
	}

	tests := []struct {
		name           string
		constraintName string
		mutate         func(*stagescheduler.WorkerAuthority, *stagescheduler.CapacityObservation)
	}{
		{
			name:           "capacity observation sequence",
			constraintName: "stage_scheduler_capacity_observation_stale",
			mutate: func(_ *stagescheduler.WorkerAuthority, observation *stagescheduler.CapacityObservation) {
				observation.Sequence++
			},
		},
		{
			name:           "model runtime epoch",
			constraintName: "stage_scheduler_model_residency_stale",
			mutate: func(authority *stagescheduler.WorkerAuthority, _ *stagescheduler.CapacityObservation) {
				authority.ModelRuntimeEpoch++
			},
		},
		{
			name:           "device set digest",
			constraintName: "stage_scheduler_worker_authority_stale",
			mutate: func(authority *stagescheduler.WorkerAuthority, _ *stagescheduler.CapacityObservation) {
				authority.DeviceSetDigest = append([]byte(nil), authority.DeviceSetDigest...)
				authority.DeviceSetDigest[0] ^= 0xff
			},
		},
		{
			name:           "membership digest",
			constraintName: "stage_scheduler_worker_authority_stale",
			mutate: func(authority *stagescheduler.WorkerAuthority, _ *stagescheduler.CapacityObservation) {
				authority.MembershipDigest = append([]byte(nil), authority.MembershipDigest...)
				authority.MembershipDigest[0] ^= 0xff
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authority := fixture.authority
			observation := fixture.observation
			test.mutate(&authority, &observation)

			assignment, ok, err := scheduling.Acquire(
				context.Background(), authority, observation,
			)
			if err == nil || ok || assignment != (stagescheduler.Assignment{}) {
				t.Fatalf("Acquire stale authority = %#v ok=%t error=%v", assignment, ok, err)
			}
			var postgresError *pgconn.PgError
			if !errors.As(err, &postgresError) ||
				postgresError.ConstraintName != test.constraintName {
				t.Fatalf(
					"Acquire stale authority error=%v constraint=%q, want %q",
					err,
					postgresErrorConstraint(postgresError),
					test.constraintName,
				)
			}
			var snapshots, claims, attempts int
			if err := fixture.database.Admin.QueryRow(`
				SELECT
					(SELECT count(*) FROM stage_scheduler_snapshot_traces
					 WHERE worker_instance_id = $1),
					(SELECT count(*) FROM stage_scheduler_claims
					 WHERE stage_run_id = $2),
					(SELECT count(*) FROM stage_attempts
					 WHERE stage_run_id = $2)
			`, fixture.authority.WorkerInstanceID, fixture.stageRunID).Scan(
				&snapshots, &claims, &attempts,
			); err != nil {
				t.Fatalf("read stale-evidence side effects: %v", err)
			}
			if snapshots != 0 || claims != 0 || attempts != 0 {
				t.Fatalf(
					"stale-evidence side effects snapshots=%d claims=%d attempts=%d",
					snapshots,
					claims,
					attempts,
				)
			}
		})
	}
}

func TestStageSchedulerReadyQueueEnforcesCapacityPoolDepth(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "queue-depth")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE capacity_pools
		SET max_ready_queue_depth = 1
		WHERE id = $1
	`, fixture.authority.CapacityPoolID); err != nil {
		t.Fatalf("set StageScheduler CapacityPool queue depth: %v", err)
	}

	server := admissionServerForDatabase(t, fixture.database)
	accepted := submitJob(t, server.URL, "stage-scheduler-queue-depth-overflow", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"overflow the bounded stage queue"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit queue-depth Job status=%d body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode queue-depth Job: %v", err)
	}
	_, err := fixture.coordinator.Instantiate(
		context.Background(),
		attemptcoordinator.InstantiateCommand{
			CommandID:                  uuid.New(),
			JobID:                      uuid.MustParse(job.JobID),
			ExpectedJobVersion:         1,
			ExpectedJobFence:           0,
			ExecutionGraphSnapshotID:   uuid.New(),
			ExecutionGraphRevisionID:   uuid.MustParse(stageGraphID),
			ExecutionProfileRevisionID: uuid.MustParse(graphExecutionProfileID),
			AttemptID:                  uuid.New(),
			StorageReservationID:       uuid.New(),
			ReservedStorageBytes:       2 << 30,
		},
	)
	var postgresError *pgconn.PgError
	if !errors.As(err, &postgresError) ||
		postgresError.ConstraintName != "stage_ready_queue_depth_exceeded" {
		t.Fatalf(
			"Instantiate overflowing Stage queue error=%v constraint=%q",
			err,
			postgresErrorConstraint(postgresError),
		)
	}

	var readyCount, counterCount int
	if err := fixture.database.Admin.QueryRow(`
		SELECT count(*), counter.ready_count
		FROM stage_ready_queue_entries AS ready
		JOIN stage_capacity_pool_counters AS counter
			ON counter.capacity_pool_id = ready.capacity_pool_id
		WHERE ready.capacity_pool_id = $1
		GROUP BY counter.ready_count
	`, fixture.authority.CapacityPoolID).Scan(&readyCount, &counterCount); err != nil {
		t.Fatalf("read bounded Stage queue: %v", err)
	}
	if readyCount != 1 || counterCount != 1 {
		t.Fatalf("bounded Stage queue rows/counter = %d/%d, want 1/1", readyCount, counterCount)
	}
}

func TestStageSchedulerClaimRejectsForgedFairnessIdentity(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "forged-fairness")
	forgedServiceClassID := uuid.New()
	if _, err := fixture.database.Admin.Exec(`
		INSERT INTO service_class_revisions (
			id, stable_id, revision, state, queue_retry_allowance_seconds,
			max_attempts, max_total_compute_multiplier_milli,
			max_finalization_seconds_per_attempt, retry_backoff_policy,
			retryable_failure_classes, circuit_breaker_policy, queue_weight,
			max_queue_wait_before_protection_seconds, max_aging_credit_seconds
		)
		SELECT $1, 'forged-stage-scheduler-class', 1, state,
			queue_retry_allowance_seconds, max_attempts,
			max_total_compute_multiplier_milli, max_finalization_seconds_per_attempt,
			retry_backoff_policy, retryable_failure_classes,
			circuit_breaker_policy, queue_weight,
			max_queue_wait_before_protection_seconds, max_aging_credit_seconds
		FROM service_class_revisions
		WHERE stable_id = 'standard' AND revision = 1
	`, forgedServiceClassID); err != nil {
		t.Fatalf("seed forged ServiceClassRevision: %v", err)
	}
	repository := &tamperingStageRepository{
		PostgresRepository: fixture.repository,
		tamper: func(request *stagescheduler.ClaimRequest) {
			request.Evidence.ServiceClassRevisionID = forgedServiceClassID
		},
	}
	scheduling, err := stagescheduler.NewService(
		repository,
		fixture.coordinator,
		stagescheduler.Config{
			SchedulerID:      "stage-scheduler/forged-fairness",
			ClaimTTL:         30 * time.Second,
			LeaseTTL:         time.Minute,
			LocalDeadlineTTL: 50 * time.Second,
			SigningKeyID:     "stage-authority-key-v1",
		},
	)
	if err != nil {
		t.Fatalf("construct forged-fairness StageScheduler: %v", err)
	}

	assignment, ok, err := scheduling.Acquire(
		context.Background(), fixture.authority, fixture.observation,
	)
	var postgresError *pgconn.PgError
	if err == nil || ok || assignment != (stagescheduler.Assignment{}) ||
		!errors.As(err, &postgresError) ||
		postgresError.ConstraintName != "stage_scheduler_candidate_identity_stale" {
		t.Fatalf(
			"Acquire forged fairness identity = %#v ok=%t error=%v constraint=%q",
			assignment,
			ok,
			err,
			postgresErrorConstraint(postgresError),
		)
	}
	var decisions, claims, attempts int
	if err := fixture.database.Admin.QueryRow(`
		SELECT
			(SELECT count(*) FROM stage_decision_evidence WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_scheduler_claims WHERE stage_run_id = $1),
			(SELECT count(*) FROM stage_attempts WHERE stage_run_id = $1)
	`, fixture.stageRunID).Scan(&decisions, &claims, &attempts); err != nil {
		t.Fatalf("read forged-fairness side effects: %v", err)
	}
	if decisions != 0 || claims != 0 || attempts != 0 {
		t.Fatalf(
			"forged-fairness side effects decisions=%d claims=%d attempts=%d",
			decisions,
			claims,
			attempts,
		)
	}
}

func TestStageSchedulerMigrationRoundTripAndDurableEvidenceRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty Down Up restores exact role surface", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 36); err != nil {
			t.Fatalf("migrate empty StageScheduler down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 36 {
			t.Fatalf("StageScheduler version after Down = %d error=%v", version, err)
		}
		var stageRunsRemain, queueRemoved, captureRemoved bool
		if err := database.Admin.QueryRow(`
			SELECT
				to_regclass('stage_runs') IS NOT NULL,
				to_regclass('stage_ready_queue_entries') IS NULL,
				to_regprocedure('vela_capture_stage_scheduler_snapshot(jsonb)') IS NULL
		`).Scan(&stageRunsRemain, &queueRemoved, &captureRemoved); err != nil {
			t.Fatalf("inspect schema 36 StageScheduler rollback surface: %v", err)
		}
		if !stageRunsRemain || !queueRemoved || !captureRemoved {
			t.Fatalf(
				"schema 36 StageScheduler surface stageRuns/queueRemoved/captureRemoved = %t/%t/%t",
				stageRunsRemain,
				queueRemoved,
				captureRemoved,
			)
		}
		if err := goose.UpTo(database.Admin, migrations, 37); err != nil {
			t.Fatalf("migrate StageScheduler up again: %v", err)
		}
		version, err = goose.GetDBVersion(database.Admin)
		if err != nil || version != 37 {
			t.Fatalf("StageScheduler version after Down Up = %d error=%v", version, err)
		}
		stageSchedulerPool := newRolePool(
			t,
			database.DSN,
			"vela_stage_scheduler_login",
			"vela-stage-scheduler-password",
		)
		if err := veladb.VerifyRole(
			context.Background(), stageSchedulerPool, veladb.RoleStageScheduler,
		); err != nil {
			t.Fatalf("verify StageScheduler role after Down Up: %v", err)
		}
	})

	t.Run("durable evidence refuses Down", func(t *testing.T) {
		fixture := newStageSchedulerFixture(t, "migration-refusal")
		scheduling, err := stagescheduler.NewService(
			fixture.repository,
			fixture.coordinator,
			stagescheduler.Config{
				SchedulerID:      "stage-scheduler/migration-refusal",
				ClaimTTL:         30 * time.Second,
				LeaseTTL:         time.Minute,
				LocalDeadlineTTL: 50 * time.Second,
				SigningKeyID:     "stage-authority-key-v1",
			},
		)
		if err != nil {
			t.Fatalf("construct migration-refusal StageScheduler: %v", err)
		}
		if _, ok, err := scheduling.Acquire(
			context.Background(), fixture.authority, fixture.observation,
		); err != nil || !ok {
			t.Fatalf("Acquire migration-refusal fixture ok=%t error=%v", ok, err)
		}

		err = goose.DownTo(fixture.database.Admin, migrations, 36)
		assertPostgresConstraint(t, err, "stage_scheduler_rollback_is_unsafe")
		version, versionErr := goose.GetDBVersion(fixture.database.Admin)
		if versionErr != nil || version != 37 {
			t.Fatalf(
				"StageScheduler version after refused Down = %d error=%v",
				version,
				versionErr,
			)
		}
		var snapshots, decisions, claims int
		if err := fixture.database.Admin.QueryRow(`
			SELECT
				(SELECT count(*) FROM stage_scheduler_snapshot_traces),
				(SELECT count(*) FROM stage_decision_evidence),
				(SELECT count(*) FROM stage_scheduler_claims)
		`).Scan(&snapshots, &decisions, &claims); err != nil {
			t.Fatalf("read StageScheduler evidence after refused Down: %v", err)
		}
		if snapshots != 1 || decisions != 1 || claims != 1 {
			t.Fatalf(
				"StageScheduler evidence after refused Down = %d/%d/%d",
				snapshots,
				decisions,
				claims,
			)
		}
	})
}

func postgresErrorConstraint(postgresError *pgconn.PgError) string {
	if postgresError == nil {
		return ""
	}
	return postgresError.ConstraintName
}

type stageSchedulerFixture struct {
	database    testDatabase
	repository  *stagescheduler.PostgresRepository
	coordinator *attemptcoordinator.Service
	authority   stagescheduler.WorkerAuthority
	observation stagescheduler.CapacityObservation
	stageRunID  uuid.UUID
}

type tamperingStageRepository struct {
	*stagescheduler.PostgresRepository
	tamper func(*stagescheduler.ClaimRequest)
}

func (repository *tamperingStageRepository) Claim(
	ctx context.Context,
	request stagescheduler.ClaimRequest,
) (stagescheduler.ClaimResult, error) {
	if repository.tamper != nil {
		repository.tamper(&request)
	}
	return repository.PostgresRepository.Claim(ctx, request)
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
