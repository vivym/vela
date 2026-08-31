//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageWorkerControlStartRenewsAuthorityAtomicallyAndReplaysExactly(t *testing.T) {
	database, _, _, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-control-start")
	coordinatorPool := newRolePool(
		t, database.DSN,
		"vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	current := signedAssignedStageAuthority(t, database, job, assignment, 2)
	renewedEnvelope := proto.Clone(current.Authority).(*velav1.StageAuthority)
	renewedEnvelope.StageVersion = 3
	renewedEnvelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(10 * time.Millisecond))
	renewedEnvelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(time.Minute))
	renewedEnvelope.MonotonicValidFor = durationpb.New(90 * time.Second)
	renewedEnvelope.Signature = nil
	renewed := signAndVerifyStageAuthority(
		t, renewedEnvelope, assignment.IssuedAt.Add(11*time.Millisecond),
	)
	renewedWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(renewed.Authority)
	if err != nil {
		t.Fatalf("marshal renewed StageAuthority: %v", err)
	}
	commandID := uuid.New()
	startedAt := assignment.IssuedAt.Add(5 * time.Millisecond)
	payload := stageWorkerStartPayload(
		t, commandID, assignment, current, renewed, renewedWire, startedAt,
	)
	workerPool := newRolePool(
		t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	)
	var functionOwner string
	var ownerCanReadWorkers bool
	if err := database.Admin.QueryRow(`
		SELECT pg_get_userbyid(function.proowner),
		       has_table_privilege(
		           pg_get_userbyid(function.proowner), 'worker_instances', 'SELECT'
		       )
		FROM pg_proc AS function
		WHERE function.oid = 'vela_start_stage_worker_command(jsonb)'::regprocedure
	`).Scan(&functionOwner, &ownerCanReadWorkers); err != nil {
		t.Fatalf("inspect Stage Worker Start ownership: %v", err)
	}
	if functionOwner != "vela_attempt_coordinator_owner" || !ownerCanReadWorkers {
		t.Fatalf(
			"Stage Worker Start owner = %s worker-read=%t",
			functionOwner, ownerCanReadWorkers,
		)
	}

	var returnedWire []byte
	var stageVersion int64
	var replayed bool
	if err := workerPool.QueryRow(context.Background(), `
		SELECT renewed_authority, stage_version, replayed
		FROM vela_start_stage_worker_command($1::jsonb)
	`, payload).Scan(&returnedWire, &stageVersion, &replayed); err != nil {
		t.Fatalf("start Stage Worker command: %v", err)
	}
	if stageVersion != 3 || replayed || !bytes.Equal(returnedWire, renewedWire) {
		t.Fatalf(
			"start result = version %d replayed %t renewed=%t",
			stageVersion, replayed, bytes.Equal(returnedWire, renewedWire),
		)
	}
	var runState, physicalState string
	var durableVersion, renewalCount int64
	if err := database.Admin.QueryRow(`
		SELECT run.state::text, run.version, physical.state::text,
		       (SELECT count(*) FROM stage_authority_renewals
		        WHERE stage_lease_id = $1)
		FROM stage_runs AS run
		JOIN stage_attempts AS physical ON physical.stage_run_id = run.id
		WHERE run.id = $2 AND physical.id = $3
	`, assignment.StageLeaseID, assignment.StageRunID, assignment.StageAttemptID).Scan(
		&runState, &durableVersion, &physicalState, &renewalCount,
	); err != nil {
		t.Fatalf("read durable Stage Worker start: %v", err)
	}
	if runState != "RUNNING" || physicalState != "RUNNING" ||
		durableVersion != 3 || renewalCount != 1 {
		t.Fatalf(
			"durable start = run %s/%d physical %s renewals %d",
			runState, durableVersion, physicalState, renewalCount,
		)
	}

	returnedWire = nil
	if err := workerPool.QueryRow(context.Background(), `
		SELECT renewed_authority, stage_version, replayed
		FROM vela_start_stage_worker_command($1::jsonb)
	`, payload).Scan(&returnedWire, &stageVersion, &replayed); err != nil {
		t.Fatalf("replay Stage Worker start: %v", err)
	}
	if stageVersion != 3 || !replayed || !bytes.Equal(returnedWire, renewedWire) {
		t.Fatalf(
			"start replay = version %d replayed %t renewed=%t",
			stageVersion, replayed, bytes.Equal(returnedWire, renewedWire),
		)
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("decode Start payload: %v", err)
	}
	decoded["started_at"] = startedAt.Add(time.Millisecond).Format(time.RFC3339Nano)
	mismatched, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("encode mismatched Start payload: %v", err)
	}
	_, err = workerPool.Exec(context.Background(), `
		SELECT * FROM vela_start_stage_worker_command($1::jsonb)
	`, mismatched)
	assertPostgresConstraint(t, err, "stage_worker_command_replay_mismatch")

	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(workerPool)
	if err != nil {
		t.Fatalf("NewPostgresAuthorizer: %v", err)
	}
	identity := stageworkertransport.Identity{
		SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String(),
	}
	active, err := authorizer.IsActive(
		context.Background(), identity, 1,
		stageworkercontrol.OperationHeartbeatStage, current,
	)
	if err != nil || active {
		t.Fatalf("old StageAuthority active=%t error=%v", active, err)
	}
	active, err = authorizer.IsActive(
		context.Background(), identity, 1,
		stageworkercontrol.OperationHeartbeatStage, renewed,
	)
	if err != nil || !active {
		t.Fatalf("renewed StageAuthority active=%t error=%v", active, err)
	}
}

func TestStageWorkerControlHeartbeatRenewsAuthorityAndFencesPreviousEnvelope(t *testing.T) {
	database, _, _, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-control-heartbeat")
	coordinatorPool := newRolePool(
		t, database.DSN,
		"vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	assigned := signedAssignedStageAuthority(t, database, job, assignment, 2)
	startedEnvelope := proto.Clone(assigned.Authority).(*velav1.StageAuthority)
	startedEnvelope.StageVersion = 3
	startedEnvelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(10 * time.Millisecond))
	startedEnvelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(time.Minute))
	startedEnvelope.MonotonicValidFor = durationpb.New(90 * time.Second)
	startedEnvelope.Signature = nil
	started := signAndVerifyStageAuthority(
		t, startedEnvelope, assignment.IssuedAt.Add(11*time.Millisecond),
	)
	startedWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(started.Authority)
	if err != nil {
		t.Fatalf("marshal Start renewal: %v", err)
	}
	workerPool := newRolePool(
		t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	)
	startPayload := stageWorkerStartPayload(
		t, uuid.New(), assignment, assigned, started, startedWire,
		assignment.IssuedAt.Add(5*time.Millisecond),
	)
	if _, err := workerPool.Exec(context.Background(), `
		SELECT * FROM vela_start_stage_worker_command($1::jsonb)
	`, startPayload); err != nil {
		t.Fatalf("start Stage Worker before Heartbeat: %v", err)
	}

	heartbeatEnvelope := proto.Clone(started.Authority).(*velav1.StageAuthority)
	heartbeatEnvelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(30 * time.Millisecond))
	heartbeatEnvelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(2 * time.Minute))
	heartbeatEnvelope.MonotonicValidFor = durationpb.New(90 * time.Second)
	heartbeatEnvelope.Signature = nil
	heartbeat := signAndVerifyStageAuthority(
		t, heartbeatEnvelope, assignment.IssuedAt.Add(31*time.Millisecond),
	)
	heartbeatWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(heartbeat.Authority)
	if err != nil {
		t.Fatalf("marshal Heartbeat renewal: %v", err)
	}
	heartbeatCommandID := uuid.New()
	heartbeatPayload := stageWorkerHeartbeatPayload(
		t, heartbeatCommandID, assignment, started, heartbeat, heartbeatWire,
		1, assignment.IssuedAt.Add(20*time.Millisecond),
	)

	var returnedWire []byte
	var stageVersion int64
	var replayed bool
	if err := workerPool.QueryRow(context.Background(), `
		SELECT renewed_authority, stage_version, replayed
		FROM vela_heartbeat_stage_worker_command($1::jsonb)
	`, heartbeatPayload).Scan(&returnedWire, &stageVersion, &replayed); err != nil {
		t.Fatalf("heartbeat Stage Worker command: %v", err)
	}
	if stageVersion != 3 || replayed || !bytes.Equal(returnedWire, heartbeatWire) {
		t.Fatalf(
			"Heartbeat result = version %d replayed %t renewed=%t",
			stageVersion, replayed, bytes.Equal(returnedWire, heartbeatWire),
		)
	}
	var heartbeatCount, renewalCount int64
	var sequence int64
	var runtimeState string
	if err := database.Admin.QueryRow(`
		SELECT count(*), min(sequence), min(runtime_state),
		       (SELECT count(*) FROM stage_authority_renewals
		        WHERE stage_lease_id = $1)
		FROM stage_worker_heartbeats
		WHERE stage_lease_id = $1
	`, assignment.StageLeaseID).Scan(
		&heartbeatCount, &sequence, &runtimeState, &renewalCount,
	); err != nil {
		t.Fatalf("read durable Heartbeat evidence: %v", err)
	}
	if heartbeatCount != 1 || sequence != 1 || runtimeState != "RUNNING" || renewalCount != 2 {
		t.Fatalf(
			"durable Heartbeat = count %d sequence %d state %s renewals %d",
			heartbeatCount, sequence, runtimeState, renewalCount,
		)
	}

	returnedWire = nil
	if err := workerPool.QueryRow(context.Background(), `
		SELECT renewed_authority, stage_version, replayed
		FROM vela_heartbeat_stage_worker_command($1::jsonb)
	`, heartbeatPayload).Scan(&returnedWire, &stageVersion, &replayed); err != nil {
		t.Fatalf("replay Stage Worker Heartbeat: %v", err)
	}
	if stageVersion != 3 || !replayed || !bytes.Equal(returnedWire, heartbeatWire) {
		t.Fatalf(
			"Heartbeat replay = version %d replayed %t renewed=%t",
			stageVersion, replayed, bytes.Equal(returnedWire, heartbeatWire),
		)
	}

	var decoded map[string]any
	if err := json.Unmarshal(heartbeatPayload, &decoded); err != nil {
		t.Fatalf("decode Heartbeat payload: %v", err)
	}
	decoded["runtime_state"] = "OUTPUT_READY"
	mismatched, err := json.Marshal(decoded)
	if err != nil {
		t.Fatalf("encode mismatched Heartbeat payload: %v", err)
	}
	_, err = workerPool.Exec(context.Background(), `
		SELECT * FROM vela_heartbeat_stage_worker_command($1::jsonb)
	`, mismatched)
	assertPostgresConstraint(t, err, "stage_worker_command_replay_mismatch")

	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(workerPool)
	if err != nil {
		t.Fatalf("NewPostgresAuthorizer: %v", err)
	}
	identity := stageworkertransport.Identity{
		SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String(),
	}
	active, err := authorizer.IsActive(
		context.Background(), identity, 1,
		stageworkercontrol.OperationHeartbeatStage, started,
	)
	if err != nil || active {
		t.Fatalf("superseded StageAuthority active=%t error=%v", active, err)
	}
	active, err = authorizer.IsActive(
		context.Background(), identity, 1,
		stageworkercontrol.OperationHeartbeatStage, heartbeat,
	)
	if err != nil || !active {
		t.Fatalf("latest StageAuthority active=%t error=%v", active, err)
	}
}

func TestStageWorkerExecutionMigrationRoundTripAndDurableEvidenceRefusal(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty operations Down Up restores exact role surface", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 40); err != nil {
			t.Fatalf("migrate empty Stage Worker operations down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 40 {
			t.Fatalf("Stage Worker operations version after Down = %d error=%v", version, err)
		}
		var reattach, readClaim, verifyRegistration, verifyCapacity, reattachments bool
		if err := database.Admin.QueryRow(`
			SELECT
				to_regprocedure('vela_reattach_stage_worker_command(jsonb)') IS NOT NULL,
				to_regprocedure('vela_read_stage_scheduler_claim(uuid)') IS NOT NULL,
				to_regprocedure('vela_verify_stage_worker_registration(jsonb)') IS NOT NULL,
				to_regprocedure('vela_verify_stage_capacity_observation(jsonb)') IS NOT NULL,
				to_regclass('stage_worker_reattachments') IS NOT NULL
		`).Scan(
			&reattach, &readClaim, &verifyRegistration, &verifyCapacity, &reattachments,
		); err != nil {
			t.Fatalf("inspect schema 40 Stage Worker operations surface: %v", err)
		}
		if reattach || readClaim || verifyRegistration || verifyCapacity || reattachments {
			t.Fatalf(
				"schema 40 Stage Worker operations surface = %t/%t/%t/%t/%t",
				reattach, readClaim, verifyRegistration, verifyCapacity, reattachments,
			)
		}
		if err := goose.UpTo(database.Admin, migrations, 41); err != nil {
			t.Fatalf("migrate Stage Worker operations up again: %v", err)
		}
		version, err = goose.GetDBVersion(database.Admin)
		if err != nil || version != 41 {
			t.Fatalf("Stage Worker operations version after Down Up = %d error=%v", version, err)
		}
		var schedulerCanRecover, workerCanReattach, workerCanRegister, workerCanReport bool
		if err := database.Admin.QueryRow(`
			SELECT
				has_function_privilege(
					'vela_stage_scheduler', 'vela_read_stage_scheduler_claim(uuid)', 'EXECUTE'
				),
				has_function_privilege(
					'vela_stage_worker_control',
					'vela_reattach_stage_worker_command(jsonb)', 'EXECUTE'
				),
				has_function_privilege(
					'vela_stage_worker_control',
					'vela_verify_stage_worker_registration(jsonb)', 'EXECUTE'
				),
				has_function_privilege(
					'vela_stage_worker_control',
					'vela_verify_stage_capacity_observation(jsonb)', 'EXECUTE'
				)
		`).Scan(
			&schedulerCanRecover, &workerCanReattach, &workerCanRegister, &workerCanReport,
		); err != nil {
			t.Fatalf("inspect Stage Worker operations grants after Down Up: %v", err)
		}
		if !schedulerCanRecover || !workerCanReattach || !workerCanRegister || !workerCanReport {
			t.Fatalf(
				"Stage Worker operations grants after Down Up = %t/%t/%t/%t",
				schedulerCanRecover, workerCanReattach, workerCanRegister, workerCanReport,
			)
		}
	})
	t.Run("empty Down Up preserves Stage authority snapshot", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundation(t, database.Admin)
		if err := goose.DownTo(database.Admin, migrations, 39); err != nil {
			t.Fatalf("migrate empty Stage Worker execution down: %v", err)
		}
		version, err := goose.GetDBVersion(database.Admin)
		if err != nil || version != 39 {
			t.Fatalf("Stage Worker execution version after Down = %d error=%v", version, err)
		}
		var snapshot, start, heartbeat, commands, renewals bool
		if err := database.Admin.QueryRow(`
			SELECT
				to_regprocedure('vela_read_stage_authority_snapshot(uuid,bigint)') IS NOT NULL,
				to_regprocedure('vela_start_stage_worker_command(jsonb)') IS NOT NULL,
				to_regprocedure('vela_heartbeat_stage_worker_command(jsonb)') IS NOT NULL,
				to_regclass('stage_worker_commands') IS NOT NULL,
				to_regclass('stage_authority_renewals') IS NOT NULL
		`).Scan(&snapshot, &start, &heartbeat, &commands, &renewals); err != nil {
			t.Fatalf("inspect schema 39 Stage Worker surface: %v", err)
		}
		if !snapshot || start || heartbeat || commands || renewals {
			t.Fatalf(
				"schema 39 Stage Worker surface = snapshot/start/heartbeat/tables %t/%t/%t/%t/%t",
				snapshot, start, heartbeat, commands, renewals,
			)
		}
		if err := goose.UpTo(database.Admin, migrations, 40); err != nil {
			t.Fatalf("migrate Stage Worker execution up again: %v", err)
		}
		version, err = goose.GetDBVersion(database.Admin)
		if err != nil || version != 40 {
			t.Fatalf("Stage Worker execution version after Down Up = %d error=%v", version, err)
		}
	})

	t.Run("durable Stage Worker evidence refuses Down", func(t *testing.T) {
		database, _, _, job, attemptID, encoderRunID, _ :=
			newStageGraphCancellationFixture(t, "stage-worker-migration-refusal")
		coordinatorPool := newRolePool(
			t, database.DSN,
			"vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
		)
		coordinator, err := attemptcoordinator.NewService(coordinatorPool)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		assignment := assignEncoder(
			t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
		)
		current := signedAssignedStageAuthority(t, database, job, assignment, 2)
		renewedEnvelope := proto.Clone(current.Authority).(*velav1.StageAuthority)
		renewedEnvelope.StageVersion = 3
		renewedEnvelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(10 * time.Millisecond))
		renewedEnvelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(time.Minute))
		renewedEnvelope.MonotonicValidFor = durationpb.New(90 * time.Second)
		renewedEnvelope.Signature = nil
		renewed := signAndVerifyStageAuthority(
			t, renewedEnvelope, assignment.IssuedAt.Add(11*time.Millisecond),
		)
		renewedWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(renewed.Authority)
		if err != nil {
			t.Fatalf("marshal migration-refusal renewal: %v", err)
		}
		payload := stageWorkerStartPayload(
			t, uuid.New(), assignment, current, renewed, renewedWire,
			assignment.IssuedAt.Add(5*time.Millisecond),
		)
		workerPool := newRolePool(
			t, database.DSN,
			"vela_stage_worker_control_login", "vela-stage-worker-control-password",
		)
		if _, err := workerPool.Exec(context.Background(), `
			SELECT * FROM vela_start_stage_worker_command($1::jsonb)
		`, payload); err != nil {
			t.Fatalf("create durable Stage Worker evidence: %v", err)
		}

		err = goose.DownTo(database.Admin, migrations, 39)
		assertPostgresConstraint(t, err, "atomic_stage_graph_admission_rollback_is_unsafe")
		version, versionErr := goose.GetDBVersion(database.Admin)
		if versionErr != nil || version != 51 {
			t.Fatalf(
				"Stage Worker execution version after refusal = %d error=%v",
				version, versionErr,
			)
		}
		var commands, renewals int64
		if err := database.Admin.QueryRow(`
			SELECT count(*), (SELECT count(*) FROM stage_authority_renewals)
			FROM stage_worker_commands
		`).Scan(&commands, &renewals); err != nil {
			t.Fatalf("read Stage Worker evidence after refusal: %v", err)
		}
		if commands != 1 || renewals != 1 {
			t.Fatalf("durable Stage Worker evidence after refusal = %d/%d", commands, renewals)
		}
	})
}

func TestPostgresExecutionBackendPersistsDeterministicRenewals(t *testing.T) {
	database, _, _, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-postgres-execution-backend")
	coordinatorPool := newRolePool(
		t, database.DSN,
		"vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	assigned := signedAssignedStageAuthority(t, database, job, assignment, 2)
	keys := map[string][]byte{
		"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32),
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	workerPool := newRolePool(
		t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	)
	backend, err := stageworkercontrol.NewPostgresExecutionBackend(
		workerPool,
		signer,
		stageworkercontrol.PostgresExecutionConfig{
			ActiveSigningKeyID: "stage-authority-key-v1",
			AuthorityTTL:       2 * time.Minute,
			LocalDeadlineTTL:   90 * time.Second,
			MaxClockSkew:       time.Second,
			Now: func() time.Time {
				return assignment.IssuedAt.Add(50 * time.Millisecond)
			},
		},
	)
	if err != nil {
		t.Fatalf("NewPostgresExecutionBackend: %v", err)
	}
	command := stageworkercontrol.CommandContext{
		CommandID: uuid.New(),
		Identity: stageworkertransport.Identity{
			SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String(),
		},
		ControlSessionEpoch: 1,
	}
	startedAt := assignment.IssuedAt.Add(5 * time.Millisecond)
	started, err := backend.StartStage(
		context.Background(), command,
		&velav1.StartStageRequest{
			Authority: assigned.Authority, StartedAt: timestamppb.New(startedAt),
		},
		stageworkercontrol.VerifiedAuthorities{Stage: &assigned},
	)
	if err != nil {
		t.Fatalf("PostgresExecutionBackend StartStage: %v", err)
	}
	if started.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED ||
		started.RenewedAuthority == nil || started.RenewedAuthority.GetStageVersion() != 3 {
		t.Fatalf("PostgresExecutionBackend Start result = %#v", started)
	}
	startReplay, err := backend.StartStage(
		context.Background(), command,
		&velav1.StartStageRequest{
			Authority: assigned.Authority, StartedAt: timestamppb.New(startedAt),
		},
		stageworkercontrol.VerifiedAuthorities{Stage: &assigned},
	)
	if err != nil {
		t.Fatalf("PostgresExecutionBackend Start replay: %v", err)
	}
	if startReplay.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED ||
		!proto.Equal(startReplay.RenewedAuthority, started.RenewedAuthority) {
		t.Fatalf("PostgresExecutionBackend Start replay result = %#v", startReplay)
	}

	validator, err := stageauthority.NewValidator(keys, func() time.Time {
		return assignment.IssuedAt.Add(15 * time.Millisecond)
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	startedVerified, err := validator.ValidateEnvelope(started.RenewedAuthority)
	if err != nil {
		t.Fatalf("validate Start renewal: %v", err)
	}
	heartbeatCommand := command
	heartbeatCommand.CommandID = uuid.New()
	observedAt := assignment.IssuedAt.Add(20 * time.Millisecond)
	heartbeatResult, err := backend.HeartbeatStage(
		context.Background(), heartbeatCommand,
		&velav1.HeartbeatStageRequest{
			Authority:         started.RenewedAuthority,
			Sequence:          1,
			RuntimeState:      velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
			BoundedStatusJson: []byte(`{"phase":"denoise","step":7}`),
			ObservedAt:        timestamppb.New(observedAt),
		},
		stageworkercontrol.VerifiedAuthorities{Stage: &startedVerified},
	)
	if err != nil {
		t.Fatalf("PostgresExecutionBackend HeartbeatStage: %v", err)
	}
	if heartbeatResult.Decision !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED ||
		heartbeatResult.RenewedAuthority == nil ||
		heartbeatResult.RenewedAuthority.GetStageVersion() != 3 ||
		!heartbeatResult.RenewedAuthority.GetIssuedAt().AsTime().After(
			started.RenewedAuthority.GetIssuedAt().AsTime(),
		) {
		t.Fatalf("PostgresExecutionBackend Heartbeat result = %#v", heartbeatResult)
	}
}

func TestPostgresReattachmentBackendPersistsReplayAndFencesSupersededAuthority(t *testing.T) {
	database, _, _, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-postgres-reattachment")
	coordinatorPool := newRolePool(
		t, database.DSN,
		"vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
	)
	coordinator, err := attemptcoordinator.NewService(coordinatorPool)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	assigned := signedAssignedStageAuthority(t, database, job, assignment, 2)
	keys := map[string][]byte{
		"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32),
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	workerPool := newRolePool(
		t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	)
	execution, err := stageworkercontrol.NewPostgresExecutionBackend(
		workerPool,
		signer,
		stageworkercontrol.PostgresExecutionConfig{
			ActiveSigningKeyID: "stage-authority-key-v1",
			AuthorityTTL:       2 * time.Minute,
			LocalDeadlineTTL:   90 * time.Second,
			MaxClockSkew:       time.Second,
			Now: func() time.Time {
				return assignment.IssuedAt.Add(100 * time.Millisecond)
			},
		},
	)
	if err != nil {
		t.Fatalf("NewPostgresExecutionBackend: %v", err)
	}
	identity := stageworkertransport.Identity{
		SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String(),
	}
	started, err := execution.StartStage(
		context.Background(),
		stageworkercontrol.CommandContext{
			CommandID: uuid.New(), Identity: identity, ControlSessionEpoch: 1,
		},
		&velav1.StartStageRequest{
			Authority: assigned.Authority,
			StartedAt: timestamppb.New(assignment.IssuedAt.Add(5 * time.Millisecond)),
		},
		stageworkercontrol.VerifiedAuthorities{Stage: &assigned},
	)
	if err != nil || started.RenewedAuthority == nil {
		t.Fatalf("start before Reattach = %#v error=%v", started, err)
	}
	startedDigest, err := stageauthority.Digest(started.RenewedAuthority)
	if err != nil {
		t.Fatalf("digest started authority: %v", err)
	}
	startedVerified := stageauthority.Verified{
		Authority: started.RenewedAuthority, Digest: startedDigest,
	}
	reattachments, err := stageworkercontrol.NewPostgresReattachmentBackend(workerPool)
	if err != nil {
		t.Fatalf("NewPostgresReattachmentBackend: %v", err)
	}
	command := stageworkercontrol.CommandContext{
		CommandID: uuid.New(), Identity: identity, ControlSessionEpoch: 1,
	}
	request := &velav1.ReattachStageRequest{
		Authority:            started.RenewedAuthority,
		ObservedRuntimeState: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
	}
	result, err := reattachments.ReattachStage(
		context.Background(), command, request,
		stageworkercontrol.VerifiedAuthorities{Stage: &startedVerified},
	)
	if err != nil || result.Decision !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("Reattach result = %#v error=%v", result, err)
	}
	replayed, err := reattachments.ReattachStage(
		context.Background(), command, request,
		stageworkercontrol.VerifiedAuthorities{Stage: &startedVerified},
	)
	if err != nil || replayed.Decision !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
		t.Fatalf("Reattach replay = %#v error=%v", replayed, err)
	}

	mismatched := proto.Clone(request).(*velav1.ReattachStageRequest)
	mismatched.ObservedRuntimeState =
		velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED
	rejected, err := reattachments.ReattachStage(
		context.Background(), command, mismatched,
		stageworkercontrol.VerifiedAuthorities{Stage: &startedVerified},
	)
	if err != nil || rejected.Decision !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED {
		t.Fatalf("mismatched Reattach replay = %#v error=%v", rejected, err)
	}

	heartbeat, err := execution.HeartbeatStage(
		context.Background(),
		stageworkercontrol.CommandContext{
			CommandID: uuid.New(), Identity: identity, ControlSessionEpoch: 1,
		},
		&velav1.HeartbeatStageRequest{
			Authority: started.RenewedAuthority, Sequence: 1,
			RuntimeState:      velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING,
			BoundedStatusJson: []byte(`{"state":"running"}`),
			ObservedAt:        timestamppb.New(assignment.IssuedAt.Add(20 * time.Millisecond)),
		},
		stageworkercontrol.VerifiedAuthorities{Stage: &startedVerified},
	)
	if err != nil || heartbeat.RenewedAuthority == nil {
		t.Fatalf("Heartbeat before stale Reattach = %#v error=%v", heartbeat, err)
	}
	stale, err := reattachments.ReattachStage(
		context.Background(),
		stageworkercontrol.CommandContext{
			CommandID: uuid.New(), Identity: identity, ControlSessionEpoch: 1,
		},
		request,
		stageworkercontrol.VerifiedAuthorities{Stage: &startedVerified},
	)
	if err != nil || stale.Decision !=
		velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
		t.Fatalf("superseded Reattach = %#v error=%v", stale, err)
	}

	var reattachmentCount int64
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM stage_worker_reattachments
	`).Scan(&reattachmentCount); err != nil {
		t.Fatalf("read durable Reattach evidence: %v", err)
	}
	if reattachmentCount != 1 {
		t.Fatalf("durable Reattach evidence count = %d, want 1", reattachmentCount)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err = goose.DownTo(database.Admin, migrations, 40)
	assertPostgresConstraint(t, err, "atomic_stage_graph_admission_rollback_is_unsafe")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 51 {
		t.Fatalf(
			"Stage Worker operations version after refusal = %d error=%v",
			version, versionErr,
		)
	}
}

func TestPostgresWorkerEvidenceBackendRequiresExactFleetAuthority(t *testing.T) {
	database, _, coordinator, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-fleet-evidence")
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	authority := signedAssignedStageAuthority(t, database, job, assignment, 2).Authority
	readinessEvidence := []byte("certified encoder readiness receipt")
	readinessDigest := sha256.Sum256(readinessEvidence)
	if _, err := database.Admin.Exec(`
		UPDATE model_residencies
		SET canary_evidence_digest = $2,
		    runtime_image_digest =
		        'sha256:3333333333333333333333333333333333333333333333333333333333333333'
		WHERE id = $1
	`, assignment.ModelResidencyID, readinessDigest[:]); err != nil {
		t.Fatalf("bind readiness evidence digest: %v", err)
	}
	var observedAt, expiresAt time.Time
	var capacityVector []byte
	if err := database.Admin.QueryRow(`
		SELECT observed_at, expires_at, capacity_vector
		FROM capacity_observations
		WHERE worker_instance_id = $1 AND observation_sequence = $2
	`, assignment.WorkerInstanceID, assignment.ObservationSequence).Scan(
		&observedAt, &expiresAt, &capacityVector,
	); err != nil {
		t.Fatalf("read durable CapacityObservation: %v", err)
	}
	var capacity map[string]int64
	if err := json.Unmarshal(capacityVector, &capacity); err != nil {
		t.Fatalf("decode durable CapacityObservation: %v", err)
	}
	workerPool := newRolePool(
		t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	)
	backend, err := stageworkercontrol.NewPostgresWorkerEvidenceBackend(workerPool)
	if err != nil {
		t.Fatalf("NewPostgresWorkerEvidenceBackend: %v", err)
	}
	identity := stageworkertransport.Identity{
		SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String(),
	}
	command := stageworkercontrol.CommandContext{
		CommandID: uuid.New(), Identity: identity, ControlSessionEpoch: 1,
	}
	registration := &velav1.RegisterWorkerEvidenceRequest{
		RuntimeIdentity: &velav1.ModelRuntimeIdentity{
			WorkerInstanceId:       authority.GetWorkerInstanceId(),
			WorkerInstanceEpoch:    authority.GetWorkerInstanceEpoch(),
			DeviceSetDigest:        authority.GetDeviceSetDigest(),
			MembershipDigest:       authority.GetMembershipDigest(),
			ModelResidencyId:       authority.GetModelResidencyId(),
			RuntimeIdentity:        authority.GetModelRuntimeIdentity(),
			ModelRuntimeEpoch:      authority.GetMembers()[0].GetModelRuntimeEpoch(),
			StageProfileRevisionId: authority.GetStageProfileRevisionId(),
			WorkerMemberId:         authority.GetMembers()[0].GetWorkerMemberId(),
			WorkerMemberEpoch:      authority.GetMembers()[0].GetMemberEpoch(),
		},
		CapacityObservationSequence: authority.GetCapacityObservationSequence(),
		Devices:                     authority.GetDevices(), Members: authority.GetMembers(),
		ReadinessEvidence: readinessEvidence,
	}
	registered, err := backend.RegisterWorkerEvidence(
		context.Background(), command, registration,
	)
	if err != nil || !registered.Ready ||
		registered.WorkerInstanceID != assignment.WorkerInstanceID ||
		registered.WorkerInstanceEpoch != assignment.WorkerInstanceEpoch {
		t.Fatalf("registered durable Worker evidence = %#v error=%v", registered, err)
	}

	wrongProfile := proto.Clone(registration).(*velav1.RegisterWorkerEvidenceRequest)
	wrongProfile.RuntimeIdentity.StageProfileRevisionId = uuid.NewString()
	rejected, err := backend.RegisterWorkerEvidence(
		context.Background(), command, wrongProfile,
	)
	if err != nil || rejected.Ready {
		t.Fatalf("forged Worker profile evidence = %#v error=%v", rejected, err)
	}

	capacityRequest := &velav1.ReportStageCapacityObservationRequest{
		WorkerInstanceId:    assignment.WorkerInstanceID.String(),
		WorkerInstanceEpoch: assignment.WorkerInstanceEpoch,
		ObservationSequence: assignment.ObservationSequence,
		CapacityVector:      capacity,
		ObservedAt:          timestamppb.New(observedAt), ExpiresAt: timestamppb.New(expiresAt),
	}
	verifiedCapacity, err := backend.ReportCapacityObservation(
		context.Background(), command, capacityRequest,
	)
	if err != nil || !verifiedCapacity.Ready {
		t.Fatalf("verified durable CapacityObservation = %#v error=%v", verifiedCapacity, err)
	}
	forgedCapacity := proto.Clone(capacityRequest).(*velav1.ReportStageCapacityObservationRequest)
	forgedCapacity.CapacityVector["concurrency"]++
	rejectedCapacity, err := backend.ReportCapacityObservation(
		context.Background(), command, forgedCapacity,
	)
	if err != nil || rejectedCapacity.Ready {
		t.Fatalf("forged CapacityObservation = %#v error=%v", rejectedCapacity, err)
	}

	var observationCount int64
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM capacity_observations WHERE worker_instance_id = $1
	`, assignment.WorkerInstanceID).Scan(&observationCount); err != nil {
		t.Fatalf("count durable CapacityObservations: %v", err)
	}
	if observationCount != 1 {
		t.Fatalf("StageWorkerControl mutated Fleet observations: count=%d", observationCount)
	}
}

func TestStageWorkerControlDurableAuthorizerRejectsStaleExecutionEvidence(t *testing.T) {
	database, _, coordinator, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-control-authorizer")
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	spiffeID := "spiffe://vela/worker/" + assignment.WorkerInstanceID.String()
	verified := signedAssignedStageAuthority(t, database, job, assignment, 2)
	workerPool := newRolePool(
		t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	)
	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(workerPool)
	if err != nil {
		t.Fatalf("NewPostgresAuthorizer: %v", err)
	}
	identity := stageworkertransport.Identity{SPIFFEID: spiffeID}

	active, err := authorizer.IsActive(
		context.Background(), identity, 1, stageworkercontrol.OperationStartStage, verified,
	)
	if err != nil || !active {
		t.Fatalf("assigned StageAuthority active=%t error=%v", active, err)
	}
	active, err = authorizer.IsActive(
		context.Background(), identity, 1,
		stageworkercontrol.OperationHeartbeatStage, verified,
	)
	if err != nil || active {
		t.Fatalf("ASSIGNED Heartbeat StageAuthority active=%t error=%v", active, err)
	}
	for _, test := range []struct {
		name   string
		mutate func(*velav1.StageAuthority)
	}{
		{
			name: "forged lease token",
			mutate: func(authority *velav1.StageAuthority) {
				authority.LeaseToken = bytes.Repeat([]byte{0xff}, 32)
			},
		},
		{
			name: "stale member epoch",
			mutate: func(authority *velav1.StageAuthority) {
				authority.Members[0].MemberEpoch++
			},
		},
		{
			name: "stale device epoch",
			mutate: func(authority *velav1.StageAuthority) {
				authority.Devices[0].DeviceEpoch++
			},
		},
		{
			name: "stale runtime epoch",
			mutate: func(authority *velav1.StageAuthority) {
				authority.Members[0].ModelRuntimeEpoch++
			},
		},
		{
			name: "stale capacity observation",
			mutate: func(authority *velav1.StageAuthority) {
				authority.CapacityObservationSequence++
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			mutatedEnvelope := proto.Clone(verified.Authority).(*velav1.StageAuthority)
			test.mutate(mutatedEnvelope)
			mutatedEnvelope.Signature = nil
			mutated := signAndVerifyStageAuthority(
				t, mutatedEnvelope, assignment.IssuedAt.Add(time.Millisecond),
			)
			active, activeErr := authorizer.IsActive(
				context.Background(), identity, 1,
				stageworkercontrol.OperationStartStage, mutated,
			)
			if activeErr != nil || active {
				t.Fatalf("stale StageAuthority active=%t error=%v", active, activeErr)
			}
		})
	}
	for _, test := range []struct {
		name         string
		identity     stageworkertransport.Identity
		sessionEpoch int64
	}{
		{name: "forged SPIFFE", identity: stageworkertransport.Identity{SPIFFEID: spiffeID + "/forged"}, sessionEpoch: 1},
		{name: "stale session", identity: identity, sessionEpoch: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			active, activeErr := authorizer.IsActive(
				context.Background(), test.identity, test.sessionEpoch,
				stageworkercontrol.OperationStartStage, verified,
			)
			if activeErr != nil || active {
				t.Fatalf("stale StageAuthority active=%t error=%v", active, activeErr)
			}
		})
	}

	renewedEnvelope := proto.Clone(verified.Authority).(*velav1.StageAuthority)
	renewedEnvelope.StageVersion = 3
	renewedEnvelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(10 * time.Millisecond))
	renewedEnvelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(time.Minute))
	renewedEnvelope.MonotonicValidFor = durationpb.New(90 * time.Second)
	renewedEnvelope.Signature = nil
	renewed := signAndVerifyStageAuthority(
		t, renewedEnvelope, assignment.IssuedAt.Add(11*time.Millisecond),
	)
	renewedWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(renewed.Authority)
	if err != nil {
		t.Fatalf("marshal durable-authorizer renewal: %v", err)
	}
	startPayload := stageWorkerStartPayload(
		t, uuid.New(), assignment, verified, renewed, renewedWire,
		assignment.IssuedAt.Add(5*time.Millisecond),
	)
	if _, err := workerPool.Exec(context.Background(), `
		SELECT * FROM vela_start_stage_worker_command($1::jsonb)
	`, startPayload); err != nil {
		t.Fatalf("start StageAttempt through StageWorkerControl: %v", err)
	}
	active, err = authorizer.IsActive(
		context.Background(), identity, 1, stageworkercontrol.OperationHeartbeatStage, verified,
	)
	if err != nil || active {
		t.Fatalf("old StageVersion active=%t error=%v", active, err)
	}

	active, err = authorizer.IsActive(
		context.Background(), identity, 1, stageworkercontrol.OperationHeartbeatStage, renewed,
	)
	if err != nil || !active {
		t.Fatalf("renewed StageAuthority active=%t error=%v", active, err)
	}
}

func stageWorkerStartPayload(
	t *testing.T,
	commandID uuid.UUID,
	assignment attemptcoordinator.AssignStageCommand,
	current, renewed stageauthority.Verified,
	renewedWire []byte,
	startedAt time.Time,
) []byte {
	t.Helper()
	leaseTokenDigest := sha256.Sum256(current.Authority.GetLeaseToken())
	currentWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(current.Authority)
	if err != nil {
		t.Fatalf("marshal current StageAuthority: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":                1,
		"command_kind":                  "START",
		"command_id":                    commandID,
		"current_authority_digest":      hex.EncodeToString(current.Digest[:]),
		"current_authority":             hex.EncodeToString(currentWire),
		"renewed_authority_digest":      hex.EncodeToString(renewed.Digest[:]),
		"renewed_authority":             hex.EncodeToString(renewedWire),
		"attempt_id":                    assignment.AttemptID,
		"stage_run_id":                  assignment.StageRunID,
		"stage_attempt_id":              assignment.StageAttemptID,
		"stage_allocation_id":           assignment.StageAllocationID,
		"stage_lease_id":                assignment.StageLeaseID,
		"expected_attempt_fence":        current.Authority.GetAttemptFence(),
		"expected_stage_fence":          current.Authority.GetStageFence(),
		"expected_stage_version":        current.Authority.GetStageVersion(),
		"renewed_stage_version":         renewed.Authority.GetStageVersion(),
		"worker_instance_id":            assignment.WorkerInstanceID,
		"worker_instance_epoch":         assignment.WorkerInstanceEpoch,
		"device_set_digest":             hex.EncodeToString(assignment.DeviceSetDigest),
		"membership_digest":             hex.EncodeToString(assignment.MembershipDigest),
		"model_residency_id":            assignment.ModelResidencyID,
		"model_runtime_epoch":           assignment.ModelRuntimeEpoch,
		"capacity_observation_sequence": assignment.ObservationSequence,
		"capacity_vector":               assignment.CapacityVector,
		"lease_token_digest":            hex.EncodeToString(leaseTokenDigest[:]),
		"execution_nonce":               hex.EncodeToString(assignment.ExecutionNonce),
		"control_session_epoch":         1,
		"started_at":                    startedAt.UTC().Format(time.RFC3339Nano),
		"renewed_signing_key_id":        renewed.Authority.GetSigningKeyId(),
		"renewed_issued_at":             renewed.Authority.GetIssuedAt().AsTime().UTC().Format(time.RFC3339Nano),
		"renewed_expires_at":            renewed.Authority.GetExpiresAt().AsTime().UTC().Format(time.RFC3339Nano),
		"renewed_local_deadline_at": renewed.Authority.GetIssuedAt().AsTime().UTC().Add(
			renewed.Authority.GetMonotonicValidFor().AsDuration(),
		).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("encode Stage Worker Start payload: %v", err)
	}
	return payload
}

func stageWorkerHeartbeatPayload(
	t *testing.T,
	commandID uuid.UUID,
	assignment attemptcoordinator.AssignStageCommand,
	current, renewed stageauthority.Verified,
	renewedWire []byte,
	sequence int64,
	observedAt time.Time,
) []byte {
	t.Helper()
	leaseTokenDigest := sha256.Sum256(current.Authority.GetLeaseToken())
	currentWire, err := proto.MarshalOptions{Deterministic: true}.Marshal(current.Authority)
	if err != nil {
		t.Fatalf("marshal current Heartbeat StageAuthority: %v", err)
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version":                1,
		"command_kind":                  "HEARTBEAT",
		"command_id":                    commandID,
		"current_authority_digest":      hex.EncodeToString(current.Digest[:]),
		"current_authority":             hex.EncodeToString(currentWire),
		"renewed_authority_digest":      hex.EncodeToString(renewed.Digest[:]),
		"renewed_authority":             hex.EncodeToString(renewedWire),
		"attempt_id":                    assignment.AttemptID,
		"stage_run_id":                  assignment.StageRunID,
		"stage_attempt_id":              assignment.StageAttemptID,
		"stage_allocation_id":           assignment.StageAllocationID,
		"stage_lease_id":                assignment.StageLeaseID,
		"expected_attempt_fence":        current.Authority.GetAttemptFence(),
		"expected_stage_fence":          current.Authority.GetStageFence(),
		"expected_stage_version":        current.Authority.GetStageVersion(),
		"renewed_stage_version":         renewed.Authority.GetStageVersion(),
		"worker_instance_id":            assignment.WorkerInstanceID,
		"worker_instance_epoch":         assignment.WorkerInstanceEpoch,
		"device_set_digest":             hex.EncodeToString(assignment.DeviceSetDigest),
		"membership_digest":             hex.EncodeToString(assignment.MembershipDigest),
		"model_residency_id":            assignment.ModelResidencyID,
		"model_runtime_epoch":           assignment.ModelRuntimeEpoch,
		"capacity_observation_sequence": assignment.ObservationSequence,
		"capacity_vector":               assignment.CapacityVector,
		"lease_token_digest":            hex.EncodeToString(leaseTokenDigest[:]),
		"execution_nonce":               hex.EncodeToString(assignment.ExecutionNonce),
		"control_session_epoch":         1,
		"sequence":                      sequence,
		"runtime_state":                 "RUNNING",
		"bounded_status":                map[string]any{"step": 7, "phase": "denoise"},
		"local_receipt_id":              nil,
		"local_receipt_digest":          nil,
		"observed_at":                   observedAt.UTC().Format(time.RFC3339Nano),
		"renewed_signing_key_id":        renewed.Authority.GetSigningKeyId(),
		"renewed_issued_at":             renewed.Authority.GetIssuedAt().AsTime().UTC().Format(time.RFC3339Nano),
		"renewed_expires_at":            renewed.Authority.GetExpiresAt().AsTime().UTC().Format(time.RFC3339Nano),
		"renewed_local_deadline_at": renewed.Authority.GetIssuedAt().AsTime().UTC().Add(
			renewed.Authority.GetMonotonicValidFor().AsDuration(),
		).Format(time.RFC3339Nano),
	})
	if err != nil {
		t.Fatalf("encode Stage Worker Heartbeat payload: %v", err)
	}
	return payload
}

func signedAssignedStageAuthority(
	t *testing.T,
	database testDatabase,
	job jobResponse,
	assignment attemptcoordinator.AssignStageCommand,
	stageVersion int64,
) stageauthority.Verified {
	t.Helper()
	var runtimeIdentity string
	var memberID, deviceID uuid.UUID
	var memberEpoch, deviceEpoch int64
	if err := database.Admin.QueryRow(`
		SELECT residency.runtime_identity, member.id, member.member_epoch,
		       device.id, device.device_epoch
		FROM model_residencies AS residency
		JOIN worker_members AS member
		  ON member.worker_instance_id = residency.worker_instance_id
		JOIN worker_member_devices AS binding
		  ON binding.worker_instance_id = member.worker_instance_id
		 AND binding.worker_member_id = member.id
		JOIN devices AS device ON device.id = binding.device_id
		WHERE residency.id = $1
	`, assignment.ModelResidencyID).Scan(
		&runtimeIdentity, &memberID, &memberEpoch, &deviceID, &deviceEpoch,
	); err != nil {
		t.Fatalf("read assigned Stage runtime evidence: %v", err)
	}
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(&velav1.StageExecutionSpec{})
	if err != nil {
		t.Fatalf("ExecutionSpecDigest: %v", err)
	}
	envelope := &velav1.StageAuthority{
		SchemaVersion: stageauthority.SchemaVersionV1,
		JobId:         uuid.MustParse(job.JobID).String(), AttemptId: assignment.AttemptID.String(),
		StageRunId: assignment.StageRunID.String(), StageAttemptId: assignment.StageAttemptID.String(),
		StageAllocationId: assignment.StageAllocationID.String(), StageLeaseId: assignment.StageLeaseID.String(),
		AttemptFence: 1, StageFence: 1, StageVersion: stageVersion,
		WorkerInstanceId:    assignment.WorkerInstanceID.String(),
		WorkerInstanceEpoch: assignment.WorkerInstanceEpoch,
		DeviceSetDigest:     append([]byte(nil), assignment.DeviceSetDigest...),
		Devices: []*velav1.StageAuthorityDeviceEpoch{{
			DeviceId: deviceID.String(), DeviceEpoch: deviceEpoch,
		}},
		MembershipDigest: append([]byte(nil), assignment.MembershipDigest...),
		Members: []*velav1.StageAuthorityMemberEpoch{{
			WorkerMemberId: memberID.String(), MemberEpoch: memberEpoch,
			ModelRuntimeEpoch: assignment.ModelRuntimeEpoch,
		}},
		ModelResidencyId:            assignment.ModelResidencyID.String(),
		ModelRuntimeIdentity:        runtimeIdentity,
		StageProfileRevisionId:      assignment.StageProfileRevisionID.String(),
		CapacityObservationSequence: assignment.ObservationSequence,
		CapacityVector:              assignment.CapacityVector,
		LeaseToken:                  bytes.Repeat([]byte{0xb3}, 32),
		ExecutionNonce:              append([]byte(nil), assignment.ExecutionNonce...),
		SigningKeyId:                assignment.SigningKeyID,
		IssuedAt:                    timestamppb.New(assignment.IssuedAt),
		ExpiresAt:                   timestamppb.New(assignment.ExpiresAt),
		MonotonicValidFor:           durationpb.New(assignment.LocalDeadlineAt.Sub(assignment.IssuedAt)),
		ExecutionSpecDigest:         executionSpecDigest[:],
	}
	return signAndVerifyStageAuthority(t, envelope, assignment.IssuedAt.Add(time.Millisecond))
}

func signAndVerifyStageAuthority(
	t *testing.T,
	envelope *velav1.StageAuthority,
	now time.Time,
) stageauthority.Verified {
	t.Helper()
	keys := map[string][]byte{
		"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32),
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	signed, err := signer.Sign(envelope)
	if err != nil {
		t.Fatalf("Sign StageAuthority: %v", err)
	}
	validator, err := stageauthority.NewValidator(keys, func() time.Time { return now })
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	verified, err := validator.ValidateEnvelope(signed)
	if err != nil {
		t.Fatalf("ValidateEnvelope: %v", err)
	}
	return verified
}
