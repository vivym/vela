//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/fleet"
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

func TestStageWorkerExecutionMigrationEmptyDownUp(t *testing.T) {
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	t.Run("empty operations Down Up restores exact role surface", func(t *testing.T) {
		database := newPostgres(t)
		applyFoundationTo(t, database.Admin, 41)
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
		applyFoundationTo(t, database.Admin, 40)
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
	for name, config := range map[string]stageworkercontrol.PostgresExecutionConfig{
		"authority TTL": {
			ActiveSigningKeyID: "stage-authority-key-v1",
			AuthorityTTL:       2*time.Minute + time.Nanosecond,
			LocalDeadlineTTL:   90 * time.Second,
		},
		"local deadline TTL": {
			ActiveSigningKeyID: "stage-authority-key-v1",
			AuthorityTTL:       2 * time.Minute,
			LocalDeadlineTTL:   90*time.Second + time.Nanosecond,
		},
	} {
		t.Run("reject sub-microsecond "+name, func(t *testing.T) {
			if _, err := stageworkercontrol.NewPostgresExecutionBackend(
				workerPool, signer, config,
			); err == nil {
				t.Fatal("NewPostgresExecutionBackend accepted a sub-microsecond duration")
			}
		})
	}
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
	startedAt := assignment.IssuedAt.Add(5*time.Millisecond + 999*time.Nanosecond)
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
	if len(started.RenewedAuthority.GetMembers()) != len(assigned.Authority.GetMembers()) ||
		!bytes.Equal(
			started.RenewedAuthority.GetMembers()[0].GetIdentityDigest(),
			assigned.Authority.GetMembers()[0].GetIdentityDigest(),
		) {
		t.Fatal("Start renewal changed the signed WorkerMember identity digest")
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
		return assignment.IssuedAt.Add(25 * time.Millisecond)
	})
	if err != nil {
		t.Fatalf("NewValidator: %v", err)
	}
	startedVerified, err := validator.ValidateEnvelope(started.RenewedAuthority)
	if err != nil {
		t.Fatalf("validate Start renewal: %v", err)
	}
	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(workerPool)
	if err != nil {
		t.Fatalf("NewPostgresAuthorizer: %v", err)
	}
	active, err := authorizer.IsActive(
		context.Background(), command.Identity, command.ControlSessionEpoch,
		stageworkercontrol.OperationHeartbeatStage, startedVerified,
	)
	if err != nil || !active {
		t.Fatalf("persisted Start renewal active=%t error=%v", active, err)
	}
	heartbeatCommand := command
	heartbeatCommand.CommandID = uuid.New()
	observedAt := assignment.IssuedAt.Add(20*time.Millisecond + 999*time.Nanosecond)
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
		!bytes.Equal(
			heartbeatResult.RenewedAuthority.GetMembers()[0].GetIdentityDigest(),
			started.RenewedAuthority.GetMembers()[0].GetIdentityDigest(),
		) ||
		!heartbeatResult.RenewedAuthority.GetIssuedAt().AsTime().After(
			started.RenewedAuthority.GetIssuedAt().AsTime(),
		) {
		t.Fatalf("PostgresExecutionBackend Heartbeat result = %#v", heartbeatResult)
	}
	heartbeatVerified, err := validator.ValidateEnvelope(heartbeatResult.RenewedAuthority)
	if err != nil {
		t.Fatalf("validate Heartbeat renewal: %v", err)
	}
	active, err = authorizer.IsActive(
		context.Background(), command.Identity, command.ControlSessionEpoch,
		stageworkercontrol.OperationHeartbeatStage, heartbeatVerified,
	)
	if err != nil || !active {
		t.Fatalf("persisted Heartbeat renewal active=%t error=%v", active, err)
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
	if len(started.RenewedAuthority.GetMembers()) != 1 ||
		len(started.RenewedAuthority.GetMembers()[0].GetIdentityDigest()) != sha256.Size {
		t.Fatal("Reattach fixture lost the signed WorkerMember identity digest")
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
}

func TestPostgresWorkerEvidenceBackendRequiresExactFleetAuthority(t *testing.T) {
	database, _, coordinator, job, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-fleet-evidence")
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	authority := signedAssignedStageAuthorityWithoutRuntimeBarrier(
		t, database, job, assignment, 2,
	).Authority
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
		CommandID: uuid.New(), Identity: identity, ControlSessionEpoch: 2,
	}
	observationSequence := (assignment.WorkerInstanceEpoch << 32) + 1
	capacityObservedAt := time.Now().UTC().Truncate(time.Microsecond)
	capacityRequest := &velav1.ReportStageCapacityObservationRequest{
		WorkerInstanceId:    assignment.WorkerInstanceID.String(),
		WorkerInstanceEpoch: assignment.WorkerInstanceEpoch,
		ObservationSequence: observationSequence,
		CapacityVector:      capacity,
		ObservedAt:          timestamppb.New(capacityObservedAt),
		ExpiresAt:           timestamppb.New(capacityObservedAt.Add(2 * time.Minute)),
	}
	rejectedCapacity := proto.Clone(capacityRequest).(*velav1.ReportStageCapacityObservationRequest)
	rejectedCapacity.CapacityVector["uncertified-resource"] = 1
	_, err = backend.ReportCapacityObservation(
		context.Background(), command, rejectedCapacity,
	)
	assertPostgresConstraint(t, err, "capacity_observation_exceeds_worker_profile")
	var controlSessionEpoch int64
	if err := database.Admin.QueryRow(`
		SELECT control_session_epoch FROM worker_instances WHERE id = $1
	`, assignment.WorkerInstanceID).Scan(&controlSessionEpoch); err != nil || controlSessionEpoch != 2 {
		t.Fatalf("durable failed-operation Stage Worker session = %d error=%v", controlSessionEpoch, err)
	}
	command = stageworkercontrol.CommandContext{
		CommandID: uuid.New(), Identity: identity, ControlSessionEpoch: 3,
	}
	reported, err := backend.ReportCapacityObservation(
		context.Background(), command, capacityRequest,
	)
	if err != nil || !reported.Ready ||
		reported.ControlSessionEpoch != 3 ||
		reported.CapacityObservationSequence != observationSequence {
		t.Fatalf("record current Stage Worker capacity = %#v error=%v", reported, err)
	}
	nodeEvidence := workerRegistryEvidenceValue(t, assignment.WorkerInstanceID, 0xb0)
	if nodeEvidence.ControlSessionEpoch >= command.ControlSessionEpoch {
		t.Fatalf(
			"Node Agent fixture control epoch=%d is not stale relative to Stage Worker epoch=%d",
			nodeEvidence.ControlSessionEpoch,
			command.ControlSessionEpoch,
		)
	}
	nodeEvidence.Residencies[0].ModelComponentRevision = "h3-encoder-v1"
	nodeEvidence.Residencies[0].RuntimeImageDigest =
		"sha256:3333333333333333333333333333333333333333333333333333333333333333"
	nodeEvidence.Residencies[0].CanaryEvidenceDigest = hex.EncodeToString(readinessDigest[:])
	spiffeDigest := sha256.Sum256([]byte(identity.SPIFFEID))
	nodeEvidence.Members[0].IdentityDigest = hex.EncodeToString(spiffeDigest[:])
	nodeObservedAt := time.Now().UTC().Truncate(time.Microsecond)
	nodeEvidence.ObservedAt = nodeObservedAt
	nodeEvidence.ObservedBy = "node-agent/stage-capacity-takeover"
	nodeEvidence.Capacity.Sequence = assignment.ObservationSequence + 1
	nodeEvidence.Capacity.ObservedAt = nodeObservedAt
	nodeEvidence.Capacity.ExpiresAt = nodeObservedAt.Add(2 * time.Minute)
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	registry, err := fleet.NewService(fleetPool)
	if err != nil {
		t.Fatalf("construct post-takeover Worker Registry: %v", err)
	}
	nodeDecision, err := registry.Observe(context.Background(), nodeEvidence)
	if err != nil {
		t.Fatalf("refresh Fleet evidence after Stage capacity takeover: %v", err)
	}
	if nodeDecision.ControlSessionEpoch != command.ControlSessionEpoch {
		t.Fatalf(
			"Node Agent durable control epoch=%d want=%d",
			nodeDecision.ControlSessionEpoch,
			command.ControlSessionEpoch,
		)
	}
	nodeEvidence.ControlSessionEpoch = nodeDecision.ControlSessionEpoch
	var nodeCapacityRows int64
	var workerObservedBy string
	if err := database.Admin.QueryRow(`
		SELECT
			count(*) FILTER (WHERE observation_sequence = $2),
			(SELECT observed_by FROM worker_instances WHERE id = $1)
		FROM capacity_observations
		WHERE worker_instance_id = $1
	`, assignment.WorkerInstanceID, nodeEvidence.Capacity.Sequence).Scan(
		&nodeCapacityRows, &workerObservedBy,
	); err != nil {
		t.Fatalf("inspect post-takeover Fleet evidence: %v", err)
	}
	if nodeCapacityRows != 0 || workerObservedBy != nodeEvidence.ObservedBy {
		t.Fatalf(
			"post-takeover Fleet evidence capacity rows=%d observed_by=%q",
			nodeCapacityRows,
			workerObservedBy,
		)
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
		CapacityObservationSequence: observationSequence,
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
	if err := database.Admin.QueryRow(`
		SELECT control_session_epoch FROM worker_instances WHERE id = $1
	`, assignment.WorkerInstanceID).Scan(&controlSessionEpoch); err != nil || controlSessionEpoch != 3 {
		t.Fatalf("durable Stage Worker control session = %d error=%v", controlSessionEpoch, err)
	}
	recoveredCapacity, err := backend.ReportCapacityObservation(
		context.Background(), command, capacityRequest,
	)
	if err != nil || !recoveredCapacity.Ready ||
		recoveredCapacity.CapacityObservationSequence != observationSequence {
		t.Fatalf("recover stale local capacity high-water = %#v error=%v", recoveredCapacity, err)
	}
	renewedCapacity := proto.Clone(capacityRequest).(*velav1.ReportStageCapacityObservationRequest)
	renewedObservedAt := capacityObservedAt.Add(time.Second)
	renewedCapacity.ObservedAt = timestamppb.New(renewedObservedAt)
	renewedCapacity.ExpiresAt = timestamppb.New(capacityObservedAt.Add(3 * time.Minute))
	renewedResult, err := backend.ReportCapacityObservation(
		context.Background(), command, renewedCapacity,
	)
	if err != nil || !renewedResult.Ready ||
		renewedResult.CapacityObservationSequence != observationSequence {
		t.Fatalf("renew current Stage Worker capacity = %#v error=%v", renewedResult, err)
	}
	staleRenewal := proto.Clone(renewedCapacity).(*velav1.ReportStageCapacityObservationRequest)
	staleRenewal.ObservedAt = timestamppb.New(capacityObservedAt)
	_, err = backend.ReportCapacityObservation(context.Background(), command, staleRenewal)
	assertPostgresConstraint(t, err, "stage_worker_capacity_renewal_stale")
	replayedRegistration, err := backend.RegisterWorkerEvidence(
		context.Background(), command, registration,
	)
	if err != nil || !replayedRegistration.Ready {
		t.Fatalf("replay Stage Worker registration = %#v error=%v", replayedRegistration, err)
	}
	resolvedRegistration := proto.Clone(registration).(*velav1.RegisterWorkerEvidenceRequest)
	resolvedRegistration.CapacityObservationSequence = 0
	resolvedResult, err := backend.RegisterWorkerEvidence(
		context.Background(), command, resolvedRegistration,
	)
	if err != nil || !resolvedResult.Ready {
		t.Fatalf("resolve current Stage Worker capacity for registration = %#v error=%v", resolvedResult, err)
	}
	t.Run("registration serializes behind Fleet observation", func(t *testing.T) {
		const lockKey int64 = 660066
		concurrentEvidence := nodeEvidence
		concurrentEvidence.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
		concurrentEvidence.ObservedBy = "node-agent/stage-registration-lock-order"
		concurrentEvidence.Capacity.ObservedAt = concurrentEvidence.ObservedAt
		concurrentEvidence.Capacity.ExpiresAt = concurrentEvidence.ObservedAt.Add(2 * time.Minute)
		if _, err := database.Admin.Exec(fmt.Sprintf(`
			CREATE FUNCTION vela_test_pause_stage_worker_device() RETURNS trigger
			LANGUAGE plpgsql AS $$
			BEGIN
				IF NEW.id = '%s'::uuid
				   AND NEW.observed_by = 'node-agent/stage-registration-lock-order' THEN
					PERFORM pg_catalog.pg_advisory_xact_lock(660066);
				END IF;
				RETURN NEW;
			END
			$$;
			CREATE TRIGGER vela_test_pause_stage_worker_device
			BEFORE INSERT OR UPDATE ON devices
			FOR EACH ROW EXECUTE FUNCTION vela_test_pause_stage_worker_device();
		`, registration.GetDevices()[0].GetDeviceId())); err != nil {
			t.Fatalf("install Stage Worker registration lock-order trigger: %v", err)
		}

		blocker, err := database.Admin.Conn(context.Background())
		if err != nil {
			t.Fatalf("acquire Stage Worker lock-order blocker: %v", err)
		}
		defer blocker.Close()
		if _, err := blocker.ExecContext(
			context.Background(), "SELECT pg_advisory_lock($1)", lockKey,
		); err != nil {
			t.Fatalf("lock Stage Worker registration pause: %v", err)
		}
		unlocked := false
		defer func() {
			if !unlocked {
				var released bool
				_ = blocker.QueryRowContext(
					context.Background(), "SELECT pg_advisory_unlock($1)", lockKey,
				).Scan(&released)
			}
		}()

		fleetConnection, err := fleetPool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire concurrent Fleet connection: %v", err)
		}
		defer fleetConnection.Release()
		workerConnection, err := workerPool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire concurrent Stage Worker connection: %v", err)
		}
		defer workerConnection.Release()
		var fleetPID, workerPID int
		if err := fleetConnection.QueryRow(
			context.Background(), "SELECT pg_backend_pid()",
		).Scan(&fleetPID); err != nil {
			t.Fatalf("read concurrent Fleet backend PID: %v", err)
		}
		if err := workerConnection.QueryRow(
			context.Background(), "SELECT pg_backend_pid()",
		).Scan(&workerPID); err != nil {
			t.Fatalf("read concurrent Stage Worker backend PID: %v", err)
		}

		devices := make([]map[string]any, 0, len(registration.GetDevices()))
		for _, device := range registration.GetDevices() {
			devices = append(devices, map[string]any{
				"device_id": device.GetDeviceId(), "device_epoch": device.GetDeviceEpoch(),
			})
		}
		members := make([]map[string]any, 0, len(registration.GetMembers()))
		for _, member := range registration.GetMembers() {
			members = append(members, map[string]any{
				"worker_member_id":    member.GetWorkerMemberId(),
				"member_epoch":        member.GetMemberEpoch(),
				"model_runtime_epoch": member.GetModelRuntimeEpoch(),
			})
		}
		registrationPayload := mustJSON(t, map[string]any{
			"schema_version":                1,
			"worker_instance_id":            assignment.WorkerInstanceID,
			"worker_instance_epoch":         assignment.WorkerInstanceEpoch,
			"control_session_epoch":         command.ControlSessionEpoch,
			"device_set_digest":             hex.EncodeToString(registration.GetRuntimeIdentity().GetDeviceSetDigest()),
			"membership_digest":             hex.EncodeToString(registration.GetRuntimeIdentity().GetMembershipDigest()),
			"model_residency_id":            assignment.ModelResidencyID,
			"runtime_identity":              registration.GetRuntimeIdentity().GetRuntimeIdentity(),
			"model_runtime_epoch":           registration.GetRuntimeIdentity().GetModelRuntimeEpoch(),
			"stage_profile_revision_id":     registration.GetRuntimeIdentity().GetStageProfileRevisionId(),
			"worker_member_id":              registration.GetRuntimeIdentity().GetWorkerMemberId(),
			"worker_member_epoch":           registration.GetRuntimeIdentity().GetWorkerMemberEpoch(),
			"capacity_observation_sequence": 0,
			"spiffe_id_digest":              hex.EncodeToString(spiffeDigest[:]),
			"readiness_evidence_digest":     hex.EncodeToString(readinessDigest[:]),
			"devices":                       devices,
			"members":                       members,
		})
		fleetPayload := mustJSON(t, concurrentEvidence)

		type concurrentResult struct {
			ready bool
			err   error
		}
		fleetResults := make(chan error, 1)
		workerResults := make(chan concurrentResult, 1)
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		go func() {
			_, observeErr := fleetConnection.Exec(
				ctx, "SELECT * FROM vela_observe_worker_instance($1::jsonb)",
				fleetPayload,
			)
			fleetResults <- observeErr
		}()

		waitForLock := func(pid int, label string, early <-chan error) {
			t.Helper()
			deadline := time.Now().Add(6 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case err := <-early:
					t.Fatalf("%s completed before lock observation: %v", label, err)
				default:
				}
				var waitEventType string
				if err := database.Admin.QueryRow(`
					SELECT COALESCE(wait_event_type, '')
					FROM pg_catalog.pg_stat_activity
					WHERE pid = $1
				`, pid).Scan(&waitEventType); err != nil {
					t.Fatalf("inspect %s lock wait: %v", label, err)
				}
				if waitEventType == "Lock" {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatalf("%s did not reach the expected lock wait", label)
		}
		waitForLock(fleetPID, "Fleet observation", fleetResults)

		workerEarly := make(chan error, 1)
		go func() {
			var result concurrentResult
			var reason string
			result.err = workerConnection.QueryRow(ctx, `
				SELECT ready, reason
					FROM vela_register_stage_worker_runtime($1::jsonb)
			`, registrationPayload).Scan(&result.ready, &reason)
			workerResults <- result
			workerEarly <- result.err
		}()
		waitForLock(workerPID, "Stage Worker registration", workerEarly)

		if err := blocker.QueryRowContext(
			context.Background(), "SELECT pg_advisory_unlock($1)", lockKey,
		).Scan(&unlocked); err != nil || !unlocked {
			t.Fatalf("release Stage Worker registration pause = %t error=%v", unlocked, err)
		}
		select {
		case err := <-fleetResults:
			if err != nil {
				t.Fatalf("serialized Fleet observation: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("serialized Fleet observation did not finish")
		}
		select {
		case result := <-workerResults:
			if result.err != nil || !result.ready {
				t.Fatalf("serialized Stage Worker registration = %#v", result)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("serialized Stage Worker registration did not finish")
		}
	})
	t.Run("ASSIGN and registration preserve WorkerInstance residency lock order", func(t *testing.T) {
		const gateSeed int64 = 590059
		devices := make([]map[string]any, 0, len(registration.GetDevices()))
		for _, device := range registration.GetDevices() {
			devices = append(devices, map[string]any{
				"device_id": device.GetDeviceId(), "device_epoch": device.GetDeviceEpoch(),
			})
		}
		members := make([]map[string]any, 0, len(registration.GetMembers()))
		for _, member := range registration.GetMembers() {
			members = append(members, map[string]any{
				"worker_member_id":    member.GetWorkerMemberId(),
				"member_epoch":        member.GetMemberEpoch(),
				"model_runtime_epoch": member.GetModelRuntimeEpoch(),
			})
		}
		registrationPayload := mustJSON(t, map[string]any{
			"schema_version":                1,
			"worker_instance_id":            assignment.WorkerInstanceID,
			"worker_instance_epoch":         assignment.WorkerInstanceEpoch,
			"control_session_epoch":         command.ControlSessionEpoch,
			"device_set_digest":             hex.EncodeToString(registration.GetRuntimeIdentity().GetDeviceSetDigest()),
			"membership_digest":             hex.EncodeToString(registration.GetRuntimeIdentity().GetMembershipDigest()),
			"model_residency_id":            assignment.ModelResidencyID,
			"runtime_identity":              registration.GetRuntimeIdentity().GetRuntimeIdentity(),
			"model_runtime_epoch":           registration.GetRuntimeIdentity().GetModelRuntimeEpoch(),
			"stage_profile_revision_id":     registration.GetRuntimeIdentity().GetStageProfileRevisionId(),
			"worker_member_id":              registration.GetRuntimeIdentity().GetWorkerMemberId(),
			"worker_member_epoch":           registration.GetRuntimeIdentity().GetWorkerMemberEpoch(),
			"capacity_observation_sequence": 0,
			"spiffe_id_digest":              hex.EncodeToString(spiffeDigest[:]),
			"readiness_evidence_digest":     hex.EncodeToString(readinessDigest[:]),
			"devices":                       devices,
			"members":                       members,
		})
		assignPayload := mustJSON(t, map[string]any{
			"schema_version":                1,
			"command_kind":                  "ASSIGN",
			"command_id":                    assignment.CommandID,
			"attempt_id":                    assignment.AttemptID,
			"stage_run_id":                  assignment.StageRunID,
			"expected_attempt_fence":        assignment.ExpectedAttemptFence,
			"expected_stage_fence":          assignment.ExpectedStageFence,
			"expected_stage_version":        assignment.ExpectedStageVersion,
			"stage_attempt_id":              assignment.StageAttemptID,
			"stage_allocation_id":           assignment.StageAllocationID,
			"stage_lease_id":                assignment.StageLeaseID,
			"stage_profile_revision_id":     assignment.StageProfileRevisionID,
			"capacity_pool_id":              assignment.CapacityPoolID,
			"worker_instance_id":            assignment.WorkerInstanceID,
			"worker_instance_epoch":         assignment.WorkerInstanceEpoch,
			"capacity_observation_sequence": assignment.ObservationSequence,
			"device_set_digest":             hex.EncodeToString(assignment.DeviceSetDigest),
			"membership_digest":             hex.EncodeToString(assignment.MembershipDigest),
			"model_residency_id":            assignment.ModelResidencyID,
			"model_runtime_epoch":           assignment.ModelRuntimeEpoch,
			"capacity_vector":               assignment.CapacityVector,
			"token_digest":                  hex.EncodeToString(assignment.TokenDigest),
			"signing_key_id":                assignment.SigningKeyID,
			"execution_nonce":               hex.EncodeToString(assignment.ExecutionNonce),
			"issued_at":                     assignment.IssuedAt.UTC().Format(time.RFC3339Nano),
			"expires_at":                    assignment.ExpiresAt.UTC().Format(time.RFC3339Nano),
			"local_deadline_at":             assignment.LocalDeadlineAt.UTC().Format(time.RFC3339Nano),
		})

		blocker, err := database.Admin.Conn(context.Background())
		if err != nil {
			t.Fatalf("acquire runtime gate blocker: %v", err)
		}
		defer blocker.Close()
		if _, err := blocker.ExecContext(context.Background(), `
			SELECT pg_advisory_lock(pg_catalog.hashtextextended($1::text, $2))
		`, assignment.ModelResidencyID.String(), gateSeed); err != nil {
			t.Fatalf("lock ModelRuntime epoch gate: %v", err)
		}
		unlocked := false
		defer func() {
			if !unlocked {
				var released bool
				_ = blocker.QueryRowContext(context.Background(), `
					SELECT pg_advisory_unlock(pg_catalog.hashtextextended($1::text, $2))
				`, assignment.ModelResidencyID.String(), gateSeed).Scan(&released)
			}
		}()

		coordinatorPool := newRolePool(
			t, database.DSN,
			"vela_attempt_coordinator_login", "vela-attempt-coordinator-password",
		)
		assignConnection, err := coordinatorPool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire concurrent ASSIGN connection: %v", err)
		}
		defer assignConnection.Release()
		registrationConnection, err := workerPool.Acquire(context.Background())
		if err != nil {
			t.Fatalf("acquire concurrent registration connection: %v", err)
		}
		defer registrationConnection.Release()
		var assignPID, registrationPID int
		if err := assignConnection.QueryRow(
			context.Background(), "SELECT pg_backend_pid()",
		).Scan(&assignPID); err != nil {
			t.Fatalf("read ASSIGN backend PID: %v", err)
		}
		if err := registrationConnection.QueryRow(
			context.Background(), "SELECT pg_backend_pid()",
		).Scan(&registrationPID); err != nil {
			t.Fatalf("read registration backend PID: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		assignResults := make(chan error, 1)
		registrationResults := make(chan error, 1)
		go func() {
			_, assignErr := assignConnection.Exec(
				ctx, "SELECT * FROM vela_apply_stage_command($1::jsonb)", assignPayload,
			)
			assignResults <- assignErr
		}()
		waitForLock := func(pid int, label string, early <-chan error) {
			t.Helper()
			deadline := time.Now().Add(6 * time.Second)
			for time.Now().Before(deadline) {
				select {
				case err := <-early:
					t.Fatalf("%s completed before lock observation: %v", label, err)
				default:
				}
				var waitEventType string
				if err := database.Admin.QueryRow(`
					SELECT COALESCE(wait_event_type, '')
					FROM pg_catalog.pg_stat_activity
					WHERE pid = $1
				`, pid).Scan(&waitEventType); err != nil {
					t.Fatalf("inspect %s lock wait: %v", label, err)
				}
				if waitEventType == "Lock" {
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			t.Fatalf("%s did not reach the expected lock wait", label)
		}
		waitForLock(assignPID, "ASSIGN", assignResults)
		go func() {
			var ready bool
			var reason string
			registerErr := registrationConnection.QueryRow(ctx, `
				SELECT ready, reason FROM vela_register_stage_worker_runtime($1::jsonb)
			`, registrationPayload).Scan(&ready, &reason)
			if registerErr == nil && !ready {
				registerErr = fmt.Errorf("registration not ready: %s", reason)
			}
			registrationResults <- registerErr
		}()
		waitForLock(registrationPID, "registration", registrationResults)

		if err := blocker.QueryRowContext(context.Background(), `
			SELECT pg_advisory_unlock(pg_catalog.hashtextextended($1::text, $2))
		`, assignment.ModelResidencyID.String(), gateSeed).Scan(&unlocked); err != nil || !unlocked {
			t.Fatalf("release ModelRuntime epoch gate = %t error=%v", unlocked, err)
		}
		for label, results := range map[string]<-chan error{
			"ASSIGN": assignResults, "registration": registrationResults,
		} {
			select {
			case resultErr := <-results:
				if resultErr != nil {
					if strings.Contains(resultErr.Error(), "SQLSTATE 40P01") {
						t.Fatalf("%s hit a WorkerInstance/residency deadlock: %v", label, resultErr)
					}
					t.Fatalf("serialized %s: %v", label, resultErr)
				}
			case <-time.After(10 * time.Second):
				t.Fatalf("serialized %s did not finish", label)
			}
		}
	})
	_, err = backend.RegisterWorkerEvidence(
		context.Background(), stageworkercontrol.CommandContext{
			CommandID:           uuid.New(),
			Identity:            stageworkertransport.Identity{SPIFFEID: identity.SPIFFEID + "/forged"},
			ControlSessionEpoch: 5,
		}, registration,
	)
	assertPostgresConstraint(t, err, "stage_worker_control_identity_conflict")

	restarted := proto.Clone(registration).(*velav1.RegisterWorkerEvidenceRequest)
	restarted.RuntimeIdentity.ModelRuntimeEpoch++
	for _, member := range restarted.Members {
		member.ModelRuntimeEpoch = restarted.RuntimeIdentity.ModelRuntimeEpoch
	}
	restartedResult, err := backend.RegisterWorkerEvidence(
		context.Background(), command, restarted,
	)
	if err != nil || !restartedResult.Ready {
		t.Fatalf("register restarted ModelRuntime = %#v error=%v", restartedResult, err)
	}
	nodeEvidence.ObservedAt = time.Now().UTC().Truncate(time.Microsecond)
	nodeEvidence.ObservedBy = "node-agent/post-runtime-epoch-refresh"
	nodeEvidence.Capacity.ObservedAt = nodeEvidence.ObservedAt
	nodeEvidence.Capacity.ExpiresAt = nodeEvidence.ObservedAt.Add(2 * time.Minute)
	refreshedNode, err := registry.Observe(context.Background(), nodeEvidence)
	if err != nil || refreshedNode.ModelRuntimeEpoch != restarted.RuntimeIdentity.GetModelRuntimeEpoch() {
		t.Fatalf(
			"refresh Node evidence after ModelRuntime epoch advance = %#v error=%v",
			refreshedNode,
			err,
		)
	}
	var durableEpoch, registrationCount int64
	var durableCanaryDigest []byte
	var leaseState, allocationState, stageState, physicalState, jobState string
	var billableStarted, emptyResourceTotals bool
	var consumedResourceUnits int64
	var oldAuthorityMatches bool
	if err := database.Admin.QueryRow(`
		SELECT residency.model_runtime_epoch,
		       (SELECT count(*) FROM model_runtime_epoch_registrations
		        WHERE model_residency_id = residency.id),
		       residency.canary_evidence_digest,
		       lease.state::text,
		       allocation.state::text,
		       run.state::text,
		       physical.state::text,
		       job.state::text,
		       job.billable_started_at IS NOT NULL,
		       physical.resource_totals = '{}'::jsonb,
		       attempt_budget.consumed_resource_units,
		       vela_worker_instance_authority_matches($2, $3, $4, $5, $1, $6)
		FROM model_residencies AS residency
		JOIN stage_leases AS lease ON lease.id = $7
		JOIN stage_allocations AS allocation ON allocation.id = lease.stage_allocation_id
		JOIN stage_runs AS run ON run.id = lease.stage_run_id
		JOIN stage_attempts AS physical ON physical.id = lease.stage_attempt_id
		JOIN attempts AS attempt ON attempt.id = lease.attempt_id
		JOIN jobs AS job ON job.id = attempt.job_id
		JOIN attempt_retry_budgets AS attempt_budget ON attempt_budget.attempt_id = attempt.id
		WHERE residency.id = $1
	`, assignment.ModelResidencyID, assignment.WorkerInstanceID,
		assignment.WorkerInstanceEpoch, authority.GetDeviceSetDigest(),
		authority.GetMembershipDigest(), registration.RuntimeIdentity.GetModelRuntimeEpoch(),
		assignment.StageLeaseID,
	).Scan(
		&durableEpoch, &registrationCount, &durableCanaryDigest,
		&leaseState, &allocationState, &stageState, &physicalState, &jobState,
		&billableStarted, &emptyResourceTotals, &consumedResourceUnits,
		&oldAuthorityMatches,
	); err != nil {
		t.Fatalf("read restarted ModelRuntime authority: %v", err)
	}
	if durableEpoch != restarted.RuntimeIdentity.GetModelRuntimeEpoch() || registrationCount != 2 ||
		!bytes.Equal(durableCanaryDigest, readinessDigest[:]) || leaseState != "REVOKED" ||
		allocationState != "RELEASED" || stageState != "RETRY_WAIT" || physicalState != "LOST" ||
		jobState != "QUEUED" || billableStarted || !emptyResourceTotals ||
		consumedResourceUnits != 0 || oldAuthorityMatches {
		t.Fatalf(
			"restarted ModelRuntime authority = epoch %d registrations %d lease/allocation/stage/physical/job %s/%s/%s/%s/%s billable=%t empty-resources=%t consumed=%d old-match=%t",
			durableEpoch, registrationCount, leaseState, allocationState,
			stageState, physicalState, jobState, billableStarted,
			emptyResourceTotals, consumedResourceUnits, oldAuthorityMatches,
		)
	}
	reconciled, err := backend.RegisterWorkerEvidence(
		context.Background(), stageworkercontrol.CommandContext{
			CommandID: uuid.New(), Identity: identity, ControlSessionEpoch: 9223372036854775806,
		}, registration,
	)
	if err != nil || reconciled.Ready || reconciled.ControlSessionEpoch != 4 ||
		!strings.Contains(reconciled.Reason, "stale") {
		t.Fatalf("reconcile client-selected control epoch = %#v error=%v", reconciled, err)
	}
	command.ControlSessionEpoch = reconciled.ControlSessionEpoch
	newSessionCapacity := proto.Clone(capacityRequest).(*velav1.ReportStageCapacityObservationRequest)
	newSessionObservedAt := time.Now().UTC().Truncate(time.Microsecond)
	newSessionCapacity.ObservedAt = timestamppb.New(newSessionObservedAt)
	newSessionCapacity.ExpiresAt = timestamppb.New(newSessionObservedAt.Add(2 * time.Minute))
	newSessionResult, err := backend.ReportCapacityObservation(
		context.Background(), command, newSessionCapacity,
	)
	if err != nil || !newSessionResult.Ready ||
		newSessionResult.CapacityObservationSequence != observationSequence+1 {
		t.Fatalf("publish new-session Stage Worker capacity = %#v error=%v", newSessionResult, err)
	}

	changedCapacity := proto.Clone(newSessionCapacity).(*velav1.ReportStageCapacityObservationRequest)
	for resource := range changedCapacity.CapacityVector {
		changedCapacity.CapacityVector[resource] = 0
		break
	}
	changedObservedAt := time.Now().UTC().Truncate(time.Microsecond)
	changedExpiresAt := changedObservedAt.Add(time.Second)
	changedCapacity.ObservedAt = timestamppb.New(changedObservedAt)
	changedCapacity.ExpiresAt = timestamppb.New(changedExpiresAt)
	changedResult, err := backend.ReportCapacityObservation(
		context.Background(), command, changedCapacity,
	)
	if err != nil || !changedResult.Ready ||
		changedResult.CapacityObservationSequence != observationSequence+2 {
		t.Fatalf("publish changed Stage Worker capacity = %#v error=%v", changedResult, err)
	}
	backwardCapacity := proto.Clone(changedCapacity).(*velav1.ReportStageCapacityObservationRequest)
	backwardCapacity.ObservedAt = timestamppb.New(changedObservedAt.Add(-time.Microsecond))
	_, err = backend.ReportCapacityObservation(context.Background(), command, backwardCapacity)
	assertPostgresConstraint(t, err, "stage_worker_capacity_renewal_stale")
	if delay := time.Until(changedExpiresAt.Add(100 * time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}
	expiredCapacity := proto.Clone(changedCapacity).(*velav1.ReportStageCapacityObservationRequest)
	expiredObservedAt := time.Now().UTC().Truncate(time.Microsecond)
	expiredCapacity.ObservedAt = timestamppb.New(expiredObservedAt)
	expiredCapacity.ExpiresAt = timestamppb.New(expiredObservedAt.Add(2 * time.Minute))
	expiredResult, err := backend.ReportCapacityObservation(
		context.Background(), command, expiredCapacity,
	)
	if err != nil || !expiredResult.Ready ||
		expiredResult.CapacityObservationSequence != observationSequence+3 {
		t.Fatalf("replace expired Stage Worker capacity = %#v error=%v", expiredResult, err)
	}

	wrongProfile := proto.Clone(registration).(*velav1.RegisterWorkerEvidenceRequest)
	wrongProfile.RuntimeIdentity.StageProfileRevisionId = uuid.NewString()
	rejected, err := backend.RegisterWorkerEvidence(
		context.Background(), command, wrongProfile,
	)
	if err != nil || rejected.Ready {
		t.Fatalf("forged Worker profile evidence = %#v error=%v", rejected, err)
	}

	forgedCapacity := proto.Clone(capacityRequest).(*velav1.ReportStageCapacityObservationRequest)
	forgedCapacity.CapacityVector["concurrency"]++
	_, err = backend.ReportCapacityObservation(
		context.Background(), command, forgedCapacity,
	)
	assertPostgresConstraint(t, err, "capacity_observation_exceeds_worker_profile")
	futureCapacity := proto.Clone(capacityRequest).(*velav1.ReportStageCapacityObservationRequest)
	futureObservedAt := time.Now().UTC().Add(2 * time.Hour).Truncate(time.Microsecond)
	futureCapacity.ObservationSequence++
	futureCapacity.ObservedAt = timestamppb.New(futureObservedAt)
	futureCapacity.ExpiresAt = timestamppb.New(futureObservedAt.Add(2 * time.Minute))
	_, err = backend.ReportCapacityObservation(context.Background(), command, futureCapacity)
	assertPostgresConstraint(t, err, "stage_worker_capacity_report_invalid")

	var observationCount int64
	if err := database.Admin.QueryRow(`
		SELECT count(*) FROM capacity_observations WHERE worker_instance_id = $1
	`, assignment.WorkerInstanceID).Scan(&observationCount); err != nil {
		t.Fatalf("count durable CapacityObservations: %v", err)
	}
	if observationCount != 5 {
		t.Fatalf("StageWorkerControl capacity observation count=%d, want bootstrap plus four Stage reports", observationCount)
	}
}

func TestStageWorkerControlSessionCapacityMigrationEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 65)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")

	canonicalFunctionOIDs := func() (int64, int64) {
		t.Helper()
		var registerOID, capacityOID int64
		if err := database.Admin.QueryRow(`
			SELECT
				'public.vela_register_stage_worker_runtime(jsonb)'::regprocedure::oid::bigint,
				'public.vela_verify_stage_capacity_observation(jsonb)'::regprocedure::oid::bigint
		`).Scan(&registerOID, &capacityOID); err != nil {
			t.Fatalf("inspect canonical Stage Worker function OIDs: %v", err)
		}
		return registerOID, capacityOID
	}
	assertCanonicalFunctionOIDs := func(wantRegister, wantCapacity int64) {
		t.Helper()
		gotRegister, gotCapacity := canonicalFunctionOIDs()
		if gotRegister != wantRegister || gotCapacity != wantCapacity {
			t.Fatalf(
				"canonical Stage Worker function OIDs register/capacity=%d/%d want=%d/%d",
				gotRegister, gotCapacity, wantRegister, wantCapacity,
			)
		}
	}

	assertSurface := func(want bool) {
		t.Helper()
		var reconnectHelper, register, capacity, reportHelper, directFleet bool
		var ownerReconnect, directSessionLock, ownerSessionLock, ownerReport bool
		if err := database.Admin.QueryRow(`
			SELECT
				COALESCE(has_function_privilege(
					'vela_stage_worker_control',
					to_regprocedure('vela_reconnect_stage_worker_control(jsonb)'), 'EXECUTE'
				), false),
				has_function_privilege(
					'vela_stage_worker_control',
					'vela_register_stage_worker_runtime(jsonb)', 'EXECUTE'
				),
				has_function_privilege(
					'vela_stage_worker_control',
					'vela_verify_stage_capacity_observation(jsonb)', 'EXECUTE'
				),
				COALESCE(has_function_privilege(
					'vela_stage_worker_control',
					to_regprocedure('vela_report_stage_worker_capacity_v66(jsonb)'), 'EXECUTE'
				), false),
				has_function_privilege(
					'vela_stage_worker_control',
					'vela_reconnect_worker_instance(uuid,bigint,bigint,text,timestamp with time zone,text)',
					'EXECUTE'
				),
				COALESCE(has_function_privilege(
					'vela_attempt_coordinator_owner',
					to_regprocedure('vela_reconnect_stage_worker_control(jsonb)'), 'EXECUTE'
				), false),
				COALESCE(has_function_privilege(
					'vela_stage_worker_control',
					to_regprocedure('vela_lock_stage_worker_control_session(uuid,bigint,bigint)'),
					'EXECUTE'
				), false),
				COALESCE(has_function_privilege(
					'vela_attempt_coordinator_owner',
					to_regprocedure('vela_lock_stage_worker_control_session(uuid,bigint,bigint)'),
					'EXECUTE'
				), false),
				COALESCE(has_function_privilege(
					'vela_attempt_coordinator_owner',
					to_regprocedure('vela_report_stage_worker_capacity_v66(jsonb)'), 'EXECUTE'
				), false)
		`).Scan(
			&reconnectHelper, &register, &capacity, &reportHelper, &directFleet,
			&ownerReconnect, &directSessionLock, &ownerSessionLock, &ownerReport,
		); err != nil {
			t.Fatalf("inspect Stage Worker session/capacity role surface: %v", err)
		}
		if reconnectHelper || !register || !capacity || reportHelper || directFleet ||
			ownerReconnect != want || directSessionLock || ownerSessionLock != want ||
			ownerReport != want {
			t.Fatalf("Stage Worker session/capacity surface helper/register/capacity/report/fleet/owner/lock/report-owner=%t/%t/%t/%t/%t/%t/%t/%t/%t want=false/true/true/false/false/%t/false/%t/%t",
				reconnectHelper, register, capacity, reportHelper, directFleet,
				ownerReconnect, directSessionLock, ownerSessionLock, ownerReport,
				want, want, want)
		}
		var observeDefinition string
		if err := database.Admin.QueryRow(`
			SELECT pg_get_functiondef('public.vela_observe_worker_instance(jsonb)'::regprocedure)
		`).Scan(&observeDefinition); err != nil {
			t.Fatalf("inspect WorkerInstance observation definition: %v", err)
		}
		hasStageCapacityTakeover := strings.Contains(
			observeDefinition,
			"v_worker.instance_epoch::numeric * 4294967296",
		)
		if hasStageCapacityTakeover != want {
			t.Fatalf("WorkerInstance observation Stage capacity takeover=%t want=%t", hasStageCapacityTakeover, want)
		}
		if !want {
			return
		}
		var registrationDefinition string
		if err := database.Admin.QueryRow(`
			SELECT pg_get_functiondef(to_regprocedure('public.vela_register_stage_worker_runtime(jsonb)'))
		`).Scan(&registrationDefinition); err != nil {
			t.Fatalf("inspect Stage Worker registration definition: %v", err)
		}
		workerLock := strings.Index(registrationDefinition, "vela_lock_stage_worker_control_session")
		residencyGate := strings.Index(registrationDefinition, "vela_lock_model_runtime_epoch_gate")
		residencyLock := strings.Index(registrationDefinition, "FROM model_residencies AS residency")
		if workerLock < 0 || residencyGate <= workerLock || residencyLock <= residencyGate {
			t.Fatalf(
				"Stage Worker registration lock order Worker/ResidencyGate/Residency=%d/%d/%d",
				workerLock,
				residencyGate,
				residencyLock,
			)
		}
	}
	assertSurface(false)
	registerOID, capacityOID := canonicalFunctionOIDs()
	if err := goose.UpTo(database.Admin, migrations, 66); err != nil {
		t.Fatalf("expand Stage Worker session/capacity migration: %v", err)
	}
	assertSurface(true)
	assertCanonicalFunctionOIDs(registerOID, capacityOID)
	if err := goose.DownTo(database.Admin, migrations, 65); err != nil {
		t.Fatalf("contract Stage Worker session/capacity migration: %v", err)
	}
	assertSurface(false)
	assertCanonicalFunctionOIDs(registerOID, capacityOID)
	if err := goose.UpTo(database.Admin, migrations, 66); err != nil {
		t.Fatalf("re-expand Stage Worker session/capacity migration: %v", err)
	}
	assertSurface(true)
	assertCanonicalFunctionOIDs(registerOID, capacityOID)
}

func TestStageWorkerControlSessionCapacityMigrationRefusesNonemptyDown(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-capacity-down")
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	now := time.Now().UTC().Truncate(time.Microsecond)
	stageSequence := (assignment.WorkerInstanceEpoch << 32) + 1
	if _, err := database.Admin.Exec(`
		INSERT INTO capacity_observations (
			worker_instance_id, worker_instance_epoch, observation_sequence,
			capacity_vector, observed_at, expires_at, observed_by
		) VALUES ($1, $2, $3, '{"gpu": 0}'::jsonb, $4, $5, 'stage-worker-control/down-test')
	`, assignment.WorkerInstanceID, assignment.WorkerInstanceEpoch,
		stageSequence, now, now.Add(time.Minute)); err != nil {
		t.Fatalf("seed Stage Worker capacity before Down: %v", err)
	}
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	err := goose.DownTo(database.Admin, migrations, 65)
	assertPostgresConstraint(t, err, "stage_worker_capacity_rollback_is_unsafe")
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 66 {
		t.Fatalf("Stage Worker capacity version after refused Down = %d error=%v", version, versionErr)
	}
}

func TestStageWorkerControlSessionCapacityMigrationDownSerializesConcurrentWriter(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ :=
		newStageGraphCancellationFixture(t, "stage-worker-capacity-concurrent-down")
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour),
	)
	writer, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin concurrent Stage Worker capacity writer: %v", err)
	}
	defer func() { _ = writer.Rollback() }()
	now := time.Now().UTC().Truncate(time.Microsecond)
	stageSequence := (assignment.WorkerInstanceEpoch << 32) + 1
	if _, err := writer.Exec(`
		INSERT INTO capacity_observations (
			worker_instance_id, worker_instance_epoch, observation_sequence,
			capacity_vector, observed_at, expires_at, observed_by
		) VALUES ($1, $2, $3, '{"gpu": 0}'::jsonb, $4, $5, 'stage-worker-control/concurrent-down')
	`, assignment.WorkerInstanceID, assignment.WorkerInstanceEpoch,
		stageSequence, now, now.Add(time.Minute)); err != nil {
		t.Fatalf("write concurrent Stage Worker capacity evidence: %v", err)
	}

	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	downErrors := make(chan error, 1)
	go func() {
		downErrors <- goose.DownTo(database.Admin, migrations, 65)
	}()
	waitForRoleDatabaseLock(t, database.Admin, "postgres")
	if err := writer.Commit(); err != nil {
		t.Fatalf("commit concurrent Stage Worker capacity evidence: %v", err)
	}

	select {
	case downErr := <-downErrors:
		assertPostgresConstraint(t, downErr, "stage_worker_capacity_rollback_is_unsafe")
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent Stage Worker capacity migration Down did not finish")
	}
	version, versionErr := goose.GetDBVersion(database.Admin)
	if versionErr != nil || version != 66 {
		t.Fatalf(
			"Stage Worker capacity version after concurrent refused Down = %d error=%v",
			version,
			versionErr,
		)
	}
}

func TestModelRuntimeEpochRegistrationWaitsForExactMultiMemberBarrier(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	seedWorkerRegistryPlan(t, database.Admin)
	seedMultiMemberProfile(t, database.Admin)
	workerID := uuid.MustParse("49200000-0000-0000-0000-000000000103")
	if _, err := database.Admin.Exec(`
		INSERT INTO worker_instances (
			id, worker_profile_revision_id, capacity_pool_id, worker_bundle_id,
			lifecycle_state, reachability_state, instance_epoch,
			control_session_epoch, desired_member_count, desired_device_count
		) VALUES ($1, $2, $3, $4, 'PROVISIONING', 'DISCONNECTED', 1, 1, 2, 4)
	`, workerID, multiWorkerProfileID, multiCapacityPoolID, workerRegistryBundleID); err != nil {
		t.Fatalf("seed multi-member WorkerInstance: %v", err)
	}
	fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
	if _, err := fleetPool.Exec(
		context.Background(),
		"SELECT * FROM vela_observe_worker_instance($1::jsonb)",
		multiMemberWorkerEvidence(workerID, true),
	); err != nil {
		t.Fatalf("observe multi-member WorkerInstance: %v", err)
	}
	seedModelRuntimeCapacityRoute(
		t, database.Admin, workerID,
		uuid.MustParse("49200000-0000-0000-0000-000000000123"),
		uuid.MustParse(multiCapacityPoolID), uuid.MustParse(multiStageProfileID),
	)

	var deviceSetDigest, membershipDigest, canaryDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT worker.device_set_digest, worker.membership_digest,
		       residency.canary_evidence_digest
		FROM worker_instances AS worker
		JOIN model_residencies AS residency ON residency.worker_instance_id = worker.id
		WHERE worker.id = $1
	`, workerID).Scan(&deviceSetDigest, &membershipDigest, &canaryDigest); err != nil {
		t.Fatalf("read multi-member runtime authority: %v", err)
	}
	devices := []map[string]any{}
	for suffix := 2; suffix <= 5; suffix++ {
		devices = append(devices, map[string]any{
			"device_id":    "49200000-0000-0000-0000-00000000011" + string(rune('0'+suffix)),
			"device_epoch": 1,
		})
	}
	members := []map[string]any{
		{
			"worker_member_id": "49200000-0000-0000-0000-000000000120",
			"member_epoch":     1, "model_runtime_epoch": 2,
		},
		{
			"worker_member_id": "49200000-0000-0000-0000-000000000121",
			"member_epoch":     1, "model_runtime_epoch": 7,
		},
	}
	registration := func(memberIndex int, identityByte byte, controlSessionEpoch int64) []byte {
		t.Helper()
		member := members[memberIndex]
		payload, err := json.Marshal(map[string]any{
			"schema_version": 1, "worker_instance_id": workerID,
			"worker_instance_epoch": 1, "control_session_epoch": controlSessionEpoch,
			"device_set_digest":         hex.EncodeToString(deviceSetDigest),
			"membership_digest":         hex.EncodeToString(membershipDigest),
			"model_residency_id":        "49200000-0000-0000-0000-000000000123",
			"runtime_identity":          "future-llm-runtime@sha256:runtime-v1",
			"model_runtime_epoch":       member["model_runtime_epoch"],
			"stage_profile_revision_id": multiStageProfileID,
			"worker_member_id":          member["worker_member_id"], "worker_member_epoch": 1,
			"capacity_observation_sequence": 1,
			"spiffe_id_digest":              hex.EncodeToString(bytes.Repeat([]byte{identityByte}, 32)),
			"readiness_evidence_digest":     hex.EncodeToString(canaryDigest),
			"devices":                       devices[memberIndex*2 : memberIndex*2+2],
			"members":                       []map[string]any{member},
		})
		if err != nil {
			t.Fatalf("encode multi-member registration: %v", err)
		}
		return payload
	}
	workerPool := newRolePool(
		t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password",
	)
	reconnect := func(payload []byte) (int64, error) {
		var durableWorkerID uuid.UUID
		var workerEpoch, sessionEpoch int64
		if err := workerPool.QueryRow(context.Background(), `
			SELECT worker_instance_id, worker_instance_epoch,
			       COALESCE(barrier_generation, 0)
			FROM vela_register_stage_worker_runtime(
				jsonb_set(
					$1::jsonb,
					'{stage_worker_control_reconnect}',
					'true'::jsonb,
					true
				)
			)
		`, payload).Scan(&durableWorkerID, &workerEpoch, &sessionEpoch); err != nil {
			return 0, err
		}
		if durableWorkerID != workerID || workerEpoch != 1 {
			return 0, fmt.Errorf(
				"malformed reconnect result %s/%d/%d",
				durableWorkerID, workerEpoch, sessionEpoch,
			)
		}
		return sessionEpoch, nil
	}
	register := func(payload []byte) (bool, string, error) {
		var ready bool
		var reason string
		err := workerPool.QueryRow(context.Background(), `
			SELECT ready, reason FROM vela_register_stage_worker_runtime($1::jsonb)
		`, payload).Scan(&ready, &reason)
		return ready, reason, err
	}

	firstPayload := registration(0, 0xa1, 2)
	if sessionEpoch, err := reconnect(firstPayload); err != nil || sessionEpoch != 2 {
		t.Fatalf("first multi-member reconnect epoch=%d error=%v", sessionEpoch, err)
	}
	firstReady, firstReason, err := register(firstPayload)
	if err != nil || firstReady || !strings.Contains(firstReason, "waiting") {
		t.Fatalf("first multi-member registration ready=%t reason=%q error=%v", firstReady, firstReason, err)
	}
	var durableEpoch, barrierGeneration, registrationCount int64
	var residencyState, barrierState string
	if err := database.Admin.QueryRow(`
		SELECT residency.model_runtime_epoch, residency.state::text,
		       barrier.barrier_generation, barrier.state::text,
		       count(registration.worker_member_id)
		FROM model_residencies AS residency
		JOIN model_runtime_barriers AS barrier
		  ON barrier.model_residency_id = residency.id
		LEFT JOIN model_runtime_epoch_registrations AS registration
		  ON registration.model_residency_id = barrier.model_residency_id
		 AND registration.barrier_generation = barrier.barrier_generation
		WHERE residency.id = '49200000-0000-0000-0000-000000000123'
		GROUP BY residency.model_runtime_epoch, residency.state,
		         barrier.barrier_generation, barrier.state
	`).Scan(
		&durableEpoch, &residencyState, &barrierGeneration, &barrierState,
		&registrationCount,
	); err != nil {
		t.Fatalf("read partial multi-member barrier: %v", err)
	}
	if durableEpoch != 1 || residencyState != "WARMING" || barrierGeneration != 2 ||
		barrierState != "WAITING" || registrationCount != 1 {
		t.Fatalf(
			"partial registration epoch/state=%d/%s barrier=%d/%s registrations=%d",
			durableEpoch, residencyState, barrierGeneration, barrierState, registrationCount,
		)
	}

	secondPayload := registration(1, 0xa3, 2)
	if sessionEpoch, err := reconnect(secondPayload); err != nil || sessionEpoch != 2 {
		t.Fatalf("second multi-member reconnect epoch=%d error=%v", sessionEpoch, err)
	}
	secondReady, secondReason, err := register(secondPayload)
	if err != nil || !secondReady || !strings.Contains(secondReason, "complete") {
		t.Fatalf("second multi-member registration ready=%t reason=%q error=%v", secondReady, secondReason, err)
	}
	var localEpochs []int64
	var localEpochsJSON []byte
	if err := database.Admin.QueryRow(`
		SELECT residency.model_runtime_epoch, residency.state::text,
		       barrier.state::text,
		       jsonb_agg(registration.local_model_runtime_epoch
		                 ORDER BY registration.worker_member_id)
		FROM model_residencies AS residency
		JOIN model_runtime_barriers AS barrier
		  ON barrier.model_residency_id = residency.id
		 AND barrier.barrier_generation = residency.model_runtime_epoch
		JOIN model_runtime_epoch_registrations AS registration
		  ON registration.model_residency_id = barrier.model_residency_id
		 AND registration.barrier_generation = barrier.barrier_generation
		WHERE residency.id = '49200000-0000-0000-0000-000000000123'
		GROUP BY residency.model_runtime_epoch, residency.state, barrier.state
	`).Scan(&durableEpoch, &residencyState, &barrierState, &localEpochsJSON); err != nil {
		t.Fatalf("read ready multi-member barrier: %v", err)
	}
	if err := json.Unmarshal(localEpochsJSON, &localEpochs); err != nil {
		t.Fatalf("decode ready multi-member local epochs: %v", err)
	}
	if durableEpoch != 2 || residencyState != "READY" || barrierState != "READY" ||
		!slices.Equal(localEpochs, []int64{2, 7}) {
		t.Fatalf(
			"ready registration epoch/state=%d/%s barrier=%s local epochs=%v",
			durableEpoch, residencyState, barrierState, localEpochs,
		)
	}

	leaderEpoch, err := reconnect(registration(0, 0xa1, 3))
	if err != nil || leaderEpoch != 3 {
		t.Fatalf("leader multi-member reconnect epoch=%d error=%v", leaderEpoch, err)
	}
	nonLeaderEpoch, err := reconnect(registration(1, 0xa3, 2))
	if err != nil || nonLeaderEpoch != 3 {
		t.Fatalf("stale non-leader synchronization epoch=%d error=%v", nonLeaderEpoch, err)
	}
	for memberIndex, identityByte := range []byte{0xa1, 0xa3} {
		ready, reason, registerErr := register(registration(memberIndex, identityByte, 3))
		if registerErr != nil || !ready {
			t.Fatalf(
				"post-reconnect member %d ready=%t reason=%q error=%v",
				memberIndex,
				ready,
				reason,
				registerErr,
			)
		}
	}
	var durableControlEpoch int64
	if err := database.Admin.QueryRow(`
		SELECT worker.control_session_epoch
		FROM worker_instances AS worker
		WHERE worker.id = $1
	`, workerID).Scan(&durableControlEpoch); err != nil || durableControlEpoch != 3 {
		t.Fatalf("durable multi-member control epoch=%d error=%v", durableControlEpoch, err)
	}

	beginAcquire := func(spiffeByte byte) uuid.UUID {
		t.Helper()
		commandID := uuid.New()
		payload, err := json.Marshal(map[string]any{
			"schema_version": 1, "command_id": commandID,
			"worker_instance_id": workerID, "worker_instance_epoch": 1,
			"control_session_epoch": 3, "capacity_observation_sequence": 1,
			"model_residency_id":  "49200000-0000-0000-0000-000000000123",
			"model_runtime_epoch": 2, "stage_profile_revision_id": multiStageProfileID,
			"spiffe_id_digest": hex.EncodeToString(bytes.Repeat([]byte{spiffeByte}, 32)),
		})
		if err != nil {
			t.Fatalf("encode multi-member acquire command: %v", err)
		}
		if _, err := workerPool.Exec(
			context.Background(), "SELECT * FROM vela_begin_stage_worker_acquire($1::jsonb)", payload,
		); err != nil {
			t.Fatalf("begin multi-member acquire: %v", err)
		}
		return commandID
	}
	leaderCommandID := beginAcquire(0xa1)
	var acquireDecision, acquireReason string
	var acquireAuthority []byte
	if err := workerPool.QueryRow(context.Background(), `
		SELECT decision, reason, authority
		FROM vela_read_stage_worker_acquire_authority($1)
	`, leaderCommandID).Scan(&acquireDecision, &acquireReason, &acquireAuthority); err != nil {
		t.Fatalf("read leader multi-member acquire authority: %v", err)
	}
	var authoritySnapshot struct {
		Members []struct {
			WorkerMemberID    string `json:"worker_member_id"`
			ModelRuntimeEpoch int64  `json:"model_runtime_epoch"`
		} `json:"members"`
	}
	if err := json.Unmarshal(acquireAuthority, &authoritySnapshot); err != nil {
		t.Fatalf("decode leader multi-member acquire authority: %v", err)
	}
	if acquireDecision != "AUTHORIZED" || acquireReason == "" ||
		len(authoritySnapshot.Members) != 2 ||
		authoritySnapshot.Members[0].WorkerMemberID != members[0]["worker_member_id"] ||
		authoritySnapshot.Members[0].ModelRuntimeEpoch != 2 ||
		authoritySnapshot.Members[1].WorkerMemberID != members[1]["worker_member_id"] ||
		authoritySnapshot.Members[1].ModelRuntimeEpoch != 7 {
		t.Fatalf(
			"leader acquire decision/reason=%s/%q members=%#v",
			acquireDecision, acquireReason, authoritySnapshot.Members,
		)
	}
	nonLeaderCommandID := beginAcquire(0xa3)
	if err := workerPool.QueryRow(context.Background(), `
		SELECT decision, reason FROM vela_read_stage_worker_acquire_authority($1)
	`, nonLeaderCommandID).Scan(&acquireDecision, &acquireReason); err != nil {
		t.Fatalf("read non-leader multi-member acquire authority: %v", err)
	}
	if acquireDecision != "REJECTED" || !strings.Contains(acquireReason, "leader") {
		t.Fatalf("non-leader acquire decision/reason=%s/%q", acquireDecision, acquireReason)
	}

	duplicate := registration(0, 0xa1, 3)
	var duplicatePayload map[string]any
	if err := json.Unmarshal(duplicate, &duplicatePayload); err != nil {
		t.Fatalf("decode duplicate-member registration fixture: %v", err)
	}
	duplicatePayload["members"] = []map[string]any{members[0], members[0]}
	duplicate, err = json.Marshal(duplicatePayload)
	if err != nil {
		t.Fatalf("encode duplicate-member registration fixture: %v", err)
	}
	_, _, err = register(duplicate)
	assertPostgresConstraint(t, err, "model_runtime_epoch_registration_invalid")
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
	registerAssignedStageRuntimeBarrier(t, database, assignment)
	return signedAssignedStageAuthorityWithoutRuntimeBarrier(
		t, database, job, assignment, stageVersion,
	)
}

func signedAssignedStageAuthorityWithoutRuntimeBarrier(
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
	var memberIdentityDigest []byte
	if err := database.Admin.QueryRow(`
		SELECT residency.runtime_identity, member.id, member.member_epoch,
		       member.identity_digest,
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
		&runtimeIdentity, &memberID, &memberEpoch, &memberIdentityDigest,
		&deviceID, &deviceEpoch,
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
			IdentityDigest:    memberIdentityDigest,
		}},
		ModelResidencyId:              assignment.ModelResidencyID.String(),
		ModelRuntimeIdentity:          runtimeIdentity,
		ModelRuntimeBarrierGeneration: assignment.ModelRuntimeEpoch,
		StageProfileRevisionId:        assignment.StageProfileRevisionID.String(),
		CapacityObservationSequence:   assignment.ObservationSequence,
		CapacityVector:                assignment.CapacityVector,
		LeaseToken:                    bytes.Repeat([]byte{0xb3}, 32),
		ExecutionNonce:                append([]byte(nil), assignment.ExecutionNonce...),
		SigningKeyId:                  assignment.SigningKeyID,
		IssuedAt:                      timestamppb.New(assignment.IssuedAt),
		ExpiresAt:                     timestamppb.New(assignment.ExpiresAt),
		MonotonicValidFor:             durationpb.New(assignment.LocalDeadlineAt.Sub(assignment.IssuedAt)),
		ExecutionSpecDigest:           executionSpecDigest[:],
	}
	return signAndVerifyStageAuthority(t, envelope, assignment.IssuedAt.Add(time.Millisecond))
}

func registerAssignedStageRuntimeBarrier(
	t *testing.T,
	database testDatabase,
	assignment attemptcoordinator.AssignStageCommand,
) {
	t.Helper()
	transaction, err := database.Admin.Begin()
	if err != nil {
		t.Fatalf("begin assigned Stage runtime barrier fixture: %v", err)
	}
	defer func() { _ = transaction.Rollback() }()

	var workerID, memberID uuid.UUID
	var workerEpoch, memberEpoch, desiredMemberCount int64
	var deviceSubsetDigest, identityDigest, readinessDigest []byte
	if err := transaction.QueryRow(`
		SELECT residency.worker_instance_id, residency.worker_instance_epoch,
		       worker.desired_member_count, member.id, member.member_epoch,
		       member.device_subset_digest, member.identity_digest,
		       COALESCE(residency.canary_evidence_digest, residency.warmup_evidence_digest)
		FROM model_residencies AS residency
		JOIN worker_instances AS worker ON worker.id = residency.worker_instance_id
		JOIN worker_members AS member
		  ON member.worker_instance_id = residency.worker_instance_id
		 AND member.worker_instance_epoch = residency.worker_instance_epoch
		WHERE residency.id = $1
	`, assignment.ModelResidencyID).Scan(
		&workerID, &workerEpoch, &desiredMemberCount, &memberID, &memberEpoch,
		&deviceSubsetDigest, &identityDigest, &readinessDigest,
	); err != nil {
		t.Fatalf("read assigned Stage runtime barrier evidence: %v", err)
	}
	if workerID != assignment.WorkerInstanceID ||
		workerEpoch != assignment.WorkerInstanceEpoch || desiredMemberCount != 1 ||
		len(deviceSubsetDigest) != sha256.Size || len(identityDigest) != sha256.Size ||
		len(readinessDigest) != sha256.Size {
		t.Fatalf(
			"assigned Stage runtime barrier evidence is inconsistent: worker=%s/%d members=%d digests=%d/%d/%d",
			workerID, workerEpoch, desiredMemberCount, len(deviceSubsetDigest),
			len(identityDigest), len(readinessDigest),
		)
	}
	if _, err := transaction.Exec(`
		INSERT INTO model_runtime_barriers (
			model_residency_id, barrier_generation,
			worker_instance_id, worker_instance_epoch,
			expected_member_count, leader_worker_member_id,
			state, created_at, ready_at
		) VALUES ($1, $2, $3, $4, 1, $5, 'READY', $6, $6)
		ON CONFLICT (model_residency_id, barrier_generation) DO NOTHING
	`, assignment.ModelResidencyID, assignment.ModelRuntimeEpoch,
		workerID, workerEpoch, memberID, assignment.IssuedAt); err != nil {
		t.Fatalf("seed assigned Stage runtime barrier: %v", err)
	}
	if _, err := transaction.Exec(`
		INSERT INTO model_runtime_epoch_registrations (
			model_residency_id, barrier_generation,
			worker_instance_id, worker_instance_epoch,
			worker_member_id, worker_member_epoch,
			local_model_runtime_epoch, device_subset_digest,
			readiness_evidence_digest, spiffe_id_digest, registered_at
		) VALUES ($1, $2, $3, $4, $5, $6, $2, $7, $8, $9, $10)
		ON CONFLICT (model_residency_id, barrier_generation, worker_member_id)
		DO NOTHING
	`, assignment.ModelResidencyID, assignment.ModelRuntimeEpoch,
		workerID, workerEpoch, memberID, memberEpoch, deviceSubsetDigest,
		readinessDigest, identityDigest, assignment.IssuedAt); err != nil {
		t.Fatalf("seed assigned Stage runtime member registration: %v", err)
	}
	var exact bool
	if err := transaction.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM model_runtime_barriers AS barrier
			JOIN model_runtime_epoch_registrations AS registration
			  ON registration.model_residency_id = barrier.model_residency_id
			 AND registration.barrier_generation = barrier.barrier_generation
			WHERE barrier.model_residency_id = $1
			  AND barrier.barrier_generation = $2
			  AND barrier.worker_instance_id = $3
			  AND barrier.worker_instance_epoch = $4
			  AND barrier.expected_member_count = 1
			  AND barrier.leader_worker_member_id = $5
			  AND barrier.state = 'READY'
			  AND registration.worker_member_id = $5
			  AND registration.worker_member_epoch = $6
			  AND registration.local_model_runtime_epoch = $2
			  AND registration.device_subset_digest = $7
			  AND registration.readiness_evidence_digest = $8
			  AND registration.spiffe_id_digest = $9
		)
	`, assignment.ModelResidencyID, assignment.ModelRuntimeEpoch,
		workerID, workerEpoch, memberID, memberEpoch, deviceSubsetDigest,
		readinessDigest, identityDigest).Scan(&exact); err != nil {
		t.Fatalf("verify assigned Stage runtime barrier fixture: %v", err)
	}
	if !exact {
		t.Fatal("assigned Stage runtime barrier fixture does not match durable Fleet evidence")
	}
	if err := transaction.Commit(); err != nil {
		t.Fatalf("commit assigned Stage runtime barrier fixture: %v", err)
	}
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
