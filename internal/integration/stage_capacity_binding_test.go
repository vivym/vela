//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRunningStageSurvivesCapacityObservationExpiry(t *testing.T) {
	for _, mode := range []string{"expired", "superseded", "pruned"} {
		t.Run(mode, func(t *testing.T) { verifyStageCapacityBinding(t, mode) })
	}
}

func TestStageSealUsesRenewedLeaseExpiry(t *testing.T) {
	verifyStageCapacityBinding(t, "renewed-lease")
}

func verifyStageCapacityBinding(t *testing.T, mode string) {
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
	initialExpiry := time.Now().Add(time.Hour)
	if mode == "renewed-lease" {
		initialExpiry = time.Now().Add(2 * time.Second)
	}
	assignment := assignEncoder(
		t, database, coordinator, attemptID, encoderRunID, initialExpiry,
	)
	assigned := signedAssignedStageAuthority(t, database, job, assignment, 2)
	// Expiry and supersession do not change the execution lease or reservation.
	if mode != "superseded" {
		if _, err := database.Admin.Exec(`UPDATE capacity_observations
 SET observed_at=clock_timestamp()-interval '2 minutes', expires_at=clock_timestamp()-interval '1 minute'
 WHERE worker_instance_id=$1`, assignment.WorkerInstanceID); err != nil {
			t.Fatal(err)
		}

	}
	if mode != "expired" {
		if _, err := database.Admin.Exec(`INSERT INTO capacity_observations
   (worker_instance_id,worker_instance_epoch,observation_sequence,capacity_vector,observed_at,expires_at,observed_by)
   SELECT worker_instance_id,worker_instance_epoch,observation_sequence+1,capacity_vector,
    clock_timestamp(),clock_timestamp()+interval '2 minutes',observed_by
   FROM capacity_observations WHERE worker_instance_id=$1 AND observation_sequence=$2`,
			assignment.WorkerInstanceID, assignment.ObservationSequence); err != nil {
			t.Fatal(err)
		}
	}
	if mode == "pruned" {
		var n int
		if err := database.Admin.QueryRow(`SELECT vela_prune_worker_capacity_observations($1)`, assignment.WorkerInstanceID).Scan(&n); err != nil || n != 1 {
			t.Fatalf("prune original observation: count=%d error=%v", n, err)
		}
	}
	var fresh bool
	if err := database.Admin.QueryRow(`SELECT vela_lock_exact_capacity_observation($1,$2,$3,$4::jsonb)`,
		assignment.WorkerInstanceID, assignment.WorkerInstanceEpoch, assignment.ObservationSequence,
		capacityBindingJSON(t, assignment.CapacityVector)).Scan(&fresh); err != nil || fresh {
		t.Fatalf("new assignment accepted stale capacity: fresh=%t error=%v", fresh, err)
	}
	if _, err := database.Admin.Exec(`UPDATE stage_allocations SET capacity_observation_sequence=capacity_observation_sequence+1 WHERE id=$1`, assignment.StageAllocationID); err == nil {
		t.Fatal("allocation capacity binding was mutable")
	}
	startedEnvelope := proto.Clone(assigned.Authority).(*velav1.StageAuthority)
	startedEnvelope.StageVersion = 3
	startedEnvelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(10 * time.Millisecond))
	startedEnvelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(2 * time.Minute))
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
	heartbeatEnvelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(3 * time.Minute))
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

	var capacityOK bool
	if err := workerPool.QueryRow(context.Background(), `SELECT capacity_observation_active
 FROM vela_read_stage_authority_snapshot($1,$2)`, assignment.StageLeaseID, assigned.Authority.CapacityObservationSequence).Scan(&capacityOK); err != nil {
		t.Fatal(err)
	}
	if !capacityOK {
		t.Fatal("running allocation lost its capacity binding when the scheduling observation expired")
	}

	for _, mutation := range []struct {
		name  string
		value any
	}{
		{"capacity_observation_sequence", assignment.ObservationSequence + 1},
		{"capacity_vector", map[string]int64{"concurrency": 99}},
		{"stage_allocation_id", uuid.New().String()},
		{"worker_instance_epoch", assignment.WorkerInstanceEpoch + 1},
	} {
		var altered map[string]any
		if err := json.Unmarshal(heartbeatPayload, &altered); err != nil {
			t.Fatal(err)
		}
		altered[mutation.name] = mutation.value
		altered["command_id"] = uuid.New().String()
		if _, err := workerPool.Exec(context.Background(), `SELECT * FROM vela_heartbeat_stage_worker_command($1::jsonb)`, capacityBindingJSON(t, altered)); err == nil {
			t.Fatalf("accepted mutated heartbeat field %s", mutation.name)
		}
	}
	var wrongSequence bool
	if err := workerPool.QueryRow(context.Background(), `SELECT capacity_observation_active FROM vela_read_stage_authority_snapshot($1,$2)`, assignment.StageLeaseID, assignment.ObservationSequence+1).Scan(&wrongSequence); err != nil || wrongSequence {
		t.Fatalf("snapshot accepted wrong capacity sequence: %t %v", wrongSequence, err)
	}
	var returnedWire []byte
	if err := workerPool.QueryRow(context.Background(), `SELECT renewed_authority
 FROM vela_heartbeat_stage_worker_command($1::jsonb)`, heartbeatPayload).Scan(&returnedWire); err != nil {
		t.Fatalf("heartbeat after capacity expiry: %v", err)
	}
	if !bytes.Equal(returnedWire, heartbeatWire) {
		t.Fatal("unexpected renewed authority")
	}
	authorizer, err := stageworkercontrol.NewPostgresAuthorizer(workerPool)
	if err != nil {
		t.Fatal(err)
	}
	identity := stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String()}
	for _, op := range []stageworkercontrol.Operation{stageworkercontrol.OperationHeartbeatStage, stageworkercontrol.OperationSealStageOutput} {
		active, err := authorizer.IsActive(context.Background(), identity, 1, op, heartbeat)
		if err != nil || !active {
			t.Fatalf("operation %v rejected after capacity expiry: %t %v", op, active, err)
		}
	}
	reattachments, err := stageworkercontrol.NewPostgresReattachmentBackend(workerPool)
	if err != nil {
		t.Fatal(err)
	}
	result, err := reattachments.ReattachStage(context.Background(), stageworkercontrol.CommandContext{
		CommandID: uuid.New(), Identity: identity, ControlSessionEpoch: 1,
	}, &velav1.ReattachStageRequest{Authority: heartbeat.Authority, ObservedRuntimeState: velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING},
		stageworkercontrol.VerifiedAuthorities{Stage: &heartbeat})
	if err != nil || result.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
		t.Fatalf("reattach after capacity expiry: %#v %v", result, err)
	}
	repository, err := stageartifact.NewPostgresRepository(newRolePool(t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password"))
	if err != nil {
		t.Fatal(err)
	}
	if mode == "renewed-lease" {
		if delay := time.Until(assignment.ExpiresAt.Add(10 * time.Millisecond)); delay > 0 {
			time.Sleep(delay)
		}
	}
	now := time.Now().UTC().Truncate(time.Millisecond)
	digest := sha256.Sum256([]byte("capacity expiry sealed output"))
	if _, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: encoderRunID, StageAttemptID: assignment.StageAttemptID,
		StageAllocationID: assignment.StageAllocationID, StageLeaseID: assignment.StageLeaseID,
		ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "capacity-binding-output", LocalReceiptDigest: digest, ManifestSHA256: digest, SHA256: digest, LineageDigest: digest, TokenDigest: digest,
		SizeBytes: 64, ArtifactID: uuid.New(), MaterializationLeaseID: uuid.New(),
		ObjectKey:   "artifacts/stage/org/project/" + attemptID.String() + "/encoder/capacity-output.bin",
		ContentType: "application/octet-stream", SealedAt: now, LeaseExpiresAt: now.Add(30 * time.Minute),
	}); err != nil {
		t.Fatalf("seal after capacity expiry: %v", err)
	}
	if _, err := workerPool.Exec(context.Background(), `SELECT * FROM vela_heartbeat_stage_worker_command($1::jsonb)`,
		stageWorkerHeartbeatPayload(t, uuid.New(), assignment, started, heartbeat, heartbeatWire, 2, assignment.IssuedAt.Add(20*time.Millisecond))); err == nil {
		t.Fatal("released allocation accepted another heartbeat")
	}
	if err := goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), 105); err == nil {
		t.Fatal("downgrade discarded committed capacity identity")
	}
}

func capacityBindingJSON(t *testing.T, value any) []byte {
	t.Helper()
	wire, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestStageCapacityBindingMigrationPreservesFunctionPrivileges(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 105)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	identity := func() string {
		t.Helper()
		var result string
		if err := database.Admin.QueryRow(`SELECT jsonb_agg(jsonb_build_array(oid,proowner,proacl) ORDER BY oid)::text FROM pg_proc
   WHERE proname IN ('vela_apply_stage_command','vela_read_stage_authority_snapshot','vela_start_stage_worker_command',
   'vela_heartbeat_stage_worker_command','vela_reattach_stage_worker_command','vela_read_stage_assignment_execution_v1')`).Scan(&result); err != nil {
			t.Fatal(err)
		}
		return result
	}
	want := identity()
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 106); err != nil {
			t.Fatal(err)
		}
		if got := identity(); got != want {
			t.Fatal("function identity or privileges changed")
		}
		var exposed bool
		if err := database.Admin.QueryRow(`SELECT EXISTS(SELECT 1 FROM pg_roles WHERE rolcanlogin AND NOT rolsuper AND
   has_function_privilege(oid,'vela_worker_instance_execution_identity_matches(uuid,bigint,bytea,bytea,uuid,bigint)','EXECUTE'))`).Scan(&exposed); err != nil || exposed {
			t.Fatalf("internal execution predicate exposed=%t error=%v", exposed, err)
		}
		if err := goose.DownTo(database.Admin, migrations, 105); err != nil {
			t.Fatal(err)
		}
		if got := identity(); got != want {
			t.Fatal("rollback changed function identity or privileges")
		}
	}
}

func TestStageCapacityBindingMigrationRequiresNoActiveAllocation(t *testing.T) {
	database, _, coordinator, _, attemptID, encoderRunID, _ := newStageGraphCancellationFixture(t, "capacity-binding-quiescence")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 105); err != nil {
		t.Fatal(err)
	}
	assignEncoder(t, database, coordinator, attemptID, encoderRunID, time.Now().Add(time.Hour))
	if err := goose.UpTo(database.Admin, migrations, 106); err == nil {
		t.Fatal("migration accepted an unbound active allocation")
	}
	var version int
	if err := database.Admin.QueryRow(`SELECT max(version_id) FROM goose_db_version WHERE is_applied`).Scan(&version); err != nil || version != 105 {
		t.Fatalf("failed migration changed schema version: %d %v", version, err)
	}
}

func TestStageSealRejectsExpiredLeaseWithBackdatedReceipt(t *testing.T) {
	database, _, coordinator, _, attemptID, runID, _ := newStageGraphCancellationFixture(t, "stage-seal-expired-server-clock")
	assignment := assignAndStartEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(2*time.Second))
	repository, err := stageartifact.NewPostgresRepository(newRolePool(t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password"))
	if err != nil {
		t.Fatal(err)
	}
	if delay := time.Until(assignment.ExpiresAt.Add(10 * time.Millisecond)); delay > 0 {
		time.Sleep(delay)
	}
	digest := sha256.Sum256([]byte("expired backdated receipt"))
	_, err = repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: runID, StageAttemptID: assignment.StageAttemptID,
		StageAllocationID: assignment.StageAllocationID, StageLeaseID: assignment.StageLeaseID,
		ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 3, OutputPort: "conditioning",
		LocalReceiptID: "backdated-receipt", LocalReceiptDigest: digest, ManifestSHA256: digest, SHA256: digest, LineageDigest: digest, TokenDigest: digest,
		SizeBytes: 64, ArtifactID: uuid.New(), MaterializationLeaseID: uuid.New(),
		ObjectKey:   "artifacts/stage/org/project/" + attemptID.String() + "/encoder/backdated-output.bin",
		ContentType: "application/octet-stream", SealedAt: assignment.IssuedAt.Add(100 * time.Millisecond), LeaseExpiresAt: time.Now().Add(30 * time.Minute),
	})
	assertPostgresConstraint(t, err, "stage_output_seal_authority_stale")
	var state string
	if err := database.Admin.QueryRow(`SELECT state::text FROM stage_allocations WHERE id=$1`, assignment.StageAllocationID).Scan(&state); err != nil || state != "ALLOCATED" {
		t.Fatalf("expired receipt released execution capacity: %s %v", state, err)
	}
}
