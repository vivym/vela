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
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagescheduler"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestStageWorkerAcquireReplayAcrossIdleProbeUpgradeAndRollback(t *testing.T) {
	for _, initialVersion := range []int64{77, 78} {
		for _, oldClientFirst := range []bool{false, true} {
			t.Run(fmt.Sprintf("schema-%d/old-first-%t", initialVersion, oldClientFirst), func(t *testing.T) {
				fixture := newStageSchedulerFixture(t, "idle-upgrade")
				migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
				if err := goose.DownTo(fixture.database.Admin, migrations, initialVersion); err != nil {
					t.Fatal(err)
				}
				capture := &legacyAcquireReplayScheduler{AssignmentScheduler: newStageSchedulerTestService(t, fixture)}
				backend := newPostgresAssignmentTestBackendWithScheduler(t, fixture, capture)
				command := stageWorkerAcquireCommand(fixture)
				request := stageWorkerAcquireRequest(fixture)
				digest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
				payload, err := json.Marshal(map[string]any{
					"schema_version": 1, "command_id": command.CommandID,
					"worker_instance_id":            request.GetWorkerInstanceId(),
					"worker_instance_epoch":         request.GetWorkerInstanceEpoch(),
					"control_session_epoch":         command.ControlSessionEpoch,
					"capacity_observation_sequence": request.GetCapacityObservationSequence(),
					"model_residency_id":            request.GetModelResidencyId(),
					"model_runtime_epoch":           request.GetModelRuntimeEpoch(),
					"stage_profile_revision_id":     request.GetStageProfileRevisionId(),
					"spiffe_id_digest":              hex.EncodeToString(digest[:]),
				})
				if err != nil {
					t.Fatal(err)
				}
				oldBegin := func() []byte {
					t.Helper()
					var wire []byte
					if err := fixture.database.Admin.QueryRow(`
						SELECT assignment_wire FROM vela_begin_stage_worker_acquire($1::jsonb)
					`, payload).Scan(&wire); err != nil {
						t.Fatalf("old client begin/replay: %v", err)
					}
					return wire
				}
				if oldClientFirst {
					if wire := oldBegin(); wire != nil {
						t.Fatal("unfinished old-client command already has an assignment")
					}
				}
				rejectLegacyAcquireProduction(t, fixture, backend, capture, command, request)
				assigned := completeLegacyAcquireReplayFixture(t, fixture, command, capture.assignment)
				originalWire := oldBegin()
				for _, version := range []int64{78, 77, 78} {
					current, versionErr := goose.GetDBVersion(fixture.database.Admin)
					if versionErr != nil {
						t.Fatal(versionErr)
					}
					if current > version {
						err = goose.DownTo(fixture.database.Admin, migrations, version)
					} else {
						err = goose.UpTo(fixture.database.Admin, migrations, version)
					}
					if err != nil {
						t.Fatal(err)
					}
					replayed, replayErr := backend.AcquireStage(context.Background(), command, request)
					if replayErr != nil || !proto.Equal(replayed.Assignment, assigned) {
						t.Fatalf("schema %d new client replay: result=%#v error=%v", version, replayed, replayErr)
					}
					replayedWire, replayErr := proto.MarshalOptions{Deterministic: true}.Marshal(replayed.Assignment)
					if replayErr != nil || !bytes.Equal(replayedWire, originalWire) {
						t.Fatalf("schema %d current Backend did not preserve historical wire bytes: %v", version, replayErr)
					}
					if wire := oldBegin(); !bytes.Equal(originalWire, wire) {
						t.Fatalf("schema %d old-client wire changed after migration", version)
					}
				}
			})
		}
	}
}

type legacyAcquireReplayScheduler struct {
	stageworkercontrol.AssignmentScheduler
	assignment stagescheduler.Assignment
}

func (capture *legacyAcquireReplayScheduler) AcquireIdentified(
	ctx context.Context,
	authority stagescheduler.WorkerAuthority,
	observation stagescheduler.CapacityObservation,
	identity stagescheduler.AssignmentIdentity,
) (stagescheduler.Assignment, bool, error) {
	assignment, assigned, err := capture.AssignmentScheduler.AcquireIdentified(ctx, authority, observation, identity)
	if err == nil && assigned {
		capture.assignment = assignment
	}
	return assignment, assigned, err
}

func rejectLegacyAcquireProduction(
	t *testing.T,
	fixture stageSchedulerFixture,
	backend *stageworkercontrol.PostgresAssignmentBackend,
	capture *legacyAcquireReplayScheduler,
	command stageworkercontrol.CommandContext,
	request *velav1.AcquireStageRequest,
) {
	t.Helper()
	result, err := backend.AcquireStage(context.Background(), command, request)
	if err == nil || !strings.Contains(err.Error(), "durable StageAssignment execution snapshot is incomplete") ||
		result.Assignment != nil || capture.assignment.StageAllocationID == uuid.Nil {
		t.Fatalf("new producer on unsequenced schema: result=%#v allocation=%s error=%v", result, capture.assignment.StageAllocationID, err)
	}
	var completed int
	var sequenced bool
	if queryErr := fixture.database.Admin.QueryRow(`SELECT
		(SELECT count(*) FROM stage_worker_acquire_results WHERE command_id = $1),
		COALESCE((to_jsonb(allocation)->>'execution_sequence')::bigint, 0) > 0
		FROM stage_allocations allocation WHERE allocation.id = $2`, command.CommandID, capture.assignment.StageAllocationID).
		Scan(&completed, &sequenced); queryErr != nil {
		t.Fatal(queryErr)
	}
	if completed != 0 || sequenced {
		t.Fatalf("new producer manufactured historical authority: results=%d sequenced=%t", completed, sequenced)
	}
	t.Logf("new producer failed closed before signing or returning legacy work: %v", err)
}

// This signed V1 fixture models a historical response for wire replay only.
// It is never offered to ModelRuntime as a newly executable assignment.
func completeLegacyAcquireReplayFixture(
	t *testing.T,
	fixture stageSchedulerFixture,
	command stageworkercontrol.CommandContext,
	scheduled stagescheduler.Assignment,
) *velav1.StageAssignment {
	t.Helper()
	assignment := attemptcoordinator.AssignStageCommand{
		StageRunID: scheduled.StageRunID, StageAttemptID: scheduled.StageAttemptID,
		StageAllocationID: scheduled.StageAllocationID, StageLeaseID: scheduled.StageLeaseID,
		WorkerInstanceID: fixture.authority.WorkerInstanceID, WorkerInstanceEpoch: fixture.authority.WorkerInstanceEpoch,
		StageProfileRevisionID: fixture.authority.StageProfileRevisionID,
		ModelResidencyID:       fixture.authority.ModelResidencyID, ModelRuntimeEpoch: fixture.authority.ModelRuntimeEpoch,
		DeviceSetDigest: fixture.authority.DeviceSetDigest, MembershipDigest: fixture.authority.MembershipDigest,
		ObservationSequence: fixture.observation.Sequence, CapacityVector: fixture.authority.CapacityVector,
	}
	var job jobResponse
	var stageVersion int64
	if err := fixture.database.Admin.QueryRow(`SELECT attempt.job_id::text, lease.attempt_id, lease.execution_nonce,
		lease.issued_at, lease.expires_at, lease.local_deadline_at, lease.signing_key_id, run.version
		FROM stage_leases lease JOIN attempts attempt ON attempt.id = lease.attempt_id
		JOIN stage_runs run ON run.id = lease.stage_run_id WHERE lease.id = $1`, scheduled.StageLeaseID).
		Scan(&job.JobID, &assignment.AttemptID, &assignment.ExecutionNonce, &assignment.IssuedAt,
			&assignment.ExpiresAt, &assignment.LocalDeadlineAt, &assignment.SigningKeyID, &stageVersion); err != nil {
		t.Fatal(err)
	}
	verified := signedAssignedStageAuthorityWithoutRuntimeBarrier(t, fixture.database, job, assignment, stageVersion)
	envelope := proto.Clone(verified.Authority).(*velav1.StageAuthority)
	if envelope.GetSchemaVersion() != stageauthority.SchemaVersionV1 || envelope.GetExecutionSequence() != 0 {
		t.Fatal("historical replay fixture must retain unsequenced V1 authority")
	}
	envelope.LeaseToken = bytes.Clone(scheduled.LeaseToken[:])
	envelope.Signature = nil
	verified = signAndVerifyStageAuthority(t, envelope, assignment.IssuedAt.Add(time.Millisecond))
	legacy := &velav1.StageAssignment{
		Authority: verified.Authority, ExecutionSpec: &velav1.StageExecutionSpec{},
		MemberStartTimeout: durationpb.New(30 * time.Second),
	}
	for _, member := range verified.Authority.GetMembers() {
		legacy.RequiredWorkerMemberIds = append(legacy.RequiredWorkerMemberIds, member.GetWorkerMemberId())
	}
	if _, err := stageassignment.Validate(legacy); err != nil {
		t.Fatalf("historical signed assignment contract: %v", err)
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(map[string]any{
		"schema_version": 1, "command_id": command.CommandID, "result_kind": "ASSIGNMENT",
		"assignment_wire": hex.EncodeToString(wire), "retry_after_ms": nil, "detail": nil,
	})
	if err != nil {
		t.Fatal(err)
	}
	pool := newRolePool(t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	var durableWire []byte
	if err := pool.QueryRow(context.Background(), `SELECT assignment_wire FROM vela_complete_stage_worker_acquire($1::jsonb)`, payload).
		Scan(&durableWire); err != nil || !bytes.Equal(durableWire, wire) {
		t.Fatalf("persist historical wire through acquire completion: error=%v exact=%t", err, bytes.Equal(durableWire, wire))
	}
	return legacy
}

func TestIdleStageWorkerPollsDoNotAppendDurableHistory(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "idle-history")
	if _, err := fixture.database.Admin.Exec(`
		UPDATE credentials SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel'] WHERE id = $1
	`, testCredentialID); err != nil {
		t.Fatal(err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	var jobID string
	if err := fixture.database.Admin.QueryRow(`
		SELECT attempt.job_id FROM stage_runs AS run
		JOIN attempts AS attempt ON attempt.id = run.attempt_id WHERE run.id = $1
	`, fixture.stageRunID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	if response := cancelJob(t, server.URL, testProjectID, jobID, testBearerCredential()); response.StatusCode != http.StatusOK {
		t.Fatalf("cancel unstarted work: %s", response.Body)
	}
	countHistory := func() int {
		t.Helper()
		var count int
		if err := fixture.database.Admin.QueryRow(`
			SELECT (SELECT count(*) FROM stage_worker_acquire_intents)
			     + (SELECT count(*) FROM stage_worker_acquire_results)
			     + (SELECT count(*) FROM stage_scheduler_snapshot_traces)
		`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	before := countHistory()
	backend := newPostgresAssignmentTestBackend(t, fixture)
	request := stageWorkerAcquireRequest(fixture)
	started := time.Now()
	for range 64 {
		result, err := backend.AcquireStage(context.Background(), stageWorkerAcquireCommand(fixture), request)
		if err != nil || result.Assignment != nil || result.Command != nil || result.RetryAfter != 250*time.Millisecond {
			t.Fatalf("idle result=%#v error=%v", result, err)
		}
	}
	after := countHistory()
	t.Logf("64 idle polls: history rows %d -> %d, elapsed %s", before, after, time.Since(started))
	if after != before {
		t.Fatalf("idle polling appended %d authority history rows", after-before)
	}
	command := stageWorkerAcquireCommand(fixture)
	if _, err := backend.AcquireStage(context.Background(), command, request); err != nil {
		t.Fatal(err)
	}
	newJob, _ := instantiateH3IntegrationGraph(t, fixture.database, server.URL, "idle-new-work")
	assigned, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || assigned.Assignment == nil || assigned.Assignment.GetAuthority().GetJobId() != newJob.JobID {
		t.Fatalf("work arriving after advisory NoWork: result=%#v error=%v", assigned, err)
	}
	replayed, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || !proto.Equal(replayed.Assignment, assigned.Assignment) {
		t.Fatalf("assignment acquired after idle must replay exactly: result=%#v error=%v", replayed, err)
	}
}

func TestIdleStageWorkerProbeMigrationEmptyDownUp(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(database.Admin, migrations, 77); err != nil {
		t.Fatal(err)
	}
	var removed, beginAccessible, readerAccessible bool
	if err := database.Admin.QueryRow(`
		SELECT to_regprocedure('vela_stage_worker_acquire_queue_empty(jsonb)') IS NULL,
		       has_function_privilege('vela_stage_worker_control', 'vela_begin_stage_worker_acquire(jsonb)', 'EXECUTE'),
		       has_function_privilege('vela_stage_worker_control', 'vela_read_stage_worker_acquire_authority(uuid)', 'EXECUTE')
	`).Scan(&removed, &beginAccessible, &readerAccessible); err != nil {
		t.Fatal(err)
	}
	if !removed || !beginAccessible || !readerAccessible {
		t.Fatal("rollback did not restore the prior acquisition API")
	}
	if err := goose.UpTo(database.Admin, migrations, 78); err != nil {
		t.Fatal(err)
	}
	var probeExposed bool
	if err := database.Admin.QueryRow(`
		SELECT has_function_privilege('vela_stage_worker_control',
		       'vela_stage_worker_acquire_queue_empty(jsonb)', 'EXECUTE')
	`).Scan(&probeExposed); err != nil {
		t.Fatal(err)
	}
	if probeExposed {
		t.Fatal("internal idle probe added an externally callable authority surface")
	}
}
