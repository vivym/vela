//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageWorkerControlFailureHandlerAcknowledgesDurableRetryAndTerminal(t *testing.T) {
	for _, wantState := range []string{"RETRY_WAIT", "RETRY_WAIT_V86", "FAILED", "LOCK_WAIT", "LOCK_WAIT_V85", "NO_RECEIPT"} {
		t.Run(wantState, func(t *testing.T) {
			database, _, coordinator, job, attemptID, runID, _ := newStageGraphCancellationFixture(t, "fail-handler-"+wantState)
			historicalReplay := wantState == "RETRY_WAIT_V86"
			var historicalSchema int64
			if historicalReplay {
				historicalSchema = 86
			} else if wantState == "LOCK_WAIT_V85" {
				historicalSchema = 85
			}
			if historicalSchema != 0 {
				// Historical V1 work must be allocated before execution sequences exist.
				if err := goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), historicalSchema); err != nil {
					t.Fatal(err)
				}
			}
			assignment := assignEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(2*time.Second))
			renewStageExpiryFixture(t, database, job, assignment)
			var wire []byte
			if err := database.Admin.QueryRow(`SELECT renewed_authority FROM stage_authority_renewals
				WHERE stage_lease_id = $1 ORDER BY issued_at DESC LIMIT 1`, assignment.StageLeaseID).Scan(&wire); err != nil {
				t.Fatal(err)
			}
			envelope := &velav1.StageAuthority{}
			if err := proto.Unmarshal(wire, envelope); err != nil {
				t.Fatal(err)
			}
			if historicalSchema != 0 && (envelope.GetSchemaVersion() != stageauthority.SchemaVersionV1 || envelope.GetExecutionSequence() != 0) {
				t.Fatal("historical schema fixture must use unsequenced V1 authority")
			}
			repository, err := stageartifact.NewPostgresRepository(newRolePool(t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password"))
			if err != nil {
				t.Fatal(err)
			}
			unused := unusedMaterializationReplayDependencies{}
			backend, err := stageworkercontrol.NewPostgresOperationBackend(stageworkercontrol.PostgresOperationConfig{
				WorkerEvidence: unused, Assignments: unused, Execution: unused, MaterializationIssuer: unused,
				StageArtifacts: repository, StageAttempts: coordinator, Reattachments: unused, Transfers: unused,
			})
			if err != nil {
				t.Fatal(err)
			}
			executor, err := stageworkercontrol.NewProductionExecutor(backend)
			if err != nil {
				t.Fatal(err)
			}
			keys := map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}
			clockOffset := time.Duration(0)
			validator, err := stageauthority.NewValidator(keys, func() time.Time { return time.Now().Add(clockOffset) })
			if err != nil {
				t.Fatal(err)
			}
			authority, err := validator.ValidateEnvelopeWithClockSkew(envelope, 100*time.Millisecond)
			if err != nil {
				t.Fatal(err)
			}
			materializationValidator, err := materializationauthority.NewValidator(keys, nil)
			if err != nil {
				t.Fatal(err)
			}
			authorizer, err := stageworkercontrol.NewPostgresAuthorizer(newRolePool(t, database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password"))
			if err != nil {
				t.Fatal(err)
			}
			handler, err := stageworkercontrol.NewHandler(stageworkercontrol.Config{
				Validator: validator, Authorizer: authorizer, MaterializationValidator: materializationValidator,
				MaterializationAuthorizer: repository, Executor: executor,
				MaxClockSkew: 100 * time.Millisecond,
			})
			if err != nil {
				t.Fatal(err)
			}
			identity := stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String()}
			fingerprint := sha256.Sum256([]byte("handler retryable execution failure"))
			units := int64(1)
			var failureClass string
			if err := database.Admin.QueryRow(`SELECT execution_retryable_failure_classes[1] FROM jobs WHERE id = $1`, job.JobID).Scan(&failureClass); err != nil {
				t.Fatal(err)
			}
			if wantState == "FAILED" {
				if err := database.Admin.QueryRow(`SELECT max_resource_units FROM attempt_retry_budgets WHERE attempt_id = $1`, attemptID).Scan(&units); err != nil {
					t.Fatal(err)
				}
			}
			request := &velav1.StageWorkerControlServiceConnectRequest{
				RequestId: uuid.NewString(), Operation: &velav1.StageWorkerControlServiceConnectRequest_FailStage{
					FailStage: &velav1.FailStageRequest{Authority: authority.Authority, FailureClass: failureClass,
						FailureFingerprint: fingerprint[:], ConsumedResourceUnits: units,
						FailedAt: timestamppb.New(envelope.GetIssuedAt().AsTime().Add(2 * time.Millisecond)), RetryAt: timestamppb.New(assignment.IssuedAt.Add(time.Second))},
				},
			}
			before := (&materializationReplayFixture{database: database}).snapshot(t)
			clockOffset = -time.Minute
			(&materializationReplayFixture{handler: handler}).reject(t, request, identity, 1)
			clockOffset = time.Hour
			(&materializationReplayFixture{handler: handler}).reject(t, request, identity, 1)
			clockOffset = 0
			(&materializationReplayFixture{database: database}).unchanged(t, before)
			if wantState == "NO_RECEIPT" {
				tx, err := database.Admin.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
					t.Fatal(err)
				}
				if _, err := tx.Exec(`
					WITH revoked AS (
						UPDATE stage_leases SET state = 'REVOKED', revoked_at = clock_timestamp(), revoke_reason = 'STAGE_FAILED' WHERE id = $1 RETURNING id
					), failed AS (
						UPDATE stage_attempts SET state = 'FAILED', ended_at = clock_timestamp() WHERE id = $2 RETURNING id
					)
					UPDATE stage_allocations SET state = 'RELEASED', released_at = clock_timestamp(), release_reason = 'STAGE_FAILED' WHERE id = $3
				`, assignment.StageLeaseID, assignment.StageAttemptID, assignment.StageAllocationID); err != nil {
					t.Fatal(err)
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				before = (&materializationReplayFixture{database: database}).snapshot(t)
				(&materializationReplayFixture{handler: handler}).reject(t, request, identity, 1)
				(&materializationReplayFixture{database: database}).unchanged(t, before)
				return
			}
			if wantState == "LOCK_WAIT" || wantState == "LOCK_WAIT_V85" {
				tx, err := database.Admin.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer tx.Rollback()
				if _, err := tx.Exec(`SELECT id FROM jobs WHERE id = $1 FOR UPDATE`, job.JobID); err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				type result struct {
					response *velav1.StageWorkerControlServiceConnectResponse
					err      error
				}
				completed := make(chan result, 1)
				go func() { response, err := handler.Handle(ctx, identity, 1, request); completed <- result{response, err} }()
				for {
					var waiting bool
					if err := database.Admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
						WHERE usename = 'vela_attempt_coordinator_login' AND wait_event_type = 'Lock')`).Scan(&waiting); err != nil {
						t.Fatal(err)
					}
					if waiting {
						break
					}
					if time.Now().After(envelope.GetIssuedAt().AsTime().Add(envelope.GetMonotonicValidFor().AsDuration())) {
						t.Fatal("FAIL did not enter lock wait before deadline")
					}
					time.Sleep(10 * time.Millisecond)
				}
				waitStageExpiry(t, envelope.GetExpiresAt().AsTime())
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				got := <-completed
				wantDecision := velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE
				if wantState == "LOCK_WAIT_V85" {
					wantDecision = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED
				}
				if got.err != nil || got.response.GetStageCommandResult().GetDecision() != wantDecision {
					t.Fatalf("lock-wait response=%v error=%v want=%s", got.response, got.err, wantDecision)
				}
				if wantState == "LOCK_WAIT" {
					(&materializationReplayFixture{database: database}).unchanged(t, before)
				}
				t.Logf("post-lock deadline evidence: %s returned %s", wantState, wantDecision)
				return
			}
			response, err := handler.Handle(context.Background(), identity, 1, request)
			if err != nil || response.GetStageCommandResult().GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED {
				t.Fatalf("first failure response=%v error=%v", response, err)
			}
			var state string
			expectedState := wantState
			if historicalReplay {
				expectedState = "RETRY_WAIT"
			}
			if err := database.Admin.QueryRow(`SELECT state::text FROM stage_runs WHERE id = $1`, runID).Scan(&state); err != nil || state != expectedState {
				t.Fatalf("state=%s want=%s err=%v", state, expectedState, err)
			}
			snapshot := (&materializationReplayFixture{database: database}).snapshot(t)
			response, err = handler.Handle(context.Background(), identity, 1, request)
			if err != nil || response.GetStageCommandResult().GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
				t.Fatalf("immediate failure replay response=%v error=%v", response, err)
			}
			if !bytes.Equal(response.GetStageCommandResult().GetAuthorityDigest(), authority.Digest[:]) {
				t.Fatal("failure replay changed authority digest")
			}
			waitStageExpiry(t, envelope.GetExpiresAt().AsTime())
			response, err = handler.Handle(context.Background(), identity, 1, request)
			if err != nil || response.GetStageCommandResult().GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
				t.Fatalf("expired failure replay response=%v error=%v", response, err)
			}
			(&materializationReplayFixture{database: database}).unchanged(t, snapshot)
			if historicalReplay {
				migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
				if err := goose.DownTo(database.Admin, migrations, 85); err != nil {
					t.Fatal(err)
				}
				(&materializationReplayFixture{handler: handler}).reject(t, request, identity, 1)
				if err := goose.UpTo(database.Admin, migrations, 86); err != nil {
					t.Fatal(err)
				}
				response, err = handler.Handle(context.Background(), identity, 1, request)
				if err != nil || response.GetStageCommandResult().GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
					t.Fatalf("schema86 replay=%v error=%v", response, err)
				}
				(&materializationReplayFixture{database: database}).unchanged(t, snapshot)
			}
			for name, mutate := range map[string]func(*velav1.StageWorkerControlServiceConnectRequest){
				"signature": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetFailStage().Authority.Signature[0] ^= 0xff
				},
				"detail": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetFailStage().Detail = "changed diagnostic"
				},
				"worker reusable": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetFailStage().WorkerReusable = !request.GetFailStage().WorkerReusable
				},
				"command ID": func(request *velav1.StageWorkerControlServiceConnectRequest) { request.RequestId = uuid.NewString() },
				"failure class": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetFailStage().FailureClass = "OTHER_FAILURE"
				},
				"failure class whitespace": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetFailStage().FailureClass += " "
				},
				"fingerprint": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetFailStage().FailureFingerprint[0] ^= 0xff
				},
				"units": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetFailStage().ConsumedResourceUnits++
				},
				"failed at": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetFailStage().FailedAt = timestamppb.New(envelope.GetIssuedAt().AsTime().Add(3 * time.Millisecond))
				},
				"retry at": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetFailStage().RetryAt = timestamppb.New(assignment.IssuedAt.Add(2 * time.Second))
				},
			} {
				t.Run(name, func(t *testing.T) {
					changed := proto.Clone(request).(*velav1.StageWorkerControlServiceConnectRequest)
					mutate(changed)
					(&materializationReplayFixture{handler: handler}).reject(t, changed, identity, 1)
					(&materializationReplayFixture{database: database}).unchanged(t, snapshot)
				})
			}
			(&materializationReplayFixture{handler: handler}).reject(t, request, stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/" + uuid.NewString()}, 1)
			fleetPool := newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password")
			var session int64
			if err := fleetPool.QueryRow(context.Background(), `SELECT control_session_epoch FROM vela_reconnect_worker_instance(
				$1, 1, 1, 'failure-replay-session-2', clock_timestamp(), 'worker-agent/h3-node-01')`, assignment.WorkerInstanceID).Scan(&session); err != nil || session != 2 {
				t.Fatalf("reconnect session=%d error=%v", session, err)
			}
			(&materializationReplayFixture{handler: handler}).reject(t, request, identity, 1)
			response, err = handler.Handle(context.Background(), identity, session, request)
			if err != nil || response.GetStageCommandResult().GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
				t.Fatalf("reconnect replay=%v error=%v", response, err)
			}
			(&materializationReplayFixture{database: database}).unchanged(t, snapshot)
			if _, err := fleetPool.Exec(context.Background(), `SELECT * FROM vela_fence_worker_instance($1, 1, 'failure replay fence', 'node-agent/h3-node-01')`, assignment.WorkerInstanceID); err != nil {
				t.Fatal(err)
			}
			before = (&materializationReplayFixture{database: database}).snapshot(t)
			(&materializationReplayFixture{handler: handler}).reject(t, request, identity, session)
			(&materializationReplayFixture{database: database}).unchanged(t, before)
		})
	}
}

func TestStageFailureReplayMigrationRoundTripPreservesEntrypoints(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 86)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	definition := func() string {
		var value string
		if err := database.Admin.QueryRow(`SELECT jsonb_build_array(oid, pg_get_userbyid(proowner), proacl::text, pg_get_functiondef(oid))::text
			FROM pg_proc WHERE oid = 'vela_apply_stage_command(jsonb)'::regprocedure`).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	up := definition()
	if err := goose.DownTo(database.Admin, migrations, 85); err != nil {
		t.Fatal(err)
	}
	down := definition()
	if err := goose.UpTo(database.Admin, migrations, 86); err != nil {
		t.Fatal(err)
	}
	if definition() != up {
		t.Fatal("Up changed canonical Stage command OID, owner, ACL or function body")
	}
	var owner string
	var workerCanProbe, artifactCanProbe, workerCanWrite bool
	if err := database.Admin.QueryRow(`SELECT pg_get_userbyid(proowner),
		has_function_privilege('vela_stage_worker_control_login', oid, 'EXECUTE'),
		has_function_privilege('vela_stage_artifact_login', oid, 'EXECUTE'),
		has_table_privilege('vela_stage_worker_control_login', 'attempt_coordinator_commands', 'INSERT')
		FROM pg_proc WHERE oid = 'vela_is_stage_failure_authority_replayable(jsonb)'::regprocedure`).Scan(&owner, &workerCanProbe, &artifactCanProbe, &workerCanWrite); err != nil {
		t.Fatal(err)
	}
	if owner != "vela_attempt_coordinator_owner" || !workerCanProbe || artifactCanProbe || workerCanWrite {
		t.Fatalf("probe owner=%s worker=%t artifact=%t write=%t", owner, workerCanProbe, artifactCanProbe, workerCanWrite)
	}
	workerPool := newRolePool(t, database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	var sequenceReaderExists bool
	if err := workerPool.QueryRow(context.Background(), `SELECT to_regprocedure($1) IS NOT NULL`,
		"public.vela_read_stage_allocation_execution_sequence(uuid,uuid)").Scan(&sequenceReaderExists); err != nil || sequenceReaderExists {
		t.Fatalf("schema86 sequence reader exists=%t error=%v", sequenceReaderExists, err)
	}
	if err := veladb.VerifyRole(context.Background(), workerPool, veladb.RoleStageWorkerControl); err == nil {
		t.Fatal("new control startup accepted schema86 without execution sequence capability")
	}
	if err := goose.UpTo(database.Admin, migrations, 89); err != nil {
		t.Fatal(err)
	}
	if err := veladb.VerifyRole(context.Background(), workerPool, veladb.RoleStageWorkerControl); err != nil {
		t.Fatalf("schema89 role contract: %v", err)
	}
	if err := goose.DownTo(database.Admin, migrations, 85); err != nil {
		t.Fatal(err)
	}
	if err := veladb.VerifyRole(context.Background(), workerPool, veladb.RoleStageWorkerControl); err == nil {
		t.Fatal("new control startup accepted schema85 without replay capability")
	}
	if definition() != down {
		t.Fatal("Down did not restore Stage command exactly")
	}
	if err := goose.UpTo(database.Admin, migrations, 86); err != nil {
		t.Fatal(err)
	}
}
