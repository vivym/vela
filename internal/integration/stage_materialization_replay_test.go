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
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/materializationauthority"
	"github.com/vivym/vela/internal/stageartifact"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageMaterializationHandlerReplaysTerminalCommandsAfterTTLAndReconnect(t *testing.T) {
	for _, kind := range []string{"COMMIT", "SOURCE_LOST", "SOURCE_LOST_TERMINAL"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newMaterializationReplayFixture(t, kind, 2*time.Second)
			before := fixture.snapshot(t)
			fixture.clockOffset -= time.Minute
			fixture.reject(t, fixture.request, fixture.identity, 1)
			fixture.clockOffset += time.Minute
			fixture.unchanged(t, before)
			fixture.accept(t, velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED, 1)
			var state string
			if err := fixture.database.Admin.QueryRow(`SELECT state::text FROM stage_runs WHERE id = $1`, fixture.assignment.StageRunID).Scan(&state); err != nil {
				t.Fatal(err)
			}
			wantState := map[string]string{"COMMIT": "SUCCEEDED", "SOURCE_LOST": "RETRY_WAIT", "SOURCE_LOST_TERMINAL": "FAILED"}[kind]
			if state != wantState {
				t.Fatalf("durable StageRun state=%s want=%s", state, wantState)
			}
			committed := fixture.snapshot(t)
			fixture.accept(t, velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED, 1)
			fixture.waitExpired(t)
			// The first response is deliberately unused by recovery. Only the exact
			// original request and the durable receipt survive the TTL boundary.
			fixture.accept(t, velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED, 1)
			fixture.unchanged(t, committed)
			mutations := map[string]func(*velav1.StageWorkerControlServiceConnectRequest){
				"command ID": func(request *velav1.StageWorkerControlServiceConnectRequest) { request.RequestId = uuid.NewString() },
				"signature": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					replayAuthority(request).Token[0] ^= 0xff
				},
				"worker epoch": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					replayAuthority(request).SourceWorkerInstanceEpoch++
				},
				"operation kind": func(request *velav1.StageWorkerControlServiceConnectRequest) {
					authority := replayAuthority(request)
					if request.GetCommitStageMaterialization() != nil {
						request.Operation = &velav1.StageWorkerControlServiceConnectRequest_ReportMaterializationSourceLost{
							ReportMaterializationSourceLost: &velav1.ReportMaterializationSourceLostRequest{
								MaterializationAuthority: authority, FailureFingerprint: authority.GetSha256(),
								ConsumedResourceUnits: 1, LostAt: authority.GetIssuedAt(), RetryAt: authority.GetExpiresAt(),
							},
						}
					} else {
						request.Operation = &velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization{
							CommitStageMaterialization: &velav1.CommitStageMaterializationRequest{
								MaterializationAuthority: authority, ObjectVersion: "other-operation", CommittedAt: authority.GetIssuedAt(),
							},
						}
					}
				},
			}
			if kind == "COMMIT" {
				mutations["object version"] = func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetCommitStageMaterialization().ObjectVersion += "-other"
				}
				mutations["object version whitespace"] = func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetCommitStageMaterialization().ObjectVersion += " "
				}
				mutations["committed at"] = func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetCommitStageMaterialization().CommittedAt = timestamppb.New(fixture.authority.GetIssuedAt().AsTime().Add(2 * time.Millisecond))
				}
			} else {
				mutations["fingerprint"] = func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetReportMaterializationSourceLost().FailureFingerprint[0] ^= 0xff
				}
				mutations["consumed units"] = func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetReportMaterializationSourceLost().ConsumedResourceUnits++
				}
				mutations["lost at"] = func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetReportMaterializationSourceLost().LostAt = timestamppb.New(fixture.authority.GetIssuedAt().AsTime().Add(2 * time.Millisecond))
				}
				mutations["retry at"] = func(request *velav1.StageWorkerControlServiceConnectRequest) {
					request.GetReportMaterializationSourceLost().RetryAt = timestamppb.New(fixture.authority.GetExpiresAt().AsTime().Add(time.Second))
				}
			}
			for name, mutate := range mutations {
				t.Run(name, func(t *testing.T) {
					request := proto.Clone(fixture.request).(*velav1.StageWorkerControlServiceConnectRequest)
					mutate(request)
					fixture.reject(t, request, fixture.identity, 1)
					fixture.unchanged(t, committed)
				})
			}
			fixture.reject(t, fixture.request, stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/" + uuid.NewString()}, 1)
			fixture.unchanged(t, committed)
			fleetPool := newRolePool(t, fixture.database.DSN, "vela_fleet_login", "vela-fleet-password")
			var session int64
			if err := fleetPool.QueryRow(context.Background(), `
				SELECT control_session_epoch FROM vela_reconnect_worker_instance(
					$1, 1, 1, 'materialization-replay-session-2', clock_timestamp(), 'worker-agent/h3-node-01')
			`, fixture.assignment.WorkerInstanceID).Scan(&session); err != nil || session != 2 {
				t.Fatalf("reconnect session=%d error=%v", session, err)
			}
			fixture.reject(t, fixture.request, fixture.identity, 1)
			fixture.accept(t, velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED, session)
			fixture.unchanged(t, committed)
			if _, err := fleetPool.Exec(context.Background(), `
				SELECT * FROM vela_fence_worker_instance($1, 1, 'replay epoch test', 'node-agent/h3-node-01')
			`, fixture.assignment.WorkerInstanceID); err != nil {
				t.Fatal(err)
			}
			fenced := fixture.snapshot(t)
			fixture.reject(t, fixture.request, fixture.identity, session)
			fixture.unchanged(t, fenced)
		})
	}
}

func TestStageMaterializationTerminalReplayRequiresDurableReceipt(t *testing.T) {
	for _, kind := range []string{"COMMIT", "SOURCE_LOST"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newMaterializationReplayFixture(t, kind, time.Minute)
			if kind == "COMMIT" {
				if _, err := fixture.database.Admin.Exec(`UPDATE stage_materialization_leases
					SET state = 'COMMITTED', committed_at = clock_timestamp() WHERE id = $1`, fixture.authority.GetStageMaterializationLeaseId()); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := fixture.database.Admin.Exec(`UPDATE stage_materialization_leases
					SET state = 'REVOKED', revoked_at = clock_timestamp(), revoke_reason = 'LOCAL_SOURCE_LOST' WHERE id = $1`, fixture.authority.GetStageMaterializationLeaseId()); err != nil {
					t.Fatal(err)
				}
			}
			before := fixture.snapshot(t)
			fixture.reject(t, fixture.request, fixture.identity, 1)
			fixture.unchanged(t, before)
		})
	}
}

func TestStageMaterializationSourceLossExpiresDuringHandlerLockWait(t *testing.T) {
	fixture := newMaterializationReplayFixture(t, "SOURCE_LOST", 2*time.Second)
	before := fixture.snapshot(t)
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM stage_materialization_leases WHERE id = $1 FOR UPDATE`, fixture.authority.GetStageMaterializationLeaseId()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	type result struct {
		response *velav1.StageWorkerControlServiceConnectResponse
		err      error
	}
	completed := make(chan result, 1)
	go func() {
		response, err := fixture.handler.Handle(ctx, fixture.identity, 1, fixture.request)
		completed <- result{response, err}
	}()
	for {
		var waiting, expired bool
		if err := fixture.database.Admin.QueryRow(`SELECT
			EXISTS (SELECT 1 FROM pg_stat_activity WHERE usename = 'vela_stage_artifact_login' AND wait_event_type = 'Lock'),
			clock_timestamp() >= $1`, fixture.authority.GetExpiresAt().AsTime()).Scan(&waiting, &expired); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		if expired {
			t.Fatal("SOURCE_LOST did not reach lock wait before TTL")
		}
		time.Sleep(10 * time.Millisecond)
	}
	fixture.waitExpired(t)
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	got := <-completed
	if got.err != nil || got.response.GetStageCommandResult().GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE {
		t.Fatalf("expired lock-wait response=%v error=%v", got.response, got.err)
	}
	fixture.unchanged(t, before)
	fixture.reject(t, fixture.request, fixture.identity, 1)
	fixture.unchanged(t, before)
}

func TestStageMaterializationReplayMigrationRoundTripPreservesEntrypoints(t *testing.T) {
	fixture := newMaterializationReplayFixtureAtSchema(t, "COMMIT", time.Minute, 85)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	definition := func() string {
		var value string
		if err := fixture.database.Admin.QueryRow(`SELECT jsonb_agg(jsonb_build_array(
			oid, pg_get_userbyid(proowner), proacl::text, pg_get_functiondef(oid)) ORDER BY oid)::text
			FROM pg_proc WHERE oid IN ('vela_is_stage_materialization_authority_active(jsonb)'::regprocedure,
			'vela_fail_stage_materialization_source(jsonb)'::regprocedure)`).Scan(&value); err != nil {
			t.Fatal(err)
		}
		return value
	}
	up := definition()
	if err := goose.DownTo(fixture.database.Admin, migrations, 84); err != nil {
		t.Fatal(err)
	}
	down := definition()
	fixture.accept(t, velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED, 1)
	fixture.reject(t, fixture.request, fixture.identity, 1)
	if err := goose.UpTo(fixture.database.Admin, migrations, 85); err != nil {
		t.Fatal(err)
	}
	if definition() != up {
		t.Fatal("Up changed canonical OID, owner, ACL or function body")
	}
	fixture.accept(t, velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED, 1)
	if err := goose.DownTo(fixture.database.Admin, migrations, 84); err != nil {
		t.Fatal(err)
	}
	if definition() != down {
		t.Fatal("Down did not restore canonical entrypoints exactly")
	}
	if err := goose.UpTo(fixture.database.Admin, migrations, 85); err != nil {
		t.Fatal(err)
	}
}

type materializationReplayFixture struct {
	database    testDatabase
	assignment  attemptcoordinator.AssignStageCommand
	handler     *stageworkercontrol.Handler
	authority   *velav1.MaterializationAuthority
	identity    stageworkertransport.Identity
	request     *velav1.StageWorkerControlServiceConnectRequest
	clockOffset time.Duration
}

// These paths are deliberately unavailable: COMMIT and SOURCE_LOST below use
// the real production backend and role-scoped PostgreSQL repository throughout.
type unusedMaterializationReplayDependencies struct {
	stageworkercontrol.TerminalDispositionOperations
	stageworkercontrol.WorkerEvidenceOperations
	stageworkercontrol.AssignmentOperations
	stageworkercontrol.ExecutionOperations
	stageworkercontrol.MaterializationIssuer
	stageworkercontrol.ReattachmentOperations
	stageworkercontrol.TransferOperations
}

func newMaterializationReplayFixture(t *testing.T, kind string, ttl time.Duration) *materializationReplayFixture {
	t.Helper()
	return newMaterializationReplayFixtureAtSchema(t, kind, ttl, 0)
}

func newMaterializationReplayFixtureAtSchema(t *testing.T, kind string, ttl time.Duration, schema int64) *materializationReplayFixture {
	t.Helper()
	database, _, coordinator, _, attemptID, runID, _ := newStageGraphCancellationFixture(t, "materialization-handler-"+kind)
	if schema != 0 {
		// Migration replay uses historical allocations; schema88 cannot discard issued sequences.
		if err := goose.DownTo(database.Admin, filepath.Join(repositoryRoot(t), "db", "migrations"), schema); err != nil {
			t.Fatal(err)
		}
	}
	assignment := assignAndStartEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(time.Hour))
	repository, err := stageartifact.NewPostgresRepository(newRolePool(t, database.DSN, "vela_stage_artifact_login", "vela-stage-artifact-password"))
	if err != nil {
		t.Fatal(err)
	}
	var now time.Time
	var memberID string
	var memberEpoch int64
	if err := database.Admin.QueryRow(`SELECT clock_timestamp(), id::text, member_epoch FROM worker_members WHERE worker_instance_id = $1`, assignment.WorkerInstanceID).Scan(&now, &memberID, &memberEpoch); err != nil {
		t.Fatal(err)
	}
	fixture := &materializationReplayFixture{database: database, assignment: assignment, clockOffset: now.Sub(time.Now())}
	fixture.identity = stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/" + assignment.WorkerInstanceID.String()}
	digest := sha256.Sum256([]byte("materialization handler replay"))
	spiffeDigest := sha256.Sum256([]byte(fixture.identity.SPIFFEID))
	keys := map[string][]byte{"replay-key": bytes.Repeat([]byte{0xb9}, 32)}
	signer, err := materializationauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	fixture.authority, err = signer.Sign(&velav1.MaterializationAuthority{
		SchemaVersion: 1, StageAuthorityDigest: assignment.TokenDigest,
		StageMaterializationLeaseId: uuid.NewString(), StageArtifactId: uuid.NewString(),
		ObjectKey: "artifacts/stage/replay/" + attemptID.String() + "/output.bin", ContentType: "application/octet-stream",
		Sha256: digest[:], SizeBytes: 64, LocalReceiptId: "replay-local-receipt", LocalReceiptDigest: digest[:],
		SigningKeyId: "replay-key", IssuedAt: timestamppb.New(now), ExpiresAt: timestamppb.New(now.Add(ttl)),
		SourceWorkerInstanceId: assignment.WorkerInstanceID.String(), SourceWorkerInstanceEpoch: assignment.WorkerInstanceEpoch,
		SourceWorkerMemberId: memberID, SourceWorkerMemberEpoch: memberEpoch, SourceSpiffeIdDigest: spiffeDigest[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	tokenDigest, err := materializationauthority.Digest(fixture.authority)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repository.Seal(context.Background(), stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: runID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 3,
		OutputPort: "conditioning", LocalReceiptID: fixture.authority.GetLocalReceiptId(),
		LocalReceiptDigest: digest, ManifestSHA256: digest, SHA256: digest, LineageDigest: digest, TokenDigest: tokenDigest, SizeBytes: 64,
		ArtifactID: uuid.MustParse(fixture.authority.GetStageArtifactId()), MaterializationLeaseID: uuid.MustParse(fixture.authority.GetStageMaterializationLeaseId()),
		ObjectKey: fixture.authority.GetObjectKey(), ContentType: fixture.authority.GetContentType(), SealedAt: now, LeaseExpiresAt: now.Add(ttl),
	}); err != nil {
		t.Fatal(err)
	}
	unused := unusedMaterializationReplayDependencies{}
	backend, err := stageworkercontrol.NewPostgresOperationBackend(stageworkercontrol.PostgresOperationConfig{
		TerminalDispositions: unused,
		WorkerEvidence:       unused, Assignments: unused, Execution: unused, MaterializationIssuer: unused,
		StageArtifacts: repository, StageAttempts: coordinator, Reattachments: unused, Transfers: unused,
	})
	if err != nil {
		t.Fatal(err)
	}
	executor, err := stageworkercontrol.NewProductionExecutor(backend)
	if err != nil {
		t.Fatal(err)
	}
	stageValidator, err := stageauthority.NewValidator(keys, nil)
	if err != nil {
		t.Fatal(err)
	}
	materializationValidator, err := materializationauthority.NewValidator(keys, func() time.Time { return time.Now().Add(fixture.clockOffset) })
	if err != nil {
		t.Fatal(err)
	}
	stageAuthorizer, err := stageworkercontrol.NewPostgresAuthorizer(newRolePool(t, database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password"))
	if err != nil {
		t.Fatal(err)
	}
	fixture.handler, err = stageworkercontrol.NewHandler(stageworkercontrol.Config{
		Validator: stageValidator, Authorizer: stageAuthorizer, MaterializationValidator: materializationValidator,
		MaterializationAuthorizer: repository, Executor: executor,
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.request = &velav1.StageWorkerControlServiceConnectRequest{RequestId: uuid.NewString()}
	if kind == "COMMIT" {
		fixture.request.Operation = &velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization{
			CommitStageMaterialization: &velav1.CommitStageMaterializationRequest{MaterializationAuthority: fixture.authority, ObjectVersion: "exact-object-v1", CommittedAt: timestamppb.New(now.Add(time.Millisecond))},
		}
	} else {
		units := int64(1)
		if kind == "SOURCE_LOST_TERMINAL" {
			if err := database.Admin.QueryRow(`SELECT max_resource_units FROM attempt_retry_budgets WHERE attempt_id = $1`, attemptID).Scan(&units); err != nil {
				t.Fatal(err)
			}
		}
		fixture.request.Operation = &velav1.StageWorkerControlServiceConnectRequest_ReportMaterializationSourceLost{
			ReportMaterializationSourceLost: &velav1.ReportMaterializationSourceLostRequest{MaterializationAuthority: fixture.authority,
				FailureFingerprint: digest[:], ConsumedResourceUnits: units, LostAt: timestamppb.New(now.Add(time.Millisecond)), RetryAt: timestamppb.New(now.Add(time.Second))},
		}
	}
	return fixture
}

func replayAuthority(request *velav1.StageWorkerControlServiceConnectRequest) *velav1.MaterializationAuthority {
	if request.GetCommitStageMaterialization() != nil {
		return request.GetCommitStageMaterialization().GetMaterializationAuthority()
	}
	return request.GetReportMaterializationSourceLost().GetMaterializationAuthority()
}

func (fixture *materializationReplayFixture) accept(t *testing.T, decision velav1.StageWorkerCommandDecision, session int64) {
	t.Helper()
	response, err := fixture.handler.Handle(context.Background(), fixture.identity, session, fixture.request)
	if err != nil || response.GetStageCommandResult().GetDecision() != decision {
		t.Fatalf("materialization decision want=%s response=%v error=%v", decision, response, err)
	}
	digest, err := materializationauthority.Digest(fixture.authority)
	if err != nil || !bytes.Equal(response.GetStageCommandResult().GetAuthorityDigest(), digest[:]) {
		t.Fatalf("response authority digest changed: %v", err)
	}
}

func (fixture *materializationReplayFixture) reject(t *testing.T, request *velav1.StageWorkerControlServiceConnectRequest, identity stageworkertransport.Identity, session int64) {
	t.Helper()
	response, err := fixture.handler.Handle(context.Background(), identity, session, request)
	if err != nil {
		t.Fatal(err)
	}
	decision := response.GetStageCommandResult().GetDecision()
	if decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE && decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED {
		t.Fatalf("invalid materialization request accepted: %v", response)
	}
}

func (fixture *materializationReplayFixture) waitExpired(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var expired bool
		if err := fixture.database.Admin.QueryRow(`SELECT clock_timestamp() >= $1`, fixture.authority.GetExpiresAt().AsTime()).Scan(&expired); err != nil {
			t.Fatal(err)
		}
		if expired {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("materialization fixture did not expire")
}

func (fixture *materializationReplayFixture) snapshot(t *testing.T) string {
	t.Helper()
	var value string
	if err := fixture.database.Admin.QueryRow(`SELECT jsonb_build_object(
		'jobs', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM jobs AS row),
		'attempts', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM attempts AS row),
		'runs', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM stage_runs AS row),
		'physical', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM stage_attempts AS row),
		'allocations', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM stage_allocations AS row),
		'execution_leases', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM stage_leases AS row),
		'leases', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM stage_materialization_leases AS row),
		'artifacts', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM stage_artifacts AS row),
		'receipts', (SELECT jsonb_agg(to_jsonb(row) ORDER BY command_id) FROM stage_artifact_commands AS row),
		'attempt_commands', (SELECT jsonb_agg(to_jsonb(row) ORDER BY command_id) FROM attempt_coordinator_commands AS row),
		'storage', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM stage_storage_reservations AS row),
		'retry', (SELECT jsonb_agg(to_jsonb(row) ORDER BY attempt_id) FROM attempt_retry_budgets AS row),
		'credit', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM credit_reservations AS row),
		'charges', (SELECT jsonb_agg(to_jsonb(row) ORDER BY id) FROM charges AS row)
	)::text`).Scan(&value); err != nil {
		t.Fatal(err)
	}
	return value
}

func (fixture *materializationReplayFixture) unchanged(t *testing.T, before string) {
	t.Helper()
	if after := fixture.snapshot(t); before != after {
		t.Fatalf("replay or rejection mutated durable authority, receipts, storage, retry budget or Charges\nbefore=%s\nafter=%s", before, after)
	}
}
