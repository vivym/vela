//go:build integration

package integration_test

import (
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
	"github.com/vivym/vela/internal/fleet"
	"github.com/vivym/vela/internal/stageartifact"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageGraphFailureEntryPointsConvergeAndRetainSiblingAuthority(t *testing.T) {
	for _, cause := range []string{"worker_fail", "runtime_epoch", "source_lost", "lease_expiry"} {
		t.Run(cause, func(t *testing.T) {
			database, coordinator, serverURL := newH3IntegrationEnvironmentWithCatalogSetup(t, seedParallelFailureGraph)
			job, attemptID := instantiateH3IntegrationGraph(t, database, serverURL, "terminal-"+cause)
			seedWorkerRegistryPlan(t, database.Admin)
			registry, err := fleet.NewService(newRolePool(t, database.DSN, "vela_fleet_login", "vela-fleet-password"))
			if err != nil {
				t.Fatal(err)
			}
			stage := h3IntegrationStages([]string{"failure-node", "unused-dit", "unused-vae"}, nil)[0]
			var failedRun, siblingRun uuid.UUID
			if err := database.Admin.QueryRow(`SELECT
				(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder'),
				(SELECT id FROM stage_runs WHERE attempt_id = $1 AND stage_key = 'encoder_aux')`, attemptID).
				Scan(&failedRun, &siblingRun); err != nil {
				t.Fatal(err)
			}
			failedWorker := seedH3IntegrationWorker(t, database, registry, stage, 0xa1)
			stage.key, stage.nodeIdentity = "encoder_aux", "sibling-node"
			siblingWorker := seedH3IntegrationWorker(t, database, registry, stage, 0xa2)
			failed := assignFailureGraphStage(t, coordinator, attemptID, failedRun, failedWorker, time.Now().Add(2*time.Second))
			sibling := assignFailureGraphStage(t, coordinator, attemptID, siblingRun, siblingWorker, time.Now().Add(3*time.Second))
			latestSiblingExpiry := renewStageExpiryFixture(t, database, job, sibling)
			if cause == "worker_fail" || cause == "source_lost" {
				if _, err := coordinator.Apply(context.Background(), attemptcoordinator.StartStageCommand{
					CommandID: uuid.New(), AttemptID: attemptID, StageRunID: failedRun,
					StageAttemptID: failed.StageAttemptID, StageLeaseID: failed.StageLeaseID,
					ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 2,
					StartedAt: time.Now(),
				}); err != nil {
					t.Fatal(err)
				}
			}
			tx, err := database.Admin.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`SET LOCAL ROLE vela_attempt_coordinator_owner`); err != nil {
				t.Fatal(err)
			}
			if _, err := tx.Exec(`UPDATE stage_retry_budgets SET max_attempts = attempts_consumed WHERE stage_run_id = $1`, failedRun); err != nil {
				t.Fatal(err)
			}
			if err := tx.Commit(); err != nil {
				t.Fatal(err)
			}
			switch cause {
			case "worker_fail":
				command := attemptcoordinator.FailStageCommand{
					CommandID: uuid.New(), AttemptID: attemptID, StageRunID: failedRun,
					StageAttemptID: failed.StageAttemptID, StageLeaseID: failed.StageLeaseID,
					ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 3,
					FailureClass: "WORKER_LOST", FailureFingerprint: bytesOf(0xb1, 32),
					ConsumedResourceUnits: 1, FailedAt: time.Now(), RetryAt: time.Now().Add(time.Second),
				}
				if decision, err := coordinator.Apply(context.Background(), command); err != nil || decision.State != "FAILED" {
					t.Fatalf("terminal Worker FAIL=%#v error=%v", decision, err)
				}
				if replay, err := coordinator.Apply(context.Background(), command); err != nil || !replay.Replayed {
					t.Fatalf("Worker FAIL replay=%#v error=%v", replay, err)
				}
			case "runtime_epoch":
				// Registration's internal transition has exact lease/residency/epoch
				// inputs. Registration and barrier behavior has separate regressions.
				if _, err := database.Admin.Exec(`SELECT vela_fence_assigned_stage_for_runtime_epoch($1, $2, 1, 2, clock_timestamp())`,
					failed.StageLeaseID, failed.ModelResidencyID); err != nil {
					t.Fatal(err)
				}
			case "source_lost":
				repository, seal := sealFailureGraphOutput(t, database, failed)
				command := stageartifact.SourceLostCommand{
					CommandID: uuid.New(), MaterializationLeaseID: seal.MaterializationLeaseID,
					TokenDigest: seal.TokenDigest, FailureFingerprint: seal.SHA256,
					ConsumedResourceUnits: 1, LostAt: time.Now(), RetryAt: time.Now().Add(time.Second),
				}
				if decision, err := repository.FailSourceLost(context.Background(), command); err != nil || decision.State != "FAILED" {
					t.Fatalf("terminal source loss=%#v error=%v", decision, err)
				}
				if replay, err := repository.FailSourceLost(context.Background(), command); err != nil || !replay.Replayed {
					t.Fatalf("source loss replay=%#v error=%v", replay, err)
				}
			case "lease_expiry":
				waitStageExpiry(t, failed.ExpiresAt)
				if _, err := coordinator.Reconcile(context.Background(), 100); err != nil {
					t.Fatal(err)
				}
			}
			assertStageFailureConverged(t, database, job.JobID, failedRun, siblingRun)
			assertStageExpiryAllocation(t, database, failed, "RELEASED")
			assertStageExpiryAllocation(t, database, sibling, "ALLOCATED")
			waitStageExpiry(t, sibling.ExpiresAt)
			if _, err := coordinator.Reconcile(context.Background(), 100); err != nil {
				t.Fatal(err)
			}
			assertStageExpiryAllocation(t, database, sibling, "ALLOCATED")
			waitStageExpiry(t, latestSiblingExpiry)
			if _, err := coordinator.Reconcile(context.Background(), 100); err != nil {
				t.Fatal(err)
			}
			assertStageExpiryAllocation(t, database, sibling, "RELEASED")
			if _, err := coordinator.Reconcile(context.Background(), 100); err != nil {
				t.Fatal(err)
			}
			assertStageFailureConverged(t, database, job.JobID, failedRun, siblingRun)
		})
	}
}

func TestStageGraphFailureMigrationPreservesEntrypointAuthority(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 74)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	var before, after uint32
	if err := database.Admin.QueryRow(`SELECT 'vela_start_stage_worker_command(jsonb)'::regprocedure::oid`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 75); err != nil {
			t.Fatal(err)
		}
		var helperVisible, rollbackVisible, workerAllowed bool
		if err := database.Admin.QueryRow(`SELECT
			'vela_start_stage_worker_command(jsonb)'::regprocedure::oid,
			has_function_privilege('vela_request', 'vela_terminalize_stage_graph_failure(uuid,uuid,uuid,text)', 'EXECUTE'),
			has_function_privilege('vela_stage_worker_control', 'vela_start_stage_worker_command_v74(jsonb)', 'EXECUTE'),
			has_function_privilege('vela_stage_worker_control', 'vela_start_stage_worker_command(jsonb)', 'EXECUTE')`).
			Scan(&after, &helperVisible, &rollbackVisible, &workerAllowed); err != nil {
			t.Fatal(err)
		}
		if before != after || helperVisible || rollbackVisible || !workerAllowed {
			t.Fatalf("terminalization OIDs=%d/%d grants helper=%t rollback=%t Worker=%t", before, after, helperVisible, rollbackVisible, workerAllowed)
		}
		if err := goose.DownTo(database.Admin, migrations, 74); err != nil {
			t.Fatal(err)
		}
	}
}

func TestStageGraphParentLocksPrecedeWorkerCommandChildLocks(t *testing.T) {
	for _, kind := range []string{"START", "HEARTBEAT", "REATTACH"} {
		t.Run(kind, func(t *testing.T) {
			database, _, coordinator, job, attemptID, runID, _ :=
				newStageGraphCancellationFixture(t, "parent-lock-"+kind)
			assignment := assignEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(time.Minute))
			assigned := signedAssignedStageAuthority(t, database, job, assignment, 2)
			envelope := proto.Clone(assigned.Authority).(*velav1.StageAuthority)
			envelope.StageVersion = 3
			envelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(10 * time.Millisecond))
			envelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(time.Second))
			envelope.MonotonicValidFor = durationpb.New(time.Minute)
			envelope.Signature = nil
			started := signAndVerifyStageAuthority(t, envelope, assignment.IssuedAt.Add(11*time.Millisecond))
			wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(started.Authority)
			if err != nil {
				t.Fatal(err)
			}
			payload := stageWorkerStartPayload(t, uuid.New(), assignment, assigned, started, wire,
				assignment.IssuedAt.Add(5*time.Millisecond))
			query := `SELECT * FROM vela_start_stage_worker_command($1::jsonb)`
			pool := newRolePool(t, database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
			if kind == "HEARTBEAT" {
				if _, err := pool.Exec(context.Background(), query, payload); err != nil {
					t.Fatal(err)
				}
				envelope = proto.Clone(started.Authority).(*velav1.StageAuthority)
				envelope.IssuedAt = timestamppb.New(assignment.IssuedAt.Add(30 * time.Millisecond))
				envelope.ExpiresAt = timestamppb.New(assignment.ExpiresAt.Add(2 * time.Second))
				envelope.Signature = nil
				heartbeat := signAndVerifyStageAuthority(t, envelope, assignment.IssuedAt.Add(31*time.Millisecond))
				wire, err = proto.MarshalOptions{Deterministic: true}.Marshal(heartbeat.Authority)
				if err != nil {
					t.Fatal(err)
				}
				payload = stageWorkerHeartbeatPayload(t, uuid.New(), assignment, started, heartbeat, wire,
					1, assignment.IssuedAt.Add(20*time.Millisecond))
				query = `SELECT * FROM vela_heartbeat_stage_worker_command($1::jsonb)`
			} else if kind == "REATTACH" {
				var fields map[string]any
				if err := json.Unmarshal(payload, &fields); err != nil {
					t.Fatal(err)
				}
				identity := sha256.Sum256([]byte("spiffe://vela/worker/" + assignment.WorkerInstanceID.String()))
				fields["command_kind"] = "REATTACH"
				fields["spiffe_id_digest"] = hex.EncodeToString(identity[:])
				fields["observed_runtime_state"] = "PREPARED"
				payload, err = json.Marshal(fields)
				if err != nil {
					t.Fatal(err)
				}
				query = `SELECT * FROM vela_reattach_stage_worker_command($1::jsonb)`
			}
			assertStageCommandParentLock(t, database, job.JobID, "vela_stage_worker_control_login",
				`SELECT id FROM attempts WHERE id = $1 FOR UPDATE NOWAIT`, attemptID,
				func(ctx context.Context) error {
					_, err := pool.Exec(ctx, query, payload)
					return err
				})
		})
	}
}

func TestStageGraphParentLocksPrecedeMaterializationLocks(t *testing.T) {
	for _, kind := range []string{"commit", "source_lost"} {
		t.Run(kind, func(t *testing.T) {
			database, _, coordinator, job, attemptID, runID, _ :=
				newStageGraphCancellationFixture(t, "materialization-parent-"+kind)
			assignment := assignAndStartEncoder(t, database, coordinator, attemptID, runID, time.Now().Add(time.Minute))
			repository, seal := sealFailureGraphOutput(t, database, assignment)
			assertStageCommandParentLock(t, database, job.JobID, "vela_stage_artifact_login",
				`SELECT id FROM stage_materialization_leases WHERE id = $1 FOR UPDATE NOWAIT`, seal.MaterializationLeaseID,
				func(ctx context.Context) error {
					if kind == "commit" {
						_, err := repository.Commit(ctx, stageartifact.CommitCommand{
							CommandID: uuid.New(), ProgressReceiptID: uuid.New(),
							MaterializationLeaseID: seal.MaterializationLeaseID, ArtifactID: seal.ArtifactID,
							ObjectKey: seal.ObjectKey, ObjectVersion: "lock-order-version", SHA256: seal.SHA256,
							SizeBytes: seal.SizeBytes, TokenDigest: seal.TokenDigest, CommittedAt: time.Now(),
						})
						return err
					}
					_, err := repository.FailSourceLost(ctx, stageartifact.SourceLostCommand{
						CommandID: uuid.New(), MaterializationLeaseID: seal.MaterializationLeaseID,
						TokenDigest: seal.TokenDigest, FailureFingerprint: seal.SHA256, ConsumedResourceUnits: 1,
						LostAt: time.Now(), RetryAt: time.Now().Add(time.Second),
					})
					return err
				})
		})
	}
}

func assertStageCommandParentLock(t *testing.T, database testDatabase, jobID, login, childQuery string,
	childID uuid.UUID, command func(context.Context) error,
) {
	t.Helper()
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`SELECT id FROM jobs WHERE id = $1 FOR UPDATE`, jobID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- command(ctx) }()
	for {
		var waiting bool
		if err := database.Admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE usename = $1 AND wait_event_type = 'Lock')`, login).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-result:
			t.Fatalf("command returned before parent lock: %v", err)
		case <-ctx.Done():
			t.Fatal("command did not wait for parent Job")
		case <-time.After(10 * time.Millisecond):
		}
	}
	if _, err := tx.Exec(childQuery, childID); err != nil {
		t.Fatalf("command locked child while waiting for parent Job: %v", err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatalf("command after parent lock release: %v", err)
	}
}

func seedParallelFailureGraph(t *testing.T, database testDatabase) {
	t.Helper()
	// A second independent Encoder root gives this DAG real concurrent authority
	// before either output is committed. Both roots are declared before activation.
	if _, err := database.Admin.Exec(`
		INSERT INTO execution_graph_stages SELECT execution_graph_revision_id, 'encoder_aux',
			stage_definition_revision_id, required, max_fan_out
		FROM execution_graph_stages WHERE stage_key = 'encoder';
		INSERT INTO execution_graph_inputs SELECT execution_graph_revision_id, 'request_aux',
			interface_revision_id, 'encoder_aux', destination_port
		FROM execution_graph_inputs WHERE destination_stage_key = 'encoder';
		INSERT INTO execution_graph_outputs (execution_graph_revision_id, output_key,
			interface_revision_id, source_stage_key, source_port, required)
		VALUES ('49000000-0000-0000-0000-000000000001', 'conditioning_aux',
			'49000000-0000-0000-0000-000000000011', 'encoder_aux', 'conditioning', false);
		INSERT INTO execution_profile_stage_options SELECT execution_profile_revision_id,
			execution_graph_revision_id, 'encoder_aux', stage_definition_revision_id,
			stage_profile_revision_id, preference, eligibility_metadata
		FROM execution_profile_stage_options WHERE stage_key = 'encoder';
		UPDATE execution_graph_revisions SET content_digest = vela_execution_graph_content_digest(id)
		WHERE id = '49000000-0000-0000-0000-000000000001';
	`); err != nil {
		t.Fatal(err)
	}
}

func assignFailureGraphStage(t *testing.T, coordinator *attemptcoordinator.Service,
	attemptID, runID uuid.UUID, worker h3IntegrationWorker, expiresAt time.Time,
) attemptcoordinator.AssignStageCommand {
	t.Helper()
	tokenDigest := sha256.Sum256(bytesOf(0xb3, 32))
	command := attemptcoordinator.AssignStageCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: runID,
		ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 1,
		StageAttemptID: uuid.New(), StageAllocationID: uuid.New(), StageLeaseID: uuid.New(),
		StageProfileRevisionID: uuid.MustParse(encoderStageProfileID), CapacityPoolID: worker.poolID,
		WorkerInstanceID: worker.workerID, WorkerInstanceEpoch: 1,
		ObservationSequence: worker.evidence.Capacity.Sequence,
		DeviceSetDigest:     worker.authority.DeviceSetDigest, MembershipDigest: worker.authority.MembershipDigest,
		ModelResidencyID: worker.authority.ModelResidencyID, ModelRuntimeEpoch: 1,
		CapacityVector: worker.capacity, TokenDigest: tokenDigest[:],
		SigningKeyID: "stage-authority-key-v1", ExecutionNonce: bytesOf(0xa5, 32),
		IssuedAt: time.Now().UTC().Truncate(time.Millisecond), ExpiresAt: expiresAt,
		LocalDeadlineAt: expiresAt.Add(-time.Millisecond),
	}
	if _, err := coordinator.Apply(context.Background(), command); err != nil {
		t.Fatal(err)
	}
	return command
}

func sealFailureGraphOutput(t *testing.T, database testDatabase, assignment attemptcoordinator.AssignStageCommand, deadlines ...time.Time,
) (*stageartifact.PostgresRepository, stageartifact.SealCommand) {
	t.Helper()
	repository, err := stageartifact.NewPostgresRepository(newRolePool(t, database.DSN,
		"vela_stage_artifact_login", "vela-stage-artifact-password"))
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256([]byte("terminal failure output"))
	seal := stageartifact.SealCommand{
		CommandID: uuid.New(), AttemptID: assignment.AttemptID, StageRunID: assignment.StageRunID,
		StageAttemptID: assignment.StageAttemptID, StageAllocationID: assignment.StageAllocationID,
		StageLeaseID: assignment.StageLeaseID, ExpectedAttemptFence: 1, ExpectedStageFence: 1,
		ExpectedStageVersion: 3, OutputPort: "conditioning", LocalReceiptID: "terminal-output",
		LocalReceiptDigest: digest, ManifestSHA256: digest, SHA256: digest, LineageDigest: digest,
		TokenDigest: digest, SizeBytes: 64, ArtifactID: uuid.New(), MaterializationLeaseID: uuid.New(),
		ObjectKey:   "artifacts/stage/terminal/" + assignment.StageAttemptID.String() + "/output.bin",
		ContentType: "application/octet-stream", SealedAt: time.Now(), LeaseExpiresAt: time.Now().Add(time.Minute),
	}
	for _, deadline := range deadlines {
		if deadline.Before(seal.LeaseExpiresAt) {
			seal.LeaseExpiresAt = deadline
		}
	}
	if _, err := repository.Seal(context.Background(), seal); err != nil {
		t.Fatal(err)
	}
	return repository, seal
}

func assertStageFailureConverged(t *testing.T, database testDatabase, jobID string, failedRun, siblingRun uuid.UUID) {
	t.Helper()
	var state, graph, credit, failed, sibling, physical, lease string
	var queued, running, reserved, charges, events int64
	if err := database.Admin.QueryRow(`SELECT job.state::text, attempt.graph_state::text,
		credit.state::text, failed.state::text, sibling.state::text, physical.state::text,
		lease.state::text, project.queued_count, project.running_count, account.reserved_minor,
		(SELECT count(*) FROM charges WHERE job_id = job.id),
		(SELECT count(*) FROM outbox_events WHERE aggregate_id = job.id AND event_type = 'job.failed')
		FROM jobs AS job JOIN attempts AS attempt ON attempt.job_id = job.id
		JOIN credit_reservations AS credit ON credit.job_id = job.id
		JOIN projects AS project ON project.id = job.project_id
		JOIN organization_credit_accounts AS account ON account.organization_id = job.organization_id
		JOIN stage_runs AS failed ON failed.id = $2
		JOIN stage_runs AS sibling ON sibling.id = $3
		JOIN stage_attempts AS physical ON physical.stage_run_id = sibling.id
		JOIN stage_leases AS lease ON lease.stage_attempt_id = physical.id
		WHERE job.id = $1`, jobID, failedRun, siblingRun).
		Scan(&state, &graph, &credit, &failed, &sibling, &physical, &lease,
			&queued, &running, &reserved, &charges, &events); err != nil {
		t.Fatal(err)
	}
	if state != "FAILED" || graph != "FAILED" || credit != "RELEASED" || failed != "FAILED" ||
		sibling != "CANCELED" || physical != "CANCELED" || lease != "REVOKED" ||
		queued != 0 || running != 0 || reserved != 0 || charges != 0 || events != 1 {
		t.Fatalf("terminal graph=%s/%s credit=%s stages=%s/%s physical/lease=%s/%s counters=%d/%d/%d/%d/%d",
			state, graph, credit, failed, sibling, physical, lease, queued, running, reserved, charges, events)
	}
}
