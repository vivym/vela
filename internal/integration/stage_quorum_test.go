//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stagescheduler"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestStageAuthorityWritesRequireSynchronousQuorum(t *testing.T) {
	for _, entrypoint := range []string{"worker-acquire", "scheduler-acquire", "coordinator-assign"} {
		t.Run(entrypoint, func(t *testing.T) {
			fixture := newStageSchedulerFixture(t, "quorum-"+entrypoint)
			enableStageQuorumRequirement(t, fixture.database)
			var err error
			fixture.repository, err = stagescheduler.NewPostgresRepository(newRolePool(t, fixture.database.DSN,
				"vela_stage_scheduler_login", "vela-stage-scheduler-password"))
			if err != nil {
				t.Fatal(err)
			}
			fixture.coordinator, err = attemptcoordinator.NewService(newRolePool(t, fixture.database.DSN,
				"vela_attempt_coordinator_login", "vela-attempt-coordinator-password"))
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			switch entrypoint {
			case "worker-acquire":
				_, err = newPostgresAssignmentTestBackend(t, fixture).AcquireStage(ctx,
					stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture))
			case "scheduler-acquire":
				_, _, err = newStageSchedulerTestService(t, fixture).Acquire(ctx, fixture.authority, fixture.observation)
			case "coordinator-assign":
				_, err = fixture.coordinator.Apply(ctx, stageQuorumAssignment(t, fixture))
			}
			var failure *pgconn.PgError
			if !errors.As(err, &failure) || failure.Code != "55000" || failure.Message != "synchronous replication quorum is unavailable" {
				t.Fatalf("%s without required quorum error=%v, want exact quorum rejection", entrypoint, err)
			}
			var writes int
			if err := fixture.database.Admin.QueryRow(`SELECT
				(SELECT count(*) FROM stage_worker_acquire_intents) +
				(SELECT count(*) FROM stage_scheduler_snapshot_traces) +
				(SELECT count(*) FROM stage_scheduler_claims) +
				(SELECT count(*) FROM stage_attempts) +
				(SELECT count(*) FROM stage_allocations) +
				(SELECT count(*) FROM stage_leases)`).Scan(&writes); err != nil || writes != 0 {
				t.Fatalf("rejected %s left authority writes=%d error=%v", entrypoint, writes, err)
			}
		})
	}
}

func TestStageQuorumRequirementPreservesDurableAssignmentReplay(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "quorum-replay")
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	original, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(context.Background(), command, request)
	if err != nil || original.Assignment == nil {
		t.Fatalf("initial assignment=%#v error=%v", original, err)
	}
	enableStageQuorumRequirement(t, fixture.database)
	replay, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(context.Background(), command, request)
	if err != nil || !proto.Equal(original.Assignment, replay.Assignment) {
		t.Fatalf("quorum requirement altered durable replay=%#v error=%v", replay, err)
	}
}

func TestStageQuorumRequirementRejectsRenewalWithoutBillableStart(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "quorum-renewal")
	command := stageWorkerAcquireCommand(fixture)
	assigned, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(context.Background(),
		command, stageWorkerAcquireRequest(fixture))
	if err != nil || assigned.Assignment == nil {
		t.Fatalf("initial assignment=%#v error=%v", assigned, err)
	}
	enableStageQuorumRequirement(t, fixture.database)
	_, err = startIntegrationAssignedStage(t, fixture.database, command, assigned.Assignment)
	var failure *pgconn.PgError
	if !errors.As(err, &failure) || failure.Code != "55000" || failure.Message != "synchronous replication quorum is unavailable" {
		t.Fatalf("Start without required quorum error=%v, want exact quorum rejection", err)
	}
	var state string
	var billable bool
	var renewals int
	if err := fixture.database.Admin.QueryRow(`SELECT run.state::text, job.billable_started_at IS NOT NULL,
		(SELECT count(*) FROM stage_authority_renewals)
		FROM stage_runs AS run JOIN attempts AS attempt ON attempt.id = run.attempt_id
		JOIN jobs AS job ON job.id = attempt.job_id WHERE run.id = $1`, fixture.stageRunID).
		Scan(&state, &billable, &renewals); err != nil || state != "ASSIGNED" || billable || renewals != 0 {
		t.Fatalf("rejected renewal left stage=%s billable=%t renewals=%d error=%v", state, billable, renewals, err)
	}
}

func TestStageQuorumRequirementPreservesReadOnlyIdleHint(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "quorum-idle")
	if _, err := fixture.database.Admin.Exec(`UPDATE credentials
		SET scopes = ARRAY['jobs:submit', 'jobs:read', 'jobs:cancel'] WHERE id = $1`, testCredentialID); err != nil {
		t.Fatal(err)
	}
	var jobID string
	if err := fixture.database.Admin.QueryRow(`SELECT attempt.job_id FROM stage_runs AS run
		JOIN attempts AS attempt ON attempt.id = run.attempt_id WHERE run.id = $1`, fixture.stageRunID).
		Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	server := admissionServerForDatabase(t, fixture.database)
	if result := cancelJob(t, server.URL, testProjectID, jobID, testBearerCredential()); result.StatusCode != http.StatusOK {
		t.Fatalf("cancel idle fixture: %s", result.Body)
	}
	enableStageQuorumRequirement(t, fixture.database)
	result, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(context.Background(),
		stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture))
	if err != nil || result.Assignment != nil || result.Command != nil || result.RetryAfter <= 0 {
		t.Fatalf("quorum gate blocked read-only idle hint=%#v error=%v", result, err)
	}
}

func TestStageQuorumMigrationRoundTripRestoresExistingGuard(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 80)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	var before string
	if err := database.Admin.QueryRow(`SELECT pg_get_functiondef('vela_enforce_synchronous_quorum()'::regprocedure)`).Scan(&before); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := goose.UpTo(database.Admin, migrations, 81); err != nil {
			t.Fatal(err)
		}
		var guards int
		if err := database.Admin.QueryRow(`SELECT count(*) FROM pg_trigger
			WHERE tgname LIKE 'stage_%_require_synchronous_quorum'
			AND tgfoid = 'vela_enforce_synchronous_quorum()'::regprocedure
			AND tgdeferrable AND tginitdeferred AND NOT tgisinternal`).Scan(&guards); err != nil || guards != 5 {
			t.Fatalf("new deferred Stage quorum guards=%d error=%v", guards, err)
		}
		if err := goose.DownTo(database.Admin, migrations, 80); err != nil {
			t.Fatal(err)
		}
		var after string
		if err := database.Admin.QueryRow(`SELECT pg_get_functiondef('vela_enforce_synchronous_quorum()'::regprocedure)`).Scan(&after); err != nil || before != after {
			t.Fatalf("Stage quorum Down changed existing guard: %v", err)
		}
	}
}

func enableStageQuorumRequirement(t *testing.T, database testDatabase) {
	t.Helper()
	var name string
	if err := database.Admin.QueryRow(`SELECT current_database()`).Scan(&name); err != nil {
		t.Fatal(err)
	}
	if _, err := database.Admin.Exec("ALTER DATABASE " + pgx.Identifier{name}.Sanitize() +
		` SET "vela.require_synchronous_quorum" = 'on'`); err != nil {
		t.Fatal(err)
	}
}

func stageQuorumAssignment(t *testing.T, fixture stageSchedulerFixture) attemptcoordinator.AssignStageCommand {
	t.Helper()
	var attemptID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`SELECT attempt_id FROM stage_runs WHERE id = $1`, fixture.stageRunID).
		Scan(&attemptID); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	digest := sha256.Sum256([]byte("quorum-stage-token"))
	authority := fixture.authority
	return attemptcoordinator.AssignStageCommand{
		CommandID: uuid.New(), AttemptID: attemptID, StageRunID: fixture.stageRunID,
		ExpectedAttemptFence: 1, ExpectedStageFence: 1, ExpectedStageVersion: 1,
		StageAttemptID: uuid.New(), StageAllocationID: uuid.New(), StageLeaseID: uuid.New(),
		StageProfileRevisionID: authority.StageProfileRevisionID, CapacityPoolID: authority.CapacityPoolID,
		WorkerInstanceID: authority.WorkerInstanceID, WorkerInstanceEpoch: authority.WorkerInstanceEpoch,
		ObservationSequence: fixture.observation.Sequence,
		DeviceSetDigest:     authority.DeviceSetDigest, MembershipDigest: authority.MembershipDigest,
		ModelResidencyID: authority.ModelResidencyID, ModelRuntimeEpoch: authority.ModelRuntimeEpoch,
		CapacityVector: authority.CapacityVector, TokenDigest: digest[:], SigningKeyID: "stage-authority-key-v1",
		ExecutionNonce: bytesOf(0xa5, 32), IssuedAt: now, ExpiresAt: now.Add(time.Minute), LocalDeadlineAt: now.Add(50 * time.Second),
	}
}

func startIntegrationAssignedStage(t *testing.T, database testDatabase, command stageworkercontrol.CommandContext,
	assignment *velav1.StageAssignment) (stageworkercontrol.CommandResult, error) {
	t.Helper()
	keys := map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)}
	validator, err := stageauthority.NewValidator(keys, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := validator.ValidateEnvelope(assignment.GetAuthority())
	if err != nil {
		t.Fatal(err)
	}
	signer, err := stageauthority.NewSigner(keys)
	if err != nil {
		t.Fatal(err)
	}
	execution, err := stageworkercontrol.NewPostgresExecutionBackend(newRolePool(t, database.DSN,
		"vela_stage_worker_control_login", "vela-stage-worker-control-password"), signer,
		stageworkercontrol.PostgresExecutionConfig{ActiveSigningKeyID: "stage-authority-key-v1",
			AuthorityTTL: 2 * time.Minute, LocalDeadlineTTL: 90 * time.Second, MaxClockSkew: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	command.CommandID = uuid.New()
	return execution.StartStage(context.Background(), command,
		&velav1.StartStageRequest{Authority: assignment.GetAuthority(), StartedAt: timestamppb.Now()},
		stageworkercontrol.VerifiedAuthorities{Stage: &verified})
}
