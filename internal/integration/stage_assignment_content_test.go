//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pressly/goose/v3"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/retention"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestStageAssignmentContentDeletionRetiresDeliveryButPreservesAuthority(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "assignment-content-deletion")
	ctx := context.Background()
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	acquired, err := backend.AcquireStage(ctx, command, request)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	authority := acquired.Assignment.GetAuthority()
	replayed, err := backend.AcquireStage(ctx, command, request)
	if err != nil || !proto.Equal(replayed.Assignment, acquired.Assignment) {
		t.Fatalf("active delivery replay changed: %v", err)
	}
	failTerminalHistoryAssignment(t, fixture, authority)
	service, principal := contentDeletionAuthority(t, fixture.database)
	deletion, err := service.AcceptContentDeletion(ctx, principal, uuid.MustParse(testProjectID),
		uuid.MustParse(authority.GetJobId()), "assignment-delivery-delete")
	if err != nil {
		t.Fatal(err)
	}
	reconciler := newStageLifecycleReconciler(t, fixture.database, &recordingRetentionStore{})
	if _, err := reconciler.ReconcileBatch(ctx); err != nil {
		t.Fatal(err)
	}
	deleted, err := service.GetContentDeletion(ctx, principal, uuid.MustParse(testProjectID), deletion.RequestID)
	if err != nil || deleted.State != retention.DeletionStateCompleted {
		t.Fatalf("content deletion state=%s error=%v", deleted.State, err)
	}
	replayed, err = backend.AcquireStage(ctx, command, request)
	if err != nil || replayed.Assignment != nil || replayed.Command == nil ||
		replayed.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED ||
		replayed.Command.Detail != "ASSIGNMENT_CONTENT_DELETED" {
		t.Fatalf("completed content deletion still permits original delivery: assignment=%t command=%v error=%v",
			replayed.Assignment != nil, replayed.Command, err)
	}
	reader := newTerminalHistoryReaderForTest(t, fixture, authority)
	history, err := reader.Read(ctx, command, authority, command.CommandID)
	if err != nil || history == nil || history.StageRunID.String() != authority.GetStageRunId() {
		t.Fatalf("content deletion erased historical authority: history=%v error=%v", history, err)
	}
	assertAssignmentDeliveryRetired(t, fixture.database, command.CommandID)
}

func TestStageAssignmentContentRetentionExpiresOriginalDelivery(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "assignment-content-retention")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	acquired, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	setAssignmentContentExpiry(t, fixture.database, acquired.Assignment.GetAuthority().GetJobId(), time.Now().Add(-time.Second))
	result, err := newStageLifecycleReconciler(t, fixture.database, &recordingRetentionStore{}).ReconcileBatch(context.Background())
	if err != nil || result.RequestContentExpired != 1 {
		t.Fatalf("retention result=%+v error=%v", result, err)
	}
	replayed, err := backend.AcquireStage(context.Background(), command, request)
	assertAssignmentDeletedResult(t, replayed, err)
	assertAssignmentDeliveryRetired(t, fixture.database, command.CommandID)
}

func TestStageAssignmentContentCompletionAfterDeletionCannotResurrectDelivery(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "assignment-completion-after-delete")
	// Pause after Go has assembled and signed the delivery, before its final SQL
	// transaction locks the Job. Cancellation and deletion run through public services.
	if _, err := fixture.database.Admin.Exec(`DO $test$
	DECLARE definition text;
	BEGIN
		definition := pg_get_functiondef('vela_complete_stage_worker_acquire(jsonb)'::regprocedure);
		EXECUTE replace(definition, '    PERFORM 1 FROM public.jobs',
			E'    PERFORM pg_advisory_xact_lock(90001);\n    PERFORM 1 FROM public.jobs');
	END $test$`); err != nil {
		t.Fatal(err)
	}
	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec(`SELECT pg_advisory_xact_lock(90001)`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	done := make(chan assignmentContentResponse, 1)
	go func() {
		result, err := backend.AcquireStage(ctx, command, request)
		done <- assignmentContentResponse{result, err}
	}()
	waitAssignmentControlLock(t, fixture.database, "advisory")
	var jobID uuid.UUID
	if err := fixture.database.Admin.QueryRow(`SELECT attempt.job_id FROM stage_runs AS run
		JOIN attempts AS attempt ON attempt.id = run.attempt_id WHERE run.id = $1`, fixture.stageRunID).Scan(&jobID); err != nil {
		t.Fatal(err)
	}
	service, principal := contentDeletionAuthority(t, fixture.database)
	server := admissionServerForDatabase(t, fixture.database)
	if response := cancelJob(t, server.URL, testProjectID, jobID.String(), testBearerCredential()); response.StatusCode != http.StatusOK {
		t.Fatalf("cancel before late completion: %d", response.StatusCode)
	}
	if _, err := service.AcceptContentDeletion(ctx, principal, uuid.MustParse(testProjectID), jobID, "late-assignment-delete"); err != nil {
		t.Fatal(err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	response := <-done
	assertAssignmentDeletedResult(t, response.result, response.err)
	assertAssignmentDeliveryRetired(t, fixture.database, command.CommandID)
	replayed, err := backend.AcquireStage(ctx, command, request)
	assertAssignmentDeletedResult(t, replayed, err)
	var receipts int
	if err := fixture.database.Admin.QueryRow(`SELECT count(*) FROM stage_assignment_authority_receipts
		WHERE command_id = $1`, command.CommandID).Scan(&receipts); err != nil || receipts != 1 {
		t.Fatalf("late completion authority receipts=%d error=%v", receipts, err)
	}
}

func TestStageAssignmentContentReplayChecksDeadlineAfterJobLock(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "assignment-replay-deadline")
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	acquired, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	jobID := acquired.Assignment.GetAuthority().GetJobId()
	expiresAt := time.Now().Add(800 * time.Millisecond)
	setAssignmentContentExpiry(t, fixture.database, jobID, expiresAt)
	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	if _, err := blocker.Exec(`SELECT id FROM jobs WHERE id = $1 FOR UPDATE`, jobID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan assignmentContentResponse, 1)
	go func() {
		result, err := backend.AcquireStage(ctx, command, request)
		done <- assignmentContentResponse{result, err}
	}()
	waitAssignmentControlLock(t, fixture.database, "transactionid")
	if time.Now().After(expiresAt) {
		t.Fatal("replay did not start its lock wait before content expiry")
	}
	waitStageExpiry(t, expiresAt)
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	response := <-done
	assertAssignmentDeletedResult(t, response.result, response.err)
	assertAssignmentDeliveryRetired(t, fixture.database, command.CommandID)
}

func TestStageAssignmentContentLegacyBackfillPreservesOriginalWire(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "assignment-legacy-backfill")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(fixture.database.Admin, migrations, 89); err != nil {
		t.Fatal(err)
	}
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	acquired, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("legacy Acquire failed: %v", err)
	}
	original := legacyAssignmentWire(t, fixture.database, command.CommandID)
	// Preserve unknown fields of the *delivery*, without accepting unknown fields
	// in the retained authority. The original delivery hash covers these bytes.
	original = protowire.AppendBytes(protowire.AppendTag(original, 10000, protowire.BytesType), []byte("legacy-customer-extension"))
	setLegacyAssignmentWire(t, fixture.database, command.CommandID, original)
	if err := goose.UpTo(fixture.database.Admin, migrations, 90); err != nil {
		t.Fatal(err)
	}
	worker := newRolePool(t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	if err := veladb.VerifyRole(context.Background(), worker, veladb.RoleStageWorkerControl); err == nil {
		t.Fatal("startup accepted pending legacy assignment history")
	}
	if result, err := backend.AcquireStage(context.Background(), command, request); err == nil || result.Assignment != nil {
		t.Fatal("unverified legacy delivery was replayed")
	}
	legacyCompletion, err := json.Marshal(map[string]any{"schema_version": 1, "command_id": command.CommandID,
		"result_kind": "ASSIGNMENT", "assignment_wire": hex.EncodeToString(original)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := worker.Exec(context.Background(), `SELECT * FROM vela_complete_stage_worker_acquire($1::jsonb)`, legacyCompletion); err == nil {
		t.Fatal("schema1 writer crossed the durable authority-evidence fence")
	}
	pool := assignmentHistoryMigrationPool(t, fixture.database)
	validator := assignmentHistoryValidator(t, acquired.Assignment.GetAuthority())
	if count, err := stageworkercontrol.BackfillAssignmentHistory(context.Background(), pool, validator, 1); err != nil || count != 1 {
		t.Fatalf("backfill count=%d error=%v", count, err)
	}
	if count, err := stageworkercontrol.BackfillAssignmentHistory(context.Background(), pool, validator, 1); err != nil || count != 0 {
		t.Fatalf("backfill replay count=%d error=%v", count, err)
	}
	if err := veladb.VerifyRole(context.Background(), worker, veladb.RoleStageWorkerControl); err != nil {
		t.Fatalf("startup after backfill: %v", err)
	}
	if wire := legacyAssignmentWire(t, fixture.database, command.CommandID); !bytes.Equal(wire, original) {
		t.Fatal("backfill rewrote original delivery bytes")
	}
	replayed, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || replayed.Assignment == nil || !proto.Equal(replayed.Assignment.GetAuthority(), acquired.Assignment.GetAuthority()) {
		t.Fatalf("backfilled original replay error=%v", err)
	}
	var receiptWire, receiptDigest []byte
	if err := fixture.database.Admin.QueryRow(`SELECT authority_wire, assignment_digest FROM stage_assignment_authority_receipts
		WHERE command_id = $1`, command.CommandID).Scan(&receiptWire, &receiptDigest); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(original)
	if !bytes.Equal(receiptDigest, digest[:]) || bytes.Contains(receiptWire, []byte("legacy-customer-extension")) {
		t.Fatal("authority receipt did not separate delivery content and original digest")
	}
	if err := goose.DownTo(fixture.database.Admin, migrations, 89); err == nil || !strings.Contains(err.Error(), "cannot be rolled back") {
		t.Fatalf("Down lost retained authority evidence: %v", err)
	}
}

func TestStageAssignmentContentBackfillRetiresAlreadyDeletedLegacyJob(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "assignment-already-deleted-legacy")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(fixture.database.Admin, migrations, 89); err != nil {
		t.Fatal(err)
	}
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	acquired, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("legacy Acquire failed: %v", err)
	}
	authority := acquired.Assignment.GetAuthority()
	failTerminalHistoryAssignment(t, fixture, authority)
	service, principal := contentDeletionAuthority(t, fixture.database)
	deletion, err := service.AcceptContentDeletion(context.Background(), principal, uuid.MustParse(testProjectID),
		uuid.MustParse(authority.GetJobId()), "legacy-already-deleted")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := newStageLifecycleReconciler(t, fixture.database, &recordingRetentionStore{}).ReconcileBatch(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(legacyAssignmentWire(t, fixture.database, command.CommandID)) == 0 {
		t.Fatal("fixture did not reproduce retained schema89 assignment content")
	}
	if err := goose.UpTo(fixture.database.Admin, migrations, 90); err != nil {
		t.Fatal(err)
	}
	// The completion guard applies even to a task accepted before the upgrade,
	// where no new Job UPDATE would run. Exercise it in a rolled-back transaction.
	tx, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`UPDATE content_deletion_requests SET state = 'COMPLETED' WHERE id = $1`, deletion.RequestID); err == nil || !strings.Contains(err.Error(), "backfill must finish") {
		t.Fatalf("legacy deletion completion bypassed pending history: %v", err)
	}
	_ = tx.Rollback()
	pool := assignmentHistoryMigrationPool(t, fixture.database)
	if count, err := stageworkercontrol.BackfillAssignmentHistory(context.Background(), pool, assignmentHistoryValidator(t, authority), 10); err != nil || count != 1 {
		t.Fatalf("deleted legacy backfill count=%d error=%v", count, err)
	}
	assertAssignmentDeliveryRetired(t, fixture.database, command.CommandID)
	replayed, err := backend.AcquireStage(context.Background(), command, request)
	assertAssignmentDeletedResult(t, replayed, err)
	if history, err := newTerminalHistoryReaderForTest(t, fixture, authority).Read(context.Background(), command, authority, command.CommandID); err != nil || history == nil {
		t.Fatalf("verified deleted legacy history=%v error=%v", history, err)
	}
}

func TestStageAssignmentContentMigrationRoleAndRetirementBoundary(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "assignment-history-role-boundary")
	assignmentHistoryMigrationPool(t, fixture.database)
	for _, role := range []string{"vela_stage_worker_control", "vela_assignment_history_migration", "vela_retention", "vela_request"} {
		for _, function := range []string{"vela_read_stage_assignment_history_backfill(integer)", "vela_record_stage_assignment_authority(jsonb)", "vela_retire_unverifiable_stage_assignment(jsonb)"} {
			var allowed bool
			if err := fixture.database.Admin.QueryRow(`SELECT has_function_privilege($1, $2, 'EXECUTE')`, role, function).Scan(&allowed); err != nil || allowed != (role == "vela_assignment_history_migration") {
				t.Fatalf("migration function=%s role=%s allowed=%t error=%v", function, role, allowed, err)
			}
		}
		for _, table := range []string{"stage_assignment_authority_receipts", "stage_assignment_unverifiable_tombstones", "stage_assignment_history_pending", "stage_worker_acquire_results"} {
			var allowed bool
			if err := fixture.database.Admin.QueryRow(`SELECT has_table_privilege($1, $2, 'SELECT,INSERT,UPDATE,DELETE')`, role, table).Scan(&allowed); err != nil || allowed {
				t.Fatalf("direct history table=%s role=%s allowed=%t error=%v", table, role, allowed, err)
			}
		}
		for _, function := range []string{"vela_complete_stage_worker_acquire_v89(jsonb)", "vela_begin_stage_worker_acquire_v89(jsonb)", "vela_read_stage_terminal_history_v89(jsonb)", "vela_read_stage_assignment_delivery(uuid)"} {
			var allowed bool
			if err := fixture.database.Admin.QueryRow(`SELECT has_function_privilege($1, $2, 'EXECUTE')`, role, function).Scan(&allowed); err != nil || allowed {
				t.Fatalf("private history function=%s role=%s allowed=%t error=%v", function, role, allowed, err)
			}
		}
	}
	backend := newPostgresAssignmentTestBackend(t, fixture)
	command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
	acquired, err := backend.AcquireStage(context.Background(), command, request)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire failed: %v", err)
	}
	for _, query := range []string{
		`UPDATE stage_assignment_authority_receipts SET authority_wire = authority_wire WHERE command_id = $1`,
		`DELETE FROM stage_assignment_authority_receipts WHERE command_id = $1`,
		`UPDATE stage_worker_acquire_results SET assignment_digest = decode(repeat('00',32),'hex') WHERE command_id = $1`,
		`UPDATE stage_worker_acquire_results SET result_kind = 'REJECTED', detail = 'changed', assignment_wire = NULL WHERE command_id = $1`,
	} {
		if _, err := fixture.database.Admin.Exec(query, command.CommandID); err == nil {
			t.Fatalf("immutable historical result accepted %s", query)
		}
	}
}

func TestStageAssignmentContentBackfillDoesNotBlockExpiryWhileWaitingForRoot(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "assignment-backfill-expiry-lock-order")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.DownTo(fixture.database.Admin, migrations, 89); err != nil {
		t.Fatal(err)
	}
	command := stageWorkerAcquireCommand(fixture)
	acquired, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(context.Background(), command, stageWorkerAcquireRequest(fixture))
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("legacy Acquire failed: %v", err)
	}
	authority := acquired.Assignment.GetAuthority()
	if err := goose.UpTo(fixture.database.Admin, migrations, 90); err != nil {
		t.Fatal(err)
	}
	pool := assignmentHistoryMigrationPool(t, fixture.database)
	validator := assignmentHistoryValidator(t, authority)
	blocker, err := fixture.database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = blocker.Rollback() }()
	// Metadata expiry takes this root lock in its legal-hold check before it
	// locks/deletes the live Job. Backfill must leave that second lock available.
	if _, err := blocker.Exec(`SELECT id FROM non_content_job_roots WHERE id = $1 FOR UPDATE`, authority.GetJobId()); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := stageworkercontrol.BackfillAssignmentHistory(ctx, pool, validator, 1)
		done <- err
	}()
	waitAssignmentRoleLock(t, fixture.database, "vela_assignment_history_migration_login", "transactionid")
	if _, err := blocker.Exec(`SELECT id FROM jobs WHERE id = $1 FOR UPDATE NOWAIT`, authority.GetJobId()); err != nil {
		t.Fatalf("backfill locked the live Job while waiting for the expiry-owned root: %v", err)
	}
	if err := blocker.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatalf("backfill after expiry lock release: %v", err)
	}
}

func TestStageAssignmentContentMigrationRefusesActiveWritersWithoutPartialChange(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "assignment-migration-active-writer")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	for _, version := range []int64{90, 89} {
		for _, relation := range []string{"non_content_job_roots", "content_deletion_requests"} {
			blocker, err := fixture.database.Admin.Begin()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := blocker.Exec("LOCK TABLE " + relation + " IN ROW EXCLUSIVE MODE"); err != nil {
				_ = blocker.Rollback()
				t.Fatal(err)
			}
			if version == 90 {
				err = goose.DownTo(fixture.database.Admin, migrations, 89)
			} else {
				err = goose.UpTo(fixture.database.Admin, migrations, 90)
			}
			_ = blocker.Rollback()
			if err == nil || !strings.Contains(err.Error(), "55P03") {
				t.Fatalf("schema%d migration did not reject the active %s writer: %v", version, relation, err)
			}
			actual, err := goose.GetDBVersion(fixture.database.Admin)
			if err != nil || actual != version {
				t.Fatalf("blocked migration changed version %d to %d: %v", version, actual, err)
			}
			var readyFunction bool
			if err := fixture.database.Admin.QueryRow(`SELECT to_regprocedure('vela_stage_assignment_history_ready()') IS NOT NULL`).Scan(&readyFunction); err != nil || readyFunction != (version == 90) {
				t.Fatalf("blocked migration partially changed function contract: ready=%t error=%v", readyFunction, err)
			}
		}
		var err error
		if version == 90 {
			err = goose.DownTo(fixture.database.Admin, migrations, 89)
		} else {
			err = goose.UpTo(fixture.database.Admin, migrations, 90)
		}
		if err != nil {
			t.Fatal(err)
		}
	}
}

func TestStageAssignmentContentUnverifiableLegacyRequiresExplicitRetirement(t *testing.T) {
	for _, kind := range []string{"malformed", "missing_key", "mapping", "missing_wire"} {
		t.Run(kind, func(t *testing.T) {
			fixture := newStageSchedulerFixture(t, "assignment-unverifiable-"+kind)
			migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
			if err := goose.DownTo(fixture.database.Admin, migrations, 89); err != nil {
				t.Fatal(err)
			}
			backend := newPostgresAssignmentTestBackend(t, fixture)
			command, request := stageWorkerAcquireCommand(fixture), stageWorkerAcquireRequest(fixture)
			acquired, err := backend.AcquireStage(context.Background(), command, request)
			if err != nil || acquired.Assignment == nil {
				t.Fatalf("legacy Acquire failed: %v", err)
			}
			original := legacyAssignmentWire(t, fixture.database, command.CommandID)
			validator := assignmentHistoryValidator(t, acquired.Assignment.GetAuthority())
			switch kind {
			case "malformed":
				original = []byte("private-legacy-content")
			case "missing_key":
				validator, err = stageauthority.NewVerifier(map[string][]byte{"different-key": bytes.Repeat([]byte{0x9a}, 32)}, nil)
				if err != nil {
					t.Fatal(err)
				}
			case "mapping":
				changed := proto.Clone(acquired.Assignment).(*velav1.StageAssignment)
				changed.Authority.JobId = uuid.NewString()
				signer, err := stageauthority.NewSigner(map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)})
				if err != nil {
					t.Fatal(err)
				}
				changed.Authority, err = signer.Sign(changed.Authority)
				if err != nil {
					t.Fatal(err)
				}
				original, err = proto.Marshal(changed)
				if err != nil {
					t.Fatal(err)
				}
			case "missing_wire":
				original = nil
			}
			setLegacyAssignmentWire(t, fixture.database, command.CommandID, original)
			if err := goose.UpTo(fixture.database.Admin, migrations, 90); err != nil {
				t.Fatal(err)
			}
			pool := assignmentHistoryMigrationPool(t, fixture.database)
			if kind != "missing_wire" {
				if count, err := stageworkercontrol.BackfillAssignmentHistory(context.Background(), pool, validator, 1); err == nil || count != 0 || strings.Contains(err.Error(), "private-legacy-content") {
					t.Fatalf("strict backfill count=%d error=%v", count, err)
				}
				if wire := legacyAssignmentWire(t, fixture.database, command.CommandID); !bytes.Equal(wire, original) {
					t.Fatal("strict failure removed unverified delivery")
				}
				if count, err := stageworkercontrol.BackfillAssignmentHistoryWithOptions(context.Background(), pool, validator, 1,
					stageworkercontrol.AssignmentHistoryMigrationOptions{RetireUnverifiable: true}); err != nil || count != 1 {
					t.Fatalf("explicit retirement count=%d error=%v", count, err)
				}
			}
			assertAssignmentDeliveryRetired(t, fixture.database, command.CommandID)
			var receipts, tombstones, pending int
			if err := fixture.database.Admin.QueryRow(`SELECT
				(SELECT count(*) FROM stage_assignment_authority_receipts),
				(SELECT count(*) FROM stage_assignment_unverifiable_tombstones),
				(SELECT count(*) FROM stage_assignment_history_pending)`).Scan(&receipts, &tombstones, &pending); err != nil || receipts != 0 || tombstones != 1 || pending != 0 {
				t.Fatalf("retirement receipts=%d tombstones=%d pending=%d error=%v", receipts, tombstones, pending, err)
			}
			var reason string
			if err := fixture.database.Admin.QueryRow(`SELECT reason FROM stage_assignment_unverifiable_tombstones
				WHERE command_id = $1`, command.CommandID).Scan(&reason); err != nil {
				t.Fatal(err)
			}
			if expected := map[string]string{"malformed": "INVALID_ASSIGNMENT_PROTOBUF", "missing_key": "UNKNOWN_SIGNING_KEY",
				"mapping": "AUTHORITY_MAPPING_MISMATCH", "missing_wire": "MISSING_ASSIGNMENT_WIRE"}[kind]; reason != expected {
				t.Fatalf("retirement reason=%s want %s", reason, expected)
			}
			replayed, err := backend.AcquireStage(context.Background(), command, request)
			assertAssignmentDeletedResult(t, replayed, err)
		})
	}
}

type assignmentContentResponse struct {
	result stageworkercontrol.AcquireResult
	err    error
}

func assertAssignmentDeletedResult(t *testing.T, result stageworkercontrol.AcquireResult, err error) {
	t.Helper()
	if err != nil || result.Assignment != nil || result.Command == nil ||
		result.Command.Decision != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REJECTED ||
		result.Command.Detail != "ASSIGNMENT_CONTENT_DELETED" {
		t.Fatalf("deleted delivery assignment=%t command=%v error=%v", result.Assignment != nil, result.Command, err)
	}
}

func assertAssignmentDeliveryRetired(t *testing.T, database testDatabase, commandID uuid.UUID) {
	t.Helper()
	var retired bool
	if err := database.Admin.QueryRow(`SELECT result_kind = 'ASSIGNMENT' AND assignment_wire IS NULL
		AND assignment_digest IS NOT NULL AND assignment_retired_at IS NOT NULL
		FROM stage_worker_acquire_results WHERE command_id = $1`, commandID).Scan(&retired); err != nil || !retired {
		t.Fatalf("delivery storage retired=%t error=%v", retired, err)
	}
}

func legacyAssignmentWire(t *testing.T, database testDatabase, commandID uuid.UUID) []byte {
	t.Helper()
	var wire []byte
	if err := database.Admin.QueryRow(`SELECT assignment_wire FROM stage_worker_acquire_results WHERE command_id = $1`, commandID).Scan(&wire); err != nil {
		t.Fatal(err)
	}
	return wire
}

func setLegacyAssignmentWire(t *testing.T, database testDatabase, commandID uuid.UUID, wire []byte) {
	t.Helper()
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SET LOCAL session_replication_role = 'replica'`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE stage_worker_acquire_results SET assignment_wire = $2 WHERE command_id = $1`, commandID, wire); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func setAssignmentContentExpiry(t *testing.T, database testDatabase, jobID string, expiresAt time.Time) {
	t.Helper()
	tx, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.Exec(`SET LOCAL session_replication_role = 'replica'`); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(`UPDATE jobs SET created_at = $2::timestamptz - interval '40 days',
		job_expires_at = $2::timestamptz - interval '30 days', request_content_expires_at = $2 WHERE id = $1`, jobID, expiresAt); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
}

func assignmentHistoryMigrationPool(t *testing.T, database testDatabase) *pgxpool.Pool {
	t.Helper()
	if _, err := database.Admin.Exec(`CREATE ROLE vela_assignment_history_migration_login LOGIN
		PASSWORD 'vela-assignment-history-password' IN ROLE vela_assignment_history_migration`); err != nil {
		t.Fatal(err)
	}
	pool := newRolePool(t, database.DSN, "vela_assignment_history_migration_login", "vela-assignment-history-password")
	config := pool.Config()
	config.MaxConns = 1
	single, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(single.Close)
	if err := veladb.VerifyRole(context.Background(), single, veladb.RoleAssignmentHistoryMigration); err != nil {
		t.Fatal(err)
	}
	return single
}

func assignmentHistoryValidator(t *testing.T, authority *velav1.StageAuthority) *stageauthority.Validator {
	t.Helper()
	keys, err := stageauthority.DeriveVerifierKeyring(map[string][]byte{"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32)})
	if err != nil {
		t.Fatal(err)
	}
	defer stageauthority.ClearKeyring(keys)
	validator, err := stageauthority.NewVerifier(keys, func() time.Time { return authority.GetExpiresAt().AsTime().Add(time.Hour) })
	if err != nil {
		t.Fatal(err)
	}
	return validator
}

func waitAssignmentControlLock(t *testing.T, database testDatabase, event string) {
	t.Helper()
	waitAssignmentRoleLock(t, database, "vela_stage_worker_control_login", event)
}

func waitAssignmentRoleLock(t *testing.T, database testDatabase, role, event string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		var waiting bool
		if err := database.Admin.QueryRow(`SELECT EXISTS (SELECT 1 FROM pg_stat_activity
			WHERE usename = $1 AND wait_event = $2)`, role, event).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		if waiting {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("assignment did not reach expected database lock")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
