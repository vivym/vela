//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"maps"
	"net/http"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/attemptcoordinator"
	veladb "github.com/vivym/vela/internal/database"
	"github.com/vivym/vela/internal/noncontentexpiry"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkercontrol"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestStageTerminalHistoryRequiresTerminalScope(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "terminal-history")
	command := stageWorkerAcquireCommand(fixture)
	acquired, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(), command, stageWorkerAcquireRequest(fixture),
	)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire: %v %v", acquired, err)
	}
	authority := acquired.Assignment.GetAuthority()
	request := terminalHistoryRequest(t, command, authority)
	before := readTerminalHistory(t, fixture, request)
	if before.Eligible || before.Reason != "STAGE_RUN_NOT_TERMINAL" {
		t.Fatalf("live StageRun history = %+v", before)
	}
	failTerminalHistoryAssignment(t, fixture, authority)
	after := readTerminalHistory(t, fixture, request)
	if !after.Eligible || after.Cutoff != authority.GetExecutionSequence() ||
		after.TerminalState != "FAILED" || after.StageFence <= authority.GetStageFence() ||
		after.StageVersion <= authority.GetStageVersion() || len(after.Allocations) != 1 {
		t.Fatalf("terminal StageRun history = %+v", after)
	}
	wire, err := hex.DecodeString(after.AuthorityWire)
	if err != nil {
		t.Fatal(err)
	}
	var recorded velav1.StageAuthority
	if err := proto.Unmarshal(wire, &recorded); err != nil || !proto.Equal(&recorded, authority) || after.AssignmentWire != "" {
		t.Fatalf("historical original authority changed: %v", err)
	}
	if after.WorkerMemberID != authority.GetMembers()[0].GetWorkerMemberId() || after.ObservedAt.IsZero() {
		t.Fatal("terminal history omitted authenticated member or observation time")
	}
	for _, test := range []struct {
		field string
		value any
	}{
		{"schema_version", 2}, {"worker_instance_epoch", 0}, {"execution_sequence", 0},
		{"control_session_epoch", command.ControlSessionEpoch + 1},
		{"job_id", uuid.NewString()}, {"attempt_id", uuid.NewString()},
		{"stage_run_id", uuid.NewString()}, {"stage_attempt_id", uuid.NewString()},
		{"stage_allocation_id", uuid.NewString()}, {"stage_lease_id", uuid.NewString()},
		{"worker_instance_id", uuid.NewString()}, {"model_residency_id", uuid.NewString()},
		{"stage_profile_revision_id", uuid.NewString()}, {"acquire_command_id", uuid.NewString()},
		{"capacity_observation_sequence", authority.GetCapacityObservationSequence() + 1},
		{"model_runtime_barrier_generation", authority.GetModelRuntimeBarrierGeneration() + 1},
		{"execution_sequence", authority.GetExecutionSequence() + 1},
		{"token_digest", hex.EncodeToString(make([]byte, sha256.Size))},
		{"spiffe_id_digest", hex.EncodeToString(make([]byte, sha256.Size))},
	} {
		t.Run(test.field, func(t *testing.T) {
			changed := maps.Clone(request)
			changed[test.field] = test.value
			snapshot := readTerminalHistory(t, fixture, changed)
			if snapshot.Eligible || snapshot.Reason == "" || len(snapshot.Allocations) != 0 ||
				snapshot.AssignmentWire != "" || snapshot.AuthorityWire != "" || snapshot.RenewalWire != "" {
				t.Fatalf("mismatched %s disclosed terminal history", test.field)
			}
		})
	}
}

func TestStageTerminalHistoryCoversAllocatedUndeliveredRetry(t *testing.T) {
	assertStageTerminalHistoryUndeliveredRetry(t, false)
}

func TestStageTerminalRecoveryThroughReplacementRuntimeRestoresCapacity(t *testing.T) {
	assertStageTerminalHistoryUndeliveredRetry(t, true)
}

func assertStageTerminalHistoryUndeliveredRetry(t *testing.T, replaceRuntime bool) {
	t.Helper()
	fixture := newStageSchedulerFixture(t, "terminal-history-unseen-retry")
	command := stageWorkerAcquireCommand(fixture)
	acquired, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(), command, stageWorkerAcquireRequest(fixture),
	)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire: %v %v", acquired, err)
	}
	authority := acquired.Assignment.GetAuthority()
	request := terminalHistoryRequest(t, command, authority)
	started := startTerminalHistoryAssignment(t, fixture, authority)
	failedAt := terminalHistoryDatabaseTime(t, fixture)
	fingerprint := sha256.Sum256([]byte("retry before terminal history"))
	failed, err := fixture.coordinator.Apply(context.Background(), attemptcoordinator.FailStageCommand{
		CommandID: uuid.New(), AttemptID: uuid.MustParse(authority.GetAttemptId()),
		StageRunID: uuid.MustParse(authority.GetStageRunId()), StageAttemptID: uuid.MustParse(authority.GetStageAttemptId()),
		StageLeaseID: uuid.MustParse(authority.GetStageLeaseId()), ExpectedAttemptFence: authority.GetAttemptFence(),
		ExpectedStageFence: authority.GetStageFence(), ExpectedStageVersion: started.StageVersion,
		FailureClass: "TRANSIENT_BACKEND", FailureFingerprint: fingerprint[:], ConsumedResourceUnits: 1,
		FailedAt: failedAt, RetryAt: failedAt.Add(time.Millisecond),
	})
	if err != nil || failed.State != "RETRY_WAIT" {
		t.Fatalf("retry FAIL = %+v error=%v", failed, err)
	}
	if snapshot := readTerminalHistory(t, fixture, request); snapshot.Eligible || snapshot.Reason != "STAGE_RUN_NOT_TERMINAL" {
		t.Fatalf("RETRY_WAIT produced terminal history: %+v", snapshot)
	}
	waitStageExpiry(t, failedAt.Add(time.Millisecond))
	if _, err := fixture.coordinator.Reconcile(context.Background(), 10); err != nil {
		t.Fatal(err)
	}
	// Stop at durable allocation: no second signed assignment exists or reaches a Worker.
	next, assigned, err := newStageSchedulerTestService(t, fixture).Acquire(context.Background(), fixture.authority, fixture.observation)
	if err != nil || !assigned || next.StageRunID.String() != authority.GetStageRunId() {
		t.Fatalf("allocate undelivered retry = %+v assigned=%t error=%v", next, assigned, err)
	}
	grantRecoveryCancellation(t, fixture.database)
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(t, server.URL, testProjectID, authority.GetJobId(), testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel after undelivered retry status=%d", canceled.StatusCode)
	}
	snapshot := readTerminalHistory(t, fixture, request)
	if !snapshot.Eligible || snapshot.TerminalState != "CANCELED" || len(snapshot.Allocations) != 2 ||
		snapshot.Cutoff <= authority.GetExecutionSequence() {
		t.Fatalf("terminal history missed undelivered retry: %+v", snapshot)
	}
	var last struct {
		StageAllocationID string `json:"stage_allocation_id"`
		StageLeaseID      string `json:"stage_lease_id"`
		Sequence          int64  `json:"execution_sequence"`
	}
	if err := json.Unmarshal(snapshot.Allocations[1], &last); err != nil {
		t.Fatal(err)
	}
	if last.StageAllocationID != next.StageAllocationID.String() || last.StageLeaseID != next.StageLeaseID.String() ||
		last.Sequence != snapshot.Cutoff {
		t.Fatalf("cutoff does not cover undelivered allocation: %+v cutoff=%d", last, snapshot.Cutoff)
	}
	reader := newTerminalHistoryReaderForTest(t, fixture, authority)
	verifiedHistory, err := reader.Read(context.Background(), command, authority, command.CommandID)
	if err != nil || verifiedHistory == nil || verifiedHistory.Cutoff != last.Sequence ||
		len(verifiedHistory.Allocations) != 2 || verifiedHistory.Allocations[1].StageAllocationID != next.StageAllocationID {
		t.Fatalf("verified history missed undelivered retry: history=%v error=%v", verifiedHistory, err)
	}
	handler, validator, _ := terminalDispositionControl(t, fixture)
	disposition := readSignedTerminalDisposition(t, handler, validator, command, authority)
	if disposition.GetCutoff() != last.Sequence || len(disposition.GetAllocations()) != 2 ||
		disposition.GetAllocations()[1].GetStageAllocationId() != next.StageAllocationID.String() {
		t.Fatal("signed disposition omitted allocated but undelivered retry")
	}
	assertUnsignedTerminalAllocationNonAdmission(t, fixture, validator, acquired.Assignment, command, disposition, next.StageAllocationID.String(), replaceRuntime)
	if stale := readTerminalHistory(t, fixture, request); stale.Eligible || stale.Reason != "WORKER_SESSION_CHANGED" {
		t.Fatalf("recovery reconnect did not fence the original session: eligible=%t reason=%s", stale.Eligible, stale.Reason)
	}
	var recoveredSession int64
	if err := fixture.database.Admin.QueryRow(`SELECT control_session_epoch FROM worker_instances WHERE id = $1`, authority.GetWorkerInstanceId()).Scan(&recoveredSession); err != nil {
		t.Fatal(err)
	}
	request["control_session_epoch"] = recoveredSession
	// Global issuance can advance independently; it is not this StageRun's cutoff.
	if _, err := fixture.database.Admin.Exec(`SELECT nextval('stage_allocation_execution_sequence')`); err != nil {
		t.Fatal(err)
	}
	if after := readTerminalHistory(t, fixture, request); !after.Eligible || after.Cutoff != snapshot.Cutoff {
		t.Fatalf("unrelated issuance changed cutoff from %d to %d", snapshot.Cutoff, after.Cutoff)
	}
	if replaceRuntime {
		return
	}
	for _, test := range []struct {
		name, mutation, reason string
	}{
		{"missing ASSIGN", `DELETE FROM attempt_coordinator_commands
			WHERE result ->> 'stage_attempt_id' = $1 AND command_kind = 'ASSIGN'`, "ALLOCATION_HISTORY_INCOMPLETE"},
		{"missing lease", `DELETE FROM stage_leases WHERE stage_attempt_id = $1`, "ALLOCATION_HISTORY_INCOMPLETE"},
		{"missing allocation", `DELETE FROM stage_allocations WHERE stage_attempt_id = $1`, "ALLOCATION_HISTORY_INCOMPLETE"},
		{"missing physical", `DELETE FROM stage_attempts WHERE id = $1`, "ATTEMPT_HISTORY_INCOMPLETE"},
		{"unnumbered retry", `UPDATE stage_allocations SET execution_sequence = NULL WHERE stage_attempt_id = $1`, "UNNUMBERED_ALLOCATION_HISTORY"},
		{"missing runtime registration", `DELETE FROM model_runtime_epoch_registrations WHERE model_residency_id IN
			(SELECT model_residency_id FROM stage_allocations WHERE stage_attempt_id = $1)`, "RUNTIME_HISTORY_INCOMPLETE"},
	} {
		t.Run(test.name, func(t *testing.T) {
			tx, err := fixture.database.Admin.Begin()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = tx.Rollback() }()
			// Deliberate incomplete-history fixture; assertions use the restricted SQL reader.
			if _, err := tx.Exec(`SET LOCAL session_replication_role = 'replica'`); err != nil {
				t.Fatal(err)
			}
			if result, err := tx.Exec(test.mutation, next.StageAttemptID.String()); err != nil {
				t.Fatal(err)
			} else if rows, err := result.RowsAffected(); err != nil || rows != 1 {
				t.Fatalf("history fault affected=%d error=%v", rows, err)
			}
			if _, err := tx.Exec(`SET LOCAL ROLE vela_stage_worker_control`); err != nil {
				t.Fatal(err)
			}
			encoded, err := json.Marshal(request)
			if err != nil {
				t.Fatal(err)
			}
			var raw []byte
			if err := tx.QueryRow(`SELECT vela_read_stage_terminal_history($1::jsonb)`, encoded).Scan(&raw); err != nil {
				t.Fatal(err)
			}
			var rejected terminalHistorySnapshot
			if err := json.Unmarshal(raw, &rejected); err != nil {
				t.Fatal(err)
			}
			if rejected.Eligible || rejected.Reason != test.reason || len(rejected.Allocations) != 0 ||
				rejected.AssignmentWire != "" || rejected.AuthorityWire != "" || rejected.RenewalWire != "" {
				t.Fatalf("incomplete history result eligible=%t reason=%s", rejected.Eligible, rejected.Reason)
			}
		})
	}
}

func TestStageTerminalHistorySurvivesJobMetadataExpiry(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "terminal-history-metadata-expiry")
	command := stageWorkerAcquireCommand(fixture)
	acquired, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(), command, stageWorkerAcquireRequest(fixture),
	)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire: %v %v", acquired, err)
	}
	authority := acquired.Assignment.GetAuthority()
	failTerminalHistoryAssignment(t, fixture, authority)
	pool := newRolePool(t, fixture.database.DSN, "vela_non_content_expiry_login", "vela-non-content-expiry-password")
	reconciler, err := noncontentexpiry.New(pool, noncontentexpiry.Config{
		InstanceID: "terminal-history-expiry", BatchSize: 1, ClaimTTL: 30 * time.Second, HeldRetry: time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	makeJobExpiryDue(t, fixture.database, uuid.MustParse(authority.GetJobId()), "JOB_METADATA")
	result, err := reconciler.ReconcileBatch(context.Background())
	if err != nil || result != (noncontentexpiry.Result{Claimed: 1, Expired: 1}) {
		t.Fatalf("expire terminal Job metadata = %+v error=%v", result, err)
	}
	snapshot := readTerminalHistory(t, fixture, terminalHistoryRequest(t, command, authority))
	if !snapshot.Eligible || snapshot.TerminalState != "FAILED" || snapshot.Cutoff != authority.GetExecutionSequence() {
		t.Fatalf("retained roots did not preserve terminal history: eligible=%t reason=%s", snapshot.Eligible, snapshot.Reason)
	}
	reader := newTerminalHistoryReaderForTest(t, fixture, authority)
	if history, err := reader.Read(context.Background(), command, authority, command.CommandID); err != nil || history == nil ||
		history.StageRunID.String() != authority.GetStageRunId() {
		t.Fatalf("verified history after metadata expiry=%v error=%v", history, err)
	}
	assertAssignmentDeliveryRetired(t, fixture.database, command.CommandID)
	replayed, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(context.Background(), command, stageWorkerAcquireRequest(fixture))
	assertAssignmentDeletedResult(t, replayed, err)
}

func TestStageTerminalHistoryReadsRenewalWithoutAcquireID(t *testing.T) {
	fixture := newStageSchedulerFixture(t, "terminal-history-renewal")
	command := stageWorkerAcquireCommand(fixture)
	acquired, err := newPostgresAssignmentTestBackend(t, fixture).AcquireStage(
		context.Background(), command, stageWorkerAcquireRequest(fixture),
	)
	if err != nil || acquired.Assignment == nil {
		t.Fatalf("Acquire: %v %v", acquired, err)
	}
	authority := acquired.Assignment.GetAuthority()
	validator, err := stageauthority.NewValidator(map[string][]byte{
		"stage-authority-key-v1": bytes.Repeat([]byte{0x9a}, 32),
	}, func() time.Time { return authority.GetIssuedAt().AsTime().Add(time.Millisecond) })
	if err != nil {
		t.Fatal(err)
	}
	verified, err := validator.ValidateEnvelope(authority)
	if err != nil {
		t.Fatal(err)
	}
	started := startH3IntegrationStage(t, fixture.database, attemptcoordinator.AssignStageCommand{
		WorkerInstanceID: uuid.MustParse(authority.GetWorkerInstanceId()), IssuedAt: authority.GetIssuedAt().AsTime(),
	}, verified)
	grantRecoveryCancellation(t, fixture.database)
	server := admissionServerForDatabase(t, fixture.database)
	canceled := cancelJob(t, server.URL, testProjectID, authority.GetJobId(), testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel renewed authority status=%d", canceled.StatusCode)
	}
	request := terminalHistoryRequest(t, command, started.Authority)
	delete(request, "acquire_command_id")
	snapshot := readTerminalHistory(t, fixture, request)
	if !snapshot.Eligible || snapshot.AssignmentWire != "" || snapshot.AuthorityWire != "" || snapshot.RenewalWire == "" ||
		snapshot.Cutoff != authority.GetExecutionSequence() {
		t.Fatalf("renewal history eligible=%t reason=%s cutoff=%d", snapshot.Eligible, snapshot.Reason, snapshot.Cutoff)
	}
	wire, err := hex.DecodeString(snapshot.RenewalWire)
	if err != nil {
		t.Fatal(err)
	}
	var recorded velav1.StageAuthority
	if err := proto.Unmarshal(wire, &recorded); err != nil || !proto.Equal(&recorded, started.Authority) {
		t.Fatalf("recorded renewal differs: %v", err)
	}
	reader := newTerminalHistoryReaderForTest(t, fixture, started.Authority)
	if history, err := reader.Read(context.Background(), command, started.Authority, uuid.Nil); err != nil || history == nil ||
		history.OriginalAuthorityDigest != started.Digest {
		t.Fatalf("verified history did not match recorded renewal: history=%v error=%v", history, err)
	}
	request["authority_digest"] = hex.EncodeToString(make([]byte, sha256.Size))
	if changed := readTerminalHistory(t, fixture, request); changed.Eligible || changed.Reason != "SIGNED_HISTORY_UNAVAILABLE" {
		t.Fatalf("different full renewal digest eligible=%t reason=%s", changed.Eligible, changed.Reason)
	}
}

func TestStageTerminalHistoryMigrationKeepsRoleBoundary(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 88)
	seedAdmissionFixture(t, database.Admin)
	workerPool := newRolePool(t, database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	for range 2 {
		if err := veladb.VerifyRole(context.Background(), workerPool, veladb.RoleStageWorkerControl); err == nil {
			t.Fatal("control startup accepted schema88 without terminal history capability")
		}
		if err := goose.UpTo(database.Admin, migrations, 89); err != nil {
			t.Fatal(err)
		}
		if err := veladb.VerifyRole(context.Background(), workerPool, veladb.RoleStageWorkerControl); err == nil {
			t.Fatal("control startup accepted schema89 without assignment content lifecycle")
		}
		if err := goose.UpTo(database.Admin, migrations, 90); err != nil {
			t.Fatal(err)
		}
		if err := veladb.VerifyRole(context.Background(), workerPool, veladb.RoleStageWorkerControl); err != nil {
			t.Fatalf("control startup rejected schema90: %v", err)
		}
		for _, role := range []string{"vela_request", "vela_stage_scheduler", "vela_fleet", "vela_stage_worker_control"} {
			var allowed bool
			if err := database.Admin.QueryRow(`SELECT has_function_privilege($1,
				'public.vela_read_stage_terminal_history(jsonb)', 'EXECUTE')`, role).Scan(&allowed); err != nil {
				t.Fatal(err)
			}
			if allowed != (role == "vela_stage_worker_control") {
				t.Fatalf("terminal history EXECUTE role=%s allowed=%t", role, allowed)
			}
		}
		for _, table := range []string{"stage_allocations", "stage_leases", "stage_worker_acquire_results", "device_set_members"} {
			var allowed bool
			if err := database.Admin.QueryRow(`SELECT has_table_privilege('vela_stage_worker_control', $1, 'SELECT,INSERT,UPDATE,DELETE')`, table).Scan(&allowed); err != nil || allowed {
				t.Fatalf("Worker control direct table grant %s=%t error=%v", table, allowed, err)
			}
		}
		if err := goose.DownTo(database.Admin, migrations, 88); err != nil {
			t.Fatal(err)
		}
		var removed bool
		if err := database.Admin.QueryRow(`SELECT to_regprocedure('public.vela_read_stage_terminal_history(jsonb)') IS NULL`).Scan(&removed); err != nil || !removed {
			t.Fatalf("terminal history Down removed=%t error=%v", removed, err)
		}
		for _, table := range []string{"non_content_job_roots", "non_content_attempt_roots", "device_set_members"} {
			var allowed bool
			if err := database.Admin.QueryRow(`SELECT has_table_privilege('vela_attempt_coordinator_owner', $1, 'SELECT')`, table).Scan(&allowed); err != nil || allowed {
				t.Fatalf("terminal history Down grant %s=%t error=%v", table, allowed, err)
			}
		}
	}
}

type terminalHistorySnapshot struct {
	Eligible       bool              `json:"eligible"`
	Reason         string            `json:"reason"`
	TerminalState  string            `json:"terminal_state"`
	StageFence     int64             `json:"stage_fence"`
	StageVersion   int64             `json:"stage_version"`
	Cutoff         int64             `json:"cutoff"`
	AssignmentWire string            `json:"assignment_wire"`
	AuthorityWire  string            `json:"authority_wire"`
	RenewalWire    string            `json:"renewal_wire"`
	WorkerMemberID string            `json:"worker_member_id"`
	ObservedAt     time.Time         `json:"observed_at"`
	Allocations    []json.RawMessage `json:"allocations"`
}

func terminalHistoryRequest(t *testing.T, command stageworkercontrol.CommandContext, authority *velav1.StageAuthority) map[string]any {
	t.Helper()
	digest, err := stageauthority.Digest(authority)
	if err != nil {
		t.Fatal(err)
	}
	tokenDigest := sha256.Sum256(authority.GetLeaseToken())
	spiffeDigest := sha256.Sum256([]byte(command.Identity.SPIFFEID))
	return map[string]any{
		"schema_version": 1, "acquire_command_id": command.CommandID,
		"job_id": authority.GetJobId(), "attempt_id": authority.GetAttemptId(),
		"stage_run_id": authority.GetStageRunId(), "stage_attempt_id": authority.GetStageAttemptId(),
		"stage_allocation_id": authority.GetStageAllocationId(), "stage_lease_id": authority.GetStageLeaseId(),
		"worker_instance_id": authority.GetWorkerInstanceId(), "worker_instance_epoch": authority.GetWorkerInstanceEpoch(),
		"execution_sequence": authority.GetExecutionSequence(), "authority_digest": hex.EncodeToString(digest[:]),
		"token_digest": hex.EncodeToString(tokenDigest[:]), "spiffe_id_digest": hex.EncodeToString(spiffeDigest[:]),
		"control_session_epoch":            command.ControlSessionEpoch,
		"model_residency_id":               authority.GetModelResidencyId(),
		"model_runtime_barrier_generation": authority.GetModelRuntimeBarrierGeneration(),
		"stage_profile_revision_id":        authority.GetStageProfileRevisionId(),
		"capacity_observation_sequence":    authority.GetCapacityObservationSequence(),
	}
}

func readTerminalHistory(t *testing.T, fixture stageSchedulerFixture, request map[string]any) terminalHistorySnapshot {
	t.Helper()
	pool := newRolePool(t, fixture.database.DSN, "vela_stage_worker_control_login", "vela-stage-worker-control-password")
	ctx := context.Background()
	tx, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead, AccessMode: pgx.ReadOnly})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	encoded, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := tx.QueryRow(ctx, `SELECT vela_read_stage_terminal_history($1::jsonb)`, encoded).Scan(&raw); err != nil {
		t.Fatalf("read terminal history as Stage Worker control: %v", err)
	}
	var snapshot terminalHistorySnapshot
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	return snapshot
}

func failTerminalHistoryAssignment(t *testing.T, fixture stageSchedulerFixture, authority *velav1.StageAuthority) {
	t.Helper()
	started := startTerminalHistoryAssignment(t, fixture, authority)
	var units int64
	if err := fixture.database.Admin.QueryRow(`SELECT max_resource_units - consumed_resource_units
		FROM attempt_retry_budgets WHERE attempt_id = $1`, authority.GetAttemptId()).Scan(&units); err != nil {
		t.Fatal(err)
	}
	fingerprint := sha256.Sum256([]byte("terminal history fixture failure"))
	failedAt := terminalHistoryDatabaseTime(t, fixture)
	decision, err := fixture.coordinator.Apply(context.Background(), attemptcoordinator.FailStageCommand{
		CommandID: uuid.New(), AttemptID: uuid.MustParse(authority.GetAttemptId()),
		StageRunID: uuid.MustParse(authority.GetStageRunId()), StageAttemptID: uuid.MustParse(authority.GetStageAttemptId()),
		StageLeaseID: uuid.MustParse(authority.GetStageLeaseId()), ExpectedAttemptFence: authority.GetAttemptFence(),
		ExpectedStageFence: authority.GetStageFence(), ExpectedStageVersion: started.StageVersion,
		FailureClass: "WORKER_LOST", FailureFingerprint: fingerprint[:], ConsumedResourceUnits: units,
		FailedAt: failedAt, RetryAt: failedAt.Add(time.Second),
	})
	if err != nil || decision.State != "FAILED" {
		t.Fatalf("terminal FAIL = %+v error=%v", decision, err)
	}
}

func startTerminalHistoryAssignment(t *testing.T, fixture stageSchedulerFixture, authority *velav1.StageAuthority) attemptcoordinator.StageDecision {
	t.Helper()
	startedAt := terminalHistoryDatabaseTime(t, fixture)
	started, err := fixture.coordinator.Apply(context.Background(), attemptcoordinator.StartStageCommand{
		CommandID: uuid.New(), AttemptID: uuid.MustParse(authority.GetAttemptId()),
		StageRunID: uuid.MustParse(authority.GetStageRunId()), StageAttemptID: uuid.MustParse(authority.GetStageAttemptId()),
		StageLeaseID: uuid.MustParse(authority.GetStageLeaseId()), ExpectedAttemptFence: authority.GetAttemptFence(),
		ExpectedStageFence: authority.GetStageFence(), ExpectedStageVersion: authority.GetStageVersion(),
		StartedAt: startedAt,
	})
	if err != nil || started.State != "RUNNING" {
		t.Fatalf("START before terminal FAIL = %+v error=%v; started_at=%s issued_at=%s expires_at=%s", started, err, startedAt, authority.GetIssuedAt().AsTime(), authority.GetExpiresAt().AsTime())
	}
	return started
}

func terminalHistoryDatabaseTime(t *testing.T, fixture stageSchedulerFixture) time.Time {
	t.Helper()
	// These fixtures call the coordinator directly, without ingress clock policy.
	var now time.Time
	if err := fixture.database.Admin.QueryRow(`SELECT clock_timestamp()`).Scan(&now); err != nil {
		t.Fatal(err)
	}
	return now
}
