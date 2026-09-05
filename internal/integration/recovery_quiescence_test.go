//go:build integration

package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/pressly/goose/v3"
	"github.com/vivym/vela/internal/recovery"
)

const recoveryJobRequest = `{"model":"minimax-h3","generation_preset":"balanced","service_class":"standard","output_spec":"video-1080p-5s-24fps","generation_count":1,"prompt":"recovery transaction fixture"}`

func TestRecoveryGateDrainReplayAndAdmissionReopen(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	grantRecoveryCancellation(t, database)
	connection := recoveryConnection(t, database)
	accepted := submitJob(t, server.URL, "recovery-admitted", []byte(recoveryJobRequest))
	if accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("accept fixture: %d %s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatal(err)
	}
	operation := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := recovery.Quiesce(ctx, connection, operation, 10*time.Millisecond); err == nil {
		t.Fatal("active Job was accepted as quiescent")
	}
	refused := submitJob(t, server.URL, "recovery-blocked", []byte(recoveryJobRequest))
	if refused.StatusCode != http.StatusServiceUnavailable || refused.Header.Get("Retry-After") == "" {
		t.Fatalf("closed Admission = %d %s", refused.StatusCode, refused.Body)
	}
	replayed := submitJob(t, server.URL, "recovery-admitted", []byte(recoveryJobRequest))
	if replayed.StatusCode != http.StatusAccepted {
		t.Fatalf("closed gate rejected preexisting idempotent result: %d %s", replayed.StatusCode, replayed.Body)
	}
	canceled := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential())
	if canceled.StatusCode != http.StatusOK {
		t.Fatalf("cancel drain fixture: %d %s", canceled.StatusCode, canceled.Body)
	}
	receipt, err := recovery.Quiesce(context.Background(), connection, operation, 10*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	repeated, err := recovery.Quiesce(context.Background(), connection, operation, 10*time.Millisecond)
	if err != nil || !repeated.SealedAt.Equal(receipt.SealedAt) {
		t.Fatalf("receipt replay changed: %#v %v", repeated, err)
	}
	encodedStatus, err := recovery.Status(context.Background(), connection)
	if err != nil {
		t.Fatal(err)
	}
	var status struct {
		Gate struct {
			AdmissionOpen bool      `json:"admission_open"`
			OperationID   uuid.UUID `json:"operation_id"`
		} `json:"gate"`
		Receipt recovery.Receipt `json:"receipt"`
	}
	if err := json.Unmarshal(encodedStatus, &status); err != nil || status.Gate.AdmissionOpen ||
		status.Gate.OperationID != operation || status.Receipt.Validate() != nil {
		t.Fatalf("recovery status = %s: %v", encodedStatus, err)
	}
	if err := recovery.Reopen(context.Background(), connection, operation, receipt.Generation+1); err == nil {
		t.Fatal("wrong gate generation reopened Admission")
	}
	if err := recovery.Reopen(context.Background(), connection, operation, receipt.Generation); err != nil {
		t.Fatal(err)
	}
	resumed := submitJob(t, server.URL, "recovery-after-reopen", []byte(recoveryJobRequest))
	if resumed.StatusCode != http.StatusAccepted {
		t.Fatalf("Admission did not resume: %d %s", resumed.StatusCode, resumed.Body)
	}
	if _, err := recovery.Quiesce(context.Background(), connection, operation, time.Second); err == nil {
		t.Fatal("old operation reacquired a reopened gate")
	}
}

func TestRecoveryGateCloseWaitsForInflightAdmissionGuard(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	connection := recoveryConnection(t, database)
	transaction, err := database.Admin.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = transaction.Rollback() }()
	if _, err := transaction.Exec(`SELECT admission_open FROM recovery_admission_control WHERE singleton FOR SHARE`); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	if _, err := recovery.Quiesce(ctx, connection, uuid.New(), time.Millisecond); err == nil {
		t.Fatal("gate close bypassed the Admission transaction lock")
	}
	if err := transaction.Commit(); err != nil {
		t.Fatal(err)
	}
	connection = newRoleConnection(t, database.DSN, "vela_recovery_login", "vela-recovery-test")
	if _, err := recovery.Quiesce(context.Background(), connection, uuid.New(), time.Millisecond); err != nil {
		t.Fatal(err)
	}
}

func TestRecoverySnapshotRestoresIndependentPostgres17(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	seedAdmissionFixture(t, database.Admin)
	seedStageExecutionCatalog(t, database.Admin)
	activateH3StageGraph(t, database)
	server := admissionServerForDatabase(t, database)
	grantRecoveryCancellation(t, database)
	accepted := submitJob(t, server.URL, "restore-canceled-job", []byte(recoveryJobRequest))
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil || accepted.StatusCode != http.StatusAccepted {
		t.Fatalf("accept restore fixture: %d %s %v", accepted.StatusCode, accepted.Body, err)
	}
	if result := cancelJob(t, server.URL, testProjectID, job.JobID, testBearerCredential()); result.StatusCode != http.StatusOK {
		t.Fatalf("cancel restore fixture: %d %s", result.StatusCode, result.Body)
	}
	connection := recoveryConnection(t, database)
	operation := uuid.New()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if _, err := recovery.Quiesce(ctx, connection, operation, 10*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	backup, err := pgx.Connect(ctx, database.DSN)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = backup.Close(context.Background()) }()
	directory := filepath.Join(t.TempDir(), "snapshot")
	rolesPath := filepath.Join(repositoryRoot(t), "db", "bootstrap", "roles.sql")
	dump := func(ctx context.Context, snapshot string, writer io.Writer) error {
		command := exec.CommandContext(ctx, "docker", "exec", database.Container.GetContainerID(),
			"pg_dump", "--username=postgres", "--format=custom", "--snapshot="+snapshot, "vela")
		command.Stdout = writer
		return command.Run()
	}
	manifest, err := recovery.Capture(ctx, backup, operation, directory, rolesPath, dump)
	if err != nil {
		t.Fatal(err)
	}
	if manifest.Fingerprints["jobs"].Rows != 1 || manifest.Fingerprints["stage_runs"].Rows != 3 {
		t.Fatalf("snapshot Job/Stage counts = %d/%d", manifest.Fingerprints["jobs"].Rows, manifest.Fingerprints["stage_runs"].Rows)
	}
	for _, name := range []string{"database.dump", "snapshot.json"} {
		info, err := os.Stat(filepath.Join(directory, name))
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("private artifact %s = %v %v", name, info, err)
		}
	}
	restored, err := recovery.RestoreDrill(ctx, directory, rolesPath, "postgres:17-alpine")
	if err != nil {
		t.Fatal(err)
	}
	if restored.ProductionGate || restored.Scope != "DATABASE_ONLY" || !restored.CatalogVerified ||
		!restored.AdmissionClosed || !restored.UnboundTenantReadDenied || restored.TablesVerified != len(manifest.Tables) ||
		restored.TargetSystemIdentifier == restored.SourceSystemIdentifier {
		t.Fatalf("restore evidence incomplete: %#v", restored)
	}
	if _, err := recovery.RestoreDrill(ctx, directory, rolesPath, "postgres:17-alpine"); err == nil {
		t.Fatal("restore drill overwrote an existing receipt")
	}
	if _, err := recovery.Capture(ctx, backup, operation, directory, rolesPath, dump); err == nil {
		t.Fatal("capture overwrote an existing evidence directory")
	}
}

func recoveryConnection(t *testing.T, database testDatabase) *pgx.Conn {
	t.Helper()
	if _, err := database.Admin.Exec(`CREATE ROLE vela_recovery_login LOGIN PASSWORD 'vela-recovery-test' IN ROLE vela_recovery`); err != nil {
		t.Fatal(err)
	}
	return newRoleConnection(t, database.DSN, "vela_recovery_login", "vela-recovery-test")
}

func TestRecoveryRoleIsolationReceiptImmutabilityAndRollbackGuard(t *testing.T) {
	database := newPostgres(t)
	applyFoundationTo(t, database.Admin, 72)
	migrations := filepath.Join(repositoryRoot(t), "db", "migrations")
	if err := goose.Down(database.Admin, migrations); err != nil {
		t.Fatalf("empty recovery rollback: %v", err)
	}
	assertTableDoesNotExist(t, database.Admin, "recovery_admission_control")
	if err := goose.UpTo(database.Admin, migrations, 72); err != nil {
		t.Fatalf("reapply recovery migration: %v", err)
	}
	connection := recoveryConnection(t, database)
	for _, query := range []string{
		`SELECT * FROM jobs`,
		`UPDATE recovery_admission_control SET admission_open=true`,
		`INSERT INTO recovery_quiescence_receipts VALUES(gen_random_uuid(),'{}',clock_timestamp())`,
		`SET ROLE vela_recovery_owner`,
	} {
		if _, err := connection.Exec(context.Background(), query); err == nil {
			t.Fatalf("recovery runtime received direct or owner privileges: %s", query)
		}
	}
	var widened bool
	if err := database.Admin.QueryRow(`SELECT bool_or(has_function_privilege(role_name,'vela_close_recovery_admission(uuid)','EXECUTE'))
		FROM unnest(ARRAY['vela_request','vela_stage_worker_control','vela_stage_scheduler','vela_fleet','vela_retention']) role_name`).Scan(&widened); err != nil || widened {
		t.Fatalf("unrelated workload can close Admission: %t %v", widened, err)
	}
	operation := uuid.New()
	if _, err := recovery.Quiesce(context.Background(), connection, operation, time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if _, err := recovery.Quiesce(context.Background(), connection, uuid.New(), time.Millisecond); err == nil {
		t.Fatal("another operation replaced the closed gate owner")
	}
	for _, query := range []string{
		`UPDATE recovery_quiescence_receipts SET receipt='{}'`,
		`DELETE FROM recovery_quiescence_receipts`,
		`TRUNCATE recovery_quiescence_receipts`,
	} {
		_, err := database.Admin.Exec(query)
		assertPostgresConstraint(t, err, "recovery_receipt_immutable")
	}
	if err := goose.Down(database.Admin, migrations); err == nil {
		t.Fatal("rollback erased recovery authority")
	}
}

func grantRecoveryCancellation(t *testing.T, database testDatabase) {
	t.Helper()
	if _, err := database.Admin.Exec(`UPDATE credentials SET scopes = ARRAY['jobs:submit','jobs:read','jobs:cancel'] WHERE id=$1`, testCredentialID); err != nil {
		t.Fatal(err)
	}
}
