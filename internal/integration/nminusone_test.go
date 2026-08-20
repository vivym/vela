//go:build integration

package integration_test

import (
	"archive/tar"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	veladb "github.com/vivym/vela/internal/database"
)

const nMinusOneControlCommit = "450dd5c379ed7d26588e2a76140f0b3281acfbb2"

func TestNMinusOneControlStartupAcrossRenewalProtocolTransition(t *testing.T) {
	database := newPostgres(t)
	applyFoundation(t, database.Admin)
	nMinusOne := buildNMinusOneBinaries(t)
	requestPool := newRolePool(t, database.DSN, "vela_request_login", "vela-request-password")

	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify new request role during expand: %v", err)
	}
	seedAdmissionFixture(t, database.Admin)
	server := admissionServerForDatabase(t, database)
	accepted := submitJob(t, server.URL, "n-minus-one-writer-replay", []byte(`{
		"model":"minimax-h3",
		"generation_preset":"balanced",
		"service_class":"standard",
		"output_spec":"video-1080p-5s-24fps",
		"generation_count":1,
		"prompt":"prove the fixed-point writer and replay against the expanded schema"
	}`))
	if accepted.StatusCode != 202 {
		t.Fatalf("submit N-1 probe Job status = %d; body=%s", accepted.StatusCode, accepted.Body)
	}
	var job jobResponse
	if err := json.Unmarshal(accepted.Body, &job); err != nil {
		t.Fatalf("decode N-1 probe Job: %v", err)
	}
	if _, err := database.Admin.Exec(`
		INSERT INTO workers (
			id, worker_pool_id, spiffe_id, epoch, lifecycle_state, reachability_condition
		) VALUES (
			$1, '00000000-0000-0000-0000-000000000005',
			'spiffe://vela.internal/worker/n-minus-one-probe', 7, 'READY', 'HEALTHY'
		)
	`, testWorkerID); err != nil {
		t.Fatalf("seed N-1 probe Worker: %v", err)
	}
	attemptID := runNMinusOneAssignmentProbe(t, nMinusOne.AssignmentProbe, database.DSN, job.JobID)
	var protocolVersion int16
	var claimMatchesExpiry bool
	if err := database.Admin.QueryRow(`
		SELECT renewal_protocol_version, token_claim_expires_at = expires_at
		FROM attempt_leases
		WHERE attempt_id = $1
	`, attemptID).Scan(&protocolVersion, &claimMatchesExpiry); err != nil {
		t.Fatalf("read N-1 Assignment protocol state: %v", err)
	}
	if protocolVersion != 1 || !claimMatchesExpiry {
		t.Fatalf("N-1 Assignment protocol version=%d claim_matches_expiry=%t", protocolVersion, claimMatchesExpiry)
	}
	if _, err := database.Admin.Exec(
		"UPDATE attempt_leases SET revoked_at = clock_timestamp() WHERE attempt_id = $1",
		attemptID,
	); err != nil {
		t.Fatalf("drain N-1 probe Lease before switch: %v", err)
	}

	if _, err := database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(true, 'N-1 startup integration switch')",
	); err != nil {
		t.Fatalf("enable renewal protocol: %v", err)
	}
	assertNMinusOneRequestStartupRejected(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify new request role after switch: %v", err)
	}
	if _, err := database.Admin.Exec("GRANT SELECT ON retry_runtime_states TO vela_request"); err != nil {
		t.Fatalf("inject legacy request privilege after switch: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err == nil {
		t.Fatal("new request-role verification accepted legacy table access after switch")
	}
	if _, err := database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(true, 'repair switched privilege boundary')",
	); err != nil {
		t.Fatalf("repair switched request privilege boundary: %v", err)
	}
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify repaired new request role after switch: %v", err)
	}

	if _, err := database.Admin.Exec(
		"SELECT vela_transition_execution_lease_renewal_protocol(false, 'N-1 startup integration rollback')",
	); err != nil {
		t.Fatalf("disable renewal protocol: %v", err)
	}
	assertNMinusOneDatabaseStartupPassed(t, runNMinusOneControl(t, nMinusOne.Control, database.DSN))
	if err := veladb.VerifyRole(context.Background(), requestPool, veladb.RoleRequest); err != nil {
		t.Fatalf("verify new request role after rollback: %v", err)
	}
}

type nMinusOneBinaries struct {
	Control         string
	AssignmentProbe string
}

func buildNMinusOneBinaries(t *testing.T) nMinusOneBinaries {
	t.Helper()
	sourceRoot := t.TempDir()
	archive := exec.Command("git", "archive", "--format=tar", nMinusOneControlCommit)
	archive.Dir = repositoryRoot(t)
	stdout, err := archive.StdoutPipe()
	if err != nil {
		t.Fatalf("open N-1 source archive: %v", err)
	}
	if err := archive.Start(); err != nil {
		t.Fatalf("start N-1 source archive: %v", err)
	}
	reader := tar.NewReader(stdout)
	for {
		header, readErr := reader.Next()
		if readErr == io.EOF {
			break
		}
		if readErr != nil {
			t.Fatalf("read N-1 source archive: %v", readErr)
		}
		name := filepath.Clean(header.Name)
		if filepath.IsAbs(name) || name == ".." || strings.HasPrefix(name, ".."+string(filepath.Separator)) {
			t.Fatalf("N-1 source archive contains unsafe path %q", header.Name)
		}
		target := filepath.Join(sourceRoot, name)
		switch header.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, 0o755); err != nil {
				t.Fatalf("create N-1 source directory: %v", err)
			}
		case tar.TypeReg:
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				t.Fatalf("create N-1 source parent: %v", err)
			}
			file, openErr := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_EXCL, header.FileInfo().Mode())
			if openErr != nil {
				t.Fatalf("create N-1 source file: %v", openErr)
			}
			_, copyErr := io.Copy(file, reader)
			closeErr := file.Close()
			if copyErr != nil || closeErr != nil {
				t.Fatalf("extract N-1 source file: copy=%v close=%v", copyErr, closeErr)
			}
		}
	}
	if err := archive.Wait(); err != nil {
		t.Fatalf("archive N-1 source: %v", err)
	}

	probeSource, err := os.ReadFile(filepath.Join(
		repositoryRoot(t), "internal", "integration", "testdata", "nminusone_assignment_probe.go.txt",
	))
	if err != nil {
		t.Fatalf("read N-1 Assignment probe: %v", err)
	}
	probeDirectory := filepath.Join(sourceRoot, "cmd", "vela-nminusone-assignment-probe")
	if err := os.MkdirAll(probeDirectory, 0o755); err != nil {
		t.Fatalf("create N-1 Assignment probe directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(probeDirectory, "main.go"), probeSource, 0o600); err != nil {
		t.Fatalf("write N-1 Assignment probe: %v", err)
	}

	binaryDirectory := t.TempDir()
	binaries := nMinusOneBinaries{
		Control:         filepath.Join(binaryDirectory, "vela-control-n-minus-one"),
		AssignmentProbe: filepath.Join(binaryDirectory, "vela-assignment-probe-n-minus-one"),
	}
	build := exec.Command(
		"go", "build",
		"-o", binaries.Control, "./cmd/vela-control",
	)
	build.Dir = sourceRoot
	build.Env = environmentWith(map[string]string{"GOWORK": "off"})
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build N-1 vela-control: %v\n%s", err, output)
	}
	build = exec.Command(
		"go", "build",
		"-o", binaries.AssignmentProbe, "./cmd/vela-nminusone-assignment-probe",
	)
	build.Dir = sourceRoot
	build.Env = environmentWith(map[string]string{"GOWORK": "off"})
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build N-1 Assignment probe: %v\n%s", err, output)
	}
	return binaries
}

func runNMinusOneAssignmentProbe(
	t *testing.T,
	binary string,
	adminDSN string,
	jobID string,
) string {
	t.Helper()
	command := exec.Command(binary)
	command.Env = environmentWith(map[string]string{
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(
			t, adminDSN, "vela_internal_login", "vela-internal-password",
		),
		"VELA_WORKER_ID":           testWorkerID,
		"VELA_JOB_ID":              jobID,
		"VELA_PROFILE_REVISION_ID": "00000000-0000-0000-0000-000000000014",
	})
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("run N-1 Assignment writer/replay probe: %v\n%s", err, output)
	}
	var result struct {
		AttemptID string `json:"attempt_id"`
	}
	if err := json.Unmarshal(output, &result); err != nil {
		t.Fatalf("decode N-1 Assignment probe output: %v\n%s", err, output)
	}
	if _, err := uuid.Parse(result.AttemptID); err != nil {
		t.Fatalf("N-1 Assignment probe returned invalid Attempt id %q", result.AttemptID)
	}
	return result.AttemptID
}

func runNMinusOneControl(t *testing.T, binary, adminDSN string) string {
	t.Helper()
	temporary := t.TempDir()
	credentialsFile := filepath.Join(temporary, "invalid.creds")
	rootCAFile := filepath.Join(temporary, "invalid-ca.pem")
	if err := os.WriteFile(credentialsFile, []byte("not-a-nats-credential\n"), 0o600); err != nil {
		t.Fatalf("write NATS credential sentinel: %v", err)
	}
	if err := os.WriteFile(rootCAFile, []byte("not-a-ca\n"), 0o600); err != nil {
		t.Fatalf("write NATS root CA sentinel: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, binary)
	command.Env = environmentWith(map[string]string{
		"VELA_HTTP_ADDRESS":          "127.0.0.1:0",
		"VELA_AUTH_DATABASE_URL":     roleDatabaseURL(t, adminDSN, "vela_auth_login", "vela-auth-password"),
		"VELA_REQUEST_DATABASE_URL":  roleDatabaseURL(t, adminDSN, "vela_request_login", "vela-request-password"),
		"VELA_INTERNAL_DATABASE_URL": roleDatabaseURL(t, adminDSN, "vela_internal_login", "vela-internal-password"),
		"VELA_CREDENTIAL_PEPPER_BASE64": base64.StdEncoding.EncodeToString(
			[]byte("0123456789abcdef0123456789abcdef"),
		),
		"VELA_NATS_URL":              "nats://127.0.0.1:1",
		"VELA_NATS_CREDENTIALS_FILE": credentialsFile,
		"VELA_NATS_ROOT_CA_FILE":     rootCAFile,
	})
	output, err := command.CombinedOutput()
	if ctx.Err() != nil {
		t.Fatalf("N-1 vela-control startup timed out: %v\n%s", ctx.Err(), output)
	}
	if err == nil {
		t.Fatalf("N-1 vela-control unexpectedly passed the deliberate NATS sentinel")
	}
	return string(output)
}

func assertNMinusOneDatabaseStartupPassed(t *testing.T, output string) {
	t.Helper()
	if strings.Contains(output, "open auth database pool") ||
		strings.Contains(output, "open request database pool") ||
		strings.Contains(output, "open internal database pool") {
		t.Fatalf("N-1 database startup failed before NATS sentinel:\n%s", output)
	}
	if !strings.Contains(output, "connect NATS") {
		t.Fatalf("N-1 startup did not reach the NATS sentinel:\n%s", output)
	}
}

func assertNMinusOneRequestStartupRejected(t *testing.T, output string) {
	t.Helper()
	if !strings.Contains(output, "open request database pool") ||
		!strings.Contains(output, "request transaction privilege boundary") {
		t.Fatalf("N-1 startup was not rejected at the request-role boundary:\n%s", output)
	}
}

func roleDatabaseURL(t *testing.T, adminDSN, username, password string) string {
	t.Helper()
	dsn, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("parse PostgreSQL DSN: %v", err)
	}
	dsn.User = url.UserPassword(username, password)
	return dsn.String()
}

func environmentWith(overrides map[string]string) []string {
	environment := make([]string, 0, len(os.Environ())+len(overrides))
	for _, entry := range os.Environ() {
		name, _, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		if _, overridden := overrides[name]; !overridden {
			environment = append(environment, entry)
		}
	}
	for name, value := range overrides {
		environment = append(environment, fmt.Sprintf("%s=%s", name, value))
	}
	return environment
}
