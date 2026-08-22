package main

import (
	"context"
	"encoding/base64"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vivym/vela/internal/cancellation"
	"github.com/vivym/vela/internal/scheduler"
)

func TestLoadConfigRequiresNATSWorkloadCredentialsAndRootCA(t *testing.T) {
	tests := []struct {
		name       string
		missingEnv string
	}{
		{name: "workload credentials", missingEnv: "VELA_NATS_CREDENTIALS_FILE"},
		{name: "root CA", missingEnv: "VELA_NATS_ROOT_CA_FILE"},
		{name: "Artifact request database", missingEnv: "VELA_ARTIFACT_REQUEST_DATABASE_URL"},
		{name: "Scheduler database", missingEnv: "VELA_SCHEDULER_DATABASE_URL"},
		{name: "Scheduler identity", missingEnv: "VELA_SCHEDULER_ID"},
		{name: "Artifact S3 endpoint", missingEnv: "VELA_ARTIFACT_S3_ENDPOINT"},
		{name: "Artifact S3 region", missingEnv: "VELA_ARTIFACT_S3_REGION"},
		{name: "Artifact S3 bucket", missingEnv: "VELA_ARTIFACT_S3_BUCKET"},
		{name: "Artifact S3 access key", missingEnv: "VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE"},
		{name: "Artifact S3 secret key", missingEnv: "VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE"},
		{name: "Lease active key", missingEnv: "VELA_LEASE_ACTIVE_KEY_ID"},
		{name: "Lease keyring", missingEnv: "VELA_LEASE_KEYRING_FILE"},
		{name: "Artifact validator helper", missingEnv: "VELA_ARTIFACT_VALIDATOR_HELPER_PATH"},
		{name: "ffprobe", missingEnv: "VELA_ARTIFACT_FFPROBE_PATH"},
		{name: "Artifact sandbox root", missingEnv: "VELA_ARTIFACT_SANDBOX_ROOT"},
		{name: "Artifact spool", missingEnv: "VELA_ARTIFACT_SPOOL_DIRECTORY"},
		{name: "ffprobe version", missingEnv: "VELA_ARTIFACT_FFPROBE_VERSION"},
		{name: "Artifact validator revision", missingEnv: "VELA_ARTIFACT_VALIDATOR_REVISION"},
		{name: "Artifact Reconciler identity", missingEnv: "VELA_ARTIFACT_RECONCILER_ID"},
		{name: "Worker gRPC server certificate", missingEnv: "VELA_WORKER_GRPC_TLS_CERT_FILE"},
		{name: "Worker gRPC server key", missingEnv: "VELA_WORKER_GRPC_TLS_KEY_FILE"},
		{name: "Worker gRPC client CA", missingEnv: "VELA_WORKER_GRPC_CLIENT_CA_FILE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv(test.missingEnv, "")

			_, err := loadConfig()
			if err == nil || !strings.Contains(err.Error(), test.missingEnv+" is required") {
				t.Fatalf("loadConfig error = %v, want missing %s", err, test.missingEnv)
			}
		})
	}
}

func setValidConfigEnvironment(t *testing.T) {
	t.Helper()
	t.Setenv("VELA_AUTH_DATABASE_URL", "postgres://auth.example/vela")
	t.Setenv("VELA_REQUEST_DATABASE_URL", "postgres://request.example/vela")
	t.Setenv("VELA_ARTIFACT_REQUEST_DATABASE_URL", "postgres://artifact-request.example/vela")
	t.Setenv("VELA_CANCEL_DATABASE_URL", "postgres://cancel.example/vela")
	t.Setenv("VELA_INTERNAL_DATABASE_URL", "postgres://internal.example/vela")
	t.Setenv("VELA_SCHEDULER_DATABASE_URL", "postgres://scheduler.example/vela")
	t.Setenv("VELA_SCHEDULER_ID", "vela-control-scheduler-1")
	t.Setenv("VELA_NATS_URL", "nats://nats.example:4222")
	t.Setenv("VELA_NATS_CREDENTIALS_FILE", "/run/secrets/vela-control.creds")
	t.Setenv("VELA_NATS_ROOT_CA_FILE", "/run/secrets/nats-root-ca.pem")
	t.Setenv("VELA_ARTIFACT_S3_ENDPOINT", "https://s3.example")
	t.Setenv("VELA_ARTIFACT_S3_REGION", "us-east-1")
	t.Setenv("VELA_ARTIFACT_S3_BUCKET", "vela-artifacts")
	t.Setenv("VELA_ARTIFACT_S3_ACCESS_KEY_ID_FILE", "/run/secrets/s3-access-key-id")
	t.Setenv("VELA_ARTIFACT_S3_SECRET_ACCESS_KEY_FILE", "/run/secrets/s3-secret-access-key")
	t.Setenv("VELA_ARTIFACT_S3_PATH_STYLE", "false")
	t.Setenv("VELA_LEASE_ACTIVE_KEY_ID", "lease-key-v2")
	t.Setenv("VELA_LEASE_KEYRING_FILE", "/run/secrets/lease-keyring.json")
	t.Setenv("VELA_ARTIFACT_VALIDATOR_HELPER_PATH", "/usr/local/bin/vela-artifact-validator")
	t.Setenv("VELA_ARTIFACT_FFPROBE_PATH", "/usr/local/libexec/ffprobe-static")
	t.Setenv("VELA_ARTIFACT_SANDBOX_ROOT", "/var/lib/vela/sandbox")
	t.Setenv("VELA_ARTIFACT_SPOOL_DIRECTORY", "/var/lib/vela/spool")
	t.Setenv("VELA_ARTIFACT_FFPROBE_VERSION", "8.0.1")
	t.Setenv("VELA_ARTIFACT_VALIDATOR_REVISION", "ffprobe-8.0.1-sandbox-v1")
	t.Setenv(
		"VELA_ARTIFACT_RECONCILER_ID",
		"spiffe://vela.internal/reconciler/artifact-finalization",
	)
	t.Setenv("VELA_WORKER_GRPC_TLS_CERT_FILE", "/run/tls/worker-control/tls.crt")
	t.Setenv("VELA_WORKER_GRPC_TLS_KEY_FILE", "/run/tls/worker-control/tls.key")
	t.Setenv("VELA_WORKER_GRPC_CLIENT_CA_FILE", "/run/tls/worker-control/client-ca.crt")
	t.Setenv(
		"VELA_CREDENTIAL_PEPPER_BASE64",
		base64.StdEncoding.EncodeToString(make([]byte, 32)),
	)
	t.Setenv("VELA_NATS_CLIENT_CERT_FILE", "")
	t.Setenv("VELA_NATS_CLIENT_KEY_FILE", "")
	t.Setenv("VELA_OUTBOX_BATCH_SIZE", "")
	t.Setenv("VELA_SCHEDULER_TICK", "")
	t.Setenv("VELA_SCHEDULER_CLAIM_TTL", "")
	t.Setenv("VELA_SCHEDULER_CANDIDATE_ATTEMPTS", "")
}

func TestLoadConfigParsesBoundedSchedulerControls(t *testing.T) {
	setValidConfigEnvironment(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load default Scheduler config: %v", err)
	}
	if configuration.schedulerTick != 500*time.Millisecond ||
		configuration.schedulerClaimTTL != 30*time.Second ||
		configuration.schedulerCandidateAttempts != 5 {
		t.Fatalf(
			"default Scheduler controls = tick %s claim TTL %s attempts %d",
			configuration.schedulerTick,
			configuration.schedulerClaimTTL,
			configuration.schedulerCandidateAttempts,
		)
	}

	t.Setenv("VELA_SCHEDULER_TICK", "125ms")
	t.Setenv("VELA_SCHEDULER_CLAIM_TTL", "45s")
	t.Setenv("VELA_SCHEDULER_CANDIDATE_ATTEMPTS", "7")
	configuration, err = loadConfig()
	if err != nil {
		t.Fatalf("load explicit Scheduler config: %v", err)
	}
	if configuration.schedulerTick != 125*time.Millisecond ||
		configuration.schedulerClaimTTL != 45*time.Second ||
		configuration.schedulerCandidateAttempts != 7 {
		t.Fatalf(
			"explicit Scheduler controls = tick %s claim TTL %s attempts %d",
			configuration.schedulerTick,
			configuration.schedulerClaimTTL,
			configuration.schedulerCandidateAttempts,
		)
	}

	for _, test := range []struct {
		name  string
		env   string
		value string
	}{
		{name: "zero tick", env: "VELA_SCHEDULER_TICK", value: "0s"},
		{name: "oversized tick", env: "VELA_SCHEDULER_TICK", value: "1m1s"},
		{name: "invalid claim TTL", env: "VELA_SCHEDULER_CLAIM_TTL", value: "invalid"},
		{name: "oversized claim TTL", env: "VELA_SCHEDULER_CLAIM_TTL", value: "5m1s"},
		{name: "zero attempts", env: "VELA_SCHEDULER_CANDIDATE_ATTEMPTS", value: "0"},
		{name: "too many attempts", env: "VELA_SCHEDULER_CANDIDATE_ATTEMPTS", value: "21"},
	} {
		t.Run(test.name, func(t *testing.T) {
			setValidConfigEnvironment(t)
			t.Setenv(test.env, test.value)
			if _, loadErr := loadConfig(); loadErr == nil || !strings.Contains(loadErr.Error(), test.env) {
				t.Fatalf("loadConfig error = %v, want bounded %s rejection", loadErr, test.env)
			}
		})
	}
}

func TestReadLeaseKeyringRequiresOneStrictStrongKeyMap(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lease-keyring.json")
	strongKey := base64.StdEncoding.EncodeToString([]byte("0123456789abcdef0123456789abcdef"))
	if err := os.WriteFile(
		path,
		[]byte(`{"lease-key-v2":"`+strongKey+`"}`),
		0o600,
	); err != nil {
		t.Fatalf("write Lease keyring: %v", err)
	}
	keyring, err := readLeaseKeyring(path)
	if err != nil {
		t.Fatalf("readLeaseKeyring: %v", err)
	}
	if string(keyring["lease-key-v2"]) != "0123456789abcdef0123456789abcdef" {
		t.Fatalf("decoded Lease keyring = %#v", keyring)
	}

	for _, document := range []string{
		`{"lease-key-v2":"` + base64.StdEncoding.EncodeToString([]byte("too-short")) + `"}`,
		`{"lease-key-v2":"` + strongKey + `"} {}`,
		`{"":"` + strongKey + `"}`,
	} {
		if err := os.WriteFile(path, []byte(document), 0o600); err != nil {
			t.Fatalf("write invalid Lease keyring: %v", err)
		}
		if _, err := readLeaseKeyring(path); err == nil {
			t.Fatalf("readLeaseKeyring accepted %q", document)
		}
	}
}

func TestCancellationStopReconcilerRetriesAndStopsWithContext(t *testing.T) {
	reconciler := &testCancellationStopReconciler{calls: make(chan struct{}, 2)}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runCancellationStopReconciler(ctx, reconciler, time.Millisecond)
	}()

	for range 2 {
		select {
		case <-reconciler.calls:
		case <-time.After(time.Second):
			t.Fatal("cancellation stop reconciler did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("cancellation stop reconciler did not stop with context")
	}
}

func TestSchedulerRetriesTransientFailureAndStopsWithContext(t *testing.T) {
	scheduling := &testHierarchicalScheduler{
		calls:      make(chan struct{}, 2),
		reconciles: make(chan struct{}, 2),
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		runScheduler(ctx, scheduling, time.Millisecond)
	}()

	for range 2 {
		select {
		case <-scheduling.reconciles:
		case <-time.After(time.Second):
			t.Fatal("Scheduler did not reconcile expired claims")
		}
		select {
		case <-scheduling.calls:
		case <-time.After(time.Second):
			t.Fatal("Scheduler did not retry")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Scheduler did not stop with context")
	}
}

type testHierarchicalScheduler struct {
	invocations atomic.Int32
	calls       chan struct{}
	reconciles  chan struct{}
}

func (s *testHierarchicalScheduler) ReconcileExpired(context.Context) (int64, error) {
	s.reconciles <- struct{}{}
	return 1, nil
}

func (s *testHierarchicalScheduler) RunCycle(context.Context) ([]scheduler.Dispatch, error) {
	invocation := s.invocations.Add(1)
	s.calls <- struct{}{}
	if invocation == 1 {
		return nil, errors.New("transient Scheduler failure")
	}
	return nil, nil
}

type testCancellationStopReconciler struct {
	invocations atomic.Int32
	calls       chan struct{}
}

func (r *testCancellationStopReconciler) ReconcileNextCancellationStop(
	context.Context,
) (cancellation.StopResult, error) {
	invocation := r.invocations.Add(1)
	r.calls <- struct{}{}
	if invocation == 1 {
		return cancellation.StopResult{}, errors.New("transient reconciliation failure")
	}
	return cancellation.StopResult{Decision: cancellation.StopNoWork}, nil
}
