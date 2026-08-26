package main

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/workeragent"
)

func TestLoadConfigRequiresWorkerRuntimeBoundaries(t *testing.T) {
	t.Setenv("VELA_WORKER_ID", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_WORKER_ID") {
		t.Fatalf("missing Worker identity error = %v", err)
	}
}

func TestLoadConfigAcceptsExplicitDevelopmentStatFSProbe(t *testing.T) {
	setValidWorkerAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if configuration.scratchProbe != scratchProbeStatFSDevelopment ||
		configuration.workerEpoch != 9 || configuration.nodeIdentity != "h3-node-01" ||
		configuration.runnerExpectedUID != 10001 ||
		configuration.outputOwnerUID != 10001 || configuration.heartbeatInterval != 20*time.Second ||
		configuration.capacityReportInterval != 30*time.Second ||
		configuration.outputCleanupMinBytesPerSecond != 1<<20 ||
		configuration.artifactStoreHealthURL != "https://artifact-store.vela.internal/healthz" ||
		configuration.artifactStoreProbeTimeout != 2*time.Second {
		t.Fatalf("configuration = %#v", configuration)
	}
}

func TestLoadConfigRequiresKubernetesNodeIdentity(t *testing.T) {
	for _, value := range []string{"", "H3-Node-01", "h3..node", "-h3-node-01"} {
		t.Run(value, func(t *testing.T) {
			setValidWorkerAgentEnv(t)
			t.Setenv("VELA_WORKER_NODE_IDENTITY", value)
			if _, err := loadConfig(); err == nil ||
				!strings.Contains(err.Error(), "VELA_WORKER_NODE_IDENTITY") {
				t.Fatalf("invalid Worker node identity %q error = %v", value, err)
			}
		})
	}
}

func TestLoadConfigRequiresAnHTTPSArtifactStoreHealthProbe(t *testing.T) {
	setValidWorkerAgentEnv(t)
	t.Setenv("VELA_WORKER_ARTIFACT_STORE_HEALTH_URL", "")
	if _, err := loadConfig(); err == nil ||
		!strings.Contains(err.Error(), "VELA_WORKER_ARTIFACT_STORE_HEALTH_URL") {
		t.Fatalf("missing Artifact-store health URL error = %v", err)
	}

	setValidWorkerAgentEnv(t)
	t.Setenv("VELA_WORKER_ARTIFACT_STORE_HEALTH_URL", "http://artifact-store/healthz")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "HTTPS") {
		t.Fatalf("insecure Artifact-store health URL error = %v", err)
	}
}

func TestLoadConfigRequiresRecoveryAndOutputUnderScratchRoot(t *testing.T) {
	setValidWorkerAgentEnv(t)
	t.Setenv("VELA_WORKER_OUTPUT_ROOT", filepath.Join(t.TempDir(), "outside"))
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "under VELA_WORKER_SCRATCH_ROOT") {
		t.Fatalf("outside output root error = %v", err)
	}
}

func TestLoadConfigRequiresCertifiedOutputCleanupThroughput(t *testing.T) {
	setValidWorkerAgentEnv(t)
	t.Setenv("VELA_WORKER_OUTPUT_CLEANUP_MIN_BYTES_PER_SECOND", "")

	if _, err := loadConfig(); err == nil ||
		!strings.Contains(err.Error(), "VELA_WORKER_OUTPUT_CLEANUP_MIN_BYTES_PER_SECOND") {
		t.Fatalf("missing output cleanup throughput error = %v", err)
	}
}

func TestLoadConfigRejectsOutputCleanupBudgetBeyondTerminalRetention(t *testing.T) {
	setValidWorkerAgentEnv(t)
	t.Setenv("VELA_WORKER_OUTPUT_CLEANUP_MIN_BYTES_PER_SECOND", "1")

	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "terminal retention") {
		t.Fatalf("unbounded output cleanup budget error = %v", err)
	}
}

func TestLoadConfigIncludesControlPhaseInTerminalCleanupBudget(t *testing.T) {
	setValidWorkerAgentEnv(t)
	t.Setenv("VELA_WORKER_OUTPUT_CLEANUP_MIN_BYTES_PER_SECOND", "1073741824")
	t.Setenv("VELA_WORKER_TERMINAL_RETENTION", "20s")

	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "terminal retention") {
		t.Fatalf("control-plus-output cleanup budget error = %v", err)
	}
}

func TestLoadConfigRequiresXFSProjectQuotaInputsForProduction(t *testing.T) {
	setValidWorkerAgentEnv(t)
	t.Setenv("VELA_WORKER_SCRATCH_PROBE", scratchProbeXFSProjectQuota)
	t.Setenv("VELA_WORKER_HOST_QUOTA_SOCKET", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_WORKER_HOST_QUOTA_SOCKET") {
		t.Fatalf("missing Worker host quota socket error = %v", err)
	}
	setValidWorkerAgentEnv(t)
	t.Setenv("VELA_WORKER_SCRATCH_PROBE", scratchProbeXFSProjectQuota)
	t.Setenv("VELA_WORKER_HOST_QUOTA_SOCKET", filepath.Join(t.TempDir(), "quota.sock"))
	t.Setenv("VELA_WORKER_HOST_QUOTA_SOCKET_UID", "0")
	t.Setenv("VELA_WORKER_HOST_QUOTA_SOCKET_GID", "10001")
	t.Setenv("VELA_WORKER_XFS_PROJECT_ID", "7001")
	t.Setenv("VELA_WORKER_XFS_DEVICE", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_WORKER_XFS_DEVICE") {
		t.Fatalf("missing XFS device error = %v", err)
	}
	t.Setenv("VELA_WORKER_XFS_DEVICE", "/dev/nvme1n1")
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load production config: %v", err)
	}
	if configuration.workerHostQuotaSocket == "" ||
		configuration.workerHostExpectedSocketUID != 0 ||
		configuration.workerHostExpectedSocketGID != 10001 ||
		configuration.workerHostQuotaTimeout != 5*time.Second {
		t.Fatalf("Worker host quota configuration = %#v", configuration)
	}
}

func TestRunAgentLoopBacksOffAndResetsAfterSuccess(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	runOnce := func(context.Context) (workeragent.Result, error) {
		calls++
		switch calls {
		case 1, 2:
			return workeragent.Result{}, errors.New("transient control failure")
		case 3:
			return workeragent.Result{Outcome: workeragent.OutcomeIdle}, nil
		default:
			cancel()
			return workeragent.Result{}, context.Canceled
		}
	}
	var waits []time.Duration
	wait := func(ctx context.Context, delay time.Duration) error {
		waits = append(waits, delay)
		return nil
	}
	if err := runAgentLoop(ctx, runOnce, wait, time.Second, 8*time.Second, 250*time.Millisecond, nil); err != nil {
		t.Fatalf("runAgentLoop: %v", err)
	}
	if !reflect.DeepEqual(waits, []time.Duration{time.Second, 2 * time.Second, 250 * time.Millisecond}) {
		t.Fatalf("waits = %v", waits)
	}
}

func TestRunAgentLoopDoesNotStartWorkAfterShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := runAgentLoop(
		ctx,
		func(context.Context) (workeragent.Result, error) {
			calls++
			return workeragent.Result{}, nil
		},
		func(context.Context, time.Duration) error { return nil },
		time.Second,
		2*time.Second,
		time.Second,
		nil,
	)
	if err != nil || calls != 0 {
		t.Fatalf("shutdown loop error=%v calls=%d", err, calls)
	}
}

func setValidWorkerAgentEnv(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	values := map[string]string{
		"VELA_WORKER_ID":                                  "81000000-0000-0000-0000-000000000001",
		"VELA_WORKER_EPOCH":                               "9",
		"VELA_WORKER_NODE_IDENTITY":                       "h3-node-01",
		"VELA_WORKER_CONTROL_ADDRESS":                     "control.vela.internal:8443",
		"VELA_WORKER_CONTROL_SERVER_NAME":                 "control.vela.internal",
		"VELA_WORKER_TLS_CERT_FILE":                       filepath.Join(root, "tls.crt"),
		"VELA_WORKER_TLS_KEY_FILE":                        filepath.Join(root, "tls.key"),
		"VELA_WORKER_CONTROL_CA_FILE":                     filepath.Join(root, "ca.crt"),
		"VELA_WORKER_RUNNER_SOCKET":                       filepath.Join(root, "runner.sock"),
		"VELA_WORKER_RUNNER_EXPECTED_UID":                 "10001",
		"VELA_WORKER_SCRATCH_ROOT":                        filepath.Join(root, "scratch"),
		"VELA_WORKER_RECOVERY_ROOT":                       filepath.Join(root, "scratch", "recovery"),
		"VELA_WORKER_OUTPUT_ROOT":                         filepath.Join(root, "scratch", "outputs"),
		"VELA_WORKER_OUTPUT_OWNER_UID":                    "10001",
		"VELA_WORKER_OUTPUT_CLEANUP_MIN_BYTES_PER_SECOND": "1048576",
		"VELA_WORKER_INFERENCE_BACKEND_REVISION":          "sglang@sha256:test",
		"VELA_WORKER_ARTIFACT_STORE_HEALTH_URL":           "https://artifact-store.vela.internal/healthz",
		"VELA_WORKER_ARTIFACT_STORE_CA_FILE":              filepath.Join(root, "artifact-store-ca.crt"),
		"VELA_WORKER_SCRATCH_PROBE":                       scratchProbeStatFSDevelopment,
		"VELA_WORKER_ATTEMPT_QUOTA_BYTES":                 "1073741824",
		"VELA_WORKER_MAX_ENTRY_BYTES":                     "16777216",
		"VELA_WORKER_MAX_ENTRIES":                         "1024",
		"VELA_WORKER_HIGH_WATERMARK_BYTES":                "805306368",
		"VELA_WORKER_LOW_WATERMARK_BYTES":                 "536870912",
		"VELA_WORKER_CRITICAL_FREE_BYTES":                 "134217728",
		"VELA_WORKER_CAPACITY_REPORT_INTERVAL":            "30s",
		"VELA_WORKER_TERMINAL_RETENTION":                  "1h",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
}
