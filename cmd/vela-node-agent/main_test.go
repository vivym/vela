package main

import (
	"context"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/remediation"
	"google.golang.org/grpc"
)

func TestLoadConfigRequiresHostAgentBoundaries(t *testing.T) {
	t.Setenv("VELA_NODE_AGENT_WORKER_ID", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_NODE_AGENT_WORKER_ID") {
		t.Fatalf("missing Worker identity error = %v", err)
	}
}

func TestLoadConfigRequiresCurrentWorkerEpoch(t *testing.T) {
	setValidNodeAgentEnv(t)
	t.Setenv("VELA_NODE_AGENT_WORKER_EPOCH", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_NODE_AGENT_WORKER_EPOCH") {
		t.Fatalf("missing Worker epoch error = %v", err)
	}
}

func TestLoadCapabilitiesBindsGPUUUIDPCIBDFFailureAndAction(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "capabilities.json")
	const gpuUUID = "GPU-00000000-0000-0000-0000-000000000001"
	encoded, err := json.Marshal(map[string]capabilityConfig{
		gpuUUID: {
			CertificationRevision: "matrix-v1", PCIBDF: "0000:41:00.0",
			FailureClasses: []string{"PROCESS_FAILURE"}, Actions: []string{"L0_PROCESS_RESTART"},
		},
	})
	if err != nil {
		t.Fatalf("encode capability fixture: %v", err)
	}
	if err := os.WriteFile(path, encoded, 0o600); err != nil {
		t.Fatalf("write capability fixture: %v", err)
	}
	capabilities, err := loadCapabilities(path)
	if err != nil {
		t.Fatalf("loadCapabilities: %v", err)
	}
	policy, err := nodeagent.NewStaticCapabilityPolicy(capabilities)
	if err != nil {
		t.Fatalf("NewStaticCapabilityPolicy: %v", err)
	}
	binding, err := policy.Authorize(remediation.Plan{
		DeviceIdentity: gpuUUID, FailureClass: "PROCESS_FAILURE",
		ActionLevel: remediation.ActionL0ProcessRestart, CertificationRevision: "matrix-v1",
	})
	if err != nil || binding.GPUUUID != gpuUUID || binding.PCIBDF != "0000:41:00.0" {
		t.Fatalf("capability binding = %#v error=%v", binding, err)
	}
}

func TestLoadConfigRequiresWorkerHostQuotaBoundary(t *testing.T) {
	setValidNodeAgentEnv(t)
	t.Setenv("VELA_NODE_AGENT_WORKER_QUOTA_SOCKET", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_NODE_AGENT_WORKER_QUOTA_SOCKET") {
		t.Fatalf("missing Worker host quota socket error = %v", err)
	}
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("loadConfig: %v", err)
	}
	if configuration.workerUID != 10001 || configuration.workerGID != 10001 ||
		configuration.workerXFSProjectID != 7001 ||
		configuration.workerQuotaSocket == "" || configuration.workerScratchRoot == "" {
		t.Fatalf("Worker host quota configuration = %#v", configuration)
	}
}

func TestPortFromAddressRejectsWildcardOrInvalidPort(t *testing.T) {
	for _, address := range []string{"", ":0", ":65536", "127.0.0.1"} {
		if _, err := portFromAddress(address); err == nil {
			t.Fatalf("address %q was accepted", address)
		}
	}
	if port, err := portFromAddress("127.0.0.1:9443"); err != nil || port != 9443 {
		t.Fatalf("valid address = %d error=%v", port, err)
	}
}

func TestReadJSONFileRejectsWritableOrUnknownConfiguration(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "commands.json")
	if err := os.WriteFile(path, []byte(`{"L0_PROCESS_RESTART":{"path":"/bin/true","args":[],"unknown":true}}`), 0o600); err != nil {
		t.Fatalf("write JSON fixture: %v", err)
	}
	var commands map[string]commandConfig
	if err := readJSONFile(path, &commands); err == nil {
		t.Fatal("unknown command configuration field was accepted")
	}
	if err := os.WriteFile(path, []byte(`{"L0_PROCESS_RESTART":{"path":"/bin/true","args":[]}}`), 0o600); err != nil {
		t.Fatalf("replace JSON fixture: %v", err)
	}
	if err := os.Chmod(path, 0o622); err != nil {
		t.Fatalf("relax JSON fixture permissions: %v", err)
	}
	if err := readJSONFile(path, &commands); err == nil {
		t.Fatal("group/world-writable command configuration was accepted")
	}
}

func TestServeGRPCUntilCanceledStopsEveryListener(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	firstListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen first endpoint: %v", err)
	}
	secondListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = firstListener.Close()
		t.Fatalf("listen second endpoint: %v", err)
	}
	firstServer := grpc.NewServer()
	secondServer := grpc.NewServer()
	done := make(chan error, 1)
	go func() {
		done <- serveGRPCUntilCanceled(ctx, []grpcEndpoint{
			{name: "first", server: firstServer, listener: firstListener},
			{name: "second", server: secondServer, listener: secondListener},
		})
	}()
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveGRPCUntilCanceled: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Node Agent gRPC servers did not stop after cancellation")
	}
	for _, address := range []string{firstListener.Addr().String(), secondListener.Addr().String()} {
		connection, err := net.DialTimeout("tcp", address, 50*time.Millisecond)
		if err == nil {
			_ = connection.Close()
			t.Fatalf("gRPC listener %s remained open after shutdown", address)
		}
	}
}

func setValidNodeAgentEnv(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	values := map[string]string{
		"VELA_NODE_AGENT_ADDRESS":               "127.0.0.1:9443",
		"VELA_NODE_AGENT_NODE_IDENTITY":         "node-1",
		"VELA_NODE_AGENT_WORKER_ID":             "83000000-0000-0000-0000-000000000001",
		"VELA_NODE_AGENT_WORKER_EPOCH":          "7",
		"VELA_NODE_AGENT_TLS_CERT_FILE":         filepath.Join(root, "tls.crt"),
		"VELA_NODE_AGENT_TLS_KEY_FILE":          filepath.Join(root, "tls.key"),
		"VELA_NODE_AGENT_CONTROLLER_CA_FILE":    filepath.Join(root, "ca.crt"),
		"VELA_NODE_AGENT_RECEIPT_DIRECTORY":     filepath.Join(root, "receipts"),
		"VELA_NODE_AGENT_CONTROLLERS_FILE":      filepath.Join(root, "controllers.json"),
		"VELA_NODE_AGENT_COMMANDS_FILE":         filepath.Join(root, "commands.json"),
		"VELA_NODE_AGENT_CAPABILITIES_FILE":     filepath.Join(root, "capabilities.json"),
		"VELA_NODE_AGENT_POSTCHECK_PATH":        "/usr/local/libexec/vela-postcheck",
		"VELA_NODE_AGENT_FENCE_PATH":            "/usr/local/libexec/vela-fence",
		"VELA_NODE_AGENT_WORKER_QUOTA_SOCKET":   filepath.Join(root, "worker-quota.sock"),
		"VELA_NODE_AGENT_WORKER_UID":            "10001",
		"VELA_NODE_AGENT_WORKER_GID":            "10001",
		"VELA_NODE_AGENT_WORKER_SCRATCH_ROOT":   filepath.Join(root, "scratch"),
		"VELA_NODE_AGENT_WORKER_XFS_DEVICE":     "/dev/nvme1n1",
		"VELA_NODE_AGENT_WORKER_XFS_PROJECT_ID": "7001",
		"VELA_NODE_AGENT_POSTCHECK_ARGS_JSON":   "[]",
		"VELA_NODE_AGENT_FENCE_ARGS_JSON":       "[]",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
}
