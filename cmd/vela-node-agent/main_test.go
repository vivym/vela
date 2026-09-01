package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/remediation"
	"google.golang.org/grpc"
)

func TestLoadConfigRequiresHostAgentBoundaries(t *testing.T) {
	t.Setenv("VELA_NODE_AGENT_ID", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_NODE_AGENT_ID") {
		t.Fatalf("missing Agent identity error = %v", err)
	}
}

func TestLoadConfigRequiresCurrentAgentEpoch(t *testing.T) {
	setValidNodeAgentEnv(t)
	t.Setenv("VELA_NODE_AGENT_EPOCH", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_NODE_AGENT_EPOCH") {
		t.Fatalf("missing Agent epoch error = %v", err)
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

func TestLoadConfigRequiresWorkerInstanceOutboundReportingBoundary(t *testing.T) {
	required := []string{
		"VELA_NODE_AGENT_FLEET_ADDRESS",
		"VELA_NODE_AGENT_FLEET_SERVER_NAME",
		"VELA_NODE_AGENT_FLEET_CA_FILE",
		"VELA_NODE_AGENT_FLEET_CLIENT_CERT_FILE",
		"VELA_NODE_AGENT_FLEET_CLIENT_KEY_FILE",
		"VELA_NODE_AGENT_WORKER_INSTANCES_FILE",
		"VELA_NODE_AGENT_WORKER_INSTANCE_STATE_DIRECTORY",
		"VELA_NODE_AGENT_NVIDIA_SMI_PATH",
		"VELA_NODE_AGENT_PCI_BUS_DEVICES_ROOT",
		"VELA_NODE_AGENT_SYS_DEVICES_ROOT",
		"VELA_NODE_AGENT_NVIDIA_DRIVER_VERSION_PATH",
		"VELA_NODE_AGENT_BOOT_ID_PATH",
	}
	for _, name := range required {
		t.Run(name, func(t *testing.T) {
			setValidNodeAgentEnv(t)
			t.Setenv(name, "")
			if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), name) {
				t.Fatalf("missing outbound configuration %s error=%v", name, err)
			}
		})
	}

	setValidNodeAgentEnv(t)
	t.Setenv("VELA_NODE_AGENT_WORKER_INSTANCE_REPORT_INTERVAL", "45s")
	t.Setenv("VELA_NODE_AGENT_WORKER_INSTANCE_CALL_TIMEOUT", "12s")
	t.Setenv("VELA_NODE_AGENT_WORKER_INSTANCE_BACKOFF_INITIAL", "2s")
	t.Setenv("VELA_NODE_AGENT_WORKER_INSTANCE_BACKOFF_MAX", "20s")
	t.Setenv("VELA_NODE_AGENT_WORKER_INSTANCE_EVIDENCE_TTL", "3m")
	t.Setenv("VELA_NODE_AGENT_FLEET_DIAL_TIMEOUT", "18s")
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load outbound Node Agent configuration: %v", err)
	}
	if configuration.fleetAddress != "fleet.vela-system.svc:8444" ||
		configuration.fleetServerName != "fleet.vela-system.svc" ||
		configuration.workerInstanceReportInterval != 45*time.Second ||
		configuration.workerInstanceCallTimeout != 12*time.Second ||
		configuration.workerInstanceBackoffInitial != 2*time.Second ||
		configuration.workerInstanceBackoffMax != 20*time.Second ||
		configuration.workerInstanceEvidenceTTL != 3*time.Minute ||
		configuration.fleetDialTimeout != 18*time.Second {
		t.Fatalf("outbound Node Agent configuration=%#v", configuration)
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

func TestLoadWorkerInstanceTemplatesInjectsCanonicalNodeAgentIdentity(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker-instances.json")
	if err := os.WriteFile(path, []byte(validWorkerInstanceTemplatesJSON()), 0o600); err != nil {
		t.Fatalf("write WorkerInstance templates: %v", err)
	}
	identity := nodeagent.NodeAgentIdentity{
		NodeIdentity: "node-1",
		AgentID:      uuid.MustParse("83000000-0000-0000-0000-000000000001"),
		AgentEpoch:   7,
	}
	templates, err := loadWorkerInstanceTemplates(path, identity)
	if err != nil {
		t.Fatalf("load WorkerInstance templates: %v", err)
	}
	if len(templates) != 1 ||
		templates[0].ObservedBy != nodeagent.NodeAgentSPIFFEIdentity(identity) ||
		templates[0].Evidence.WorkerInstanceID.String() != "49440000-0000-0000-0000-000000000001" ||
		templates[0].Evidence.DeviceSet.Devices[0].NodeIdentity != identity.NodeIdentity ||
		templates[0].Evidence.Capacity.Vector["concurrency"] != 1 ||
		templates[0].Evidence.Capacity.Sequence != 0 ||
		!templates[0].Evidence.ObservedAt.IsZero() || templates[0].Evidence.ObservedBy != "" {
		t.Fatalf("loaded WorkerInstance templates=%#v", templates)
	}
}

func TestLoadWorkerInstanceTemplatesRejectsRuntimeFieldsDuplicateKeysAndCrossNode(t *testing.T) {
	identity := nodeagent.NodeAgentIdentity{
		NodeIdentity: "node-1",
		AgentID:      uuid.MustParse("83000000-0000-0000-0000-000000000001"),
		AgentEpoch:   7,
	}
	cases := map[string]string{
		"top-level runtime field": strings.Replace(
			validWorkerInstanceTemplatesJSON(),
			`"capacity":`,
			`"observed_at":"2026-08-30T00:00:00Z","capacity":`,
			1,
		),
		"capacity sequence": strings.Replace(
			validWorkerInstanceTemplatesJSON(),
			`"capacity":{"vector":`,
			`"capacity":{"sequence":1,"vector":`,
			1,
		),
		"device epoch": strings.Replace(
			validWorkerInstanceTemplatesJSON(),
			`"ordinal":0`,
			`"ordinal":0,"device_epoch":1`,
			1,
		),
		"duplicate JSON key": strings.Replace(
			validWorkerInstanceTemplatesJSON(),
			`"schema_version":1,`,
			`"schema_version":1,"schema_version":1,`,
			1,
		),
		"cross Node": strings.Replace(
			validWorkerInstanceTemplatesJSON(),
			`"node_identity":"node-1"`,
			`"node_identity":"node-2"`,
			1,
		),
		"duplicate WorkerInstance": strings.TrimSuffix(validWorkerInstanceTemplatesJSON(), "]") +
			"," + strings.TrimPrefix(validWorkerInstanceTemplatesJSON(), "[") + "]",
		"duplicate DeviceSet across WorkerInstances": twoWorkerInstanceTemplatesJSON(
			"49450000-0000-0000-0000-000000000002",
			"49440000-0000-0000-0000-000000000002",
		),
		"duplicate Device across WorkerInstances": twoWorkerInstanceTemplatesJSON(
			"49450000-0000-0000-0000-000000000003",
			"49440000-0000-0000-0000-000000000003",
		),
		"duplicate GPU UUID across WorkerInstances": twoWorkerInstanceTemplatesJSON(
			"GPU-00000000-0000-0000-0000-000000000002",
			"GPU-00000000-0000-0000-0000-000000000001",
		),
		"duplicate PCI BDF across WorkerInstances": twoWorkerInstanceTemplatesJSON(
			"0000:42:00.0",
			"0000:41:00.0",
		),
		"duplicate ModelResidency across WorkerInstances": twoWorkerInstanceTemplatesJSON(
			"49450000-0000-0000-0000-000000000006",
			"49440000-0000-0000-0000-000000000006",
		),
	}
	for name, encoded := range cases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "worker-instances.json")
			if err := os.WriteFile(path, []byte(encoded), 0o600); err != nil {
				t.Fatalf("write WorkerInstance template fixture: %v", err)
			}
			if _, err := loadWorkerInstanceTemplates(path, identity); err == nil {
				t.Fatal("invalid WorkerInstance template file was accepted")
			}
		})
	}
}

func twoWorkerInstanceTemplatesJSON(oldIdentity, newIdentity string) string {
	first := validWorkerInstanceTemplatesJSON()
	second := strings.ReplaceAll(first, "49440000", "49450000")
	second = strings.Replace(
		second,
		"GPU-00000000-0000-0000-0000-000000000001",
		"GPU-00000000-0000-0000-0000-000000000002",
		1,
	)
	second = strings.Replace(second, "0000:41:00.0", "0000:42:00.0", 1)
	second = strings.Replace(second, oldIdentity, newIdentity, 1)
	return strings.TrimSuffix(first, "]") + "," + strings.TrimPrefix(second, "[")
}

func validWorkerInstanceTemplatesJSON() string {
	return `[{` +
		`"schema_version":1,` +
		`"worker_instance_id":"49440000-0000-0000-0000-000000000001",` +
		`"instance_epoch":1,"control_session_epoch":1,` +
		`"device_set":{"id":"49440000-0000-0000-0000-000000000002","devices":[{` +
		`"id":"49440000-0000-0000-0000-000000000003",` +
		`"compute_node_id":"49440000-0000-0000-0000-000000000004",` +
		`"node_identity":"node-1","region":"cn-shanghai",` +
		`"network_domain":"h3-rack-01","fault_domain":"power-a",` +
		`"kind":"GPU","gpu_uuid":"GPU-00000000-0000-0000-0000-000000000001",` +
		`"pci_bdf":"0000:41:00.0","ordinal":0` +
		`}]},` +
		`"members":[{"id":"49440000-0000-0000-0000-000000000005",` +
		`"member_key":"dit-0","compute_node_id":"49440000-0000-0000-0000-000000000004",` +
		`"member_epoch":1,"device_ids":["49440000-0000-0000-0000-000000000003"],` +
		`"readiness":"READY"}],` +
		`"residencies":[{"id":"49440000-0000-0000-0000-000000000006",` +
		`"model_component_revision":"h3-dit-v1","runtime_identity":"h3-dit-runtime-v1",` +
		`"runtime_image_digest":"sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",` +
		`"model_runtime_epoch":1,"state":"READY",` +
		`"warmup_evidence_digest":"dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",` +
		`"canary_evidence_digest":"eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee"}],` +
		`"capacity":{"vector":{"concurrency":1}}` +
		`}]`
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

func TestServeGRPCUntilCanceledStopsBackgroundProcesses(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen endpoint: %v", err)
	}
	server := grpc.NewServer()
	started := make(chan struct{})
	stopped := make(chan struct{})
	done := make(chan error, 1)
	go func() {
		done <- serveGRPCUntilCanceled(
			ctx,
			[]grpcEndpoint{{name: "Node Agent", server: server, listener: listener}},
			nodeAgentProcess{
				name: "WorkerInstance evidence reporting",
				run: func(processContext context.Context) error {
					close(started)
					<-processContext.Done()
					close(stopped)
					return nil
				},
			},
		)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("background process did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("serveGRPCUntilCanceled: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Node Agent did not wait for its background process to stop")
	}
	select {
	case <-stopped:
	default:
		t.Fatal("background process was not canceled before shutdown returned")
	}
}

func TestServeGRPCUntilCanceledReturnsBackgroundProcessError(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen endpoint: %v", err)
	}
	server := grpc.NewServer()
	err = serveGRPCUntilCanceled(
		context.Background(),
		[]grpcEndpoint{{name: "Node Agent", server: server, listener: listener}},
		nodeAgentProcess{
			name: "WorkerInstance evidence reporting",
			run: func(context.Context) error {
				return errors.New("report loop failed")
			},
		},
	)
	if err == nil || !strings.Contains(
		err.Error(),
		"run node-agent process WorkerInstance evidence reporting: report loop failed",
	) {
		t.Fatalf("background process error=%v", err)
	}
	connection, dialErr := net.DialTimeout("tcp", listener.Addr().String(), 50*time.Millisecond)
	if dialErr == nil {
		_ = connection.Close()
		t.Fatal("gRPC listener remained open after background process failure")
	}
}

func setValidNodeAgentEnv(t *testing.T) {
	t.Helper()
	root := t.TempDir()
	values := map[string]string{
		"VELA_NODE_AGENT_ADDRESS":             "127.0.0.1:9443",
		"VELA_NODE_AGENT_NODE_IDENTITY":       "node-1",
		"VELA_NODE_AGENT_ID":                  "83000000-0000-0000-0000-000000000001",
		"VELA_NODE_AGENT_EPOCH":               "7",
		"VELA_NODE_AGENT_TLS_CERT_FILE":       filepath.Join(root, "tls.crt"),
		"VELA_NODE_AGENT_TLS_KEY_FILE":        filepath.Join(root, "tls.key"),
		"VELA_NODE_AGENT_CONTROLLER_CA_FILE":  filepath.Join(root, "ca.crt"),
		"VELA_NODE_AGENT_RECEIPT_DIRECTORY":   filepath.Join(root, "receipts"),
		"VELA_NODE_AGENT_CONTROLLERS_FILE":    filepath.Join(root, "controllers.json"),
		"VELA_NODE_AGENT_COMMANDS_FILE":       filepath.Join(root, "commands.json"),
		"VELA_NODE_AGENT_CAPABILITIES_FILE":   filepath.Join(root, "capabilities.json"),
		"VELA_NODE_AGENT_POSTCHECK_PATH":      "/usr/local/libexec/vela-postcheck",
		"VELA_NODE_AGENT_FENCE_PATH":          "/usr/local/libexec/vela-fence",
		"VELA_NODE_AGENT_POSTCHECK_ARGS_JSON": "[]",
		"VELA_NODE_AGENT_FENCE_ARGS_JSON":     "[]",
		"VELA_NODE_AGENT_FLEET_ADDRESS":       "fleet.vela-system.svc:8444",
		"VELA_NODE_AGENT_FLEET_SERVER_NAME":   "fleet.vela-system.svc",
		"VELA_NODE_AGENT_FLEET_CA_FILE":       filepath.Join(root, "fleet-ca.crt"),
		"VELA_NODE_AGENT_FLEET_CLIENT_CERT_FILE": filepath.Join(
			root,
			"fleet-client.crt",
		),
		"VELA_NODE_AGENT_FLEET_CLIENT_KEY_FILE": filepath.Join(root, "fleet-client.key"),
		"VELA_NODE_AGENT_WORKER_INSTANCES_FILE": filepath.Join(root, "worker-instances.json"),
		"VELA_NODE_AGENT_WORKER_INSTANCE_STATE_DIRECTORY": filepath.Join(
			root,
			"worker-instance-state",
		),
		"VELA_NODE_AGENT_NVIDIA_SMI_PATH":            "/usr/bin/nvidia-smi",
		"VELA_NODE_AGENT_PCI_BUS_DEVICES_ROOT":       "/sys/bus/pci/devices",
		"VELA_NODE_AGENT_SYS_DEVICES_ROOT":           "/sys/devices",
		"VELA_NODE_AGENT_NVIDIA_DRIVER_VERSION_PATH": "/proc/driver/nvidia/version",
		"VELA_NODE_AGENT_BOOT_ID_PATH":               "/proc/sys/kernel/random/boot_id",
	}
	for name, value := range values {
		t.Setenv(name, value)
	}
}
