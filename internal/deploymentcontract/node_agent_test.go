package deploymentcontract

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

func TestNodeAgentDeploymentBindsWorkerEpochAndGPUDeviceTopology(t *testing.T) {
	unit := readNodeAgentDeploymentFile(t, "vela-node-agent.service")
	for _, required := range []string{
		"ExecStart=/usr/local/bin/vela-node-agent",
		"EnvironmentFile=/etc/vela/node-agent.env",
		"UMask=0077",
		"StateDirectory=vela-node-agent",
		"NoNewPrivileges=false",
	} {
		if !strings.Contains(unit, required) {
			t.Fatalf("Node Agent systemd unit lacks %q", required)
		}
	}

	contract := readNodeAgentDeploymentFile(t, "README.md")
	for _, required := range []string{
		"VELA_NODE_AGENT_WORKER_EPOCH",
		`"worker_epoch": 7`,
		`"pci_bdf": "0000:41:00.0"`,
		`"failure_classes": ["PROCESS_FAILURE"]`,
		"--vela-gpu-uuid=<canonical GPU UUID>",
		"--vela-pci-bdf=<canonical PCI BDF>",
	} {
		if !strings.Contains(contract, required) {
			t.Fatalf("Node Agent deployment contract lacks %q", required)
		}
	}
}

func readNodeAgentDeploymentFile(t *testing.T, name string) string {
	t.Helper()
	_, currentFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("resolve Node Agent deployment contract path")
	}
	path := filepath.Join(filepath.Dir(currentFile), "..", "..", "deploy", "node-agent", name)
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read Node Agent deployment file %s: %v", name, err)
	}
	return string(content)
}
