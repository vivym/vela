package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadConfigRequiresHostAgentBoundaries(t *testing.T) {
	t.Setenv("VELA_NODE_AGENT_WORKER_ID", "")
	if _, err := loadConfig(); err == nil || !strings.Contains(err.Error(), "VELA_NODE_AGENT_WORKER_ID") {
		t.Fatalf("missing Worker identity error = %v", err)
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
