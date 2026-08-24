package main

import (
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
