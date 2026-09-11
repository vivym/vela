package main

import (
	"errors"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/nodeagent"
)

func TestRuntimeStartupAuthorityInjectionRequiresEnabledModeAndPlan(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	if _, err := newRuntimeStartupAuthority(configuration, nil, nodeagent.RuntimeStartupAuthorityConfig{}); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled injection result = %v", err)
	}
	configuration.runtimeStartupEnabled = true
	if _, err := newRuntimeStartupAuthority(configuration, nil, nodeagent.RuntimeStartupAuthorityConfig{}); !errors.Is(err, nodeagent.ErrRuntimeStartupAuthority) {
		t.Fatalf("missing plan injection error = %v", err)
	}
}
