package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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

func TestLoadRuntimeContainerObserverRequiresEnabledTrustedSocket(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	if observer, err := loadRuntimeContainerObserver(context.Background(), configuration); err == nil || observer != nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled CRI observer result observer=%v error=%v", observer, err)
	}
	configuration.runtimeStartupEnabled = true
	configuration.runtimeCRISocket = filepath.Join(t.TempDir(), "containerd.sock")
	if observer, err := loadRuntimeContainerObserver(context.Background(), configuration); err == nil || observer != nil {
		t.Fatalf("missing CRI socket result observer=%v error=%v", observer, err)
	}
	if observer, err := loadRuntimeContainerObserver(nil, configuration); err == nil || observer != nil || !errors.Is(err, nodeagent.ErrRuntimeObserverCustody) {
		t.Fatalf("nil context CRI observer result observer=%v error=%v", observer, err)
	}
}

func TestListenRuntimeStartupSocketOwnsProtectedPath(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	if socket, err := listenRuntimeStartupSocket(configuration); err == nil || socket != nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled startup socket result socket=%v error=%v", socket, err)
	}
	configuration.runtimeStartupEnabled = true
	configuration.runtimeStartupSocket = filepath.Join(t.TempDir(), "startup.sock")
	socket, err := listenRuntimeStartupSocket(configuration)
	if err != nil {
		t.Fatalf("listen runtime startup socket: %v", err)
	}
	if socket.Listener() == nil {
		t.Fatal("startup socket listener is nil")
	}
	info, err := os.Stat(configuration.runtimeStartupSocket)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		t.Fatalf("startup socket metadata info=%v error=%v", info, err)
	}
	if err := socket.Close(); err != nil {
		t.Fatalf("close startup socket: %v", err)
	}
	if _, err := os.Lstat(configuration.runtimeStartupSocket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startup socket path after close error=%v", err)
	}
}
