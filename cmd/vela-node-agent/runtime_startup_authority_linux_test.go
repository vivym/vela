package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
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

func TestReceiveRuntimeStartupCallerRequiresTrustedAssembly(t *testing.T) {
	if caller, err := receiveRuntimeStartupCaller(context.Background(), nil, nil); caller != nil || !errors.Is(err, nodeagent.ErrRuntimeCallerIdentity) {
		t.Fatalf("nil startup caller assembly result caller=%v error=%v", caller, err)
	}
}

func TestRuntimeStartupLifecycleRejectsNilContext(t *testing.T) {
	if err := (&runtimeStartupLifecycle{}).Shutdown(nil); !errors.Is(err, nodeagent.ErrRuntimeCallerIdentity) {
		t.Fatalf("nil lifecycle context error = %v", err)
	}
	if err := (*runtimeStartupLifecycle)(nil).Shutdown(context.Background()); err != nil {
		t.Fatalf("nil lifecycle shutdown error = %v", err)
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
	root, err := os.MkdirTemp(os.Getenv("HOME"), "vela-runtime-startup-")
	if err != nil {
		t.Fatalf("create trusted startup socket directory: %v", err)
	}
	defer os.RemoveAll(root)
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("protect startup socket directory: %v", err)
	}
	configuration.runtimeStartupSocket = filepath.Join(root, "startup.sock")
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

func TestListenRuntimeStartupSocketPublishesRuntimeGID(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("GID publication is a root-only production path")
	}
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	configuration.runtimeStartupEnabled = true
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupSocket = filepath.Join(root, "startup.sock")
	const runtimeGID = uint32(1)
	socket, err := listenRuntimeStartupSocketWithGID(configuration, runtimeGID)
	if err != nil {
		t.Fatalf("publish runtime GID socket: %v", err)
	}
	defer socket.Close()
	info, err := os.Stat(configuration.runtimeStartupSocket)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || stat.Gid != runtimeGID || info.Mode().Perm() != 0o660 {
		t.Fatalf("unexpected runtime socket ownership uid=%d gid=%d mode=%o", stat.Uid, stat.Gid, info.Mode().Perm())
	}
}

func TestLoadRuntimeStartupResourcesStopsBeforeAnyImplicitFallback(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	if resources, err := loadRuntimeStartupResources(context.Background(), configuration); err == nil || resources != nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled resources result resources=%v error=%v", resources, err)
	}
	configuration.runtimeStartupEnabled = true
	if resources, err := loadRuntimeStartupResources(context.Background(), configuration); err == nil || resources != nil || !strings.Contains(err.Error(), "path is missing") {
		t.Fatalf("incomplete resources result resources=%v error=%v", resources, err)
	}
	if err := (&runtimeStartupResources{}).Close(); err != nil {
		t.Fatalf("empty resource close: %v", err)
	}
}
