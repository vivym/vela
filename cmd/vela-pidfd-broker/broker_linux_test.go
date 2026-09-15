//go:build linux

package main

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBrokerDoesNotUnlinkPathnameReplacement(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to bind the protected broker socket")
	}
	root, err := os.MkdirTemp("/run", "vela-pidfd-broker-unit-")
	if err != nil {
		t.Skipf("/run unavailable: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(root, "pidfd-broker.sock")
	oldSocket := os.Getenv("VELA_PIDFD_BROKER_SOCKET")
	oldGID := os.Getenv("VELA_PIDFD_BROKER_RUNTIME_GID")
	t.Cleanup(func() {
		_ = os.Setenv("VELA_PIDFD_BROKER_SOCKET", oldSocket)
		_ = os.Setenv("VELA_PIDFD_BROKER_RUNTIME_GID", oldGID)
	})
	if err := os.Setenv("VELA_PIDFD_BROKER_SOCKET", socketPath); err != nil {
		t.Fatal(err)
	}
	if err := os.Setenv("VELA_PIDFD_BROKER_RUNTIME_GID", "65532"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- run(ctx) }()
	deadline := time.Now().Add(time.Second)
	for {
		if _, err := os.Lstat(socketPath); err == nil {
			break
		}
		if time.Now().After(deadline) {
			cancel()
			t.Fatal("broker did not publish socket")
		}
		time.Sleep(time.Millisecond)
	}
	if err := os.Remove(socketPath); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(socketPath, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("broker stop error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("broker did not stop")
	}
	if data, err := os.ReadFile(socketPath); err != nil || string(data) != "replacement" {
		t.Fatalf("pathname replacement was removed or changed: %q %v", data, err)
	}
}
