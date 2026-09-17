//go:build linux

package main

import (
	"errors"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRemoveStaleSocketRemovesRefusedListenerPath(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("stale socket replacement is a root-owned production boundary")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "issuer.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	listener.SetUnlinkOnClose(false)
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("listener did not leave a pathname for stale probe: %v", err)
	}
	if err := removeStaleSocket(path); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale socket remains: %v", err)
	}
}

func TestRemoveStaleSocketRefusesLiveListener(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("stale socket replacement is a root-owned production boundary")
	}
	directory := t.TempDir()
	path := filepath.Join(directory, "issuer.sock")
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(listener.Close)
	if err := removeStaleSocket(path); err == nil || !strings.Contains(err.Error(), "refusing to replace") {
		t.Fatalf("live listener was not protected: %v", err)
	}
}
