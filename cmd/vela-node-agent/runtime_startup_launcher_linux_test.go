//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"golang.org/x/sys/unix"
)

func TestRuntimeLauncherRequestBindsExpectedPodDigest(t *testing.T) {
	pod := json.RawMessage(`{"metadata":{"name":"runtime","namespace":"vela-cpu"},"spec":{"containers":[{"name":"model-runtime","image":"example.invalid/runtime@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]}}`)
	digest := sha256.Sum256(pod)
	wire, err := json.Marshal(runtimeLauncherRequest{Version: runtimeLauncherProtocolVersion, Manifest: json.RawMessage(`{}`), ExpectedPod: pod, ExpectedPodDigest: digest, StartupSocket: "/run/vela/startup.sock"})
	if err != nil {
		t.Fatal(err)
	}
	var decoded runtimeLauncherRequest
	if err := strictLauncherJSON(wire, &decoded); err != nil {
		t.Fatalf("decode request: %v", err)
	}
	if string(decoded.ExpectedPod) != string(pod) || decoded.ExpectedPodDigest != sha256.Sum256(decoded.ExpectedPod) {
		t.Fatal("expected Pod was not digest-bound in launcher request")
	}
	decoded.ExpectedPod = json.RawMessage(`{"metadata":{"name":"other"}}`)
	if decoded.ExpectedPodDigest == sha256.Sum256(decoded.ExpectedPod) {
		t.Fatal("mutated expected Pod unexpectedly retained the original digest")
	}
}

func TestStrictLauncherJSONRejectsUnknownAndTrailingFields(t *testing.T) {
	var reply runtimeLauncherPolicyReply
	if err := strictLauncherJSON([]byte(`{"version":1,"operation_id":"00000000-0000-0000-0000-000000000001","request_digest":[1],"evidence_digest":[2],"issued_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-01T00:01:00Z","extra":true}`), &reply); err == nil {
		t.Fatal("unknown launcher field was accepted")
	}
	if err := strictLauncherJSON([]byte(`{"version":1} {"version":1}`), &struct {
		Version int `json:"version"`
	}{}); err == nil {
		t.Fatal("trailing launcher frame was accepted")
	}
}

func TestRuntimeLauncherControlReceivesSCMRights(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Skipf("SOCK_SEQPACKET unavailable: %v", err)
	}
	parent := os.NewFile(uintptr(fds[0]), "launcher-control-test")
	child := os.NewFile(uintptr(fds[1]), "launcher-control-test-peer")
	defer parent.Close()
	defer child.Close()
	pidfd, err := unix.PidfdOpen(unix.Getpid(), 0)
	if err != nil {
		t.Skipf("pidfd unavailable: %v", err)
	}
	defer unix.Close(pidfd)
	go func() {
		_ = unix.Sendmsg(int(child.Fd()), []byte(`{"version":1}`), unix.UnixRights(pidfd), nil, 0)
	}()
	control := &runtimeLauncherControl{file: parent}
	packet, rights, err := control.recv(context.Background(), 1)
	if err != nil {
		t.Fatalf("receive SCM_RIGHTS frame: %v", err)
	}
	defer closeRights(rights)
	if string(packet) != `{"version":1}` || len(rights) != 1 {
		t.Fatalf("unexpected frame packet=%q rights=%d", packet, len(rights))
	}
}

func TestTrustedLauncherBinaryRequiresRootAndExecutable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "launcher")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := trustedLauncherBinary(path); err == nil {
		t.Fatal("user-owned launcher was accepted")
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := trustedLauncherBinary(path); err == nil {
		t.Fatal("non-executable launcher was accepted")
	}
}
