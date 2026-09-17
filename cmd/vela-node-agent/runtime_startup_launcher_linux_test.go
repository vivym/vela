//go:build linux

package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
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

func TestRuntimeStartupLaunchTargetsBindLivePodUID(t *testing.T) {
	// A signed launch plan carries a Pod template and therefore normally has
	// no Kubernetes metadata.uid. The launcher supplies the UID of the live Pod;
	// CRI/Kubernetes observation binds that UID before reservation.
	// Kubernetes and CRI count the first container launch as attempt zero.
	uid := uuid.New()
	target := nodeagent.RuntimeContainerTarget{
		ContainerID: strings.Repeat("a", 64), SandboxID: strings.Repeat("b", 64),
		PodUID: uid, PodNamespace: "vela-system", PodName: "vela-worker",
		ContainerName: "model-runtime", ContainerAttempt: 0,
	}
	worker := target
	worker.ContainerID, worker.ContainerName = strings.Repeat("c", 64), "stage-worker-agent"
	launch := runtimeStartupLaunch{Target: target, WorkerTarget: worker}
	if err := validateRuntimeStartupLaunchTargets(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: target.PodNamespace, Name: target.PodName}}, launch); err != nil {
		t.Fatalf("template without UID rejected live launcher target: %v", err)
	}
	launch.WorkerTarget.PodUID = uuid.New()
	if err := validateRuntimeStartupLaunchTargets(&corev1.Pod{ObjectMeta: metav1.ObjectMeta{Namespace: target.PodNamespace, Name: target.PodName}}, launch); err == nil {
		t.Fatal("runtime and worker targets with different live Pod UIDs were accepted")
	}
}

func TestStrictLauncherJSONRejectsUnknownAndTrailingFields(t *testing.T) {
	var reply struct {
		Version int `json:"version"`
	}
	if err := strictLauncherJSON([]byte(`{"version":1,"operation_id":"00000000-0000-0000-0000-000000000001","request_digest":[1],"evidence_digest":[2],"issued_at":"2026-01-01T00:00:00Z","expires_at":"2026-01-01T00:01:00Z","extra":true}`), &reply); err == nil {
		t.Fatal("unknown launcher field was accepted")
	}
	if err := strictLauncherJSON([]byte(`{"version":1} {"version":1}`), &struct {
		Version int `json:"version"`
	}{}); err == nil {
		t.Fatal("trailing launcher frame was accepted")
	}
	if err := strictLauncherJSON([]byte(`{"version":1,"version":2}`), &struct {
		Version int `json:"version"`
	}{}); err == nil {
		t.Fatal("duplicate launcher field was accepted")
	}
}

func TestRuntimeLauncherReplyRejectsValidationOnlyHelper(t *testing.T) {
	target := nodeagent.RuntimeContainerTarget{ContainerID: strings.Repeat("a", 64), SandboxID: strings.Repeat("b", 64),
		PodUID: uuid.New(), PodNamespace: "vela-cpu", PodName: "runtime", ContainerName: "model-runtime", ContainerAttempt: 1}
	worker := target
	worker.ContainerID, worker.ContainerName = strings.Repeat("c", 64), "stage-worker-agent"
	reply := runtimeLauncherReply{Version: runtimeLauncherProtocolVersion, Target: target, WorkerTarget: worker, FDCount: 4}
	if err := reply.validate(4); err != nil {
		t.Fatalf("valid production handoff: %v", err)
	}
	reply.ValidationOnly = true
	if err := reply.validate(3); err == nil || !strings.Contains(err.Error(), "validation-only") {
		t.Fatalf("validation-only helper was not rejected: %v", err)
	}
}

func TestRuntimeLauncherControlReceivesSCMRights(t *testing.T) {
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Skipf("SOCK_SEQPACKET unavailable: %v", err)
	}
	parent := os.NewFile(uintptr(fds[0]), "launcher-control-test")
	child := os.NewFile(uintptr(fds[1]), "launcher-control-test-peer")
	defer func(cleanup func() error) { _ = cleanup() }(parent.Close)
	defer func(cleanup func() error) { _ = cleanup() }(child.Close)
	pidfd, err := unix.PidfdOpen(unix.Getpid(), 0)
	if err != nil {
		t.Skipf("pidfd unavailable: %v", err)
	}
	defer func(fd int) { _ = unix.Close(fd) }(pidfd)
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

func TestRuntimeLauncherReceiveSurvivesThreadSignals(t *testing.T) {
	runtime.LockOSThread()
	defer runtime.UnlockOSThread()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_SEQPACKET|unix.SOCK_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	parent := os.NewFile(uintptr(fds[0]), "interrupted-launcher-control")
	defer func(cleanup func() error) { _ = cleanup() }(parent.Close)
	defer func(fd int) { _ = unix.Close(fd) }(fds[1])
	tid := unix.Gettid()
	done := make(chan error, 1)
	go func() {
		for range 20 {
			time.Sleep(5 * time.Millisecond)
			if err := unix.Tgkill(unix.Getpid(), tid, unix.SIGURG); err != nil {
				done <- err
				return
			}
		}
		done <- unix.Sendmsg(fds[1], []byte("handoff"), nil, nil, 0)
	}()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	control := &runtimeLauncherControl{file: parent}
	packet, rights, receiveErr := control.recv(ctx, 0)
	closeRights(rights)
	if sendErr := <-done; sendErr != nil || receiveErr != nil || string(packet) != "handoff" {
		t.Fatalf("signal interrupted handoff: packet=%q recv=%v sender=%v", packet, receiveErr, sendErr)
	}
}
