//go:build linux

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/fleetcontract"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/runtimelaunch"
	"golang.org/x/sys/unix"
	corev1 "k8s.io/api/core/v1"
)

// This opt-in test consumes an operator-created disposable gated Pod. It
// proves the real Kubernetes/DRA/process boundary, not a backend Permit, Job,
// model output or billing. The caller retains ownership of Pod/claim cleanup.
func TestKubernetesLaunchOriginalPidfds(t *testing.T) {
	path := os.Getenv("VELA_KUBERNETES_LAUNCH_TEST_POD")
	if path == "" {
		t.Skip("requires a disposable live Kubernetes/DRA Pod and root host access")
	}
	if os.Geteuid() != 0 {
		t.Fatal("host root is required")
	}
	wire, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var pod corev1.Pod
	if err := json.Unmarshal(wire, &pod); err != nil {
		t.Fatal(err)
	}
	if pod.Labels["vela.ai/test-purpose"] != "kubernetes-pidfd-handoff" || pod.Labels[fleetcontract.ProtectedLabel] != "" {
		t.Fatal("integration test requires an explicitly disposable, unprotected Pod")
	}
	root := runtimelaunch.MemberRoot(pod.Labels[fleetcontract.WorkerMemberIDLabel])
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	w, err := launchKubernetesWorkload(ctx, filepath.Join(root, "startup.sock"), &pod)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	for _, fd := range []*os.File{w.runtimeFD, w.workerFD, w.observerFD} {
		if fd == nil || runtimechannel.SameLiveProcess(int(fd.Fd()), int(fd.Fd())) != nil {
			t.Fatal("handoff did not retain live original processes")
		}
	}
	connection, err := net.FileConn(w.observerEnd)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	creator, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		t.Fatal(err)
	}
	creatorFile := os.NewFile(uintptr(creator), "test-creator")
	defer creatorFile.Close()
	custody, err := nodeagent.ReceiveRuntimeObserverCustodyFromCreator(ctx, connection.(*net.UnixConn), w.observerFD, creatorFile)
	if err != nil {
		t.Fatal(err)
	}
	defer custody.Close()
	if err := custody.Start(ctx); err != nil {
		t.Fatal(err)
	}
	progressPath := filepath.Join(root, "progress", "runtime.json")
	readProgress := func() (int, int) {
		t.Helper()
		wire, err := os.ReadFile(progressPath)
		if err != nil {
			t.Fatal("diagnostic target did not execute", err)
		}
		var progress struct {
			Ticks   int `json:"ticks"`
			Signals int `json:"signals"`
		}
		if err := json.Unmarshal(wire, &progress); err != nil {
			t.Fatal(err)
		}
		return progress.Ticks, progress.Signals
	}
	time.Sleep(time.Second)
	pid, err := pidFromFD(int(w.runtimeFD.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Readlink(fmt.Sprintf("/proc/%d/exe", pid))
	if err != nil || executable != "/usr/local/bin/vela-model-runtime" {
		t.Fatal("target did not exec the diagnostic Runtime", executable, err)
	}
	before, _ := readProgress()
	for _, signal := range []unix.Signal{unix.SIGUSR1, unix.SIGURG, unix.SIGCONT} {
		if err := unix.PidfdSendSignal(int(w.runtimeFD.Fd()), signal, nil, 0); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(time.Second)
	after, signals := readProgress()
	if after <= before || signals == 0 {
		t.Fatal("signal delivery stopped target progress", before, after, signals)
	}
	// Exercise the production observer beyond its handshake timeout, including
	// fresh nonces, then prove that loss of heartbeats kills the original target.
	until := time.Now().Add(35 * time.Second)
	for time.Now().Before(until) {
		if err := custody.Check(ctx); err != nil {
			t.Fatal("healthy observer heartbeat failed", err)
		}
		time.Sleep(time.Second)
		progress, _ := readProgress()
		if progress <= after {
			t.Fatal("observer replied while target made no progress", after, progress)
		}
		after = progress
	}
	dead := []unix.PollFd{{Fd: int32(w.runtimeFD.Fd()), Events: unix.POLLIN}}
	if _, err := unix.Poll(dead, 35000); err != nil || dead[0].Revents&unix.POLLIN == 0 {
		t.Fatal("observer did not terminate the original Runtime after heartbeat loss", err)
	}
	if err := w.stopKubernetesWorkload(); err != nil {
		t.Fatal(err)
	}
	for _, fd := range []*os.File{w.runtimeFD, w.workerFD} {
		poll := []unix.PollFd{{Fd: int32(fd.Fd()), Events: unix.POLLIN}}
		if _, err := unix.Poll(poll, 10000); err != nil || poll[0].Revents&unix.POLLIN == 0 {
			t.Fatal("original container process did not exit after cleanup", err)
		}
	}
	t.Logf("live Kubernetes/DRA handoff and original-process exit verified: runtime=%+v worker=%+v; business_e2e=false", w.target, w.worker)
}
