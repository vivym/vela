package nodeagent

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRuntimeNamespaceOwnerFromPIDFDRejectsWrongCredentials(t *testing.T) {
	connection, _, _ := runtimeCallerConnection(t, "hold-after-disconnect", "unixpacket", true)
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	process, err := caller.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cri, _, observer := runtimeCallerObserverFixture(t, process)
	wrong := RuntimeCallerCredentials{UID: process.UID + 1, GID: process.GID}
	fd, err := unix.FcntlInt(caller.pidfd.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	pidfd := os.NewFile(uintptr(fd), "wrong-credentials-pidfd")
	defer func(cleanup func() error) { _ = cleanup() }(pidfd.Close)
	owner, err := observer.RetainNamespaceOwnerFromPIDFD(t.Context(), cri.target, pidfd, wrong)
	if owner != nil || !errors.Is(err, ErrRuntimeNamespaceOwnerLost) {
		t.Fatalf("pidfd owner accepted credentials that differ from /proc identity: owner=%v err=%v", owner != nil, err)
	}
}

func TestRuntimeNamespaceOwnerIndependentLifetime(t *testing.T) {
	connection, process, _ := runtimeCallerConnection(t, "hold-after-disconnect", "unixpacket", true)
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	original, err := caller.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cri, tasks, observer := runtimeCallerObserverFixture(t, original)
	before := runtimeCallerDescriptorCount(t)
	owner, err := observer.RetainNamespaceOwner(t.Context(), cri.target, caller)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	if after := runtimeCallerDescriptorCount(t); after != before+1 {
		t.Fatalf("retained owner descriptors: before=%d after=%d", before, after)
	}
	assertNamespaceOwnerLive(t, owner)
	if err := process.Signal(unix.SIGSTOP); err != nil {
		t.Fatal(err)
	}
	stoppedDeadline := time.Now().Add(5 * time.Second)
	for {
		status, err := readRuntimeProc(caller.process, "status")
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(status, "State:\tT (stopped)\n") {
			break
		}
		if time.Now().After(stoppedDeadline) {
			t.Fatal("fixture did not reach the actual kernel stopped state")
		}
		time.Sleep(10 * time.Millisecond)
	}
	assertNamespaceOwnerLive(t, owner)
	// Metadata loss does not indicate process exit and is not consulted by the
	// retained owner. The original authenticated process is still running.
	tasks.mu.Lock()
	tasks.hook = func(int) error { return status.Error(codes.NotFound, "fixture metadata removed") }
	tasks.mu.Unlock()
	if observed, err := observer.ObserveCaller(t.Context(), cri.target, caller); err == nil || observed != (RuntimeContainerCallerObservation{}) {
		t.Fatal("missing native task unexpectedly produced a live correlation")
	}
	assertNamespaceOwnerLive(t, owner)
	if err := errors.Join(caller.Close(), connection.Close(), observer.Close()); err != nil {
		t.Fatal(err)
	}
	assertNamespaceOwnerLive(t, owner)
	if err := process.Kill(); err != nil {
		t.Fatal(err)
	}
	exit := waitNamespaceOwnerExit(t, owner)
	if exit.Owner.Process.HostPID != original.HostPID || exit.Owner.Process.BootID != original.BootID ||
		exit.Owner.Container.Target != cri.target || exit.RetainedAt.IsZero() || exit.ObservedAt.Before(exit.RetainedAt) {
		t.Fatalf("wrong exact-owner exit observation: %+v", exit)
	}
	var checks sync.WaitGroup
	for range 8 {
		checks.Go(func() {
			observed, err := owner.ObserveExit(t.Context())
			if err != nil || observed != exit {
				t.Errorf("repeat exit observation changed: %+v %v", observed, err)
			}
		})
	}
	checks.Wait()
	before = runtimeCallerDescriptorCount(t)
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if after := runtimeCallerDescriptorCount(t); after != before-1 {
		t.Fatalf("owner close did not release its descriptor: before=%d after=%d", before, after)
	}
	if observed, err := owner.ObserveExit(t.Context()); !errors.Is(err, ErrRuntimeNamespaceOwnerLost) || observed != (RuntimeNamespaceExitObservation{}) {
		t.Fatal("closed handle manufactured a retirement observation")
	}
	t.Log("exact PID-1 exit observed after original caller, request socket and CRI observer closed; metadata absence alone did not suffice")
}

func TestRuntimeNamespaceOwnerRejectsUnprovenOwner(t *testing.T) {
	for _, scenario := range []string{"non-init", "closed-caller", "canceled", "missing-task", "closed-owner"} {
		t.Run(scenario, func(t *testing.T) {
			connection, _, _ := runtimeCallerConnection(t, "normal", "unixpacket", scenario != "non-init")
			caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = caller.Close() })
			process, err := caller.Inspect(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			cri, tasks, observer := runtimeCallerObserverFixture(t, process)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			switch scenario {
			case "closed-caller":
				_ = caller.Close()
			case "canceled":
				cancel()
			case "missing-task":
				tasks.mu.Lock()
				tasks.hook = func(int) error { return status.Error(codes.NotFound, "removed") }
				tasks.mu.Unlock()
			}
			before := runtimeCallerDescriptorCount(t)
			owner, err := observer.RetainNamespaceOwner(ctx, cri.target, caller)
			if scenario == "closed-owner" {
				if err != nil {
					t.Fatal(err)
				}
				if err := owner.Close(); err != nil {
					t.Fatal(err)
				}
				if observed, err := owner.ObserveExit(t.Context()); !errors.Is(err, ErrRuntimeNamespaceOwnerLost) || observed != (RuntimeNamespaceExitObservation{}) {
					t.Fatal("closing a live owner was treated as process exit")
				}
			} else if err == nil || owner != nil {
				if owner != nil {
					_ = owner.Close()
				}
				t.Fatalf("unproven owner retained: %v", err)
			}
			if after := runtimeCallerDescriptorCount(t); after != before {
				t.Fatalf("rejection leaked descriptors: before=%d after=%d", before, after)
			}
		})
	}
}

func TestRuntimeNamespaceOwnerKeepsGenerationsDistinct(t *testing.T) {
	firstConnection, firstProcess, _ := runtimeCallerConnection(t, "hold-after-disconnect", "unixpacket", true)
	firstCaller, err := ReceiveRuntimeCaller(t.Context(), firstConnection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = firstCaller.Close() })
	firstObserved, err := firstCaller.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	cri, tasks, observer := runtimeCallerObserverFixture(t, firstObserved)
	firstOwner, err := observer.RetainNamespaceOwner(t.Context(), cri.target, firstCaller)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = firstOwner.Close() })
	secondConnection, _, _ := runtimeCallerConnection(t, "normal", "unixpacket", true)
	secondCaller, err := ReceiveRuntimeCaller(t.Context(), secondConnection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondCaller.Close() })
	secondObserved, err := secondCaller.Inspect(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	// A changed metadata fixture names a new process under the same target.
	// Neither that change nor its fresh namespace retires the original process.
	tasks.mu.Lock()
	tasks.process.Pid = uint32(secondObserved.HostPID)
	tasks.mu.Unlock()
	secondOwner, err := observer.RetainNamespaceOwner(t.Context(), cri.target, secondCaller)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = secondOwner.Close() })
	assertNamespaceOwnerLive(t, firstOwner)
	assertNamespaceOwnerLive(t, secondOwner)
	if err := errors.Join(firstCaller.Close(), firstConnection.Close()); err != nil {
		t.Fatal(err)
	}
	assertNamespaceOwnerLive(t, firstOwner)
	if err := firstProcess.Kill(); err != nil {
		t.Fatal(err)
	}
	exit := waitNamespaceOwnerExit(t, firstOwner)
	if exit.Owner.Process.HostPID != firstObserved.HostPID || exit.Owner.Process.HostPID == secondObserved.HostPID ||
		exit.Owner.Process.PIDNamespace == secondObserved.PIDNamespace {
		t.Fatal("original exit was reassigned to the replacement generation")
	}
	assertNamespaceOwnerLive(t, secondOwner)
	t.Log("same target metadata named a second live PID-1 process; only the original retained pidfd reported its actual exit")
}

func assertNamespaceOwnerLive(t *testing.T, owner *RuntimeNamespaceOwner) {
	t.Helper()
	if observed, err := owner.ObserveExit(t.Context()); !errors.Is(err, ErrRuntimeNamespaceOwnerLive) || observed != (RuntimeNamespaceExitObservation{}) {
		t.Fatalf("live owner produced an exit observation: %+v %v", observed, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if observed, err := owner.ObserveExit(ctx); !errors.Is(err, context.Canceled) || observed != (RuntimeNamespaceExitObservation{}) {
		t.Fatalf("canceled observation lost its cause: %+v %v", observed, err)
	}
}

func waitNamespaceOwnerExit(t *testing.T, owner *RuntimeNamespaceOwner) RuntimeNamespaceExitObservation {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		observed, err := owner.ObserveExit(t.Context())
		if err == nil {
			return observed
		}
		if !errors.Is(err, ErrRuntimeNamespaceOwnerLive) || time.Now().After(deadline) {
			t.Fatalf("exact owner exit was not observed: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
}
