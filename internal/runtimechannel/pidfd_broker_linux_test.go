//go:build linux

package runtimechannel

import (
	"context"
	"errors"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"golang.org/x/sys/unix"
)

func TestPIDFDBrokerComparesRetainedHandles(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires a root broker and non-root broker client")
	}
	// The broker socket contract rejects writable ancestors. macOS/Linux test
	// runners commonly place TempDir below sticky /tmp, so use /run where the
	// ancestor chain can satisfy the root-owned non-writable requirement.
	root, err := os.MkdirTemp("/run", "vela-pidfd-broker-")
	if err != nil {
		t.Skipf("/run is unavailable for root-owned broker fixture: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o755); err != nil {
		t.Fatal(err)
	}
	socketPath := filepath.Join(root, "pidfd-broker.sock")
	// Keep the executable outside /run: some validation hosts mount /run with
	// noexec while still requiring the broker socket itself to live there.
	helperRoot, err := os.MkdirTemp("/var/tmp", "vela-pidfd-helper-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(helperRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(helperRoot) })
	helperPath := filepath.Join(helperRoot, "runtimechannel.test")
	helper, err := os.ReadFile(os.Args[0])
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(helperPath, helper, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(helperPath, 0o755); err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socketPath, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	const runtimeGID = 65532
	if err := os.Chown(socketPath, 0, runtimeGID); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketPath, 0o660); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	serverDone := make(chan error, 1)
	go func() { serverDone <- ServePIDFDBroker(ctx, listener, runtimeGID) }()
	for _, mode := range []string{"same", "different", "nested-same", "wrong-gid"} {
		t.Run(mode, func(t *testing.T) {
			child := exec.Command(helperPath, "-test.run=^TestPIDFDBrokerClientHelper$")
			child.Env = append(os.Environ(), "VELA_PIDFD_BROKER_CLIENT=1", "VELA_PIDFD_BROKER_SOCKET="+socketPath, "VELA_PIDFD_BROKER_MODE="+mode)
			credential := uint32(runtimeGID)
			if mode == "wrong-gid" {
				credential = 65533
			}
			child.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: credential, Gid: credential}}
			if mode == "nested-same" {
				hostPIDFD, err := unix.PidfdOpen(os.Getpid(), 0)
				if err != nil {
					t.Skipf("pidfd_open unavailable for nested host handle: %v", err)
				}
				t.Cleanup(func() { _ = unix.Close(hostPIDFD) })
				child.ExtraFiles = []*os.File{os.NewFile(uintptr(hostPIDFD), "nested-host-pidfd")}
				child.SysProcAttr.Cloneflags = unix.CLONE_NEWPID
			}
			output, err := child.CombinedOutput()
			if err != nil {
				if mode == "nested-same" && strings.Contains(string(output), "operation not permitted") {
					t.Skipf("nested PID namespace is blocked by container policy: %s", output)
				}
				t.Fatalf("broker client: %s: %v", output, err)
			}
		})
	}
	cancel()
	select {
	case <-serverDone:
	case <-time.After(time.Second):
		t.Fatal("pidfd broker did not stop after cancellation")
	}
	if err := os.Remove(socketPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		t.Fatal("remove stopped broker socket: ", err)
	}
	probe, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		t.Fatal("open restart probe pidfd: ", err)
	}
	if err := ComparePIDFDsWithBroker(context.Background(), socketPath, probe, probe); !errors.Is(err, ErrPIDFDBrokerUnavailable) {
		_ = unix.Close(probe)
		t.Fatalf("request during broker downtime returned %v, want ErrPIDFDBrokerUnavailable", err)
	}
	_ = unix.Close(probe)
	// A stopped broker must not leave a usable authority path behind. After the
	// socket is recreated, a fresh descriptor handoff may succeed; no request
	// state from the previous listener is reused.
	restartedListener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socketPath, Net: "unixpacket"})
	if err != nil {
		t.Fatal("restart pidfd broker listener: ", err)
	}
	t.Cleanup(func() { _ = restartedListener.Close() })
	if err := os.Chown(socketPath, 0, runtimeGID); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(socketPath, 0o660); err != nil {
		t.Fatal(err)
	}
	restartedCtx, restartCancel := context.WithCancel(context.Background())
	restartedDone := make(chan error, 1)
	go func() { restartedDone <- ServePIDFDBroker(restartedCtx, restartedListener, runtimeGID) }()
	child := exec.Command(helperPath, "-test.run=^TestPIDFDBrokerClientHelper$")
	child.Env = append(os.Environ(), "VELA_PIDFD_BROKER_CLIENT=1", "VELA_PIDFD_BROKER_SOCKET="+socketPath, "VELA_PIDFD_BROKER_MODE=same")
	child.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: runtimeGID, Gid: runtimeGID}}
	if output, err := child.CombinedOutput(); err != nil {
		restartCancel()
		<-restartedDone
		t.Fatalf("broker client after restart: %s: %v", output, err)
	}
	restartCancel()
	select {
	case <-restartedDone:
	case <-time.After(time.Second):
		t.Fatal("restarted pidfd broker did not stop after cancellation")
	}
}

func TestPIDFDBrokerClientHelper(t *testing.T) {
	if os.Getenv("VELA_PIDFD_BROKER_CLIENT") != "1" {
		return
	}
	mode := os.Getenv("VELA_PIDFD_BROKER_MODE")
	self, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		t.Skipf("pidfd_open unavailable: %v", err)
	}
	defer unix.Close(self)
	second := self
	first := self
	if mode == "nested-same" {
		first = 3
		second = 3
		if _, err := unix.FcntlInt(uintptr(first), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
			t.Fatalf("set inherited pidfd close-on-exec: %v", err)
		}
		if err := ValidatePIDFD(first); err != nil {
			t.Fatalf("inherited host pidfd invalid: %v", err)
		}
	}
	if mode == "different" {
		second, err = unix.PidfdOpen(os.Getppid(), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Close(second)
	}
	if mode == "nested-same" {
		if err := SameLiveProcess(first, second); err != nil && !errors.Is(err, ErrPIDFDIdentityUnavailable) {
			t.Fatalf("nested direct comparison = %v, want success or ErrPIDFDIdentityUnavailable", err)
		}
	}
	err = ComparePIDFDsWithBroker(context.Background(), os.Getenv("VELA_PIDFD_BROKER_SOCKET"), first, second)
	if mode == "wrong-gid" {
		if !errors.Is(err, ErrPIDFDBrokerUnavailable) {
			t.Fatalf("wrong runtime GID result = %v, want ErrPIDFDBrokerUnavailable", err)
		}
		return
	}
	if mode == "same" || mode == "nested-same" {
		if err != nil {
			t.Fatalf("same retained pidfd rejected: %v", err)
		}
	} else if !errors.Is(err, ErrIdentity) {
		t.Fatalf("different retained pidfd result: %v", err)
	}
}
