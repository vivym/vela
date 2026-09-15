//go:build linux

package runtimechannel

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"strings"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

// TestPIDFDBrokerExternalSocket exercises a separately deployed root-owned
// broker. It is opt-in so ordinary unit tests never depend on host services.
// The child receives retained pidfds through SCM_RIGHTS and never serializes a
// numeric PID. Set VELA_PIDFD_BROKER_EXTERNAL_SOCKET to run this against the
// host service.
func TestPIDFDBrokerExternalSocket(t *testing.T) {
	socket := os.Getenv("VELA_PIDFD_BROKER_EXTERNAL_SOCKET")
	if socket == "" {
		t.Skip("VELA_PIDFD_BROKER_EXTERNAL_SOCKET is not configured")
	}
	if os.Geteuid() != 0 {
		t.Skip("external broker test requires root to pass retained pidfds")
	}
	for _, mode := range []string{"same", "different", "nested-same", "wrong-gid"} {
		t.Run(mode, func(t *testing.T) {
			child := exec.Command(os.Args[0], "-test.run=^TestPIDFDBrokerExternalClient$")
			child.Env = append(os.Environ(),
				"VELA_PIDFD_BROKER_EXTERNAL_CLIENT=1",
				"VELA_PIDFD_BROKER_EXTERNAL_SOCKET="+socket,
				"VELA_PIDFD_BROKER_EXTERNAL_MODE="+mode,
			)
			credential := uint32(65532)
			if mode == "wrong-gid" {
				credential = 65533
			}
			child.SysProcAttr = &syscall.SysProcAttr{Credential: &syscall.Credential{Uid: credential, Gid: credential}}
			if mode == "nested-same" {
				hostPIDFD, err := unix.PidfdOpen(os.Getpid(), 0)
				if err != nil {
					t.Skipf("pidfd_open unavailable: %v", err)
				}
				t.Cleanup(func() { _ = unix.Close(hostPIDFD) })
				child.ExtraFiles = []*os.File{os.NewFile(uintptr(hostPIDFD), "nested-host-pidfd")}
				child.SysProcAttr.Cloneflags = unix.CLONE_NEWPID
			}
			output, err := child.CombinedOutput()
			if err != nil {
				if mode == "nested-same" && strings.Contains(string(output), "operation not permitted") {
					t.Skipf("nested PID namespace is blocked by host policy: %s", output)
				}
				t.Fatalf("external broker client: %s: %v", output, err)
			}
		})
	}
}

func TestPIDFDBrokerExternalClient(t *testing.T) {
	if os.Getenv("VELA_PIDFD_BROKER_EXTERNAL_CLIENT") != "1" {
		return
	}
	mode := os.Getenv("VELA_PIDFD_BROKER_EXTERNAL_MODE")
	self, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer unix.Close(self)
	first, second := self, self
	if mode == "nested-same" {
		first = 3
		second = 3
		if _, err := unix.FcntlInt(uintptr(first), unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
			t.Fatal(err)
		}
		if err := ValidatePIDFD(first); err != nil {
			t.Fatal(err)
		}
		if os.Getenv("VELA_PIDFD_BROKER_DEBUG") == "1" {
			if identity, classifyErr := ClassifyPIDFD(first); classifyErr != nil {
				t.Logf("nested pidfd classify error: %v", classifyErr)
			} else {
				t.Logf("nested pidfd class: %d", identity)
			}
			if directErr := SameLiveProcess(first, second); directErr != nil {
				t.Logf("nested direct comparison: %v", directErr)
			}
		}
	}
	if mode == "different" {
		second, err = unix.PidfdOpen(os.Getppid(), 0)
		if err != nil {
			t.Fatal(err)
		}
		defer unix.Close(second)
	}
	err = ComparePIDFDsWithBroker(context.Background(), os.Getenv("VELA_PIDFD_BROKER_EXTERNAL_SOCKET"), first, second)
	if mode == "wrong-gid" {
		if !errors.Is(err, ErrPIDFDBrokerUnavailable) {
			t.Fatalf("wrong GID result = %v, want ErrPIDFDBrokerUnavailable", err)
		}
		return
	}
	if mode == "same" || mode == "nested-same" {
		if err != nil {
			t.Fatalf("same retained pidfd rejected: %v", err)
		}
		return
	}
	if !errors.Is(err, ErrIdentity) {
		t.Fatalf("different retained pidfd result = %v, want ErrIdentity", err)
	}
}
