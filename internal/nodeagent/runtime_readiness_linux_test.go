package nodeagent

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/google/uuid"
	"golang.org/x/sys/unix"
)

func TestRuntimeReadinessSocketRequiresOriginalProcess(t *testing.T) {
	if os.Getenv("VELA_READINESS_PEER_HELPER") == "1" {
		listener, err := net.Listen("unix", os.Getenv("VELA_READINESS_SOCKET"))
		if err != nil {
			panic(err)
		}
		if err := os.Chmod(os.Getenv("VELA_READINESS_SOCKET"), 0600); err != nil {
			panic(err)
		}
		defer listener.Close()
		fmt.Println("listening")
		for {
			conn, err := listener.Accept()
			if err != nil {
				return
			}
			_ = conn.Close()
		}
	}
	directory, err := os.MkdirTemp("/tmp", "vela-ready-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(directory)
	path := filepath.Join(directory, "runtime.sock")
	start := func() (*exec.Cmd, int) {
		cmd := exec.Command(os.Args[0], "-test.run=^TestRuntimeReadinessSocketRequiresOriginalProcess$")
		cmd.Env = append(os.Environ(), "VELA_READINESS_PEER_HELPER=1", "VELA_READINESS_SOCKET="+path)
		pidfd := -1
		cmd.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd}
		output, err := cmd.StdoutPipe()
		if err != nil {
			t.Fatal(err)
		}
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
			if pidfd >= 0 {
				_ = unix.Close(pidfd)
			}
		})
		scanner := bufio.NewScanner(output)
		if !scanner.Scan() || scanner.Text() != "listening" {
			t.Fatal("helper did not publish socket")
		}
		return cmd, pidfd
	}
	original, pidfd := start()
	boot, err := readBootID("/proc/sys/kernel/random/boot_id")
	if err != nil {
		t.Fatal(err)
	}
	duplicate, err := unix.FcntlInt(uintptr(pidfd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	owner := &RuntimeNamespaceOwner{pidfd: os.NewFile(uintptr(duplicate), "original"), owner: RuntimeContainerCallerObservation{Process: RuntimeCallerObservation{HostPID: int32(original.Process.Pid), UID: uint32(os.Getuid()), GID: uint32(os.Getgid()), BootID: uuid.MustParse(boot)}}}
	defer owner.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	conn, err := owner.connectReadiness(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	_ = conn.Close()
	// Replace only the socket path. The original process remains alive, so a
	// liveness-only check would falsely accept this second process's evidence.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	_, _ = start()
	if conn, err := owner.connectReadiness(ctx, path); err == nil {
		_ = conn.Close()
		t.Fatal("accepted a replacement socket creator")
	}
	if err := original.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = original.Wait()
	if err := owner.checkLive(ctx); err == nil {
		t.Fatal("dead original accepted")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	if err := owner.checkLive(ctx); err == nil {
		t.Fatal("closed owner accepted")
	}
}
