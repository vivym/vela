package nodeagent

import (
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

const runtimeCallerExhaustionHelper = "VELA_RUNTIME_CALLER_EXHAUSTION_HELPER"

func TestRuntimeCallerPIDFDExhaustion(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires the non-root Runtime caller fixture")
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	command := exec.CommandContext(t.Context(), binary, "-test.run=^TestRuntimeCallerPIDFDExhaustionHelper$", "-test.v", "-test.timeout=20s")
	command.Env = append(os.Environ(), runtimeCallerExhaustionHelper+"=1")
	output, err := command.CombinedOutput()
	if err != nil || !strings.Contains(string(output), "--- PASS: TestRuntimeCallerPIDFDExhaustionHelper ") || strings.Contains(string(output), "--- SKIP:") {
		t.Fatalf("isolated descriptor exhaustion failed: %v\n%s", err, output)
	}
	t.Log(string(output))
}

func TestRuntimeCallerPIDFDExhaustionHelper(t *testing.T) {
	if os.Getenv(runtimeCallerExhaustionHelper) != "1" {
		t.Skip("requires the isolated descriptor-exhaustion subprocess")
	}
	connection, _, wait := runtimeCallerConnection(t, "normal", "unixpacket")
	var original unix.Rlimit
	if err := unix.Getrlimit(unix.RLIMIT_NOFILE, &original); err != nil {
		t.Fatal(err)
	}
	var stdin unix.Stat_t
	if err := unix.Fstat(0, &stdin); err != nil {
		t.Fatal(err)
	}
	var held []int
	restore := func() {
		for _, fd := range held {
			_ = unix.Close(fd)
		}
		held = nil
		if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &original); err != nil {
			t.Error(err)
		}
	}
	defer restore()
	if err := unix.Setrlimit(unix.RLIMIT_NOFILE, &unix.Rlimit{Cur: 128, Max: original.Max}); err != nil {
		t.Fatal(err)
	}
	for {
		fd, err := unix.Dup(0)
		if errors.Is(err, unix.EMFILE) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, fd)
	}
	raw, err := connection.SyscallConn()
	if err != nil {
		t.Fatal(err)
	}
	var acquisitionErr error
	if err := raw.Control(func(socket uintptr) {
		fd, err := unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		acquisitionErr = err
		if err == nil {
			_ = unix.Close(fd)
		}
	}); err != nil || !errors.Is(acquisitionErr, unix.EMFILE) {
		t.Fatalf("fixture did not exhaust kernel pidfd allocation: %v %v", err, acquisitionErr)
	}
	caller, err := ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if caller != nil || !errors.Is(err, unix.EMFILE) {
		t.Fatalf("exhausted pidfd did not fail before authentication: %v", err)
	}
	var current unix.Stat_t
	if err := unix.Fstat(0, &current); err != nil || current.Dev != stdin.Dev || current.Ino != stdin.Ino {
		t.Fatalf("failed pidfd acquisition closed or replaced stdin: %v", err)
	}
	restore()
	caller, err = ReceiveRuntimeCaller(t.Context(), connection, RuntimeCallerCredentials{UID: 65532, GID: 65532})
	if err != nil || caller == nil || string(caller.Payload()) != "test-startup-request" {
		t.Fatalf("released descriptor capacity did not permit authenticated retry: %v", err)
	}
	t.Cleanup(func() { _ = caller.Close() })
	if _, err := connection.Write([]byte("exit")); err != nil {
		t.Fatal(err)
	}
	wait()
}
