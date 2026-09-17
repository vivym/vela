package nodeagent

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"syscall"
	"testing"

	"golang.org/x/sys/unix"
)

func TestValidateRuntimeWorkerOwnerPIDFDRejectsMissingHandles(t *testing.T) {
	if err := ValidateRuntimeWorkerOwnerPIDFD(context.Background(), nil, nil); err == nil {
		t.Fatal("nil caller and pidfd unexpectedly accepted")
	}
	file, err := os.CreateTemp(t.TempDir(), "not-pidfd")
	if err != nil {
		t.Fatal(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(file.Close)
	if err := ValidateRuntimeWorkerOwnerPIDFD(context.Background(), &RuntimeCaller{}, file); err == nil {
		t.Fatal("invalid pidfd unexpectedly accepted")
	}
}

func TestValidateRuntimeWorkerOwnerPIDFDAcceptsDistinctLiveProcess(t *testing.T) {
	caller, callerFD := startHandoffPIDFDProcess(t)
	defer stopHandoffPIDFDProcess(t, caller, callerFD)
	worker, workerFD := startHandoffPIDFDProcess(t)
	defer stopHandoffPIDFDProcess(t, worker, workerFD)

	if err := ValidateRuntimeWorkerOwnerPIDFD(context.Background(), &RuntimeCaller{pidfd: callerFD}, workerFD); err != nil {
		t.Fatalf("distinct live worker was rejected: %v", err)
	}
}

func TestValidateRuntimeWorkerOwnerPIDFDRejectsSameProcess(t *testing.T) {
	process, pidfd := startHandoffPIDFDProcess(t)
	defer stopHandoffPIDFDProcess(t, process, pidfd)
	duplicateFD, err := unix.FcntlInt(pidfd.Fd(), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		t.Fatal(err)
	}
	duplicate := os.NewFile(uintptr(duplicateFD), "duplicate-worker-pidfd")
	defer func(cleanup func() error) { _ = cleanup() }(duplicate.Close)
	if err := ValidateRuntimeWorkerOwnerPIDFD(context.Background(), &RuntimeCaller{pidfd: pidfd}, duplicate); err == nil {
		t.Fatal("same process was accepted as both Runtime and Worker owner")
	}
}

func TestValidateRuntimeWorkerOwnerPIDFDRejectsExitedWorker(t *testing.T) {
	caller, callerFD := startHandoffPIDFDProcess(t)
	defer stopHandoffPIDFDProcess(t, caller, callerFD)
	worker, workerFD := startHandoffPIDFDProcess(t)
	if err := unix.PidfdSendSignal(int(workerFD.Fd()), unix.SIGTERM, nil, 0); err != nil {
		t.Fatal(err)
	}
	_, _ = worker.Wait()
	err := ValidateRuntimeWorkerOwnerPIDFD(context.Background(), &RuntimeCaller{pidfd: callerFD}, workerFD)
	if err == nil {
		t.Fatal("exited worker was accepted")
	}
	if !errors.Is(err, ErrRuntimeNamespaceOwnerLost) {
		t.Fatalf("exited worker error = %v, want ErrRuntimeNamespaceOwnerLost", err)
	}
	_ = workerFD.Close()
}

func startHandoffPIDFDProcess(t *testing.T) (*os.Process, *os.File) {
	t.Helper()
	pidfd := -1
	command := exec.Command("/bin/sleep", "30")
	command.SysProcAttr = &syscall.SysProcAttr{PidFD: &pidfd}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	if pidfd < 0 {
		_ = command.Process.Kill()
		_ = command.Wait()
		t.Fatal("process did not provide a pidfd")
	}
	return command.Process, os.NewFile(uintptr(pidfd), "handoff-pidfd")
}

func stopHandoffPIDFDProcess(t *testing.T, process *os.Process, pidfd *os.File) {
	t.Helper()
	if pidfd != nil {
		_ = unix.PidfdSendSignal(int(pidfd.Fd()), unix.SIGKILL, nil, 0)
		_ = pidfd.Close()
	}
	if process != nil {
		_, _ = process.Wait()
	}
}
