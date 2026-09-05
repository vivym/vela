//go:build darwin || linux

package modelruntime

import (
	"errors"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

func configureDriverProcess(command *exec.Cmd) error {
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	return nil
}

func killDriverProcessGroup(process *os.Process) error {
	// This covers the resident driver's group only. Descendants may escape it;
	// successful signal delivery is not proof of execution-level quiescence.
	err := unix.Kill(-process.Pid, unix.SIGKILL)
	if errors.Is(err, unix.ESRCH) {
		return nil
	}
	if errors.Is(err, unix.EPERM) && driverGroupAlreadyExited(process.Pid) {
		return nil
	}
	if err != nil {
		return errors.Join(err, process.Kill())
	}
	return nil
}
