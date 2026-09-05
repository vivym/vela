package modelruntime

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func waitDriverProcessExit(process *os.Process) error {
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, process.Pid, &info, unix.WEXITED|unix.WNOWAIT, nil)
		if !errors.Is(err, unix.EINTR) {
			return err
		}
	}
}

func driverGroupAlreadyExited(int) bool { return false }
