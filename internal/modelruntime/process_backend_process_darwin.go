package modelruntime

import (
	"errors"
	"os"

	"golang.org/x/sys/unix"
)

func waitDriverProcessExit(process *os.Process) error {
	queue, err := unix.Kqueue()
	if err != nil {
		return err
	}
	defer func() { _ = unix.Close(queue) }()
	change := unix.Kevent_t{
		Ident: uint64(process.Pid), Filter: unix.EVFILT_PROC,
		Flags: unix.EV_ADD | unix.EV_ONESHOT, Fflags: unix.NOTE_EXIT,
	}
	if _, err := unix.Kevent(queue, []unix.Kevent_t{change}, nil, nil); err != nil {
		// The direct child can exit before registration. It is still unreaped.
		if errors.Is(err, unix.ESRCH) {
			return nil
		}
		return err
	}
	var events [1]unix.Kevent_t
	for {
		_, err := unix.Kevent(queue, nil, events[:], nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		return err
	}
}

func driverGroupAlreadyExited(pgid int) bool {
	// Darwin returns EPERM when a group contains only zombies. Keep the error
	// unless the kernel process snapshot confirms that no live member remains.
	processes, err := unix.SysctlKinfoProcSlice("kern.proc.pgrp", pgid)
	if err != nil {
		return false
	}
	const zombie = 5 // SZOMB from Darwin sys/proc.h.
	for _, process := range processes {
		if process.Proc.P_stat != zombie {
			return false
		}
	}
	return true
}
