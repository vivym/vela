package runtimechannel

import (
	"errors"

	"golang.org/x/sys/unix"
)

// PollLivePIDFD checks that an independently authenticated pidfd has no exit or
// error event. It does not establish descriptor identity or process authority.
// Signals can interrupt even a zero-timeout poll; EINTR is not exit evidence.
func PollLivePIDFD(fd int) error {
	return pollLivePIDFD(fd, unix.Poll)
}

func pollLivePIDFD(fd int, poll func([]unix.PollFd, int) (int, error)) error {
	if fd < 0 {
		return ErrIdentity
	}
	for {
		count, err := poll([]unix.PollFd{{Fd: int32(fd), Events: unix.POLLIN}}, 0)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if err != nil || count != 0 {
			return errors.Join(ErrIdentity, err)
		}
		return nil
	}
}
