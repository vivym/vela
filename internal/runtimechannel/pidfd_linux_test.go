package runtimechannel

import (
	"errors"
	"io"
	"os"
	"os/exec"
	"testing"

	"golang.org/x/sys/unix"
)

func TestPollLivePIDFDRetriesInterruptionsWithoutHidingExit(t *testing.T) {
	for _, scenario := range []struct {
		name  string
		count int
		err   error
		want  error
	}{
		{name: "still-live"},
		{name: "exited-during-interruption", count: 1, want: ErrIdentity},
		{name: "descriptor-error", count: -1, err: unix.EBADF, want: unix.EBADF},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			calls := 0
			err := pollLivePIDFD(42, func(descriptors []unix.PollFd, timeout int) (int, error) {
				calls++
				if len(descriptors) != 1 || descriptors[0] != (unix.PollFd{Fd: 42, Events: unix.POLLIN}) || timeout != 0 {
					t.Fatal("retry changed the original nonblocking descriptor check")
				}
				if calls <= 2 {
					// The kernel need not preserve output fields after EINTR.
					descriptors[0].Revents = unix.POLLNVAL
					return -1, unix.EINTR
				}
				return scenario.count, scenario.err
			})
			if calls != 3 || !errors.Is(err, scenario.want) || scenario.want != nil && !errors.Is(err, ErrIdentity) {
				t.Fatalf("interrupted liveness check: calls=%d err=%v", calls, err)
			}
		})
	}
	if err := pollLivePIDFD(-1, func([]unix.PollFd, int) (int, error) {
		t.Fatal("invalid descriptor reached poll")
		return 0, nil
	}); !errors.Is(err, ErrIdentity) {
		t.Fatal("negative descriptor was accepted")
	}
}

func TestPollLivePIDFDActualProcessExit(t *testing.T) {
	command := exec.CommandContext(t.Context(), os.Args[0], "-test.run=^TestPollLivePIDFDProcessHelper$")
	command.Env = append(os.Environ(), "VELA_PIDFD_LIFETIME_HELPER=1")
	input, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	output, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = input.Close(); _ = command.Process.Kill(); _ = command.Wait() })
	if _, err := io.ReadFull(output, make([]byte, 1)); err != nil {
		t.Fatal(err)
	}
	fd, err := unix.PidfdOpen(command.Process.Pid, 0)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = unix.Close(fd) })
	if err := PollLivePIDFD(fd); err != nil {
		t.Fatalf("live original process rejected: %v", err)
	}
	if err := command.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = command.Wait()
	if err := PollLivePIDFD(fd); !errors.Is(err, ErrIdentity) {
		t.Fatal("exited original process accepted", err)
	}
}

func TestPollLivePIDFDProcessHelper(t *testing.T) {
	if os.Getenv("VELA_PIDFD_LIFETIME_HELPER") != "1" {
		return
	}
	if _, err := os.Stdout.Write([]byte{'r'}); err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
}
