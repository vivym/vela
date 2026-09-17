//go:build linux

// vela-runtime-entrypoint transfers its own original pidfd before exec. It
// accepts no command, environment override, numeric PID or historical handle.
package main

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"runtime"
	"syscall"
	"time"

	"github.com/vivym/vela/internal/runtimelaunch"
	"golang.org/x/sys/unix"
)

func init() {
	// Go may move a goroutine to a non-leader thread before exec. Linux then
	// retires the traced leader during de-threading and breaks Node custody.
	// init runs on the startup thread; retain it through the one approved exec.
	runtime.LockOSThread()
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if unix.Gettid() != os.Getpid() {
		return errors.New("entrypoint exec must retain the original process leader")
	}
	return runWithExec(args, unix.Exec)
}

func runWithExec(args []string, execProgram func(string, []string, []string) error) error {
	if len(args) != 1 || args[0] != "runtime" && args[0] != "worker" || os.Geteuid() == 0 || os.Getegid() == 0 {
		return errors.New("entrypoint requires non-root identity and exactly runtime or worker")
	}
	// Register before handing off the original pidfd, so an early observer
	// acknowledgement cannot be lost between sendmsg and the wait below.
	release := make(chan os.Signal, 1)
	signal.Notify(release, syscall.SIGCONT)
	defer signal.Stop(release)
	fd, err := unix.PidfdOpen(os.Getpid(), 0)
	if err != nil {
		return err
	}
	defer func(fd int) { _ = unix.Close(fd) }(fd)
	path := runtimelaunch.OfferRoot + "/" + args[0] + "-offer/pidfd.sock"
	connection, err := net.DialTimeout("unixpacket", path, 15*time.Second)
	if err != nil {
		return err
	}
	defer func(cleanup func() error) { _ = cleanup() }(connection.Close)
	if err := connection.SetWriteDeadline(time.Now().Add(15 * time.Second)); err != nil {
		return err
	}
	const frame = "vela-runtime-pidfd-v1"
	n, _, err := connection.(*net.UnixConn).WriteMsgUnix([]byte(frame), unix.UnixRights(fd), nil)
	if err != nil || n != len(frame) {
		return errors.Join(errors.New("send original process pidfd"), err)
	}
	_ = connection.Close()
	timer := time.NewTimer(2 * time.Minute)
	defer timer.Stop()
	select {
	case <-release:
	case <-timer.C:
		return errors.New("timed out waiting for Node custody")
	}
	argv := runtimelaunch.RuntimeArguments()
	if args[0] == "worker" {
		argv = []string{runtimelaunch.Worker}
	}
	// pidfd is CLOEXEC; the exec'ed program inherits only the approved vectors.
	return execProgram(argv[0], argv, os.Environ())
}
