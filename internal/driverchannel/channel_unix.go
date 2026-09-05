//go:build darwin || linux

package driverchannel

import (
	"errors"
	"net"
	"os"
	"strconv"
	"syscall"

	"golang.org/x/sys/unix"
)

// Pair supplies a private datagram channel and an inheritable endpoint.
// Datagram boundaries keep a timed-out query independent of subsequent calls.
func Pair() (*net.UnixConn, *os.File, error) {
	syscall.ForkLock.RLock()
	fds, err := unix.Socketpair(unix.AF_UNIX, unix.SOCK_DGRAM, 0)
	if err == nil {
		unix.CloseOnExec(fds[0])
		unix.CloseOnExec(fds[1])
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, nil, err
	}
	parent := os.NewFile(uintptr(fds[0]), "driver-channel-parent")
	child := os.NewFile(uintptr(fds[1]), "driver-channel-child")
	conn, err := FromFile(parent)
	_ = parent.Close()
	if err != nil {
		_ = child.Close()
		return nil, nil, err
	}
	return conn, child, nil
}

// FromFile duplicates a local datagram socket; the caller retains file ownership.
func FromFile(file *os.File) (*net.UnixConn, error) {
	kind, err := unix.GetsockoptInt(int(file.Fd()), unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil || kind != unix.SOCK_DGRAM {
		return nil, errors.New("driver channel requires a Unix datagram socket")
	}
	conn, err := net.FileConn(file)
	if err != nil {
		return nil, err
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("driver channel socket is not local")
	}
	return unixConn, nil
}

// OpenInherited consumes the dedicated fd only when the parent declared it.
func OpenInherited(value string, descriptor int) (*net.UnixConn, error) {
	if value == "" {
		return nil, nil
	}
	if descriptor < 3 || value != strconv.Itoa(descriptor) {
		return nil, errors.New("driver channel descriptor is invalid")
	}
	unix.CloseOnExec(descriptor)
	file := os.NewFile(uintptr(descriptor), "driver-channel")
	defer func() { _ = file.Close() }()
	return FromFile(file)
}
