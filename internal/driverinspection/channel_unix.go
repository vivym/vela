//go:build darwin || linux

package driverinspection

import (
	"errors"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// Pair supplies a private datagram channel and the endpoint inherited as fd 3.
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
	parent := os.NewFile(uintptr(fds[0]), "driver-inspection-parent")
	child := os.NewFile(uintptr(fds[1]), "driver-inspection-child")
	conn, err := fromFile(parent)
	_ = parent.Close()
	if err != nil {
		_ = child.Close()
		return nil, nil, err
	}
	return conn, child, nil
}

func fromFile(file *os.File) (*net.UnixConn, error) {
	kind, err := unix.GetsockoptInt(int(file.Fd()), unix.SOL_SOCKET, unix.SO_TYPE)
	if err != nil || kind != unix.SOCK_DGRAM {
		return nil, errors.New("driver inspection requires a Unix datagram socket")
	}
	conn, err := net.FileConn(file)
	if err != nil {
		return nil, err
	}
	unixConn, ok := conn.(*net.UnixConn)
	if !ok {
		_ = conn.Close()
		return nil, errors.New("driver inspection socket is not local")
	}
	return unixConn, nil
}

// OpenInherited consumes the dedicated fd only when the parent declared it.
func OpenInherited(value string) (*net.UnixConn, error) {
	if value == "" {
		return nil, nil
	}
	if value != "3" {
		return nil, errors.New("driver inspection descriptor must be 3")
	}
	unix.CloseOnExec(3)
	file := os.NewFile(3, "driver-inspection")
	defer func() { _ = file.Close() }()
	return fromFile(file)
}
