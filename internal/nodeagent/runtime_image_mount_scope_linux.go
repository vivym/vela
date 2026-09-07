package nodeagent

import (
	"errors"
	"net"
	"os"
	"sync"

	"github.com/prometheus/procfs"
	"golang.org/x/sys/unix"
)

var errRuntimeImageMountPeer = errors.New("image mount reader has no live authenticated containerd process")

// The maintenance service can have a private mount view. Its kernel completion
// check must inspect the daemon that served the RPC, using a retained pidfd.
type runtimeImageMountReader struct {
	mu     sync.Mutex
	pidfd  *os.File
	pid    int32
	closed bool
}

func (reader *runtimeImageMountReader) authenticate(connection net.Conn) error {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if reader.closed {
		return errRuntimeImageMountPeer
	}
	local, ok := connection.(*net.UnixConn)
	if !ok {
		return errRuntimeImageMountPeer
	}
	raw, err := local.SyscallConn()
	if err != nil {
		return err
	}
	var peer *unix.Ucred
	fd := -1
	var peerErr error
	err = raw.Control(func(socket uintptr) {
		peer, peerErr = unix.GetsockoptUcred(int(socket), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if peerErr != nil || peer.Pid <= 0 || peer.Uid != 0 {
			peerErr = errors.Join(errRuntimeImageMountPeer, peerErr)
			return
		}
		acquired, err := unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
		if err != nil {
			peerErr = err
			return
		}
		fd = acquired
		flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
		if err != nil || flags&unix.FD_CLOEXEC == 0 {
			peerErr = errors.Join(errRuntimeImageMountPeer, err)
		}
	})
	defer func() {
		if fd >= 0 {
			_ = unix.Close(fd)
		}
	}()
	if err := errors.Join(err, peerErr); err != nil {
		return err
	}
	if err := checkRuntimePIDFD(fd, peer.Pid); err != nil {
		return errors.Join(errRuntimeImageMountPeer, err)
	}
	if reader.pidfd != nil {
		if reader.pid != peer.Pid {
			return errRuntimeImageMountPeer
		}
		return reader.checkLocked()
	}
	reader.pidfd, reader.pid = os.NewFile(uintptr(fd), "runtime-image-containerd-pidfd"), peer.Pid
	fd = -1
	return nil
}

func (reader *runtimeImageMountReader) check() error {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	return reader.checkLocked()
}

func (reader *runtimeImageMountReader) checkLocked() error {
	if reader.closed || reader.pidfd == nil {
		return errRuntimeImageMountPeer
	}
	if err := checkRuntimePIDFD(int(reader.pidfd.Fd()), reader.pid); err != nil {
		return errors.Join(errRuntimeImageMountPeer, err)
	}
	return nil
}

func (reader *runtimeImageMountReader) mounted(path string) (bool, error) {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	if err := reader.checkLocked(); err != nil {
		return false, err
	}
	mounts, err := procfs.GetProcMounts(int(reader.pid))
	if err := errors.Join(err, reader.checkLocked()); err != nil {
		return false, err
	}
	return runtimeImageMountListed(path, mounts), nil
}

func (reader *runtimeImageMountReader) close() error {
	reader.mu.Lock()
	defer reader.mu.Unlock()
	reader.closed = true
	if reader.pidfd == nil {
		return nil
	}
	err := reader.pidfd.Close()
	reader.pidfd = nil
	return err
}
