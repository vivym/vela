package nodeagent

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

var ErrRuntimeContainerDaemon = errors.New("containerd peer process or filesystem view is untrusted or lost")

// The socket owner is trusted to serve the RPCs itself. Socket activation,
// delegated listeners and proxies are not supported for task bundle reads.
// The pidfd pins the original peer across gRPC reconnects, never a numeric PID
// recovered after daemon replacement. Root administrators remain trusted.
type runtimeContainerDaemon struct {
	mu      sync.Mutex
	pidfd   *os.File
	process *os.File
	pid     int32
	closed  bool
}

func (daemon *runtimeContainerDaemon) authenticate(connection net.Conn) error {
	local, ok := connection.(*net.UnixConn)
	if !ok {
		return ErrRuntimeContainerDaemon
	}
	raw, err := local.SyscallConn()
	if err != nil {
		return err
	}
	fd := -1
	var peer *unix.Ucred
	var peerErr error
	err = raw.Control(func(socket uintptr) {
		peer, peerErr = unix.GetsockoptUcred(int(socket), unix.SOL_SOCKET, unix.SO_PEERCRED)
		if peerErr != nil || peer.Uid != 0 || peer.Gid != 0 || peer.Pid <= 0 {
			peerErr = errors.Join(ErrRuntimeContainerDaemon, peerErr)
			return
		}
		fd, peerErr = unix.GetsockoptInt(int(socket), unix.SOL_SOCKET, unix.SO_PEERPIDFD)
	})
	if peerErr != nil {
		// GetsockoptInt returns zero on failure, which is not an acquired fd.
		fd = -1
	}
	if fd >= 0 {
		defer func() { _ = unix.Close(fd) }()
	}
	if err := errors.Join(err, peerErr); err != nil {
		return err
	}
	if err := checkRuntimePIDFD(fd, peer.Pid); err != nil {
		return err
	}
	flags, err := unix.FcntlInt(uintptr(fd), unix.F_GETFD, 0)
	if err != nil || flags&unix.FD_CLOEXEC == 0 {
		return errors.Join(ErrRuntimeContainerDaemon, err)
	}
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if daemon.closed {
		return ErrRuntimeContainerDaemon
	}
	if daemon.pidfd != nil {
		return runtimechannel.SameLiveProcess(int(daemon.pidfd.Fd()), fd)
	}
	process, err := os.Open(fmt.Sprintf("/proc/%d", peer.Pid))
	if err != nil {
		return err
	}
	var filesystem unix.Statfs_t
	if err := unix.Fstatfs(int(process.Fd()), &filesystem); err != nil || filesystem.Type != unix.PROC_SUPER_MAGIC {
		_ = process.Close()
		return errors.Join(ErrRuntimeContainerDaemon, err)
	}
	if err := checkRuntimePIDFD(fd, peer.Pid); err != nil {
		_ = process.Close()
		return err
	}
	// Duplicate with CLOEXEC because fd is closed on every return above.
	pinned, err := unix.FcntlInt(uintptr(fd), unix.F_DUPFD_CLOEXEC, 0)
	if err != nil {
		_ = process.Close()
		return err
	}
	daemon.pidfd, daemon.process, daemon.pid = os.NewFile(uintptr(pinned), "containerd-peer-pidfd"), process, peer.Pid
	return nil
}

func (daemon *runtimeContainerDaemon) check() error {
	if daemon == nil {
		return ErrRuntimeContainerDaemon
	}
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	return daemon.checkLocked()
}

func (daemon *runtimeContainerDaemon) checkLocked() error {
	if daemon.closed || daemon.pidfd == nil || daemon.process == nil {
		return ErrRuntimeContainerDaemon
	}
	return checkRuntimePIDFD(int(daemon.pidfd.Fd()), daemon.pid)
}

func (daemon *runtimeContainerDaemon) close() error {
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if daemon.closed {
		return nil
	}
	daemon.closed = true
	var err error
	if daemon.pidfd != nil {
		err = daemon.pidfd.Close()
	}
	if daemon.process != nil {
		err = errors.Join(err, daemon.process.Close())
	}
	return err
}

// Open from the retained peer's procfs root magic link, intentionally entering
// that process's filesystem view. All components below it must be root-owned
// directories without symlinks or group/other writers. The Node may expose the
// same state inode at a different bind-mount path.
func (daemon *runtimeContainerDaemon) openDirectory(path string) (*os.File, error) {
	if daemon == nil || !filepath.IsAbs(path) || filepath.Clean(path) != path || strings.ContainsRune(path, '\x00') {
		return nil, ErrRuntimeContainerDaemon
	}
	daemon.mu.Lock()
	defer daemon.mu.Unlock()
	if err := daemon.checkLocked(); err != nil {
		return nil, err
	}
	fd, err := unix.Openat(int(daemon.process.Fd()), "root", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, errors.Join(ErrRuntimeContainerDaemon, err)
	}
	current := os.NewFile(uintptr(fd), "containerd-root")
	defer func() {
		if current != nil {
			_ = current.Close()
		}
	}()
	if err := trustedDaemonDirectory(current); err != nil {
		return nil, err
	}
	for _, component := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		if component == "" { // the filesystem root
			continue
		}
		fd, err := unix.Openat(int(current.Fd()), component, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		if err != nil {
			return nil, errors.Join(ErrRuntimeContainerDaemon, err)
		}
		_ = current.Close()
		current = os.NewFile(uintptr(fd), "containerd-directory")
		if err := trustedDaemonDirectory(current); err != nil {
			return nil, err
		}
	}
	if err := daemon.checkLocked(); err != nil {
		return nil, err
	}
	result := current
	current = nil
	return result, nil
}

func trustedDaemonDirectory(directory *os.File) error {
	var stat unix.Stat_t
	if err := unix.Fstat(int(directory.Fd()), &stat); err != nil || stat.Mode&unix.S_IFMT != unix.S_IFDIR || stat.Uid != 0 || stat.Gid != 0 || stat.Mode&0o7022 != 0 {
		return errors.Join(ErrRuntimeContainerDaemon, err)
	}
	return nil
}
