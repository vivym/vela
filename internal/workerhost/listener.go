package workerhost

import (
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type UnixListenerConfig struct {
	SocketPath      string
	SocketOwnerUID  uint32
	SocketOwnerGID  uint32
	ExpectedPeerUID uint32
}

type uidListener struct {
	*net.UnixListener
	expectedPeerUID uint32
	socketPath      string
	socketInfo      os.FileInfo
	lifecycleLock   *os.File
	closeOnce       sync.Once
	closeErr        error
}

func ListenUnix(config UnixListenerConfig) (net.Listener, error) {
	socketPath := filepath.Clean(config.SocketPath)
	if !validAbsolutePath(socketPath) || len(socketPath) > maxUnixSocketPathBytes ||
		config.ExpectedPeerUID == 0 {
		return nil, errors.New("worker host Unix listener configuration is incomplete")
	}
	if err := validateSocketParent(filepath.Dir(socketPath)); err != nil {
		return nil, err
	}
	lifecycleLock, err := acquireSocketLifecycleLock(socketPath)
	if err != nil {
		return nil, err
	}
	returnLock := true
	defer func() {
		if returnLock {
			_ = lifecycleLock.Close()
		}
	}()
	if info, err := os.Lstat(socketPath); err == nil {
		if _, validationErr := validateSocket(
			socketPath, config.SocketOwnerUID, config.SocketOwnerGID,
		); validationErr != nil {
			return nil, fmt.Errorf("existing Worker host socket is unsafe: %w", validationErr)
		}
		connection, dialErr := net.DialTimeout("unix", socketPath, 100*time.Millisecond)
		if dialErr == nil {
			_ = connection.Close()
			return nil, errors.New("existing Worker host socket is accepting connections")
		}
		if !errors.Is(dialErr, syscall.ECONNREFUSED) {
			return nil, fmt.Errorf("existing Worker host socket liveness is unknown: %w", dialErr)
		}
		current, inspectErr := os.Lstat(socketPath)
		if inspectErr != nil || !os.SameFile(info, current) {
			return nil, errors.New("existing Worker host socket changed during stale-endpoint validation")
		}
		if err := os.Remove(socketPath); err != nil {
			return nil, fmt.Errorf("remove stale Worker host socket: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect Worker host socket: %w", err)
	}
	listener, err := net.ListenUnix("unix", &net.UnixAddr{Name: socketPath, Net: "unix"})
	if err != nil {
		return nil, fmt.Errorf("listen on Worker host Unix socket: %w", err)
	}
	listener.SetUnlinkOnClose(false)
	createdInfo, err := os.Lstat(socketPath)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("inspect new Worker host Unix socket: %w", err)
	}
	if err := os.Chown(socketPath, int(config.SocketOwnerUID), int(config.SocketOwnerGID)); err != nil {
		_ = listener.Close()
		_ = removeSocketIfSame(socketPath, createdInfo)
		return nil, fmt.Errorf("set Worker host socket ownership: %w", err)
	}
	if err := os.Chmod(socketPath, 0o660); err != nil {
		_ = listener.Close()
		_ = removeSocketIfSame(socketPath, createdInfo)
		return nil, fmt.Errorf("set Worker host socket mode: %w", err)
	}
	validatedInfo, err := validateSocket(socketPath, config.SocketOwnerUID, config.SocketOwnerGID)
	if err != nil {
		_ = listener.Close()
		_ = removeSocketIfSame(socketPath, createdInfo)
		return nil, err
	}
	returnLock = false
	return &uidListener{
		UnixListener: listener, expectedPeerUID: config.ExpectedPeerUID,
		socketPath: socketPath, socketInfo: validatedInfo, lifecycleLock: lifecycleLock,
	}, nil
}

func (listener *uidListener) Accept() (net.Conn, error) {
	for {
		connection, err := listener.UnixListener.Accept()
		if err != nil {
			return nil, err
		}
		uid, err := peerUID(connection)
		if err == nil && uid == listener.expectedPeerUID {
			return connection, nil
		}
		_ = connection.Close()
	}
}

func (listener *uidListener) Close() error {
	listener.closeOnce.Do(func() {
		closeErr := listener.UnixListener.Close()
		removeErr := removeSocketIfSame(listener.socketPath, listener.socketInfo)
		lockErr := listener.lifecycleLock.Close()
		if closeErr != nil && !errors.Is(closeErr, net.ErrClosed) {
			listener.closeErr = errors.Join(closeErr, removeErr, lockErr)
			return
		}
		listener.closeErr = errors.Join(removeErr, lockErr)
	})
	return listener.closeErr
}

func acquireSocketLifecycleLock(socketPath string) (*os.File, error) {
	lockPath := socketPath + ".lock"
	descriptor, err := unix.Open(
		lockPath,
		unix.O_RDWR|unix.O_CREAT|unix.O_CLOEXEC|unix.O_NOFOLLOW,
		0o600,
	)
	if err != nil {
		return nil, fmt.Errorf("open Worker host socket lifecycle lock: %w", err)
	}
	lockFile := os.NewFile(uintptr(descriptor), lockPath)
	if lockFile == nil {
		_ = unix.Close(descriptor)
		return nil, errors.New("open Worker host socket lifecycle lock")
	}
	info, err := lockFile.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		_ = lockFile.Close()
		return nil, errors.New("worker host socket lifecycle lock is unsafe")
	}
	ownerUID, _, err := fileOwner(info)
	if err != nil || ownerUID != uint32(os.Geteuid()) {
		_ = lockFile.Close()
		return nil, errors.New("worker host socket lifecycle lock has an unsafe owner")
	}
	if err := unix.Flock(descriptor, unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = lockFile.Close()
		if errors.Is(err, unix.EWOULDBLOCK) {
			return nil, errors.New("worker host socket lifecycle is owned by another process")
		}
		return nil, fmt.Errorf("lock Worker host socket lifecycle: %w", err)
	}
	return lockFile, nil
}

func removeSocketIfSame(path string, expected os.FileInfo) error {
	current, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect Worker host socket before removal: %w", err)
	}
	if expected == nil || !os.SameFile(expected, current) {
		return nil
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove Worker host socket: %w", err)
	}
	return nil
}

func validateSocketParent(path string) error {
	info, err := os.Lstat(path)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 || info.Mode().Perm()&0o022 != 0 {
		return errors.New("worker host socket parent directory is unsafe")
	}
	ownerUID, _, err := fileOwner(info)
	if err != nil || ownerUID != 0 && ownerUID != uint32(os.Geteuid()) {
		return errors.New("worker host socket parent directory has an unsafe owner")
	}
	return nil
}

func validateSocket(path string, expectedUID, expectedGID uint32) (os.FileInfo, error) {
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode()&os.ModeSymlink != 0 ||
		info.Mode().Perm() != 0o660 {
		return nil, errors.New("worker host socket is missing or unsafe")
	}
	ownerUID, ownerGID, err := fileOwner(info)
	if err != nil || ownerUID != expectedUID || ownerGID != expectedGID {
		return nil, errors.New("worker host socket ownership is unsafe")
	}
	return info, nil
}
