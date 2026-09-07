package runtimechannel

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sys/unix"
)

type nodeSocket struct {
	path      string
	directory *os.File
	inode     *os.File
}

func openNodeDirectory(path string) (*os.File, error) {
	fd, err := unix.Open("/", unix.O_PATH|unix.O_DIRECTORY|unix.O_CLOEXEC, 0)
	if err != nil {
		return nil, err
	}
	for _, part := range append([]string{"."}, strings.Split(strings.TrimPrefix(path, "/"), "/")...) {
		if part == "" {
			continue
		}
		next, openErr := unix.Openat(fd, part, unix.O_PATH|unix.O_DIRECTORY|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
		_ = unix.Close(fd)
		if openErr != nil {
			return nil, openErr
		}
		fd = next
		var info unix.Stat_t
		if err := unix.Fstat(fd, &info); err != nil || info.Uid != 0 || info.Mode&0o022 != 0 {
			_ = unix.Close(fd)
			return nil, errors.Join(errors.New("node socket requires root-owned non-writable ancestors"), err)
		}
	}
	return os.NewFile(uintptr(fd), path), nil
}

func openNodeSocket(path string) (*nodeSocket, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 100 || strings.ContainsRune(path, '\x00') {
		return nil, errors.New("node socket requires a canonical local filesystem path")
	}
	directory, err := openNodeDirectory(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	fd, err := unix.Openat(int(directory.Fd()), filepath.Base(path), unix.O_PATH|unix.O_NOFOLLOW|unix.O_CLOEXEC, 0)
	if err != nil {
		_ = directory.Close()
		return nil, err
	}
	socket := &nodeSocket{path: path, directory: directory, inode: os.NewFile(uintptr(fd), path)}
	if err := socket.check(); err != nil {
		_ = socket.close()
		return nil, err
	}
	return socket, nil
}

func (socket *nodeSocket) address() string {
	return fmt.Sprintf("/proc/self/fd/%d/%s", socket.directory.Fd(), filepath.Base(socket.path))
}

func (socket *nodeSocket) check() error {
	current, err := openNodeDirectory(filepath.Dir(socket.path))
	if err != nil {
		return err
	}
	defer func() { _ = current.Close() }()
	originalInfo, originalErr := socket.directory.Stat()
	currentInfo, currentErr := current.Stat()
	if originalErr != nil || currentErr != nil || !os.SameFile(originalInfo, currentInfo) {
		return errors.Join(ErrIdentity, originalErr, currentErr)
	}
	var pinned, named unix.Stat_t
	if err := errors.Join(unix.Fstat(int(socket.inode.Fd()), &pinned),
		unix.Fstatat(int(current.Fd()), filepath.Base(socket.path), &named, unix.AT_SYMLINK_NOFOLLOW)); err != nil {
		return err
	}
	if pinned.Dev != named.Dev || pinned.Ino != named.Ino || named.Mode&unix.S_IFMT != unix.S_IFSOCK ||
		named.Uid != 0 || named.Gid != uint32(os.Getegid()) || named.Mode&0o7777 != 0o660 {
		return errors.New("node socket inode, owner or permissions are untrusted")
	}
	return nil
}

func (socket *nodeSocket) close() error {
	return errors.Join(socket.inode.Close(), socket.directory.Close())
}
