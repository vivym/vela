package securefile

import (
	"errors"
	"io"
	"os"
	"path/filepath"
	"syscall"

	"golang.org/x/sys/unix"
)

// ReadRootPublication reads Node-published public verification material. Unlike
// a private key, it is readable by the explicitly selected runtime group, but
// neither that group nor the reader may own or replace the file or its parents.
func ReadRootPublication(path string, maxBytes int64, gid uint32) ([]byte, error) {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || maxBytes <= 0 || gid == 0 || gid == ^uint32(0) {
		return nil, errors.New("invalid root publication path, size or group")
	}
	parent, err := OpenTrustedDirectory(filepath.Dir(path))
	if err != nil {
		return nil, err
	}
	defer func() { _ = parent.Close() }()
	for directory := filepath.Dir(path); ; directory = filepath.Dir(directory) {
		info, err := os.Lstat(directory)
		if err != nil || !ownedByRoot(info) || !info.IsDir() || info.Mode().Perm()&0o022 != 0 {
			return nil, errors.New("root publication requires root-owned non-writable ancestors")
		}
		if directory == "/" {
			break
		}
	}
	var before unix.Stat_t
	if err := unix.Fstatat(int(parent.Fd()), filepath.Base(path), &before, unix.AT_SYMLINK_NOFOLLOW); err != nil || before.Mode&unix.S_IFMT != unix.S_IFREG {
		return nil, errors.New("root publication must name a regular file")
	}
	fd, err := unix.Openat(int(parent.Fd()), filepath.Base(path), unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_NOCTTY, 0)
	if err != nil {
		return nil, err
	}
	file := os.NewFile(uintptr(fd), path)
	defer func() { _ = file.Close() }()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm() != 0o440 || info.Mode()&(os.ModeType|os.ModeSetuid|os.ModeSetgid|os.ModeSticky) != 0 {
		return nil, errors.New("root publication must be a 0440 regular file")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || stat.Gid != gid || stat.Nlink != 1 || uint64(stat.Dev) != uint64(before.Dev) || uint64(stat.Ino) != uint64(before.Ino) || info.Size() <= 0 || info.Size() > maxBytes {
		return nil, errors.New("root publication ownership, link count or size is invalid")
	}
	wire, err := io.ReadAll(io.LimitReader(file, maxBytes+1))
	if err != nil || int64(len(wire)) != info.Size() {
		return nil, errors.New("root publication changed or exceeded its bound")
	}
	return wire, nil
}
