//go:build linux || darwin

package workerhost

import (
	"errors"
	"net"
	"os"
	"syscall"
)

func rootIdentity(path string) (uint64, uint64, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return 0, 0, errors.New("worker scratch root is missing or unsafe")
	}
	permissions := info.Mode().Perm()
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 ||
		permissions&0o007 != 0 || permissions&0o060 != 0 {
		return 0, 0, errors.New("worker scratch root is missing or unsafe")
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || uint64(stat.Dev) == 0 || uint64(stat.Ino) == 0 {
		return 0, 0, errors.New("worker scratch root has no filesystem identity")
	}
	return uint64(stat.Dev), uint64(stat.Ino), nil
}

func fileOwner(info os.FileInfo) (uint32, uint32, error) {
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return 0, 0, errors.New("filesystem object has no Unix owner")
	}
	return stat.Uid, stat.Gid, nil
}

func peerUID(connection net.Conn) (uint32, error) {
	unixConnection, ok := connection.(*net.UnixConn)
	if !ok {
		return 0, errors.New("worker host connection is not a Unix socket")
	}
	raw, err := unixConnection.SyscallConn()
	if err != nil {
		return 0, err
	}
	var uid uint32
	var credentialErr error
	if err := raw.Control(func(fd uintptr) {
		uid, credentialErr = socketPeerUID(int(fd))
	}); err != nil {
		return 0, err
	}
	if credentialErr != nil {
		return 0, credentialErr
	}
	return uid, nil
}
