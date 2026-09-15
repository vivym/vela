//go:build linux

package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/vivym/vela/internal/runtimechannel"
	"github.com/vivym/vela/internal/securefile"
)

func run(ctx context.Context) error {
	if ctx == nil || os.Geteuid() != 0 || os.Getegid() != 0 {
		return errors.New("pidfd broker must run as root")
	}
	path := os.Getenv("VELA_PIDFD_BROKER_SOCKET")
	if !validSocketPath(path) {
		return errors.New("VELA_PIDFD_BROKER_SOCKET must be a canonical absolute path")
	}
	runtimeGID, err := strconv.ParseUint(os.Getenv("VELA_PIDFD_BROKER_RUNTIME_GID"), 10, 32)
	if err != nil || runtimeGID == 0 || runtimeGID == uint64(^uint32(0)) {
		return errors.New("VELA_PIDFD_BROKER_RUNTIME_GID must be a non-root uint32 GID")
	}
	listener, err := listenBrokerSocket(path, uint32(runtimeGID))
	if err != nil {
		return err
	}
	identity, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		return err
	}
	defer func() {
		_ = listener.Close()
		// Never unlink a pathname replacement created after publication.
		if current, statErr := os.Lstat(path); statErr == nil {
			if os.SameFile(identity, current) {
				_ = os.Remove(path)
			}
		}
	}()
	err = runtimechannel.ServePIDFDBroker(ctx, listener, uint32(runtimeGID))
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return nil
	}
	return err
}

func validSocketPath(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path && len(path) <= 107 && path != "/"
}

func listenBrokerSocket(path string, runtimeGID uint32) (*net.UnixListener, error) {
	if _, err := securefile.ResolveTrustedDirectory(filepath.Dir(path)); err != nil {
		return nil, fmt.Errorf("validate pidfd broker socket directory: %w", err)
	}
	if _, err := os.Lstat(path); err == nil {
		return nil, errors.New("pidfd broker socket path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		return nil, fmt.Errorf("listen on pidfd broker socket: %w", err)
	}
	published, statErr := os.Lstat(path)
	if statErr != nil {
		_ = listener.Close()
		return nil, statErr
	}
	cleanup := func() {
		_ = listener.Close()
		if current, err := os.Lstat(path); err == nil && os.SameFile(published, current) {
			_ = os.Remove(path)
		}
	}
	// UnixListener defaults to unlink-on-close. Disable that behavior because it
	// could delete a replacement published after this listener was created.
	// Cleanup below compares the socket identity before removing the pathname.
	listener.SetUnlinkOnClose(false)
	if err := os.Chown(path, 0, int(runtimeGID)); err != nil {
		cleanup()
		return nil, fmt.Errorf("publish pidfd broker socket GID: %w", err)
	}
	if err := os.Chmod(path, 0o660); err != nil {
		cleanup()
		return nil, fmt.Errorf("protect pidfd broker socket: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		cleanup()
		return nil, err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o660 || stat.Uid != 0 || stat.Gid != runtimeGID {
		cleanup()
		return nil, errors.New("pidfd broker socket identity or permissions are untrusted")
	}
	return listener, nil
}
