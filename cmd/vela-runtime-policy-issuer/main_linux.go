//go:build linux

package main

import (
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"
	"time"

	"github.com/vivym/vela/internal/runtimepolicy"
	"github.com/vivym/vela/internal/securefile"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	if os.Geteuid() != 0 {
		return errors.New("vela-runtime-policy-issuer must run as root")
	}
	socketPath := os.Getenv("VELA_RUNTIME_POLICY_ISSUER_SOCKET")
	privateKeyPath := os.Getenv("VELA_RUNTIME_POLICY_PRIVATE_KEY_FILE")
	authorizationDirectory := os.Getenv("VELA_RUNTIME_POLICY_AUTHORIZATION_DIRECTORY")
	authorizationPublicKeyPath := os.Getenv("VELA_RUNTIME_POLICY_AUTHORIZATION_PUBLIC_KEY_FILE")
	replyCacheDirectory := os.Getenv("VELA_RUNTIME_POLICY_REPLY_CACHE_DIRECTORY")
	if !canonical(socketPath) || socketPath == "/" || !canonical(privateKeyPath) || privateKeyPath == "/" || !canonical(authorizationDirectory) || authorizationDirectory == "/" || !canonical(authorizationPublicKeyPath) || authorizationPublicKeyPath == "/" || !canonical(replyCacheDirectory) || replyCacheDirectory == "/" {
		return errors.New("runtime policy issuer paths are not canonical")
	}
	for _, path := range []string{filepath.Dir(privateKeyPath), filepath.Dir(authorizationPublicKeyPath)} {
		if _, err := securefile.ResolveTrustedDirectory(path); err != nil {
			return fmt.Errorf("validate runtime policy key directory: %w", err)
		}
	}
	runtimeGID, err := parseRuntimeGID(os.Getenv("VELA_RUNTIME_POLICY_RUNTIME_GID"))
	if err != nil {
		return err
	}
	privateWire, err := securefile.Read(privateKeyPath, ed25519.PrivateKeySize, true)
	if err != nil {
		return fmt.Errorf("read runtime policy private key: %w", err)
	}
	if len(privateWire) != ed25519.PrivateKeySize {
		return errors.New("runtime policy private key has an invalid length")
	}
	defer clear(privateWire)
	publicWire, err := securefile.Read(authorizationPublicKeyPath, ed25519.PublicKeySize, true)
	if err != nil {
		return fmt.Errorf("read Fleet authorization public key: %w", err)
	}
	authorizer, err := runtimepolicy.NewFileAuthorizer(authorizationDirectory, publicWire)
	if err != nil {
		return fmt.Errorf("configure runtime policy authorizer: %w", err)
	}
	replyCache, err := runtimepolicy.NewFileReplyCache(replyCacheDirectory)
	if err != nil {
		return fmt.Errorf("configure runtime policy reply cache: %w", err)
	}
	if _, err := securefile.ResolveTrustedDirectory(filepath.Dir(socketPath)); err != nil {
		return fmt.Errorf("validate runtime policy socket directory: %w", err)
	}
	if err := removeStaleSocket(socketPath); err != nil {
		return err
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: socketPath, Net: "unixpacket"})
	if err != nil {
		return fmt.Errorf("listen runtime policy issuer socket: %w", err)
	}
	listener.SetUnlinkOnClose(false)
	identity, err := os.Lstat(socketPath)
	if err != nil {
		_ = listener.Close()
		return err
	}
	defer func() {
		_ = listener.Close()
		current, err := os.Lstat(socketPath)
		if err == nil && os.SameFile(identity, current) {
			_ = os.Remove(socketPath)
		}
	}()
	if err := os.Chmod(socketPath, 0o660); err != nil {
		return fmt.Errorf("set runtime policy issuer socket mode: %w", err)
	}
	if err := os.Chown(socketPath, 0, int(runtimeGID)); err != nil {
		return fmt.Errorf("set runtime policy issuer socket group: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	server := &runtimepolicy.Server{Listener: listener, PrivateKey: ed25519.PrivateKey(privateWire), Authorizer: authorizer, ReplyCache: replyCache}
	err = server.Serve(ctx)
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func parseRuntimeGID(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 || parsed == uint64(^uint32(0)) {
		return 0, errors.New("VELA_RUNTIME_POLICY_RUNTIME_GID must be a positive numeric gid")
	}
	return uint32(parsed), nil
}

func canonical(path string) bool {
	return path != "" && filepath.IsAbs(path) && filepath.Clean(path) == path
}

func removeStaleSocket(path string) error {
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if info.Mode()&os.ModeSocket == 0 || !ok || stat.Uid != 0 {
		return errors.New("runtime policy issuer socket replacement is not a root-owned socket")
	}
	// A pathname can survive an unclean process death. Probe the endpoint
	// before unlinking it: a successful connection means a live issuer owns the
	// socket and replacement would split clients across two listeners. Only an
	// explicit connection refusal proves that the inode is stale.
	connection, dialErr := net.DialTimeout("unixpacket", path, 250*time.Millisecond)
	if dialErr == nil {
		_ = connection.Close()
		return errors.New("runtime policy issuer socket already exists; refusing to replace another listener")
	}
	var errno syscall.Errno
	if !errors.As(dialErr, &errno) || errno != syscall.ECONNREFUSED {
		return fmt.Errorf("runtime policy issuer socket is not provably stale: %w", dialErr)
	}
	current, statErr := os.Lstat(path)
	if statErr != nil {
		if errors.Is(statErr, os.ErrNotExist) {
			return nil
		}
		return statErr
	}
	if !os.SameFile(info, current) {
		return errors.New("runtime policy issuer socket changed while checking staleness")
	}
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove stale runtime policy issuer socket: %w", err)
	}
	return nil
}
