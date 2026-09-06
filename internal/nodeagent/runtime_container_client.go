package nodeagent

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/vivym/vela/internal/securefile"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	runtimev1 "k8s.io/cri-api/pkg/apis/runtime/v1"
)

type RuntimeContainerObserverConfig struct {
	SocketPath   string
	NodeIdentity string
}

// DialRuntimeContainerObserver connects only to a root-owned local CRI socket
// under trusted directories and authenticates its kernel-reported peer UID.
// It reads the host boot ID directly; no container-supplied identity is used.
func DialRuntimeContainerObserver(ctx context.Context, config RuntimeContainerObserverConfig) (*RuntimeContainerObserver, error) {
	return dialRuntimeContainerObserver(ctx, config, 0, func() (string, error) {
		return readBootID("/proc/sys/kernel/random/boot_id")
	})
}

func dialRuntimeContainerObserver(ctx context.Context, config RuntimeContainerObserverConfig, owner uint32, boot func() (string, error)) (*RuntimeContainerObserver, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	path := config.SocketPath
	if !filepath.IsAbs(path) || filepath.Clean(path) != path || len(path) > 100 || strings.ContainsRune(path, '\x00') ||
		!validText(config.NodeIdentity, maxIdentityText) || boot == nil {
		return nil, errors.New("CRI observer requires a local socket and Node identity")
	}
	parent, err := securefile.ResolveTrustedDirectory(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("validate CRI socket directory: %w", err)
	}
	path = filepath.Join(parent, filepath.Base(path))
	root, err := securefile.OpenTrustedRoot(parent)
	if err != nil {
		return nil, err
	}
	success := false
	defer func() {
		if !success {
			_ = root.Close()
		}
	}()
	parentInfo, err := root.Stat(".")
	if err != nil {
		return nil, err
	}
	validateSocket := func() (os.FileInfo, error) {
		currentParent, err := os.Lstat(parent)
		if err != nil || !os.SameFile(parentInfo, currentParent) {
			return nil, errors.New("CRI socket directory identity changed")
		}
		if err := securefile.ValidateDirectory(parent); err != nil {
			return nil, fmt.Errorf("CRI socket directory is no longer trusted: %w", err)
		}
		info, err := root.Lstat(filepath.Base(path))
		if err != nil {
			return nil, err
		}
		stat, ok := info.Sys().(*syscall.Stat_t)
		if !ok || info.Mode()&os.ModeSocket == 0 || stat.Uid != owner ||
			(info.Mode().Perm() != 0o600 && (info.Mode().Perm() != 0o660 || stat.Gid != 0)) {
			return nil, errors.New("CRI socket owner, type or permissions are untrusted")
		}
		return info, nil
	}
	original, err := validateSocket()
	if err != nil {
		return nil, err
	}
	var closed atomic.Bool
	check := func() error {
		if closed.Load() {
			return errors.New("CRI observer is closed")
		}
		current, err := validateSocket()
		if err != nil {
			return err
		}
		if !os.SameFile(original, current) {
			return errors.New("CRI socket identity changed")
		}
		return nil
	}
	dialer := func(ctx context.Context, _ string) (net.Conn, error) {
		if err := check(); err != nil {
			return nil, err
		}
		connection, err := (&net.Dialer{}).DialContext(ctx, "unix", path)
		if err != nil {
			return nil, err
		}
		peerUID, peerErr := runtimeContainerPeerUID(connection)
		if err := errors.Join(peerErr, check()); err != nil || peerUID != owner {
			_ = connection.Close()
			return nil, errors.Join(errors.New("CRI socket peer or identity is untrusted"), err)
		}
		return connection, nil
	}
	connection, err := grpc.NewClient("passthrough:///vela-node-cri", grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(1<<20), grpc.MaxCallSendMsgSize(64<<10)))
	if err != nil {
		return nil, err
	}
	observer := &RuntimeContainerObserver{reader: runtimev1.NewRuntimeServiceClient(connection), nodeIdentity: config.NodeIdentity,
		bootID: boot, clock: time.Now, check: check, close: func() error {
			if closed.Swap(true) {
				return nil
			}
			return errors.Join(connection.Close(), root.Close())
		}}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	connection.Connect()
	for state := connection.GetState(); state != connectivity.Ready; state = connection.GetState() {
		if state == connectivity.Shutdown || !connection.WaitForStateChange(ctx, state) {
			_ = observer.Close()
			return nil, errors.Join(errors.New("connect to trusted local CRI service failed"), context.Cause(ctx))
		}
	}
	if _, err := observer.readBoot(); err != nil {
		_ = observer.Close()
		return nil, err
	}
	success = true
	return observer, nil
}
