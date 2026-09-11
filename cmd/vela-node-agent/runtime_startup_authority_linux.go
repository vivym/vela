package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"syscall"

	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/securefile"
)

type runtimeStartupSocket struct {
	listener *net.UnixListener
	path     string
	identity os.FileInfo
}

// runtimeStartupResources owns the concrete resources created by Node startup
// assembly. It intentionally stops short of constructing RuntimeStartupAuthority
// until journal ownership, observer custody and worker ownership are present.
type runtimeStartupResources struct {
	plan          *nodeagent.RuntimeLaunchPlan
	pods          *nodeagent.KubernetesRuntimeLaunchPodReader
	observer      *nodeagent.RuntimeContainerObserver
	registry      *fleettransport.BootstrapClient
	registryClose func() error
	socket        *runtimeStartupSocket
}

func runRuntimeStartupGate(configuration config) error {
	resources, err := loadRuntimeStartupResources(context.Background(), configuration)
	if err != nil {
		return err
	}
	defer func() { _ = resources.Close() }()
	return errors.New("runtime startup authority composition is not wired")
}

func loadRuntimeStartupResources(ctx context.Context, configuration config) (*runtimeStartupResources, error) {
	if !configuration.runtimeStartupEnabled {
		return nil, errors.New("runtime startup is disabled")
	}
	if ctx == nil {
		return nil, errors.New("runtime startup resource context is required")
	}
	plan, err := loadRuntimeStartupPlan(configuration)
	if err != nil {
		return nil, err
	}
	core, err := loadRuntimeKubernetesCore(configuration.runtimeKubeconfig)
	if err != nil {
		return nil, fmt.Errorf("load runtime Kubernetes API: %w", err)
	}
	pods, err := nodeagent.NewKubernetesRuntimeLaunchPodReader(core)
	if err != nil {
		return nil, fmt.Errorf("configure runtime Pod reader: %w", err)
	}
	observer, err := loadRuntimeContainerObserver(ctx, configuration)
	if err != nil {
		return nil, err
	}
	registry, registryClose, err := loadRuntimeStartupRegistry(ctx, configuration)
	if err != nil {
		_ = observer.Close()
		return nil, err
	}
	socket, err := listenRuntimeStartupSocket(configuration)
	if err != nil {
		_ = registryClose()
		_ = observer.Close()
		return nil, err
	}
	return &runtimeStartupResources{plan: plan, pods: pods, observer: observer, registry: registry, registryClose: registryClose, socket: socket}, nil
}

func (resources *runtimeStartupResources) Close() error {
	if resources == nil {
		return nil
	}
	var closeErr error
	if resources.socket != nil {
		closeErr = errors.Join(closeErr, resources.socket.Close())
	}
	if resources.registryClose != nil {
		closeErr = errors.Join(closeErr, resources.registryClose())
	}
	if resources.observer != nil {
		closeErr = errors.Join(closeErr, resources.observer.Close())
	}
	return closeErr
}

func listenRuntimeStartupSocket(configuration config) (*runtimeStartupSocket, error) {
	if !configuration.runtimeStartupEnabled {
		return nil, errors.New("runtime startup is disabled")
	}
	path := configuration.runtimeStartupSocket
	cleaned := filepath.Clean(path)
	if path == "" || !filepath.IsAbs(cleaned) || cleaned != path || len(path) > 107 {
		return nil, errors.New("runtime startup socket path is missing, non-canonical or too long")
	}
	_, err := securefile.ResolveTrustedDirectory(filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("validate runtime startup socket directory: %w", err)
	}
	if _, err := os.Lstat(path); err == nil {
		return nil, errors.New("runtime startup socket path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		return nil, fmt.Errorf("listen on runtime startup socket: %w", err)
	}
	cleanup := func() { _ = listener.Close(); _ = os.Remove(path) }
	if err := os.Chmod(path, 0o600); err != nil {
		cleanup()
		return nil, fmt.Errorf("protect runtime startup socket: %w", err)
	}
	info, err := os.Lstat(path)
	stat, statOK := info.Sys().(*syscall.Stat_t)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 || !statOK || stat.Uid != uint32(os.Geteuid()) {
		cleanup()
		return nil, errors.New("runtime startup socket identity or permissions are untrusted")
	}
	return &runtimeStartupSocket{listener: listener, path: path, identity: info}, nil
}

func (socket *runtimeStartupSocket) Listener() *net.UnixListener {
	if socket == nil {
		return nil
	}
	return socket.listener
}

func (socket *runtimeStartupSocket) Close() error {
	if socket == nil {
		return nil
	}
	err := socket.listener.Close()
	if current, statErr := os.Lstat(socket.path); statErr == nil && os.SameFile(socket.identity, current) {
		err = errors.Join(err, os.Remove(socket.path))
	}
	return err
}

func loadRuntimeContainerObserver(ctx context.Context, configuration config) (*nodeagent.RuntimeContainerObserver, error) {
	if !configuration.runtimeStartupEnabled {
		return nil, errors.New("runtime startup is disabled")
	}
	if ctx == nil || configuration.runtimeCRISocket == "" || configuration.nodeIdentity == "" {
		return nil, nodeagent.ErrRuntimeObserverCustody
	}
	observer, err := nodeagent.DialRuntimeContainerObserver(ctx, nodeagent.RuntimeContainerObserverConfig{
		SocketPath:   configuration.runtimeCRISocket,
		NodeIdentity: configuration.nodeIdentity,
	})
	if err != nil {
		return nil, errors.Join(errors.New("load runtime CRI observer"), err)
	}
	return observer, nil
}

// newRuntimeStartupAuthority is the command-level injection boundary. Every
// runtime object is supplied by the caller; this helper deliberately does not
// manufacture adapters from paths or reuse the WorkerInstance Fleet client.
func newRuntimeStartupAuthority(configuration config, plan *nodeagent.RuntimeLaunchPlan, sources nodeagent.RuntimeStartupAuthorityConfig) (nodeagent.RuntimeStartupAuthority, error) {
	if !configuration.runtimeStartupEnabled {
		return nodeagent.RuntimeStartupAuthority{}, errors.New("runtime startup is disabled")
	}
	if plan == nil {
		return nodeagent.RuntimeStartupAuthority{}, nodeagent.ErrRuntimeStartupAuthority
	}
	sources.Plan = plan
	return nodeagent.NewRuntimeStartupAuthority(sources)
}
