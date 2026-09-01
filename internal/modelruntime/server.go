package modelruntime

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

const (
	maxRuntimeSocketPathBytes = 100
	defaultServerStopTimeout  = 20 * time.Second
)

type RuntimeBackendFactory func(
	context.Context,
	LaunchRuntime,
	stageauthority.RuntimeBinding,
	ProcessBackendConfig,
) (Backend, error)

type RuntimeServerConfig struct {
	Manifest        LaunchManifest
	EpochStore      EpochStore
	Validator       *stageauthority.Validator
	SocketPath      string
	CancelTimeout   time.Duration
	ShutdownTimeout time.Duration
	BackendFactory  RuntimeBackendFactory
}

type RuntimeServer struct {
	grpcServer  *grpc.Server
	supervisor  *Supervisor
	listener    net.Listener
	socketPath  string
	socketInfo  os.FileInfo
	done        chan struct{}
	stopTimeout time.Duration

	closeOnce sync.Once
	closeErr  error
	waitMu    sync.Mutex
	waitErr   error
}

type namedBackendLifecycle struct {
	identity  string
	lifecycle BackendLifecycle
}

func StartRuntimeServer(ctx context.Context, config RuntimeServerConfig) (*RuntimeServer, error) {
	if ctx == nil || config.EpochStore == nil || config.Validator == nil {
		return nil, errors.New("ModelRuntime server configuration is incomplete")
	}
	if err := validateLaunchManifest(config.Manifest); err != nil {
		return nil, err
	}
	if config.CancelTimeout <= 0 || config.CancelTimeout > time.Minute {
		return nil, errors.New("ModelRuntime server cancellation timeout is invalid")
	}
	if config.ShutdownTimeout == 0 {
		config.ShutdownTimeout = defaultServerStopTimeout
	}
	if config.ShutdownTimeout <= 0 || config.ShutdownTimeout > 10*time.Minute {
		return nil, errors.New("ModelRuntime server shutdown timeout is invalid")
	}
	socketPath, err := validateRuntimeSocketTarget(config.SocketPath)
	if err != nil {
		return nil, err
	}
	bindings, err := config.Manifest.RuntimeBindings()
	if err != nil {
		return nil, err
	}
	backendFactory := config.BackendFactory
	if backendFactory == nil {
		backendFactory = func(
			ctx context.Context,
			_ LaunchRuntime,
			binding stageauthority.RuntimeBinding,
			backendConfig ProcessBackendConfig,
		) (Backend, error) {
			return NewProcessBackend(ctx, binding, backendConfig)
		}
	}
	services := make([]*Service, 0, len(bindings))
	lifecycles := make([]namedBackendLifecycle, 0, len(bindings))
	runtimeCtx, cancelRuntimes := context.WithCancelCause(ctx)
	backendDone := make(chan error, len(bindings))
	shutdownServices := func() error {
		var shutdownErr error
		for _, service := range services {
			shutdownErr = errors.Join(shutdownErr, service.Shutdown())
		}
		return shutdownErr
	}
	rollbackStart := func(startErr error) (*RuntimeServer, error) {
		cancelRuntimes(startErr)
		return nil, errors.Join(startErr, shutdownServices())
	}
	watchBackend := func(backend namedBackendLifecycle) {
		go func() {
			<-backend.lifecycle.Done()
			backendErr := backendExitError(backend)
			cancelRuntimes(backendErr)
			backendDone <- backendErr
		}()
	}
	for index, binding := range bindings {
		if startupErr := runtimeStartupFailure(runtimeCtx, lifecycles); startupErr != nil {
			return rollbackStart(fmt.Errorf("resident runtime startup canceled: %w", startupErr))
		}
		runtime := config.Manifest.Runtimes[index]
		backendConfig, configErr := runtime.ProcessBackendConfig(config.Manifest.LocalDevices)
		if configErr != nil {
			return rollbackStart(configErr)
		}
		var startedBackend Backend
		service, serviceErr := NewService(Config{
			Binding: binding, EpochStore: config.EpochStore, Validator: config.Validator,
			BackendFactory: func(allocated stageauthority.RuntimeBinding) (Backend, error) {
				backend, backendErr := backendFactory(runtimeCtx, runtime, allocated, backendConfig)
				startedBackend = backend
				return backend, backendErr
			},
			CancelTimeout: config.CancelTimeout,
		})
		if serviceErr != nil {
			return rollbackStart(fmt.Errorf("start resident runtime %q: %w", runtime.RuntimeIdentity, serviceErr))
		}
		services = append(services, service)
		if lifecycle, ok := startedBackend.(BackendLifecycle); ok && lifecycle.Done() != nil {
			named := namedBackendLifecycle{
				identity: runtime.RuntimeIdentity, lifecycle: lifecycle,
			}
			lifecycles = append(lifecycles, named)
			watchBackend(named)
		}
		if startupErr := runtimeStartupFailure(runtimeCtx, lifecycles); startupErr != nil {
			return rollbackStart(fmt.Errorf("resident runtime readiness lost during startup: %w", startupErr))
		}
	}
	supervisor, err := NewSupervisor(services...)
	if err != nil {
		return rollbackStart(err)
	}
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(1<<20), grpc.MaxSendMsgSize(1<<20),
		grpc.MaxConcurrentStreams(128),
	)
	velav1.RegisterModelRuntimeServiceServer(grpcServer, supervisor)
	if startupErr := runtimeStartupFailure(runtimeCtx, lifecycles); startupErr != nil {
		grpcServer.Stop()
		return rollbackStart(fmt.Errorf("resident runtime readiness lost before socket publication: %w", startupErr))
	}
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		grpcServer.Stop()
		return rollbackStart(fmt.Errorf("listen on private ModelRuntime socket: %w", err))
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		closeErr := listener.Close()
		grpcServer.Stop()
		removeErr := os.Remove(socketPath)
		return rollbackStart(errors.Join(
			fmt.Errorf("protect private ModelRuntime socket: %w", err), closeErr, removeErr,
		))
	}
	socketInfo, err := os.Lstat(socketPath)
	if err != nil || socketInfo.Mode()&os.ModeSocket == 0 || socketInfo.Mode().Perm() != 0o600 {
		closeErr := listener.Close()
		grpcServer.Stop()
		removeErr := os.Remove(socketPath)
		return rollbackStart(errors.Join(
			errors.New("private ModelRuntime socket identity is invalid"), err, closeErr, removeErr,
		))
	}
	server := &RuntimeServer{
		grpcServer: grpcServer, supervisor: supervisor, listener: listener,
		socketPath: socketPath, socketInfo: socketInfo, done: make(chan struct{}),
		stopTimeout: config.ShutdownTimeout,
	}
	serveDone := make(chan error, 1)
	go func() {
		serveErr := grpcServer.Serve(listener)
		if errors.Is(serveErr, grpc.ErrServerStopped) || errors.Is(serveErr, net.ErrClosed) {
			serveErr = nil
		}
		serveDone <- serveErr
	}()
	go func() {
		var terminalErr error
		if len(lifecycles) == 0 {
			terminalErr = <-serveDone
		} else {
			select {
			case terminalErr = <-serveDone:
			case terminalErr = <-backendDone:
				grpcServer.Stop()
				terminalErr = errors.Join(terminalErr, <-serveDone)
			}
		}
		server.waitMu.Lock()
		server.waitErr = terminalErr
		server.waitMu.Unlock()
		close(server.done)
	}()
	return server, nil
}

func (server *RuntimeServer) Wait() error {
	if server == nil || server.done == nil {
		return errors.New("ModelRuntime server is not configured")
	}
	<-server.done
	return server.terminalError()
}

func (server *RuntimeServer) Close() error {
	if server == nil {
		return nil
	}
	server.closeOnce.Do(func() {
		stopped := make(chan struct{})
		go func() {
			server.grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(server.stopTimeout):
			server.grpcServer.Stop()
			<-stopped
			server.closeErr = errors.New("ModelRuntime gRPC server exceeded graceful shutdown timeout")
		}
		<-server.done
		serveErr := server.terminalError()
		if serveErr != nil {
			server.closeErr = errors.Join(server.closeErr, serveErr)
		}
		server.closeErr = errors.Join(server.closeErr, server.supervisor.Shutdown())
		if info, err := os.Lstat(server.socketPath); err == nil && os.SameFile(server.socketInfo, info) {
			if removeErr := os.Remove(server.socketPath); removeErr != nil {
				server.closeErr = errors.Join(server.closeErr, removeErr)
			}
		} else if err != nil && !errors.Is(err, os.ErrNotExist) {
			server.closeErr = errors.Join(server.closeErr, err)
		}
	})
	return server.closeErr
}

func (server *RuntimeServer) terminalError() error {
	server.waitMu.Lock()
	defer server.waitMu.Unlock()
	return server.waitErr
}

func runtimeStartupFailure(
	ctx context.Context,
	lifecycles []namedBackendLifecycle,
) error {
	if err := context.Cause(ctx); err != nil {
		return err
	}
	for _, backend := range lifecycles {
		select {
		case <-backend.lifecycle.Done():
			return backendExitError(backend)
		default:
		}
	}
	return nil
}

func backendExitError(backend namedBackendLifecycle) error {
	err := backend.lifecycle.Err()
	if err == nil {
		err = errors.New("resident ModelRuntime backend exited")
	}
	return fmt.Errorf("resident runtime %q exited: %w", backend.identity, err)
}

func validateRuntimeSocketTarget(path string) (string, error) {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path || strings.ContainsRune(path, '\x00') ||
		len([]byte(path)) > maxRuntimeSocketPathBytes {
		return "", errors.New("ModelRuntime socket path is invalid")
	}
	parent := filepath.Dir(cleaned)
	resolved, err := securefile.ResolveTrustedDirectory(parent)
	if err != nil || resolved != parent || securefile.ValidateDirectory(parent) != nil {
		return "", errors.New("ModelRuntime socket directory is not private and trusted")
	}
	if _, err := os.Lstat(cleaned); err == nil {
		return "", errors.New("ModelRuntime socket path already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", fmt.Errorf("inspect ModelRuntime socket target: %w", err)
	}
	return cleaned, nil
}
