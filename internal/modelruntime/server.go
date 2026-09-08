package modelruntime

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
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
	Manifest           LaunchManifest
	EpochStore         EpochStore
	Validator          *stageauthority.Validator
	SocketPath         string
	CancelTimeout      time.Duration
	ShutdownTimeout    time.Duration
	MaxClockSkew       time.Duration
	BackendFactory     RuntimeBackendFactory
	BackendStartupGate RuntimeBackendStartupGate
	ExecutionFloor     *ExecutionFloorConfig
	RegistryBinding    *velav1.WorkerBootstrapBinding
	RegistryVerifier   *journalbinding.Verifier
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
	return startRuntimeServer(ctx, config, nil)
}

func startRuntimeServer(ctx context.Context, config RuntimeServerConfig, opened func(*executionStateFile)) (*RuntimeServer, error) {
	if ctx == nil || config.EpochStore == nil || config.Validator == nil {
		return nil, errors.New("ModelRuntime server configuration is incomplete")
	}
	if err := context.Cause(ctx); err != nil {
		return nil, err
	}
	if err := validateLaunchManifest(config.Manifest); err != nil {
		return nil, err
	}
	// Journal identity and every factory must use the same startup snapshot,
	// independent of caller slices and aliases between AUX runtime entries.
	config.Manifest = cloneLaunchManifest(config.Manifest)
	if (config.RegistryBinding == nil) != (config.RegistryVerifier == nil) {
		return nil, errors.New("ModelRuntime Registry binding and verifier must be configured together")
	}
	if config.RegistryBinding != nil {
		if config.ExecutionFloor == nil || config.ExecutionFloor.State == nil || config.ExecutionFloor.State.Initialize ||
			config.ExecutionFloor.State.UpgradeV2 || config.ExecutionFloor.State.UpgradeV3 || config.ExecutionFloor.State.UpgradeV4 || config.ExecutionFloor.State.UpgradeV5 || config.ExecutionFloor.State.UpgradeV6 || config.ExecutionFloor.State.UpgradeV7 {
			return nil, errors.New("ModelRuntime Registry binding requires an existing execution journal without initialization or upgrade")
		}
		verified, err := config.RegistryVerifier.Verify(config.RegistryBinding)
		if err != nil {
			return nil, fmt.Errorf("verify ModelRuntime Registry journal binding: %w", err)
		}
		config.RegistryBinding = verified
	}
	if config.ExecutionFloor != nil {
		floor, err := config.Manifest.bindExecutionFloorConfig(*config.ExecutionFloor, config.Validator)
		if err != nil {
			return nil, err
		}
		config.ExecutionFloor = floor
	}
	if config.CancelTimeout <= 0 || config.CancelTimeout > time.Minute {
		return nil, errors.New("ModelRuntime server cancellation timeout is invalid")
	}
	if config.MaxClockSkew < 0 || config.MaxClockSkew > time.Minute {
		return nil, errors.New("ModelRuntime server clock skew is invalid")
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
	var startupState *executionStateFile
	if config.ExecutionFloor != nil && config.ExecutionFloor.State != nil {
		verifier, err := newExecutionFloorVerifier(*config.ExecutionFloor, bindings[0])
		if err != nil {
			return nil, err
		}
		startupState, err = openExecutionState(*config.ExecutionFloor.State, executionJournalScope{binding: cloneBinding(bindings[0]), floor: verifier})
		if err != nil {
			return nil, err
		}
		// Keep the same lock across epoch allocation, model startup and Supervisor
		// attachment. Failed startup closes it after rolling back created services.
		defer func() {
			if startupState != nil {
				_ = startupState.close()
			}
		}()
		if config.RegistryBinding != nil {
			if err := config.RegistryVerifier.VerifyJournal(config.RegistryBinding, journalbinding.RuntimeJournal, journalbinding.Journal{
				WorkerInstanceID: config.Manifest.WorkerInstanceID, WorkerInstanceEpoch: config.Manifest.WorkerInstanceEpoch,
				WorkerMemberID: config.Manifest.WorkerMemberID, WorkerMemberEpoch: config.Manifest.WorkerMemberEpoch,
				JournalID: startupState.state.ID, Scope: startupState.state.Scope,
			}); err != nil {
				return nil, fmt.Errorf("match locked ModelRuntime journal to Registry: %w", err)
			}
		}
		if opened != nil {
			opened(startupState)
		}
		if config.RegistryBinding != nil && startupState.recoveryError() == nil && config.BackendStartupGate == nil {
			return nil, ErrBackendStartupGateRequired
		}
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
	var supervisor *Supervisor
	lifecycles := make([]namedBackendLifecycle, 0, len(bindings))
	runtimeCtx, cancelRuntimes := context.WithCancelCause(ctx)
	backendDone := make(chan error, len(bindings))
	shutdownServices := func() error {
		if supervisor != nil {
			return supervisor.Shutdown()
		}
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
	backendStartupRecorded := false
	for index, binding := range bindings {
		if startupErr := runtimeStartupFailure(runtimeCtx, lifecycles); startupErr != nil {
			return rollbackStart(fmt.Errorf("resident runtime startup canceled: %w", startupErr))
		}
		if startupState != nil {
			if err := startupState.check(); err != nil {
				return rollbackStart(err)
			}
		}
		runtime := config.Manifest.Runtimes[index]
		backendConfig, configErr := runtime.ProcessBackendConfig(config.Manifest.LocalDevices)
		if configErr != nil {
			return rollbackStart(configErr)
		}
		var startedBackend Backend
		service, serviceErr := NewService(Config{
			Binding: binding, EpochStore: config.EpochStore, Validator: config.Validator,
			EpochFloor: runtime.ModelRuntimeEpochFloor, MaxClockSkew: config.MaxClockSkew,
			BackendFactory: func(allocated stageauthority.RuntimeBinding) (Backend, error) {
				// A valid pending journal permits recovery RPCs, not replacement
				// model startup while historical writers may still own the device.
				if startupState != nil {
					if err := startupState.recoveryError(); err != nil {
						return recoveryOnlyBackend{reason: err}, nil
					}
					if !backendStartupRecorded {
						if err := startupState.recordBackendStartup(config.Manifest); err != nil {
							return nil, err
						}
						if config.RegistryBinding != nil {
							if err := startupState.authorizeBackendStartup(runtimeCtx, config.RegistryBinding, config.BackendStartupGate); err != nil {
								return nil, err
							}
						}
						backendStartupRecorded = true
					}
				}
				if err := context.Cause(runtimeCtx); err != nil {
					return nil, err
				}
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
	supervisor, err = newSupervisorWithState(config.ExecutionFloor, startupState, services...)
	if err != nil {
		return rollbackStart(err)
	}
	startupState = nil
	supervisor.registryBinding, supervisor.registryVerifier = config.RegistryBinding, config.RegistryVerifier
	grpcServer := grpc.NewServer(
		grpc.MaxRecvMsgSize(4<<20), grpc.MaxSendMsgSize(1<<20),
		grpc.MaxConcurrentStreams(128),
		grpc.UnaryInterceptor(limitRuntimeRequest),
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

func (manifest LaunchManifest) bindExecutionFloorConfig(config ExecutionFloorConfig, validator *stageauthority.Validator) (*ExecutionFloorConfig, error) {
	members, err := manifest.ExecutionFloorMembers()
	if err != nil {
		return nil, err
	}
	if len(config.Members) != 0 {
		expected := make(map[string]ExecutionFloorMember, len(members))
		for _, member := range members {
			expected[member.WorkerMemberID] = member
		}
		for _, member := range config.Members {
			trusted, found := expected[member.WorkerMemberID]
			if !found || member.MemberEpoch != trusted.MemberEpoch ||
				!bytes.Equal(member.IdentityDigest, trusted.IdentityDigest) || !bytes.Equal(member.DeviceSubsetDigest, trusted.DeviceSubsetDigest) {
				return nil, errors.New("ModelRuntime execution floor topology does not match launch authority")
			}
			delete(expected, member.WorkerMemberID)
		}
		if len(expected) != 0 {
			return nil, errors.New("ModelRuntime execution floor topology omits launch members")
		}
	}
	config.Members = members
	if config.Validator == nil {
		config.Validator = validator
	}
	return &config, nil
}

// Only signed terminal history needs the larger receive bound. Existing
// execution and readiness RPCs retain their original decoded message limit.
func limitRuntimeRequest(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
	if info.FullMethod != velav1.ModelRuntimeService_InstallStageExecutionFloor_FullMethodName {
		message, ok := request.(proto.Message)
		if !ok || proto.Size(message) > 1<<20 {
			return nil, status.Error(codes.ResourceExhausted, "ModelRuntime request exceeds its size limit")
		}
	}
	return handler(ctx, request)
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
