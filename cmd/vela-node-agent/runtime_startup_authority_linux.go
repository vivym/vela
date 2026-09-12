package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleettransport"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
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
	validator     *stageauthority.Validator
	ledger        *nodeagent.RuntimeStartupLedger
	journal       *modelruntime.ExecutionJournalOwner
	pods          *nodeagent.KubernetesRuntimeLaunchPodReader
	observer      *nodeagent.RuntimeContainerObserver
	registry      *fleettransport.BootstrapClient
	registryClose func() error
	socket        *runtimeStartupSocket
	workerOwner   *nodeagent.RuntimeNamespaceOwner
	custody       *nodeagent.RuntimeObserverCustody
	launcherClose func() error
}

type runtimeStartupLaunch = nodeagent.RuntimeStartupLaunch
type runtimeStartupLauncher = nodeagent.RuntimeStartupLauncher

var errRuntimeStartupLauncherUnavailable = errors.New("runtime startup launcher contract is not configured")

type unavailableRuntimeStartupLauncher struct{}

func (unavailableRuntimeStartupLauncher) Launch(context.Context, *nodeagent.RuntimeLaunchPlan, string) (runtimeStartupLaunch, error) {
	return runtimeStartupLaunch{}, errRuntimeStartupLauncherUnavailable
}

// Injected only by a platform integration or a focused test. The default is
// fail-closed so enabling runtime startup cannot silently reuse an observer,
// PID or reservation from another subsystem.
var runtimeStartupLauncherFactory = func(config config) runtimeStartupLauncher {
	if config.runtimeLauncherPath == "" {
		return unavailableRuntimeStartupLauncher{}
	}
	return newExecRuntimeStartupLauncher(config.runtimeLauncherPath)
}

// runtimeStartupLifecycle is the command-level shutdown owner. The
// orchestration revokes active routes and stops admission before the concrete
// resources are released, so signal handling cannot close custody underneath a
// live coordinator.
type runtimeStartupLifecycle struct {
	orchestration *nodeagent.RuntimeStartupOrchestration
	resources     *runtimeStartupResources
}

func (lifecycle *runtimeStartupLifecycle) Shutdown(ctx context.Context) error {
	if lifecycle == nil {
		return nil
	}
	if ctx == nil {
		return nodeagent.ErrRuntimeCallerIdentity
	}
	var shutdownErr error
	if lifecycle.orchestration != nil {
		shutdownErr = errors.Join(shutdownErr, lifecycle.orchestration.Shutdown(ctx))
	}
	if lifecycle.resources != nil {
		shutdownErr = errors.Join(shutdownErr, lifecycle.resources.Close())
	}
	return shutdownErr
}

func runRuntimeStartupGate(configuration config) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	launcher := runtimeStartupLauncherFactory(configuration)
	if _, unavailable := launcher.(unavailableRuntimeStartupLauncher); unavailable {
		return errRuntimeStartupLauncherUnavailable
	}
	resources, err := loadRuntimeStartupResources(ctx, configuration)
	if err != nil {
		return err
	}
	lifecycle, err := composeRuntimeStartupAuthority(ctx, configuration, resources, launcher)
	if err != nil {
		_ = resources.Close()
		return err
	}
	serveErr := lifecycle.orchestration.ServeCaller(ctx)
	if serveErr == nil {
		// The startup socket exchange is one-shot; a successful Permit does not
		// end the Runtime lifetime. Keep custody and journal observation alive
		// until the monitored original process exits or Node is canceled.
		serveErr = lifecycle.orchestration.Wait(ctx)
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	return errors.Join(serveErr, lifecycle.Shutdown(shutdownCtx))
}

// composeRuntimeStartupAuthority assembles the final authority after the
// launcher has handed Node its original pidfds and observer channel. Keeping
// this operation separate makes the ownership boundary testable without
// allowing the command to manufacture authority-bearing objects.
func composeRuntimeStartupAuthority(ctx context.Context, configuration config, resources *runtimeStartupResources, launcher runtimeStartupLauncher) (*runtimeStartupLifecycle, error) {
	if ctx == nil || resources == nil || launcher == nil || resources.plan == nil || resources.observer == nil || resources.socket == nil {
		return nil, nodeagent.ErrRuntimeStartupAuthority
	}
	launch, err := launcher.Launch(ctx, resources.plan, resources.socket.path)
	if err != nil {
		return nil, err
	}
	cleanup := func() {
		if launch.ObserverConn != nil {
			_ = launch.ObserverConn.Close()
		}
		if launch.ObserverPIDFD != nil {
			_ = launch.ObserverPIDFD.Close()
		}
		if launch.WorkerOwnerPIDFD != nil {
			_ = launch.WorkerOwnerPIDFD.Close()
		}
		if launch.LauncherPIDFD != nil {
			_ = launch.LauncherPIDFD.Close()
		}
		if launch.Close != nil {
			_ = launch.Close()
			launch.Close = nil
		}
	}
	if launch.WorkerOwnerPIDFD == nil || launch.ObserverPIDFD == nil || launch.LauncherPIDFD == nil || launch.ObserverConn == nil || launch.Policy == nil {
		cleanup()
		return nil, nodeagent.ErrRuntimeStartupAuthority
	}
	custody, err := nodeagent.ReceiveRuntimeObserverCustodyFromCreator(ctx, launch.ObserverConn, launch.ObserverPIDFD, launch.LauncherPIDFD)
	if err != nil {
		cleanup()
		return nil, err
	}
	if err := custody.Start(ctx); err != nil {
		_ = custody.Close()
		cleanup()
		return nil, err
	}
	caller, err := receiveRuntimeStartupCaller(ctx, resources.socket, resources.plan)
	if err != nil {
		_ = custody.Close()
		cleanup()
		return nil, err
	}
	credentials, err := resources.plan.CallerCredentials()
	if err != nil {
		_ = caller.Close()
		_ = custody.Close()
		cleanup()
		return nil, err
	}
	// Bind the helper's returned Runtime target to the authenticated caller
	// before any reservation. The later Kubernetes/CRI observation is still
	// required, but it must not be the first place where the helper's target is
	// compared; otherwise a helper could return an unrelated valid target while
	// the caller happened to match a different Pod.
	expectedPod := resources.plan.ExpectedPod()
	if expectedPod == nil {
		_ = caller.Close()
		_ = custody.Close()
		cleanup()
		return nil, nodeagent.ErrRuntimeLaunchPlan
	}
	expectedPodUID, podUIDErr := uuid.Parse(string(expectedPod.GetUID()))
	runtimeObservation, err := resources.observer.ObserveCaller(ctx, launch.Target, caller)
	if podUIDErr != nil || expectedPodUID == uuid.Nil || launch.Target.PodUID != expectedPodUID || launch.Target.PodNamespace != expectedPod.Namespace || launch.Target.PodName != expectedPod.Name || launch.Target.ContainerName != "model-runtime" || launch.Target.ContainerAttempt == 0 || err != nil || runtimeObservation.Process.UID != credentials.UID || runtimeObservation.Process.GID != credentials.GID {
		_ = caller.Close()
		_ = custody.Close()
		cleanup()
		return nil, errors.Join(nodeagent.ErrRuntimeLaunchPlan, err)
	}
	if err := nodeagent.ValidateRuntimeWorkerOwnerPIDFD(ctx, caller, launch.WorkerOwnerPIDFD); err != nil {
		_ = caller.Close()
		_ = custody.Close()
		cleanup()
		return nil, err
	}
	if err := custody.MatchCaller(ctx, caller); err != nil {
		_ = caller.Close()
		_ = custody.Close()
		cleanup()
		return nil, err
	}
	workerOwner, err := resources.observer.RetainNamespaceOwnerFromPIDFD(ctx, launch.WorkerTarget, launch.WorkerOwnerPIDFD, credentials)
	if err != nil {
		_ = caller.Close()
		_ = custody.Close()
		cleanup()
		return nil, err
	}
	resources.workerOwner, resources.custody = workerOwner, custody
	resources.launcherClose = launch.Close
	// The helper pidfd is only needed to bind observer ancestry during custody
	// receipt. The control channel remains the helper lifetime owner after this
	// point, so release the extra Node-side handle explicitly.
	_ = launch.LauncherPIDFD.Close()
	launch.ObserverConn, launch.ObserverPIDFD, launch.WorkerOwnerPIDFD, launch.LauncherPIDFD = nil, nil, nil, nil
	launch.Close = nil
	authority, err := newRuntimeStartupAuthority(configuration, resources.plan, nodeagent.RuntimeStartupAuthorityConfig{
		Ledger: resources.ledger, Plan: resources.plan, Pods: resources.pods, Observer: resources.observer,
		Custody: custody, Journal: resources.journal, WorkerOwner: workerOwner, Registry: resources.registry,
		AuthorizationPolicy: launch.Policy, Credentials: []nodeagent.RuntimeCallerCredentials{credentials},
		ObserverInterval: 250 * time.Millisecond, ObserverTimeout: 5 * time.Second, ExchangeTimeout: 30 * time.Second,
	})
	if err != nil {
		_ = caller.Close()
		return nil, err
	}
	orchestration, _, err := authority.Prepare(ctx, caller)
	if err != nil {
		_ = caller.Close()
		return nil, err
	}
	return &runtimeStartupLifecycle{orchestration: orchestration, resources: resources}, nil
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
	validator, err := loadRuntimeStageAuthorityValidator(configuration.runtimeStageVerifierFile)
	if err != nil {
		return nil, err
	}
	journal, err := loadRuntimeJournalOwner(configuration, plan, validator)
	if err != nil {
		return nil, err
	}
	ledger, err := nodeagent.OpenRuntimeStartupLedger(ctx, configuration.runtimeStartupLedgerDir, configuration.nodeIdentity, true)
	if err != nil {
		_ = journal.Close()
		return nil, fmt.Errorf("open runtime startup ledger: %w", err)
	}
	core, err := loadRuntimeKubernetesCore(configuration.runtimeKubeconfig)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		return nil, fmt.Errorf("load runtime Kubernetes API: %w", err)
	}
	pods, err := nodeagent.NewKubernetesRuntimeLaunchPodReader(core)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		return nil, fmt.Errorf("configure runtime Pod reader: %w", err)
	}
	observer, err := loadRuntimeContainerObserver(ctx, configuration)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		return nil, err
	}
	registry, registryClose, err := loadRuntimeStartupRegistry(ctx, configuration)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		_ = observer.Close()
		return nil, err
	}
	credentials, err := plan.CallerCredentials()
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		_ = registryClose()
		_ = observer.Close()
		return nil, fmt.Errorf("derive runtime startup socket credentials: %w", err)
	}
	socket, err := listenRuntimeStartupSocketWithGID(configuration, credentials.GID)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		_ = registryClose()
		_ = observer.Close()
		return nil, err
	}
	return &runtimeStartupResources{plan: plan, validator: validator, ledger: ledger, journal: journal, pods: pods, observer: observer, registry: registry, registryClose: registryClose, socket: socket}, nil
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
	if resources.custody != nil {
		closeErr = errors.Join(closeErr, resources.custody.Close())
	}
	if resources.workerOwner != nil {
		closeErr = errors.Join(closeErr, resources.workerOwner.Close())
	}
	if resources.launcherClose != nil {
		closeErr = errors.Join(closeErr, resources.launcherClose())
		resources.launcherClose = nil
	}
	if resources.observer != nil {
		closeErr = errors.Join(closeErr, resources.observer.Close())
	}
	if resources.journal != nil {
		closeErr = errors.Join(closeErr, resources.journal.Close())
	}
	if resources.ledger != nil {
		closeErr = errors.Join(closeErr, resources.ledger.Close())
	}
	return closeErr
}

func listenRuntimeStartupSocket(configuration config) (*runtimeStartupSocket, error) {
	return listenRuntimeStartupSocketWithGID(configuration, 0)
}

// listenRuntimeStartupSocketWithGID publishes the protected endpoint for the
// exact non-root Runtime identity from the verified plan. A zero GID is only
// supported by focused tests and retains the stricter root-only 0600 mode.
func listenRuntimeStartupSocketWithGID(configuration config, runtimeGID uint32) (*runtimeStartupSocket, error) {
	if !configuration.runtimeStartupEnabled {
		return nil, errors.New("runtime startup is disabled")
	}
	if runtimeGID == ^uint32(0) {
		return nil, errors.New("runtime startup socket GID is invalid")
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
	listener.SetUnlinkOnClose(true)
	cleanup := func() { _ = listener.Close(); _ = os.Remove(path) }
	mode := os.FileMode(0o600)
	if runtimeGID != 0 {
		mode = 0o660
		if os.Geteuid() != 0 {
			cleanup()
			return nil, errors.New("runtime startup socket GID publication requires root")
		}
		if err := os.Chown(path, 0, int(runtimeGID)); err != nil {
			cleanup()
			return nil, fmt.Errorf("publish runtime startup socket GID: %w", err)
		}
	}
	if err := os.Chmod(path, mode); err != nil {
		cleanup()
		return nil, fmt.Errorf("protect runtime startup socket: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		// The path may have been replaced after the listener was created. Close
		// the listener and let its unlink-on-close policy remove only the socket
		// it owns; never unlink an unverified replacement.
		_ = listener.Close()
		return nil, fmt.Errorf("inspect runtime startup socket: %w", err)
	}
	stat, statOK := info.Sys().(*syscall.Stat_t)
	if !statOK || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != mode.Perm() || stat.Uid != uint32(os.Geteuid()) || (runtimeGID != 0 && stat.Gid != runtimeGID) {
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

// receiveRuntimeStartupCaller performs the one caller handshake before any
// Fleet reservation. The returned RuntimeCaller retains the same connection
// and kernel pidfd for the subsequent authority Prepare/ServeCaller steps.
func receiveRuntimeStartupCaller(ctx context.Context, socket *runtimeStartupSocket, plan *nodeagent.RuntimeLaunchPlan) (*nodeagent.RuntimeCaller, error) {
	if ctx == nil || socket == nil || socket.listener == nil || plan == nil {
		return nil, nodeagent.ErrRuntimeCallerIdentity
	}
	credentials, err := plan.CallerCredentials()
	if err != nil {
		return nil, err
	}
	stop := context.AfterFunc(ctx, func() { _ = socket.listener.SetDeadline(time.Now()) })
	defer stop()
	connection, err := socket.listener.AcceptUnix()
	if err != nil {
		return nil, err
	}
	caller, err := nodeagent.ReceiveRuntimeCaller(ctx, connection, credentials)
	if err != nil {
		_ = connection.Close()
		return nil, err
	}
	return caller, nil
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
