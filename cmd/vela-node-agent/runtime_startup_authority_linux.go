package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleetcontroller"
	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/runtimelaunch"
	"github.com/vivym/vela/internal/runtimepolicy"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	"github.com/vivym/vela/internal/workerjournalwire"
	corev1 "k8s.io/api/core/v1"
	coreclient "k8s.io/client-go/kubernetes/typed/core/v1"
)

type runtimeStartupSocket struct {
	listener *net.UnixListener
	path     string
	identity os.FileInfo
}

// runtimeStartupResourceFactory contains only construction seams for external
// resources. The daemon uses runtimeStartupProductionResourceFactory; a
// validation harness may replace individual constructors with local fakes
// while still executing loadRuntimeStartupResources' ordering and cleanup.
// The production factory supplies the signed plan, validator, journal owner
// and ledger constructors. Validation may replace those constructors with
// disposable local implementations while the composition ordering remains
// identical. The Fleet client is represented by its narrow authority
// interface so validation can use an operation-recording fake.
type runtimeStartupResourceFactory struct {
	loadPlan              func(config) (*nodeagent.RuntimeLaunchPlan, error)
	loadValidator         func(string) (*stageauthority.Validator, error)
	loadJournal           func(config, *nodeagent.RuntimeLaunchPlan, *stageauthority.Validator) (*modelruntime.ExecutionJournalOwner, error)
	openLedger            func(context.Context, string, string, bool) (*nodeagent.RuntimeStartupLedger, error)
	loadKubernetesCore    func(string) (coreclient.CoreV1Interface, error)
	newPodReader          func(coreclient.CoreV1Interface) (nodeagent.RuntimeLaunchPodReader, error)
	loadContainerObserver func(context.Context, config) (*nodeagent.RuntimeContainerObserver, error)
	loadFleetRegistry     func(context.Context, config) (nodeagent.RuntimeStartupRegistry, func() error, error)
	listenStartupSocket   func(config, uint32) (*runtimeStartupSocket, error)
}

func runtimeStartupProductionResourceFactory() runtimeStartupResourceFactory {
	return runtimeStartupResourceFactory{
		loadPlan:           loadRuntimeStartupPlan,
		loadValidator:      loadRuntimeStageAuthorityValidator,
		loadJournal:        loadRuntimeJournalOwner,
		openLedger:         nodeagent.OpenRuntimeStartupLedger,
		loadKubernetesCore: loadRuntimeKubernetesCore,
		newPodReader: func(core coreclient.CoreV1Interface) (nodeagent.RuntimeLaunchPodReader, error) {
			return nodeagent.NewKubernetesRuntimeLaunchPodReader(core)
		},
		loadContainerObserver: loadRuntimeContainerObserver,
		loadFleetRegistry:     loadProductionFleetRegistry,
		listenStartupSocket:   listenRuntimeStartupSocketWithGID,
	}
}

func loadProductionFleetRegistry(ctx context.Context, configuration config) (nodeagent.RuntimeStartupRegistry, func() error, error) {
	return loadRuntimeStartupRegistry(ctx, configuration)
}

// runtimeStartupResources owns the concrete resources created by Node startup
// assembly. It intentionally stops short of constructing RuntimeStartupAuthority
// until journal ownership, observer custody and worker ownership are present.
type runtimeStartupResources struct {
	plan                         *nodeagent.RuntimeLaunchPlan
	validator                    *stageauthority.Validator
	ledger                       *nodeagent.RuntimeStartupLedger
	journal                      *modelruntime.ExecutionJournalOwner
	pods                         nodeagent.RuntimeLaunchPodReader
	observer                     *nodeagent.RuntimeContainerObserver
	registry                     nodeagent.RuntimeStartupRegistry
	registryClose                func() error
	socket                       *runtimeStartupSocket
	workerOwner                  *nodeagent.RuntimeNamespaceOwner
	custody                      *nodeagent.RuntimeObserverCustody
	stopCustodyHeartbeat         func()
	launcherClose                func() error
	workerInputJournal           *stageworkeragent.FileInputTransferJournal
	workerMaterializationJournal *stageworkeragent.FileMaterializationJournal
	workerJournalEndpoint        *nodeagent.WorkerJournalEndpoint
	workerJournalServer          *nodeagent.JournalServer
	workerJournalSocket          *runtimeStartupSocket
	workerJournalServeDone       chan error
	launcherCleanupVerify        func(context.Context) error
	retirementJournalID          uuid.UUID
	runtimeTarget                nodeagent.RuntimeContainerTarget
	workerTarget                 nodeagent.RuntimeContainerTarget
	runtimeOwner                 *nodeagent.RuntimeNamespaceOwner
	runtimeJournalEndpoint       *nodeagent.JournalEndpoint
	runtimeJournalServer         *nodeagent.JournalServer
	runtimeJournalSocket         *runtimeStartupSocket
	runtimeJournalServeDone      chan error
	runtimeBootstrap             *nodeagent.RuntimeBootstrapPublication
	registryVerifierKeys         map[string][]byte
	runtimePublication           *nodeagent.RuntimeStartupPublicationConfig
	image                        *nodeagent.RuntimeStartupImageConfig
	// authorizationPolicy is populated only by a validation composition
	// harness. Production assembly leaves it nil and uses the supervised issuer
	// through runtimeStartupPolicyFactory.
	authorizationPolicy nodeagent.RuntimeStartupAuthorizationPolicy
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

// runtimeStartupPolicyFactory is replaceable only by in-process validation
// harnesses. The daemon default always uses the separately supervised issuer;
// keeping the seam here lets a composition test exercise the command-level
// assembly without weakening the production policy boundary.
var runtimeStartupPolicyFactory = func(socketPath, publicKeyPath string) (nodeagent.RuntimeStartupAuthorizationPolicy, error) {
	return newExternalRuntimeStartupPolicy(socketPath, publicKeyPath)
}

// runtimeStartupBootstrapPublisher is replaceable only by validation tests.
// Production always uses publishRuntimeBootstrapBeforeLaunch; the seam lets
// command-level fault tests exercise launcher timeout/crash cleanup without
// manufacturing a journal or bootstrap receipt.
var runtimeStartupBootstrapPublisher = publishRuntimeBootstrapBeforeLaunch

// runtimeStartupLifecycle is the command-level shutdown owner. The
// orchestration revokes active routes and stops admission before the concrete
// resources are released, so signal handling cannot close custody underneath a
// live coordinator.
type runtimeStartupLifecycle struct {
	orchestration runtimeStartupOrchestration
	resources     *runtimeStartupResources
	// These immutable values are retained solely so the command boundary can
	// bind a persisted receipt to the exact records used during composition.
	reservation nodeagent.RuntimeStartupReservationRecord
	expected    modelruntime.BackendStartupRequest
}

// runtimeStartupOrchestration is the small lifecycle surface consumed by the
// command wrapper. Keeping it separate from the concrete implementation lets
// validation test the command success/error boundary without manufacturing a
// grant or bypassing the real authority implementation.
type runtimeStartupOrchestration interface {
	ServeCaller(context.Context) error
	CompositionReceipt(context.Context) (nodeagent.RuntimeStartupCompositionReceipt, error)
	Wait(context.Context) error
	Shutdown(context.Context) error
}

type runtimeStartupCompositionError struct {
	record nodeagent.RuntimeStartupReservationRecord
	err    error
}

func (err *runtimeStartupCompositionError) Error() string { return err.err.Error() }
func (err *runtimeStartupCompositionError) Unwrap() error { return err.err }

// CompositionReceipt exposes the same typed receipt at the command boundary.
// Keeping this forwarding method small ensures command-level harnesses inspect
// the receipt produced by the real orchestration rather than rebuilding it
// from launcher or CRI observations.
func (lifecycle *runtimeStartupLifecycle) CompositionReceipt(ctx context.Context) (nodeagent.RuntimeStartupCompositionReceipt, error) {
	if lifecycle == nil || lifecycle.orchestration == nil {
		return nodeagent.RuntimeStartupCompositionReceipt{}, nodeagent.ErrRuntimeStartupAuthority
	}
	return lifecycle.orchestration.CompositionReceipt(ctx)
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
	return runRuntimeStartupGateWithDependencies(configuration,
		func(configuration config) runtimeStartupLauncher { return runtimeStartupLauncherFactory(configuration) },
		loadRuntimeStartupResources,
		composeRuntimeStartupAuthority,
		waitRuntimeStartupWithReporting,
	)
}

type runtimeStartupResourceLoader func(context.Context, config) (*runtimeStartupResources, error)
type runtimeStartupAuthorityComposer func(context.Context, config, *runtimeStartupResources, runtimeStartupLauncher) (*runtimeStartupLifecycle, error)

func runRuntimeStartupGateWithDependencies(configuration config, launcherFactory func(config) runtimeStartupLauncher, loadResources runtimeStartupResourceLoader, composeAuthority runtimeStartupAuthorityComposer, wait func(context.Context, config, *runtimeStartupLifecycle) error) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runID := uuid.New()
	recordFailure := func(input nodeagent.RuntimeStartupFailureReceiptInput) error {
		input.RunID = runID
		receipt := nodeagent.NewRuntimeStartupFailureReceipt(input)
		return errors.Join(input.Cause, writeRuntimeStartupFailureReceipt(configuration.receiptDirectory, runID, receipt))
	}
	if launcherFactory == nil || loadResources == nil || composeAuthority == nil || wait == nil {
		return recordFailure(nodeagent.RuntimeStartupFailureReceiptInput{Phase: nodeagent.RuntimeStartupFailureResourceLoad, Cause: errors.New("runtime startup gate dependencies are incomplete"), CleanupVerified: true})
	}
	launcher := launcherFactory(configuration)
	if launcher == nil {
		return recordFailure(nodeagent.RuntimeStartupFailureReceiptInput{Phase: nodeagent.RuntimeStartupFailureLauncher, Cause: errRuntimeStartupLauncherUnavailable, CleanupVerified: true})
	}
	if _, unavailable := launcher.(unavailableRuntimeStartupLauncher); unavailable {
		return recordFailure(nodeagent.RuntimeStartupFailureReceiptInput{Phase: nodeagent.RuntimeStartupFailureLauncher, Cause: errRuntimeStartupLauncherUnavailable, CleanupVerified: true})
	}
	resources, err := loadResources(ctx, configuration)
	if err != nil {
		return recordFailure(nodeagent.RuntimeStartupFailureReceiptInput{Phase: nodeagent.RuntimeStartupFailureResourceLoad, Cause: err, CleanupVerified: true})
	}
	lifecycle, err := composeAuthority(ctx, configuration, resources, launcher)
	if err != nil {
		cleanupErr := resources.Close()
		input := nodeagent.RuntimeStartupFailureReceiptInput{Phase: nodeagent.RuntimeStartupFailureCompose, Cause: errors.Join(err, cleanupErr), CleanupVerified: cleanupErr == nil}
		var compositionFailure *runtimeStartupCompositionError
		if errors.As(err, &compositionFailure) && compositionFailure.record.OperationID != uuid.Nil {
			record := compositionFailure.record
			input.OperationID, input.JournalID, input.RequestDigest, input.ReservationCreated = record.OperationID, record.JournalID, record.RequestDigest, true
			if digest, digestErr := runtimepolicy.ReservationBindingDigest(record.OperationID, record.JournalID, record.RequestDigest, record.ReservedAt); digestErr == nil {
				input.ReservationDigest = digest
			}
		}
		return recordFailure(input)
	}
	if lifecycle == nil || lifecycle.orchestration == nil {
		var cleanupErr error
		if resources != nil {
			cleanupErr = resources.Close()
		}
		return recordFailure(nodeagent.RuntimeStartupFailureReceiptInput{
			Phase:           nodeagent.RuntimeStartupFailureCompose,
			Cause:           errors.Join(nodeagent.ErrRuntimeStartupAuthority, cleanupErr),
			CleanupVerified: cleanupErr == nil,
		})
	}
	compositionReceipt, serveErr := serveRuntimeStartupComposition(ctx, lifecycle)
	if serveErr == nil {
		// Persist the exact typed receipt before entering the long-lived wait.
		// A successful Permit without a durable receipt is not replayable evidence
		// and must therefore fail closed at the command boundary.
		if err := writeRuntimeStartupCompositionReceipt(configuration.receiptDirectory, compositionReceipt); err != nil {
			serveErr = fmt.Errorf("persist runtime startup composition receipt: %w", err)
		}
	}
	if serveErr == nil {
		// The startup socket exchange is one-shot; a successful Permit does not
		// end the Runtime lifetime. Keep custody and journal observation alive
		// until the monitored original process exits or Node is canceled.
		serveErr = wait(ctx, configuration, lifecycle)
	}
	// Snapshot the authority state before Shutdown revokes the live grant. A
	// wait/cleanup failure after a successful Permit must retain the fact that a
	// Permit was issued; reading only the post-shutdown receipt would turn that
	// historical fact into a false negative in the failure receipt.
	compositionBeforeShutdown := compositionReceipt
	var compositionBeforeShutdownErr error
	if compositionBeforeShutdown.OperationID == uuid.Nil {
		compositionBeforeShutdown, compositionBeforeShutdownErr = lifecycle.CompositionReceipt(context.Background())
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	shutdownErr := lifecycle.Shutdown(shutdownCtx)
	if serveErr != nil {
		composition := compositionBeforeShutdown
		if compositionBeforeShutdownErr != nil && composition.OperationID == uuid.Nil {
			return errors.Join(serveErr, shutdownErr, recordFailure(nodeagent.RuntimeStartupFailureReceiptInput{Phase: nodeagent.RuntimeStartupFailureBackend, Cause: compositionBeforeShutdownErr, CleanupVerified: shutdownErr == nil}))
		}
		return recordFailure(runtimeStartupFailureInputFromComposition(nodeagent.RuntimeStartupFailureBackend, serveErr, resources, composition, shutdownErr == nil))
	}
	if shutdownErr != nil {
		composition := compositionBeforeShutdown
		if compositionBeforeShutdownErr != nil && composition.OperationID == uuid.Nil {
			return recordFailure(nodeagent.RuntimeStartupFailureReceiptInput{Phase: nodeagent.RuntimeStartupFailureShutdown, Cause: shutdownErr})
		}
		return recordFailure(runtimeStartupFailureInputFromComposition(nodeagent.RuntimeStartupFailureShutdown, shutdownErr, resources, composition, false))
	}
	return shutdownErr
}

func runtimeStartupFailureInputFromComposition(phase nodeagent.RuntimeStartupFailurePhase, cause error, resources *runtimeStartupResources, composition nodeagent.RuntimeStartupCompositionReceipt, cleanupVerified bool) nodeagent.RuntimeStartupFailureReceiptInput {
	return nodeagent.RuntimeStartupFailureReceiptInput{
		Phase: phase, Cause: cause, OperationID: composition.OperationID, JournalID: composition.JournalID,
		RequestDigest:       composition.RequestDigest,
		ReservationDigest:   runtimeStartupReservationDigest(context.Background(), resources, composition),
		AuthorizationDigest: composition.AuthorizationDigest, GrantAttemptDigest: composition.GrantAttemptDigest,
		ReservationCreated: composition.OperationID != uuid.Nil,
		GrantCreated:       composition.GrantAttemptDigest != ([sha256.Size]byte{}),
		PermitIssued:       composition.Permit, CleanupVerified: cleanupVerified,
	}
}

// serveRuntimeStartupComposition runs the one-shot command-level exchange and
// returns the typed receipt produced by the orchestration. It deliberately
// excludes signal handling, receipt filesystem publication and lifetime wait;
// validation drivers can therefore exercise the exact success boundary with
// temporary resources while runRuntimeStartupGate retains production
// shutdown semantics.
func serveRuntimeStartupComposition(ctx context.Context, lifecycle *runtimeStartupLifecycle) (nodeagent.RuntimeStartupCompositionReceipt, error) {
	if ctx == nil || lifecycle == nil || lifecycle.orchestration == nil {
		return nodeagent.RuntimeStartupCompositionReceipt{}, nodeagent.ErrRuntimeStartupAuthority
	}
	if err := lifecycle.orchestration.ServeCaller(ctx); err != nil {
		return nodeagent.RuntimeStartupCompositionReceipt{}, err
	}
	if lifecycle.resources != nil && lifecycle.resources.workerJournalEndpoint != nil {
		if err := lifecycle.resources.workerJournalEndpoint.Activate(ctx); err != nil {
			return nodeagent.RuntimeStartupCompositionReceipt{}, err
		}
	}
	receipt, err := lifecycle.CompositionReceipt(ctx)
	if err != nil {
		return nodeagent.RuntimeStartupCompositionReceipt{}, err
	}
	if err := receipt.Verify(); err != nil {
		return nodeagent.RuntimeStartupCompositionReceipt{}, err
	}
	if !receipt.Permit || receipt.Outcome != "permitted" {
		return nodeagent.RuntimeStartupCompositionReceipt{}, errors.New("runtime startup did not produce a Permit")
	}
	return receipt, nil
}

func runtimeStartupReservationDigest(ctx context.Context, resources *runtimeStartupResources, receipt nodeagent.RuntimeStartupCompositionReceipt) [sha256.Size]byte {
	if receipt.ReservationDigest != ([sha256.Size]byte{}) || resources == nil || resources.ledger == nil || receipt.JournalID == uuid.Nil {
		return receipt.ReservationDigest
	}
	record, err := resources.ledger.InspectReservation(ctx, receipt.JournalID)
	if err != nil {
		return [sha256.Size]byte{}
	}
	digest, err := runtimepolicy.ReservationBindingDigest(record.OperationID, record.JournalID, record.RequestDigest, record.ReservedAt)
	if err != nil {
		return [sha256.Size]byte{}
	}
	return digest
}

func writeRuntimeStartupFailureReceipt(directory string, runID uuid.UUID, receipt nodeagent.RuntimeStartupFailureReceipt) error {
	if directory == "" || runID == uuid.Nil || receipt.RunID != runID {
		return errors.New("runtime startup failure receipt destination is invalid")
	}
	if err := receipt.Verify(); err != nil {
		return fmt.Errorf("validate runtime startup failure receipt: %w", err)
	}
	wire, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	// O_EXCL prevents an attacker from replacing a prior receipt. The receipt
	// directory is created and owned by the normal Node startup path before
	// this gate is entered.
	path := filepath.Join(directory, "runtime-startup-failure-"+runID.String()+".json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("write runtime startup failure receipt: %w", err)
	}
	_, writeErr := file.Write(wire)
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(writeErr, syncErr, closeErr)
}

func writeRuntimeStartupCompositionReceipt(directory string, receipt nodeagent.RuntimeStartupCompositionReceipt) error {
	if directory == "" || receipt.OperationID == uuid.Nil {
		return errors.New("runtime startup composition receipt destination is invalid")
	}
	if err := receipt.Verify(); err != nil {
		return fmt.Errorf("validate runtime startup composition receipt: %w", err)
	}
	if !receipt.Permit || receipt.Outcome != "permitted" {
		return errors.New("runtime startup composition receipt is not permitted")
	}
	wire, err := json.Marshal(receipt)
	if err != nil {
		return err
	}
	path := filepath.Join(directory, "runtime-startup-composition-"+receipt.OperationID.String()+".json")
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|syscall.O_NOFOLLOW, 0o600)
	if err != nil {
		return fmt.Errorf("write runtime startup composition receipt: %w", err)
	}
	_, writeErr := file.Write(wire)
	syncErr := file.Sync()
	closeErr := file.Close()
	return errors.Join(writeErr, syncErr, closeErr)
}

// composeRuntimeStartupAuthority assembles the final authority after the
// launcher has handed Node its original pidfds and observer channel. Keeping
// this operation separate makes the ownership boundary testable without
// allowing the command to manufacture authority-bearing objects.
func composeRuntimeStartupAuthority(ctx context.Context, configuration config, resources *runtimeStartupResources, launcher runtimeStartupLauncher) (*runtimeStartupLifecycle, error) {
	if ctx == nil || resources == nil || launcher == nil || resources.plan == nil || resources.observer == nil || resources.socket == nil {
		return nil, nodeagent.ErrRuntimeStartupAuthority
	}
	authorizationPolicy := resources.authorizationPolicy
	var err error
	if authorizationPolicy == nil {
		authorizationPolicy, err = runtimeStartupPolicyFactory(configuration.runtimePolicyIssuerSocket, configuration.runtimePolicyPublicKeyFile)
	}
	if err != nil {
		return nil, fmt.Errorf("configure external runtime startup policy: %w", err)
	}
	if _, err := securefile.ResolveTrustedDirectory(filepath.Dir(configuration.runtimePolicyAuthorizationPublicKeyFile)); err != nil {
		return nil, fmt.Errorf("validate Fleet authorization key directory: %w", err)
	}
	authorizationKeyWire, err := securefile.Read(configuration.runtimePolicyAuthorizationPublicKeyFile, ed25519.PublicKeySize, true)
	if err != nil || len(authorizationKeyWire) != ed25519.PublicKeySize {
		return nil, errors.New("fleet authorization public key is unavailable")
	}
	authorizationPublicKey := ed25519.PublicKey(authorizationKeyWire)
	policyAuthorizationPublisher := func(ctx context.Context, wire []byte) error {
		if len(wire) == 0 {
			return errors.New("fleet policy authorization is missing")
		}
		return runtimepolicy.PublishAuthorizationWire(ctx, configuration.runtimePolicyAuthorizationDirectory, wire, authorizationPublicKey, time.Now().UTC())
	}
	if err := runtimeStartupBootstrapPublisher(ctx, configuration, resources); err != nil {
		return nil, err
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
		if launch.RuntimePIDFD != nil {
			_ = launch.RuntimePIDFD.Close()
		}
		if launch.LauncherPIDFD != nil {
			_ = launch.LauncherPIDFD.Close()
		}
		if launch.Close != nil {
			_ = launch.Close()
			launch.Close = nil
		}
	}
	if launch.RuntimePIDFD == nil || launch.WorkerOwnerPIDFD == nil || launch.ObserverPIDFD == nil || launch.LauncherPIDFD == nil || launch.ObserverConn == nil {
		cleanup()
		return nil, nodeagent.ErrRuntimeStartupAuthority
	}
	credentials, err := resources.plan.CallerCredentials()
	if err != nil {
		cleanup()
		return nil, err
	}
	expectedPod := resources.plan.ExpectedPod()
	if expectedPod == nil {
		cleanup()
		return nil, nodeagent.ErrRuntimeLaunchPlan
	}
	if err := validateRuntimeStartupLaunchTargets(expectedPod, launch); err != nil {
		cleanup()
		return nil, err
	}
	runtimeOwner, err := resources.observer.RetainNamespaceOwnerFromPIDFD(ctx, launch.Target, launch.RuntimePIDFD, credentials)
	if err != nil {
		cleanup()
		return nil, err
	}
	workerOwner, err := resources.observer.RetainNamespaceOwnerFromPIDFD(ctx, launch.WorkerTarget, launch.WorkerOwnerPIDFD, credentials)
	if err != nil {
		_ = runtimeOwner.Close()
		cleanup()
		return nil, err
	}
	resources.runtimeOwner, resources.workerOwner = runtimeOwner, workerOwner
	resources.runtimeTarget, resources.workerTarget = launch.Target, launch.WorkerTarget
	resources.launcherCleanupVerify = func(checkCtx context.Context) error {
		return resources.observer.VerifyWorkloadAbsent(checkCtx, launch.Target, launch.WorkerTarget)
	}
	if err := startRuntimeJournalService(ctx, configuration, resources, runtimeOwner, workerOwner, credentials); err != nil {
		_ = runtimeOwner.Close()
		_ = workerOwner.Close()
		resources.runtimeOwner, resources.workerOwner = nil, nil
		cleanup()
		return nil, err
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
	// Keep the observer alive through caller/CRI inspection, reservation, and
	// the handoff to ongoing orchestration. Close joins it before releasing custody.
	resources.stopCustodyHeartbeat = maintainRuntimeStartupCustody(custody)
	caller, err := receiveRuntimeStartupCaller(ctx, resources.socket, resources.plan)
	if err != nil {
		_ = custody.Close()
		cleanup()
		return nil, err
	}
	credentials, err = resources.plan.CallerCredentials()
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
	expectedPod = resources.plan.ExpectedPod()
	if expectedPod == nil {
		_ = caller.Close()
		_ = custody.Close()
		cleanup()
		return nil, nodeagent.ErrRuntimeLaunchPlan
	}
	runtimeObservation, err := resources.observer.ObserveCaller(ctx, launch.Target, caller)
	if launch.Target.PodUID == uuid.Nil || launch.Target.ContainerName != "model-runtime" || err != nil || runtimeObservation.Process.UID != credentials.UID || runtimeObservation.Process.GID != credentials.GID {
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
	// The Worker target is authority-bearing too. Bind it to the same signed
	// Pod and sandbox before retaining its namespace owner; otherwise a helper
	// could return a valid process from another Pod on the same node.
	if launch.WorkerTarget.PodUID != launch.Target.PodUID ||
		launch.WorkerTarget.PodNamespace != expectedPod.Namespace ||
		launch.WorkerTarget.PodName != expectedPod.Name ||
		launch.WorkerTarget.ContainerName != "stage-worker-agent" ||
		launch.WorkerTarget.SandboxID != launch.Target.SandboxID {
		_ = caller.Close()
		// custody is still local until all launch targets have been bound;
		// resources.custody is intentionally nil at this point.
		_ = custody.Close()
		cleanup()
		return nil, nodeagent.ErrRuntimeLaunchPlan
	}
	if resources.workerOwner == nil {
		workerOwner, err = resources.observer.RetainNamespaceOwnerFromPIDFD(ctx, launch.WorkerTarget, launch.WorkerOwnerPIDFD, credentials)
	}
	if err != nil {
		_ = caller.Close()
		_ = custody.Close()
		cleanup()
		return nil, err
	}
	resources.workerOwner, resources.custody = workerOwner, custody
	if configuration.workerJournalSocket != "" {
		workerCredentials, credentialsErr := workerOwner.Credentials()
		if credentialsErr != nil {
			_ = caller.Close()
			_ = resources.workerOwner.Close()
			resources.workerOwner = nil
			_ = resources.custody.Close()
			resources.custody = nil
			cleanup()
			return nil, credentialsErr
		}
		if err := startWorkerJournalService(ctx, configuration, resources, workerOwner, workerCredentials); err != nil {
			_ = caller.Close()
			_ = resources.workerOwner.Close()
			resources.workerOwner = nil
			_ = resources.custody.Close()
			resources.custody = nil
			cleanup()
			return nil, err
		}
	}
	resources.launcherClose = launch.Close
	// The helper pidfd is only needed to bind observer ancestry during custody
	// receipt. The control channel remains the helper lifetime owner after this
	// point, so release the extra Node-side handle explicitly.
	_ = launch.LauncherPIDFD.Close()
	launch.ObserverConn, launch.ObserverPIDFD, launch.WorkerOwnerPIDFD, launch.RuntimePIDFD, launch.LauncherPIDFD = nil, nil, nil, nil, nil
	launch.Close = nil
	authority, err := newRuntimeStartupAuthority(configuration, resources.plan, nodeagent.RuntimeStartupAuthorityConfig{
		Ledger: resources.ledger, Plan: resources.plan, Pods: resources.pods, Observer: resources.observer,
		Image:   resources.image,
		Custody: custody, Journal: resources.journal, WorkerOwner: workerOwner, RuntimeOwner: resources.runtimeOwner, RuntimeJournalEndpoint: resources.runtimeJournalEndpoint, RuntimePublication: resources.runtimePublication, Registry: resources.registry,
		AuthorizationPolicy: authorizationPolicy, PolicyAuthorizationPublisher: policyAuthorizationPublisher, Credentials: []nodeagent.RuntimeCallerCredentials{credentials},
		ObserverInterval: 250 * time.Millisecond, ObserverTimeout: 5 * time.Second, ExchangeTimeout: 30 * time.Second,
	})
	if err != nil {
		_ = caller.Close()
		return nil, err
	}
	orchestration, record, err := authority.Prepare(ctx, caller)
	if err != nil {
		_ = caller.Close()
		resources.retirementJournalID = record.JournalID
		return nil, &runtimeStartupCompositionError{record: record, err: err}
	}
	expectedRequest, err := orchestration.ExpectedBackendStartupRequest()
	if err != nil {
		_ = orchestration.Close()
		_ = caller.Close()
		return nil, err
	}
	resources.retirementJournalID = record.JournalID
	return &runtimeStartupLifecycle{orchestration: orchestration, resources: resources, reservation: record, expected: expectedRequest}, nil
}

// validateRuntimeStartupLaunchTargets binds launcher-reported live identities
// to the signed Pod template. The template intentionally has no Kubernetes
// metadata.uid; that value is assigned when the Pod is created and is
// independently checked by the authenticated CRI/Kubernetes observation.
func validateRuntimeStartupLaunchTargets(expectedPod *corev1.Pod, launch runtimeStartupLaunch) error {
	if expectedPod == nil || launch.Target.PodUID == uuid.Nil || launch.WorkerTarget.PodUID == uuid.Nil ||
		launch.Target.PodNamespace != expectedPod.Namespace || launch.Target.PodName != expectedPod.Name ||
		launch.Target.ContainerName != "model-runtime" ||
		launch.WorkerTarget.PodUID != launch.Target.PodUID || launch.WorkerTarget.PodNamespace != expectedPod.Namespace ||
		launch.WorkerTarget.PodName != expectedPod.Name || launch.WorkerTarget.ContainerName != "stage-worker-agent" ||
		launch.WorkerTarget.SandboxID != launch.Target.SandboxID {
		return nodeagent.ErrRuntimeLaunchPlan
	}
	return nil
}

func publishRuntimeBootstrapBeforeLaunch(ctx context.Context, configuration config, resources *runtimeStartupResources) error {
	if resources == nil || resources.plan == nil || resources.journal == nil {
		return nodeagent.ErrRuntimeStartupAuthority
	}
	credentials, err := resources.plan.CallerCredentials()
	if err != nil {
		return err
	}
	socket, err := listenRuntimeStartupSocketAt(configuration, filepath.Join(filepath.Dir(configuration.runtimeStartupSocket), "runtime-journal.sock"), credentials.GID)
	if err != nil {
		return err
	}
	resources.runtimeJournalSocket = socket
	pod := resources.plan.ExpectedPod()
	if pod == nil {
		return nodeagent.ErrRuntimeLaunchPlan
	}
	var runtimeContainer *corev1.Container
	for i := range pod.Spec.Containers {
		if pod.Spec.Containers[i].Name == "model-runtime" {
			runtimeContainer = &pod.Spec.Containers[i]
			break
		}
	}
	bootstrapPath, bootstrapErr := runtimeContainerBootstrapPath(runtimeContainer)
	if runtimeContainer == nil || bootstrapErr != nil {
		return nodeagent.ErrRuntimeLaunchPlan
	}
	// Verify the authenticated Kubernetes reader before recording the
	// irreversible backend startup intent. A missing RBAC grant, deleted Pod,
	// or incomplete Pod identity is a preflight failure and must remain
	// retryable; it must not poison the journal with an unresolved incarnation.
	expectedPod := resources.plan.ExpectedPod()
	if expectedPod == nil {
		return nodeagent.ErrRuntimeLaunchPlan
	}
	if _, err := resources.pods.GetWorkerInstancePod(ctx, fleetcontroller.ResourceKey{
		Namespace: expectedPod.Namespace,
		Name:      expectedPod.Name,
	}); err != nil {
		return fmt.Errorf("preflight authenticated Fleet-created Pod: %w", err)
	}
	if pod.Annotations[runtimelaunch.ProtocolAnnotation] == runtimelaunch.Protocol {
		// A CRI pull can leave only overlay snapshots. Verify the independently
		// prepared native image view before consuming a backend incarnation.
		_, digest, pinned := strings.Cut(runtimeContainer.Image, "@")
		if !pinned || resources.image == nil || resources.image.Images == nil {
			return errors.New("preflight requires a pinned runtime image observer")
		}
		if _, err := resources.image.Images.InspectLaunch(ctx, digest); err != nil {
			return fmt.Errorf("preflight native runtime image (prepare its native snapshot before starting): %w", err)
		}
		for _, name := range []string{"launch", "worker-bootstrap"} {
			path := filepath.Join(filepath.Dir(configuration.runtimeStartupSocket), name)
			if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
				return fmt.Errorf("preflight requires unused startup directory %s: %w", path, errors.Join(errors.New("path already exists or cannot be inspected"), err))
			}
		}
	}
	runtimeSocket, brokerSocket := "", ""
	if pod.Annotations[runtimelaunch.ProtocolAnnotation] == runtimelaunch.Protocol {
		runtimeSocket = "/run/vela-model-runtime/private/runtime.sock"
		brokerSocket = runtimelaunch.BrokerSocket
	}
	for _, env := range runtimeContainer.Env {
		if env.ValueFrom == nil {
			if env.Name == "VELA_MODEL_RUNTIME_SOCKET" {
				runtimeSocket = env.Value
			}
			if env.Name == "VELA_MODEL_RUNTIME_PIDFD_BROKER_SOCKET" {
				brokerSocket = env.Value
			}
		}
	}
	if runtimeSocket == "" || !filepath.IsAbs(runtimeSocket) || filepath.Clean(runtimeSocket) != runtimeSocket {
		return nodeagent.ErrRuntimeLaunchPlan
	}
	publicationDir := filepath.Join(filepath.Dir(configuration.runtimeStartupSocket), "runtime-bootstrap")
	if err := os.MkdirAll(publicationDir, 0o700); err != nil {
		return err
	}
	if err := os.Chown(publicationDir, 0, 0); err != nil {
		return err
	}
	if err := os.Chmod(publicationDir, 0o700); err != nil {
		return err
	}
	resources.runtimePublication = &nodeagent.RuntimeStartupPublicationConfig{Directory: publicationDir, BootstrapPath: bootstrapPath}
	if _, err := resources.journal.RecordBackendStartupIntent(ctx); err != nil {
		return fmt.Errorf("record runtime startup intent: %w", err)
	}
	publication, err := nodeagent.PublishRuntimeBootstrap(ctx, nodeagent.RuntimeBootstrapPublicationConfig{Directory: publicationDir, Plan: resources.plan, Journal: resources.journal, RegistryKeys: resources.registryVerifierKeys, JournalSocket: "/run/vela-node/runtime-journal.sock", StartupSocket: filepath.Join("/run/vela-node", filepath.Base(configuration.runtimeStartupSocket)), RuntimeSocket: runtimeSocket, PIDFDBrokerSocket: brokerSocket, JournalTimeout: 30 * time.Second, CancelTimeout: time.Second, ShutdownTimeout: 20 * time.Second})
	if err != nil {
		return fmt.Errorf("publish runtime bootstrap: %w", err)
	}
	resources.runtimeBootstrap = publication
	if pod.Annotations[runtimelaunch.ProtocolAnnotation] == runtimelaunch.Protocol {
		if err := publishKubernetesWorkerBootstrap(configuration, resources, credentials); err != nil {
			return err
		}
	}
	return nil
}

func publishKubernetesWorkerBootstrap(configuration config, resources *runtimeStartupResources, credentials nodeagent.RuntimeCallerCredentials) error {
	binding, err := journalbinding.Encode(resources.plan.RegistryBinding())
	if err != nil {
		return err
	}
	keys, err := json.Marshal(resources.registryVerifierKeys)
	if err != nil {
		return err
	}
	root := filepath.Join(filepath.Dir(configuration.runtimeStartupSocket), "worker-bootstrap")
	if err := os.Mkdir(root, 0o700); err != nil {
		return fmt.Errorf("create fresh Worker bootstrap publication: %w", err)
	}
	for _, entry := range []struct {
		name string
		wire []byte
	}{{"binding.json", binding}, {"verifier.json", keys}} {
		file, err := os.OpenFile(filepath.Join(root, entry.name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o400)
		if err != nil {
			return err
		}
		_, writeErr := file.Write(entry.wire)
		err = errors.Join(writeErr, file.Chown(0, int(credentials.GID)), file.Chmod(0o440), file.Sync(), file.Close())
		if err != nil {
			return err
		}
	}
	if err := os.Chown(root, 0, int(credentials.GID)); err != nil {
		return err
	}
	return os.Chmod(root, 0o750)
}

// runtimeContainerBootstrapPath validates the effective OCI argv. Kubernetes
// stores the executable in Command and passes only the remaining arguments in
// Args; CRI then concatenates both when creating the task. Keeping this
// normalization in one place prevents the Node contract from requiring an
// argv[0] duplicate in the signed Pod Args field.
func runtimeContainerBootstrapPath(container *corev1.Container) (string, error) {
	if container == nil {
		return "", nodeagent.ErrRuntimeLaunchPlan
	}
	if len(container.Command) == 0 && len(container.Args) == 0 {
		// The Kubernetes image contract fixes both pre-exec and Runtime argv.
		// Actual image content/defaults are independently checked before grant.
		return runtimelaunch.Bootstrap, nil
	}
	command := append([]string(nil), container.Command...)
	if len(command) == 0 {
		command = []string{"/usr/local/bin/vela-model-runtime"}
	}
	argv := append(command, container.Args...)
	if len(argv) != 4 || argv[0] != "/usr/local/bin/vela-model-runtime" || argv[1] != "serve-remote" || argv[2] != "--bootstrap-file" || !filepath.IsAbs(argv[3]) || filepath.Clean(argv[3]) != argv[3] {
		return "", nodeagent.ErrRuntimeLaunchPlan
	}
	return argv[3], nil
}

func startWorkerJournalService(ctx context.Context, configuration config, resources *runtimeStartupResources, workerOwner *nodeagent.RuntimeNamespaceOwner, credentials nodeagent.RuntimeCallerCredentials) error {
	if ctx == nil || resources == nil || resources.plan == nil || workerOwner == nil || credentials.UID == 0 || credentials.GID == 0 {
		return nodeagent.ErrRuntimeStartupAuthority
	}
	binding := resources.plan.RegistryBinding()
	if binding == nil || binding.GetPair() == nil {
		return errors.New("worker journal service requires verified Registry journal binding")
	}
	journalID, err := uuid.Parse(binding.GetPair().GetWorkerJournalId())
	if err != nil || journalID == uuid.Nil || len(binding.GetPair().GetWorkerScope()) != 32 {
		return errors.New("worker journal service binding identity is invalid")
	}
	var scope [32]byte
	copy(scope[:], binding.GetPair().GetWorkerScope())
	if scope == ([32]byte{}) {
		return errors.New("worker journal service binding scope is empty")
	}
	expectedPod := resources.plan.ExpectedPod()
	if expectedPod == nil ||
		!podEnvironmentEquals(expectedPod, "stage-worker-agent", "VELA_WORKER_JOURNAL_SOCKET", configuration.workerJournalSocket) ||
		!podEnvironmentEquals(expectedPod, "stage-worker-agent", "VELA_WORKER_JOURNAL_PIDFD_BROKER_SOCKET", configuration.workerJournalPIDFDBrokerSocket) {
		return errors.New("signed Worker Pod does not bind the configured Worker journal or pidfd broker socket")
	}
	input, err := stageworkeragent.NewFileInputTransferJournal(configuration.workerJournalInputRoot)
	if err != nil {
		return fmt.Errorf("open root-owned Worker input journal: %w", err)
	}
	materialization, err := stageworkeragent.NewFileMaterializationJournal(configuration.workerJournalMaterializationRoot, configuration.workerJournalMaterializationLimit)
	if err != nil {
		_ = input.Close()
		return fmt.Errorf("open root-owned Worker materialization journal: %w", err)
	}
	endpoint, err := nodeagent.NewWorkerJournalEndpoint(ctx, nodeagent.WorkerJournalEndpointConfig{
		Input: input, Materialization: materialization, WorkerOwner: workerOwner,
		Identity:          workerjournalwire.Identity{JournalID: journalID, Scope: scope},
		PIDFDBrokerSocket: configuration.workerJournalPIDFDBrokerSocket,
	})
	if err != nil {
		_ = materialization.Close()
		_ = input.Close()
		return fmt.Errorf("bind Worker journal endpoint: %w", err)
	}
	socket, err := listenRuntimeStartupSocketAt(configuration, configuration.workerJournalSocket, credentials.GID)
	if err != nil {
		_ = endpoint.Close()
		_ = materialization.Close()
		_ = input.Close()
		return fmt.Errorf("listen Worker journal socket: %w", err)
	}
	server, err := nodeagent.NewJournalServerForHandler(endpoint, nodeagent.JournalServerConfig{
		Credentials: []nodeagent.RuntimeCallerCredentials{credentials}, MaxConcurrent: 8, ExchangeTimeout: 45 * time.Second,
	})
	if err != nil {
		_ = socket.Close()
		_ = endpoint.Close()
		_ = materialization.Close()
		_ = input.Close()
		return fmt.Errorf("configure Worker journal server: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, socket.listener) }()
	resources.workerInputJournal, resources.workerMaterializationJournal = input, materialization
	resources.workerJournalEndpoint, resources.workerJournalSocket = endpoint, socket
	resources.workerJournalServer, resources.workerJournalServeDone = server, done
	return nil
}

// startRuntimeJournalService publishes the read-only endpoint consumed by
// serve-remote before its startup handshake. The endpoint is later activated
// by Prepare on this same server after Fleet authorization.
func startRuntimeJournalService(ctx context.Context, configuration config, resources *runtimeStartupResources, runtimeOwner, workerOwner *nodeagent.RuntimeNamespaceOwner, credentials nodeagent.RuntimeCallerCredentials) error {
	if ctx == nil || resources == nil || resources.journal == nil || runtimeOwner == nil || workerOwner == nil {
		return nodeagent.ErrRuntimeStartupAuthority
	}
	dir := filepath.Dir(configuration.runtimeStartupSocket)
	journalPath := filepath.Join(dir, "runtime-journal.sock")
	endpoint, err := nodeagent.NewReadOnlyJournalEndpoint(ctx, resources.journal, runtimeOwner, workerOwner)
	if err != nil {
		return fmt.Errorf("bind runtime journal endpoint: %w", err)
	}
	socket := resources.runtimeJournalSocket
	if socket == nil {
		socket, err = listenRuntimeStartupSocketAt(configuration, journalPath, credentials.GID)
		if err != nil {
			_ = endpoint.Close()
			return fmt.Errorf("listen runtime journal socket: %w", err)
		}
	}
	server, err := nodeagent.NewJournalServer(endpoint, nodeagent.JournalServerConfig{Credentials: []nodeagent.RuntimeCallerCredentials{credentials}, MaxConcurrent: 8, ExchangeTimeout: 45 * time.Second})
	if err != nil {
		_ = socket.Close()
		_ = endpoint.Close()
		return fmt.Errorf("configure runtime journal server: %w", err)
	}
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, socket.listener) }()
	resources.runtimeJournalEndpoint, resources.runtimeJournalServer, resources.runtimeJournalSocket, resources.runtimeJournalServeDone = endpoint, server, socket, done
	return nil
}

func podEnvironmentEquals(pod *corev1.Pod, containerName, key, expected string) bool {
	if pod == nil || key == "" || expected == "" {
		return false
	}
	for _, container := range pod.Spec.Containers {
		if container.Name != containerName {
			continue
		}
		for _, env := range container.Env {
			if env.Name == key && env.ValueFrom == nil {
				return env.Value == expected
			}
		}
	}
	return false
}

func loadRuntimeStageAuthorityValidator(path string) (*stageauthority.Validator, error) {
	keys, err := stageauthority.ReadVerifierKeyringFile(path)
	if err != nil {
		return nil, fmt.Errorf("load runtime StageAuthority verifier: %w", err)
	}
	defer stageauthority.ClearKeyring(keys)
	validator, err := stageauthority.NewVerifier(keys, time.Now)
	if err != nil {
		return nil, fmt.Errorf("configure runtime StageAuthority verifier: %w", err)
	}
	return validator, nil
}

func loadRuntimeJournalOwner(configuration config, plan *nodeagent.RuntimeLaunchPlan, validator *stageauthority.Validator) (*modelruntime.ExecutionJournalOwner, error) {
	if plan == nil || validator == nil || configuration.runtimeJournalStateDir == "" {
		return nil, errors.New("runtime journal owner sources are incomplete")
	}
	manifest, err := plan.LaunchManifest()
	if err != nil {
		return nil, fmt.Errorf("read verified runtime launch manifest: %w", err)
	}
	routes, err := modelruntime.RemoteStartupBindings(manifest)
	if err != nil {
		return nil, fmt.Errorf("derive runtime journal routes: %w", err)
	}
	owner, err := modelruntime.OpenExecutionJournalOwner(modelruntime.ExecutionJournalOwnerConfig{
		Manifest: manifest, Validator: validator,
		State:  modelruntime.ExecutionFloorStateConfig{Directory: configuration.runtimeJournalStateDir},
		Routes: routes, MaxClockSkew: 30 * time.Second, Now: time.Now,
	})
	if err != nil {
		return nil, fmt.Errorf("open runtime execution journal owner: %w", err)
	}
	return owner, nil
}

func loadRuntimeStartupResources(ctx context.Context, configuration config) (*runtimeStartupResources, error) {
	resources, err := loadRuntimeStartupResourcesWithFactory(ctx, configuration, runtimeStartupProductionResourceFactory())
	if err != nil {
		return nil, err
	}
	if resources.plan.ExpectedPod().Annotations[runtimelaunch.ProtocolAnnotation] == runtimelaunch.Protocol {
		resources.image, err = loadRuntimeStartupImage(ctx, configuration)
		if err != nil {
			return nil, errors.Join(err, resources.Close())
		}
	}
	return resources, nil
}

// loadRuntimeStartupResourcesWithFactory is the single composition-root
// implementation. Keeping the production wrapper above tiny makes it
// possible for validation to exercise the exact same dependency ordering,
// ownership transfers and failure cleanup without replacing the authority
// itself.
func loadRuntimeStartupResourcesWithFactory(ctx context.Context, configuration config, factory runtimeStartupResourceFactory) (*runtimeStartupResources, error) {
	if !configuration.runtimeStartupEnabled {
		return nil, errors.New("runtime startup is disabled")
	}
	if ctx == nil {
		return nil, errors.New("runtime startup resource context is required")
	}
	if factory.loadPlan == nil || factory.loadValidator == nil || factory.loadJournal == nil || factory.openLedger == nil || factory.loadKubernetesCore == nil || factory.newPodReader == nil || factory.loadContainerObserver == nil || factory.loadFleetRegistry == nil || factory.listenStartupSocket == nil {
		return nil, errors.New("runtime startup resource factory is incomplete")
	}
	plan, err := factory.loadPlan(configuration)
	if err != nil {
		return nil, err
	}
	if plan == nil {
		return nil, errors.New("load runtime launch plan: factory returned nil plan")
	}
	validator, err := factory.loadValidator(configuration.runtimeStageVerifierFile)
	if err != nil {
		return nil, err
	}
	if validator == nil {
		return nil, errors.New("load runtime StageAuthority verifier: factory returned nil validator")
	}
	registryKeys, keyErr := stageauthority.ReadVerifierKeyringFile(configuration.runtimeBindingVerifierFile)
	if keyErr != nil {
		return nil, fmt.Errorf("load runtime Registry verifier keys: %w", keyErr)
	}
	registryVerifierKeys := make(map[string][]byte, len(registryKeys))
	for id, key := range registryKeys {
		registryVerifierKeys[id] = append([]byte(nil), key...)
	}
	stageauthority.ClearKeyring(registryKeys)
	journal, err := factory.loadJournal(configuration, plan, validator)
	if err != nil {
		return nil, err
	}
	if journal == nil {
		return nil, errors.New("open runtime execution journal: factory returned nil owner")
	}
	// Initialization is only valid for the first process that owns this
	// per-node ledger.  A systemd restart must reopen the existing append-only
	// journal; passing initialize=true would (correctly) reject the non-empty
	// directory and create a restart storm after any startup failure.
	initializeLedger := true
	ledgerPath := filepath.Join(configuration.runtimeStartupLedgerDir, "runtime-startups.jsonl")
	if _, statErr := os.Stat(ledgerPath); statErr == nil {
		initializeLedger = false
	} else if !errors.Is(statErr, os.ErrNotExist) {
		return nil, fmt.Errorf("inspect runtime startup ledger: %w", statErr)
	}
	ledger, err := factory.openLedger(ctx, configuration.runtimeStartupLedgerDir, configuration.nodeIdentity, initializeLedger)
	if err != nil {
		_ = journal.Close()
		return nil, fmt.Errorf("open runtime startup ledger: %w", err)
	}
	core, err := factory.loadKubernetesCore(configuration.runtimeKubeconfig)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		return nil, fmt.Errorf("load runtime Kubernetes API: %w", err)
	}
	if core == nil {
		_ = ledger.Close()
		_ = journal.Close()
		return nil, errors.New("load runtime Kubernetes API: factory returned nil client")
	}
	pods, err := factory.newPodReader(core)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		return nil, fmt.Errorf("configure runtime Pod reader: %w", err)
	}
	if pods == nil {
		_ = ledger.Close()
		_ = journal.Close()
		return nil, errors.New("configure runtime Pod reader: factory returned nil reader")
	}
	observer, err := factory.loadContainerObserver(ctx, configuration)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		return nil, err
	}
	if observer == nil {
		_ = ledger.Close()
		_ = journal.Close()
		return nil, errors.New("load runtime CRI observer: factory returned nil observer")
	}
	registry, registryClose, err := factory.loadFleetRegistry(ctx, configuration)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		_ = observer.Close()
		return nil, err
	}
	if registry == nil || registryClose == nil {
		_ = ledger.Close()
		_ = journal.Close()
		_ = observer.Close()
		if registryClose != nil {
			_ = registryClose()
		}
		return nil, errors.New("connect runtime startup Fleet registry: factory returned incomplete client")
	}
	credentials, err := plan.CallerCredentials()
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		_ = registryClose()
		_ = observer.Close()
		return nil, fmt.Errorf("derive runtime startup socket credentials: %w", err)
	}
	socket, err := factory.listenStartupSocket(configuration, credentials.GID)
	if err != nil {
		_ = ledger.Close()
		_ = journal.Close()
		_ = registryClose()
		_ = observer.Close()
		return nil, err
	}
	if socket == nil {
		_ = ledger.Close()
		_ = journal.Close()
		_ = registryClose()
		_ = observer.Close()
		return nil, errors.New("listen runtime startup socket: factory returned nil socket")
	}
	return &runtimeStartupResources{plan: plan, validator: validator, ledger: ledger, journal: journal, pods: pods, observer: observer, registry: registry, registryClose: registryClose, socket: socket, registryVerifierKeys: registryVerifierKeys}, nil
}

func (resources *runtimeStartupResources) Close() error {
	if resources == nil {
		return nil
	}
	var closeErr error
	if resources.socket != nil {
		closeErr = errors.Join(closeErr, resources.socket.Close())
		resources.socket = nil
	}
	if resources.workerJournalServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		closeErr = errors.Join(closeErr, resources.workerJournalServer.Shutdown(shutdownCtx))
		cancel()
		resources.workerJournalServer = nil
	}
	if resources.workerJournalServeDone != nil {
		closeErr = errors.Join(closeErr, <-resources.workerJournalServeDone)
		resources.workerJournalServeDone = nil
	}
	if resources.runtimeJournalServer != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		closeErr = errors.Join(closeErr, resources.runtimeJournalServer.Shutdown(shutdownCtx))
		cancel()
		resources.runtimeJournalServer = nil
	}
	if resources.runtimeJournalServeDone != nil {
		closeErr = errors.Join(closeErr, <-resources.runtimeJournalServeDone)
		resources.runtimeJournalServeDone = nil
	}
	if resources.runtimeJournalEndpoint != nil {
		closeErr = errors.Join(closeErr, resources.runtimeJournalEndpoint.Close())
		resources.runtimeJournalEndpoint = nil
	}
	if resources.runtimeJournalSocket != nil {
		closeErr = errors.Join(closeErr, resources.runtimeJournalSocket.Close())
		resources.runtimeJournalSocket = nil
	}
	if resources.runtimeBootstrap != nil && resources.runtimePublication != nil {
		closeErr = errors.Join(closeErr, resources.runtimeBootstrap.Remove(context.Background(), resources.runtimePublication.Directory))
		resources.runtimeBootstrap = nil
	}
	if resources.workerJournalEndpoint != nil {
		closeErr = errors.Join(closeErr, resources.workerJournalEndpoint.Close())
		resources.workerJournalEndpoint = nil
	}
	if resources.workerJournalSocket != nil {
		closeErr = errors.Join(closeErr, resources.workerJournalSocket.Close())
		resources.workerJournalSocket = nil
	}
	if resources.workerInputJournal != nil {
		closeErr = errors.Join(closeErr, resources.workerInputJournal.Close())
		resources.workerInputJournal = nil
	}
	if resources.workerMaterializationJournal != nil {
		closeErr = errors.Join(closeErr, resources.workerMaterializationJournal.Close())
		resources.workerMaterializationJournal = nil
	}
	if resources.registryClose != nil {
		closeErr = errors.Join(closeErr, resources.registryClose())
		resources.registryClose = nil
	}
	if resources.stopCustodyHeartbeat != nil {
		resources.stopCustodyHeartbeat()
		resources.stopCustodyHeartbeat = nil
	}
	if resources.custody != nil {
		closeErr = errors.Join(closeErr, resources.custody.Close())
		resources.custody = nil
	}
	if resources.workerOwner != nil {
		closeErr = errors.Join(closeErr, resources.workerOwner.Close())
		resources.workerOwner = nil
	}
	if resources.launcherClose != nil {
		closeErr = errors.Join(closeErr, resources.launcherClose())
		resources.launcherClose = nil
	}
	if resources.launcherCleanupVerify != nil {
		checkCtx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		closeErr = errors.Join(closeErr, resources.launcherCleanupVerify(checkCtx))
		cancel()
		resources.launcherCleanupVerify = nil
	}
	if resources.retirementJournalID != uuid.Nil && resources.ledger != nil && resources.journal != nil {
		retireCtx, cancel := context.WithTimeout(context.Background(), 25*time.Second)
		closeErr = errors.Join(closeErr, resources.ledger.RetireBackendIncarnation(retireCtx, resources.retirementJournalID, resources.journal))
		cancel()
		resources.retirementJournalID = uuid.Nil
	}
	// The ledger retains the same RuntimeNamespaceOwner pointer used during
	// startup enrollment. Keep that pidfd alive until retirement has consumed
	// the exact-owner exit observation; closing it earlier makes every cleanup
	// path look like a lost owner and leaves the journal UNRESOLVED.
	if resources.runtimeOwner != nil {
		closeErr = errors.Join(closeErr, resources.runtimeOwner.Close())
		resources.runtimeOwner = nil
	}
	if resources.image != nil {
		closeErr = errors.Join(closeErr, resources.image.Images.Close())
		resources.image = nil
	}
	if resources.observer != nil {
		closeErr = errors.Join(closeErr, resources.observer.Close())
		resources.observer = nil
	}
	if resources.journal != nil {
		closeErr = errors.Join(closeErr, resources.journal.Close())
		resources.journal = nil
	}
	if resources.ledger != nil {
		closeErr = errors.Join(closeErr, resources.ledger.Close())
		resources.ledger = nil
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
	return listenRuntimeStartupSocketAt(configuration, configuration.runtimeStartupSocket, runtimeGID)
}

func listenRuntimeStartupSocketAt(configuration config, path string, runtimeGID uint32) (*runtimeStartupSocket, error) {
	if !configuration.runtimeStartupEnabled {
		return nil, errors.New("runtime startup is disabled")
	}
	if runtimeGID == ^uint32(0) {
		return nil, errors.New("runtime startup socket GID is invalid")
	}
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
	// Close cleanup below is identity-aware. Disable UnixListener's default
	// unconditional unlink so a pathname replacement cannot be deleted by an
	// older startup listener.
	listener.SetUnlinkOnClose(false)
	published, err := os.Lstat(path)
	if err != nil {
		_ = listener.Close()
		return nil, fmt.Errorf("inspect published runtime startup socket: %w", err)
	}
	cleanup := func() {
		_ = listener.Close()
		if current, statErr := os.Lstat(path); statErr == nil && os.SameFile(published, current) {
			_ = os.Remove(path)
		}
	}
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
		cleanup()
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
