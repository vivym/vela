package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/nodeagent"
	corev1 "k8s.io/api/core/v1"
)

type commandCompositionOrchestrationFake struct {
	receipt   nodeagent.RuntimeStartupCompositionReceipt
	serveErr  error
	serveCall bool
}

type gateCompositionOrchestrationFake struct {
	receipt nodeagent.RuntimeStartupCompositionReceipt
	waitErr error
	revoked bool
}

func (fake *gateCompositionOrchestrationFake) ServeCaller(context.Context) error { return nil }
func (fake *gateCompositionOrchestrationFake) CompositionReceipt(context.Context) (nodeagent.RuntimeStartupCompositionReceipt, error) {
	receipt := fake.receipt
	if fake.revoked {
		receipt.Permit, receipt.Outcome = false, "revoked"
		unsigned := receipt
		unsigned.ReceiptDigest = [sha256.Size]byte{}
		receipt.ReceiptDigest = sha256.Sum256(mustJSON(unsigned))
	}
	return receipt, nil
}
func (fake *gateCompositionOrchestrationFake) Wait(context.Context) error { return fake.waitErr }
func (fake *gateCompositionOrchestrationFake) Shutdown(context.Context) error {
	fake.revoked = true
	return nil
}

func mustJSON(value any) []byte {
	wire, err := json.Marshal(value)
	if err != nil {
		panic(err)
	}
	return wire
}

func (fake *commandCompositionOrchestrationFake) ServeCaller(context.Context) error {
	fake.serveCall = true
	return fake.serveErr
}
func (fake *commandCompositionOrchestrationFake) CompositionReceipt(context.Context) (nodeagent.RuntimeStartupCompositionReceipt, error) {
	return fake.receipt, nil
}
func (*commandCompositionOrchestrationFake) Wait(context.Context) error     { return nil }
func (*commandCompositionOrchestrationFake) Shutdown(context.Context) error { return nil }

func commandCompositionReceiptFixture() nodeagent.RuntimeStartupCompositionReceipt {
	receipt := nodeagent.RuntimeStartupCompositionReceipt{
		SchemaVersion: 1, OperationID: uuid.New(), JournalID: uuid.New(), Permit: true, Outcome: "permitted",
		RequestDigest: [sha256.Size]byte{1}, ReservationDigest: [sha256.Size]byte{2},
		AuthorizationDigest: [sha256.Size]byte{3}, GrantAttemptDigest: [sha256.Size]byte{4},
	}
	wire, _ := json.Marshal(receipt)
	receipt.ReceiptDigest = sha256.Sum256(wire)
	return receipt
}

func TestRuntimeStartupAuthorityInjectionRequiresEnabledModeAndPlan(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	if _, err := newRuntimeStartupAuthority(configuration, nil, nodeagent.RuntimeStartupAuthorityConfig{}); err == nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled injection result = %v", err)
	}
	configuration.runtimeStartupEnabled = true
	if _, err := newRuntimeStartupAuthority(configuration, nil, nodeagent.RuntimeStartupAuthorityConfig{}); !errors.Is(err, nodeagent.ErrRuntimeStartupAuthority) {
		t.Fatalf("missing plan injection error = %v", err)
	}
}

func TestLoadRuntimeStartupResourcesWithFactoryRejectsIncompleteFactory(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	configuration.runtimeStartupEnabled = true
	if resources, err := loadRuntimeStartupResourcesWithFactory(context.Background(), configuration, runtimeStartupResourceFactory{}); resources != nil || err == nil || !strings.Contains(err.Error(), "resource factory is incomplete") {
		t.Fatalf("incomplete resource factory result resources=%v error=%v", resources, err)
	}
}

func TestRuntimeStartupProductionResourceFactoryIsComplete(t *testing.T) {
	factory := runtimeStartupProductionResourceFactory()
	if factory.loadKubernetesCore == nil || factory.newPodReader == nil || factory.loadContainerObserver == nil || factory.loadFleetRegistry == nil || factory.listenStartupSocket == nil {
		t.Fatal("production resource factory has an unconfigured constructor")
	}
}

func TestLoadRuntimeContainerObserverRequiresEnabledTrustedSocket(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	if observer, err := loadRuntimeContainerObserver(context.Background(), configuration); err == nil || observer != nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled CRI observer result observer=%v error=%v", observer, err)
	}
	configuration.runtimeStartupEnabled = true
	configuration.runtimeCRISocket = filepath.Join(t.TempDir(), "containerd.sock")
	if observer, err := loadRuntimeContainerObserver(context.Background(), configuration); err == nil || observer != nil {
		t.Fatalf("missing CRI socket result observer=%v error=%v", observer, err)
	}
	if observer, err := loadRuntimeContainerObserver(nil, configuration); err == nil || observer != nil || !errors.Is(err, nodeagent.ErrRuntimeObserverCustody) {
		t.Fatalf("nil context CRI observer result observer=%v error=%v", observer, err)
	}
}

func TestReceiveRuntimeStartupCallerRequiresTrustedAssembly(t *testing.T) {
	if caller, err := receiveRuntimeStartupCaller(context.Background(), nil, nil); caller != nil || !errors.Is(err, nodeagent.ErrRuntimeCallerIdentity) {
		t.Fatalf("nil startup caller assembly result caller=%v error=%v", caller, err)
	}
}

func TestRuntimeStartupLifecycleRejectsNilContext(t *testing.T) {
	if err := (&runtimeStartupLifecycle{}).Shutdown(nil); !errors.Is(err, nodeagent.ErrRuntimeCallerIdentity) {
		t.Fatalf("nil lifecycle context error = %v", err)
	}
	if err := (*runtimeStartupLifecycle)(nil).Shutdown(context.Background()); err != nil {
		t.Fatalf("nil lifecycle shutdown error = %v", err)
	}
}

func TestRuntimeStartupLifecycleCompositionReceiptRequiresOrchestration(t *testing.T) {
	if receipt, err := (*runtimeStartupLifecycle)(nil).CompositionReceipt(context.Background()); receipt != (nodeagent.RuntimeStartupCompositionReceipt{}) || !errors.Is(err, nodeagent.ErrRuntimeStartupAuthority) {
		t.Fatalf("nil lifecycle receipt=%+v err=%v", receipt, err)
	}
	if receipt, err := (&runtimeStartupLifecycle{}).CompositionReceipt(context.Background()); receipt != (nodeagent.RuntimeStartupCompositionReceipt{}) || !errors.Is(err, nodeagent.ErrRuntimeStartupAuthority) {
		t.Fatalf("empty lifecycle receipt=%+v err=%v", receipt, err)
	}
}

func TestRuntimeStartupResourcesCloseVerifiesLauncherCleanup(t *testing.T) {
	called := false
	want := errors.New("CRI cleanup verification failed")
	resources := &runtimeStartupResources{launcherCleanupVerify: func(context.Context) error {
		called = true
		return want
	}}
	if err := resources.Close(); !errors.Is(err, want) || !called || resources.launcherCleanupVerify != nil {
		t.Fatalf("launcher cleanup verifier was not authoritative: called=%v err=%v verifier=%v", called, err, resources.launcherCleanupVerify != nil)
	}
}

func TestRuntimeStartupGateSnapshotsPermitBeforeShutdownFailure(t *testing.T) {
	setValidNodeAgentEnv(t)
	directory := t.TempDir()
	fake := &gateCompositionOrchestrationFake{receipt: commandCompositionReceiptFixture(), waitErr: errors.New("runtime exited unexpectedly")}
	configuration := config{runtimeStartupEnabled: true, receiptDirectory: directory}
	resources := &runtimeStartupResources{}
	launcher := runtimeStartupLauncherFunc(func(context.Context, *nodeagent.RuntimeLaunchPlan, string) (runtimeStartupLaunch, error) {
		return runtimeStartupLaunch{}, nil
	})
	err := runRuntimeStartupGateWithDependencies(configuration,
		func(config) runtimeStartupLauncher { return launcher },
		func(context.Context, config) (*runtimeStartupResources, error) { return resources, nil },
		func(context.Context, config, *runtimeStartupResources, runtimeStartupLauncher) (*runtimeStartupLifecycle, error) {
			return &runtimeStartupLifecycle{orchestration: fake, resources: resources}, nil
		},
	)
	if !errors.Is(err, fake.waitErr) {
		t.Fatalf("gate error=%v, want wait failure", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	var failure nodeagent.RuntimeStartupFailureReceipt
	foundFailure := false
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), "runtime-startup-failure-") {
			continue
		}
		wire, readErr := os.ReadFile(filepath.Join(directory, entry.Name()))
		if readErr != nil {
			t.Fatal(readErr)
		}
		if err := json.Unmarshal(wire, &failure); err != nil {
			t.Fatal(err)
		}
		foundFailure = true
	}
	if !foundFailure || !failure.PermitIssued || !failure.GrantCreated || failure.Phase != string(nodeagent.RuntimeStartupFailureBackend) {
		t.Fatalf("failure receipt lost pre-shutdown Permit: found=%v receipt=%+v", foundFailure, failure)
	}
	if err := failure.Verify(); err != nil {
		t.Fatalf("failure receipt invalid: %v", err)
	}
}

func TestRuntimeStartupGatePersistsSuccessfulCompositionBeforeWait(t *testing.T) {
	setValidNodeAgentEnv(t)
	directory := t.TempDir()
	receipt := commandCompositionReceiptFixture()
	fake := &gateCompositionOrchestrationFake{receipt: receipt}
	configuration := config{runtimeStartupEnabled: true, receiptDirectory: directory}
	resources := &runtimeStartupResources{}
	launcher := runtimeStartupLauncherFunc(func(context.Context, *nodeagent.RuntimeLaunchPlan, string) (runtimeStartupLaunch, error) {
		return runtimeStartupLaunch{}, nil
	})
	err := runRuntimeStartupGateWithDependencies(configuration,
		func(config) runtimeStartupLauncher { return launcher },
		func(context.Context, config) (*runtimeStartupResources, error) { return resources, nil },
		func(context.Context, config, *runtimeStartupResources, runtimeStartupLauncher) (*runtimeStartupLifecycle, error) {
			return &runtimeStartupLifecycle{orchestration: fake, resources: resources}, nil
		},
	)
	if err != nil {
		t.Fatalf("successful gate returned error: %v", err)
	}
	path := filepath.Join(directory, "runtime-startup-composition-"+receipt.OperationID.String()+".json")
	wire, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("successful composition receipt was not persisted before wait: %v", err)
	}
	var replayed nodeagent.RuntimeStartupCompositionReceipt
	if err := json.Unmarshal(wire, &replayed); err != nil || replayed != receipt || replayed.Verify() != nil {
		t.Fatalf("persisted successful composition receipt is not replayable: %+v err=%v", replayed, err)
	}
}

func TestRuntimeStartupGateFailsClosedOnNilComposedLifecycle(t *testing.T) {
	directory := t.TempDir()
	configuration := config{runtimeStartupEnabled: true, receiptDirectory: directory}
	resources := &runtimeStartupResources{}
	launcher := runtimeStartupLauncherFunc(func(context.Context, *nodeagent.RuntimeLaunchPlan, string) (runtimeStartupLaunch, error) {
		return runtimeStartupLaunch{}, nil
	})
	err := runRuntimeStartupGateWithDependencies(configuration,
		func(config) runtimeStartupLauncher { return launcher },
		func(context.Context, config) (*runtimeStartupResources, error) { return resources, nil },
		func(context.Context, config, *runtimeStartupResources, runtimeStartupLauncher) (*runtimeStartupLifecycle, error) {
			return nil, nil
		},
	)
	if !errors.Is(err, nodeagent.ErrRuntimeStartupAuthority) {
		t.Fatalf("nil composed lifecycle crossed gate: %v", err)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("nil lifecycle wrote %d receipts, want one failure receipt", len(entries))
	}
}

func TestRuntimeStartupGateRecordsHelperFailuresAndCleansResources(t *testing.T) {
	for _, scenario := range []struct {
		name string
		err  error
	}{
		{name: "timeout", err: context.DeadlineExceeded},
		{name: "crash", err: errors.New("launcher helper exited before handoff")},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			directory := t.TempDir()
			configuration := config{runtimeStartupEnabled: true, receiptDirectory: directory}
			closed := false
			resources := &runtimeStartupResources{launcherCleanupVerify: func(context.Context) error {
				closed = true
				return nil
			}}
			launcher := runtimeStartupLauncherFunc(func(context.Context, *nodeagent.RuntimeLaunchPlan, string) (runtimeStartupLaunch, error) {
				return runtimeStartupLaunch{}, scenario.err
			})
			compose := func(context.Context, config, *runtimeStartupResources, runtimeStartupLauncher) (*runtimeStartupLifecycle, error) {
				return nil, scenario.err
			}
			err := runRuntimeStartupGateWithDependencies(configuration,
				func(config) runtimeStartupLauncher { return launcher },
				func(context.Context, config) (*runtimeStartupResources, error) { return resources, nil },
				compose,
			)
			if err == nil || !strings.Contains(err.Error(), scenario.err.Error()) || !closed {
				t.Fatalf("helper failure was not propagated and cleaned: err=%v closed=%v", err, closed)
			}
			entries, readErr := os.ReadDir(directory)
			if readErr != nil || len(entries) != 1 {
				t.Fatalf("helper failure receipt count=%d err=%v read=%v", len(entries), err, readErr)
			}
			wire, readErr := os.ReadFile(filepath.Join(directory, entries[0].Name()))
			if readErr != nil {
				t.Fatal(readErr)
			}
			var receipt nodeagent.RuntimeStartupFailureReceipt
			if readErr := json.Unmarshal(wire, &receipt); readErr != nil {
				t.Fatal(readErr)
			}
			if receipt.Phase != string(nodeagent.RuntimeStartupFailureCompose) || !receipt.CleanupVerified || receipt.ReservationCreated || receipt.GrantCreated || receipt.PermitIssued {
				t.Fatalf("helper failure receipt contains invalid authority: %+v", receipt)
			}
			if verifyErr := receipt.Verify(); verifyErr != nil {
				t.Fatalf("helper failure receipt is invalid: %v", verifyErr)
			}
		})
	}
}

func TestServeRuntimeStartupCompositionRejectsIncompleteLifecycle(t *testing.T) {
	if receipt, err := serveRuntimeStartupComposition(nil, nil); receipt != (nodeagent.RuntimeStartupCompositionReceipt{}) || !errors.Is(err, nodeagent.ErrRuntimeStartupAuthority) {
		t.Fatalf("nil composition result receipt=%+v err=%v", receipt, err)
	}
	if receipt, err := serveRuntimeStartupComposition(context.Background(), &runtimeStartupLifecycle{}); receipt != (nodeagent.RuntimeStartupCompositionReceipt{}) || !errors.Is(err, nodeagent.ErrRuntimeStartupAuthority) {
		t.Fatalf("empty composition result receipt=%+v err=%v", receipt, err)
	}
}

func TestServeRuntimeStartupCompositionUsesVerifiedReceipt(t *testing.T) {
	fake := &commandCompositionOrchestrationFake{receipt: commandCompositionReceiptFixture()}
	receipt, err := serveRuntimeStartupComposition(context.Background(), &runtimeStartupLifecycle{orchestration: fake})
	if err != nil || !fake.serveCall || receipt != fake.receipt {
		t.Fatalf("composition success boundary receipt=%+v err=%v served=%v", receipt, err, fake.serveCall)
	}
}

func TestServeRuntimeStartupCompositionFailsClosedBeforeReceiptOnServeError(t *testing.T) {
	fake := &commandCompositionOrchestrationFake{receipt: commandCompositionReceiptFixture(), serveErr: errors.New("caller exchange failed")}
	receipt, err := serveRuntimeStartupComposition(context.Background(), &runtimeStartupLifecycle{orchestration: fake})
	if receipt != (nodeagent.RuntimeStartupCompositionReceipt{}) || !errors.Is(err, fake.serveErr) || !fake.serveCall {
		t.Fatalf("composition serve failure crossed boundary receipt=%+v err=%v served=%v", receipt, err, fake.serveCall)
	}
}

func TestRuntimeStartupGatePersistsEarlyFailureReceipt(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupEnabled = true
	if err := os.MkdirAll(configuration.receiptDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := runRuntimeStartupGate(configuration); err == nil {
		t.Fatal("runtime startup gate unexpectedly succeeded with missing inputs")
	}
	entries, err := os.ReadDir(configuration.receiptDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("early startup failure wrote %d receipts, want one", len(entries))
	}
	wire, err := os.ReadFile(filepath.Join(configuration.receiptDirectory, entries[0].Name()))
	if err != nil {
		t.Fatal(err)
	}
	var receipt nodeagent.RuntimeStartupFailureReceipt
	if err := json.Unmarshal(wire, &receipt); err != nil {
		t.Fatal(err)
	}
	if err := receipt.Verify(); err != nil {
		t.Fatalf("early startup failure receipt is invalid: %v", err)
	}
	if receipt.ReservationCreated || receipt.GrantCreated || receipt.PermitIssued || !receipt.CleanupVerified {
		t.Fatalf("early failure receipt contains impossible authority: %+v", receipt)
	}
}

func TestComposeRuntimeStartupAuthorityFailsClosedBeforeLauncherWithoutFleetKey(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupEnabled = true
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupSocket = filepath.Join(root, "startup.sock")
	socket, err := listenRuntimeStartupSocket(configuration)
	if err != nil {
		t.Fatal(err)
	}
	defer socket.Close()
	called := false
	launcher := runtimeStartupLauncherFunc(func(context.Context, *nodeagent.RuntimeLaunchPlan, string) (runtimeStartupLaunch, error) {
		called = true
		return runtimeStartupLaunch{}, nil
	})
	resources := &runtimeStartupResources{plan: &nodeagent.RuntimeLaunchPlan{}, observer: &nodeagent.RuntimeContainerObserver{}, socket: socket}
	_, err = composeRuntimeStartupAuthority(context.Background(), configuration, resources, launcher)
	if err == nil || !strings.Contains(err.Error(), "Fleet authorization public key") {
		t.Fatalf("missing Fleet key was accepted: %v", err)
	}
	if called {
		t.Fatal("launcher was called before Fleet authorization key validation")
	}
}

type runtimeStartupLauncherFunc func(context.Context, *nodeagent.RuntimeLaunchPlan, string) (runtimeStartupLaunch, error)

func (f runtimeStartupLauncherFunc) Launch(ctx context.Context, plan *nodeagent.RuntimeLaunchPlan, socket string) (runtimeStartupLaunch, error) {
	return f(ctx, plan, socket)
}

func TestRuntimeContainerBootstrapPathUsesOCICommandAndArgs(t *testing.T) {
	container := &corev1.Container{
		Name:    "model-runtime",
		Command: []string{"/usr/local/bin/vela-model-runtime"},
		Args:    []string{"serve-remote", "--bootstrap-file", "/run/vela-model-runtime-bootstrap/bootstrap.json"},
	}
	path, err := runtimeContainerBootstrapPath(container)
	if err != nil || path != container.Args[2] {
		t.Fatalf("bootstrap path=%q err=%v", path, err)
	}
	container.Command = nil
	if path, err := runtimeContainerBootstrapPath(container); err != nil || path != container.Args[2] {
		t.Fatalf("default entrypoint bootstrap path=%q err=%v", path, err)
	}
	container.Args = []string{"/usr/local/bin/vela-model-runtime", "serve-remote", "--bootstrap-file", container.Args[2]}
	if _, err := runtimeContainerBootstrapPath(container); err == nil {
		t.Fatal("argv[0] duplicated in Args was accepted")
	}
}

func TestListenRuntimeStartupSocketOwnsProtectedPath(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	if socket, err := listenRuntimeStartupSocket(configuration); err == nil || socket != nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled startup socket result socket=%v error=%v", socket, err)
	}
	configuration.runtimeStartupEnabled = true
	root, err := os.MkdirTemp(os.Getenv("HOME"), "vela-runtime-startup-")
	if err != nil {
		t.Fatalf("create trusted startup socket directory: %v", err)
	}
	defer os.RemoveAll(root)
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatalf("protect startup socket directory: %v", err)
	}
	configuration.runtimeStartupSocket = filepath.Join(root, "startup.sock")
	socket, err := listenRuntimeStartupSocket(configuration)
	if err != nil {
		t.Fatalf("listen runtime startup socket: %v", err)
	}
	if socket.Listener() == nil {
		t.Fatal("startup socket listener is nil")
	}
	info, err := os.Stat(configuration.runtimeStartupSocket)
	if err != nil || info.Mode()&os.ModeSocket == 0 || info.Mode().Perm() != 0o600 {
		t.Fatalf("startup socket metadata info=%v error=%v", info, err)
	}
	if err := socket.Close(); err != nil {
		t.Fatalf("close startup socket: %v", err)
	}
	if _, err := os.Lstat(configuration.runtimeStartupSocket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("startup socket path after close error=%v", err)
	}
}

func TestListenRuntimeStartupSocketPublishesRuntimeGID(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("GID publication is a root-only production path")
	}
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	configuration.runtimeStartupEnabled = true
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupSocket = filepath.Join(root, "startup.sock")
	const runtimeGID = uint32(1)
	socket, err := listenRuntimeStartupSocketWithGID(configuration, runtimeGID)
	if err != nil {
		t.Fatalf("publish runtime GID socket: %v", err)
	}
	defer socket.Close()
	info, err := os.Stat(configuration.runtimeStartupSocket)
	if err != nil {
		t.Fatal(err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Uid != 0 || stat.Gid != runtimeGID || info.Mode().Perm() != 0o660 {
		t.Fatalf("unexpected runtime socket ownership uid=%d gid=%d mode=%o", stat.Uid, stat.Gid, info.Mode().Perm())
	}
}

func TestListenRuntimeStartupSocketDoesNotUnlinkPathnameReplacement(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("replacement cleanup test requires the production root listener path")
	}
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupEnabled = true
	root, err := os.MkdirTemp("/run", "vela-runtime-startup-replacement-")
	if err != nil {
		t.Skipf("/run unavailable: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(root) })
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupSocket = filepath.Join(root, "startup.sock")
	socket, err := listenRuntimeStartupSocket(configuration)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(configuration.runtimeStartupSocket); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(configuration.runtimeStartupSocket, []byte("replacement"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := socket.Close(); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(configuration.runtimeStartupSocket); err != nil || string(data) != "replacement" {
		t.Fatalf("startup socket replacement was removed or changed: %q %v", data, err)
	}
}

func TestLoadRuntimeStartupResourcesStopsBeforeAnyImplicitFallback(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatalf("load base config: %v", err)
	}
	if resources, err := loadRuntimeStartupResources(context.Background(), configuration); err == nil || resources != nil || !strings.Contains(err.Error(), "disabled") {
		t.Fatalf("disabled resources result resources=%v error=%v", resources, err)
	}
	configuration.runtimeStartupEnabled = true
	if resources, err := loadRuntimeStartupResources(context.Background(), configuration); err == nil || resources != nil || !strings.Contains(err.Error(), "path is missing") {
		t.Fatalf("incomplete resources result resources=%v error=%v", resources, err)
	}
	if err := (&runtimeStartupResources{}).Close(); err != nil {
		t.Fatalf("empty resource close: %v", err)
	}
}
