//go:build linux

package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/nodeagent"
)

type compositionNoopPolicy struct{}

func (compositionNoopPolicy) IssueRuntimeStartupAuthorization(context.Context, nodeagent.RuntimeStartupReservationRecord) (nodeagent.RuntimeStartupAuthorizationEvidence, error) {
	return nodeagent.RuntimeStartupAuthorizationEvidence{}, nil
}

func TestComposeRuntimeStartupAuthorityPropagatesHelperFaults(t *testing.T) {
	setValidNodeAgentEnv(t)
	configuration, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	configuration.receiptDirectory = filepath.Join(t.TempDir(), "receipts")
	if err := os.Mkdir(configuration.receiptDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupEnabled = true
	root := t.TempDir()
	if err := os.Chmod(root, 0o700); err != nil {
		t.Fatal(err)
	}
	configuration.runtimeStartupSocket = filepath.Join(root, "startup.sock")
	configuration.runtimePolicyAuthorizationPublicKeyFile = filepath.Join(root, "fleet.pub")
	configuration.runtimePolicyAuthorizationDirectory = filepath.Join(root, "authorization")
	if err := os.WriteFile(configuration.runtimePolicyAuthorizationPublicKeyFile, make([]byte, ed25519.PublicKeySize), 0o600); err != nil {
		t.Fatal(err)
	}
	socket, err := listenRuntimeStartupSocket(configuration)
	if err != nil {
		t.Fatal(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(socket.Close)
	resources := &runtimeStartupResources{plan: &nodeagent.RuntimeLaunchPlan{}, observer: &nodeagent.RuntimeContainerObserver{}, socket: socket}
	originalPolicy := runtimeStartupPolicyFactory
	runtimeStartupPolicyFactory = func(string, string) (nodeagent.RuntimeStartupAuthorizationPolicy, error) {
		return compositionNoopPolicy{}, nil
	}
	t.Cleanup(func() { runtimeStartupPolicyFactory = originalPolicy })
	originalPublisher := runtimeStartupBootstrapPublisher
	runtimeStartupBootstrapPublisher = func(context.Context, config, *runtimeStartupResources) error { return nil }
	t.Cleanup(func() { runtimeStartupBootstrapPublisher = originalPublisher })
	for _, scenario := range []struct {
		name string
		err  error
	}{
		{name: "timeout", err: context.DeadlineExceeded},
		{name: "crash", err: errors.New("helper exited before handoff")},
	} {
		t.Run(scenario.name, func(t *testing.T) {
			called := false
			launcher := runtimeStartupLauncherFunc(func(context.Context, *nodeagent.RuntimeLaunchPlan, string) (runtimeStartupLaunch, error) {
				called = true
				return runtimeStartupLaunch{}, scenario.err
			})
			_, err := composeRuntimeStartupAuthority(context.Background(), configuration, resources, launcher)
			if !called || err == nil || !strings.Contains(err.Error(), scenario.err.Error()) {
				t.Fatalf("helper %s was not failed closed: called=%v err=%v", scenario.name, called, err)
			}
		})
	}
}

func TestComposeRuntimeStartupAuthorityFailsClosedOnPolicyIssuerLoss(t *testing.T) {
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
	configuration.runtimePolicyAuthorizationPublicKeyFile = filepath.Join(root, "fleet.pub")
	if err := os.WriteFile(configuration.runtimePolicyAuthorizationPublicKeyFile, make([]byte, ed25519.PublicKeySize), 0o600); err != nil {
		t.Fatal(err)
	}
	socket, err := listenRuntimeStartupSocket(configuration)
	if err != nil {
		t.Fatal(err)
	}
	defer func(cleanup func() error) { _ = cleanup() }(socket.Close)
	resources := &runtimeStartupResources{plan: &nodeagent.RuntimeLaunchPlan{}, observer: &nodeagent.RuntimeContainerObserver{}, socket: socket}
	originalPolicy := runtimeStartupPolicyFactory
	runtimeStartupPolicyFactory = func(string, string) (nodeagent.RuntimeStartupAuthorizationPolicy, error) {
		return nil, errors.New("policy issuer unavailable")
	}
	t.Cleanup(func() { runtimeStartupPolicyFactory = originalPolicy })
	called := false
	launcher := runtimeStartupLauncherFunc(func(context.Context, *nodeagent.RuntimeLaunchPlan, string) (runtimeStartupLaunch, error) {
		called = true
		return runtimeStartupLaunch{}, nil
	})
	_, err = composeRuntimeStartupAuthority(context.Background(), configuration, resources, launcher)
	if err == nil || !strings.Contains(err.Error(), "policy issuer unavailable") || called {
		t.Fatalf("policy issuer loss crossed composition boundary: called=%v err=%v", called, err)
	}
}

// TestRuntimeStartupCommandCompositionHarness is the opt-in command-level
// validation harness. Unlike the package tests, it runs the exact resource
// loader and composeRuntimeStartupAuthority used by the daemon. The test is
// intentionally skipped until a validation host provides a signed plan, CRI,
// Fleet, issuer and a CPU ModelRuntime image; silently substituting a fixture
// here would make the resulting receipt meaningless.
func TestRuntimeStartupCommandCompositionHarness(t *testing.T) {
	if os.Getenv("VELA_RUNTIME_STARTUP_COMPOSITION_HARNESS") != "1" {
		t.Skip("set VELA_RUNTIME_STARTUP_COMPOSITION_HARNESS=1 with complete validation inputs")
	}
	if os.Geteuid() != 0 {
		t.Fatal("command-level startup composition requires root Node")
	}
	configuration, err := loadConfig()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 3*time.Minute)
	defer cancel()
	resources, err := loadRuntimeStartupResources(ctx, configuration)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = resources.Close() }()
	launcher := runtimeStartupLauncherFactory(configuration)
	if _, unavailable := launcher.(unavailableRuntimeStartupLauncher); unavailable {
		t.Fatal(errRuntimeStartupLauncherUnavailable)
	}
	lifecycle, err := composeRuntimeStartupAuthority(ctx, configuration, resources, launcher)
	if err != nil {
		t.Fatal(err)
	}
	shutdown := true
	defer func() {
		if shutdown {
			_ = lifecycle.Shutdown(context.Background())
		}
	}()
	receipt, err := serveRuntimeStartupComposition(ctx, lifecycle)
	if err != nil {
		t.Fatalf("command-level composition did not produce a verified Permit: %v", err)
	}
	if err := writeRuntimeStartupCompositionReceipt(configuration.receiptDirectory, receipt); err != nil {
		t.Fatalf("persist command-level composition receipt: %v", err)
	}
	wire, err := os.ReadFile(filepath.Join(configuration.receiptDirectory, "runtime-startup-composition-"+receipt.OperationID.String()+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var replayed nodeagent.RuntimeStartupCompositionReceipt
	if err := json.Unmarshal(wire, &replayed); err != nil {
		t.Fatal(err)
	}
	reservationWire, err := json.Marshal(lifecycle.reservation)
	if err != nil {
		t.Fatal(err)
	}
	reservationDigest := sha256.Sum256(reservationWire)
	requestWire, err := modelruntime.EncodeBackendStartupRequest(lifecycle.expected)
	if err != nil {
		t.Fatal(err)
	}
	requestDigest := sha256.Sum256(requestWire)
	if !reflect.DeepEqual(replayed, receipt) || replayed.Verify() != nil || replayed.VerifyBinding(lifecycle.reservation.OperationID, lifecycle.reservation.JournalID, requestDigest, reservationDigest, receipt.AuthorizationDigest) != nil {
		t.Fatalf("persisted composition receipt is not replayable: %+v", replayed)
	}
	shutdown = false
	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancelShutdown()
	if err := lifecycle.Shutdown(shutdownCtx); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if err := resources.Close(); err != nil {
		t.Fatal(err)
	}
}
