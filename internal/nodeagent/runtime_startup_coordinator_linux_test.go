package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

func TestRuntimeStartupCoordinatorPermitsOnlyAfterObservedActivation(t *testing.T) {
	f, custody := observedActivationFixture(t)
	expected := f.ledger.starts[f.identity.JournalID].Request
	coordinator, err := NewRuntimeStartupCoordinator(f.ledger, f.plan, expected, f.grant, custody, 50*time.Millisecond, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = coordinator.Close() })
	wire, err := coordinator.HandleBackendStartup(t.Context(), expected)
	if err != nil {
		t.Fatal(err)
	}
	var decision modelruntime.BackendStartupDecision
	if err := json.Unmarshal(wire, &decision); err != nil || !decision.Permit {
		t.Fatalf("permit=%+v err=%v", decision, err)
	}
	f.write(t, true)
	if _, err := coordinator.HandleBackendStartup(t.Context(), expected); err != nil {
		t.Fatal(err)
	}
	var repeated modelruntime.BackendStartupDecision
	_ = json.Unmarshal(mustCoordinatorWire(t, coordinator, expected), &repeated)
	if repeated.Permit {
		t.Fatal("one-shot coordinator permitted retry")
	}
}

func mustCoordinatorWire(t *testing.T, coordinator *RuntimeStartupCoordinator, request modelruntime.BackendStartupRequest) []byte {
	t.Helper()
	wire, err := coordinator.HandleBackendStartup(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	return wire
}

func TestRuntimeStartupCoordinatorDeniesMismatchWithoutConsumption(t *testing.T) {
	f, custody := observedActivationFixture(t)
	expected := f.ledger.starts[f.identity.JournalID].Request
	coordinator, err := NewRuntimeStartupCoordinator(f.ledger, f.plan, expected, f.grant, custody, 50*time.Millisecond, 500*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = coordinator.Close() }()
	mismatch := expected
	mismatch.NodeIdentity = "other-node"
	wire, err := coordinator.HandleBackendStartup(t.Context(), mismatch)
	if err != nil {
		t.Fatal(err)
	}
	var decision modelruntime.BackendStartupDecision
	if err := json.Unmarshal(wire, &decision); err != nil || decision.Permit {
		t.Fatalf("mismatch decision=%+v err=%v", decision, err)
	}
	if _, err := f.ledger.InspectJournalGrantAttempt(t.Context(), f.identity.JournalID); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("mismatch consumed grant: %v", err)
	}
	f.write(t, false)
}
