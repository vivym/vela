package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"sync"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

// RuntimeStartupCoordinator is the Node-side bridge for the authenticated
// Runtime startup socket. It returns Permit only after exact request matching,
// durable grant consumption, observer checks and route activation. Grant issue
// and policy approval remain independent prerequisites.
type RuntimeStartupCoordinator struct {
	ledger            *RuntimeStartupLedger
	plan              *RuntimeLaunchPlan
	expected          modelruntime.BackendStartupRequest
	grant             *JournalWriteGrant
	custody           *RuntimeObserverCustody
	interval, timeout time.Duration
	mu                sync.Mutex
	handled           bool
	observation       *RuntimeJournalObservation
}

func NewRuntimeStartupCoordinator(ledger *RuntimeStartupLedger, plan *RuntimeLaunchPlan, expected modelruntime.BackendStartupRequest, grant *JournalWriteGrant, custody *RuntimeObserverCustody, interval, timeout time.Duration) (*RuntimeStartupCoordinator, error) {
	if ledger == nil || plan == nil || grant == nil || custody == nil || interval <= 0 || timeout <= 0 {
		return nil, ErrRuntimeObserverCustody
	}
	if err := expected.Validate(); err != nil {
		return nil, err
	}
	return &RuntimeStartupCoordinator{ledger: ledger, plan: plan, expected: expected, grant: grant, custody: custody, interval: interval, timeout: timeout}, nil
}

// HandleBackendStartup validates the exact request sent by ModelRuntime. It
// returns a decision document suitable for the Node startup socket. Any error
// returns deny; the coordinator is one-shot and must not be retried after an
// uncertain response.
func (coordinator *RuntimeStartupCoordinator) HandleBackendStartup(ctx context.Context, request modelruntime.BackendStartupRequest) ([]byte, error) {
	if coordinator == nil {
		return nil, ErrRuntimeObserverCustody
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.handled {
		return coordinator.decision(request, false)
	}
	coordinator.handled = true
	wire, err := modelruntime.EncodeBackendStartupRequest(request)
	if err != nil {
		return coordinator.decision(request, false)
	}
	expected, err := modelruntime.EncodeBackendStartupRequest(coordinator.expected)
	if err != nil || !bytes.Equal(wire, expected) {
		return coordinator.decision(request, false)
	}
	observation, _, err := coordinator.ledger.ActivateObservedJournalWriteGrant(ctx, coordinator.plan, coordinator.grant, coordinator.custody, coordinator.interval, coordinator.timeout)
	if err != nil {
		return coordinator.decision(request, false)
	}
	coordinator.observation = observation
	return coordinator.decision(request, true)
}

func (coordinator *RuntimeStartupCoordinator) decision(request modelruntime.BackendStartupRequest, permit bool) ([]byte, error) {
	wire, err := modelruntime.EncodeBackendStartupRequest(request)
	if err != nil {
		return nil, errors.Join(ErrRuntimeObserverCustody, err)
	}
	return json.Marshal(modelruntime.BackendStartupDecision{SchemaVersion: 1, RequestDigest: sha256.Sum256(wire), Permit: permit})
}

func (coordinator *RuntimeStartupCoordinator) Close() error {
	if coordinator == nil {
		return nil
	}
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.observation != nil {
		return coordinator.observation.Close()
	}
	return coordinator.custody.Close()
}
