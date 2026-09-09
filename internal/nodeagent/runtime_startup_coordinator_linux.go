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
	"github.com/vivym/vela/internal/runtimechannel"
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
	closed            bool
	lifetime          context.Context
	cancel            context.CancelFunc
	active            chan struct{}
	closeDone         chan struct{}
	closeErr          error
	observation       *RuntimeJournalObservation
}

func NewRuntimeStartupCoordinator(ledger *RuntimeStartupLedger, plan *RuntimeLaunchPlan, expected modelruntime.BackendStartupRequest, grant *JournalWriteGrant, custody *RuntimeObserverCustody, interval, timeout time.Duration) (*RuntimeStartupCoordinator, error) {
	if ledger == nil || plan == nil || grant == nil || custody == nil || interval <= 0 || timeout <= 0 {
		return nil, ErrRuntimeObserverCustody
	}
	if err := expected.Validate(); err != nil {
		return nil, err
	}
	lifetime, cancel := context.WithCancel(context.Background())
	return &RuntimeStartupCoordinator{lifetime: lifetime, cancel: cancel, ledger: ledger, plan: plan, expected: expected, grant: grant, custody: custody, interval: interval, timeout: timeout}, nil
}

// HandleBackendStartup validates the exact request sent by ModelRuntime. It
// returns a decision document suitable for the Node startup socket. Any error
// returns deny; the coordinator is one-shot and must not be retried after an
// uncertain response.
func (coordinator *RuntimeStartupCoordinator) HandleBackendStartup(ctx context.Context, request modelruntime.BackendStartupRequest) ([]byte, error) {
	return coordinator.handleBackendStartup(ctx, nil, request)
}

// HandleBackendStartupWithCaller is the production seam: the caller was
// authenticated exactly once before reservation and is bound to the retained
// Runtime owner by pidfd. A request digest or matching UID cannot substitute
// for this check.
func (coordinator *RuntimeStartupCoordinator) HandleBackendStartupWithCaller(ctx context.Context, caller *RuntimeCaller, request modelruntime.BackendStartupRequest) ([]byte, error) {
	if err := coordinator.matchCaller(caller); err != nil {
		return coordinator.decision(request, false)
	}
	return coordinator.handleBackendStartup(ctx, caller, request)
}

func (coordinator *RuntimeStartupCoordinator) handleBackendStartup(ctx context.Context, caller *RuntimeCaller, request modelruntime.BackendStartupRequest) ([]byte, error) {
	if coordinator == nil {
		return nil, ErrRuntimeObserverCustody
	}
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	_ = caller // the caller was checked by HandleBackendStartupWithCaller
	coordinator.mu.Lock()
	if coordinator.handled || coordinator.closed {
		coordinator.mu.Unlock()
		return coordinator.decision(request, false)
	}
	coordinator.handled = true
	coordinator.active = make(chan struct{})
	coordinator.mu.Unlock()
	defer close(coordinator.active)

	wire, err := modelruntime.EncodeBackendStartupRequest(request)
	if err != nil {
		return coordinator.decision(request, false)
	}
	expected, err := modelruntime.EncodeBackendStartupRequest(coordinator.expected)
	if err != nil || !bytes.Equal(wire, expected) {
		return coordinator.decision(request, false)
	}
	// The exchange can cancel activation, but a successful exchange must not
	// become the running backend's lifetime. Close owns that lifetime instead.
	activation, cancel := coordinator.lifetime, coordinator.cancel
	stopExchange := context.AfterFunc(ctx, cancel)
	observation, _, err := coordinator.ledger.ActivateObservedJournalWriteGrant(activation, coordinator.plan, coordinator.grant, coordinator.custody, coordinator.interval, coordinator.timeout)
	detached := stopExchange()
	coordinator.mu.Lock()
	coordinator.observation = observation
	permit := err == nil && detached && ctx.Err() == nil && !coordinator.closed && activation.Err() == nil
	coordinator.mu.Unlock()
	if !permit {
		cancel()
		if observation != nil {
			_ = observation.Close()
		}
	}
	return coordinator.decision(request, permit)
}

func (coordinator *RuntimeStartupCoordinator) matchCaller(caller *RuntimeCaller) error {
	if caller == nil || coordinator == nil || coordinator.grant == nil || coordinator.grant.endpoint == nil {
		return ErrRuntimeCallerIdentity
	}
	caller.mu.Lock()
	defer caller.mu.Unlock()
	if caller.pidfd == nil {
		return ErrRuntimeCallerIdentity
	}
	endpoint := coordinator.grant.endpoint
	endpoint.mu.Lock()
	defer endpoint.mu.Unlock()
	if endpoint.runtime == nil || endpoint.grantUsed || endpoint.grantPending {
		return ErrRuntimeCallerIdentity
	}
	return runtimechannel.SameLiveProcess(int(caller.pidfd.Fd()), int(endpoint.runtime.Fd()))
}

func (coordinator *RuntimeStartupCoordinator) decision(request modelruntime.BackendStartupRequest, permit bool) ([]byte, error) {
	wire, err := modelruntime.EncodeBackendStartupRequest(request)
	if err != nil {
		return nil, errors.Join(ErrRuntimeObserverCustody, err)
	}
	return json.Marshal(modelruntime.BackendStartupDecision{SchemaVersion: 1, RequestDigest: sha256.Sum256(wire), Permit: permit})
}

// stop fences the lifetime before joining activation or journal I/O. Cleanup
// happens once in the background so callers with a deadline can bound the join.
func (coordinator *RuntimeStartupCoordinator) stop() <-chan struct{} {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()
	if coordinator.closeDone != nil {
		return coordinator.closeDone
	}
	coordinator.closed = true
	coordinator.closeDone = make(chan struct{})
	coordinator.cancel()
	// Revoke uses independent process handles and never takes the ledger lock.
	revokeErr := coordinator.custody.Revoke()
	active := coordinator.active
	go func() {
		if active != nil {
			<-active
		}
		var err error
		if coordinator.observation != nil {
			err = coordinator.observation.Close()
		}
		// Also close an unactivated endpoint owned by this attempt.
		if coordinator.grant != nil {
			err = errors.Join(err, coordinator.grant.endpoint.Close())
		}
		coordinator.closeErr = errors.Join(revokeErr, err, coordinator.custody.Close())
		close(coordinator.closeDone)
	}()
	return coordinator.closeDone
}

func (coordinator *RuntimeStartupCoordinator) Close() error {
	if coordinator == nil {
		return nil
	}
	<-coordinator.stop()
	return coordinator.closeErr
}
