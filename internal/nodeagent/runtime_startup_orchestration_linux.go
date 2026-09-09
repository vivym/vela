package nodeagent

import (
	"context"
	"errors"
	"net"
	"sync"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

// RuntimeStartupOrchestration is the explicit Node-owned composition root for
// one startup attempt. Every authority-bearing input is required; it has no
// defaults and cannot recover any input from a receipt or ledger history.
type RuntimeStartupOrchestration struct {
	coordinator *RuntimeStartupCoordinator
	server      *RuntimeStartupServer
	mu          sync.Mutex
	closed      bool
}

type RuntimeStartupOrchestrationConfig struct {
	Ledger           *RuntimeStartupLedger
	Plan             *RuntimeLaunchPlan
	ExpectedRequest  modelruntime.BackendStartupRequest
	Grant            *JournalWriteGrant
	Observer         *RuntimeObserverCustody
	Credentials      []RuntimeCallerCredentials
	ObserverInterval time.Duration
	ObserverTimeout  time.Duration
	ExchangeTimeout  time.Duration
}

func NewRuntimeStartupOrchestration(config RuntimeStartupOrchestrationConfig) (*RuntimeStartupOrchestration, error) {
	coordinator, err := NewRuntimeStartupCoordinator(config.Ledger, config.Plan, config.ExpectedRequest, config.Grant, config.Observer, config.ObserverInterval, config.ObserverTimeout)
	if err != nil {
		return nil, err
	}
	server, err := NewRuntimeStartupServer(coordinator, config.Credentials, config.ExchangeTimeout)
	if err != nil {
		_ = coordinator.Close()
		return nil, err
	}
	return &RuntimeStartupOrchestration{coordinator: coordinator, server: server}, nil
}

// Serve uses the protected listener supplied by trusted Node assembly. This
// method never creates, chmods or unlinks a socket path.
func (orchestration *RuntimeStartupOrchestration) Serve(ctx context.Context, listener *net.UnixListener) error {
	if orchestration == nil {
		return ErrRuntimeCallerIdentity
	}
	orchestration.mu.Lock()
	if orchestration.closed {
		orchestration.mu.Unlock()
		return net.ErrClosed
	}
	orchestration.mu.Unlock()
	return orchestration.server.Serve(ctx, listener)
}
func (orchestration *RuntimeStartupOrchestration) Shutdown(ctx context.Context) error {
	if orchestration == nil || ctx == nil {
		return ErrRuntimeCallerIdentity
	}
	orchestration.mu.Lock()
	if orchestration.closed {
		orchestration.mu.Unlock()
		return nil
	}
	orchestration.mu.Unlock()
	if err := orchestration.server.Shutdown(ctx); err != nil {
		return err
	}
	return orchestration.Close()
}
func (orchestration *RuntimeStartupOrchestration) Close() error {
	if orchestration == nil {
		return nil
	}
	orchestration.mu.Lock()
	if orchestration.closed {
		orchestration.mu.Unlock()
		return nil
	}
	orchestration.closed = true
	orchestration.mu.Unlock()
	return errors.Join(orchestration.coordinator.Close())
}
