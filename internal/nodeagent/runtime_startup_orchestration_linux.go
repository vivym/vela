package nodeagent

import (
	"context"
	"errors"
	"net"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

// RuntimeStartupOrchestration is the explicit Node-owned composition root for
// one startup attempt. Every authority-bearing input is required; it has no
// defaults and cannot recover any input from a receipt or ledger history.
type RuntimeStartupOrchestration struct {
	coordinator *RuntimeStartupCoordinator
	server      *RuntimeStartupServer
	caller      *RuntimeCaller
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
	Caller           *RuntimeCaller
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
	return &RuntimeStartupOrchestration{coordinator: coordinator, server: server, caller: config.Caller}, nil
}

// Serve uses the protected listener supplied by trusted Node assembly. This
// method never creates, chmods or unlinks a socket path.
func (orchestration *RuntimeStartupOrchestration) Serve(ctx context.Context, listener *net.UnixListener) error {
	if orchestration == nil {
		return ErrRuntimeCallerIdentity
	}
	return orchestration.server.Serve(ctx, listener)
}

// ServeCaller completes the one authenticated handshake retained by Node
// reservation. It is the only production startup path; Serve remains a
// transport adapter for callers that own a protected listener.
func (orchestration *RuntimeStartupOrchestration) ServeCaller(ctx context.Context) error {
	if orchestration == nil || orchestration.caller == nil {
		return ErrRuntimeCallerIdentity
	}
	return orchestration.server.HandleCaller(ctx, orchestration.caller)
}

// ExpectedBackendStartupRequest returns the immutable request captured during
// composition. It is a read-only evidence source for command-level receipt
// binding; it does not grant permission or expose any mutable coordinator
// state.
func (orchestration *RuntimeStartupOrchestration) ExpectedBackendStartupRequest() (modelruntime.BackendStartupRequest, error) {
	if orchestration == nil || orchestration.coordinator == nil {
		return modelruntime.BackendStartupRequest{}, ErrRuntimeStartupAuthority
	}
	return orchestration.coordinator.expected, nil
}

// Wait keeps the Node-owned startup composition alive after the one-shot
// startup reply. A successful Permit starts a monitored lifetime; returning
// from ServeCaller must not close custody or the runtime immediately.
func (orchestration *RuntimeStartupOrchestration) Wait(ctx context.Context) error {
	if orchestration == nil || ctx == nil || orchestration.coordinator == nil {
		return ErrRuntimeCallerIdentity
	}
	for {
		orchestration.coordinator.mu.Lock()
		observation := orchestration.coordinator.observation
		handled := orchestration.coordinator.handled
		closed := orchestration.coordinator.closed
		orchestration.coordinator.mu.Unlock()
		if observation != nil {
			select {
			case <-observation.Done():
				return observation.Err()
			case <-ctx.Done():
				return context.Cause(ctx)
			}
		}
		if handled || closed {
			return errors.Join(ErrRuntimeObserverCustody, context.Cause(ctx))
		}
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case <-time.After(time.Millisecond):
		}
	}
}

func (orchestration *RuntimeStartupOrchestration) Shutdown(ctx context.Context) error {
	if orchestration == nil || ctx == nil {
		return ErrRuntimeCallerIdentity
	}
	// Stop admission and revoke before waiting for either handlers or storage.
	done := orchestration.coordinator.stop()
	if err := orchestration.server.Shutdown(ctx); err != nil {
		return err
	}
	select {
	case <-done:
		return orchestration.coordinator.closeErr
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Close joins all owned work. Use Shutdown to bound the wait for outstanding
// I/O; a timeout leaves the attempt revoked and cleanup continues in background.
func (orchestration *RuntimeStartupOrchestration) Close() error {
	if orchestration == nil {
		return nil
	}
	return orchestration.Shutdown(context.Background())
}
