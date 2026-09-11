package main

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/nodeagent"
)

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
