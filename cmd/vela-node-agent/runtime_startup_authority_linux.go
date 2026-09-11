package main

import (
	"errors"

	"github.com/vivym/vela/internal/nodeagent"
)

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
