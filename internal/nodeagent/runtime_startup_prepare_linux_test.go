package nodeagent

import (
	"errors"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
)

func TestPrepareRemoteStartupOrchestrationRequiresIndependentInputs(t *testing.T) {
	ledger := &RuntimeStartupLedger{}
	_, _, err := ledger.PrepareRemoteStartupOrchestration(t.Context(), RemoteStartupOrchestrationConfig{ObserverInterval: time.Millisecond, ObserverTimeout: time.Second, ExchangeTimeout: time.Second})
	if !errors.Is(err, ErrRuntimeStartupLedger) {
		t.Fatalf("missing authority inputs accepted: %v", err)
	}
}

func TestPrepareRemoteStartupOrchestrationPreflightsConsumptiveInputs(t *testing.T) {
	base := RemoteStartupOrchestrationConfig{
		Reservation: RuntimeStartupReservationConfig{
			Plan: &RuntimeLaunchPlan{}, Journal: &modelruntime.ExecutionJournalOwner{}, Caller: &RuntimeCaller{},
			Observer: &RuntimeContainerObserver{}, Registry: &startupReservationRegistryFixture{},
		},
		WorkerOwner: &RuntimeNamespaceOwner{}, Observer: &RuntimeObserverCustody{},
		AuthorizationPolicy: startupAuthorizationPolicyFixture{}, Credentials: []RuntimeCallerCredentials{{UID: 10001, GID: 10001}},
		ObserverInterval: time.Millisecond, ObserverTimeout: time.Second, ExchangeTimeout: time.Second,
	}
	for name, mutate := range map[string]func(*RemoteStartupOrchestrationConfig){
		"missing-credentials":          func(c *RemoteStartupOrchestrationConfig) { c.Credentials = nil },
		"zero-exchange-timeout":        func(c *RemoteStartupOrchestrationConfig) { c.ExchangeTimeout = 0 },
		"long-observer-interval":       func(c *RemoteStartupOrchestrationConfig) { c.ObserverInterval = 2 * time.Second },
		"duplicate-credentials":        func(c *RemoteStartupOrchestrationConfig) { c.Credentials = append(c.Credentials, c.Credentials[0]) },
		"missing-authorization-policy": func(c *RemoteStartupOrchestrationConfig) { c.AuthorizationPolicy = nil },
	} {
		t.Run(name, func(t *testing.T) {
			config := base
			mutate(&config)
			if err := validateRemoteStartupOrchestrationConfig(config); !errors.Is(err, ErrRuntimeStartupLedger) {
				t.Fatalf("preflight accepted consumptive invalid input: %v", err)
			}
		})
	}
}
