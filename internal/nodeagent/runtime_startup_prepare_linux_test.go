package nodeagent

import (
	"errors"
	"testing"
	"time"
)

func TestPrepareRemoteStartupOrchestrationRequiresIndependentInputs(t *testing.T) {
	ledger := &RuntimeStartupLedger{}
	_, _, err := ledger.PrepareRemoteStartupOrchestration(t.Context(), RemoteStartupOrchestrationConfig{ObserverInterval: time.Millisecond, ObserverTimeout: time.Second, ExchangeTimeout: time.Second})
	if !errors.Is(err, ErrRuntimeStartupLedger) {
		t.Fatalf("missing authority inputs accepted: %v", err)
	}
}
