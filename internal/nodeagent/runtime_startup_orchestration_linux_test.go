package nodeagent

import (
	"testing"
	"time"
)

func TestRuntimeStartupOrchestrationRequiresEveryAuthorityInput(t *testing.T) {
	cases := []RuntimeStartupOrchestrationConfig{
		{ExchangeTimeout: time.Second, ObserverInterval: time.Millisecond, ObserverTimeout: time.Second},
		{Credentials: []RuntimeCallerCredentials{{UID: 10001, GID: 10001}}, ExchangeTimeout: time.Second, ObserverInterval: time.Millisecond, ObserverTimeout: time.Second},
	}
	for _, config := range cases {
		if orchestration, err := NewRuntimeStartupOrchestration(config); orchestration != nil || err == nil {
			t.Fatalf("incomplete orchestration accepted: %+v", config)
		}
	}
}
