package nodeagent

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/vivym/vela/internal/modelruntime"
	"net"
	"path/filepath"
	"sync"
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

func TestRuntimeStartupOrchestrationShutdownDuringPersistence(t *testing.T) {
	f, custody := observedActivationFixture(t)
	expected := f.ledger.starts[f.identity.JournalID].Request
	orchestration, err := NewRuntimeStartupOrchestration(RuntimeStartupOrchestrationConfig{
		Ledger: f.ledger, Plan: f.plan, ExpectedRequest: expected, Grant: f.grant, Observer: custody,
		Credentials:      []RuntimeCallerCredentials{{UID: 10001, GID: 10001}},
		ObserverInterval: 50 * time.Millisecond, ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = orchestration.Close() }()
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	f.ledger.boundary = func(phase string) error {
		if phase == "before-append" {
			close(entered)
			<-release
		}
		return nil
	}
	result := make(chan []byte, 1)
	go func() {
		wire, _ := orchestration.coordinator.HandleBackendStartup(t.Context(), expected)
		result <- wire
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("activation did not enter persistence")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	shutdown := make(chan error, 1)
	go func() { shutdown <- orchestration.Shutdown(ctx) }()
	select {
	case err := <-shutdown:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("outstanding persistence incorrectly joined: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown deadline blocked behind persistence")
	}
	awaitObservedExit(t, custody)
	f.write(t, false)
	unblock()
	select {
	case wire := <-result:
		var decision modelruntime.BackendStartupDecision
		if json.Unmarshal(wire, &decision) != nil || decision.Permit {
			t.Fatalf("closed attempt permitted: %s", wire)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("activation did not join")
	}
	if err := orchestration.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeStartupOrchestrationStopsListenerAndRoutes(t *testing.T) {
	for _, scenario := range []string{"close", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			f, custody := observedActivationFixture(t)
			expected := f.ledger.starts[f.identity.JournalID].Request
			orchestration, err := NewRuntimeStartupOrchestration(RuntimeStartupOrchestrationConfig{
				Ledger: f.ledger, Plan: f.plan, ExpectedRequest: expected, Grant: f.grant, Observer: custody,
				Credentials:      []RuntimeCallerCredentials{{UID: f.plan.uid, GID: f.plan.gid}},
				ObserverInterval: 50 * time.Millisecond, ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = orchestration.Close() }()
			listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "startup.sock"), Net: "unixpacket"})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = listener.Close() }()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			served := make(chan error, 1)
			go func() { served <- orchestration.Serve(ctx, listener) }()
			deadline := time.Now().Add(time.Second)
			for {
				orchestration.server.mu.Lock()
				started := orchestration.server.done != nil
				orchestration.server.mu.Unlock()
				if started {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("Serve did not start")
				}
				time.Sleep(time.Millisecond)
			}
			wire, err := orchestration.coordinator.HandleBackendStartup(t.Context(), expected)
			var decision modelruntime.BackendStartupDecision
			if err != nil || json.Unmarshal(wire, &decision) != nil || !decision.Permit {
				t.Fatalf("activate: %s %v", wire, err)
			}
			f.write(t, true)
			if scenario == "cancel" {
				cancel()
			} else if err := orchestration.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-served:
				if err == nil {
					t.Fatal("closed listener returned no error")
				}
			case <-time.After(time.Second):
				t.Fatal("shutdown left listener accepting")
			}
			f.write(t, false)
			// Concurrent joins all observe completed cleanup, not merely a flag.
			joined := make(chan error, 4)
			for range 4 {
				go func() { joined <- orchestration.Close() }()
			}
			for range 4 {
				if err := <-joined; err != nil {
					t.Fatal(err)
				}
			}
			custody.mu.Lock()
			defer custody.mu.Unlock()
			if custody.connection != nil || custody.observer != nil || custody.target != nil {
				t.Fatal("join returned before custody release")
			}
		})
	}
}
