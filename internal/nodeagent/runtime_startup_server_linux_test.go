package nodeagent

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"testing"
	"time"
)

func TestRuntimeStartupServerRejectsInvalidConfiguration(t *testing.T) {
	if server, err := NewRuntimeStartupServer(nil, []RuntimeCallerCredentials{{UID: 10001, GID: 10001}}, time.Second); server != nil || err == nil {
		t.Fatal("nil coordinator accepted")
	}
	if err := (&RuntimeStartupServer{}).HandleConnection(t.Context(), nil); err == nil {
		t.Fatal("nil connection accepted")
	}
	if err := (&RuntimeStartupServer{}).Serve(context.Background(), nil); err == nil {
		t.Fatal("nil listener accepted")
	}
}

func TestRuntimeStartupServerShutdownClosesIdleListener(t *testing.T) {
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "startup.sock"), Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	server := &RuntimeStartupServer{}
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(context.Background(), listener) }()
	deadline := time.Now().Add(time.Second)
	for {
		server.mu.Lock()
		started := server.done != nil
		server.mu.Unlock()
		if started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not start")
		}
		time.Sleep(time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		t.Fatalf("idle shutdown blocked: %v", err)
	}
	if err := <-serveDone; err == nil {
		t.Fatal("Serve returned nil after listener shutdown")
	}
}

func TestRuntimeStartupServerCancellationClosesIdleListener(t *testing.T) {
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "startup.sock"), Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	server := &RuntimeStartupServer{}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(ctx, listener) }()
	cancel()
	select {
	case err := <-serveDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("lost cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled Serve remained in AcceptUnix")
	}
}

func TestRuntimeStartupServerShutdownBeforeServeIsTerminal(t *testing.T) {
	server := &RuntimeStartupServer{}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	listener, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: filepath.Join(t.TempDir(), "startup.sock"), Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = listener.Close() }()
	// A deadline makes this regression terminate even if stopped is forgotten.
	if err := listener.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := server.Serve(t.Context(), listener); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Serve after Shutdown: %v", err)
	}
}

// Exercise the real transport with the same original process reserved in the
// ledger. Direct coordinator tests cannot detect per-exchange lifetime leaks.
func TestRuntimeStartupServerOwnsPostReplyLifetime(t *testing.T) {
	for _, scenario := range []string{"delivered", "connection-lost"} {
		t.Run(scenario, func(t *testing.T) {
			var custody *RuntimeObserverCustody
			original, config := newRemoteReservationFixture(t, false, &custody)
			f := startupActivationFromReservation(t, config)
			orchestration, err := NewRuntimeStartupOrchestration(RuntimeStartupOrchestrationConfig{
				Ledger: f.ledger, Plan: f.plan, ExpectedRequest: original.request, Grant: f.grant, Observer: custody,
				Credentials:      []RuntimeCallerCredentials{{UID: f.plan.uid, GID: f.plan.gid}},
				ObserverInterval: 50 * time.Millisecond, ObserverTimeout: 500 * time.Millisecond, ExchangeTimeout: time.Second,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = orchestration.Close() }()
			if scenario == "connection-lost" {
				f.ledger.boundary = func(phase string) error {
					if phase == "after-sync" {
						return original.connection.Close()
					}
					return nil
				}
			}
			if err := original.caller.Reply(t.Context(), []byte("next")); err != nil {
				t.Fatal(err)
			}
			err = orchestration.server.HandleConnection(t.Context(), original.connection)
			if scenario == "connection-lost" {
				if err == nil {
					t.Fatal("lost reply reported success")
				}
				select {
				case <-custody.revokedSignal:
				case <-time.After(time.Second):
					t.Fatal("lost reply left observer active")
				}
				f.write(t, false)
			} else {
				if err != nil {
					t.Fatal(err)
				}
				observation := orchestration.coordinator.observation
				if observation == nil {
					t.Fatal("transport replied without observation")
				}
				select {
				case <-observation.Done():
					t.Fatalf("successful transport canceled backend: %v", observation.Err())
				case <-time.After(200 * time.Millisecond):
				}
				f.write(t, true)
			}
			if err := orchestration.Close(); err != nil {
				t.Fatal(err)
			}
			f.write(t, false)
		})
	}
}
