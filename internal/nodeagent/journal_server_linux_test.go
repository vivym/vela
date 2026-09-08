package nodeagent

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func journalServerRequest(t *testing.T, child *journalEndpointChild, request journalEndpointControl) journalEndpointReport {
	t.Helper()
	if err := child.reader.SetReadDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := child.input.Encode(request); err != nil {
		t.Fatal(err)
	}
	var report journalEndpointReport
	if err := child.reports.Decode(&report); err != nil {
		t.Fatal(err)
	}
	return report
}

func startJournalTestServer(t *testing.T, f journalEndpointFixture, timeout time.Duration) (*JournalServer, <-chan error) {
	t.Helper()
	credentials := []RuntimeCallerCredentials{{UID: 65532, GID: 65532}}
	server, err := NewJournalServer(f.endpoint, JournalServerConfig{Credentials: credentials, MaxConcurrent: 2, ExchangeTimeout: timeout})
	if err != nil {
		t.Fatal(err)
	}
	// Caller-owned configuration cannot change the admitted credential set.
	credentials[0].UID--
	done := make(chan error, 1)
	go func() { done <- server.Serve(t.Context(), f.listener) }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := server.Shutdown(ctx); err != nil {
			t.Errorf("journal server did not join: %v", err)
		}
	})
	return server, done
}

func waitJournalServer(t *testing.T, server *JournalServer, predicate func(JournalServerStats) bool) JournalServerStats {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	for {
		stats := server.Stats()
		if predicate(stats) {
			return stats
		}
		select {
		case <-ctx.Done():
			t.Fatalf("server state did not converge: %+v", stats)
		case <-time.After(time.Millisecond):
		}
	}
}

func assertJournalServerJoined(t *testing.T, server *JournalServer, done <-chan error) {
	t.Helper()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) && !errors.Is(err, net.ErrClosed) {
			t.Fatalf("serve termination: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("server did not join accepted connections")
	}
	stats := server.Stats()
	if stats.InFlight != 0 || stats.PeakInFlight > 2 || stats.Accepted != stats.Overloaded+stats.Replied+stats.Failed {
		t.Fatalf("unjoined work or inconsistent final counters: %+v", stats)
	}
	t.Logf("joined Node journal server: %+v", stats)
}

func TestJournalServer(t *testing.T) {
	f := newJournalEndpointFixture(t)
	server, done := startJournalTestServer(t, f, 30*time.Second)
	before := runtimeCallerDescriptorCount(t)
	if report := journalServerRequest(t, f.sibling, journalEndpointControl{IdleConnections: 2}); report.IdleReady != 2 {
		t.Fatalf("real peers failed to fill handshake slots: %+v", report)
	}
	stats := server.Stats()
	if stats.InFlight != 2 || stats.PeakInFlight != 2 || stats.Authenticated != 0 {
		t.Fatalf("idle connections were not bounded before request allocation: %+v", stats)
	}
	held := runtimeCallerDescriptorCount(t)
	for range 32 {
		if report := journalServerRequest(t, f.runtime, journalEndpointControl{Identity: f.identity, Read: true}); report.Error == "" || report.ReadCompleted {
			t.Fatal("overloaded connection bypassed handshake bound")
		}
	}
	if after := runtimeCallerDescriptorCount(t); after != held {
		t.Fatalf("overload leaked descriptors: before=%d after=%d", held, after)
	}
	if stats := server.Stats(); stats.Overloaded != 32 || stats.InFlight != 2 || stats.Authenticated != 0 {
		t.Fatalf("overload created more authenticated work: %+v", stats)
	}
	journalServerRequest(t, f.sibling, journalEndpointControl{CloseIdle: true})
	waitJournalServer(t, server, func(s JournalServerStats) bool { return s.InFlight == 0 })
	if after := runtimeCallerDescriptorCount(t); after != before {
		t.Fatalf("idle disconnect leaked sockets/pidfds: before=%d after=%d", before, after)
	}
	if report := journalServerRequest(t, f.sibling, journalEndpointControl{Identity: f.identity, Read: true}); report.Error == "" {
		t.Fatal("same-UID sibling obtained journal role")
	}
	admit := modelruntime.JournalCommand{SchemaVersion: 1, Admit: &modelruntime.JournalAuthorityCommand{Authority: f.authority}}
	if report := journalServerRequest(t, f.worker, journalEndpointControl{Identity: f.identity, Command: admit}); report.Error == "" {
		t.Fatal("Worker obtained Runtime admission role")
	}
	if report := journalServerRequest(t, f.worker, journalEndpointControl{Identity: f.identity, Read: true}); !report.ReadCompleted {
		t.Fatalf("capacity release did not restore authorized read: %+v", report)
	}
	if report := journalServerRequest(t, f.runtime, journalEndpointControl{Identity: f.identity, Manifest: &f.manifest, Startup: f.startup, Authority: f.authority}); !report.SupervisorCompleted {
		t.Fatalf("actual Supervisor failed through server: %+v", report)
	}
	floor := modelruntime.JournalCommand{SchemaVersion: 1, Floor: &modelruntime.JournalFloorCommand{Disposition: f.floor}}
	if report := journalServerRequest(t, f.worker, journalEndpointControl{Identity: f.identity, Command: floor}); report.Error != "" || report.Receipt.Floor != 1 {
		t.Fatalf("Worker could not persist restriction through server: %+v", report)
	}
	status, err := f.owner.Status(t.Context())
	if err != nil || status.Highest != 1 || status.Floor != 1 || status.PendingExecutions != 0 {
		t.Fatalf("incorrect actual durable history: %+v %v", status, err)
	}
	journalServerRequest(t, f.sibling, journalEndpointControl{IdleConnections: 2})
	// Shutdown must not unlink a replacement path owned by other assembly.
	path := f.listener.Addr().String()
	if err := os.Rename(path, path+".original"); err != nil {
		t.Fatal(err)
	}
	replacement, err := net.ListenUnix("unixpacket", &net.UnixAddr{Name: path, Net: "unixpacket"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = replacement.Close() }()
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertJournalServerJoined(t, server, done)
	if _, err := os.Lstat(path); err != nil {
		t.Fatalf("shutdown removed replacement socket: %v", err)
	}
	if _, err := f.owner.Status(t.Context()); err != nil {
		t.Fatalf("server shutdown closed journal owner: %v", err)
	}
	if err := server.Serve(t.Context(), replacement); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("stopped server restarted: %v", err)
	}
}

func TestJournalServerLiveSupervisorRecoversAfterOverload(t *testing.T) {
	f := newJournalEndpointFixture(t)
	server, done := startJournalTestServer(t, f, 30*time.Second)
	ready := journalServerRequest(t, f.runtime, journalEndpointControl{Identity: f.identity, Manifest: &f.manifest, Startup: f.startup, Authority: f.authority, InteractiveSupervisor: true})
	if !ready.SupervisorReady {
		t.Fatalf("Supervisor not prepared: %+v", ready)
	}
	waitJournalServer(t, server, func(s JournalServerStats) bool { return s.InFlight == 0 })
	before, err := f.owner.Status(t.Context())
	if err != nil || before.Highest != 1 || before.PendingExecutions != 1 {
		t.Fatalf("prepared journal: %+v %v", before, err)
	}
	statePath := filepath.Join(filepath.Dir(f.listener.Addr().String()), "state", "execution-admission.json")
	beforeBytes, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	journalServerRequest(t, f.sibling, journalEndpointControl{IdleConnections: 2})
	failed := journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "start"})
	if failed.Decision == velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || failed.StartCalls != 0 || server.Stats().Overloaded != 1 {
		t.Fatalf("overload entered backend or did not reach server limit: %+v %+v", failed, server.Stats())
	}
	after, err := f.owner.Status(t.Context())
	if err != nil || after.Highest != before.Highest || after.PendingExecutions != before.PendingExecutions {
		t.Fatalf("overloaded read changed durable history: %+v %v", after, err)
	}
	afterBytes, err := os.ReadFile(statePath)
	if err != nil || !bytes.Equal(beforeBytes, afterBytes) {
		t.Fatalf("overload changed journal bytes: %v", err)
	}
	journalServerRequest(t, f.sibling, journalEndpointControl{CloseIdle: true})
	waitJournalServer(t, server, func(s JournalServerStats) bool { return s.InFlight == 0 })
	recovered := journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "start"})
	if recovered.Decision != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED || recovered.StartCalls != 1 {
		t.Fatalf("same live Supervisor remained fenced after read-only overload: first=%+v retry=%+v", failed, recovered)
	}
	if report := journalServerRequest(t, f.runtime, journalEndpointControl{SupervisorAction: "finish"}); !report.SupervisorCompleted {
		t.Fatalf("recovered Supervisor could not seal/drain: %+v", report)
	}
	if status, err := f.owner.Status(t.Context()); err != nil || status.PendingExecutions != 0 || status.Highest != 1 {
		t.Fatalf("recovered history: %+v %v", status, err)
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertJournalServerJoined(t, server, done)
	t.Log("same non-root PID-1 Supervisor prepared before overload, refused Start without backend entry, then started once and sealed/drained after capacity returned")
}

func TestJournalServerTimeoutAndJoin(t *testing.T) {
	f := newJournalEndpointFixture(t)
	server, done := startJournalTestServer(t, f, 500*time.Millisecond)
	journalServerRequest(t, f.sibling, journalEndpointControl{IdleConnections: 1})
	waitJournalServer(t, server, func(s JournalServerStats) bool { return s.Failed == 1 && s.InFlight == 0 })
	if report := journalServerRequest(t, f.runtime, journalEndpointControl{Identity: f.identity, Read: true}); !report.ReadCompleted {
		t.Fatalf("timed out slot not reusable: %+v", report)
	}
	waitJournalServer(t, server, func(s JournalServerStats) bool { return s.InFlight == 0 })
	// Model a synchronous owner operation that cannot observe cancellation
	// while waiting for the endpoint lock. Shutdown must report unjoined work.
	f.endpoint.mu.Lock()
	locked := true
	defer func() {
		if locked {
			f.endpoint.mu.Unlock()
		}
	}()
	before := server.Stats().Authenticated
	if err := f.runtime.input.Encode(journalEndpointControl{Identity: f.identity, Read: true}); err != nil {
		t.Fatal(err)
	}
	waitJournalServer(t, server, func(s JournalServerStats) bool { return s.Authenticated == before+1 })
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Millisecond)
	defer cancel()
	if err := server.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown forgot unjoined handler: %v", err)
	}
	if server.Stats().InFlight != 1 {
		t.Fatal("shutdown fabricated completed handler")
	}
	f.endpoint.mu.Unlock()
	locked = false
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	assertJournalServerJoined(t, server, done)
	if status, err := f.owner.Status(t.Context()); err != nil || status.Highest != 0 {
		t.Fatalf("shutdown changed journal or closed owner: %+v %v", status, err)
	}
}

func TestJournalServerListenerFailure(t *testing.T) {
	f := newJournalEndpointFixture(t)
	server, done := startJournalTestServer(t, f, 30*time.Second)
	journalServerRequest(t, f.sibling, journalEndpointControl{IdleConnections: 2})
	if err := f.listener.Close(); err != nil {
		t.Fatal(err)
	}
	assertJournalServerJoined(t, server, done)
	if stats := server.Stats(); stats.Failed != 2 {
		t.Fatalf("listener failure retained handshakes: %+v", stats)
	}
}

func TestJournalServerConfiguration(t *testing.T) {
	f := newJournalEndpointFixture(t)
	for _, config := range []JournalServerConfig{
		{}, {Credentials: []RuntimeCallerCredentials{{UID: 0, GID: 1}}, MaxConcurrent: 1, ExchangeTimeout: time.Second},
		{Credentials: []RuntimeCallerCredentials{{UID: 1, GID: 1}}, MaxConcurrent: MaximumJournalConnections + 1, ExchangeTimeout: time.Second},
		{Credentials: []RuntimeCallerCredentials{{UID: 1, GID: 1}}, MaxConcurrent: 1, ExchangeTimeout: 46 * time.Second},
		{Credentials: []RuntimeCallerCredentials{{UID: 1, GID: 1}, {UID: 1, GID: 1}}, MaxConcurrent: 1, ExchangeTimeout: time.Second},
	} {
		if server, err := NewJournalServer(f.endpoint, config); err == nil || server != nil {
			t.Fatalf("accepted invalid configuration: %+v", config)
		}
	}
	server, err := NewJournalServer(f.endpoint, JournalServerConfig{Credentials: []RuntimeCallerCredentials{{UID: 65532, GID: 65532}}, MaxConcurrent: 1, ExchangeTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	stream, err := net.ListenUnix("unix", &net.UnixAddr{Name: filepath.Join(filepath.Dir(f.listener.Addr().String()), "stream.sock"), Net: "unix"})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()
	if err := server.Serve(t.Context(), stream); err == nil {
		t.Fatal("accepted stream listener")
	}
	if err := server.Shutdown(t.Context()); err != nil {
		t.Fatal(err)
	}
	if err := server.Serve(t.Context(), f.listener); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("shutdown-before-serve reopened: %v", err)
	}
}
