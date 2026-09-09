package nodeagent

import (
	"context"
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
