package main

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
)

func TestStageWorkerControlLifecycleStartsAndShutsDownGracefully(t *testing.T) {
	server := newLifecycleTestServer(false)
	lifecycle := newTestStageWorkerControlLifecycle(t, server)
	errors := lifecycle.Start()
	waitLifecycleSignal(t, server.serveStarted, "Serve start")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := lifecycle.Shutdown(ctx); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	waitLifecycleSignal(t, server.gracefulCalled, "GracefulStop")
	if err := <-errors; err != grpc.ErrServerStopped {
		t.Fatalf("Serve error = %v, want grpc.ErrServerStopped", err)
	}
	lifecycle.Close()
	select {
	case <-server.stopCalled:
		t.Fatal("Close called Stop after successful GracefulStop")
	default:
	}
}

func TestStageWorkerControlLifecycleForcesStopAtShutdownDeadline(t *testing.T) {
	server := newLifecycleTestServer(true)
	lifecycle := newTestStageWorkerControlLifecycle(t, server)
	errors := lifecycle.Start()
	waitLifecycleSignal(t, server.serveStarted, "Serve start")

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := lifecycle.Shutdown(ctx); err == nil {
		t.Fatal("Shutdown error = nil, want deadline failure")
	}
	waitLifecycleSignal(t, server.gracefulCalled, "GracefulStop")
	waitLifecycleSignal(t, server.stopCalled, "Stop")
	if err := <-errors; err != grpc.ErrServerStopped {
		t.Fatalf("Serve error = %v, want grpc.ErrServerStopped", err)
	}
}

func TestStageWorkerControlLifecycleCloseOwnsPartialStartupResources(t *testing.T) {
	server := newLifecycleTestServer(false)
	lifecycle := newTestStageWorkerControlLifecycle(t, server)
	lifecycle.Close()
	waitLifecycleSignal(t, server.stopCalled, "Stop")
	if err := lifecycle.listener.Close(); err == nil {
		t.Fatal("listener remained open after Close")
	}
}

func newTestStageWorkerControlLifecycle(
	t *testing.T,
	server *lifecycleTestServer,
) *stageWorkerControlLifecycle {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return &stageWorkerControlLifecycle{
		address: listener.Addr().String(), server: server, listener: listener,
		serveErrors: make(chan error, 1),
	}
}

func waitLifecycleSignal(t *testing.T, signal <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for %s", name)
	}
}

type lifecycleTestServer struct {
	blockGraceful  bool
	serveStarted   chan struct{}
	gracefulCalled chan struct{}
	stopCalled     chan struct{}
	serveRelease   chan struct{}
	serveOnce      sync.Once
	gracefulOnce   sync.Once
	stopOnce       sync.Once
	releaseOnce    sync.Once
}

func newLifecycleTestServer(blockGraceful bool) *lifecycleTestServer {
	return &lifecycleTestServer{
		blockGraceful:  blockGraceful,
		serveStarted:   make(chan struct{}),
		gracefulCalled: make(chan struct{}),
		stopCalled:     make(chan struct{}),
		serveRelease:   make(chan struct{}),
	}
}

func (server *lifecycleTestServer) Serve(net.Listener) error {
	server.serveOnce.Do(func() { close(server.serveStarted) })
	<-server.serveRelease
	return grpc.ErrServerStopped
}

func (server *lifecycleTestServer) GracefulStop() {
	server.gracefulOnce.Do(func() { close(server.gracefulCalled) })
	if server.blockGraceful {
		<-server.stopCalled
	}
	server.releaseOnce.Do(func() { close(server.serveRelease) })
}

func (server *lifecycleTestServer) Stop() {
	server.stopOnce.Do(func() { close(server.stopCalled) })
	server.releaseOnce.Do(func() { close(server.serveRelease) })
}
