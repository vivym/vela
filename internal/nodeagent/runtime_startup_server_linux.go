package nodeagent

import (
	"context"
	"errors"
	"net"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

// RuntimeStartupServer authenticates one-shot ModelRuntime startup requests and
// delegates the decision to one RuntimeStartupCoordinator. It owns no policy,
// socket path, chmod or unlink operation; trusted Node assembly owns those.
type RuntimeStartupServer struct {
	coordinator                              *RuntimeStartupCoordinator
	credentials                              []RuntimeCallerCredentials
	timeout                                  time.Duration
	mu                                       sync.Mutex
	stopped                                  bool
	cancel                                   context.CancelFunc
	done                                     chan struct{}
	accepted, authenticated, replied, failed atomic.Uint64
}

func NewRuntimeStartupServer(coordinator *RuntimeStartupCoordinator, credentials []RuntimeCallerCredentials, timeout time.Duration) (*RuntimeStartupServer, error) {
	if coordinator == nil || len(credentials) == 0 || timeout <= 0 || timeout > runtimechannel.ExchangeTimeout {
		return nil, ErrRuntimeCallerIdentity
	}
	for i, c := range credentials {
		if c.UID == 0 || c.GID == 0 || c.UID == ^uint32(0) || c.GID == ^uint32(0) || slices.Contains(credentials[:i], c) {
			return nil, ErrRuntimeCallerIdentity
		}
	}
	return &RuntimeStartupServer{coordinator: coordinator, credentials: slices.Clone(credentials), timeout: timeout}, nil
}

func (server *RuntimeStartupServer) HandleConnection(ctx context.Context, connection *net.UnixConn) error {
	if server == nil || ctx == nil || connection == nil {
		return ErrRuntimeCallerIdentity
	}
	deadline := time.Now().Add(server.timeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	exchange, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	credential, err := startupPeerCredential(connection)
	if err != nil {
		server.failed.Add(1)
		return err
	}
	if !slices.Contains(server.credentials, credential) {
		server.failed.Add(1)
		return ErrRuntimeCallerIdentity
	}
	caller, err := ReceiveRuntimeCallerWithRequestLimit(exchange, connection, credential, 4096)
	if err != nil {
		server.failed.Add(1)
		return err
	}
	defer func() { _ = caller.Close() }()
	server.authenticated.Add(1)
	request, err := modelruntime.ParseBackendStartupRequest(caller.Payload())
	if err != nil {
		server.failed.Add(1)
		return err
	}
	wire, err := server.coordinator.HandleBackendStartup(exchange, request)
	if err != nil {
		server.failed.Add(1)
		return err
	}
	if err := caller.Reply(exchange, wire); err != nil {
		server.failed.Add(1)
		return err
	}
	server.replied.Add(1)
	return nil
}

func (server *RuntimeStartupServer) Serve(ctx context.Context, listener *net.UnixListener) error {
	if server == nil || ctx == nil || listener == nil || listener.Addr().Network() != "unixpacket" {
		return ErrRuntimeCallerIdentity
	}
	server.mu.Lock()
	if server.done != nil || server.stopped {
		server.mu.Unlock()
		return net.ErrClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	server.cancel = cancel
	server.done = make(chan struct{})
	server.mu.Unlock()
	listener.SetUnlinkOnClose(false)
	var wg sync.WaitGroup
	defer func() {
		cancel()
		_ = listener.Close()
		wg.Wait()
		server.mu.Lock()
		server.stopped = true
		close(server.done)
		server.mu.Unlock()
	}()
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			return errors.Join(err, context.Cause(ctx))
		}
		server.accepted.Add(1)
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { _ = connection.Close() }()
			if err := server.HandleConnection(ctx, connection); err != nil {
				server.failed.Add(1)
			}
		}()
	}
}
func (server *RuntimeStartupServer) Shutdown(ctx context.Context) error {
	if server == nil || ctx == nil {
		return ErrRuntimeCallerIdentity
	}
	server.mu.Lock()
	if server.cancel != nil {
		server.cancel()
	}
	done := server.done
	server.mu.Unlock()
	if done == nil {
		return nil
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}
func (server *RuntimeStartupServer) Stats() (accepted, authenticated, replied, failed uint64) {
	return server.accepted.Load(), server.authenticated.Load(), server.replied.Load(), server.failed.Load()
}
func startupPeerCredential(connection *net.UnixConn) (RuntimeCallerCredentials, error) {
	raw, err := connection.SyscallConn()
	if err != nil {
		return RuntimeCallerCredentials{}, err
	}
	var peer *unix.Ucred
	var peerErr error
	if err := raw.Control(func(fd uintptr) { peer, peerErr = unix.GetsockoptUcred(int(fd), unix.SOL_SOCKET, unix.SO_PEERCRED) }); err != nil {
		return RuntimeCallerCredentials{}, err
	}
	if peerErr != nil {
		return RuntimeCallerCredentials{}, peerErr
	}
	return RuntimeCallerCredentials{UID: peer.Uid, GID: peer.Gid}, nil
}
