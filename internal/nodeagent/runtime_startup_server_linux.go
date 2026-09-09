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
	listener                                 *net.UnixListener
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
	return server.HandleCaller(exchange, caller)
}

// HandleCaller consumes an already authenticated RuntimeCaller. It is the
// single-handshake production path used after Node reservation; it never reads
// another challenge or reconstructs identity from a request.
func (server *RuntimeStartupServer) HandleCaller(ctx context.Context, caller *RuntimeCaller) error {
	if server == nil || ctx == nil || caller == nil {
		return ErrRuntimeCallerIdentity
	}
	defer func() { _ = caller.Close() }()
	server.authenticated.Add(1)
	request, err := modelruntime.ParseBackendStartupRequest(caller.Payload())
	if err != nil {
		server.failed.Add(1)
		return err
	}
	wire, err := server.coordinator.HandleBackendStartupWithCaller(ctx, caller, request)
	if err != nil {
		server.failed.Add(1)
		return err
	}
	if err := caller.Reply(ctx, wire); err != nil {
		server.failed.Add(1)
		server.coordinator.stop()
		return err
	}
	server.replied.Add(1)
	return nil
}

func (server *RuntimeStartupServer) Serve(ctx context.Context, listener *net.UnixListener) error {
	if server == nil || ctx == nil || listener == nil || listener.Addr().Network() != "unixpacket" {
		return ErrRuntimeCallerIdentity
	}
	listener.SetUnlinkOnClose(false)
	server.mu.Lock()
	if server.done != nil || server.stopped {
		server.mu.Unlock()
		return net.ErrClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	server.cancel = cancel
	server.listener = listener
	server.done = make(chan struct{})
	server.mu.Unlock()
	stopListener := context.AfterFunc(ctx, func() { _ = listener.Close() })
	defer stopListener()
	var wg sync.WaitGroup
	defer func() {
		cancel()
		if server.coordinator != nil {
			server.coordinator.stop()
		}
		_ = listener.Close()
		wg.Wait()
		server.mu.Lock()
		server.stopped = true
		server.listener = nil
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
	done := server.stop()
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

// stop is terminal even before Serve has started. It closes admission and
// cancels active exchanges without waiting for handlers to return.
func (server *RuntimeStartupServer) stop() <-chan struct{} {
	server.mu.Lock()
	defer server.mu.Unlock()
	server.stopped = true
	if server.cancel != nil {
		server.cancel()
	}
	if server.listener != nil {
		_ = server.listener.Close()
	}
	if server.coordinator != nil {
		server.coordinator.stop()
	}
	return server.done
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
