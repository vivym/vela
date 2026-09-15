package nodeagent

import (
	"context"
	"errors"
	"net"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vivym/vela/internal/modelruntime"
	"github.com/vivym/vela/internal/runtimechannel"
	"golang.org/x/sys/unix"
)

const MaximumJournalConnections = 64

// Credentials are resolved by trusted assembly, never by a request. They only
// filter handshake admission; JournalEndpoint still authenticates exact roles.
type JournalServerConfig struct {
	Credentials     []RuntimeCallerCredentials
	MaxConcurrent   int
	ExchangeTimeout time.Duration
}

// Replied counts transmitted responses, including domain rejections. It does
// not prove receipt or count committed mutations. Accepted includes overloads.
// Authenticated is a verified handshake, not an authorized journal role.
// Concurrent snapshots are not transactional; final counters balance after join.
type JournalServerStats struct {
	Accepted, Overloaded, Authenticated, Replied, Failed uint64
	InFlight, PeakInFlight                               int64
}

type JournalServer struct {
	handler JournalHandler
	config  JournalServerConfig
	mu      sync.Mutex
	stopped bool
	cancel  context.CancelFunc
	done    chan struct{}

	accepted, overloaded, authenticated, replied, failed atomic.Uint64
	inFlight, peakInFlight                               atomic.Int64
}

// JournalHandler is the authenticated domain boundary behind the Node's
// seqpacket accept loop. It deliberately receives RuntimeCaller rather than
// an arbitrary payload so every handler can re-check the original kernel
// process identity before applying a domain operation.
type JournalHandler interface {
	Handle(context.Context, *RuntimeCaller) ([]byte, error)
}

func NewJournalServer(endpoint *JournalEndpoint, config JournalServerConfig) (*JournalServer, error) {
	if endpoint == nil {
		return nil, errors.New("invalid Node journal server endpoint")
	}
	return NewJournalServerForHandler(endpoint, config)
}

// NewJournalServerForHandler constructs the same bounded, root-owned accept
// loop for a non-execution Worker journal handler. The handler owns its own
// domain state and process binding; this constructor only owns transport
// authentication, overload and shutdown.
func NewJournalServerForHandler(handler JournalHandler, config JournalServerConfig) (*JournalServer, error) {
	if handler == nil || os.Geteuid() != 0 || os.Getegid() != 0 || len(config.Credentials) == 0 || len(config.Credentials) > 2 ||
		config.MaxConcurrent < 1 || config.MaxConcurrent > MaximumJournalConnections ||
		config.ExchangeTimeout <= 0 || config.ExchangeTimeout > runtimechannel.ExchangeTimeout {
		return nil, errors.New("invalid Node journal server configuration")
	}
	for i, credential := range config.Credentials {
		if credential.UID == 0 || credential.GID == 0 || credential.UID == ^uint32(0) || credential.GID == ^uint32(0) ||
			slices.Contains(config.Credentials[:i], credential) {
			return nil, ErrRuntimeCallerIdentity
		}
	}
	if endpoint, ok := handler.(*JournalEndpoint); ok {
		endpoint.mu.Lock()
		defer endpoint.mu.Unlock()
		if endpoint.owner == nil || endpoint.runtime == nil || endpoint.worker == nil {
			return nil, ErrRuntimeNamespaceOwnerLost
		}
	}
	config.Credentials = slices.Clone(config.Credentials)
	return &JournalServer{handler: handler, config: config}, nil
}

func (server *JournalServer) Stats() JournalServerStats {
	return JournalServerStats{Accepted: server.accepted.Load(), Overloaded: server.overloaded.Load(),
		Authenticated: server.authenticated.Load(), Replied: server.replied.Load(), Failed: server.failed.Load(),
		InFlight: server.inFlight.Load(), PeakInFlight: server.peakInFlight.Load()}
}

// Serve owns listener and accepted connections after successful entry. Trusted
// assembly supplies a protected filesystem socket; this method does not create,
// chmod, unlink or enroll anything. Only one Serve is allowed per server.
// At most MaxConcurrent exchanges allocate handshake buffers/retain callers.
// One additional accepted socket is immediately closed on overload; the kernel
// listen backlog is separate. No waiting Go queue or overload goroutine is made.
func (server *JournalServer) Serve(ctx context.Context, listener *net.UnixListener) error {
	if server == nil || server.handler == nil || ctx == nil || listener == nil || listener.Addr().Network() != "unixpacket" {
		return errors.New("node journal server requires context and Unix seqpacket listener")
	}
	server.mu.Lock()
	if server.stopped || server.done != nil {
		server.mu.Unlock()
		return net.ErrClosed
	}
	ctx, cancel := context.WithCancel(ctx)
	server.cancel, server.done = cancel, make(chan struct{})
	server.mu.Unlock()
	// Go's default unlink-on-close could delete a replacement pathname. Socket
	// retirement belongs to the protected assembly that knows its exact identity.
	listener.SetUnlinkOnClose(false)
	var active sync.WaitGroup
	listenerClosed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = listener.Close(); close(listenerClosed) })
	defer func() {
		cancel()
		if !stop() {
			<-listenerClosed
		} else {
			_ = listener.Close()
		}
		active.Wait()
		server.mu.Lock()
		server.stopped = true
		close(server.done)
		server.mu.Unlock()
	}()
	slots := make(chan struct{}, server.config.MaxConcurrent)
	if err := listener.SetDeadline(time.Time{}); err != nil {
		return err
	}
	for {
		connection, err := listener.AcceptUnix()
		if err != nil {
			return errors.Join(err, context.Cause(ctx))
		}
		server.accepted.Add(1)
		deadline := time.Now().Add(server.config.ExchangeTimeout)
		select {
		case slots <- struct{}{}:
		default:
			server.overloaded.Add(1)
			_ = connection.Close()
			continue
		}
		count := server.inFlight.Add(1)
		for peak := server.peakInFlight.Load(); count > peak; peak = server.peakInFlight.Load() {
			if server.peakInFlight.CompareAndSwap(peak, count) {
				break
			}
		}
		active.Add(1)
		go func() {
			defer active.Done()
			defer func() { server.inFlight.Add(-1); <-slots }()
			server.exchange(ctx, connection, deadline)
		}()
	}
}

func (server *JournalServer) exchange(parent context.Context, connection *net.UnixConn, deadline time.Time) {
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	closed := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { _ = connection.Close(); close(closed) })
	defer func() {
		if !stop() {
			<-closed
		} else {
			_ = connection.Close()
		}
	}()
	credential, err := server.credentials(connection)
	if err != nil {
		server.failed.Add(1)
		return
	}
	caller, err := ReceiveRuntimeCallerWithRequestLimit(ctx, connection, credential, modelruntime.MaximumJournalCommandBytes)
	if err != nil {
		server.failed.Add(1)
		return
	}
	defer func() { _ = caller.Close() }()
	server.authenticated.Add(1)
	reply, err := server.handler.Handle(ctx, caller)
	if err == nil {
		err = caller.Reply(ctx, reply)
	}
	if err != nil {
		server.failed.Add(1)
	} else {
		server.replied.Add(1)
	}
}

func (server *JournalServer) credentials(connection *net.UnixConn) (RuntimeCallerCredentials, error) {
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
	credential := RuntimeCallerCredentials{UID: peer.Uid, GID: peer.Gid}
	if !slices.Contains(server.config.Credentials, credential) {
		return RuntimeCallerCredentials{}, ErrRuntimeCallerIdentity
	}
	return credential, nil
}

// Shutdown cancels accept/handshake/reply I/O and joins all accepted work. A
// timeout does not prove that a synchronous journal fsync or handler stopped;
// retain the endpoint and owner until Serve actually returns. Neither is closed
// here. Shutdown before Serve permanently prevents starting this server.
func (server *JournalServer) Shutdown(ctx context.Context) error {
	if server == nil || ctx == nil {
		return errors.New("node journal shutdown requires server and context")
	}
	server.mu.Lock()
	server.stopped = true
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
