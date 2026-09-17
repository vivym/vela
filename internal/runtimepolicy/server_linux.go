//go:build linux

package runtimepolicy

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"

	"github.com/vivym/vela/internal/strictjson"
	"golang.org/x/sys/unix"
)

// Authorizer is the independent reservation boundary. Implementations must
// reject requests that are not backed by Fleet evidence.
type Authorizer interface {
	Authorize(context.Context, Request) (time.Time, error)
}

// Server signs a short-lived Reply only after Authorizer approves the exact
// request. The listener is expected to be root-owned and supervised by the
// host service manager.
type Server struct {
	Listener   *net.UnixListener
	PrivateKey ed25519.PrivateKey
	Authorizer Authorizer
	ReplyCache ReplyCache
	Now        func() time.Time
}

func (server *Server) Serve(ctx context.Context) error {
	if ctx == nil || server == nil || server.Listener == nil || len(server.PrivateKey) != ed25519.PrivateKeySize || server.Authorizer == nil {
		return errors.New("runtime policy server is incomplete")
	}
	now := server.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	var closeOnce sync.Once
	closeListener := func() { closeOnce.Do(func() { _ = server.Listener.Close() }) }
	defer closeListener()
	stopped := make(chan struct{})
	defer close(stopped)
	go func() {
		select {
		case <-ctx.Done():
			closeListener()
		case <-stopped:
		}
	}()
	var workers sync.WaitGroup
	slots := make(chan struct{}, 16)
	for {
		connection, err := server.Listener.AcceptUnix()
		if err != nil {
			if ctx.Err() != nil {
				workers.Wait()
				return ctx.Err()
			}
			if errors.Is(err, net.ErrClosed) {
				workers.Wait()
				return nil
			}
			workers.Wait()
			return err
		}
		select {
		case slots <- struct{}{}:
		default:
			_ = connection.Close()
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			defer func() { <-slots }()
			_ = server.handle(ctx, connection, now)
		}()
	}
}

func (server *Server) handle(ctx context.Context, connection *net.UnixConn, now func() time.Time) error {
	defer func(cleanup func() error) { _ = cleanup() }(connection.Close)
	if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return err
	}
	stop := context.AfterFunc(ctx, func() { _ = connection.Close() })
	defer stop()
	if err := requireRootPeer(connection); err != nil {
		return err
	}
	buffer := make([]byte, 64<<10)
	oob := make([]byte, unix.CmsgSpace(16*4))
	n, oobn, flags, _, err := connection.ReadMsgUnix(buffer, oob)
	ancillaryErr := closeAncillaryRights(oob[:oobn])
	if err != nil {
		return err
	}
	if n <= 0 || oobn != 0 || ancillaryErr != nil || flags&(unix.MSG_TRUNC|unix.MSG_CTRUNC) != 0 {
		return errors.New("runtime policy request frame is invalid")
	}
	if err := strictjson.RejectDuplicateKeys(buffer[:n]); err != nil {
		return err
	}
	var request Request
	decoder := json.NewDecoder(bytes.NewReader(buffer[:n]))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&request); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("runtime policy request has trailing data")
	}
	if !request.Valid() {
		return errors.New("runtime policy request is invalid")
	}
	if server.ReplyCache != nil {
		if cached, found, err := server.ReplyCache.Load(ctx, request, server.PrivateKey.Public().(ed25519.PublicKey)); err != nil {
			return err
		} else if found {
			return writeReply(connection, cached)
		}
	}
	authorizationExpiry, err := server.Authorizer.Authorize(ctx, request)
	if err != nil {
		return err
	}
	issued := now().UTC()
	if authorizationExpiry.IsZero() || !authorizationExpiry.After(issued) {
		return errors.New("runtime policy authorization expiry is invalid")
	}
	expires := issued.Add(2 * time.Minute)
	if authorizationExpiry.Before(expires) {
		expires = authorizationExpiry
	}
	evidence := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(evidence); err != nil {
		return err
	}
	reply := Reply{Version: ProtocolVersion, OperationID: request.OperationID, JournalID: request.JournalID, RequestDigest: request.RequestDigest, ReservationDigest: request.ReservationDigest, EvidenceDigest: sha256Digest(evidence), IssuedAt: issued, ExpiresAt: expires}
	signingBytes, err := SigningBytes(reply)
	if err != nil {
		return err
	}
	reply.Signature = ed25519.Sign(server.PrivateKey, signingBytes)
	if server.ReplyCache != nil {
		if err := server.ReplyCache.Store(ctx, request, reply); err != nil {
			return err
		}
	}
	return writeReply(connection, reply)
}

func writeReply(connection *net.UnixConn, reply Reply) error {
	wire, err := json.Marshal(reply)
	if err != nil || len(wire) > 64<<10 {
		return errors.New("runtime policy reply frame is invalid")
	}
	written, err := connection.Write(wire)
	if err != nil {
		return err
	}
	if written != len(wire) {
		return io.ErrShortWrite
	}
	return nil
}

func sha256Digest(value []byte) [32]byte {
	return sha256.Sum256(value)
}
