// Package driverdrain carries execution writer-drain acknowledgements over a
// dedicated bounded channel, independent of driver commands and inspection.
package driverdrain

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net"
	"time"

	"github.com/vivym/vela/internal/driverchannel"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	Protocol    = "vela-driver-drain-v1"
	Contract    = "vela-execution-writer-drain-v1"
	Environment = "VELA_MODEL_DRIVER_DRAIN_FD"
	maxPacket   = 1024
	callTimeout = time.Second
)

var ErrUnproven = errors.New("driver execution writer drain is unproven")

type Identity struct {
	AuthorityDigest   string `json:"authority_digest"`
	ExecutionSequence int64  `json:"execution_sequence"`
}

type request struct {
	SchemaVersion int      `json:"schema_version"`
	RequestID     uint64   `json:"request_id"`
	Identity      Identity `json:"identity"`
}

type response struct {
	SchemaVersion int      `json:"schema_version"`
	RequestID     uint64   `json:"request_id"`
	Identity      Identity `json:"identity"`
	Drained       bool     `json:"drained"`
	Contract      string   `json:"contract"`
}

type Client struct {
	conn   *net.UnixConn
	gate   chan struct{}
	nextID uint64
}

func OpenInherited(value string) (*net.UnixConn, error) {
	return driverchannel.OpenInherited(value, 4)
}

func NewClient(conn *net.UnixConn) *Client {
	return &Client{conn: conn, gate: make(chan struct{}, 1)}
}

func (client *Client) Close() error {
	if client == nil || client.conn == nil {
		return nil
	}
	return client.conn.Close()
}

// Drain accepts only an exact positive acknowledgement. An expired call may
// have frozen execution but cannot prove it; a retry needs its own reply.
func (client *Client) Drain(ctx context.Context, identity Identity) error {
	if client == nil || client.conn == nil || ctx == nil || !validIdentity(identity) {
		return ErrUnproven
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	select {
	case client.gate <- struct{}{}:
		defer func() { <-client.gate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if client.nextID == math.MaxUint64 {
		return errors.New("driver drain request IDs are exhausted")
	}
	client.nextID++
	query := request{SchemaVersion: 1, RequestID: client.nextID, Identity: identity}
	encoded, err := json.Marshal(query)
	if err != nil {
		return err
	}
	deadline, _ := ctx.Deadline()
	if err := client.conn.SetDeadline(deadline); err != nil {
		return err
	}
	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = client.conn.SetDeadline(time.Now())
		close(finished)
	})
	defer func() {
		if !stop() {
			<-finished
		}
		_ = client.conn.SetDeadline(time.Time{})
	}()
	if _, err := client.conn.Write(encoded); err != nil {
		return queryError(ctx, err)
	}
	packet := make([]byte, maxPacket+1)
	for range 32 {
		n, err := client.conn.Read(packet)
		if err != nil {
			return queryError(ctx, err)
		}
		var reply response
		if err := decode(packet[:n], &reply); err != nil {
			return err
		}
		if reply.SchemaVersion != 1 || reply.RequestID == 0 || !validIdentity(reply.Identity) ||
			(reply.Drained && reply.Contract != Contract) || (!reply.Drained && reply.Contract != "") {
			return ErrUnproven
		}
		if reply.RequestID < query.RequestID {
			continue
		}
		if reply.RequestID != query.RequestID || reply.Identity != query.Identity {
			return ErrUnproven
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		if !reply.Drained {
			return ErrUnproven
		}
		return nil
	}
	return errors.New("driver drain stale response bound exceeded")
}

// Serve owns only the drain socket. freeze must be nonblocking: either join
// already-finished writers and irreversibly freeze this exact execution, or
// return false. It must never start execution or stop the resident model.
func Serve(ctx context.Context, conn *net.UnixConn, freeze func(Identity) bool) error {
	if ctx == nil || conn == nil || freeze == nil {
		return ErrUnproven
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	packet := make([]byte, maxPacket+1)
	for {
		n, err := conn.Read(packet)
		if err != nil {
			return queryError(ctx, err)
		}
		var query request
		if err := decode(packet[:n], &query); err != nil {
			return err
		}
		if query.SchemaVersion != 1 || query.RequestID == 0 || !validIdentity(query.Identity) {
			return ErrUnproven
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		reply := response{SchemaVersion: 1, RequestID: query.RequestID, Identity: query.Identity, Drained: freeze(query.Identity)}
		if reply.Drained {
			reply.Contract = Contract
		}
		encoded, err := json.Marshal(reply)
		if err != nil {
			return err
		}
		if err := conn.SetWriteDeadline(time.Now().Add(callTimeout)); err != nil {
			return err
		}
		if _, err := conn.Write(encoded); err != nil {
			if timeout, ok := err.(net.Error); ok && timeout.Timeout() && ctx.Err() == nil {
				continue
			}
			return queryError(ctx, err)
		}
	}
}

func validIdentity(identity Identity) bool {
	decoded, err := hex.DecodeString(identity.AuthorityDigest)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == identity.AuthorityDigest && identity.ExecutionSequence > 0
}

func decode(encoded []byte, value any) error {
	if len(encoded) == 0 || len(encoded) > maxPacket || !json.Valid(encoded) {
		return ErrUnproven
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return ErrUnproven
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}

func queryError(ctx context.Context, err error) error {
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if timeout, ok := err.(net.Error); ok && timeout.Timeout() {
		return context.DeadlineExceeded
	}
	return err
}
