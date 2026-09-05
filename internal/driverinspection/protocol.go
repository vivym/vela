// Package driverinspection provides a bounded, read-only side channel which is
// independent of resident driver execution, cancellation and shutdown commands.
package driverinspection

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"net"
	"time"

	"github.com/vivym/vela/internal/strictjson"
)

const (
	Protocol    = "vela-driver-inspection-v1"
	Environment = "VELA_MODEL_DRIVER_INSPECTION_FD"
	maxPacket   = 1024
	callTimeout = time.Second
)

type Observation struct {
	Known    bool   `json:"known"`
	State    string `json:"state"`
	Sequence int64  `json:"sequence"`
}

type request struct {
	SchemaVersion   int    `json:"schema_version"`
	RequestID       uint64 `json:"request_id"`
	AuthorityDigest string `json:"authority_digest"`
}

type response struct {
	SchemaVersion   int         `json:"schema_version"`
	RequestID       uint64      `json:"request_id"`
	AuthorityDigest string      `json:"authority_digest"`
	Observation     Observation `json:"observation"`
}

type Client struct {
	conn   *net.UnixConn
	gate   chan struct{}
	nextID uint64
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

// Inspect never signals the driver or uses its command channel. Timeouts affect
// only this query; late datagrams cannot be accepted for a later request ID.
func (client *Client) Inspect(ctx context.Context, digest string) (Observation, error) {
	if client == nil || client.conn == nil || ctx == nil || !validDigest(digest) {
		return Observation{}, errors.New("driver inspection request is invalid")
	}
	ctx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	select {
	case client.gate <- struct{}{}:
		defer func() { <-client.gate }()
	case <-ctx.Done():
		return Observation{}, ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return Observation{}, err
	}
	if client.nextID == math.MaxUint64 {
		return Observation{}, errors.New("driver inspection request IDs are exhausted")
	}
	client.nextID++
	query := request{SchemaVersion: 1, RequestID: client.nextID, AuthorityDigest: digest}
	encoded, err := json.Marshal(query)
	if err != nil {
		return Observation{}, err
	}
	deadline, _ := ctx.Deadline()
	if err := client.conn.SetDeadline(deadline); err != nil {
		return Observation{}, err
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
		return Observation{}, queryError(ctx, err)
	}
	packet := make([]byte, maxPacket+1)
	for range 32 {
		n, err := client.conn.Read(packet)
		if err != nil {
			return Observation{}, queryError(ctx, err)
		}
		var reply response
		if err := decode(packet[:n], &reply); err != nil {
			return Observation{}, err
		}
		if reply.SchemaVersion != 1 || reply.RequestID == 0 || !validDigest(reply.AuthorityDigest) || !validObservation(reply.Observation) {
			return Observation{}, errors.New("driver inspection response is invalid")
		}
		if reply.RequestID < query.RequestID {
			continue
		}
		if reply.RequestID != query.RequestID || reply.AuthorityDigest != query.AuthorityDigest {
			return Observation{}, errors.New("driver inspection response identity is mismatched")
		}
		if err := ctx.Err(); err != nil {
			return Observation{}, err
		}
		if !time.Now().Before(deadline) {
			return Observation{}, context.DeadlineExceeded
		}
		return reply.Observation, nil
	}
	return Observation{}, errors.New("driver inspection stale response bound exceeded")
}

// Serve owns only the inspection socket. lookup must read an immutable snapshot
// without waiting on execution or filesystem work. Errors never stop the driver.
func Serve(ctx context.Context, conn *net.UnixConn, lookup func(string) Observation) error {
	if ctx == nil || conn == nil || lookup == nil {
		return errors.New("driver inspection server is not configured")
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
		if query.SchemaVersion != 1 || query.RequestID == 0 || !validDigest(query.AuthorityDigest) {
			return errors.New("driver inspection query is invalid")
		}
		observation := lookup(query.AuthorityDigest)
		if !validObservation(observation) {
			return errors.New("driver inspection snapshot is invalid")
		}
		encoded, err := json.Marshal(response{SchemaVersion: 1, RequestID: query.RequestID,
			AuthorityDigest: query.AuthorityDigest, Observation: observation})
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

func decode(encoded []byte, value any) error {
	if len(encoded) == 0 || len(encoded) > maxPacket || !json.Valid(encoded) {
		return errors.New("driver inspection packet exceeds bounds or is malformed")
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return errors.New("driver inspection packet has duplicate keys")
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	return decoder.Decode(value)
}

func validDigest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == 32 && hex.EncodeToString(decoded) == value
}

func validObservation(value Observation) bool {
	if !value.Known {
		return value.State == "" && value.Sequence == 0
	}
	if value.Sequence < 0 {
		return false
	}
	switch value.State {
	case "PREPARING", "PREPARED", "RUNNING", "CANCELING", "STOPPED", "OUTPUT_READY", "OUTPUT_SEALED", "FAILED":
		return true
	}
	return false
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
