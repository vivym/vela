//go:build darwin || linux

package driverdrain

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/vivym/vela/internal/driverchannel"
)

func testPair(t *testing.T) (*Client, *net.UnixConn) {
	t.Helper()
	parent, file, err := driverchannel.Pair()
	if err != nil {
		t.Fatal(err)
	}
	peer, err := driverchannel.FromFile(file)
	_ = file.Close()
	if err != nil {
		_ = parent.Close()
		t.Fatal(err)
	}
	client := NewClient(parent)
	t.Cleanup(func() { _ = client.Close(); _ = peer.Close() })
	return client, peer
}

func TestDrainChannelRequiresExactProofAndJoinsServerOnClose(t *testing.T) {
	client, peer := testPair(t)
	identity := Identity{AuthorityDigest: strings.Repeat("a", 64), ExecutionSequence: 7}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Serve(ctx, peer, func(query Identity) bool { return query == identity }) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Error(err)
			}
		case <-time.After(time.Second):
			t.Error("drain server did not exit")
		}
	})
	for _, query := range []Identity{
		{AuthorityDigest: strings.Repeat("b", 64), ExecutionSequence: 7},
		{AuthorityDigest: identity.AuthorityDigest, ExecutionSequence: 8}, identity, identity,
	} {
		err := client.Drain(context.Background(), query)
		if (err == nil) != (query == identity) {
			t.Fatalf("wrong drain decision: %+v %v", query, err)
		}
	}
}

func TestDrainChannelLateProofCannotAuthorizeNextCall(t *testing.T) {
	client, peer := testPair(t)
	identity := Identity{AuthorityDigest: strings.Repeat("a", 64), ExecutionSequence: 7}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Drain(ctx, identity) }()
	first := readQuery(t, peer)
	if err := <-done; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	go func() { done <- client.Drain(context.Background(), identity) }()
	second := readQuery(t, peer)
	writeReply(t, peer, response{SchemaVersion: 1, RequestID: first.RequestID, Identity: identity, Drained: true, Contract: Contract})
	writeReply(t, peer, response{SchemaVersion: 1, RequestID: second.RequestID, Identity: identity})
	if err := <-done; !errors.Is(err, ErrUnproven) {
		t.Fatalf("late proof authorized later unproven reply: %v", err)
	}
}

func TestDrainChannelRejectsFaultsAndAllowsFreshRetry(t *testing.T) {
	for _, fault := range []string{"malformed", "oversized", "duplicate", "unknown", "schema", "future", "digest", "sequence", "contract", "unproven-contract", "null", "missing-proof"} {
		t.Run(fault, func(t *testing.T) {
			client, peer := testPair(t)
			identity := Identity{AuthorityDigest: strings.Repeat("a", 64), ExecutionSequence: 7}
			done := make(chan error, 1)
			go func() { done <- client.Drain(context.Background(), identity) }()
			query := readQuery(t, peer)
			reply := response{SchemaVersion: 1, RequestID: query.RequestID, Identity: identity, Drained: true, Contract: Contract}
			var packet []byte
			switch fault {
			case "malformed":
				packet = []byte("{")
			case "oversized":
				packet = []byte(strings.Repeat("x", maxPacket+1))
			case "duplicate":
				packet = []byte(`{"schema_version":1,"schema_version":1}`)
			case "unknown":
				packet = []byte(`{"shutdown":true}`)
			case "schema":
				reply.SchemaVersion++
			case "future":
				reply.RequestID++
			case "digest":
				reply.Identity.AuthorityDigest = strings.Repeat("b", 64)
			case "sequence":
				reply.Identity.ExecutionSequence++
			case "contract":
				reply.Contract = "stopped"
			case "unproven-contract":
				reply.Drained = false
			case "null":
				packet = []byte("null")
			case "missing-proof":
				reply.Drained, reply.Contract = false, ""
			}
			if packet == nil {
				writeReply(t, peer, reply)
			} else if _, err := peer.Write(packet); err != nil {
				t.Fatal(err)
			}
			if err := <-done; err == nil {
				t.Fatal("invalid drain accepted")
			}
			go func() { done <- client.Drain(context.Background(), identity) }()
			query = readQuery(t, peer)
			writeReply(t, peer, response{SchemaVersion: 1, RequestID: query.RequestID, Identity: identity, Drained: true, Contract: Contract})
			if err := <-done; err != nil {
				t.Fatalf("fault poisoned retry: %v", err)
			}
		})
	}
}

func TestDrainChannelBoundsBackpressureGateWaitAndRequestIDs(t *testing.T) {
	client, _ := testPair(t)
	identity := Identity{AuthorityDigest: strings.Repeat("a", 64), ExecutionSequence: 7}
	if err := client.conn.SetWriteBuffer(1024); err != nil {
		t.Fatal(err)
	}
	if err := client.conn.SetWriteDeadline(time.Now().Add(30 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	blocked := false
	for range 1024 {
		if _, err := client.conn.Write(make([]byte, 512)); err != nil {
			blocked = true
			break
		}
	}
	if !blocked {
		t.Fatal("datagram backpressure was not created")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := client.Drain(ctx, identity); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	client.gate <- struct{}{}
	ctx, cancel = context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := client.Drain(ctx, identity); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
	<-client.gate
	client.nextID = math.MaxUint64
	if err := client.Drain(context.Background(), identity); err == nil {
		t.Fatal("request IDs wrapped")
	}
}

func TestDrainChannelInvalidRequestsNeverInvokeFreeze(t *testing.T) {
	for _, packet := range []string{`null`, `{}`, `{"schema_version":1,"request_id":1,"identity":{"authority_digest":"a","execution_sequence":7}}`,
		`{"schema_version":1,"request_id":1,"identity":{"authority_digest":"` + strings.Repeat("a", 64) + `","execution_sequence":0}}`} {
		client, peer := testPair(t)
		done := make(chan error, 1)
		go func() {
			done <- Serve(context.Background(), peer, func(Identity) bool { t.Error("invalid query entered freeze"); return true })
		}()
		if _, err := client.conn.Write([]byte(packet)); err != nil {
			t.Fatal(err)
		}
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("invalid query accepted")
			}
		case <-time.After(time.Second):
			t.Fatal("invalid query did not close channel")
		}
	}
}

func readQuery(t *testing.T, peer *net.UnixConn) request {
	t.Helper()
	if err := peer.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	packet := make([]byte, maxPacket+1)
	n, err := peer.Read(packet)
	if err != nil {
		t.Fatal(err)
	}
	var query request
	if err := decode(packet[:n], &query); err != nil {
		t.Fatal(err)
	}
	return query
}

func writeReply(t *testing.T, peer *net.UnixConn, reply response) {
	t.Helper()
	packet, err := json.Marshal(reply)
	if err != nil {
		t.Fatal(err)
	}
	if err := peer.SetWriteDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := peer.Write(packet); err != nil {
		t.Fatal(err)
	}
}
