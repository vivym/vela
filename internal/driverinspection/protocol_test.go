//go:build darwin || linux

package driverinspection

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"strings"
	"testing"
	"time"
)

func testPair(t *testing.T) (*Client, *net.UnixConn) {
	t.Helper()
	parent, childFile, err := Pair()
	if err != nil {
		t.Fatal(err)
	}
	child, err := fromFile(childFile)
	_ = childFile.Close()
	if err != nil {
		_ = parent.Close()
		t.Fatal(err)
	}
	client := NewClient(parent)
	t.Cleanup(func() { _ = client.Close(); _ = child.Close() })
	return client, child
}

func TestInspectionChannelReadsExactSnapshotsAndCloses(t *testing.T) {
	client, peer := testPair(t)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	digest := strings.Repeat("a", 64)
	go func() {
		done <- Serve(ctx, peer, func(query string) Observation {
			if query == digest {
				return Observation{Known: true, State: "RUNNING", Sequence: 7}
			}
			return Observation{}
		})
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if !errors.Is(err, context.Canceled) {
				t.Errorf("inspection server close: %v", err)
			}
		case <-time.After(time.Second):
			t.Error("inspection server did not exit")
		}
	})
	for _, query := range []string{strings.Repeat("b", 64), digest, digest} {
		result, err := client.Inspect(context.Background(), query)
		if err != nil || result.Known != (query == digest) || (result.Known && (result.State != "RUNNING" || result.Sequence != 7)) {
			t.Fatalf("snapshot query %s: %+v %v", query, result, err)
		}
	}
}

func TestInspectionChannelRejectsLateSuccessWithoutPoisoningNextQuery(t *testing.T) {
	client, peer := testPair(t)
	digest := strings.Repeat("a", 64)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	firstResult := make(chan error, 1)
	go func() { _, err := client.Inspect(ctx, digest); firstResult <- err }()
	first := readQuery(t, peer)
	if err := <-firstResult; !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked query did not expire: %v", err)
	}
	secondResult := make(chan Observation, 1)
	secondErr := make(chan error, 1)
	go func() {
		result, err := client.Inspect(context.Background(), digest)
		secondResult <- result
		secondErr <- err
	}()
	second := readQuery(t, peer)
	writeReply(t, peer, response{SchemaVersion: 1, RequestID: first.RequestID, AuthorityDigest: digest,
		Observation: Observation{Known: true, State: "STOPPED", Sequence: 99}})
	writeReply(t, peer, response{SchemaVersion: 1, RequestID: second.RequestID, AuthorityDigest: digest,
		Observation: Observation{Known: true, State: "RUNNING", Sequence: 1}})
	result := <-secondResult
	if err := <-secondErr; err != nil || result.State != "RUNNING" || result.Sequence != 1 {
		t.Fatalf("late stopped observation contaminated next query: %+v %v", result, err)
	}
}

func TestInspectionChannelMalformedReplyDoesNotPoisonDatagrams(t *testing.T) {
	for _, mutation := range []string{"malformed", "oversized", "duplicate", "unknown field", "future request", "digest", "unknown stopped", "state", "sequence"} {
		t.Run(mutation, func(t *testing.T) {
			client, peer := testPair(t)
			digest := strings.Repeat("a", 64)
			completed := make(chan error, 1)
			go func() { _, err := client.Inspect(context.Background(), digest); completed <- err }()
			query := readQuery(t, peer)
			reply := response{SchemaVersion: 1, RequestID: query.RequestID, AuthorityDigest: digest,
				Observation: Observation{Known: true, State: "STOPPED", Sequence: 1}}
			var packet []byte
			switch mutation {
			case "malformed":
				packet = []byte("{")
			case "oversized":
				packet = []byte(strings.Repeat("x", maxPacket+1))
			case "duplicate":
				packet = []byte(`{"schema_version":1,"schema_version":1}`)
			case "unknown field":
				packet = []byte(`{"shutdown":true}`)
			case "future request":
				reply.RequestID++
			case "digest":
				reply.AuthorityDigest = strings.Repeat("b", 64)
			case "unknown stopped":
				reply.Observation.Known = false
			case "state":
				reply.Observation.State = "DRAINED"
			case "sequence":
				reply.Observation.Sequence = -1
			}
			if packet == nil {
				writeReply(t, peer, reply)
			} else if _, err := peer.Write(packet); err != nil {
				t.Fatal(err)
			}
			if err := <-completed; err == nil {
				t.Fatal("malformed response accepted")
			}
			go func() { _, err := client.Inspect(context.Background(), digest); completed <- err }()
			query = readQuery(t, peer)
			writeReply(t, peer, response{SchemaVersion: 1, RequestID: query.RequestID, AuthorityDigest: digest})
			if err := <-completed; err != nil {
				t.Fatalf("malformed datagram poisoned next query: %v", err)
			}
		})
	}
}

func TestInspectionChannelBoundsWriteBackpressureAndGateWait(t *testing.T) {
	client, peer := testPair(t)
	if err := client.conn.SetWriteBuffer(1024); err != nil {
		t.Fatal(err)
	}
	if err := client.conn.SetWriteDeadline(time.Now().Add(20 * time.Millisecond)); err != nil {
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
		t.Fatal("failed to create datagram backpressure")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if value, err := client.Inspect(ctx, strings.Repeat("a", 64)); value.Known || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("backpressure produced a state observation: %+v %v", value, err)
	}
	_ = peer.Close()
	client.gate <- struct{}{}
	ctx, cancel = context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := client.Inspect(ctx, strings.Repeat("a", 64)); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("waiting query ignored deadline: %v", err)
	}
	<-client.gate
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
