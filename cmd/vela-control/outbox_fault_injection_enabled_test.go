//go:build vela_lab_fault_injection

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/outbox"
)

func TestReadLabOutboxFaultMarkerReturnsExactPrivateFile(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	want := []byte("{\"schema\":\"vela-lab-outbox-fault-marker-v1\",\"phase\":\"publisher-post-puback-pre-mark-crash\",\"subject\":\"vela.events.job.ready\",\"event_id\":\"00000000-0000-0000-0000-000000000001\",\"broker_stream\":\"VELA_EVENTS\",\"broker_sequence\":42}\n")
	if err := os.WriteFile(markerPath, want, 0o600); err != nil {
		t.Fatalf("write lab Outbox fault marker: %v", err)
	}
	var output bytes.Buffer
	if err := readLabOutboxFaultMarker(markerPath, &output); err != nil {
		t.Fatalf("read lab Outbox fault marker: %v", err)
	}
	if !bytes.Equal(output.Bytes(), want) {
		t.Fatalf("marker output = %q, want %q", output.Bytes(), want)
	}
}

func TestReadLabOutboxFaultMarkerRejectsDuplicateKeys(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	encoded := []byte("{\"schema\":\"vela-lab-outbox-fault-marker-v1\",\"schema\":\"vela-lab-outbox-fault-marker-v1\",\"phase\":\"publisher-post-puback-pre-mark-crash\",\"subject\":\"vela.events.job.ready\",\"event_id\":\"00000000-0000-0000-0000-000000000001\",\"broker_stream\":\"VELA_EVENTS\",\"broker_sequence\":42}\n")
	if err := os.WriteFile(markerPath, encoded, 0o600); err != nil {
		t.Fatalf("write duplicate-key lab Outbox fault marker: %v", err)
	}
	if err := readLabOutboxFaultMarker(markerPath, &bytes.Buffer{}); err == nil {
		t.Fatal("read duplicate-key lab Outbox fault marker succeeded")
	}
}

func TestLabOutboxFaultBrokerPausesAtConfiguredPublisherBoundary(t *testing.T) {
	for _, test := range []struct {
		name              string
		phase             string
		wantDelegateCalls int
		wantStream        string
		wantSequence      int64
	}{
		{
			name:              "before PubAck",
			phase:             publisherPrePubAckCrash,
			wantDelegateCalls: 0,
		},
		{
			name:              "after PubAck before marker",
			phase:             publisherPostPubAckPreMarkCrash,
			wantDelegateCalls: 1,
			wantStream:        "VELA_EVENTS",
			wantSequence:      42,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			delegate := &enabledTestBroker{}
			markerPath := filepath.Join(t.TempDir(), "marker.json")
			broker, err := newLabOutboxFaultBroker(delegate, test.phase, markerPath, time.Minute)
			if err != nil {
				t.Fatalf("create lab Outbox fault Broker: %v", err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			result := make(chan error, 1)
			go func() {
				_, publishErr := broker.Publish(
					ctx,
					"vela.events.job.ready",
					"00000000-0000-0000-0000-000000000001",
					[]byte("private-payload-must-not-enter-marker"),
				)
				result <- publishErr
			}()

			var markerBytes []byte
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				markerBytes, err = os.ReadFile(markerPath)
				if err == nil {
					break
				}
				if !errors.Is(err, os.ErrNotExist) {
					t.Fatalf("read lab Outbox fault marker: %v", err)
				}
				time.Sleep(10 * time.Millisecond)
			}
			if len(markerBytes) == 0 {
				t.Fatal("lab Outbox fault marker was not written")
			}
			var marker labOutboxFaultMarker
			if err := json.Unmarshal(markerBytes, &marker); err != nil {
				t.Fatalf("decode lab Outbox fault marker: %v", err)
			}
			if marker.Schema != labOutboxFaultMarkerSchema || marker.Phase != test.phase ||
				marker.Subject != "vela.events.job.ready" ||
				marker.EventID != "00000000-0000-0000-0000-000000000001" ||
				marker.BrokerStream != test.wantStream || marker.BrokerSequence != test.wantSequence {
				t.Fatalf("lab Outbox fault marker = %#v", marker)
			}
			if delegate.callCount() != test.wantDelegateCalls {
				t.Fatalf("delegate calls = %d, want %d", delegate.callCount(), test.wantDelegateCalls)
			}
			if bytes.Contains(markerBytes, []byte("private-payload-must-not-enter-marker")) {
				t.Fatal("lab Outbox fault marker contains payload bytes")
			}

			cancel()
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("paused Publish error = %v, want context cancellation", err)
			}
		})
	}
}

func TestLabOutboxFaultBrokerPublishesNormallyAfterFaultMarkerExists(t *testing.T) {
	delegate := &enabledTestBroker{}
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	if err := os.WriteFile(markerPath, []byte("{}\n"), 0o600); err != nil {
		t.Fatalf("write prior lab Outbox fault marker: %v", err)
	}
	broker, err := newLabOutboxFaultBroker(
		delegate,
		publisherPostPubAckPreMarkCrash,
		markerPath,
		time.Minute,
	)
	if err != nil {
		t.Fatalf("create lab Outbox fault Broker: %v", err)
	}

	receipt, err := broker.Publish(
		context.Background(),
		"vela.events.job.ready",
		"00000000-0000-0000-0000-000000000001",
		[]byte("payload"),
	)
	if err != nil {
		t.Fatalf("publish after prior lab Outbox fault marker: %v", err)
	}
	if receipt.Stream != "VELA_EVENTS" || receipt.Sequence != 42 {
		t.Fatalf("receipt = %#v", receipt)
	}
	if delegate.callCount() != 1 {
		t.Fatalf("delegate calls = %d, want 1", delegate.callCount())
	}
}

type enabledTestBroker struct {
	mutex sync.Mutex
	calls int
}

func (b *enabledTestBroker) Publish(
	context.Context,
	string,
	string,
	[]byte,
) (outbox.Receipt, error) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	b.calls++
	return outbox.Receipt{Stream: "VELA_EVENTS", Sequence: 42}, nil
}

func (b *enabledTestBroker) callCount() int {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	return b.calls
}
