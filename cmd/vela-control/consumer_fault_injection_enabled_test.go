//go:build vela_lab_fault_injection

package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vivym/vela/internal/eventstream"
	"github.com/vivym/vela/internal/inbox"
)

func TestReadLabConsumerFaultMarkerRejectsDuplicateKeysAndExtraFields(t *testing.T) {
	valid := "{\"schema\":\"vela-lab-consumer-fault-marker-v1\",\"phase\":\"consumer-post-db-pre-ack-crash\",\"subject\":\"vela.events.job.ready\",\"event_id\":\"00000000-0000-0000-0000-000000000001\",\"stream\":\"VELA_EVENTS\",\"consumer\":\"VELA_SCHEDULER\",\"stream_sequence\":42,\"consumer_sequence\":17,\"num_delivered\":1,\"inbox_committed\":true"
	for _, test := range []struct {
		name    string
		encoded string
	}{
		{
			name:    "duplicate key",
			encoded: valid + ",\"schema\":\"vela-lab-consumer-fault-marker-v1\"}\n",
		},
		{
			name:    "extra field",
			encoded: valid + ",\"unexpected\":true}\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			markerPath := filepath.Join(t.TempDir(), "marker.json")
			if err := os.WriteFile(markerPath, []byte(test.encoded), 0o600); err != nil {
				t.Fatalf("write invalid lab consumer fault marker: %v", err)
			}
			if err := readLabConsumerFaultMarker(markerPath, &bytes.Buffer{}); err == nil {
				t.Fatal("read invalid lab consumer fault marker succeeded")
			}
		})
	}
}

func TestLabConsumerFaultReadCommandRejectsAdditionalArguments(t *testing.T) {
	handled, err := runLabConsumerFaultCommand(
		[]string{labConsumerFaultReadMarkerArg, "unexpected"},
		&bytes.Buffer{},
	)
	if !handled || err == nil {
		t.Fatalf("read consumer marker command handled = %t, error = %v", handled, err)
	}
}

func TestWriteLabConsumerFaultMarkerDoesNotReplaceExistingMarker(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	want := []byte("existing\n")
	if err := os.WriteFile(markerPath, want, 0o600); err != nil {
		t.Fatalf("write existing lab consumer fault marker: %v", err)
	}
	if err := writeLabConsumerFaultMarker(markerPath, labConsumerFaultMarker{}); err == nil {
		t.Fatal("replaced existing lab consumer fault marker")
	}
	got, err := os.ReadFile(markerPath)
	if err != nil {
		t.Fatalf("read existing lab consumer fault marker: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("existing marker = %q, want %q", got, want)
	}
}

func TestLabConsumerPostCommitHookPublishesPrivateMarkerAndWaits(t *testing.T) {
	markerPath := filepath.Join(t.TempDir(), "marker.json")
	hook, err := newLabConsumerPostCommitHook(markerPath, time.Minute)
	if err != nil {
		t.Fatalf("create lab consumer post-commit hook: %v", err)
	}
	eventID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	message := &enabledTestJetStreamMessage{
		subject: "vela.events.job.ready",
		headers: nats.Header{
			jetstream.MsgIDHeader: []string{eventID.String()},
		},
		metadata: &jetstream.MsgMetadata{
			Stream:       eventstream.StreamName,
			Consumer:     eventstream.SchedulerConsumerName,
			NumDelivered: 1,
			Sequence:     jetstream.SequencePair{Stream: 42, Consumer: 17},
		},
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- hook(ctx, inbox.Event{ID: eventID, Type: "job.ready"}, message)
	}()

	var markerBytes []byte
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		markerBytes, err = os.ReadFile(markerPath)
		if err == nil {
			break
		}
		if !errors.Is(err, os.ErrNotExist) {
			t.Fatalf("read lab consumer fault marker: %v", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(markerBytes) == 0 {
		t.Fatal("lab consumer fault marker was not written")
	}
	information, err := os.Stat(markerPath)
	if err != nil || information.Mode().Perm() != 0o600 {
		t.Fatalf("lab consumer fault marker mode = %v error %v", information.Mode(), err)
	}
	want := []byte("{\"schema\":\"vela-lab-consumer-fault-marker-v1\",\"phase\":\"consumer-post-db-pre-ack-crash\",\"subject\":\"vela.events.job.ready\",\"event_id\":\"00000000-0000-0000-0000-000000000001\",\"stream\":\"VELA_EVENTS\",\"consumer\":\"VELA_SCHEDULER\",\"stream_sequence\":42,\"consumer_sequence\":17,\"num_delivered\":1,\"inbox_committed\":true}\n")
	if !bytes.Equal(markerBytes, want) {
		t.Fatalf("lab consumer fault marker = %q, want %q", markerBytes, want)
	}
	var output bytes.Buffer
	if err := readLabConsumerFaultMarker(markerPath, &output); err != nil {
		t.Fatalf("read lab consumer fault marker command: %v", err)
	}
	if !bytes.Equal(output.Bytes(), want) {
		t.Fatalf("lab consumer marker command output = %q, want %q", output.Bytes(), want)
	}

	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("paused consumer hook error = %v, want context cancellation", err)
	}
}

func TestLabConsumerPostCommitHookRejectsWrongDeliveryIdentity(t *testing.T) {
	hook, err := newLabConsumerPostCommitHook(filepath.Join(t.TempDir(), "marker.json"), time.Minute)
	if err != nil {
		t.Fatalf("create lab consumer post-commit hook: %v", err)
	}
	eventID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	message := &enabledTestJetStreamMessage{
		subject: "vela.events.job.ready",
		headers: nats.Header{
			jetstream.MsgIDHeader: []string{eventID.String()},
		},
		metadata: &jetstream.MsgMetadata{
			Stream:       eventstream.StreamName,
			Consumer:     eventstream.SchedulerConsumerName,
			NumDelivered: 2,
			Sequence:     jetstream.SequencePair{Stream: 42, Consumer: 18},
		},
	}
	if err := hook(context.Background(), inbox.Event{ID: eventID, Type: "job.ready"}, message); err == nil {
		t.Fatal("lab consumer hook accepted a redelivered event as the crash target")
	}
}

type enabledTestJetStreamMessage struct {
	data     []byte
	headers  nats.Header
	subject  string
	reply    string
	metadata *jetstream.MsgMetadata
}

func (m *enabledTestJetStreamMessage) Metadata() (*jetstream.MsgMetadata, error) {
	return m.metadata, nil
}

func (m *enabledTestJetStreamMessage) Data() []byte                     { return m.data }
func (m *enabledTestJetStreamMessage) Headers() nats.Header             { return m.headers }
func (m *enabledTestJetStreamMessage) Subject() string                  { return m.subject }
func (m *enabledTestJetStreamMessage) Reply() string                    { return m.reply }
func (m *enabledTestJetStreamMessage) Ack() error                       { return nil }
func (m *enabledTestJetStreamMessage) DoubleAck(context.Context) error  { return nil }
func (m *enabledTestJetStreamMessage) Nak() error                       { return nil }
func (m *enabledTestJetStreamMessage) NakWithDelay(time.Duration) error { return nil }
func (m *enabledTestJetStreamMessage) InProgress() error                { return nil }
func (m *enabledTestJetStreamMessage) Term() error                      { return nil }
func (m *enabledTestJetStreamMessage) TermWithReason(string) error      { return nil }
