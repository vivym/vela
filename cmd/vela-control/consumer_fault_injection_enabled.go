//go:build vela_lab_fault_injection

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/vivym/vela/internal/eventstream"
	"github.com/vivym/vela/internal/inbox"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	labConsumerFaultWait           = 2 * time.Minute
	labConsumerFaultMarkerMaxBytes = 4 * 1024
)

type labConsumerFaultMarker struct {
	Schema           string `json:"schema"`
	Phase            string `json:"phase"`
	Subject          string `json:"subject"`
	EventID          string `json:"event_id"`
	Stream           string `json:"stream"`
	Consumer         string `json:"consumer"`
	StreamSequence   uint64 `json:"stream_sequence"`
	ConsumerSequence uint64 `json:"consumer_sequence"`
	NumDelivered     uint64 `json:"num_delivered"`
	InboxCommitted   bool   `json:"inbox_committed"`
}

func runLabConsumerFaultCommand(args []string, output io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labConsumerFaultReadMarkerArg {
		return false, nil
	}
	if len(args) != 1 {
		return true, errors.New("lab consumer fault marker command accepts no additional arguments")
	}
	return true, readLabConsumerFaultMarker(labConsumerFaultMarkerPath, output)
}

func configureSchedulerMessageConsumer(
	processor *inbox.Processor,
) (*inbox.JetStreamConsumer, error) {
	phase, configured := os.LookupEnv(labConsumerFaultPhaseEnv)
	if !configured {
		return inbox.NewJetStreamConsumer(processor)
	}
	if phase != consumerPostDBPreAckCrash {
		return nil, fmt.Errorf("unsupported lab consumer fault phase %q", phase)
	}
	hook, err := newLabConsumerPostCommitHook(
		labConsumerFaultMarkerPath,
		labConsumerFaultWait,
	)
	if err != nil {
		return nil, err
	}
	return inbox.NewJetStreamConsumerWithPostCommitHook(processor, hook)
}

func newLabConsumerPostCommitHook(
	markerPath string,
	wait time.Duration,
) (inbox.PostCommitHook, error) {
	if markerPath == "" || filepath.Base(markerPath) == "." {
		return nil, errors.New("lab consumer fault marker path is required")
	}
	if wait <= 0 {
		return nil, errors.New("lab consumer fault wait must be positive")
	}
	return func(ctx context.Context, event inbox.Event, message jetstream.Msg) error {
		if message == nil {
			return errors.New("lab consumer fault message is required")
		}
		if event.ID == uuid.Nil || event.Type != "job.ready" ||
			message.Subject() != eventstream.SchedulerFilterSubject ||
			message.Headers().Get(jetstream.MsgIDHeader) != event.ID.String() {
			return errors.New("lab consumer fault event identity is invalid")
		}
		metadata, err := message.Metadata()
		if err != nil {
			return fmt.Errorf("read lab consumer fault message metadata: %w", err)
		}
		if metadata.Stream != eventstream.StreamName ||
			metadata.Consumer != eventstream.SchedulerConsumerName ||
			metadata.Sequence.Stream < 1 || metadata.Sequence.Consumer < 1 ||
			metadata.NumDelivered != 1 {
			return errors.New("lab consumer fault delivery identity is invalid")
		}
		marker := labConsumerFaultMarker{
			Schema:           labConsumerFaultMarkerSchema,
			Phase:            consumerPostDBPreAckCrash,
			Subject:          message.Subject(),
			EventID:          event.ID.String(),
			Stream:           metadata.Stream,
			Consumer:         metadata.Consumer,
			StreamSequence:   metadata.Sequence.Stream,
			ConsumerSequence: metadata.Sequence.Consumer,
			NumDelivered:     metadata.NumDelivered,
			InboxCommitted:   true,
		}
		if err := writeLabConsumerFaultMarker(markerPath, marker); err != nil {
			return err
		}
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			return errors.New("timed out waiting for lab consumer fault process termination")
		}
	}, nil
}

func readLabConsumerFaultMarker(path string, output io.Writer) error {
	information, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect lab consumer fault marker: %w", err)
	}
	if !information.Mode().IsRegular() || information.Mode().Perm() != 0o600 {
		return errors.New("lab consumer fault marker must be a private regular file")
	}
	if information.Size() < 1 || information.Size() > labConsumerFaultMarkerMaxBytes {
		return errors.New("lab consumer fault marker size is outside the supported range")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read lab consumer fault marker: %w", err)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return errors.New("lab consumer fault marker does not have the exact schema")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || len(fields) != 10 {
		return errors.New("lab consumer fault marker does not have the exact schema")
	}
	for _, field := range []string{
		"schema", "phase", "subject", "event_id", "stream", "consumer",
		"stream_sequence", "consumer_sequence", "num_delivered", "inbox_committed",
	} {
		if _, present := fields[field]; !present {
			return errors.New("lab consumer fault marker does not have the exact schema")
		}
	}
	var marker labConsumerFaultMarker
	if err := json.Unmarshal(encoded, &marker); err != nil {
		return fmt.Errorf("decode lab consumer fault marker: %w", err)
	}
	eventID, eventErr := uuid.Parse(marker.EventID)
	if marker.Schema != labConsumerFaultMarkerSchema ||
		marker.Phase != consumerPostDBPreAckCrash ||
		marker.Subject != eventstream.SchedulerFilterSubject ||
		eventErr != nil || eventID == uuid.Nil ||
		marker.Stream != eventstream.StreamName ||
		marker.Consumer != eventstream.SchedulerConsumerName ||
		marker.StreamSequence < 1 || marker.ConsumerSequence < 1 ||
		marker.NumDelivered != 1 || !marker.InboxCommitted {
		return errors.New("lab consumer fault marker identity is invalid")
	}
	if _, err := output.Write(encoded); err != nil {
		return fmt.Errorf("write lab consumer fault marker: %w", err)
	}
	return nil
}

func writeLabConsumerFaultMarker(path string, marker labConsumerFaultMarker) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create lab consumer fault marker directory: %w", err)
	}
	if information, err := os.Lstat(path); err == nil {
		if !information.Mode().IsRegular() {
			return errors.New("lab consumer fault marker is not a regular file")
		}
		return errors.New("lab consumer fault marker already exists")
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("inspect lab consumer fault marker: %w", err)
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode lab consumer fault marker: %w", err)
	}
	encoded = append(encoded, '\n')
	temporary, err := os.CreateTemp(directory, ".marker-*")
	if err != nil {
		return fmt.Errorf("create temporary lab consumer fault marker: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect temporary lab consumer fault marker: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary lab consumer fault marker: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary lab consumer fault marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary lab consumer fault marker: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish lab consumer fault marker: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open lab consumer fault marker directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync lab consumer fault marker directory: %w", err)
	}
	return nil
}
