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

	"github.com/vivym/vela/internal/outbox"
	"github.com/vivym/vela/internal/strictjson"
)

const (
	labOutboxFaultWait           = 2 * time.Minute
	labOutboxFaultMarkerMaxBytes = 4 * 1024
)

type labOutboxFaultBroker struct {
	delegate   outbox.Broker
	phase      string
	markerPath string
	wait       time.Duration
}

type labOutboxFaultMarker struct {
	Schema         string `json:"schema"`
	Phase          string `json:"phase"`
	Subject        string `json:"subject"`
	EventID        string `json:"event_id"`
	BrokerStream   string `json:"broker_stream"`
	BrokerSequence int64  `json:"broker_sequence"`
}

func runLabOutboxFaultCommand(args []string, output io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labOutboxFaultReadMarkerArg {
		return false, nil
	}
	if len(args) != 1 {
		return true, errors.New("lab Outbox fault marker command accepts no additional arguments")
	}
	return true, readLabOutboxFaultMarker(labOutboxFaultMarkerPath, output)
}

func readLabOutboxFaultMarker(path string, output io.Writer) error {
	information, err := os.Lstat(path)
	if err != nil {
		return fmt.Errorf("inspect lab Outbox fault marker: %w", err)
	}
	if !information.Mode().IsRegular() || information.Mode().Perm() != 0o600 {
		return errors.New("lab Outbox fault marker must be a private regular file")
	}
	if information.Size() < 1 || information.Size() > labOutboxFaultMarkerMaxBytes {
		return errors.New("lab Outbox fault marker size is outside the supported range")
	}
	encoded, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("read lab Outbox fault marker: %w", err)
	}
	if err := strictjson.RejectDuplicateKeys(encoded); err != nil {
		return errors.New("lab Outbox fault marker does not have the exact schema")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &fields); err != nil || len(fields) != 6 {
		return errors.New("lab Outbox fault marker does not have the exact schema")
	}
	for _, field := range []string{
		"schema", "phase", "subject", "event_id", "broker_stream", "broker_sequence",
	} {
		if _, present := fields[field]; !present {
			return errors.New("lab Outbox fault marker does not have the exact schema")
		}
	}
	var marker labOutboxFaultMarker
	if err := json.Unmarshal(encoded, &marker); err != nil {
		return fmt.Errorf("decode lab Outbox fault marker: %w", err)
	}
	if marker.Schema != labOutboxFaultMarkerSchema || marker.Subject == "" || marker.EventID == "" {
		return errors.New("lab Outbox fault marker identity is invalid")
	}
	switch marker.Phase {
	case publisherPrePubAckCrash:
		if marker.BrokerStream != "" || marker.BrokerSequence != 0 {
			return errors.New("pre-PubAck lab Outbox fault marker contains a Broker receipt")
		}
	case publisherPostPubAckPreMarkCrash:
		if marker.BrokerStream == "" || marker.BrokerSequence < 1 {
			return errors.New("post-PubAck lab Outbox fault marker lacks a Broker receipt")
		}
	default:
		return errors.New("lab Outbox fault marker phase is invalid")
	}
	if _, err := output.Write(encoded); err != nil {
		return fmt.Errorf("write lab Outbox fault marker: %w", err)
	}
	return nil
}

func configureOutboxPublisherBroker(delegate outbox.Broker) (outbox.Broker, error) {
	phase, configured := os.LookupEnv(labOutboxFaultPhaseEnv)
	if !configured {
		return delegate, nil
	}
	return newLabOutboxFaultBroker(delegate, phase, labOutboxFaultMarkerPath, labOutboxFaultWait)
}

func newLabOutboxFaultBroker(
	delegate outbox.Broker,
	phase string,
	markerPath string,
	wait time.Duration,
) (outbox.Broker, error) {
	if delegate == nil {
		return nil, errors.New("lab Outbox fault Broker delegate is required")
	}
	switch phase {
	case publisherPrePubAckCrash, publisherPostPubAckPreMarkCrash:
	default:
		return nil, fmt.Errorf("unsupported lab Outbox fault phase %q", phase)
	}
	if markerPath == "" || filepath.Base(markerPath) == "." {
		return nil, errors.New("lab Outbox fault marker path is required")
	}
	if wait <= 0 {
		return nil, errors.New("lab Outbox fault wait must be positive")
	}
	return &labOutboxFaultBroker{
		delegate:   delegate,
		phase:      phase,
		markerPath: markerPath,
		wait:       wait,
	}, nil
}

func (b *labOutboxFaultBroker) Publish(
	ctx context.Context,
	subject string,
	messageID string,
	payload []byte,
) (outbox.Receipt, error) {
	markerInformation, err := os.Lstat(b.markerPath)
	if err == nil {
		if !markerInformation.Mode().IsRegular() {
			return outbox.Receipt{}, errors.New("lab Outbox fault marker is not a regular file")
		}
		return b.delegate.Publish(ctx, subject, messageID, payload)
	}
	if !errors.Is(err, os.ErrNotExist) {
		return outbox.Receipt{}, fmt.Errorf("inspect lab Outbox fault marker: %w", err)
	}

	marker := labOutboxFaultMarker{
		Schema:  labOutboxFaultMarkerSchema,
		Phase:   b.phase,
		Subject: subject,
		EventID: messageID,
	}
	if b.phase == publisherPrePubAckCrash {
		if err := writeLabOutboxFaultMarker(b.markerPath, marker); err != nil {
			return outbox.Receipt{}, err
		}
		return outbox.Receipt{}, b.waitForCrash(ctx)
	}

	receipt, err := b.delegate.Publish(ctx, subject, messageID, payload)
	if err != nil {
		return outbox.Receipt{}, err
	}
	marker.BrokerStream = receipt.Stream
	marker.BrokerSequence = receipt.Sequence
	if err := writeLabOutboxFaultMarker(b.markerPath, marker); err != nil {
		return outbox.Receipt{}, err
	}
	return outbox.Receipt{}, b.waitForCrash(ctx)
}

func (b *labOutboxFaultBroker) waitForCrash(ctx context.Context) error {
	timer := time.NewTimer(b.wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return errors.New("timed out waiting for lab Outbox fault process termination")
	}
}

func writeLabOutboxFaultMarker(path string, marker labOutboxFaultMarker) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return fmt.Errorf("create lab Outbox fault marker directory: %w", err)
	}
	encoded, err := json.Marshal(marker)
	if err != nil {
		return fmt.Errorf("encode lab Outbox fault marker: %w", err)
	}
	encoded = append(encoded, '\n')

	temporary, err := os.CreateTemp(directory, ".marker-*")
	if err != nil {
		return fmt.Errorf("create temporary lab Outbox fault marker: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("protect temporary lab Outbox fault marker: %w", err)
	}
	if _, err := temporary.Write(encoded); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write temporary lab Outbox fault marker: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync temporary lab Outbox fault marker: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close temporary lab Outbox fault marker: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("publish lab Outbox fault marker: %w", err)
	}
	directoryHandle, err := os.Open(directory)
	if err != nil {
		return fmt.Errorf("open lab Outbox fault marker directory: %w", err)
	}
	defer directoryHandle.Close()
	if err := directoryHandle.Sync(); err != nil {
		return fmt.Errorf("sync lab Outbox fault marker directory: %w", err)
	}
	return nil
}
