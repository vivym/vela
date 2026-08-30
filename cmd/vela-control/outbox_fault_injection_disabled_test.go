//go:build !vela_lab_fault_injection

package main

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/vivym/vela/internal/outbox"
)

func TestConfigureOutboxPublisherBrokerRejectsLabFaultWithoutBuildTag(t *testing.T) {
	t.Setenv(labOutboxFaultPhaseEnv, publisherPostPubAckPreMarkCrash)
	_, err := configureOutboxPublisherBroker(disabledTestBroker{})
	if err == nil || !strings.Contains(err.Error(), "vela_lab_fault_injection") {
		t.Fatalf("configure lab Outbox fault without build tag error = %v", err)
	}
}

func TestLabOutboxFaultReadCommandRejectsBinaryWithoutBuildTag(t *testing.T) {
	handled, err := runLabOutboxFaultCommand([]string{labOutboxFaultReadMarkerArg}, &bytes.Buffer{})
	if !handled || err == nil || !strings.Contains(err.Error(), "vela_lab_fault_injection") {
		t.Fatalf("read marker command handled = %t, error = %v", handled, err)
	}
}

type disabledTestBroker struct{}

func (disabledTestBroker) Publish(
	context.Context,
	string,
	string,
	[]byte,
) (outbox.Receipt, error) {
	return outbox.Receipt{Stream: "VELA_EVENTS", Sequence: 1}, nil
}
