//go:build !vela_lab_fault_injection

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestConfigureSchedulerMessageConsumerRejectsLabFaultWithoutBuildTag(t *testing.T) {
	t.Setenv(labConsumerFaultPhaseEnv, consumerPostDBPreAckCrash)
	_, err := configureSchedulerMessageConsumer(nil)
	if err == nil || !strings.Contains(err.Error(), "vela_lab_fault_injection") {
		t.Fatalf("configure lab consumer fault without build tag error = %v", err)
	}
}

func TestLabConsumerFaultReadCommandRejectsBinaryWithoutBuildTag(t *testing.T) {
	handled, err := runLabConsumerFaultCommand(
		[]string{labConsumerFaultReadMarkerArg},
		&bytes.Buffer{},
	)
	if !handled || err == nil || !strings.Contains(err.Error(), "vela_lab_fault_injection") {
		t.Fatalf("read consumer marker command handled = %t, error = %v", handled, err)
	}
}
