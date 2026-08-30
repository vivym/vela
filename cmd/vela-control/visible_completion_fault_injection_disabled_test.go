//go:build !vela_lab_fault_injection

package main

import (
	"io"
	"strings"
	"testing"
)

func TestConfigureVisibleCompletionFaultRequiresBuildTag(t *testing.T) {
	t.Setenv(labVisibleCompletionFaultPhaseEnv, labVisibleCompletionPreCoordinator)
	if _, err := configureWorkerTransportCoordinator(nil); err == nil ||
		!strings.Contains(err.Error(), "vela_lab_fault_injection") {
		t.Fatalf("configure lab Visible Completion fault without build tag error = %v", err)
	}
}

func TestVisibleCompletionFaultMarkerCommandRequiresBuildTag(t *testing.T) {
	handled, err := runLabVisibleCompletionFaultCommand(
		[]string{labVisibleCompletionFaultMarkerArg}, io.Discard,
	)
	if !handled || err == nil || !strings.Contains(err.Error(), "vela_lab_fault_injection") {
		t.Fatalf("lab Visible Completion marker command = handled %t error %v", handled, err)
	}
}
