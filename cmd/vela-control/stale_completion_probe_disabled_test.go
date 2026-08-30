//go:build !vela_lab_fault_injection

package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestLabStaleCompletionProbeRejectsBinaryWithoutBuildTag(t *testing.T) {
	handled, err := runLabStaleCompletionProbeCommand(
		[]string{labStaleCompletionProbeArg, "--validate-only"},
		&bytes.Buffer{},
	)
	if !handled || err == nil || !strings.Contains(err.Error(), "vela_lab_fault_injection") {
		t.Fatalf("stale Completion probe handled = %t, error = %v", handled, err)
	}
}
