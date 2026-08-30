//go:build !vela_lab_fault_injection

package main

import (
	"errors"
	"io"
)

func runLabStaleCompletionProbeCommand(args []string, _ io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labStaleCompletionProbeArg {
		return false, nil
	}
	return true, errors.New(
		"lab stale Completion probe requires the vela_lab_fault_injection build tag",
	)
}
