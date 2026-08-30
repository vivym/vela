//go:build !vela_lab_fault_injection

package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/vivym/vela/internal/scheduler"
)

func runLabAssignmentFaultCommand(args []string, _ io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labAssignmentFaultReadMarkerArg {
		return false, nil
	}
	return true, errors.New(
		"lab Assignment fault marker command requires the vela_lab_fault_injection build tag",
	)
}

func configureSchedulerAssignmentCoordinator(
	delegate scheduler.AssignmentCoordinator,
) (scheduler.AssignmentCoordinator, error) {
	if _, configured := os.LookupEnv(labAssignmentFaultPhaseEnv); configured {
		return nil, fmt.Errorf(
			"%s requires a vela-control binary built with the vela_lab_fault_injection build tag",
			labAssignmentFaultPhaseEnv,
		)
	}
	return delegate, nil
}
