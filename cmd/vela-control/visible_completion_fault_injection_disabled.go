//go:build !vela_lab_fault_injection

package main

import (
	"errors"
	"io"
	"os"

	"github.com/vivym/vela/internal/workertransport"
)

func runLabVisibleCompletionFaultCommand(args []string, _ io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labVisibleCompletionFaultMarkerArg {
		return false, nil
	}
	return true, errors.New(
		"lab Visible Completion fault marker command requires the vela_lab_fault_injection build tag",
	)
}

func configureWorkerTransportCoordinator(
	delegate workertransport.WorkerCoordinator,
) (workertransport.WorkerCoordinator, error) {
	_, phaseConfigured := os.LookupEnv(labVisibleCompletionFaultPhaseEnv)
	_, workerConfigured := os.LookupEnv(labVisibleCompletionFaultWorkerEnv)
	if phaseConfigured || workerConfigured {
		return nil, errors.New(
			"lab Visible Completion fault injection requires the vela_lab_fault_injection build tag",
		)
	}
	return delegate, nil
}
