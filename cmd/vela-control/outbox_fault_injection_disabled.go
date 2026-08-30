//go:build !vela_lab_fault_injection

package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/vivym/vela/internal/outbox"
)

func runLabOutboxFaultCommand(args []string, _ io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labOutboxFaultReadMarkerArg {
		return false, nil
	}
	return true, errors.New(
		"lab Outbox fault marker command requires the vela_lab_fault_injection build tag",
	)
}

func configureOutboxPublisherBroker(delegate outbox.Broker) (outbox.Broker, error) {
	if _, configured := os.LookupEnv(labOutboxFaultPhaseEnv); configured {
		return nil, fmt.Errorf(
			"%s requires a vela-control binary built with the vela_lab_fault_injection build tag",
			labOutboxFaultPhaseEnv,
		)
	}
	return delegate, nil
}
