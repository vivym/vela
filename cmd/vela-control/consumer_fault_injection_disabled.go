//go:build !vela_lab_fault_injection

package main

import (
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/vivym/vela/internal/inbox"
)

func runLabConsumerFaultCommand(args []string, _ io.Writer) (bool, error) {
	if len(args) == 0 || args[0] != labConsumerFaultReadMarkerArg {
		return false, nil
	}
	return true, errors.New(
		"lab consumer fault marker command requires the vela_lab_fault_injection build tag",
	)
}

func configureSchedulerMessageConsumer(
	processor *inbox.Processor,
) (*inbox.JetStreamConsumer, error) {
	if _, configured := os.LookupEnv(labConsumerFaultPhaseEnv); configured {
		return nil, fmt.Errorf(
			"%s requires a vela-control binary built with the vela_lab_fault_injection build tag",
			labConsumerFaultPhaseEnv,
		)
	}
	return inbox.NewJetStreamConsumer(processor)
}
