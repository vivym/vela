package main

import (
	"context"
	"encoding/json"
	"io"

	"github.com/vivym/vela/internal/workerbootstrap"
)

func provisionBootstrap(ctx context.Context, config workerbootstrap.Config, directory string, authority workerbootstrap.Authority, stdout io.Writer) error {
	provisioned, err := workerbootstrap.Provision(ctx, config, directory, authority)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(provisioned)
}

func inspectProvisionBootstrap(ctx context.Context, config workerbootstrap.Config, directory string, reader workerbootstrap.HistoryReader, stdout io.Writer) error {
	result, err := workerbootstrap.InspectProvisionedJournals(ctx, config, directory, reader)
	if err != nil {
		return err
	}
	return json.NewEncoder(stdout).Encode(result)
}
