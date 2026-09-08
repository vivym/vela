//go:build !linux

package main

import (
	"context"
	"io"

	"github.com/vivym/vela/internal/workerbootstrap"
)

func provisionBootstrap(ctx context.Context, config workerbootstrap.Config, directory string, authority workerbootstrap.Authority, _ io.Writer) error {
	_, err := workerbootstrap.Provision(ctx, config, directory, authority)
	return err
}

func inspectProvisionBootstrap(ctx context.Context, config workerbootstrap.Config, directory string, reader workerbootstrap.HistoryReader, _ io.Writer) error {
	_, err := workerbootstrap.InspectProvisionedJournals(ctx, config, directory, reader)
	return err
}
