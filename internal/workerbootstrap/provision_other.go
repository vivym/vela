//go:build !linux

package workerbootstrap

import (
	"context"
	"errors"
	"fmt"
)

func provision(context.Context, Config, string, Authority, func(string) error) (ProvisionedJournals, error) {
	return ProvisionedJournals{}, fmt.Errorf("protected Node provisioning requires Linux root: %w", errors.ErrUnsupported)
}

func inspectProvisionedJournals(context.Context, Config, string, HistoryReader) (ProvisionedJournals, error) {
	return ProvisionedJournals{}, fmt.Errorf("protected Node inspection requires Linux root: %w", errors.ErrUnsupported)
}
