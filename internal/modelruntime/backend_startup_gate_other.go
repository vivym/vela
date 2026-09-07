//go:build !linux

package modelruntime

import (
	"context"
	"errors"
)

func nodeBackendStartupGate(string) RuntimeBackendStartupGate {
	return func(context.Context, BackendStartupRequest) error {
		return errors.New("backend startup Node authentication requires Linux")
	}
}
