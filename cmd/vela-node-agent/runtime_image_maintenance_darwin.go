package main

import (
	"context"
	"errors"
)

func dialRuntimeImageMaintenance(context.Context, runtimeImageMaintenanceConfig) (runtimeImageRecovery, error) {
	return nil, errors.New("runtime image maintenance requires Linux")
}
