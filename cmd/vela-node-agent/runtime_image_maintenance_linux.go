package main

import (
	"context"

	"github.com/vivym/vela/internal/nodeagent"
)

func dialRuntimeImageMaintenance(ctx context.Context, config runtimeImageMaintenanceConfig) (runtimeImageRecovery, error) {
	return nodeagent.DialRuntimeImageObserver(ctx, nodeagent.RuntimeImageObserverConfig{
		RuntimeContainerObserverConfig: nodeagent.RuntimeContainerObserverConfig{SocketPath: config.SocketPath, NodeIdentity: config.NodeIdentity},
		Namespace:                      config.Namespace, Snapshotter: config.Snapshotter,
	})
}
