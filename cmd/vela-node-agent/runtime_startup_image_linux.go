package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"

	"github.com/vivym/vela/internal/nodeagent"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/strictjson"
)

// This private host policy is provisioned independently of the Worker and
// its observed task. No live OCI document is accepted as its own approval.
type runtimeStartupImagePolicy struct {
	SchemaVersion  int                                `json:"schema_version"`
	StateDirectory string                             `json:"state_directory"`
	RuntimePolicy  nodeagent.RuntimeTaskRuntimePolicy `json:"runtime_policy"`
}

func loadRuntimeStartupImage(ctx context.Context, configuration config) (*nodeagent.RuntimeStartupImageConfig, error) {
	wire, err := securefile.Read(configuration.runtimeImagePolicyFile, 64<<10, true)
	if err != nil {
		return nil, err
	}
	var policy runtimeStartupImagePolicy
	decoder := json.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields()
	if strictjson.RejectDuplicateKeys(wire) != nil || decoder.Decode(&policy) != nil || !errors.Is(decoder.Decode(&struct{}{}), io.EOF) ||
		policy.SchemaVersion != 1 || !filepath.IsAbs(policy.StateDirectory) || filepath.Clean(policy.StateDirectory) != policy.StateDirectory {
		return nil, errors.New("invalid independently provisioned runtime image policy")
	}
	images, err := nodeagent.DialRuntimeImageObserver(ctx, nodeagent.RuntimeImageObserverConfig{
		RuntimeContainerObserverConfig: nodeagent.RuntimeContainerObserverConfig{SocketPath: configuration.runtimeCRISocket, NodeIdentity: configuration.nodeIdentity},
		Namespace:                      "k8s.io", Snapshotter: "native",
	})
	if err != nil {
		return nil, err
	}
	return &nodeagent.RuntimeStartupImageConfig{Images: images, StateDirectory: policy.StateDirectory, RuntimePolicy: policy.RuntimePolicy}, nil
}
