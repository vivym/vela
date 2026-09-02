package h3launchevidence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"github.com/vivym/vela/internal/fleetcontroller"
)

var ErrUnstableLaunchAuthority = errors.New("H3 launch authority changed during capture")

type CaptureRequest struct {
	ReleaseDigest         string
	ConfigurationRevision string
	ValidationEnvironment string
	CollectorIdentity     string
	Rollout               fleetcontroller.ResidencyPlanRollout
	ExternalResources     []ExternalResourceExpectation
}

// Capture double-reads both authorities. The output is emitted only when the
// Fleet state and every Kubernetes object identity remain unchanged across the
// collection window.
func Capture(
	ctx context.Context,
	kubernetes KubernetesReader,
	registry RegistryReader,
	request CaptureRequest,
) (Evidence, error) {
	if ctx == nil || kubernetes == nil || registry == nil {
		return Evidence{}, errors.New("launch evidence collectors and context are required")
	}
	planID := request.Rollout.ApprovedPlan.ID
	firstRegistry, err := registry.Capture(ctx, planID)
	if err != nil {
		return Evidence{}, fmt.Errorf("capture initial Fleet Registry authority: %w", err)
	}
	firstKubernetes, err := CollectKubernetes(
		ctx, kubernetes, request.Rollout, request.ExternalResources,
	)
	if err != nil {
		return Evidence{}, fmt.Errorf("capture initial Kubernetes authority: %w", err)
	}
	secondRegistry, err := registry.Capture(ctx, planID)
	if err != nil {
		return Evidence{}, fmt.Errorf("capture final Fleet Registry authority: %w", err)
	}
	secondKubernetes, err := CollectKubernetes(
		ctx, kubernetes, request.Rollout, request.ExternalResources,
	)
	if err != nil {
		return Evidence{}, fmt.Errorf("capture final Kubernetes authority: %w", err)
	}
	if !reflect.DeepEqual(firstRegistry.Workers, secondRegistry.Workers) ||
		!reflect.DeepEqual(firstKubernetes, secondKubernetes) {
		return Evidence{}, ErrUnstableLaunchAuthority
	}
	return Verify(Input{
		ReleaseDigest: request.ReleaseDigest, ConfigurationRevision: request.ConfigurationRevision,
		ValidationEnvironment: request.ValidationEnvironment,
		CollectorIdentity:     request.CollectorIdentity, CapturedAt: time.Now().UTC(),
		Rollout: request.Rollout, ExternalResources: request.ExternalResources,
		Kubernetes: secondKubernetes, Registry: secondRegistry,
	})
}
