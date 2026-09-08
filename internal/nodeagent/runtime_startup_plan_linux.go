package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"

	"github.com/vivym/vela/internal/modelruntime"
	"google.golang.org/protobuf/proto"
)

// parseRuntimeStartupPlan validates the complete canonical envelope before its
// bytes may replace the manifest declaration in an original-caller observation.
// Incarnation first-use is checked separately against the held Node journal.
func parseRuntimeStartupPlan(plan *RuntimeLaunchPlan, payload []byte) (modelruntime.BackendStartupRequest, []byte, error) {
	if plan == nil || plan.binding == nil || plan.binding.Claim == nil || plan.binding.Pair == nil {
		return modelruntime.BackendStartupRequest{}, nil, ErrRuntimeLaunchPlan
	}
	request, err := modelruntime.ParseBackendStartupRequest(payload)
	if err != nil {
		return modelruntime.BackendStartupRequest{}, nil, err
	}
	binding, err := proto.MarshalOptions{Deterministic: true}.Marshal(plan.binding)
	if err != nil || request.NodeIdentity != plan.binding.Claim.NodeIdentity || request.JournalID.String() != plan.binding.Pair.RuntimeJournalId ||
		!bytes.Equal(request.JournalScope[:], plan.binding.Pair.RuntimeScope) || request.RegistryBindingDigest != sha256.Sum256(binding) || request.LaunchDigest != sha256.Sum256(plan.manifest) {
		return modelruntime.BackendStartupRequest{}, nil, errors.Join(ErrRuntimeLaunchPlan, err)
	}
	return request, binding, nil
}

// ObserveStartupImageCaller accepts only the original caller's canonical startup
// envelope, fully bound to the verified plan. The returned observation cannot
// prove journal first-use or authorize a backend. ReserveImageRemote additionally
// checks the Node-held journal and brackets its single Fleet call with sampling.
func (observer *RuntimeContainerObserver) ObserveStartupImageCaller(ctx context.Context, config RuntimePlannedImageCallerConfig) (*RuntimePlannedImageCallerObservation, error) {
	if err := contextError(ctx); err != nil {
		return nil, err
	}
	if config.Caller == nil {
		return nil, ErrRuntimeLaunchPlan
	}
	payload := config.Caller.Payload()
	if _, _, err := parseRuntimeStartupPlan(config.Plan, payload); err != nil {
		return nil, err
	}
	return observer.observePlannedImageCaller(ctx, config, payload)
}
