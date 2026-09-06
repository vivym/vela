package modelruntime

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

// The caller holds operationMu. A fresh compatible successor can authorize a
// stop, but cannot replace the installed envelope or extend its watchdog.
func (service *Service) cancellationTarget(request stageauthority.Verified, allowSuccessor bool) (stageauthority.Verified, velav1.ModelRuntimeExecutionState, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.cancellationTargetLocked(request, allowSuccessor)
}

func (service *Service) cancellationTargetLocked(request stageauthority.Verified, allowSuccessor bool) (stageauthority.Verified, velav1.ModelRuntimeExecutionState, error) {
	if service.active == nil {
		return stageauthority.Verified{}, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED, errActiveAuthorityMismatch
	}
	active := service.active
	if reusableExecution(active) {
		return stageauthority.Verified{}, active.state, errActiveAuthorityMismatch
	}
	if !active.knowsAuthority(request.Digest) && (!allowSuccessor || active.deadlineExpired || terminalState(active.state) ||
		stageauthority.ValidateRenewal(active.verified.Authority, request.Authority) != nil) {
		return stageauthority.Verified{}, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED, errActiveAuthorityMismatch
	}
	target := active.verified
	target.Authority = proto.Clone(target.Authority).(*velav1.StageAuthority)
	return target, active.state, nil
}

var errExecutionCancellation = errors.New("ModelRuntime execution cancellation is pending")

// This signal shares admission validation with the serialized cancellation,
// but must not replace the admitted operation that still owns operationMu.
func (admission *executionAdmission) interruptCancellation(ctx context.Context, service *Service, verified *stageauthority.Verified) (func(), error) {
	if ctx == nil {
		return nil, errors.New("ModelRuntime cancellation context is required")
	}
	admission.mu.Lock()
	defer admission.mu.Unlock()
	allowSuccessor, err := admission.validateOperationLocked(service, verified, true)
	if err != nil {
		return nil, err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if service.sealed[verified.Digest] != nil {
		return func() {}, nil
	}
	_, _, err = service.cancellationTargetLocked(*verified, allowSuccessor)
	if err != nil {
		return nil, err
	}
	if !needsExecutionCancellation(service.active) {
		return func() {}, nil
	}
	active := service.active
	active.pendingCancellations++
	if call := service.backendCall; call != nil && call.generation == service.generation {
		call.cancel(errExecutionCancellation)
	}
	return func() {
		service.mu.Lock()
		defer service.mu.Unlock()
		active.pendingCancellations--
	}, nil
}

func needsExecutionCancellation(active *activeExecution) bool {
	return active != nil && !reusableExecution(active) &&
		active.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED &&
		active.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED &&
		active.state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING
}
