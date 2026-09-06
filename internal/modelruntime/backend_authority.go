package modelruntime

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

var errBackendAuthorityUncertain = errors.New("ModelRuntime backend authority requires reconciliation")

func copyVerified(verified stageauthority.Verified) stageauthority.Verified {
	verified.Authority = proto.Clone(verified.Authority).(*velav1.StageAuthority)
	return verified
}

func (active *activeExecution) knowsAuthority(digest [32]byte) bool {
	return active != nil && (active.verified.Digest == digest ||
		active.backendAuthority != nil && active.backendAuthority.Digest == digest)
}

func (service *Service) confirmBackendAuthority(verified stageauthority.Verified) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != nil && service.active.verified.Digest == verified.Digest {
		confirmed := copyVerified(verified)
		service.active.backendAuthority = &confirmed
	}
}

// Cleanup can observe STOPPED without a health assertion. Preserve explicit
// non-reusability across cancellation until a validated Status clears it.
func (service *Service) confirmBackendStatus(verified stageauthority.Verified, status BackendStatus) {
	service.confirmBackendAuthority(verified)
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil || service.active.verified.Digest != verified.Digest {
		return
	}
	switch status.State {
	case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED:
		service.active.workerReuseDenied = !status.FailureEvidence.WorkerReusable
	case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED:
		service.active.workerReuseDenied = false
	}
}

// A renewed Prepare/Start replay must reach the backend before it can claim
// the new envelope is prepared/running. Status is the existing renewal RPC.
func (service *Service) synchronizeBackendAuthority(ctx context.Context, verified stageauthority.Verified) error {
	service.mu.Lock()
	pending := service.active != nil && service.active.backendAuthority != nil &&
		service.active.backendAuthority.Digest != verified.Digest
	service.mu.Unlock()
	if !pending {
		return nil
	}
	ctx, finish, err := service.executionCallContext(ctx, verified)
	if err != nil {
		return err
	}
	defer finish()
	status, err := service.backend.Status(ctx, verified)
	if err = executionCallError(ctx, err); err != nil {
		return err
	}
	if err := validateBackendStatus(status); err != nil {
		return err
	}
	current := service.activeState()
	if terminalState(current) && (!terminalState(status.State) ||
		current == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED && status.State != current) {
		return errors.New("backend status contradicts terminal execution")
	}
	service.confirmBackendStatus(verified, status)
	service.setActiveState(verified.Digest, status.State)
	return nil
}

// operationMu excludes further backend commands throughout both exact reads.
// A missing or contradictory observation cannot select a cancellation target.
func (service *Service) resolveCancellationTarget(ctx context.Context, request stageauthority.Verified, allowSuccessor bool) (stageauthority.Verified, velav1.ModelRuntimeExecutionState, error) {
	target, state, err := service.cancellationTarget(request, allowSuccessor)
	if err != nil {
		return target, state, err
	}
	service.mu.Lock()
	active := service.active
	var previous *stageauthority.Verified
	if active.backendAuthority != nil && active.backendAuthority.Digest != target.Digest {
		copy := copyVerified(*active.backendAuthority)
		previous = &copy
	}
	service.mu.Unlock()
	if previous == nil {
		return target, state, nil
	}
	if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING ||
		state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
		return *previous, state, nil
	}
	inspector, ok := service.backend.(BackendExecutionInspector)
	if !ok {
		return target, state, errBackendAuthorityUncertain
	}
	ctx, cancel := context.WithTimeout(ctx, service.cancelTimeout)
	defer cancel()
	var observed *stageauthority.Verified
	for _, candidate := range []stageauthority.Verified{*previous, target} {
		inspection, err := inspector.InspectExecution(ctx, copyVerified(candidate))
		if err == nil {
			err = validateExecutionInspection(inspection)
		}
		if err != nil || ctx.Err() != nil {
			return target, state, errors.Join(errBackendAuthorityUncertain, err, ctx.Err())
		}
		if inspection.Known {
			if observed != nil {
				return target, state, errBackendAuthorityUncertain
			}
			observed = &candidate
		}
	}
	if observed == nil {
		return target, state, errBackendAuthorityUncertain
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != active {
		return target, state, errActiveAuthorityMismatch
	}
	confirmed := copyVerified(*observed)
	active.backendAuthority = &confirmed
	if _, _, err := service.cancellationTargetLocked(request, allowSuccessor); err != nil {
		return target, state, err
	}
	return copyVerified(confirmed), active.state, nil
}
