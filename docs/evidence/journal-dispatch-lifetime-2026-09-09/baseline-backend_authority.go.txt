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

func (service *Service) confirmBackendAuthority(ctx context.Context, verified stageauthority.Verified) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	admission := service.executionAdmission()
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := admission.checkStateLocked(ctx); err != nil {
		return err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != nil && service.active.verified.Digest == verified.Digest {
		if admission.store != nil {
			if err := admission.store.saveCandidatesContext(ctx, verified, &verified); err != nil {
				return admission.failStateLocked(err)
			}
		}
		confirmed := copyVerified(verified)
		service.active.backendAuthority = &confirmed
	}
	return ctx.Err()
}

// STOPPED proves only that execution stopped. Preserve explicit non-reusability
// until a validated status contains a new explicit health assertion.
func (service *Service) confirmBackendStatus(ctx context.Context, verified stageauthority.Verified, status BackendStatus) error {
	if err := service.confirmBackendAuthority(ctx, verified); err != nil {
		return err
	}
	admission := service.executionAdmission()
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := admission.checkStateLocked(ctx); err != nil {
		return err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil || service.active.verified.Digest != verified.Digest {
		return nil
	}
	switch status.State {
	case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED:
		if admission.store != nil {
			if err := admission.store.saveHealthContext(ctx, verified, status.FailureEvidence); err != nil {
				return admission.failStateLocked(err)
			}
		}
		service.active.workerReuseDenied = !status.FailureEvidence.WorkerReusable
	}
	return nil
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
	if err := service.confirmBackendStatus(ctx, verified, status); err != nil {
		return err
	}
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
