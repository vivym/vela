package modelruntime

import (
	"context"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

// The caller owns operationMu and has persisted the exact drain. Inspection
// alone must never release a slot or clear a backend's explicit health denial.
func (service *Service) restoreDrainedExecution(ctx context.Context, verified stageauthority.Verified) error {
	service.mu.Lock()
	active, generation := service.active, service.generation
	if !active.knowsAuthority(verified.Digest) || active.workerReuseDenied || reusableExecution(active) {
		service.mu.Unlock()
		return nil
	}
	state, reusable := active.state, active.reuseAfterDrain
	service.mu.Unlock()
	if state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED &&
		state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED &&
		(state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED || !reusable) {
		inspector, ok := service.backend.(BackendExecutionInspector)
		if !ok {
			return ErrExecutionDrainUnproven
		}
		readCtx, cancel := context.WithTimeout(ctx, service.cancelTimeout)
		defer cancel()
		read, err := inspector.InspectExecution(readCtx, copyVerified(verified))
		if err != nil {
			return err
		}
		if err := readCtx.Err(); err != nil {
			return err
		}
		if validateExecutionInspection(read) != nil || !read.Known || read.State != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
			return ErrExecutionDrainUnproven
		}
		state = read.State
	}
	admission := service.executionAdmission()
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := admission.checkStateLocked(ctx); err != nil {
		return err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	if service.active != active || service.generation != generation || !active.knowsAuthority(verified.Digest) {
		return ErrExecutionDrainUnproven
	}
	if active.workerReuseDenied {
		return nil
	}
	if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED {
		// Preserve the validated receipt exactly as ordinary Seal completion does.
		if active.receipt == nil || active.verified.Digest != verified.Digest {
			return ErrExecutionDrainUnproven
		}
		service.rememberSealedReceiptLocked(verified.Digest, active.receipt)
		service.cancelWatchdogLocked()
		service.active = nil
	} else {
		active.state, active.workerReusable, active.reuseAfterDrain = state, true, true
		service.cancelWatchdogLocked()
	}
	service.generation++
	return nil
}
