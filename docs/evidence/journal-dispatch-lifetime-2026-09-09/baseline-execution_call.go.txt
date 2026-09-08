package modelruntime

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
)

var errExecutionDeadline = errors.New("ModelRuntime monotonic execution deadline elapsed")

type backendExecutionCall struct {
	generation uint64
	cancel     context.CancelCauseFunc
}

// The caller holds operationMu until finish returns. The watchdog cancels this
// specific call before waiting for that mutex; it never releases its admission.
func (service *Service) executionCallContext(ctx context.Context, verified stageauthority.Verified) (context.Context, func(), error) {
	if ctx == nil {
		return nil, nil, errors.New("ModelRuntime execution context is required")
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	admission := service.executionAdmission()
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := admission.checkStateLocked(ctx); err != nil {
		return nil, nil, err
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil || service.active.verified.Digest != verified.Digest {
		return nil, nil, errActiveAuthorityMismatch
	}
	if service.active.deadlineExpired {
		return nil, nil, errExecutionDeadline
	}
	if service.active.pendingCancellations != 0 {
		return nil, nil, errExecutionCancellation
	}
	select {
	case <-service.closed:
		return nil, nil, errors.New("ModelRuntime service is closed")
	default:
	}
	if admission.store != nil {
		if err := admission.store.saveCandidatesContext(ctx, verified, service.active.backendAuthority); err != nil {
			return nil, nil, admission.failStateLocked(err)
		}
		// Persistence must not permit dispatch after the signed grant expires.
		if _, err := service.verify(verified.Authority); err != nil {
			return nil, nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
	}
	callCtx, cancel := context.WithCancelCause(ctx)
	call := &backendExecutionCall{generation: service.generation, cancel: cancel}
	service.backendCall = call
	return callCtx, func() {
		service.mu.Lock()
		if service.backendCall == call {
			service.backendCall = nil
		}
		service.mu.Unlock()
		cancel(nil)
	}, nil
}

func executionCallError(ctx context.Context, err error) error {
	if cause := context.Cause(ctx); cause != nil {
		return errors.Join(err, cause)
	}
	return err
}
