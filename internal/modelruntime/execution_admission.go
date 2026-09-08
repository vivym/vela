package modelruntime

import (
	"errors"
	"sync"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

var (
	errExecutionFloor         = errors.New("StageAllocation execution sequence is below the Runtime admission floor")
	errSharedSlotBusy         = errors.New("WorkerInstance member shared slot is held by another resident runtime")
	ErrExecutionStateRecovery = errors.New("ModelRuntime execution admission requires state recovery")
)

// One admission boundary covers every Service owned by a Supervisor, including
// calls made directly to a Service. Its lock never spans a backend call.
type executionAdmission struct {
	mu            sync.Mutex
	services      []*Service
	highest       int64
	floor         int64
	active        map[*Service]*admittedOperation
	store         *executionStateFile
	failed        error
	closing       bool
	shutdownReady bool
	closeErr      error
}

type admittedOperation struct {
	sequence int64
	done     chan struct{}
}

func newExecutionAdmission(services []*Service) *executionAdmission {
	return &executionAdmission{services: services, active: make(map[*Service]*admittedOperation)}
}

func (service *Service) executionAdmission() *executionAdmission {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.admissionUsed = true
	return service.admission
}

func (service *Service) checkReadinessAdmission() error {
	select {
	case <-service.closed:
		return errors.New("ModelRuntime service is closed")
	default:
	}
	service.mu.Lock()
	admission := service.admission
	service.mu.Unlock()
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := admission.checkStateLocked(); err != nil {
		return err
	}
	if admission.store != nil {
		if err := admission.store.recoveryError(); err != nil {
			return err
		}
	}
	if admission.store != nil && len(admission.store.state.Executions) >= maxRetainedExecutions {
		return ErrExecutionHistoryFull
	}
	for _, resident := range admission.services {
		resident.mu.Lock()
		active := resident.active
		unhealthy := active != nil && active.workerReuseDenied
		blocked := active != nil && active.verified.Authority.GetExecutionSequence() <= admission.floor && !reusableExecution(active)
		resident.mu.Unlock()
		if unhealthy {
			return errors.New("backend explicitly denied Worker reuse")
		}
		if blocked {
			return errors.New("terminal execution still holds the shared Runtime slot")
		}
	}
	return nil
}

// The caller owns service.operationMu throughout the returned operation.
func (admission *executionAdmission) prepare(service *Service, verified *stageauthority.Verified) (bool, func(), error) {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if admission.store != nil && proto.Size(verified.Authority) > maxExecutionWireBytes {
		return false, nil, errors.New("ModelRuntime execution authority exceeds its persistence bound")
	}
	if err := admission.checkStateLocked(); err != nil {
		return false, nil, err
	}
	if admission.store != nil {
		if _, err := admission.store.scope.floor.validator.ValidateEnvelopeSignature(verified.Authority); err != nil {
			return false, nil, err
		}
		if err := admission.store.scope.matchRetainedExecutionScope(verified.Authority); err != nil {
			return false, nil, err
		}
	}
	if _, err := service.refreshExecutionAuthority(verified, false); err != nil {
		return false, nil, err
	}
	sequence := verified.Authority.GetExecutionSequence()
	if sequence <= admission.floor {
		return false, nil, errExecutionFloor
	}
	for _, resident := range admission.services {
		if resident != service && resident.holdsExecutionSlot() {
			return false, nil, errSharedSlotBusy
		}
	}
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil || reusableExecution(service.active) {
		if sequence <= admission.highest {
			return false, nil, errors.New("StageAllocation execution sequence is retired")
		}
		if admission.store != nil {
			if err := admission.store.recoveryError(); err != nil {
				return false, nil, err
			}
		}
		// A backend failure cannot reopen an allocation, including on another profile.
		if admission.store != nil {
			if err := admission.store.saveHighest(verified.Authority); err != nil {
				if errors.Is(err, ErrExecutionHistoryFull) {
					return false, nil, err
				}
				return false, nil, admission.failStateLocked(err)
			}
		}
		admission.highest = sequence
		// fsync can consume authority lifetime. The persisted sequence remains
		// consumed even when it expires before backend entry.
		if admission.store != nil {
			if _, err := service.refreshExecutionAuthority(verified, false); err != nil {
				return false, nil, err
			}
		}
		service.cancelWatchdogLocked()
		service.active = &activeExecution{
			verified: *verified,
			state:    velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARING,
		}
		service.resetWatchdogLocked(*verified)
		return false, admission.registerLocked(service, sequence), nil
	}
	if _, err := service.renewActiveLocked(*verified, true); err != nil {
		return false, nil, err
	}
	return true, admission.registerLocked(service, sequence), nil
}

func (admission *executionAdmission) begin(service *Service, verified *stageauthority.Verified, cancellation bool) (bool, func(), error) {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	allowRenewal, err := admission.validateOperationLocked(service, verified, cancellation)
	if err != nil {
		return false, nil, err
	}
	return allowRenewal, admission.registerLocked(service, verified.Authority.GetExecutionSequence()), nil
}

func (admission *executionAdmission) validateOperationLocked(service *Service, verified *stageauthority.Verified, cancellation bool) (bool, error) {
	if admission.closing {
		return false, errors.New("ModelRuntime execution admission is closed")
	}
	stateErr := admission.checkStateLocked()
	if stateErr != nil && !cancellation {
		return false, stateErr
	}
	allowRenewal, err := service.refreshExecutionAuthority(verified, cancellation)
	if err != nil {
		return false, err
	}
	if stateErr != nil {
		allowRenewal = false
	}
	sequence := verified.Authority.GetExecutionSequence()
	aboveFloor := sequence > admission.floor
	if !aboveFloor && !cancellation {
		return false, errExecutionFloor
	}
	return allowRenewal && aboveFloor, nil
}

// Initial validation precedes operationMu, which can wait behind backend work.
// Revalidate the canonical envelope and its remaining lifetime at admission.
func (service *Service) refreshExecutionAuthority(verified *stageauthority.Verified, cancellation bool) (bool, error) {
	select {
	case <-service.closed:
		return false, errors.New("ModelRuntime service is closed")
	default:
	}
	var fresh stageauthority.Verified
	var err error
	allowRenewal := true
	if cancellation {
		fresh, allowRenewal, err = service.verifyCancellation(verified.Authority)
	} else {
		fresh, err = service.verify(verified.Authority)
	}
	if err != nil {
		return false, err
	}
	*verified = fresh
	return allowRenewal, nil
}

func (admission *executionAdmission) registerLocked(service *Service, sequence int64) func() {
	operation := &admittedOperation{sequence: sequence, done: make(chan struct{})}
	admission.active[service] = operation
	return func() {
		admission.mu.Lock()
		defer admission.mu.Unlock()
		delete(admission.active, service)
		close(operation.done)
		if admission.shutdownReady && len(admission.active) == 0 {
			admission.closeStateLocked()
		}
	}
}

func (admission *executionAdmission) checkStateLocked() error {
	if admission.closing {
		return errors.New("ModelRuntime execution admission is closed")
	}
	if admission.failed != nil {
		return admission.failed
	}
	if admission.store != nil {
		if err := admission.store.check(); err != nil {
			return admission.failStateLocked(err)
		}
	}
	return nil
}

func (admission *executionAdmission) failStateLocked(err error) error {
	if admission.failed == nil {
		admission.failed = errors.Join(ErrExecutionStateRecovery, err)
	}
	return admission.failed
}

func (admission *executionAdmission) stopAdmission() {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	admission.closing = true
}

func (admission *executionAdmission) finishShutdown() error {
	admission.mu.Lock()
	defer admission.mu.Unlock()
	admission.shutdownReady = true
	if len(admission.active) != 0 {
		return errors.New("ModelRuntime admitted calls remain in flight; execution state lock retained")
	}
	admission.closeStateLocked()
	return admission.closeErr
}

func (admission *executionAdmission) closeStateLocked() {
	if admission.store != nil {
		admission.closeErr = errors.Join(admission.closeErr, admission.store.close())
	}
}

func reusableExecution(active *activeExecution) bool {
	return active.workerReusable && !active.workerReuseDenied &&
		(active.state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED ||
			active.state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED)
}

func (service *Service) holdsExecutionSlot() bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.active != nil && !reusableExecution(service.active)
}
