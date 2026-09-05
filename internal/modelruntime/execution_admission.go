package modelruntime

import (
	"errors"
	"sync"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

var (
	errExecutionFloor = errors.New("StageAllocation execution sequence is below the Runtime admission floor")
	errSharedSlotBusy = errors.New("WorkerInstance member shared slot is held by another resident runtime")
)

// One admission boundary covers every Service owned by a Supervisor, including
// calls made directly to a Service. Its lock never spans a backend call.
type executionAdmission struct {
	mu       sync.Mutex
	services []*Service
	highest  int64
	floor    int64
	active   map[*Service]*admittedOperation
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

// The caller owns service.operationMu throughout the returned operation.
func (admission *executionAdmission) prepare(service *Service, verified *stageauthority.Verified) (bool, func(), error) {
	admission.mu.Lock()
	defer admission.mu.Unlock()
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
		service.cancelWatchdogLocked()
		// A backend failure cannot reopen an allocation, including on another profile.
		admission.highest = sequence
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
	allowRenewal, err := service.refreshExecutionAuthority(verified, cancellation)
	if err != nil {
		return false, nil, err
	}
	sequence := verified.Authority.GetExecutionSequence()
	aboveFloor := sequence > admission.floor
	if !aboveFloor && !cancellation {
		return false, nil, errExecutionFloor
	}
	return allowRenewal && aboveFloor, admission.registerLocked(service, sequence), nil
}

// Initial validation precedes operationMu, which can wait behind backend work.
// Revalidate the canonical envelope and its remaining lifetime at admission.
func (service *Service) refreshExecutionAuthority(verified *stageauthority.Verified, cancellation bool) (bool, error) {
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
	}
}

func reusableExecution(active *activeExecution) bool {
	return active.workerReusable &&
		(active.state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED ||
			active.state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED)
}

func (service *Service) holdsExecutionSlot() bool {
	service.mu.Lock()
	defer service.mu.Unlock()
	return service.active != nil && !reusableExecution(service.active)
}
