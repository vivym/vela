package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageassignment"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxRuntimeStatusBytes = 16 * 1024
	maxRuntimeDetailRunes = 1000
	maxSealedOutputBytes  = 64 * 1024
)

var errActiveAuthorityMismatch = errors.New("active ModelRuntime authority does not match")

type Timer interface {
	C() <-chan time.Time
	Stop() bool
}

type Clock interface {
	Now() time.Time
	NewTimer(time.Duration) Timer
}

type Config struct {
	Binding        stageauthority.RuntimeBinding
	EpochStore     EpochStore
	Validator      *stageauthority.Validator
	Backend        Backend
	BackendFactory func(stageauthority.RuntimeBinding) (Backend, error)
	Clock          Clock
	CancelTimeout  time.Duration
	EpochFloor     int64
	MaxClockSkew   time.Duration
}

type Service struct {
	velav1.UnimplementedModelRuntimeServiceServer

	binding       stageauthority.RuntimeBinding
	validator     *stageauthority.Validator
	backend       Backend
	clock         Clock
	cancelTimeout time.Duration
	maxClockSkew  time.Duration

	operationMu   sync.Mutex
	mu            sync.Mutex
	active        *activeExecution
	backendCall   *backendExecutionCall
	admission     *executionAdmission
	admissionUsed bool
	supervised    bool
	sealed        map[[sha256.Size]byte]*velav1.LocalMaterializationReceipt
	sealedOrder   [][sha256.Size]byte
	generation    uint64
	closed        chan struct{}
	closeOnce     sync.Once
	closeErr      error
}

const maxSealedReceiptReplay = 256

type activeExecution struct {
	verified             stageauthority.Verified
	backendAuthority     *stageauthority.Verified
	state                velav1.ModelRuntimeExecutionState
	workerReusable       bool
	workerReuseDenied    bool
	reuseAfterDrain      bool
	startedAt            time.Time
	timer                Timer
	timerCancel          chan struct{}
	receipt              *velav1.LocalMaterializationReceipt
	deadlineExpired      bool
	pendingCancellations int
}

func NewService(config Config) (*Service, error) {
	if config.Validator == nil {
		return nil, errors.New("ModelRuntime StageAuthority validator is required")
	}
	if (config.Backend == nil) == (config.BackendFactory == nil) {
		return nil, errors.New("exactly one ModelRuntime backend or backend factory is required")
	}
	if config.EpochStore == nil {
		return nil, errors.New("ModelRuntime epoch store is required")
	}
	if config.Clock == nil {
		config.Clock = realClock{}
	}
	if config.CancelTimeout <= 0 || config.CancelTimeout > time.Minute {
		return nil, errors.New("ModelRuntime cancellation timeout is invalid")
	}
	if config.MaxClockSkew < 0 || config.MaxClockSkew > time.Minute {
		return nil, errors.New("ModelRuntime clock skew is invalid")
	}
	if config.Binding.ModelRuntimeEpoch != 0 {
		return nil, errors.New("ModelRuntime epoch must be allocated by the epoch store")
	}
	if err := validateBindingTemplate(config.Binding); err != nil {
		return nil, err
	}
	if config.EpochFloor < 0 {
		return nil, errors.New("ModelRuntime epoch floor is invalid")
	}
	var (
		epoch int64
		err   error
	)
	if floorStore, ok := config.EpochStore.(EpochFloorStore); ok {
		epoch, err = floorStore.NextAfter(cloneBinding(config.Binding), config.EpochFloor)
	} else {
		epoch, err = config.EpochStore.Next(cloneBinding(config.Binding))
	}
	if err != nil {
		return nil, fmt.Errorf("allocate ModelRuntime epoch: %w", err)
	}
	if epoch <= config.EpochFloor {
		return nil, errors.New("ModelRuntime epoch store returned an invalid epoch")
	}
	config.Binding.ModelRuntimeEpoch = epoch
	if config.BackendFactory != nil {
		config.Backend, err = config.BackendFactory(cloneBinding(config.Binding))
		if err != nil {
			return nil, fmt.Errorf("start resident ModelRuntime backend: %w", err)
		}
		if config.Backend == nil {
			return nil, errors.New("ModelRuntime backend factory returned no backend")
		}
	}
	service := &Service{
		binding:       cloneBinding(config.Binding),
		validator:     config.Validator,
		backend:       config.Backend,
		clock:         config.Clock,
		cancelTimeout: config.CancelTimeout,
		maxClockSkew:  config.MaxClockSkew,
		sealed:        make(map[[sha256.Size]byte]*velav1.LocalMaterializationReceipt),
		closed:        make(chan struct{}),
	}
	service.admission = newExecutionAdmission([]*Service{service})
	return service, nil
}

func (service *Service) Close() {
	_ = service.Shutdown()
}

func (service *Service) Shutdown() error {
	if service == nil {
		return nil
	}
	service.closeOnce.Do(func() {
		close(service.closed)
		service.mu.Lock()
		service.cancelWatchdogLocked()
		service.generation++
		call := service.backendCall
		service.mu.Unlock()
		if call != nil {
			call.cancel(errors.New("ModelRuntime service is closed"))
		}
		if closer, ok := service.backend.(interface{ Close() error }); ok {
			service.closeErr = closer.Close()
		}
	})
	return service.closeErr
}

func (service *Service) ProbeReadiness(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceProbeReadinessRequest,
) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	response := &velav1.ModelRuntimeServiceProbeReadinessResponse{
		Identity: runtimeIdentityProto(service.binding),
	}
	if request == nil {
		response.Detail = "readiness request is required"
		return response, nil
	}
	response.Check = request.GetCheck()
	if !matchesRuntimeIdentity(request.GetIdentity(), service.binding) {
		response.Detail = "resident runtime identity does not match"
		return response, nil
	}
	if !validReadinessCheck(request.GetCheck()) {
		response.Detail = "readiness check is invalid"
		return response, nil
	}
	if err := service.checkReadinessAdmission(); err != nil {
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	result, err := service.backend.Probe(ctx, request.GetCheck())
	if err != nil {
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if len(result.Evidence) > maxRuntimeStatusBytes {
		response.Detail = "readiness evidence exceeds bound"
		return response, nil
	}
	if err := service.checkReadinessAdmission(); err != nil {
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	response.Ready = result.Ready
	response.Evidence = append([]byte(nil), result.Evidence...)
	response.Detail = boundedDetail(result.Detail)
	return response, nil
}

func (service *Service) PrepareStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServicePrepareStageRequest,
) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	response := &velav1.ModelRuntimeServicePrepareStageResponse{
		RuntimeIdentity: runtimeIdentityProto(service.binding),
	}
	verified, err := service.verify(request.GetAuthority())
	if err != nil {
		response.Decision = authorityDecision(err)
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	response.AuthorityDigest = verified.Digest[:]
	if err := stageassignment.ValidateExecutionSpec(request.GetExecutionSpec()); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	executionSpecDigest, err := stageauthority.ExecutionSpecDigest(request.GetExecutionSpec())
	if err != nil || !bytes.Equal(executionSpecDigest[:], verified.Authority.GetExecutionSpecDigest()) {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = "StageAuthority does not authorize the supplied execution spec"
		return response, nil
	}

	service.operationMu.Lock()
	defer service.operationMu.Unlock()
	replayed, release, err := service.executionAdmission().prepare(service, &verified)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		if errors.Is(err, errSharedSlotBusy) || errors.Is(err, ErrExecutionHistoryFull) || errors.Is(err, ErrExecutionDrainUnproven) || errors.Is(err, ErrBackendIncarnationUnproven) {
			response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		}
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer release()
	if replayed {
		if err := service.synchronizeBackendAuthority(ctx, verified); err != nil {
			response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
			response.Detail = boundedDetail(err.Error())
			return response, nil
		}
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
		response.State = service.activeState()
		response.Detail = "StageAttempt already prepared by the same authority"
		return response, nil
	}
	ctx, finishCall, err := service.executionCallContext(ctx, verified)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer finishCall()
	if err := executionCallError(ctx, service.backend.Prepare(ctx, verified, request.GetExecutionSpec())); err != nil {
		if ctx.Err() != nil {
			// Request cancellation does not prove backend failure or stop. Keep
			// this execution eligible for exact cancellation and its watchdog.
			service.setReuseAfterDrain(verified.Digest, false)
			response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
			response.Detail = boundedDetail(err.Error())
			return response, nil
		}
		service.setActiveState(verified.Digest, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED)
		service.setReuseAfterDrain(verified.Digest, true)
		if drainErr := service.checkpointExecutionDrain(ctx, verified); drainErr == nil {
			service.clearActive(verified.Digest)
		} else {
			err = errors.Join(err, drainErr)
		}
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if err := service.confirmBackendAuthority(ctx, verified); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	service.setActiveState(verified.Digest, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED)
	response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED
	response.Detail = "StageAttempt prepared"
	return response, nil
}

func (service *Service) StartStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceStartStageRequest,
) (*velav1.ModelRuntimeServiceStartStageResponse, error) {
	response := &velav1.ModelRuntimeServiceStartStageResponse{
		RuntimeIdentity: runtimeIdentityProto(service.binding),
	}
	verified, err := service.verify(request.GetAuthority())
	if err != nil {
		response.Decision = authorityDecision(err)
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	response.AuthorityDigest = verified.Digest[:]
	service.operationMu.Lock()
	defer service.operationMu.Unlock()
	_, release, err := service.executionAdmission().begin(service, &verified, false)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer release()
	_, replayed, err := service.requireActive(verified, true)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if err := service.synchronizeBackendAuthority(ctx, verified); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	state := service.activeState()
	if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
		response.State = state
		response.StartedAt = timestamppb.New(service.activeStartedAt())
		response.Detail = "StageAttempt already running"
		return response, nil
	}
	if replayed && state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.State = state
		response.Detail = "StageAttempt cannot start from current runtime state"
		return response, nil
	}
	if state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARED {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.State = state
		response.Detail = "StageAttempt is not prepared"
		return response, nil
	}
	ctx, finishCall, err := service.executionCallContext(ctx, verified)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer finishCall()
	if err := executionCallError(ctx, service.backend.Start(ctx, verified)); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = state
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	startedAt := service.clock.Now()
	if err := service.confirmBackendAuthority(ctx, verified); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	service.markStarted(verified.Digest, startedAt)
	response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING
	response.StartedAt = timestamppb.New(startedAt)
	response.Detail = "StageAttempt started"
	return response, nil
}

func (service *Service) CancelStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceCancelStageRequest,
) (*velav1.ModelRuntimeServiceCancelStageResponse, error) {
	response := &velav1.ModelRuntimeServiceCancelStageResponse{
		RuntimeIdentity: runtimeIdentityProto(service.binding),
	}
	verified, allowSuccessor, err := service.verifyCancellation(request.GetAuthority())
	if err != nil {
		response.Decision = authorityDecision(err)
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	response.AuthorityDigest = verified.Digest[:]
	if !validCancelReason(request.GetReason()) {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = "cancellation reason is invalid"
		return response, nil
	}
	finishInterruption, err := service.executionAdmission().interruptCancellation(ctx, service, &verified)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer finishInterruption()
	service.operationMu.Lock()
	defer service.operationMu.Unlock()
	admissionAllowsSuccessor, release, err := service.executionAdmission().begin(service, &verified, true)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer release()
	allowSuccessor = allowSuccessor && admissionAllowsSuccessor
	if service.sealedReceipt(verified.Digest) != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED
		response.Detail = "StageAttempt output is already sealed"
		return response, nil
	}
	target, state, err := service.resolveCancellationTarget(ctx, verified, allowSuccessor)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		if errors.Is(err, errBackendAuthorityUncertain) {
			response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		}
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING ||
		state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
		response.CancellationAcknowledged = true
		response.State = state
		response.Detail = "cancellation already acknowledged"
		return response, nil
	}
	if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.State = state
		response.Detail = "StageAttempt no longer has cancellable compute"
		return response, nil
	}
	if err := service.backend.Cancel(ctx, target, request.GetReason()); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = state
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	service.setActiveState(target.Digest, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING)
	response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	response.CancellationAcknowledged = true
	response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING
	response.Detail = "cancellation signal acknowledged; runtime stop is pending"
	return response, nil
}

func (service *Service) Status(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceStatusRequest,
) (*velav1.ModelRuntimeServiceStatusResponse, error) {
	response := &velav1.ModelRuntimeServiceStatusResponse{
		RuntimeIdentity: runtimeIdentityProto(service.binding),
	}
	verified, err := service.verify(request.GetAuthority())
	if err != nil {
		response.Decision = authorityDecision(err)
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	response.AuthorityDigest = verified.Digest[:]
	service.operationMu.Lock()
	defer service.operationMu.Unlock()
	_, release, err := service.executionAdmission().begin(service, &verified, false)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer release()
	if receipt := service.sealedReceipt(verified.Digest); receipt != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
		response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED
		response.LocalReceiptId = receipt.GetReceiptId()
		response.LocalReceiptDigest = append([]byte(nil), receipt.GetManifestSha256()...)
		response.Detail = "sealed output retained for local materialization replay"
		return response, nil
	}
	if _, _, err := service.requireActive(verified, true); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	ctx, finishCall, err := service.executionCallContext(ctx, verified)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer finishCall()
	status, err := service.backend.Status(ctx, verified)
	err = executionCallError(ctx, err)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if err := validateBackendStatus(status); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	currentState := service.activeState()
	if terminalState(currentState) && (!terminalState(status.State) ||
		(currentState == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED && status.State != currentState)) {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = currentState
		response.Detail = "backend status contradicts terminal execution"
		return response, nil
	}
	if receipt := service.activeReceipt(verified.Digest); receipt != nil {
		if (status.LocalReceiptID != "" && status.LocalReceiptID != receipt.GetReceiptId()) ||
			(len(status.LocalReceiptDigest) != 0 &&
				!bytes.Equal(status.LocalReceiptDigest, receipt.GetManifestSha256())) {
			response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
			response.Detail = "backend status local receipt contradicts sealed output"
			return response, nil
		}
		status.LocalReceiptID = receipt.GetReceiptId()
		status.LocalReceiptDigest = append([]byte(nil), receipt.GetManifestSha256()...)
	}
	if err := service.confirmBackendStatus(ctx, verified, status); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	service.setActiveState(verified.Digest, status.State)
	service.setReuseAfterDrain(verified.Digest, status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED ||
		(status.FailureEvidence != nil && status.FailureEvidence.WorkerReusable))
	if status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED ||
		(status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED &&
			status.FailureEvidence != nil && status.FailureEvidence.WorkerReusable) {
		if err := service.checkpointExecutionDrain(ctx, verified); err != nil {
			response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
			response.State = status.State
			response.Detail = boundedDetail(err.Error())
			return response, nil
		}
		service.setActiveWorkerReusable(verified.Digest)
	}
	if terminalState(status.State) && (status.State != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED || status.FailureEvidence.WorkerReusable) {
		service.stopWatchdog(verified.Digest)
	}
	response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	response.State = status.State
	response.Sequence = status.Sequence
	response.BackendStage = status.BackendStage
	response.Progress = status.Progress
	response.BoundedStatusJson = append([]byte(nil), status.BoundedStatusJSON...)
	response.LocalReceiptId = status.LocalReceiptID
	response.LocalReceiptDigest = append([]byte(nil), status.LocalReceiptDigest...)
	response.Detail = boundedDetail(status.Detail)
	if status.FailureEvidence != nil {
		response.FailureEvidence = &velav1.ModelRuntimeFailureEvidence{
			FailureClass:          status.FailureEvidence.FailureClass,
			FailureFingerprint:    append([]byte(nil), status.FailureEvidence.FailureFingerprint...),
			Detail:                status.FailureEvidence.Detail,
			WorkerReusable:        status.FailureEvidence.WorkerReusable,
			ConsumedResourceUnits: status.FailureEvidence.ConsumedResourceUnits,
			FailedAt:              timestamppb.New(status.FailureEvidence.FailedAt.UTC()),
			RetryAt:               timestamppb.New(status.FailureEvidence.RetryAt.UTC()),
		}
	}
	return response, nil
}

func (service *Service) SealOutput(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceSealOutputRequest,
) (*velav1.ModelRuntimeServiceSealOutputResponse, error) {
	response := &velav1.ModelRuntimeServiceSealOutputResponse{
		RuntimeIdentity: runtimeIdentityProto(service.binding),
	}
	verified, err := service.verify(request.GetAuthority())
	if err != nil {
		response.Decision = authorityDecision(err)
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	response.AuthorityDigest = verified.Digest[:]
	service.operationMu.Lock()
	defer service.operationMu.Unlock()
	_, release, err := service.executionAdmission().begin(service, &verified, false)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer release()
	if receipt := service.sealedReceipt(verified.Digest); receipt != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
		response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED
		response.Receipt = receipt
		response.Detail = "output already sealed"
		return response, nil
	}
	state, _, err := service.requireActive(verified, true)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED {
		if service.activeReceipt(verified.Digest) == nil {
			response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
			response.State = state
			response.Detail = "sealed output lacks its retained receipt"
			return response, nil
		}
		if err := service.checkpointExecutionDrain(ctx, verified); err != nil {
			response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
			response.State = state
			response.Detail = boundedDetail(err.Error())
			return response, nil
		}
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
		response.State = state
		response.Receipt = service.activeReceipt(verified.Digest)
		service.rememberSealedReceipt(verified.Digest, response.Receipt)
		service.clearActive(verified.Digest)
		response.Detail = "output already sealed"
		return response, nil
	}
	if terminalState(state) {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = state
		response.Detail = "terminal StageAttempt cannot seal new output"
		return response, nil
	}
	ctx, finishCall, err := service.executionCallContext(ctx, verified)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	defer finishCall()
	status, err := service.backend.Status(ctx, verified)
	err = executionCallError(ctx, err)
	if err == nil {
		err = validateBackendStatus(status)
		if err == nil {
			err = service.confirmBackendStatus(ctx, verified, status)
		}
	}
	if err != nil || status.State != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = status.State
		response.Detail = "runtime output is not ready to seal"
		return response, nil
	}
	sealed, err := service.backend.Seal(ctx, verified)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = state
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if len(sealed.OutputManifestJSON) == 0 || len(sealed.OutputManifestJSON) > maxSealedOutputBytes ||
		sealed.TotalSizeBytes < 0 {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = state
		response.Detail = "sealed output receipt is invalid"
		return response, nil
	}
	digest := sha256.Sum256(sealed.OutputManifestJSON)
	receiptID := uuid.NewSHA1(
		uuid.NameSpaceOID,
		append([]byte(verified.Authority.GetStageLeaseId()+"\x00"), digest[:]...),
	)
	receipt := &velav1.LocalMaterializationReceipt{
		ReceiptId: receiptID.String(), ManifestSha256: digest[:],
		TotalSizeBytes: sealed.TotalSizeBytes, SealedAt: timestamppb.New(service.clock.Now()),
		OutputManifestJson: append([]byte(nil), sealed.OutputManifestJSON...),
	}
	// Seal cannot be repeated if a subsequent drain or fsync fails. Retain the
	// receipt and execution slot until the exact drain checkpoint is durable.
	service.mu.Lock()
	service.active.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED
	service.active.receipt = proto.Clone(receipt).(*velav1.LocalMaterializationReceipt)
	service.cancelWatchdogLocked()
	service.mu.Unlock()
	if err := context.Cause(ctx); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if err := service.checkpointExecutionDrain(ctx, verified); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	service.rememberSealedReceipt(verified.Digest, receipt)
	service.clearActive(verified.Digest)
	response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	response.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED
	response.Receipt = receipt
	response.Detail = "local output sealed"
	return response, nil
}

func (service *Service) verify(authority *velav1.StageAuthority) (stageauthority.Verified, error) {
	if service == nil || service.validator == nil {
		return stageauthority.Verified{}, errors.New("ModelRuntime service is not configured")
	}
	return service.validator.ValidateWithClockSkew(
		authority,
		service.binding,
		service.maxClockSkew,
	)
}

func (service *Service) verifyCancellation(
	authority *velav1.StageAuthority,
) (stageauthority.Verified, bool, error) {
	verified, err := service.verify(authority)
	if err == nil {
		return verified, true, nil
	}
	if !errors.Is(err, stageauthority.ErrStale) {
		return stageauthority.Verified{}, false, err
	}
	verified, err = service.validator.ValidateSignature(authority, service.binding)
	return verified, false, err
}

func (service *Service) requireActive(
	verified stageauthority.Verified,
	allowRenewal bool,
) (velav1.ModelRuntimeExecutionState, bool, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil {
		return velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED, false,
			errActiveAuthorityMismatch
	}
	replayed, err := service.renewActiveLocked(verified, allowRenewal)
	if err != nil {
		return velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED, false, err
	}
	return service.active.state, replayed, nil
}

func (service *Service) renewActiveLocked(
	verified stageauthority.Verified,
	allowRenewal bool,
) (bool, error) {
	if service.active.deadlineExpired {
		return false, errExecutionDeadline
	}
	if service.active.pendingCancellations != 0 {
		return false, errExecutionCancellation
	}
	if service.active.verified.Digest == verified.Digest {
		return true, nil
	}
	if !allowRenewal || terminalState(service.active.state) ||
		stageauthority.ValidateRenewal(
			service.active.verified.Authority, verified.Authority,
		) != nil {
		return false, errActiveAuthorityMismatch
	}
	if service.active.backendAuthority == nil || service.active.backendAuthority.Digest != service.active.verified.Digest {
		return false, errBackendAuthorityUncertain
	}
	service.active.verified = verified
	service.resetWatchdogLocked(verified)
	return true, nil
}

func (service *Service) resetWatchdogLocked(verified stageauthority.Verified) {
	service.cancelWatchdogLocked()
	service.generation++
	generation := service.generation
	timer := service.clock.NewTimer(verified.MonotonicValidFor)
	canceled := make(chan struct{})
	service.active.timer = timer
	service.active.timerCancel = canceled
	go func() {
		select {
		case <-timer.C():
			service.expire(generation)
		case <-canceled:
		case <-service.closed:
		}
	}()
}

// Timer.Stop does not close C, so each replaced watcher also needs cancellation.
func (service *Service) cancelWatchdogLocked() {
	if service.active == nil {
		return
	}
	if service.active.timer != nil {
		service.active.timer.Stop()
		service.active.timer = nil
	}
	if service.active.timerCancel != nil {
		close(service.active.timerCancel)
		service.active.timerCancel = nil
	}
}

func (service *Service) expire(generation uint64) {
	service.mu.Lock()
	if !needsExecutionCancellation(service.active) || generation != service.generation {
		service.mu.Unlock()
		return
	}
	service.active.deadlineExpired = true
	call := service.backendCall
	if call != nil && call.generation == generation {
		call.cancel(errExecutionDeadline)
	}
	service.mu.Unlock()
	service.operationMu.Lock()
	defer service.operationMu.Unlock()
	service.mu.Lock()
	if !needsExecutionCancellation(service.active) || generation != service.generation {
		service.mu.Unlock()
		return
	}
	verified := service.active.verified
	service.mu.Unlock()
	_, release, err := service.executionAdmission().begin(service, &verified, true)
	if err != nil {
		return
	}
	defer release()
	ctx, cancel := context.WithTimeout(context.Background(), service.cancelTimeout)
	defer cancel()
	target, _, err := service.resolveCancellationTarget(ctx, verified, false)
	if err == nil {
		err = service.backend.Cancel(ctx, target, velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE)
	}
	if err != nil {
		service.setActiveState(
			verified.Digest,
			velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED,
		)
		return
	}
	service.setActiveState(
		verified.Digest,
		velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING,
	)
}

func (service *Service) setActiveState(
	digest [32]byte,
	state velav1.ModelRuntimeExecutionState,
) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active.knowsAuthority(digest) {
		service.active.state = state
	}
}

func (service *Service) setActiveWorkerReusable(digest [32]byte) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != nil && service.active.verified.Digest == digest {
		service.active.workerReusable = true
	}
}

func (service *Service) setReuseAfterDrain(digest [32]byte, reusable bool) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != nil && service.active.verified.Digest == digest {
		service.active.reuseAfterDrain = reusable
		if !reusable {
			service.active.workerReusable = false
		}
	}
}

func (service *Service) activeReceipt(digest [32]byte) *velav1.LocalMaterializationReceipt {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil || service.active.verified.Digest != digest || service.active.receipt == nil {
		return nil
	}
	return proto.Clone(service.active.receipt).(*velav1.LocalMaterializationReceipt)
}

func (service *Service) rememberSealedReceipt(
	digest [sha256.Size]byte,
	receipt *velav1.LocalMaterializationReceipt,
) {
	service.mu.Lock()
	defer service.mu.Unlock()
	service.rememberSealedReceiptLocked(digest, receipt)
}

func (service *Service) rememberSealedReceiptLocked(digest [sha256.Size]byte, receipt *velav1.LocalMaterializationReceipt) {
	if receipt == nil {
		return
	}
	if _, exists := service.sealed[digest]; !exists {
		service.sealedOrder = append(service.sealedOrder, digest)
	}
	service.sealed[digest] = proto.Clone(receipt).(*velav1.LocalMaterializationReceipt)
	for len(service.sealedOrder) > maxSealedReceiptReplay {
		oldest := service.sealedOrder[0]
		service.sealedOrder = service.sealedOrder[1:]
		delete(service.sealed, oldest)
	}
}

func (service *Service) sealedReceipt(
	digest [sha256.Size]byte,
) *velav1.LocalMaterializationReceipt {
	service.mu.Lock()
	defer service.mu.Unlock()
	receipt := service.sealed[digest]
	if receipt == nil {
		return nil
	}
	return proto.Clone(receipt).(*velav1.LocalMaterializationReceipt)
}

func (service *Service) markStarted(digest [32]byte, startedAt time.Time) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != nil && service.active.verified.Digest == digest {
		service.active.state = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING
		service.active.startedAt = startedAt
	}
}

func (service *Service) activeStartedAt() time.Time {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil {
		return time.Time{}
	}
	return service.active.startedAt
}

func (service *Service) activeState() velav1.ModelRuntimeExecutionState {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil {
		return velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED
	}
	return service.active.state
}

func (service *Service) clearActive(digest [32]byte) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != nil && service.active.verified.Digest == digest {
		service.cancelWatchdogLocked()
		service.generation++
		service.active = nil
	}
}

func (service *Service) stopWatchdog(digest [32]byte) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != nil && service.active.verified.Digest == digest && service.active.timer != nil {
		service.cancelWatchdogLocked()
		service.generation++
	}
}

func validateBindingTemplate(binding stageauthority.RuntimeBinding) error {
	if binding.WorkerInstanceID == "" || binding.WorkerInstanceEpoch <= 0 ||
		binding.WorkerMemberID == "" || binding.WorkerMemberEpoch <= 0 ||
		len(binding.DeviceSetDigest) != sha256.Size || len(binding.Devices) == 0 ||
		len(binding.MembershipDigest) != sha256.Size || len(binding.Members) == 0 ||
		binding.ModelResidencyID == "" || binding.ModelRuntimeIdentity == "" ||
		binding.StageProfileRevisionID == "" {
		return errors.New("ModelRuntime resident binding is incomplete")
	}
	return nil
}

func cloneBinding(binding stageauthority.RuntimeBinding) stageauthority.RuntimeBinding {
	binding.DeviceSetDigest = append([]byte(nil), binding.DeviceSetDigest...)
	binding.MembershipDigest = append([]byte(nil), binding.MembershipDigest...)
	binding.Devices = append([]stageauthority.DeviceEpoch(nil), binding.Devices...)
	binding.Members = append([]stageauthority.MemberEpoch(nil), binding.Members...)
	return binding
}

func runtimeIdentityProto(binding stageauthority.RuntimeBinding) *velav1.ModelRuntimeIdentity {
	return &velav1.ModelRuntimeIdentity{
		WorkerInstanceId: binding.WorkerInstanceID, WorkerInstanceEpoch: binding.WorkerInstanceEpoch,
		DeviceSetDigest:  append([]byte(nil), binding.DeviceSetDigest...),
		MembershipDigest: append([]byte(nil), binding.MembershipDigest...),
		ModelResidencyId: binding.ModelResidencyID, RuntimeIdentity: binding.ModelRuntimeIdentity,
		ModelRuntimeEpoch:      binding.ModelRuntimeEpoch,
		StageProfileRevisionId: binding.StageProfileRevisionID,
		WorkerMemberId:         binding.WorkerMemberID,
		WorkerMemberEpoch:      binding.WorkerMemberEpoch,
	}
}

func matchesRuntimeIdentity(
	identity *velav1.ModelRuntimeIdentity,
	binding stageauthority.RuntimeBinding,
) bool {
	return identity != nil && identity.GetWorkerInstanceId() == binding.WorkerInstanceID &&
		identity.GetWorkerInstanceEpoch() == binding.WorkerInstanceEpoch &&
		bytes.Equal(identity.GetDeviceSetDigest(), binding.DeviceSetDigest) &&
		bytes.Equal(identity.GetMembershipDigest(), binding.MembershipDigest) &&
		identity.GetModelResidencyId() == binding.ModelResidencyID &&
		identity.GetRuntimeIdentity() == binding.ModelRuntimeIdentity &&
		identity.GetModelRuntimeEpoch() == binding.ModelRuntimeEpoch &&
		identity.GetStageProfileRevisionId() == binding.StageProfileRevisionID &&
		identity.GetWorkerMemberId() == binding.WorkerMemberID &&
		identity.GetWorkerMemberEpoch() == binding.WorkerMemberEpoch
}

func validateBackendStatus(status BackendStatus) error {
	if !validRuntimeState(status.State) || status.Sequence < 0 ||
		len(status.BoundedStatusJSON) > maxRuntimeStatusBytes ||
		!utf8.ValidString(status.BackendStage) || len(status.BackendStage) > 100 ||
		len(status.LocalReceiptDigest) != 0 && len(status.LocalReceiptDigest) != sha256.Size {
		return errors.New("ModelRuntime backend status is invalid")
	}
	if status.Progress != nil && (*status.Progress < 0 || *status.Progress > 1) {
		return errors.New("ModelRuntime backend progress is invalid")
	}
	if (status.State == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED) !=
		(status.FailureEvidence != nil) {
		return errors.New("ModelRuntime backend failure evidence does not match execution state")
	}
	if status.FailureEvidence != nil {
		if err := validateFailureEvidence(status.FailureEvidence); err != nil {
			return err
		}
	}
	return nil
}

func validateFailureEvidence(evidence *FailureEvidence) error {
	if evidence == nil || strings.TrimSpace(evidence.FailureClass) == "" ||
		evidence.FailureClass != strings.TrimSpace(evidence.FailureClass) ||
		len(evidence.FailureClass) > 100 || !utf8.ValidString(evidence.FailureClass) ||
		len(evidence.FailureFingerprint) != sha256.Size ||
		!utf8.ValidString(evidence.Detail) || utf8.RuneCountInString(evidence.Detail) > maxRuntimeDetailRunes ||
		evidence.ConsumedResourceUnits <= 0 || evidence.FailedAt.IsZero() || evidence.RetryAt.IsZero() ||
		!evidence.RetryAt.After(evidence.FailedAt) {
		return errors.New("ModelRuntime backend failure evidence is invalid")
	}
	if err := timestamppb.New(evidence.FailedAt).CheckValid(); err != nil {
		return errors.New("ModelRuntime backend failed_at is invalid")
	}
	if err := timestamppb.New(evidence.RetryAt).CheckValid(); err != nil {
		return errors.New("ModelRuntime backend retry_at is invalid")
	}
	return nil
}

func validRuntimeState(state velav1.ModelRuntimeExecutionState) bool {
	return state >= velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARING &&
		state <= velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED
}

func terminalState(state velav1.ModelRuntimeExecutionState) bool {
	return state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED ||
		state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED ||
		state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED
}

func validReadinessCheck(check velav1.ModelRuntimeReadinessCheck) bool {
	return check >= velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_DEVICE &&
		check <= velav1.ModelRuntimeReadinessCheck_MODEL_RUNTIME_READINESS_CHECK_CANARY
}

func validCancelReason(reason velav1.ModelRuntimeCancelReason) bool {
	return reason >= velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP &&
		reason <= velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MEMBER_BARRIER_FAILED
}

func authorityDecision(errorValue error) velav1.ModelRuntimeCommandDecision {
	if errors.Is(errorValue, stageauthority.ErrInvalid) ||
		errors.Is(errorValue, stageauthority.ErrInvalidSignature) ||
		errors.Is(errorValue, stageauthority.ErrUnknownKey) ||
		errors.Is(errorValue, stageauthority.ErrStale) ||
		errors.Is(errorValue, stageauthority.ErrRuntimeMismatch) {
		return velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
	}
	return velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
}

func boundedDetail(detail string) string {
	runes := []rune(detail)
	if len(runes) > maxRuntimeDetailRunes {
		runes = runes[:maxRuntimeDetailRunes]
	}
	return string(runes)
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) NewTimer(duration time.Duration) Timer {
	return realTimer{timer: time.NewTimer(duration)}
}

type realTimer struct {
	timer *time.Timer
}

func (timer realTimer) C() <-chan time.Time {
	return timer.timer.C
}

func (timer realTimer) Stop() bool {
	return timer.timer.Stop()
}
