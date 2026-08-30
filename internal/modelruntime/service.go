package modelruntime

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
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
	Binding       stageauthority.RuntimeBinding
	EpochStore    EpochStore
	Validator     *stageauthority.Validator
	Backend       Backend
	Clock         Clock
	CancelTimeout time.Duration
}

type Service struct {
	velav1.UnimplementedModelRuntimeServiceServer

	binding       stageauthority.RuntimeBinding
	validator     *stageauthority.Validator
	backend       Backend
	clock         Clock
	cancelTimeout time.Duration

	operationMu sync.Mutex
	mu          sync.Mutex
	active      *activeExecution
	sealed      map[[sha256.Size]byte]*velav1.LocalMaterializationReceipt
	sealedOrder [][sha256.Size]byte
	generation  uint64
	closed      chan struct{}
	closeOnce   sync.Once
}

const maxSealedReceiptReplay = 256

type activeExecution struct {
	verified  stageauthority.Verified
	state     velav1.ModelRuntimeExecutionState
	startedAt time.Time
	timer     Timer
	receipt   *velav1.LocalMaterializationReceipt
}

func NewService(config Config) (*Service, error) {
	if config.Validator == nil {
		return nil, errors.New("ModelRuntime StageAuthority validator is required")
	}
	if config.Backend == nil {
		return nil, errors.New("ModelRuntime backend is required")
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
	if config.Binding.ModelRuntimeEpoch != 0 {
		return nil, errors.New("ModelRuntime epoch must be allocated by the epoch store")
	}
	if err := validateBindingTemplate(config.Binding); err != nil {
		return nil, err
	}
	epoch, err := config.EpochStore.Next(cloneBinding(config.Binding))
	if err != nil {
		return nil, fmt.Errorf("allocate ModelRuntime epoch: %w", err)
	}
	if epoch <= 0 {
		return nil, errors.New("ModelRuntime epoch store returned an invalid epoch")
	}
	config.Binding.ModelRuntimeEpoch = epoch
	return &Service{
		binding:       cloneBinding(config.Binding),
		validator:     config.Validator,
		backend:       config.Backend,
		clock:         config.Clock,
		cancelTimeout: config.CancelTimeout,
		sealed:        make(map[[sha256.Size]byte]*velav1.LocalMaterializationReceipt),
		closed:        make(chan struct{}),
	}, nil
}

func (service *Service) Close() {
	if service == nil {
		return
	}
	service.closeOnce.Do(func() {
		close(service.closed)
		service.mu.Lock()
		if service.active != nil && service.active.timer != nil {
			service.active.timer.Stop()
		}
		service.mu.Unlock()
	})
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
	result, err := service.backend.Probe(ctx, request.GetCheck())
	if err != nil {
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if len(result.Evidence) > maxRuntimeStatusBytes {
		response.Detail = "readiness evidence exceeds bound"
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
	replayed, err := service.installOrRenew(verified, true)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	if replayed {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
		response.State = service.activeState()
		response.Detail = "StageAttempt already prepared by the same authority"
		return response, nil
	}
	if err := service.backend.Prepare(ctx, verified, request.GetExecutionSpec()); err != nil {
		service.clearActive(verified.Digest)
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
	state, replayed, err := service.requireActive(verified, true)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
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
	if err := service.backend.Start(ctx, verified); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = state
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	startedAt := service.clock.Now()
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
	verified, err := service.verify(request.GetAuthority())
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
	service.operationMu.Lock()
	defer service.operationMu.Unlock()
	state, _, err := service.requireActive(verified, true)
	if err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
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
	if terminalState(state) {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.State = state
		response.Detail = "StageAttempt no longer has cancellable compute"
		return response, nil
	}
	if err := service.backend.Cancel(ctx, verified, request.GetReason()); err != nil {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED
		response.State = state
		response.Detail = boundedDetail(err.Error())
		return response, nil
	}
	service.setActiveState(verified.Digest, velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING)
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
	status, err := service.backend.Status(ctx, verified)
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
	service.setActiveState(verified.Digest, status.State)
	if terminalState(status.State) {
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
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REPLAYED
		response.State = state
		response.Receipt = service.activeReceipt(verified.Digest)
		response.Detail = "output already sealed"
		return response, nil
	}
	status, err := service.backend.Status(ctx, verified)
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
	return service.validator.Validate(authority, service.binding)
}

func (service *Service) installOrRenew(
	verified stageauthority.Verified,
	allowRenewal bool,
) (bool, error) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active == nil {
		service.active = &activeExecution{
			verified: verified,
			state:    velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_PREPARING,
		}
		service.resetWatchdogLocked(verified)
		return false, nil
	}
	return service.renewActiveLocked(verified, allowRenewal)
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
	if service.active.verified.Digest == verified.Digest {
		return true, nil
	}
	if !allowRenewal || terminalState(service.active.state) ||
		stageauthority.ValidateRenewal(
			service.active.verified.Authority, verified.Authority,
		) != nil {
		return false, errActiveAuthorityMismatch
	}
	service.active.verified = verified
	service.resetWatchdogLocked(verified)
	return true, nil
}

func (service *Service) resetWatchdogLocked(verified stageauthority.Verified) {
	if service.active.timer != nil {
		service.active.timer.Stop()
	}
	service.generation++
	generation := service.generation
	timer := service.clock.NewTimer(verified.MonotonicValidFor)
	service.active.timer = timer
	go func() {
		select {
		case <-timer.C():
			service.expire(generation)
		case <-service.closed:
		}
	}()
}

func (service *Service) expire(generation uint64) {
	service.operationMu.Lock()
	defer service.operationMu.Unlock()
	service.mu.Lock()
	if service.active == nil || generation != service.generation || terminalState(service.active.state) ||
		service.active.state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
		service.mu.Unlock()
		return
	}
	verified := service.active.verified
	service.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), service.cancelTimeout)
	defer cancel()
	if err := service.backend.Cancel(
		ctx,
		verified,
		velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_MONOTONIC_DEADLINE,
	); err != nil {
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
	if service.active != nil && service.active.verified.Digest == digest {
		service.active.state = state
	}
}

func (service *Service) setActiveReceipt(
	digest [32]byte,
	receipt *velav1.LocalMaterializationReceipt,
) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != nil && service.active.verified.Digest == digest {
		service.active.receipt = proto.Clone(receipt).(*velav1.LocalMaterializationReceipt)
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
		if service.active.timer != nil {
			service.active.timer.Stop()
		}
		service.generation++
		service.active = nil
	}
}

func (service *Service) stopWatchdog(digest [32]byte) {
	service.mu.Lock()
	defer service.mu.Unlock()
	if service.active != nil && service.active.verified.Digest == digest && service.active.timer != nil {
		service.active.timer.Stop()
		service.active.timer = nil
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
