package stageworkeragent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var ErrStageWorkerBusy = errors.New("stage worker already has active compute authority")

type ControlClient interface {
	Exchange(
		context.Context,
		*velav1.StageWorkerControlServiceConnectRequest,
	) (*velav1.StageWorkerControlServiceConnectResponse, error)
	NextCommand(context.Context) (*velav1.StageWorkerControlServiceConnectResponse, error)
}

type StreamAgent struct {
	runtime            *Agent
	control            ControlClient
	inputResolver      InputResolver
	admission          *FileAssignmentAdmission
	terminalRetirement *TerminalScratchRetirement
	materialization    *streamMaterialization
	materializationMu  sync.Mutex
	assignmentMu       sync.Mutex
	inputMu            sync.Mutex
	pendingInputs      *pendingAssignmentInputs
	runtimeMu          sync.Mutex
	startingAuthority  *velav1.StageAuthority
	stopGeneration     uint64
	mu                 sync.Mutex
	active             *velav1.StageAuthority
}

type pendingAssignmentInputs struct {
	authorityDigest [sha256.Size]byte
	cancel          context.CancelFunc
}

type DurableStreamConfig struct {
	Runtime            *Agent
	Control            ControlClient
	Admission          *FileAssignmentAdmission
	InputResolver      InputResolver
	Materialization    *MaterializationConfig
	TerminalRetirement *TerminalScratchRetirement
}

func NewDurableStreamAgent(config DurableStreamConfig) (*StreamAgent, error) {
	if config.Admission == nil {
		return nil, errors.New("durable Stage Worker stream requires assignment admission")
	}
	if retirement := config.TerminalRetirement; retirement != nil &&
		(retirement.gate != config.Admission || retirement.runtime != config.Runtime || config.Materialization == nil ||
			config.Materialization.OutputOwnershipContract != AttemptOwnedFilesystemScratchV1) {
		return nil, errors.New("terminal retirement must share the durable Stream admission, Runtime and output contract")
	}
	var agent *StreamAgent
	var err error
	if config.Materialization == nil {
		agent, err = NewStreamAgent(config.Runtime, config.Control)
	} else {
		agent, err = NewMaterializingStreamAgent(config.Runtime, config.Control, *config.Materialization)
	}
	if err != nil {
		return nil, err
	}
	agent.admission, agent.inputResolver = config.Admission, config.InputResolver
	agent.terminalRetirement = config.TerminalRetirement
	return agent, nil
}

type AssignmentExecutionResult struct {
	StartBarrierResult
	ControlStartAccepted bool
}

type ReattachResult struct {
	Accepted bool
	Status   AggregateStatus
}

func NewStreamAgent(runtime *Agent, control ControlClient) (*StreamAgent, error) {
	if runtime == nil || len(runtime.ids) == 0 {
		return nil, errors.New("missing Stage Worker runtime Agent")
	}
	if control == nil {
		return nil, errors.New("missing Stage Worker control stream")
	}
	return &StreamAgent{runtime: runtime, control: control}, nil
}

func NewInputResolvingStreamAgent(
	runtime *Agent,
	control ControlClient,
	resolver InputResolver,
) (*StreamAgent, error) {
	if resolver == nil {
		return nil, errors.New("missing Stage Worker input resolver")
	}
	agent, err := NewStreamAgent(runtime, control)
	if err != nil {
		return nil, err
	}
	agent.inputResolver = resolver
	return agent, nil
}

func NewMaterializingStreamAgent(
	runtime *Agent,
	control ControlClient,
	config MaterializationConfig,
) (*StreamAgent, error) {
	agent, err := NewStreamAgent(runtime, control)
	if err != nil {
		return nil, err
	}
	if config.Validator == nil || config.Source == nil || config.Publisher == nil ||
		config.Journal == nil || config.SourceLossEvidence == nil {
		return nil, errors.New("stage worker materialization configuration is incomplete")
	}
	if config.MaxClockSkew < 0 || config.MaxClockSkew > time.Minute {
		return nil, errors.New("stage worker materialization clock skew is invalid")
	}
	if (config.ScratchRetirer != nil && config.OutputOwnershipContract != AttemptOwnedFilesystemScratchV1) ||
		(config.ScratchRetirer == nil && config.OutputOwnershipContract != "") {
		return nil, errors.New("scratch retirement requires an explicit supported output ownership contract")
	}
	agent.materialization = &streamMaterialization{
		validator: config.Validator, source: config.Source,
		publisher: config.Publisher, journal: config.Journal,
		scratchRetirer:          config.ScratchRetirer,
		outputOwnershipContract: config.OutputOwnershipContract,
		sourceLossEvidence:      config.SourceLossEvidence,
		maxClockSkew:            config.MaxClockSkew,
	}
	return agent, nil
}

func NewInputResolvingMaterializingStreamAgent(
	runtime *Agent,
	control ControlClient,
	materializationConfig MaterializationConfig,
	resolver InputResolver,
) (*StreamAgent, error) {
	if resolver == nil {
		return nil, errors.New("missing Stage Worker input resolver")
	}
	agent, err := NewMaterializingStreamAgent(runtime, control, materializationConfig)
	if err != nil {
		return nil, err
	}
	agent.inputResolver = resolver
	return agent, nil
}

func (agent *StreamAgent) RunControlCommands(ctx context.Context) error {
	if agent == nil || agent.runtime == nil || agent.control == nil || ctx == nil {
		return errors.New("missing configured Stage Worker command consumer")
	}
	for {
		command, err := agent.control.NextCommand(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if command == nil || command.GetRequestId() != "" || command.GetStopStage() == nil {
			return errors.New("invalid unsolicited Stage Worker control command")
		}
		if err := agent.handleUnsolicitedStop(ctx, command.GetStopStage()); err != nil {
			return fmt.Errorf("handle unsolicited StopStage: %w", err)
		}
	}
}

func (agent *StreamAgent) handleUnsolicitedStop(ctx context.Context, stop *velav1.StopStage) error {
	if canceled, err := agent.cancelPendingInputs(ctx, stop); canceled || err != nil {
		return err
	}
	agent.runtimeMu.Lock()
	defer agent.runtimeMu.Unlock()
	stopDigest, err := stageauthority.Digest(stop.GetAuthority())
	if err != nil || stop.GetReason() == velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_UNSPECIFIED {
		return errors.New("invalid Stage Worker StopStage command")
	}
	active := agent.runtimeAuthority()
	if active == nil {
		return nil
	}
	activeDigest, err := stageauthority.Digest(active)
	if err != nil {
		return err
	}
	// A command admitted before a renewal or new assignment can arrive late.
	if activeDigest != stopDigest {
		return nil
	}
	_, err = agent.handleStop(ctx, stop)
	return err
}

func (agent *StreamAgent) ExecuteAssignment(
	ctx context.Context,
	assignment *velav1.StageAssignment,
) (AssignmentExecutionResult, error) {
	return agent.executeAssignment(ctx, assignment, uuid.Nil)
}

func (agent *StreamAgent) ExecuteAcquiredAssignment(ctx context.Context, assignment *velav1.StageAssignment, acquireID uuid.UUID) (AssignmentExecutionResult, error) {
	if acquireID == uuid.Nil {
		return AssignmentExecutionResult{}, errors.New("StageAssignment requires its original Acquire command ID")
	}
	return agent.executeAssignment(ctx, assignment, acquireID)
}

func (agent *StreamAgent) executeAssignment(ctx context.Context, assignment *velav1.StageAssignment, acquireID uuid.UUID) (result AssignmentExecutionResult, resultErr error) {
	if agent == nil || agent.runtime == nil || agent.control == nil || ctx == nil {
		return result, errors.New("missing configured Stage Worker stream Agent")
	}
	if agent.admission != nil && acquireID == uuid.Nil {
		return result, errors.New("durable StageAssignment requires its original Acquire command ID")
	}
	agent.assignmentMu.Lock()
	defer agent.assignmentMu.Unlock()
	if agent.activeAuthority() != nil {
		return result, ErrStageWorkerBusy
	}
	if _, _, err := agent.runtime.validateAssignment(assignment); err != nil {
		return result, err
	}
	digest, err := stageauthority.Digest(assignment.GetAuthority())
	if err != nil {
		return result, err
	}
	ctx, cancelInputs := context.WithCancel(ctx)
	defer cancelInputs()
	inputs := &pendingAssignmentInputs{authorityDigest: digest, cancel: cancelInputs}
	var admission *AssignmentAdmission
	inputsReturned := true
	agent.inputMu.Lock()
	if agent.admission != nil {
		admission, err = agent.admission.Begin(ctx, assignment, acquireID)
		if err != nil {
			agent.inputMu.Unlock()
			return result, err
		}
	}
	agent.pendingInputs = inputs
	agent.inputMu.Unlock()
	defer func() {
		cancelInputs()
		agent.inputMu.Lock()
		if agent.pendingInputs == inputs {
			agent.pendingInputs = nil
		}
		agent.inputMu.Unlock()
		if admission != nil {
			// Cancellation must not erase evidence after the resolver has returned.
			// No RPC or additional input work runs under this uncanceled context.
			if inputsReturned {
				resultErr = errors.Join(resultErr, admission.CompleteInputs(context.WithoutCancel(ctx)))
			}
			admission.Release()
		}
	}()
	if agent.materialization != nil {
		if err := agent.materialization.journal.EnsureCapacity(ctx); err != nil {
			return result, fmt.Errorf("reserve local materialization recovery capacity: %w", err)
		}
	}
	if len(assignment.GetExecutionSpec().GetInputs()) != 0 ||
		len(assignment.GetExecutionSpec().GetRootInputs()) != 0 {
		if agent.inputResolver == nil {
			return result, errors.New("StageAssignment inputs require a configured input resolver")
		}
		inputsReturned = false
		resolveErr := agent.inputResolver.Resolve(ctx, assignment)
		inputsReturned = true
		if resolveErr != nil {
			return result, fmt.Errorf("resolve StageAssignment inputs: %w", resolveErr)
		}
	}
	if admission != nil {
		if err := admission.CompleteInputs(ctx); err != nil {
			return result, err
		}
	}
	agent.runtimeMu.Lock()
	// Stop either closes input admission here or follows the serialized Runtime
	// path after this transition. It never waits for the input download itself.
	agent.inputMu.Lock()
	inputErr := ctx.Err()
	agent.pendingInputs = nil
	agent.inputMu.Unlock()
	if inputErr != nil {
		agent.runtimeMu.Unlock()
		return result, inputErr
	}
	if admission != nil {
		if err := admission.EnterRuntime(ctx); err != nil {
			agent.runtimeMu.Unlock()
			return result, err
		}
	}
	barrier, err := agent.runtime.PrepareAndStart(ctx, assignment)
	result.StartBarrierResult = barrier
	if err != nil {
		agent.runtimeMu.Unlock()
		return result, err
	}
	agent.startingAuthority = proto.Clone(assignment.GetAuthority()).(*velav1.StageAuthority)
	stopGeneration := agent.stopGeneration
	agent.runtimeMu.Unlock()
	response, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_StartStage{
			StartStage: &velav1.StartStageRequest{
				Authority: assignment.GetAuthority(),
				StartedAt: timestamppb.New(barrier.StartedAt),
			},
		},
	})
	agent.runtimeMu.Lock()
	defer func() {
		agent.startingAuthority = nil
		agent.runtimeMu.Unlock()
	}()
	if err != nil {
		acknowledged, cancelErr := agent.cancelAfterControlStartFailure(
			ctx, assignment.GetAuthority(), err,
		)
		result.CancellationAcknowledgedMembers = acknowledged
		return result, cancelErr
	}
	if agent.stopGeneration != stopGeneration {
		return result, errors.New("control stopped StageAttempt while start response was pending")
	}
	if stop := response.GetStopStage(); stop != nil {
		cancelResult, cancelErr := agent.handleStop(ctx, stop)
		result.CancellationAcknowledgedMembers = cancelResult.AcknowledgedMembers
		return result, errors.Join(errors.New("control stopped StageAttempt during start"), cancelErr)
	}
	command := response.GetStageCommandResult()
	if command == nil || command.GetOperation() !=
		velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE ||
		(command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED &&
			command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED) {
		acknowledged, cancelErr := agent.cancelAfterControlStartFailure(
			ctx,
			assignment.GetAuthority(),
			errors.New("control rejected StageAttempt start authority"),
		)
		result.CancellationAcknowledgedMembers = acknowledged
		return result, cancelErr
	}
	activeAuthority := assignment.GetAuthority()
	if renewed := command.GetRenewedAuthority(); renewed != nil {
		if err := agent.observeRuntimeAuthority(ctx, renewed); err != nil {
			acknowledged, cancelErr := agent.cancelAfterControlStartFailure(ctx, assignment.GetAuthority(), err)
			result.CancellationAcknowledgedMembers = acknowledged
			return result, cancelErr
		}
		if _, err := agent.runtime.Status(ctx, renewed); err != nil {
			acknowledged, cancelErr := agent.cancelAfterControlStartFailure(
				ctx,
				assignment.GetAuthority(),
				fmt.Errorf("validate renewed StageAuthority with resident runtime: %w", err),
			)
			result.CancellationAcknowledgedMembers = acknowledged
			return result, cancelErr
		}
		activeAuthority = renewed
	}
	if err := agent.observeRuntimeAuthority(ctx, activeAuthority); err != nil {
		acknowledged, cancelErr := agent.cancelAfterControlStartFailure(ctx, activeAuthority, err)
		result.CancellationAcknowledgedMembers = acknowledged
		return result, cancelErr
	}
	result.ControlStartAccepted = true
	agent.setActive(activeAuthority)
	return result, nil
}

func (agent *StreamAgent) Heartbeat(
	ctx context.Context,
	sequence int64,
) (*velav1.StageCommandResult, error) {
	if agent == nil || sequence <= 0 {
		return nil, errors.New("invalid Stage Worker heartbeat sequence")
	}
	agent.runtimeMu.Lock()
	authority := agent.activeAuthority()
	if authority == nil {
		agent.runtimeMu.Unlock()
		return nil, errors.New("missing active StageAuthority on Stage Worker")
	}
	if err := agent.observeRuntimeAuthority(ctx, authority); err != nil {
		agent.runtimeMu.Unlock()
		return nil, err
	}
	statusResult, err := agent.runtime.Status(ctx, authority)
	stopGeneration := agent.stopGeneration
	agent.runtimeMu.Unlock()
	if err != nil {
		return nil, err
	}
	statusJSON, err := json.Marshal(struct {
		States map[string]velav1.ModelRuntimeExecutionState `json:"member_states"`
	}{States: statusResult.States})
	if err != nil {
		return nil, fmt.Errorf("encode bounded ModelRuntime status: %w", err)
	}
	response, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage{
			HeartbeatStage: &velav1.HeartbeatStageRequest{
				Authority: authority, Sequence: sequence,
				RuntimeState:      aggregateRuntimeState(statusResult),
				BoundedStatusJson: statusJSON,
				ObservedAt:        timestamppb.Now(),
			},
		},
	})
	if err != nil {
		return nil, err
	}
	agent.runtimeMu.Lock()
	defer agent.runtimeMu.Unlock()
	if agent.stopGeneration != stopGeneration {
		return nil, errors.New("control stopped StageAttempt while heartbeat response was pending")
	}
	if agent.startingAuthority != nil || !proto.Equal(agent.activeAuthority(), authority) {
		return nil, errors.New("StageAuthority changed while heartbeat response was pending")
	}
	if stop := response.GetStopStage(); stop != nil {
		_, cancelErr := agent.handleStop(ctx, stop)
		return nil, errors.Join(errors.New("control stopped StageAttempt during heartbeat"), cancelErr)
	}
	command := response.GetStageCommandResult()
	if command == nil || command.GetOperation() !=
		velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE ||
		(command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED &&
			command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED) {
		return command, errors.New("rejected Stage Worker heartbeat authority")
	}
	if renewed := command.GetRenewedAuthority(); renewed != nil {
		if err := agent.observeRuntimeAuthority(ctx, renewed); err != nil {
			return nil, err
		}
		if _, err := agent.runtime.Status(ctx, renewed); err != nil {
			return nil, fmt.Errorf("validate renewed StageAuthority with resident runtime: %w", err)
		}
		agent.setActive(renewed)
	}
	return command, nil
}

func (agent *StreamAgent) Fail(
	ctx context.Context,
	status AggregateStatus,
) (*velav1.StageCommandResult, error) {
	if agent == nil || agent.runtime == nil || agent.control == nil || ctx == nil {
		return nil, errors.New("missing configured Stage Worker failure reporter")
	}
	authority := agent.activeAuthority()
	if authority == nil {
		return nil, errors.New("missing active StageAuthority on Stage Worker")
	}
	failure, err := aggregateFailureEvidence(status)
	if err != nil {
		return nil, err
	}
	failure.Authority = authority
	response, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		RequestId: uuid.NewSHA1(uuid.NameSpaceOID, []byte("vela/stage-worker/fail/"+authority.GetStageAttemptId())).String(),
		Operation: &velav1.StageWorkerControlServiceConnectRequest_FailStage{FailStage: failure},
	})
	if err != nil {
		return nil, err
	}
	agent.runtimeMu.Lock()
	defer agent.runtimeMu.Unlock()
	if stop := response.GetStopStage(); stop != nil {
		_, cancelErr := agent.handleStop(ctx, stop)
		return nil, errors.Join(errors.New("control stopped StageAttempt during failure report"), cancelErr)
	}
	command := response.GetStageCommandResult()
	if command == nil || command.GetOperation() !=
		velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_FAIL_STAGE ||
		(command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED &&
			command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED) {
		return command, errors.New("control rejected Stage Worker failure evidence")
	}
	if err := agent.closeAdmission(ctx, authority); err != nil {
		return nil, err
	}
	agent.clearActive(authority)
	return command, nil
}

func aggregateFailureEvidence(status AggregateStatus) (*velav1.FailStageRequest, error) {
	if status.ReportingMembers <= 0 || status.ReportingMembers != len(status.States) ||
		len(status.Failures) == 0 {
		return nil, errors.New("incomplete ModelRuntime failure evidence")
	}
	memberIDs := make([]string, 0, len(status.Failures))
	failedStates := 0
	for memberID, state := range status.States {
		if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED {
			failedStates++
			if status.Failures[memberID] == nil {
				return nil, errors.New("failed ModelRuntime member omitted failure evidence")
			}
		} else if status.Failures[memberID] != nil {
			return nil, errors.New("non-failed ModelRuntime member returned failure evidence")
		}
	}
	if failedStates != len(status.Failures) {
		return nil, errors.New("ModelRuntime failure evidence references an unknown member")
	}
	for memberID, evidence := range status.Failures {
		if uuid.Validate(memberID) != nil || !validRuntimeFailureEvidence(evidence) {
			return nil, errors.New("invalid ModelRuntime member failure evidence")
		}
		memberIDs = append(memberIDs, memberID)
	}
	slices.Sort(memberIDs)

	type fingerprintMember struct {
		MemberID    string `json:"member_id"`
		Class       string `json:"failure_class"`
		Fingerprint string `json:"failure_fingerprint"`
	}
	canonical := make([]fingerprintMember, 0, len(memberIDs))
	detailParts := make([]string, 0, len(memberIDs))
	failureClass := status.Failures[memberIDs[0]].GetFailureClass()
	workerReusable := true
	var consumedResourceUnits int64
	var failedAt time.Time
	var retryAt time.Time
	for _, memberID := range memberIDs {
		evidence := status.Failures[memberID]
		if evidence.GetFailureClass() != failureClass {
			failureClass = "multi_member_failure"
		}
		if consumedResourceUnits > math.MaxInt64-evidence.GetConsumedResourceUnits() {
			return nil, errors.New("ModelRuntime failure resource units overflow")
		}
		consumedResourceUnits += evidence.GetConsumedResourceUnits()
		workerReusable = workerReusable && evidence.GetWorkerReusable()
		memberFailedAt := evidence.GetFailedAt().AsTime().UTC()
		memberRetryAt := evidence.GetRetryAt().AsTime().UTC()
		if failedAt.IsZero() || memberFailedAt.Before(failedAt) {
			failedAt = memberFailedAt
		}
		if retryAt.IsZero() || memberRetryAt.After(retryAt) {
			retryAt = memberRetryAt
		}
		canonical = append(canonical, fingerprintMember{
			MemberID: memberID, Class: evidence.GetFailureClass(),
			Fingerprint: hex.EncodeToString(evidence.GetFailureFingerprint()),
		})
		detailParts = append(detailParts, fmt.Sprintf(
			"%s[%s]: %s", memberID, evidence.GetFailureClass(), evidence.GetDetail(),
		))
	}
	fingerprint := append([]byte(nil), status.Failures[memberIDs[0]].GetFailureFingerprint()...)
	if len(memberIDs) > 1 {
		encoded, err := json.Marshal(canonical)
		if err != nil {
			return nil, fmt.Errorf("encode aggregate ModelRuntime failure fingerprint: %w", err)
		}
		digest := sha256.Sum256(append([]byte("vela-stage-failure-v1\x00"), encoded...))
		fingerprint = digest[:]
	}
	return &velav1.FailStageRequest{
		FailureClass: failureClass, FailureFingerprint: fingerprint,
		Detail:         boundedUTF8Bytes(strings.Join(detailParts, "; "), 1000),
		WorkerReusable: workerReusable, ConsumedResourceUnits: consumedResourceUnits,
		FailedAt: timestamppb.New(failedAt), RetryAt: timestamppb.New(retryAt),
	}, nil
}

func boundedUTF8Bytes(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	for limit > 0 && !utf8.ValidString(value[:limit]) {
		limit--
	}
	return value[:limit]
}

func (agent *StreamAgent) Reattach(
	ctx context.Context,
	authority *velav1.StageAuthority,
	localReceiptID string,
	localReceiptDigest []byte,
) (ReattachResult, error) {
	result := ReattachResult{}
	if agent == nil || agent.runtime == nil || agent.control == nil || ctx == nil || authority == nil ||
		(len(localReceiptDigest) != 0 && len(localReceiptDigest) != 32) {
		return result, errors.New("invalid Stage Worker reattach authority")
	}
	agent.assignmentMu.Lock()
	defer agent.assignmentMu.Unlock()
	agent.runtimeMu.Lock()
	initialActive := agent.activeAuthority()
	if err := agent.observeRuntimeAuthority(ctx, authority); err != nil {
		agent.runtimeMu.Unlock()
		return result, err
	}
	statusResult, err := agent.runtime.Status(ctx, authority)
	if err != nil {
		agent.runtimeMu.Unlock()
		return result, fmt.Errorf("resident runtime rejected reattach authority: %w", err)
	}
	var expectedReceipt *LocalReceipt
	if localReceiptID != "" || len(localReceiptDigest) != 0 {
		expectedReceipt = &LocalReceipt{
			ID: localReceiptID, Digest: append([]byte(nil), localReceiptDigest...),
		}
	}
	if err := matchLocalReceipts(statusResult, expectedReceipt); err != nil {
		agent.runtimeMu.Unlock()
		return result, err
	}
	result.Status = statusResult
	agent.startingAuthority = proto.Clone(authority).(*velav1.StageAuthority)
	stopGeneration := agent.stopGeneration
	agent.runtimeMu.Unlock()
	response, err := agent.control.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
		Operation: &velav1.StageWorkerControlServiceConnectRequest_ReattachStage{
			ReattachStage: &velav1.ReattachStageRequest{
				Authority: authority, LocalReceiptId: localReceiptID,
				LocalReceiptDigest:   append([]byte(nil), localReceiptDigest...),
				ObservedRuntimeState: aggregateRuntimeState(statusResult),
			},
		},
	})
	agent.runtimeMu.Lock()
	defer func() {
		agent.startingAuthority = nil
		agent.runtimeMu.Unlock()
	}()
	if err != nil {
		return result, err
	}
	if agent.stopGeneration != stopGeneration {
		return result, errors.New("control stopped StageAttempt while reattach response was pending")
	}
	if !proto.Equal(agent.activeAuthority(), initialActive) {
		return result, errors.New("StageAuthority changed while reattach response was pending")
	}
	if stop := response.GetStopStage(); stop != nil {
		_, cancelErr := agent.handleStop(ctx, stop)
		return result, errors.Join(errors.New("control rejected StageAttempt reattach"), cancelErr)
	}
	command := response.GetStageCommandResult()
	if command == nil || command.GetOperation() !=
		velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE ||
		(command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED &&
			command.GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED) {
		return result, errors.New("control rejected StageAttempt reattach authority")
	}
	result.Accepted = true
	agent.setActive(authority)
	return result, nil
}

func (agent *StreamAgent) HandleStop(
	ctx context.Context,
	stop *velav1.StopStage,
) (CancellationResult, error) {
	if agent == nil || ctx == nil {
		return CancellationResult{}, errors.New("missing Stage Worker stream Agent")
	}
	if canceled, err := agent.cancelPendingInputs(ctx, stop); canceled || err != nil {
		return CancellationResult{}, err
	}
	agent.runtimeMu.Lock()
	defer agent.runtimeMu.Unlock()
	return agent.handleStop(ctx, stop)
}

func (agent *StreamAgent) cancelPendingInputs(ctx context.Context, stop *velav1.StopStage) (bool, error) {
	if stop.GetReason() == velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_UNSPECIFIED {
		return false, errors.New("invalid Stage Worker StopStage command")
	}
	digest, err := stageauthority.Digest(stop.GetAuthority())
	if err != nil {
		return false, errors.New("invalid Stage Worker StopStage authority")
	}
	agent.inputMu.Lock()
	defer agent.inputMu.Unlock()
	if agent.pendingInputs == nil || agent.pendingInputs.authorityDigest != digest {
		return false, nil
	}
	agent.pendingInputs.cancel()
	// Cancellation signals the resolver; its return establishes local drain.
	return true, agent.closeAdmission(context.WithoutCancel(ctx), stop.GetAuthority())
}

func (agent *StreamAgent) handleStop(
	ctx context.Context,
	stop *velav1.StopStage,
) (CancellationResult, error) {
	if stop == nil || stop.GetAuthority() == nil ||
		stop.GetReason() == velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_UNSPECIFIED {
		return CancellationResult{}, errors.New("invalid Stage Worker StopStage command")
	}
	active := agent.runtimeAuthority()
	if active != nil {
		activeDigest, activeErr := stageauthority.Digest(active)
		stopDigest, stopErr := stageauthority.Digest(stop.GetAuthority())
		if activeErr != nil || stopErr != nil || activeDigest != stopDigest {
			return CancellationResult{}, errors.New("StopStage authority does not match active StageAttempt")
		}
		// A matching Stop invalidates pending success responses even if Cancel
		// fails or its acknowledgment is lost.
		agent.stopGeneration++
	}
	closeErr := agent.closeAdmission(context.WithoutCancel(ctx), stop.GetAuthority())
	result, cancelErr := agent.runtime.Cancel(
		ctx,
		stop.GetAuthority(),
		velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	)
	return result, errors.Join(closeErr, cancelErr)
}

// The caller holds runtimeMu. Runtime may accept authority before control ACKs it.
func (agent *StreamAgent) runtimeAuthority() *velav1.StageAuthority {
	if agent.startingAuthority != nil {
		return agent.startingAuthority
	}
	return agent.activeAuthority()
}

func (agent *StreamAgent) cancelAfterControlStartFailure(
	ctx context.Context,
	authority *velav1.StageAuthority,
	cause error,
) (int, error) {
	closeErr := agent.closeAdmission(context.WithoutCancel(ctx), authority)
	canceled, err := agent.runtime.Cancel(
		context.WithoutCancel(ctx),
		authority,
		velav1.ModelRuntimeCancelReason_MODEL_RUNTIME_CANCEL_REASON_CONTROL_PLANE_STOP,
	)
	return canceled.AcknowledgedMembers, errors.Join(cause, closeErr, err)
}

func (agent *StreamAgent) observeRuntimeAuthority(ctx context.Context, authority *velav1.StageAuthority) error {
	if agent.admission == nil {
		return nil
	}
	return agent.admission.ObserveRuntimeAuthority(ctx, authority)
}

func (agent *StreamAgent) closeAdmission(ctx context.Context, authority *velav1.StageAuthority) error {
	if agent.admission == nil {
		return nil
	}
	return agent.admission.CloseExecution(ctx, authority)
}

func (agent *StreamAgent) inspectActiveRuntime(ctx context.Context) (*velav1.StageAuthority, AggregateStatus, error) {
	agent.runtimeMu.Lock()
	defer agent.runtimeMu.Unlock()
	authority := agent.activeAuthority()
	if authority == nil {
		return nil, AggregateStatus{}, errors.New("stage worker has no active authority to inspect")
	}
	if err := agent.observeRuntimeAuthority(ctx, authority); err != nil {
		return nil, AggregateStatus{}, err
	}
	status, err := agent.runtime.Status(ctx, authority)
	return authority, status, err
}

func (agent *StreamAgent) setActive(authority *velav1.StageAuthority) {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	agent.active = proto.Clone(authority).(*velav1.StageAuthority)
}

func (agent *StreamAgent) activeAuthority() *velav1.StageAuthority {
	agent.mu.Lock()
	defer agent.mu.Unlock()
	if agent.active == nil {
		return nil
	}
	return proto.Clone(agent.active).(*velav1.StageAuthority)
}

func (agent *StreamAgent) clearActive(authority *velav1.StageAuthority) {
	if authority == nil {
		return
	}
	identity, err := assignmentExecutionIdentity(authority)
	if err != nil {
		return
	}
	agent.mu.Lock()
	defer agent.mu.Unlock()
	if agent.active == nil {
		return
	}
	// A terminal response may arrive after the same execution was renewed.
	activeIdentity, err := assignmentExecutionIdentity(agent.active)
	if err == nil && activeIdentity == identity {
		agent.active = nil
	}
}

func aggregateRuntimeState(status AggregateStatus) velav1.ModelRuntimeExecutionState {
	if status.AllStopped {
		return velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_STOPPED
	}
	state := velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED
	for _, memberState := range status.States {
		switch memberState {
		case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_FAILED:
			return memberState
		case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING:
			state = memberState
		case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING:
			if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED {
				state = memberState
			}
		case velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_READY,
			velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED:
			if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED {
				state = memberState
			}
		default:
			if state == velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED {
				state = memberState
			}
		}
	}
	return state
}

func matchLocalReceipts(
	status AggregateStatus,
	expected *LocalReceipt,
) error {
	if expected == nil {
		if len(status.LocalReceipts) != 0 {
			return errors.New("resident runtime local receipt does not match reattach request")
		}
		return nil
	}
	if expected.ID == "" || len(expected.Digest) != 32 || len(status.LocalReceipts) == 0 {
		return errors.New("resident runtime local receipt does not match reattach request")
	}
	for _, receipt := range status.LocalReceipts {
		if receipt.ID != expected.ID || !bytes.Equal(receipt.Digest, expected.Digest) {
			return errors.New("resident runtime local receipt does not match reattach request")
		}
	}
	return nil
}
