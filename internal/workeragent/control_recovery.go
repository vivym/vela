package workeragent

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runnertransport"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workerrecovery"
)

const (
	pendingControlOperationName = "control-operation.json"
	executionHeartbeatStateName = "execution-heartbeat.json"
)

type executionHeartbeatState struct {
	SchemaVersion int                   `json:"schema_version"`
	Authority     localAttemptAuthority `json:"authority"`
	Sequence      int64                 `json:"sequence"`
}

type localAttemptAuthority struct {
	AttemptID   uuid.UUID `json:"attempt_id"`
	JobID       uuid.UUID `json:"job_id"`
	WorkerID    uuid.UUID `json:"worker_id"`
	WorkerEpoch int64     `json:"worker_epoch"`
	LeaseFence  int64     `json:"lease_fence"`
}

func localAuthority(assignment workercontrol.Assignment) localAttemptAuthority {
	return localAttemptAuthority{
		AttemptID: assignment.AttemptID, JobID: assignment.JobID,
		WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		LeaseFence: assignment.LeaseFence,
	}
}

func (authority localAttemptAuthority) valid() bool {
	return authority.AttemptID != uuid.Nil && authority.JobID != uuid.Nil &&
		authority.WorkerID != uuid.Nil && authority.WorkerEpoch > 0 && authority.LeaseFence > 0
}

func (authority localAttemptAuthority) matchesAssignment(
	assignment workercontrol.Assignment,
) bool {
	return authority == localAuthority(assignment)
}

func (authority localAttemptAuthority) matchesRecoveryIdentity(
	identity workerrecovery.Identity,
) bool {
	return authority.AttemptID == identity.AttemptID && authority.WorkerID == identity.WorkerID &&
		authority.WorkerEpoch == identity.WorkerEpoch && authority.LeaseFence == identity.Fence
}

type pendingControlOperation struct {
	SchemaVersion   int                                `json:"schema_version"`
	Kind            string                             `json:"kind"`
	Assignment      workercontrol.Assignment           `json:"assignment"`
	Failure         workercontrol.FailureObservation   `json:"failure"`
	Heartbeat       workercontrol.HeartbeatObservation `json:"heartbeat"`
	ExpectedPhase   workercontrol.ExecutionPhase       `json:"expected_phase"`
	CancelReason    runnertransport.CancelReason       `json:"cancel_reason,omitempty"`
	TerminalOutcome Outcome                            `json:"terminal_outcome,omitempty"`
	StopReason      workercontrol.StopReason           `json:"stop_reason,omitempty"`
	Outputs         []runnertransport.Output           `json:"outputs,omitempty"`
}

func (agent *Agent) resumePendingControlOperation(
	ctx context.Context,
	watermark workerrecovery.WatermarkState,
) (Result, bool, error) {
	handles, err := agent.recovery.ActiveHandles(ctx)
	if err != nil {
		return Result{}, false, err
	}
	var pendingHandle *workerrecovery.Handle
	var pending pendingControlOperation
	for _, handle := range handles {
		operation, readErr := readPendingControlOperation(ctx, handle)
		if errors.Is(readErr, workerrecovery.ErrStateNotFound) {
			continue
		}
		if readErr != nil {
			return Result{}, false, readErr
		}
		if err := agent.validatePendingControlOperation(ctx, handle, operation); err != nil {
			return Result{}, false, err
		}
		if pendingHandle != nil {
			return Result{}, false, errors.New("multiple pending control operations require operator reconciliation")
		}
		pendingHandle = handle
		pending = operation
	}
	if pendingHandle == nil {
		return Result{}, false, nil
	}
	identity := runnerIdentity(pending.Assignment)
	credentials := leaseCredentials(pending.Assignment)
	switch pending.Kind {
	case "START":
		return agent.replayPendingStart(
			ctx, pendingHandle, identity, pending.Assignment, credentials, watermark,
		)
	case "FAIL":
		result, err := agent.executePendingFail(
			ctx, pendingHandle, identity, pending.Assignment, credentials, watermark,
			pending.Failure, pending.Outputs, 0,
		)
		return result, true, err
	case "HEARTBEAT":
		return agent.replayPendingHeartbeat(
			ctx, pendingHandle, identity, pending.Assignment, credentials, watermark, pending,
		)
	case "TERMINATE":
		result, err := agent.executePendingTermination(
			ctx, pendingHandle, identity, pending.Assignment, watermark,
			pending.CancelReason, pending.TerminalOutcome, pending.StopReason, pending.Outputs,
		)
		return result, true, err
	default:
		return Result{}, false, errors.New("pending control operation kind is invalid")
	}
}

func (agent *Agent) persistPendingStart(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
) error {
	operation := pendingControlOperation{
		SchemaVersion: 1,
		Kind:          "START",
		Assignment:    assignment,
	}
	encoded, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("encode pending Start operation: %w", err)
	}
	_, err = handle.Write(
		ctx, workerrecovery.StageUpload, pendingControlOperationName, bytes.NewReader(encoded),
	)
	if err != nil {
		return fmt.Errorf("persist pending Start operation: %w", err)
	}
	return nil
}

func (agent *Agent) persistPendingFail(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
	observation workercontrol.FailureObservation,
	outputs []runnertransport.Output,
) error {
	operation := pendingControlOperation{
		SchemaVersion: 1,
		Kind:          "FAIL",
		Assignment:    assignment,
		Failure:       observation,
		Outputs:       append([]runnertransport.Output(nil), outputs...),
	}
	encoded, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("encode pending Fail operation: %w", err)
	}
	_, err = handle.Write(
		ctx, workerrecovery.StageUpload, pendingControlOperationName, bytes.NewReader(encoded),
	)
	if err != nil {
		return fmt.Errorf("persist pending Fail operation: %w", err)
	}
	return nil
}

func (agent *Agent) persistPendingHeartbeat(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
	observation workercontrol.HeartbeatObservation,
	expectedPhase workercontrol.ExecutionPhase,
	outputs []runnertransport.Output,
) error {
	operation := pendingControlOperation{
		SchemaVersion: 1,
		Kind:          "HEARTBEAT",
		Assignment:    assignment,
		Heartbeat:     observation,
		ExpectedPhase: expectedPhase,
		Outputs:       append([]runnertransport.Output(nil), outputs...),
	}
	encoded, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("encode pending Heartbeat operation: %w", err)
	}
	_, err = handle.Write(
		ctx, workerrecovery.StageUpload, pendingControlOperationName, bytes.NewReader(encoded),
	)
	if err != nil {
		return fmt.Errorf("persist pending Heartbeat operation: %w", err)
	}
	return nil
}

func loadExecutionHeartbeatSequence(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
) (int64, error) {
	state, err := readExecutionHeartbeatState(ctx, handle)
	if errors.Is(err, workerrecovery.ErrStateNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if !state.Authority.matchesAssignment(assignment) {
		return 0, errors.New("execution Heartbeat State does not match active Assignment authority")
	}
	return state.Sequence, nil
}

func (agent *Agent) validateExecutionHeartbeatStates(
	ctx context.Context,
	allowExhausted bool,
) error {
	handles, err := agent.recovery.ActiveHandles(ctx)
	if err != nil {
		return err
	}
	for _, handle := range handles {
		state, err := readExecutionHeartbeatState(ctx, handle)
		if errors.Is(err, workerrecovery.ErrStateNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		if !allowExhausted && state.Sequence == math.MaxInt64 {
			return errors.New("execution Heartbeat sequence is exhausted")
		}
	}
	return nil
}

func readExecutionHeartbeatState(
	ctx context.Context,
	handle *workerrecovery.Handle,
) (executionHeartbeatState, error) {
	content, err := handle.Read(ctx, workerrecovery.StageUpload, executionHeartbeatStateName)
	if err != nil {
		return executionHeartbeatState{}, err
	}
	var state executionHeartbeatState
	if err := strictDecodeJSON(content, &state); err != nil {
		return executionHeartbeatState{}, fmt.Errorf("decode execution Heartbeat State: %w", err)
	}
	identity, err := handle.Identity()
	if err != nil {
		return executionHeartbeatState{}, err
	}
	if state.SchemaVersion != 1 || state.Sequence <= 0 || !state.Authority.valid() ||
		!state.Authority.matchesRecoveryIdentity(identity) {
		return executionHeartbeatState{}, errors.New(
			"execution Heartbeat State does not match active Worker authority",
		)
	}
	return state, nil
}

func persistExecutionHeartbeatSequence(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
	sequence int64,
) error {
	if sequence <= 0 {
		return errors.New("execution Heartbeat sequence must be positive")
	}
	identity, err := handle.Identity()
	if err != nil {
		return err
	}
	authority := localAuthority(assignment)
	if !authority.matchesRecoveryIdentity(identity) {
		return errors.New("execution Heartbeat State does not match active Assignment authority")
	}
	state := executionHeartbeatState{
		SchemaVersion: 1,
		Authority:     authority,
		Sequence:      sequence,
	}
	encoded, err := json.Marshal(state)
	if err != nil {
		return fmt.Errorf("encode execution Heartbeat State: %w", err)
	}
	if _, err := handle.Write(
		ctx, workerrecovery.StageUpload, executionHeartbeatStateName, bytes.NewReader(encoded),
	); err != nil {
		return fmt.Errorf("persist execution Heartbeat State: %w", err)
	}
	return nil
}

func (agent *Agent) persistPendingTermination(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
	cancelReason runnertransport.CancelReason,
	outcome Outcome,
	stopReason workercontrol.StopReason,
	outputs []runnertransport.Output,
) error {
	if err := agent.validatePendingOutputs(assignment.AttemptID, outputs); err != nil {
		return err
	}
	operation := pendingControlOperation{
		SchemaVersion:   1,
		Kind:            "TERMINATE",
		Assignment:      assignment,
		CancelReason:    cancelReason,
		TerminalOutcome: outcome,
		StopReason:      stopReason,
		Outputs:         append([]runnertransport.Output(nil), outputs...),
	}
	encoded, err := json.Marshal(operation)
	if err != nil {
		return fmt.Errorf("encode pending termination operation: %w", err)
	}
	cleanupContext, cancelCleanup := boundedCleanupContext(ctx)
	defer cancelCleanup()
	if _, err := handle.Write(
		cleanupContext, workerrecovery.StageUpload, pendingControlOperationName, bytes.NewReader(encoded),
	); err != nil {
		return fmt.Errorf("persist pending termination operation: %w", err)
	}
	return nil
}

func clearPendingControlOperation(ctx context.Context, handle *workerrecovery.Handle) error {
	cleanupContext, cancelCleanup := boundedCleanupContext(ctx)
	defer cancelCleanup()
	if err := handle.Delete(
		cleanupContext, workerrecovery.StageUpload, pendingControlOperationName,
	); err != nil {
		return fmt.Errorf("clear pending control operation: %w", err)
	}
	return nil
}

func readPendingControlOperation(
	ctx context.Context,
	handle *workerrecovery.Handle,
) (pendingControlOperation, error) {
	content, err := handle.Read(ctx, workerrecovery.StageUpload, pendingControlOperationName)
	if err != nil {
		return pendingControlOperation{}, err
	}
	var operation pendingControlOperation
	if err := strictDecodeJSON(content, &operation); err != nil {
		return pendingControlOperation{}, fmt.Errorf("decode pending control operation: %w", err)
	}
	return operation, nil
}

func (agent *Agent) validatePendingControlOperation(
	ctx context.Context,
	handle *workerrecovery.Handle,
	operation pendingControlOperation,
) error {
	identity, err := handle.Identity()
	if err != nil {
		return err
	}
	assignment := operation.Assignment
	authority := localAuthority(assignment)
	if operation.SchemaVersion != 1 || !authority.valid() ||
		!authority.matchesRecoveryIdentity(identity) || assignment.WorkerID != agent.workerID ||
		assignment.WorkerEpoch != agent.workerEpoch {
		return errors.New("pending control operation does not match active Worker authority")
	}
	switch operation.Kind {
	case "START":
		if err := agent.validateAssignment(assignment); err != nil {
			return err
		}
	case "FAIL":
		if err := agent.validateAssignment(assignment); err != nil {
			return err
		}
		if operation.Failure.FailureClass == "" {
			return errors.New("pending Fail operation is incomplete")
		}
	case "HEARTBEAT":
		if err := agent.validateAssignment(assignment); err != nil {
			return err
		}
		if operation.Heartbeat.Sequence <= 0 ||
			(operation.ExpectedPhase != workercontrol.ExecutionPhasePreparing &&
				operation.ExpectedPhase != workercontrol.ExecutionPhaseGenerating &&
				operation.ExpectedPhase != workercontrol.ExecutionPhaseFinalizing) {
			return errors.New("pending Heartbeat operation is incomplete")
		}
		if operation.ExpectedPhase != workercontrol.ExecutionPhaseFinalizing {
			sequence, err := loadExecutionHeartbeatSequence(ctx, handle, assignment)
			if err != nil {
				return fmt.Errorf("pending Heartbeat does not match execution Heartbeat State: %w", err)
			}
			if sequence != 0 && sequence != operation.Heartbeat.Sequence {
				return errors.New("pending Heartbeat does not match execution Heartbeat State sequence")
			}
		}
	case "TERMINATE":
		if assignment.JobID == uuid.Nil || assignment.LeaseToken == "" {
			return errors.New("pending termination Assignment authority is incomplete")
		}
		switch operation.TerminalOutcome {
		case OutcomeLeaseDeadlineElapsed:
			if operation.CancelReason != runnertransport.CancelLeaseDeadline || operation.StopReason != "" {
				return errors.New("pending Lease-deadline termination operation is incomplete")
			}
		case OutcomeControlPlaneStopped:
			if operation.CancelReason != runnertransport.CancelControlPlaneStop || operation.StopReason == "" {
				return errors.New("pending control-plane termination operation is incomplete")
			}
		default:
			return errors.New("pending termination outcome is invalid")
		}
		if err := agent.validatePendingOutputs(assignment.AttemptID, operation.Outputs); err != nil {
			return err
		}
	default:
		return errors.New("pending control operation kind is invalid")
	}
	return nil
}

func (agent *Agent) executePendingTermination(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	watermark workerrecovery.WatermarkState,
	cancelReason runnertransport.CancelReason,
	outcome Outcome,
	stopReason workercontrol.StopReason,
	outputs []runnertransport.Output,
) (Result, error) {
	_, err := agent.cancelCleanupAndMarkTerminal(
		ctx, handle, identity, assignment, cancelReason, outputs,
		func(cleanupContext context.Context, current []runnertransport.Output) ([]runnertransport.Output, error) {
			return agent.captureSuccessfulTerminationOutputs(
				cleanupContext, handle, identity, assignment,
				cancelReason, outcome, stopReason, current,
			)
		},
	)
	if err != nil {
		return Result{}, err
	}
	return Result{
		Outcome: outcome, Watermark: watermark,
		AttemptID: assignment.AttemptID, JobID: assignment.JobID, StopReason: stopReason,
	}, nil
}

func (agent *Agent) cancelCleanupAndMarkTerminal(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	cancelReason runnertransport.CancelReason,
	outputs []runnertransport.Output,
	capture func(context.Context, []runnertransport.Output) ([]runnertransport.Output, error),
) ([]runnertransport.Output, error) {
	controlContext, cancelControl := boundedCleanupContext(ctx)
	defer cancelControl()
	canceled, err := agent.runner.Cancel(controlContext, identity, cancelReason)
	if err != nil {
		return nil, err
	}
	if canceled.Decision != runnertransport.CommandAccepted {
		switch cancelReason {
		case runnertransport.CancelLeaseDeadline:
			return nil, errors.New("runner rejected Lease-deadline cancellation")
		case runnertransport.CancelControlPlaneStop:
			return nil, errors.New("runner rejected control-plane cancellation")
		default:
			cancelControl()
			return nil, errors.New("runner rejected terminal cancellation")
		}
	}
	if capture != nil {
		outputs, err = capture(controlContext, outputs)
		if err != nil {
			return nil, err
		}
	}
	cancelControl()
	cleanupContext, cancelCleanup, err := agent.outputCleanupContext(ctx, outputs)
	if err != nil {
		return nil, err
	}
	defer cancelCleanup()
	if len(outputs) != 0 {
		if err := agent.cleanupCommittedOutputs(
			cleanupContext, assignment.AttemptID, outputs,
		); err != nil {
			return nil, err
		}
	}
	if err := handle.MarkTerminal(cleanupContext); err != nil {
		return nil, err
	}
	return outputs, nil
}

func (agent *Agent) captureSuccessfulTerminationOutputs(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	cancelReason runnertransport.CancelReason,
	outcome Outcome,
	stopReason workercontrol.StopReason,
	outputs []runnertransport.Output,
) ([]runnertransport.Output, error) {
	if len(outputs) != 0 {
		return outputs, nil
	}
	status, err := agent.runner.Status(ctx, identity)
	if err != nil {
		return nil, err
	}
	switch status.State {
	case runnertransport.ExecutionCanceled, runnertransport.ExecutionFailed:
		return nil, nil
	case runnertransport.ExecutionSucceeded:
		collected, err := agent.runner.CollectOutputs(ctx, identity)
		if err != nil {
			return nil, err
		}
		if collected.Decision != runnertransport.CommandAccepted || len(collected.Outputs) == 0 {
			return nil, errors.New("canceled successful runner did not provide accepted outputs")
		}
		for _, output := range collected.Outputs {
			if err := validateOutputPath(agent.outputRoot, output.Path); err != nil {
				return nil, err
			}
		}
		if err := agent.validatePendingOutputs(assignment.AttemptID, collected.Outputs); err != nil {
			return nil, err
		}
		if err := agent.persistPendingTermination(
			ctx, handle, assignment, cancelReason, outcome, stopReason, collected.Outputs,
		); err != nil {
			return nil, err
		}
		return append([]runnertransport.Output(nil), collected.Outputs...), nil
	default:
		return nil, errors.New("accepted terminal cancellation left runner non-terminal")
	}
}

func (agent *Agent) validatePendingOutputs(
	attemptID uuid.UUID,
	outputs []runnertransport.Output,
) error {
	if len(outputs) > 32 {
		return errors.New("pending termination operation contains too many outputs")
	}
	for _, output := range outputs {
		if output.SizeBytes <= 0 || output.SHA256 == [32]byte{} ||
			!validBoundedPrintable(output.Kind, 100) ||
			!validBoundedPrintable(output.ContentType, 200) ||
			agent.validatePendingOutputPath(attemptID, output.Path) != nil {
			return errors.New("pending termination operation contains an invalid output receipt")
		}
	}
	return nil
}

func (agent *Agent) validatePendingOutputPath(attemptID uuid.UUID, path string) error {
	cleaned := filepath.Clean(path)
	if attemptID == uuid.Nil || !filepath.IsAbs(cleaned) || cleaned != path ||
		filepath.Base(cleaned) == "." ||
		filepath.Base(filepath.Dir(cleaned)) != attemptID.String() {
		return errors.New("pending termination output is outside the exact Attempt root")
	}
	return nil
}

func (agent *Agent) replayPendingStart(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	watermark workerrecovery.WatermarkState,
) (Result, bool, error) {
	operationContext, cancelOperation := context.WithTimeout(ctx, agent.heartbeatInterval)
	started, err := agent.control.Start(operationContext, credentials)
	cancelOperation()
	if err != nil {
		return Result{}, false, agent.handleOperationError(ctx, identity, err)
	}
	if started.Decision == workercontrol.Stop {
		result, terminalErr := agent.terminateForControlStop(
			ctx, handle, identity, assignment, watermark, started.StopReason,
		)
		return result, true, terminalErr
	}
	if err := validateStartGrant(started, assignment); err != nil {
		return Result{}, false, err
	}
	if err := clearPendingControlOperation(ctx, handle); err != nil {
		return Result{}, false, err
	}
	return Result{}, false, nil
}

func validateStartGrant(
	started workercontrol.StartResult,
	assignment workercontrol.Assignment,
) error {
	if started.Decision != workercontrol.StartGranted || started.AttemptID != assignment.AttemptID ||
		started.JobID != assignment.JobID || started.WorkerID != assignment.WorkerID ||
		started.WorkerEpoch != assignment.WorkerEpoch || started.LeaseFence != assignment.LeaseFence {
		return errors.New("control-plane Start did not grant the exact Assignment authority")
	}
	return nil
}

func (agent *Agent) replayPendingHeartbeat(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	watermark workerrecovery.WatermarkState,
	operation pendingControlOperation,
) (Result, bool, error) {
	heartbeatStarted := agent.monotonicNow()
	if heartbeatStarted < 0 {
		return Result{}, false, errors.New("worker agent monotonic clock returned a negative value")
	}
	operationContext, cancelOperation := context.WithTimeout(ctx, agent.heartbeatInterval)
	result, err := agent.control.Heartbeat(
		operationContext, credentials, operation.Heartbeat,
	)
	cancelOperation()
	if err != nil {
		return Result{}, false, agent.handleOperationError(ctx, identity, err)
	}
	if result.Decision == workercontrol.HeartbeatStop {
		if len(operation.Outputs) != 0 {
			terminal, terminalErr := agent.terminateFinalizationForControlStop(
				ctx, handle, identity, assignment, watermark, result.StopReason, operation.Outputs,
			)
			return terminal, true, terminalErr
		}
		terminal, terminalErr := agent.terminateForControlStop(
			ctx, handle, identity, assignment, watermark, result.StopReason,
		)
		return terminal, true, terminalErr
	}
	if result.Decision != workercontrol.HeartbeatContinue ||
		result.AttemptID != assignment.AttemptID || result.JobID != assignment.JobID ||
		result.WorkerID != assignment.WorkerID || result.WorkerEpoch != assignment.WorkerEpoch ||
		result.LeaseFence != assignment.LeaseFence ||
		result.HeartbeatSequence != operation.Heartbeat.Sequence ||
		result.ExecutionPhase != operation.ExpectedPhase || result.LeaseValidFor <= 0 {
		return Result{}, false, errors.New("replayed Heartbeat did not continue exact Assignment authority")
	}
	if operation.ExpectedPhase != workercontrol.ExecutionPhaseFinalizing {
		if err := persistExecutionHeartbeatSequence(
			ctx, handle, assignment, operation.Heartbeat.Sequence,
		); err != nil {
			return Result{}, false, err
		}
	}
	if err := clearPendingControlOperation(ctx, handle); err != nil {
		return Result{}, false, err
	}
	if operation.ExpectedPhase == workercontrol.ExecutionPhaseFinalizing {
		return Result{}, false, nil
	}
	deadline, err := monotonicDeadline(heartbeatStarted, result.LeaseValidFor)
	if err != nil {
		return Result{}, false, err
	}
	operationContext, cancelOperation, err = agent.leaseOperationContext(ctx, deadline)
	if err != nil {
		terminal, terminalErr := agent.terminateForLeaseDeadline(
			ctx, handle, identity, assignment, watermark,
		)
		return terminal, true, terminalErr
	}
	err = agent.wait(operationContext, agent.heartbeatInterval)
	cancelOperation()
	if err != nil {
		if agent.requireLeaseTime(deadline) != nil {
			terminal, terminalErr := agent.terminateForLeaseDeadline(
				ctx, handle, identity, assignment, watermark,
			)
			return terminal, true, terminalErr
		}
		return Result{}, true, agent.handleOperationError(ctx, identity, err)
	}
	continued, err := agent.runExecutionLoop(
		ctx, handle, identity, assignment, credentials, watermark, deadline,
		operation.Heartbeat.Sequence,
	)
	return continued, true, err
}

func (agent *Agent) executePendingFail(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	watermark workerrecovery.WatermarkState,
	observation workercontrol.FailureObservation,
	outputs []runnertransport.Output,
	deadline time.Duration,
) (Result, error) {
	operationContext, cancelOperation, err := agent.failOperationContext(ctx, deadline)
	if err != nil {
		if deadline > 0 {
			return agent.terminateFailedOperationForLeaseDeadline(
				ctx, handle, identity, assignment, watermark, outputs,
			)
		}
		return Result{}, err
	}
	decision, err := agent.control.Fail(operationContext, credentials, observation)
	operationErr := operationContext.Err()
	cancelOperation()
	if err != nil {
		if deadline > 0 && ctx.Err() == nil &&
			(errors.Is(operationErr, context.DeadlineExceeded) || agent.requireLeaseTime(deadline) != nil) {
			return agent.terminateFailedOperationForLeaseDeadline(
				ctx, handle, identity, assignment, watermark, outputs,
			)
		}
		return Result{}, agent.handleOperationError(ctx, identity, err)
	}
	if decision.Disposition == workercontrol.RetryDispositionRejectedStaleLease {
		if len(outputs) != 0 {
			return agent.terminateFinalizationForControlStop(
				ctx, handle, identity, assignment, watermark,
				workercontrol.StopInvalidAuthority, outputs,
			)
		}
		return agent.terminateForControlStop(
			ctx, handle, identity, assignment, watermark, workercontrol.StopInvalidAuthority,
		)
	}
	if decision.AttemptID != assignment.AttemptID || decision.JobID != assignment.JobID ||
		decision.FailureClass != observation.FailureClass ||
		decision.AttemptState != workercontrol.FailedAttempt || decision.JobFence <= 0 ||
		decision.JobVersion <= 0 || decision.DecidedAt.IsZero() {
		return Result{}, errors.New("control-plane Fail decision does not match failure authority")
	}
	outcome := OutcomeFailed
	switch decision.Disposition {
	case workercontrol.RetryDispositionRetryWait:
		outcome = OutcomeRetryScheduled
	case workercontrol.RetryDispositionFailed:
	default:
		return Result{}, errors.New("control-plane Fail returned an invalid disposition")
	}
	result := Result{
		Outcome: outcome, Watermark: watermark,
		AttemptID: assignment.AttemptID, JobID: assignment.JobID,
	}
	if len(outputs) != 0 {
		if err := agent.cleanupTerminalFinalization(
			ctx, handle, identity, assignment, outputs, runnertransport.CancelControlPlaneStop,
		); err != nil {
			return result, err
		}
		return result, nil
	}
	cleanupContext, cancelCleanup := boundedCleanupContext(ctx)
	defer cancelCleanup()
	if err := handle.MarkTerminal(cleanupContext); err != nil {
		return Result{}, err
	}
	return result, nil
}

func (agent *Agent) failOperationContext(
	ctx context.Context,
	deadline time.Duration,
) (context.Context, context.CancelFunc, error) {
	if deadline > 0 {
		return agent.leaseOperationContext(ctx, deadline)
	}
	if ctx == nil {
		return nil, nil, errors.New("worker Fail operation context is required")
	}
	operationContext, cancelOperation := context.WithTimeout(ctx, agent.heartbeatInterval)
	return operationContext, cancelOperation, nil
}

func (agent *Agent) terminateFailedOperationForLeaseDeadline(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	watermark workerrecovery.WatermarkState,
	outputs []runnertransport.Output,
) (Result, error) {
	if len(outputs) != 0 {
		return agent.terminateFinalizationForLeaseDeadline(
			ctx, handle, identity, assignment, watermark, outputs,
		)
	}
	return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
}

func runnerIdentity(assignment workercontrol.Assignment) runnertransport.AttemptIdentity {
	return runnertransport.AttemptIdentity{
		AttemptID: assignment.AttemptID, JobID: assignment.JobID,
		WorkerID: assignment.WorkerID, WorkerEpoch: assignment.WorkerEpoch,
		LeaseFence: assignment.LeaseFence,
	}
}

func leaseCredentials(assignment workercontrol.Assignment) workercontrol.LeaseCredentials {
	return workercontrol.LeaseCredentials{
		AttemptID: assignment.AttemptID, WorkerEpoch: assignment.WorkerEpoch,
		Fence: assignment.LeaseFence, Token: assignment.LeaseToken,
	}
}
