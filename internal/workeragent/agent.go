package workeragent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/runnertransport"
	"github.com/vivym/vela/internal/securefile"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workerrecovery"
	"github.com/vivym/vela/internal/workertransport"
)

const (
	defaultArtifactPartSize               int64 = 16 << 20
	defaultOutputCleanupMinBytesPerSecond int64 = 16 << 20
	maximumOutputCleanupMinBytesPerSecond int64 = 1 << 40
	defaultTerminalControlTimeout               = 10 * time.Second
	defaultOutputCleanupBaseTimeout             = 10 * time.Second
	defaultOutputCleanupMaxDuration             = 24 * time.Hour
)

type ControlPlane interface {
	Acquire(context.Context, int64) (workercontrol.Assignment, error)
	Start(context.Context, workercontrol.LeaseCredentials) (workercontrol.StartResult, error)
	Heartbeat(
		context.Context,
		workercontrol.LeaseCredentials,
		workercontrol.HeartbeatObservation,
	) (workercontrol.HeartbeatResult, error)
	Fail(
		context.Context,
		workercontrol.LeaseCredentials,
		workercontrol.FailureObservation,
	) (workercontrol.RetryDecision, error)
}

type Runner interface {
	Prepare(
		context.Context,
		runnertransport.AttemptIdentity,
		runnertransport.ExecutionSpec,
		bool,
	) (runnertransport.PrepareResult, error)
	Start(context.Context, runnertransport.AttemptIdentity) (runnertransport.CommandResult, error)
	Cancel(
		context.Context,
		runnertransport.AttemptIdentity,
		runnertransport.CancelReason,
	) (runnertransport.CommandResult, error)
	Status(context.Context, runnertransport.AttemptIdentity) (runnertransport.Status, error)
	CollectOutputs(
		context.Context,
		runnertransport.AttemptIdentity,
	) (runnertransport.CollectOutputsResult, error)
}

type FinalizationControl interface {
	BeginFinalization(context.Context, workercontrol.LeaseCredentials) (workercontrol.FinalizationPlan, error)
	ClaimArtifactUpload(
		context.Context,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		uuid.UUID,
		workertransport.ArtifactUploadPartIntent,
	) (workertransport.ArtifactUploadClaim, error)
	CompleteArtifactMultipartUpload(
		context.Context,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		uuid.UUID,
		workercontrol.ArtifactUploadReport,
	) (workercontrol.ArtifactUploadResult, error)
	VerifyArtifact(
		context.Context,
		workercontrol.LeaseCredentials,
		uuid.UUID,
		uuid.UUID,
	) (workercontrol.ArtifactVerificationResult, error)
	CompleteVisibleCompletion(
		context.Context,
		workercontrol.LeaseCredentials,
		workercontrol.VisibleCompletionCandidate,
	) (workercontrol.VisibleCompletionResult, error)
}

type ArtifactPartUploader interface {
	Upload(
		context.Context,
		workertransport.SignedArtifactUploadPart,
		[]byte,
	) (workercontrol.ArtifactUploadPart, error)
}

type Config struct {
	WorkerID                       uuid.UUID
	WorkerEpoch                    int64
	Recovery                       *workerrecovery.Manager
	Control                        ControlPlane
	Runner                         Runner
	HeartbeatInterval              time.Duration
	MonotonicNow                   func() time.Duration
	Wait                           func(context.Context, time.Duration) error
	ArtifactStoreReachable         func(context.Context) bool
	OutputRoot                     string
	OutputCleanupMinBytesPerSecond int64
	OutputCleanupMaxBytes          int64
	OutputCleanupMaxDuration       time.Duration
	InferenceBackendRevision       string
	OutputOwnerUID                 uint32
	Finalization                   FinalizationControl
	PartUploader                   ArtifactPartUploader
	ArtifactPartSize               int64
}

type Outcome string

const (
	OutcomeBackpressured        Outcome = "BACKPRESSURED"
	OutcomeIdle                 Outcome = "IDLE"
	OutcomeReadyForFinalization Outcome = "READY_FOR_FINALIZATION"
	OutcomeVisibleCompletion    Outcome = "VISIBLE_COMPLETION"
	OutcomeLeaseDeadlineElapsed Outcome = "LEASE_DEADLINE_ELAPSED"
	OutcomeRetryScheduled       Outcome = "RETRY_SCHEDULED"
	OutcomeFailed               Outcome = "FAILED"
	OutcomeControlPlaneStopped  Outcome = "CONTROL_PLANE_STOPPED"
)

type Result struct {
	Outcome           Outcome
	Watermark         workerrecovery.WatermarkState
	AttemptID         uuid.UUID
	JobID             uuid.UUID
	Outputs           []runnertransport.Output
	StopReason        workercontrol.StopReason
	VisibleCompletion workercontrol.VisibleCompletionResult
}

type Agent struct {
	workerID                       uuid.UUID
	workerEpoch                    int64
	recovery                       *workerrecovery.Manager
	control                        ControlPlane
	runner                         Runner
	heartbeatInterval              time.Duration
	monotonicNow                   func() time.Duration
	wait                           func(context.Context, time.Duration) error
	artifactStoreReachable         func(context.Context) bool
	outputRoot                     string
	outputQuarantineRoot           string
	beforeOutputQuarantine         func()
	beforeOutputHash               func(context.Context)
	afterOutputHashChunk           func(context.Context, int64)
	cleanupTimeout                 time.Duration
	outputCleanupMinBytesPerSecond int64
	outputCleanupMaxBytes          int64
	outputCleanupMaxDuration       time.Duration
	inferenceBackendRevision       string
	outputOwnerUID                 uint32
	finalization                   FinalizationControl
	partUploader                   ArtifactPartUploader
	artifactPartSize               int64
}

func New(config Config) (*Agent, error) {
	if config.WorkerID == uuid.Nil || config.WorkerEpoch <= 0 || config.Recovery == nil ||
		config.Control == nil || config.Runner == nil || config.HeartbeatInterval <= 0 ||
		config.OutputRoot == "" || !validBoundedPrintable(config.InferenceBackendRevision, 200) ||
		config.Finalization == nil || config.PartUploader == nil {
		return nil, errors.New("worker agent configuration is incomplete")
	}
	outputRoot, err := securefile.ResolveTrustedDirectory(config.OutputRoot)
	if err != nil || outputRoot == string(filepath.Separator) {
		return nil, fmt.Errorf("validate approved output root: %w", err)
	}
	outputQuarantineRoot, err := config.Recovery.QuarantineRoot()
	if err != nil {
		return nil, err
	}
	outputQuarantineRoot, err = securefile.ResolveTrustedDirectory(outputQuarantineRoot)
	if err != nil {
		return nil, fmt.Errorf("validate output quarantine root: %w", err)
	}
	if outputQuarantineRoot == outputRoot {
		return nil, errors.New("approved output root and output quarantine root must be distinct")
	}
	monotonicNow := config.MonotonicNow
	if monotonicNow == nil {
		origin := time.Now()
		monotonicNow = func() time.Duration { return time.Since(origin) }
	}
	wait := config.Wait
	if wait == nil {
		wait = waitContext
	}
	artifactStoreReachable := config.ArtifactStoreReachable
	if artifactStoreReachable == nil {
		artifactStoreReachable = func(context.Context) bool { return false }
	}
	partSize := config.ArtifactPartSize
	if partSize == 0 {
		partSize = defaultArtifactPartSize
	}
	if partSize < 5<<20 || partSize > 512<<20 {
		return nil, errors.New("worker agent Artifact part size must be between 5 MiB and 512 MiB")
	}
	cleanupMinBytesPerSecond := config.OutputCleanupMinBytesPerSecond
	if cleanupMinBytesPerSecond == 0 {
		cleanupMinBytesPerSecond = defaultOutputCleanupMinBytesPerSecond
	}
	if cleanupMinBytesPerSecond < 0 || cleanupMinBytesPerSecond > maximumOutputCleanupMinBytesPerSecond {
		return nil, errors.New("worker agent output cleanup minimum throughput is invalid")
	}
	cleanupMaxDuration := config.OutputCleanupMaxDuration
	if cleanupMaxDuration == 0 {
		cleanupMaxDuration = defaultOutputCleanupMaxDuration
	}
	if config.OutputCleanupMaxBytes < 0 {
		return nil, errors.New("worker agent output cleanup maximum bytes is invalid")
	}
	if config.OutputCleanupMaxBytes > 0 {
		if err := ValidateOutputCleanupBudget(
			config.OutputCleanupMaxBytes, cleanupMinBytesPerSecond, cleanupMaxDuration,
		); err != nil {
			return nil, fmt.Errorf("validate worker agent output cleanup budget: %w", err)
		}
	} else if cleanupMaxDuration <= defaultTerminalControlTimeout+defaultOutputCleanupBaseTimeout {
		return nil, errors.New("worker agent output cleanup maximum duration is invalid")
	}
	return &Agent{
		workerID: config.WorkerID, workerEpoch: config.WorkerEpoch,
		recovery: config.Recovery, control: config.Control, runner: config.Runner,
		heartbeatInterval: config.HeartbeatInterval,
		monotonicNow:      monotonicNow, wait: wait,
		artifactStoreReachable:         artifactStoreReachable,
		outputRoot:                     outputRoot,
		outputQuarantineRoot:           outputQuarantineRoot,
		cleanupTimeout:                 defaultOutputCleanupBaseTimeout,
		outputCleanupMinBytesPerSecond: cleanupMinBytesPerSecond,
		outputCleanupMaxBytes:          config.OutputCleanupMaxBytes,
		outputCleanupMaxDuration:       cleanupMaxDuration - defaultTerminalControlTimeout,
		inferenceBackendRevision:       config.InferenceBackendRevision,
		outputOwnerUID:                 config.OutputOwnerUID,
		finalization:                   config.Finalization,
		partUploader:                   config.PartUploader,
		artifactPartSize:               partSize,
	}, nil
}

func (agent *Agent) RunOnce(ctx context.Context) (Result, error) {
	if agent == nil || agent.recovery == nil || agent.control == nil || agent.runner == nil ||
		agent.finalization == nil || agent.partUploader == nil {
		return Result{}, errors.New("worker agent is not configured")
	}
	if ctx == nil {
		return Result{}, errors.New("worker agent context is required")
	}
	watermark, err := agent.recovery.Watermark(ctx)
	if err != nil {
		return Result{}, err
	}
	if resumed, ok, err := agent.resumePendingControlOperation(ctx, watermark.State); ok || err != nil {
		return resumed, err
	}
	if resumed, ok, err := agent.resumeFinalization(ctx, watermark.State); ok || err != nil {
		return resumed, err
	}
	if !watermark.AssignmentAllowed {
		return Result{Outcome: OutcomeBackpressured, Watermark: watermark.State}, nil
	}
	requestStarted := agent.monotonicNow()
	if requestStarted < 0 {
		return Result{}, errors.New("worker agent monotonic clock returned a negative value")
	}
	assignment, err := agent.control.Acquire(ctx, agent.workerEpoch)
	if err != nil {
		var failure *workercontrol.Failure
		if errors.As(err, &failure) && failure.Code == workercontrol.FailureNoAssignment {
			return Result{Outcome: OutcomeIdle, Watermark: watermark.State}, nil
		}
		return Result{}, err
	}
	if err := agent.validateAssignment(assignment); err != nil {
		return Result{}, err
	}
	deadline, err := monotonicDeadline(requestStarted, assignment.LeaseValidFor)
	if err != nil {
		return Result{}, err
	}
	identity := runnertransport.AttemptIdentity{
		AttemptID: assignment.AttemptID, JobID: assignment.JobID, WorkerID: assignment.WorkerID,
		WorkerEpoch: assignment.WorkerEpoch, LeaseFence: assignment.LeaseFence,
	}
	spec := runnertransport.ExecutionSpec{
		ModelRevisionID:            assignment.ModelRevisionID,
		GenerationPresetRevisionID: assignment.GenerationPresetRevisionID,
		ExecutionProfileRevisionID: assignment.ExecutionProfileRevisionID,
		OutputSpecID:               assignment.OutputSpecID,
		RequestContent:             json.RawMessage(assignment.RequestContent),
	}
	handle, err := agent.recovery.Open(ctx, workerrecovery.Identity{
		AttemptID: assignment.AttemptID, WorkerID: assignment.WorkerID,
		WorkerEpoch: assignment.WorkerEpoch, Fence: assignment.LeaseFence,
	})
	if err != nil {
		return Result{}, fmt.Errorf("open Assignment Local Recovery State: %w", err)
	}
	_ = handle
	credentials := workercontrol.LeaseCredentials{
		AttemptID: assignment.AttemptID, WorkerEpoch: assignment.WorkerEpoch,
		Fence: assignment.LeaseFence, Token: assignment.LeaseToken,
	}
	operationContext, cancelOperation, err := agent.leaseOperationContext(ctx, deadline)
	if err != nil {
		return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark.State)
	}
	prepared, err := agent.runner.Prepare(
		operationContext,
		identity,
		spec,
		watermark.ResumeEligible,
	)
	cancelOperation()
	if err != nil {
		if agent.requireLeaseTime(deadline) != nil {
			return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark.State)
		}
		return Result{}, agent.handleOperationError(ctx, identity, err)
	}
	if prepared.Decision != runnertransport.CommandAccepted {
		summary := prepared.Detail
		if !validBoundedPrintable(summary, 1000) {
			summary = "runner rejected Assignment preparation"
		}
		return agent.reportRunnerFailure(
			ctx, handle, identity, assignment, credentials, watermark.State, deadline,
			workercontrol.FailureObservation{
				FailureClass: "RUNNER_PREPARE_REJECTED", FailureFingerprint: "runner/prepare/rejected",
				ErrorSummary: summary, BackendStage: "prepare",
				InferenceBackendRevision: agent.inferenceBackendRevision,
				WorkerReusable:           true,
			},
		)
	}
	if err := agent.persistPendingStart(ctx, handle, assignment); err != nil {
		return Result{}, err
	}
	operationContext, cancelOperation, err = agent.leaseOperationContext(ctx, deadline)
	if err != nil {
		return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark.State)
	}
	started, err := agent.control.Start(operationContext, credentials)
	cancelOperation()
	if err != nil {
		if agent.requireLeaseTime(deadline) != nil {
			return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark.State)
		}
		return Result{}, agent.handleOperationError(ctx, identity, err)
	}
	if started.Decision == workercontrol.Stop {
		return agent.terminateForControlStop(
			ctx, handle, identity, assignment, watermark.State, started.StopReason,
		)
	}
	if err := validateStartGrant(started, assignment); err != nil {
		return Result{}, err
	}
	if err := clearPendingControlOperation(ctx, handle); err != nil {
		return Result{}, err
	}
	operationContext, cancelOperation, err = agent.leaseOperationContext(ctx, deadline)
	if err != nil {
		return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark.State)
	}
	runnerStarted, err := agent.runner.Start(operationContext, identity)
	cancelOperation()
	if err != nil {
		if agent.requireLeaseTime(deadline) != nil {
			return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark.State)
		}
		return Result{}, agent.handleOperationError(ctx, identity, err)
	}
	if runnerStarted.Decision != runnertransport.CommandAccepted {
		summary := runnerStarted.Detail
		if !validBoundedPrintable(summary, 1000) {
			summary = "runner rejected the granted Start"
		}
		return agent.reportRunnerFailure(
			ctx, handle, identity, assignment, credentials, watermark.State, deadline,
			workercontrol.FailureObservation{
				FailureClass: "RUNNER_START_REJECTED", FailureFingerprint: "runner/start/rejected",
				ErrorSummary: summary, BackendStage: "start",
				InferenceBackendRevision: agent.inferenceBackendRevision,
				WorkerReusable:           true,
			},
		)
	}
	return agent.runExecutionLoop(ctx, handle, identity, assignment, credentials, watermark.State, deadline)
}

func (agent *Agent) runExecutionLoop(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	watermark workerrecovery.WatermarkState,
	deadline time.Duration,
) (Result, error) {
	for {
		operationContext, cancelOperation, err := agent.leaseOperationContext(ctx, deadline)
		if err != nil {
			return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
		}
		status, err := agent.runner.Status(operationContext, identity)
		cancelOperation()
		if err != nil {
			if agent.requireLeaseTime(deadline) != nil {
				return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
			}
			return Result{}, agent.handleOperationError(ctx, identity, err)
		}
		switch status.State {
		case runnertransport.ExecutionFailed:
			if status.Failure == nil {
				return Result{}, errors.New("failed runner Status omitted FailureObservation")
			}
			if err := agent.requireLeaseTime(deadline); err != nil {
				return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
			}
			return agent.reportRunnerFailure(
				ctx, handle, identity, assignment, credentials, watermark, deadline,
				workercontrol.FailureObservation{
					FailureClass:             status.Failure.FailureClass,
					FailureFingerprint:       status.Failure.FailureFingerprint,
					ErrorSummary:             status.Failure.ErrorSummary,
					BackendStage:             status.Failure.BackendStage,
					GPUUUIDs:                 append([]string(nil), status.Failure.GPUUUIDs...),
					InferenceBackendRevision: status.Failure.InferenceBackendRevision,
					RetryRecommended:         status.Failure.RetryRecommended,
					WorkerReusable:           status.Failure.WorkerReusable,
				},
			)
		case runnertransport.ExecutionSucceeded:
			operationContext, cancelOperation, err = agent.leaseOperationContext(ctx, deadline)
			if err != nil {
				return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
			}
			collected, err := agent.runner.CollectOutputs(operationContext, identity)
			cancelOperation()
			if err != nil {
				if agent.requireLeaseTime(deadline) != nil {
					return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
				}
				return Result{}, agent.handleOperationError(ctx, identity, err)
			}
			if collected.Decision != runnertransport.CommandAccepted || len(collected.Outputs) == 0 {
				return Result{}, errors.New("runner did not provide accepted outputs")
			}
			for _, output := range collected.Outputs {
				if err := validateOutputPath(agent.outputRoot, output.Path); err != nil {
					return Result{}, err
				}
			}
			completion, cleanup, err := agent.finalizeOutputs(
				ctx, handle, assignment, credentials, collected.Outputs,
				status.Sequence, status.GPUHealth, deadline,
			)
			if errors.Is(err, errFinalizationAuthorityLost) {
				return agent.terminateFinalizationForControlStop(
					ctx, handle, identity, assignment, watermark, workercontrol.StopInvalidAuthority,
					collected.Outputs,
				)
			}
			if errors.Is(err, errLeaseDeadlineElapsed) {
				return agent.terminateFinalizationForLeaseDeadline(
					ctx, handle, identity, assignment, watermark, collected.Outputs,
				)
			}
			var validationFailure *artifactValidationFailure
			if errors.As(err, &validationFailure) && validationFailure.leaseDeadline > 0 {
				return agent.reportFinalizationValidationFailure(
					ctx, handle, identity, assignment, credentials, watermark,
					collected.Outputs, validationFailure,
				)
			}
			if err != nil {
				return Result{}, agent.handleOperationError(ctx, identity, err)
			}
			result := Result{
				Outcome: OutcomeVisibleCompletion, Watermark: watermark,
				AttemptID: assignment.AttemptID, JobID: assignment.JobID,
				VisibleCompletion: completion,
			}
			cleanupContext, cancelCleanup, err := agent.outputCleanupContext(ctx, collected.Outputs)
			if err != nil {
				return result, err
			}
			defer cancelCleanup()
			if err := cleanup(cleanupContext); err != nil {
				return result, err
			}
			if err := handle.MarkTerminal(cleanupContext); err != nil {
				return result, err
			}
			return result, nil
		case runnertransport.ExecutionCanceled:
			return agent.reportRunnerFailure(
				ctx, handle, identity, assignment, credentials, watermark, deadline,
				workercontrol.FailureObservation{
					FailureClass: "RUNNER_CANCELED", FailureFingerprint: "runner/canceled/unexpected",
					ErrorSummary:             "runner canceled execution without control-plane authority",
					BackendStage:             status.BackendStage,
					InferenceBackendRevision: agent.inferenceBackendRevision,
					RetryRecommended:         true, WorkerReusable: true,
				},
			)
		case runnertransport.ExecutionPreparing, runnertransport.ExecutionReady,
			runnertransport.ExecutionRunning:
			phase := workercontrol.ExecutionPhasePreparing
			if status.State == runnertransport.ExecutionRunning {
				phase = workercontrol.ExecutionPhaseGenerating
			}
			currentWatermark, err := agent.recovery.Watermark(ctx)
			if err != nil {
				return Result{}, err
			}
			heartbeatStarted := agent.monotonicNow()
			if heartbeatStarted < 0 || heartbeatStarted >= deadline {
				return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
			}
			operationContext, cancelOperation, err = agent.leaseOperationContext(ctx, deadline)
			if err != nil {
				return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
			}
			observation := workercontrol.HeartbeatObservation{
				Sequence: status.Sequence, BackendStage: status.BackendStage,
				BackendStageProgress:      status.BackendStageProgress,
				EstimatedRemainingSeconds: status.EstimatedRemainingSeconds,
				GPUHealthSummary:          append([]byte(nil), status.GPUHealth...),
				LocalArtifactState:        append([]byte(nil), status.LocalArtifactState...),
				ScratchFreeBytes:          currentWatermark.FreeBytes,
				ArtifactStoreReachable:    agent.artifactStoreReachable(operationContext),
			}
			if err := agent.persistPendingHeartbeat(
				ctx, handle, assignment, observation, phase, nil,
			); err != nil {
				cancelOperation()
				return Result{}, err
			}
			heartbeat, err := agent.control.Heartbeat(operationContext, credentials, observation)
			cancelOperation()
			if err != nil {
				if agent.requireLeaseTime(deadline) != nil {
					return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
				}
				return Result{}, agent.handleOperationError(ctx, identity, err)
			}
			if heartbeat.Decision == workercontrol.HeartbeatStop {
				return agent.terminateForControlStop(
					ctx, handle, identity, assignment, watermark, heartbeat.StopReason,
				)
			}
			if heartbeat.Decision != workercontrol.HeartbeatContinue ||
				heartbeat.AttemptID != assignment.AttemptID || heartbeat.JobID != assignment.JobID ||
				heartbeat.WorkerID != assignment.WorkerID ||
				heartbeat.WorkerEpoch != assignment.WorkerEpoch ||
				heartbeat.LeaseFence != assignment.LeaseFence ||
				heartbeat.HeartbeatSequence != status.Sequence ||
				heartbeat.ExecutionPhase != phase {
				return Result{}, errors.New("control-plane Heartbeat did not continue the exact Assignment authority")
			}
			if err := clearPendingControlOperation(ctx, handle); err != nil {
				return Result{}, agent.handleOperationError(ctx, identity, err)
			}
			deadline, err = monotonicDeadline(heartbeatStarted, heartbeat.LeaseValidFor)
			if err != nil {
				return Result{}, err
			}
			operationContext, cancelOperation, err = agent.leaseOperationContext(ctx, deadline)
			if err != nil {
				return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
			}
			err = agent.wait(operationContext, agent.heartbeatInterval)
			cancelOperation()
			if err != nil {
				if agent.requireLeaseTime(deadline) != nil {
					return agent.terminateForLeaseDeadline(ctx, handle, identity, assignment, watermark)
				}
				return Result{}, agent.handleOperationError(ctx, identity, err)
			}
		default:
			return Result{}, errors.New("worker agent execution loop is not implemented for this runner state")
		}
	}
}

func (agent *Agent) resumeFinalization(
	ctx context.Context,
	watermark workerrecovery.WatermarkState,
) (Result, bool, error) {
	handles, err := agent.recovery.ActiveHandles(ctx)
	if err != nil {
		return Result{}, false, err
	}
	var resumedHandle *workerrecovery.Handle
	var resumedState localFinalizationState
	for _, handle := range handles {
		state, err := readFinalizationState(ctx, handle)
		if errors.Is(err, workerrecovery.ErrStateNotFound) {
			continue
		}
		if err != nil {
			return Result{}, false, err
		}
		identity, err := handle.Identity()
		if err != nil {
			return Result{}, false, err
		}
		if state.AttemptID != identity.AttemptID || state.WorkerID != identity.WorkerID ||
			state.WorkerEpoch != identity.WorkerEpoch || state.LeaseFence != identity.Fence ||
			state.WorkerID != agent.workerID || state.WorkerEpoch != agent.workerEpoch {
			return Result{}, false, errors.New("local Finalization State does not match active Worker authority")
		}
		if resumedHandle != nil {
			return Result{}, false, errors.New("multiple active Local Finalization States require operator reconciliation")
		}
		resumedHandle = handle
		resumedState = state
	}
	if resumedHandle == nil {
		return Result{}, false, nil
	}
	assignment := workercontrol.Assignment{
		AttemptID: resumedState.AttemptID, JobID: resumedState.JobID,
		WorkerID: resumedState.WorkerID, WorkerEpoch: resumedState.WorkerEpoch,
		LeaseFence: resumedState.LeaseFence, LeaseToken: resumedState.LeaseToken,
	}
	credentials := workercontrol.LeaseCredentials{
		AttemptID: resumedState.AttemptID, WorkerEpoch: resumedState.WorkerEpoch,
		Fence: resumedState.LeaseFence, Token: resumedState.LeaseToken,
	}
	identity := runnertransport.AttemptIdentity{
		AttemptID: resumedState.AttemptID, JobID: resumedState.JobID,
		WorkerID: resumedState.WorkerID, WorkerEpoch: resumedState.WorkerEpoch,
		LeaseFence: resumedState.LeaseFence,
	}
	completion, cleanup, err := agent.finalizeOutputs(
		ctx, resumedHandle, assignment, credentials, resumedState.Outputs,
		resumedState.HeartbeatSequence, resumedState.GPUHealthSummary, 0,
	)
	if errors.Is(err, errFinalizationAuthorityLost) {
		result, err := agent.terminateFinalizationForControlStop(
			ctx, resumedHandle, identity, assignment, watermark, workercontrol.StopInvalidAuthority,
			resumedState.Outputs,
		)
		return result, true, err
	}
	if errors.Is(err, errLeaseDeadlineElapsed) {
		result, err := agent.terminateFinalizationForLeaseDeadline(
			ctx, resumedHandle, identity, assignment, watermark, resumedState.Outputs,
		)
		return result, true, err
	}
	var validationFailure *artifactValidationFailure
	if errors.As(err, &validationFailure) && validationFailure.leaseDeadline > 0 {
		result, err := agent.reportFinalizationValidationFailure(
			ctx, resumedHandle, identity, assignment, credentials, watermark,
			resumedState.Outputs, validationFailure,
		)
		return result, true, err
	}
	if err != nil {
		return Result{}, true, agent.handleOperationError(ctx, identity, err)
	}
	result := Result{
		Outcome: OutcomeVisibleCompletion, Watermark: watermark,
		AttemptID: resumedState.AttemptID, JobID: resumedState.JobID,
		VisibleCompletion: completion,
	}
	cleanupContext, cancelCleanup, err := agent.outputCleanupContext(ctx, resumedState.Outputs)
	if err != nil {
		return result, true, err
	}
	defer cancelCleanup()
	if err := cleanup(cleanupContext); err != nil {
		return result, true, err
	}
	if err := resumedHandle.MarkTerminal(cleanupContext); err != nil {
		return result, true, err
	}
	return result, true, nil
}

func (agent *Agent) reportRunnerFailure(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	watermark workerrecovery.WatermarkState,
	deadline time.Duration,
	observation workercontrol.FailureObservation,
) (Result, error) {
	if err := agent.persistPendingFail(ctx, handle, assignment, observation, nil); err != nil {
		return Result{}, err
	}
	return agent.executePendingFail(
		ctx, handle, identity, assignment, credentials, watermark, observation, nil, deadline,
	)
}

func (agent *Agent) reportFinalizationValidationFailure(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	watermark workerrecovery.WatermarkState,
	outputs []runnertransport.Output,
	failure *artifactValidationFailure,
) (Result, error) {
	if failure == nil || failure.artifactID == uuid.Nil || failure.uploadID == uuid.Nil {
		return Result{}, errors.New("artifact validation failure identity is incomplete")
	}
	if failure.leaseDeadline <= 0 {
		return Result{}, errors.New("artifact validation failure Lease authority is incomplete")
	}
	observation := workercontrol.FailureObservation{
		FailureClass: "ARTIFACT_VALIDATION_FAILED",
		FailureFingerprint: fmt.Sprintf(
			"artifact.validation/%s/%s", failure.artifactID, failure.uploadID,
		),
		ErrorSummary:             "artifact failed certified output validation",
		BackendStage:             "finalization",
		InferenceBackendRevision: agent.inferenceBackendRevision,
		RetryRecommended:         true,
		WorkerReusable:           true,
	}
	if err := agent.persistPendingFail(ctx, handle, assignment, observation, outputs); err != nil {
		return Result{}, err
	}
	return agent.executePendingFail(
		ctx, handle, identity, assignment, credentials, watermark,
		observation, outputs, failure.leaseDeadline,
	)
}

func (agent *Agent) handleOperationError(
	ctx context.Context,
	identity runnertransport.AttemptIdentity,
	cause error,
) error {
	if ctx.Err() == nil && !errors.Is(cause, context.Canceled) &&
		!errors.Is(cause, context.DeadlineExceeded) {
		return cause
	}
	cleanupContext, cancel := boundedCleanupContext(ctx)
	defer cancel()
	canceled, cancelErr := agent.runner.Cancel(
		cleanupContext, identity, runnertransport.CancelAgentShutdown,
	)
	if cancelErr == nil && canceled.Decision != runnertransport.CommandAccepted {
		cancelErr = errors.New("runner rejected Agent-shutdown cancellation")
	}
	return errors.Join(cause, cancelErr)
}

func validateOutputPath(root, path string) error {
	cleaned := filepath.Clean(path)
	if !filepath.IsAbs(cleaned) || cleaned != path {
		return errors.New("runner output is outside the approved output root")
	}
	parent, err := securefile.ResolveTrustedDirectory(filepath.Dir(cleaned))
	if err != nil {
		return fmt.Errorf("validate runner output parent: %w", err)
	}
	resolved := filepath.Join(parent, filepath.Base(cleaned))
	if resolved == root {
		return errors.New("runner output is outside the approved output root")
	}
	relative, err := filepath.Rel(root, resolved)
	if err != nil || relative == "." || relative == ".." ||
		strings.HasPrefix(relative, ".."+string(filepath.Separator)) || filepath.IsAbs(relative) {
		return errors.New("runner output is outside the approved output root")
	}
	return nil
}

func validBoundedPrintable(value string, maxRunes int) bool {
	if !utf8.ValidString(value) || strings.TrimSpace(value) == "" || strings.TrimSpace(value) != value {
		return false
	}
	count := 0
	for _, character := range value {
		if !unicode.IsPrint(character) {
			return false
		}
		count++
		if count > maxRunes {
			return false
		}
	}
	return count > 0
}

func (agent *Agent) terminateForLeaseDeadline(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	watermark workerrecovery.WatermarkState,
) (Result, error) {
	if err := agent.persistPendingTermination(
		ctx, handle, assignment, runnertransport.CancelLeaseDeadline,
		OutcomeLeaseDeadlineElapsed, "", nil,
	); err != nil {
		return Result{}, err
	}
	return agent.executePendingTermination(
		ctx, handle, identity, assignment, watermark,
		runnertransport.CancelLeaseDeadline, OutcomeLeaseDeadlineElapsed, "", nil,
	)
}

func (agent *Agent) terminateForControlStop(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	watermark workerrecovery.WatermarkState,
	stopReason workercontrol.StopReason,
) (Result, error) {
	if stopReason == "" {
		return Result{}, errors.New("control plane returned STOP without a reason")
	}
	if err := agent.persistPendingTermination(
		ctx, handle, assignment, runnertransport.CancelControlPlaneStop,
		OutcomeControlPlaneStopped, stopReason, nil,
	); err != nil {
		return Result{}, err
	}
	return agent.executePendingTermination(
		ctx, handle, identity, assignment, watermark,
		runnertransport.CancelControlPlaneStop, OutcomeControlPlaneStopped, stopReason, nil,
	)
}

func (agent *Agent) terminateFinalizationForControlStop(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	watermark workerrecovery.WatermarkState,
	stopReason workercontrol.StopReason,
	outputs []runnertransport.Output,
) (Result, error) {
	if stopReason == "" {
		return Result{}, errors.New("control plane returned STOP without a reason")
	}
	if err := agent.persistPendingTermination(
		ctx, handle, assignment, runnertransport.CancelControlPlaneStop,
		OutcomeControlPlaneStopped, stopReason, outputs,
	); err != nil {
		return Result{}, err
	}
	return agent.executePendingTermination(
		ctx, handle, identity, assignment, watermark,
		runnertransport.CancelControlPlaneStop, OutcomeControlPlaneStopped, stopReason, outputs,
	)
}

func (agent *Agent) terminateFinalizationForLeaseDeadline(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	watermark workerrecovery.WatermarkState,
	outputs []runnertransport.Output,
) (Result, error) {
	if err := agent.persistPendingTermination(
		ctx, handle, assignment, runnertransport.CancelLeaseDeadline,
		OutcomeLeaseDeadlineElapsed, "", outputs,
	); err != nil {
		return Result{}, err
	}
	return agent.executePendingTermination(
		ctx, handle, identity, assignment, watermark,
		runnertransport.CancelLeaseDeadline, OutcomeLeaseDeadlineElapsed, "", outputs,
	)
}

func (agent *Agent) cleanupTerminalFinalization(
	ctx context.Context,
	handle *workerrecovery.Handle,
	identity runnertransport.AttemptIdentity,
	assignment workercontrol.Assignment,
	outputs []runnertransport.Output,
	cancelReason runnertransport.CancelReason,
) error {
	_, err := agent.cancelCleanupAndMarkTerminal(
		ctx, handle, identity, assignment, cancelReason, outputs, nil,
	)
	return err
}

func boundedCleanupContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(ctx), defaultTerminalControlTimeout)
}

func (agent *Agent) outputCleanupContext(
	ctx context.Context,
	outputs []runnertransport.Output,
) (context.Context, context.CancelFunc, error) {
	var totalBytes int64
	for _, output := range outputs {
		if output.SizeBytes <= 0 || totalBytes > math.MaxInt64-output.SizeBytes {
			return nil, nil, errors.New("terminal output cleanup size is invalid")
		}
		totalBytes += output.SizeBytes
	}
	if agent.outputCleanupMaxBytes > 0 && totalBytes > agent.outputCleanupMaxBytes {
		return nil, nil, errors.New("terminal output cleanup size exceeds the configured maximum")
	}
	baseTimeout := agent.cleanupTimeout
	if baseTimeout <= 0 {
		baseTimeout = defaultOutputCleanupBaseTimeout
	}
	timeout, err := outputCleanupBudget(
		totalBytes, agent.outputCleanupMinBytesPerSecond, baseTimeout, agent.outputCleanupMaxDuration,
	)
	if err != nil {
		return nil, nil, err
	}
	cleanupContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	return cleanupContext, cancel, nil
}

// ValidateOutputCleanupBudget proves that the maximum Attempt payload fits its terminal retention window.
func ValidateOutputCleanupBudget(maxBytes, minBytesPerSecond int64, maxDuration time.Duration) error {
	if maxBytes <= 0 {
		return errors.New("output cleanup maximum bytes must be positive")
	}
	if maxDuration <= defaultTerminalControlTimeout {
		return errors.New("output cleanup budget exceeds maximum duration")
	}
	_, err := outputCleanupBudget(
		maxBytes, minBytesPerSecond, defaultOutputCleanupBaseTimeout,
		maxDuration-defaultTerminalControlTimeout,
	)
	return err
}

func outputCleanupBudget(
	bytes int64,
	minBytesPerSecond int64,
	baseTimeout time.Duration,
	maxDuration time.Duration,
) (time.Duration, error) {
	if bytes < 0 {
		return 0, errors.New("output cleanup bytes must be nonnegative")
	}
	if minBytesPerSecond <= 0 || minBytesPerSecond > maximumOutputCleanupMinBytesPerSecond {
		return 0, errors.New("output cleanup minimum throughput is invalid")
	}
	if baseTimeout <= 0 || maxDuration <= baseTimeout {
		return 0, errors.New("output cleanup maximum duration is invalid")
	}
	seconds := bytes / minBytesPerSecond
	remainder := bytes % minBytesPerSecond
	if seconds > int64((maxDuration-baseTimeout)/time.Second) {
		return 0, errors.New("output cleanup budget exceeds maximum duration")
	}
	readBudget := time.Duration(seconds) * time.Second
	if remainder != 0 {
		milliseconds := (remainder*1000 + minBytesPerSecond - 1) / minBytesPerSecond
		readBudget += time.Duration(milliseconds) * time.Millisecond
	}
	if readBudget > maxDuration-baseTimeout {
		return 0, errors.New("output cleanup budget exceeds maximum duration")
	}
	return baseTimeout + readBudget, nil
}

func (agent *Agent) validateAssignment(assignment workercontrol.Assignment) error {
	if assignment.AttemptID == uuid.Nil || assignment.JobID == uuid.Nil ||
		assignment.WorkerID != agent.workerID || assignment.WorkerEpoch != agent.workerEpoch ||
		assignment.ModelRevisionID == uuid.Nil || assignment.GenerationPresetRevisionID == uuid.Nil ||
		assignment.ExecutionProfileRevisionID == uuid.Nil || assignment.OutputSpecID == uuid.Nil ||
		assignment.AttemptNumber <= 0 || assignment.LeaseFence <= 0 || assignment.LeaseToken == "" ||
		assignment.LeaseValidFor <= 0 || assignment.RequestContent == "" {
		return errors.New("control plane returned an invalid or mismatched Assignment")
	}
	return nil
}

func monotonicDeadline(started, validFor time.Duration) (time.Duration, error) {
	if started < 0 || validFor <= 0 || started > time.Duration(math.MaxInt64)-validFor {
		return 0, errors.New("assignment Lease duration cannot form a monotonic deadline")
	}
	return started + validFor, nil
}

func (agent *Agent) requireLeaseTime(deadline time.Duration) error {
	now := agent.monotonicNow()
	if now < 0 || now >= deadline {
		return errors.New("assignment Lease monotonic deadline has elapsed")
	}
	return nil
}

func (agent *Agent) leaseOperationContext(
	ctx context.Context,
	deadline time.Duration,
) (context.Context, context.CancelFunc, error) {
	if ctx == nil {
		return nil, nil, errors.New("worker agent operation context is required")
	}
	now := agent.monotonicNow()
	if now < 0 || now >= deadline {
		return nil, nil, errLeaseDeadlineElapsed
	}
	operationContext, cancel := context.WithTimeout(ctx, deadline-now)
	return operationContext, cancel, nil
}

func waitContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
