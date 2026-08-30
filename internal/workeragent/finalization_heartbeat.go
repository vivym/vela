package workeragent

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/workercontrol"
	"github.com/vivym/vela/internal/workerrecovery"
)

type finalizationHeartbeatSession struct {
	ctx    context.Context
	cancel context.CancelFunc
	stop   chan struct{}
	done   chan error
	state  *finalizationHeartbeatProgress
}

type finalizationHeartbeatProgress struct {
	agent       *Agent
	handle      *workerrecovery.Handle
	assignment  workercontrol.Assignment
	credentials workercontrol.LeaseCredentials
	stateMu     sync.Mutex
	state       localFinalizationState
	deadline    time.Duration
}

func (agent *Agent) startFinalizationHeartbeats(
	ctx context.Context,
	handle *workerrecovery.Handle,
	assignment workercontrol.Assignment,
	credentials workercontrol.LeaseCredentials,
	state localFinalizationState,
	deadline time.Duration,
) (*finalizationHeartbeatSession, error) {
	runContext, cancel := context.WithCancel(ctx)
	progress := &finalizationHeartbeatProgress{
		agent: agent, handle: handle, assignment: assignment,
		credentials: credentials, state: state, deadline: deadline,
	}
	if err := progress.send(runContext); err != nil {
		cancel()
		return nil, err
	}
	session := &finalizationHeartbeatSession{
		ctx: runContext, cancel: cancel, stop: make(chan struct{}), done: make(chan error, 1),
		state: progress,
	}
	go func() {
		for {
			delay := agent.heartbeatInterval
			deadlineElapsed := false
			if progress.deadline > 0 {
				now := agent.monotonicNow()
				if now < 0 || now >= progress.deadline {
					session.done <- errLeaseDeadlineElapsed
					cancel()
					return
				}
				remaining := progress.deadline - now
				if remaining <= delay {
					delay = remaining
					deadlineElapsed = true
				}
			}
			timer := time.NewTimer(delay)
			select {
			case <-session.stop:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				session.done <- nil
				return
			case <-runContext.Done():
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				session.done <- runContext.Err()
				return
			case <-timer.C:
				if deadlineElapsed {
					session.done <- errLeaseDeadlineElapsed
					cancel()
					return
				}
				if err := progress.send(runContext); err != nil {
					session.done <- err
					cancel()
					return
				}
			}
		}
	}()
	return session, nil
}

func (session *finalizationHeartbeatSession) Context() context.Context {
	return session.ctx
}

func (session *finalizationHeartbeatSession) Stop() error {
	if session == nil {
		return errors.New("finalization Heartbeat session is not configured")
	}
	close(session.stop)
	err := <-session.done
	session.cancel()
	return err
}

func (session *finalizationHeartbeatSession) LeaseDeadline() (time.Duration, error) {
	if session == nil || session.state == nil {
		return 0, errors.New("finalization Heartbeat session is not configured")
	}
	deadline := session.state.deadline
	if deadline <= 0 {
		return 0, errors.New("finalization Heartbeat did not establish Lease authority")
	}
	return deadline, nil
}

func (session *finalizationHeartbeatSession) persistCompletionCandidate(
	ctx context.Context,
	candidate workercontrol.VisibleCompletionCandidate,
) (localFinalizationState, error) {
	if session == nil || session.state == nil {
		return localFinalizationState{}, errors.New("finalization Heartbeat session is not configured")
	}
	progress := session.state
	progress.stateMu.Lock()
	defer progress.stateMu.Unlock()
	if progress.state.CompletionCandidate != nil || progress.state.VisibleCompletion != nil {
		return localFinalizationState{}, errors.New("local Visible Completion candidate is already persisted")
	}
	candidate.ArtifactIDs = append([]uuid.UUID(nil), candidate.ArtifactIDs...)
	if err := validateCompletionCandidate(candidate, progress.state); err != nil {
		return localFinalizationState{}, err
	}
	progress.state.CompletionCandidate = &candidate
	if err := writeFinalizationState(ctx, progress.handle, progress.state); err != nil {
		progress.state.CompletionCandidate = nil
		return localFinalizationState{}, err
	}
	return progress.state, nil
}

func (progress *finalizationHeartbeatProgress) send(ctx context.Context) error {
	heartbeatStarted := progress.agent.monotonicNow()
	if heartbeatStarted < 0 {
		return errors.New("worker agent monotonic clock returned a negative value")
	}
	operationContext, cancelOperation, err := progress.agent.finalizationOperationContext(
		ctx,
		progress.deadline,
	)
	if err != nil {
		if progress.deadline > 0 {
			return errLeaseDeadlineElapsed
		}
		return err
	}
	defer cancelOperation()
	progress.stateMu.Lock()
	if progress.state.HeartbeatSequence == math.MaxInt64 {
		progress.stateMu.Unlock()
		return errors.New("finalization Heartbeat sequence is exhausted")
	}
	progress.state.HeartbeatSequence++
	state := progress.state
	if err := writeFinalizationState(operationContext, progress.handle, state); err != nil {
		progress.stateMu.Unlock()
		return progress.mapLeaseError(ctx, operationContext, err)
	}
	progress.stateMu.Unlock()
	watermark, err := progress.agent.recovery.Watermark(operationContext)
	if err != nil {
		return progress.mapLeaseError(ctx, operationContext, err)
	}
	observation := workercontrol.HeartbeatObservation{
		Sequence: state.HeartbeatSequence, BackendStage: "artifact-finalization",
		GPUHealthSummary:       append(json.RawMessage(nil), state.GPUHealthSummary...),
		LocalArtifactState:     json.RawMessage(`{"state":"artifact-finalization"}`),
		ScratchFreeBytes:       watermark.FreeBytes,
		ArtifactStoreReachable: progress.agent.artifactStoreReachable(operationContext),
	}
	if err := progress.agent.persistPendingHeartbeat(
		operationContext, progress.handle, progress.assignment, observation,
		workercontrol.ExecutionPhaseFinalizing, state.Outputs,
	); err != nil {
		return progress.mapLeaseError(ctx, operationContext, err)
	}
	result, err := progress.agent.control.Heartbeat(
		operationContext, progress.credentials, observation,
	)
	if err != nil {
		if progress.deadline > 0 && ctx.Err() == nil &&
			(errors.Is(operationContext.Err(), context.DeadlineExceeded) ||
				progress.agent.requireLeaseTime(progress.deadline) != nil) {
			return errLeaseDeadlineElapsed
		}
		return err
	}
	if result.Decision == workercontrol.HeartbeatStop {
		return errFinalizationAuthorityLost
	}
	if result.Decision != workercontrol.HeartbeatContinue ||
		result.AttemptID != progress.assignment.AttemptID ||
		result.JobID != progress.assignment.JobID || result.WorkerID != progress.assignment.WorkerID ||
		result.WorkerEpoch != progress.assignment.WorkerEpoch ||
		result.LeaseFence != progress.assignment.LeaseFence ||
		result.HeartbeatSequence != state.HeartbeatSequence ||
		result.ExecutionPhase != workercontrol.ExecutionPhaseFinalizing ||
		result.LeaseValidFor <= 0 {
		return errors.New("control-plane Heartbeat did not continue exact Finalization authority")
	}
	if err := clearPendingControlOperation(ctx, progress.handle); err != nil {
		return err
	}
	progress.deadline, err = monotonicDeadline(heartbeatStarted, result.LeaseValidFor)
	if err != nil {
		return err
	}
	return nil
}

func (progress *finalizationHeartbeatProgress) mapLeaseError(
	parent context.Context,
	operation context.Context,
	err error,
) error {
	if progress.deadline > 0 && parent.Err() == nil &&
		errors.Is(operation.Err(), context.DeadlineExceeded) {
		return errLeaseDeadlineElapsed
	}
	return err
}
