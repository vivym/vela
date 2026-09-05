package stageworkeragent_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
)

func TestStreamAgentConsumesStopWhileControlResponseIsBlocked(t *testing.T) {
	for _, test := range []struct {
		operation     velav1.StageWorkerOperation
		renewedActive bool
	}{
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE, renewedActive: true},
	} {
		name := test.operation.String()
		if test.renewedActive {
			name += "/renewed-active"
		}
		t.Run(name, func(t *testing.T) {
			operation := test.operation
			fixture := newBarrierFixture(t, false)
			stopAuthority := fixture.authority
			if test.renewedActive {
				stopAuthority = renewBarrierAuthority(t, fixture.authority)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			release := make(chan struct{})
			var releaseOnce sync.Once
			releaseResponse := func() { releaseOnce.Do(func() { close(release) }) }
			defer releaseResponse()
			control := &stopLivenessControl{
				operation: operation, blocked: make(chan struct{}), release: release,
				commands: make(chan *velav1.StageWorkerControlServiceConnectResponse, 1),
			}
			runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
				Members: []stageworkeragent.RuntimeMember{
					{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
					{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
				},
			})
			if err != nil {
				t.Fatalf("New Agent: %v", err)
			}
			agent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
			if err != nil {
				t.Fatalf("NewStreamAgent: %v", err)
			}
			if operation != velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE {
				if _, err := agent.ExecuteAssignment(ctx, fixture.assignment); err != nil {
					t.Fatalf("ExecuteAssignment: %v", err)
				}
			}
			if operation == velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE && !test.renewedActive {
				agent, err = stageworkeragent.NewStreamAgent(runtimeAgent, control)
				if err != nil {
					t.Fatalf("rebuild StreamAgent: %v", err)
				}
			}
			operationFinished := make(chan error, 1)
			go func() {
				var err error
				switch operation {
				case velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE:
					_, err = agent.ExecuteAssignment(ctx, fixture.assignment)
				case velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE:
					_, err = agent.Heartbeat(ctx, 1)
				case velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE:
					_, err = agent.Reattach(ctx, stopAuthority, "", nil)
				}
				operationFinished <- err
			}()
			select {
			case <-control.blocked:
			case <-ctx.Done():
				t.Fatal("control operation did not reach its blocked response")
			}
			control.commands <- &velav1.StageWorkerControlServiceConnectResponse{
				Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{StopStage: &velav1.StopStage{
					Authority: stopAuthority,
					Reason:    velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_AUTHORITY_REVOKED,
				}},
			}
			close(control.commands)
			commandsFinished := make(chan error, 1)
			go func() { commandsFinished <- agent.RunControlCommands(ctx) }()
			consumed := false
			deadline := time.NewTimer(time.Second)
			select {
			case err := <-commandsFinished:
				consumed = true
				if err != nil {
					t.Errorf("RunControlCommands while response is blocked: %v", err)
				}
			case <-deadline.C:
				t.Error("unsolicited Stop waited for the blocked control response instead of canceling runtime")
			}
			deadline.Stop()
			if consumed {
				status, err := runtimeAgent.Status(ctx, stopAuthority)
				if err != nil {
					t.Errorf("Status before releasing control response: %v", err)
				}
				for memberID, state := range status.States {
					if state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
						t.Errorf("member %s state before releasing control response = %s, want CANCELING", memberID, state)
					}
				}
			}
			releaseResponse()
			select {
			case <-operationFinished:
			case <-ctx.Done():
				t.Error("control operation did not finish after response release")
			}
			if !consumed {
				select {
				case <-commandsFinished:
				case <-ctx.Done():
					t.Error("command consumer did not finish after response release")
				}
			}
		})
	}
}

type stopLivenessControl struct {
	operation velav1.StageWorkerOperation
	blocked   chan struct{}
	release   <-chan struct{}
	commands  chan *velav1.StageWorkerControlServiceConnectResponse
}

func (control *stopLivenessControl) NextCommand(ctx context.Context) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	return nextTestControlCommand(ctx, control.commands)
}

func (control *stopLivenessControl) Exchange(
	ctx context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	var operation velav1.StageWorkerOperation
	switch {
	case request.GetStartStage() != nil:
		operation = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE
	case request.GetHeartbeatStage() != nil:
		operation = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE
	case request.GetReattachStage() != nil:
		operation = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE
	default:
		return nil, errors.New("unexpected operation in Stop liveness test")
	}
	if operation == control.operation {
		close(control.blocked)
		select {
		case <-control.release:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return commandResultResponse(operation, velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED), nil
}
