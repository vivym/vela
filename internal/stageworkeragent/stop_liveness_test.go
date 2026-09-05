package stageworkeragent_test

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageauthority"
	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

func TestStreamAgentConsumesStopWhileControlResponseIsBlocked(t *testing.T) {
	for _, test := range []struct {
		operation     velav1.StageWorkerOperation
		renewedActive bool
		stopFails     bool
		staleStop     bool
		directStop    bool
		renewResponse bool
		durable       bool
	}{
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE, renewedActive: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE, stopFails: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE, stopFails: true, directStop: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE, stopFails: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE, staleStop: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE, renewedActive: true, staleStop: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE, renewResponse: true, directStop: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE, renewResponse: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE, renewResponse: true, directStop: true, durable: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE, renewResponse: true, durable: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE, renewedActive: true, durable: true},
		{operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE, stopFails: true, durable: true},
	} {
		name := test.operation.String()
		if test.renewedActive {
			name += "/renewed-active"
		}
		if test.stopFails {
			name += "/cancel-error"
		}
		if test.staleStop {
			name += "/stale-stop"
		}
		if test.directStop {
			name += "/direct-stop"
		}
		if test.renewResponse {
			name += "/renewed-response"
		}
		if test.durable {
			name += "/durable"
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
			if test.renewResponse {
				control.renewedAuthority = renewBarrierAuthority(t, fixture.authority)
			}
			clients := fixture.clients
			if test.stopFails {
				clients = []velav1.ModelRuntimeServiceClient{
					failingStopRuntimeClient{ModelRuntimeServiceClient: clients[0]},
					failingStopRuntimeClient{ModelRuntimeServiceClient: clients[1]},
				}
			}
			runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
				Members: []stageworkeragent.RuntimeMember{
					{ID: fixture.memberIDs[0], Client: clients[0]},
					{ID: fixture.memberIDs[1], Client: clients[1]},
				},
			})
			if err != nil {
				t.Fatalf("New Agent: %v", err)
			}
			var admission *stageworkeragent.FileAssignmentAdmission
			journal := admissionFixture{}
			if test.durable {
				journal = newAdmissionFixture(t)
				journal.config.Validator, err = stageauthority.NewValidator(map[string][]byte{"barrier-key": bytes.Repeat([]byte{0x6b}, 32)}, time.Now)
				if err != nil {
					t.Fatal(err)
				}
				admission = journal.open(t)
			}
			newStream := func() (*stageworkeragent.StreamAgent, error) {
				if admission != nil {
					return stageworkeragent.NewDurableStreamAgent(stageworkeragent.DurableStreamConfig{Runtime: runtimeAgent, Control: control, Admission: admission})
				}
				return stageworkeragent.NewStreamAgent(runtimeAgent, control)
			}
			agent, err := newStream()
			if err != nil {
				t.Fatalf("NewStreamAgent: %v", err)
			}
			execute := func() (stageworkeragent.AssignmentExecutionResult, error) {
				if admission != nil {
					return agent.ExecuteAcquiredAssignment(ctx, fixture.assignment, journal.acquireID)
				}
				return agent.ExecuteAssignment(ctx, fixture.assignment)
			}
			if operation != velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE {
				if _, err := execute(); err != nil {
					t.Fatalf("ExecuteAssignment: %v", err)
				}
			}
			if operation == velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE && !test.renewedActive {
				agent, err = newStream()
				if err != nil {
					t.Fatalf("rebuild StreamAgent: %v", err)
				}
			}
			type operationResult struct {
				accepted bool
				err      error
			}
			operationFinished := make(chan operationResult, 1)
			go func() {
				var err error
				var accepted bool
				switch operation {
				case velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE:
					var result stageworkeragent.AssignmentExecutionResult
					result, err = execute()
					accepted = result.ControlStartAccepted
				case velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE:
					var result *velav1.StageCommandResult
					result, err = agent.Heartbeat(ctx, 1)
					accepted = result.GetDecision() == velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED
				case velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE:
					var result stageworkeragent.ReattachResult
					result, err = agent.Reattach(ctx, stopAuthority, "", nil)
					accepted = result.Accepted
				}
				operationFinished <- operationResult{accepted: accepted, err: err}
			}()
			select {
			case <-control.blocked:
			case <-ctx.Done():
				t.Fatal("control operation did not reach its blocked response")
			}
			stop := &velav1.StopStage{
				Authority: proto.Clone(stopAuthority).(*velav1.StageAuthority),
				Reason:    velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_AUTHORITY_REVOKED,
			}
			if test.staleStop {
				if test.renewedActive {
					stop.Authority = fixture.authority
				} else {
					stop.Authority.ExecutionNonce[0] ^= 1
				}
			}
			control.commands <- &velav1.StageWorkerControlServiceConnectResponse{
				Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{StopStage: stop},
			}
			close(control.commands)
			commandsFinished := make(chan error, 1)
			go func() {
				if test.directStop {
					_, err := agent.HandleStop(ctx, stop)
					commandsFinished <- err
				} else {
					commandsFinished <- agent.RunControlCommands(ctx)
				}
			}()
			consumed := false
			deadline := time.NewTimer(time.Second)
			select {
			case err := <-commandsFinished:
				consumed = true
				if (err != nil) != test.stopFails {
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
				wantState := velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING
				if test.staleStop || test.stopFails {
					wantState = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_RUNNING
				}
				for memberID, state := range status.States {
					if state != wantState {
						t.Errorf("member %s state before releasing control response = %s, want %s", memberID, state, wantState)
					}
				}
			}
			releaseResponse()
			select {
			case result := <-operationFinished:
				if (result.err == nil) != test.staleStop || result.accepted != test.staleStop {
					t.Errorf("late control response accepted after Stop: accepted=%v error=%v", result.accepted, result.err)
				}
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
			if admission != nil && admissionSnapshot(t, admission).Latest.Phase != stageworkeragent.AssignmentClosed {
				t.Error("late control response reopened durable admission")
			}
		})
	}
}

type stopLivenessControl struct {
	operation        velav1.StageWorkerOperation
	blocked          chan struct{}
	release          <-chan struct{}
	commands         chan *velav1.StageWorkerControlServiceConnectResponse
	renewedAuthority *velav1.StageAuthority
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
	response := commandResultResponse(operation, velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED)
	if operation == control.operation {
		response.GetStageCommandResult().RenewedAuthority = control.renewedAuthority
	}
	return response, nil
}

type failingStopRuntimeClient struct {
	velav1.ModelRuntimeServiceClient
}

func (failingStopRuntimeClient) CancelStage(
	context.Context,
	*velav1.ModelRuntimeServiceCancelStageRequest,
	...grpc.CallOption,
) (*velav1.ModelRuntimeServiceCancelStageResponse, error) {
	return nil, errors.New("injected Runtime cancellation transport error")
}
