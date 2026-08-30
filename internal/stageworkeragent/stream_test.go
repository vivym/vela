package stageworkeragent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/vivym/vela/internal/stageworkeragent"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

func TestStreamAgentStartsControlAuthorityOnlyAfterRuntimeBarrier(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	result, err := streamAgent.ExecuteAssignment(context.Background(), fixture.assignment)
	if err != nil || !result.BarrierPassed || !result.ControlStartAccepted ||
		control.lastRequest.GetStartStage() == nil {
		t.Fatalf("ExecuteAssignment = %#v error=%v request=%#v", result, err, control.lastRequest)
	}
}

func TestStreamAgentCancelsRuntimeWhenControlStartRejects(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_STALE}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	result, err := streamAgent.ExecuteAssignment(context.Background(), fixture.assignment)
	if err == nil || result.ControlStartAccepted || result.CancellationAcknowledgedMembers != 2 {
		t.Fatalf("rejected ExecuteAssignment = %#v error=%v", result, err)
	}
}

func TestStreamAgentReattachesOnlyAfterSameRuntimeConfirmsAuthority(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	if _, err := runtimeAgent.PrepareAndStart(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("PrepareAndStart: %v", err)
	}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	result, err := streamAgent.Reattach(context.Background(), fixture.authority, "", nil)
	if err != nil || !result.Accepted || control.lastRequest.GetReattachStage() == nil {
		t.Fatalf("Reattach = %#v error=%v request=%#v", result, err, control.lastRequest)
	}
	calls := control.calls
	stale := proto.Clone(fixture.authority).(*velav1.StageAuthority)
	stale.Members[0].ModelRuntimeEpoch++
	if _, err := streamAgent.Reattach(context.Background(), stale, "", nil); err == nil {
		t.Fatal("Reattach accepted a stale ModelRuntime epoch")
	}
	if control.calls != calls {
		t.Fatalf("stale reattach reached control stream: calls=%d want=%d", control.calls, calls)
	}
}

func TestStreamAgentReattachRequiresExactLocalReceipt(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	if _, err := runtimeAgent.PrepareAndStart(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("PrepareAndStart: %v", err)
	}
	manifest := []byte(`{"kind":"SHARDED_OUTPUT","path":"/local/output.bin"}`)
	var receipt *velav1.LocalMaterializationReceipt
	for index, backend := range fixture.backends {
		backend.MarkOutputReady(manifest)
		sealed, sealErr := fixture.clients[index].SealOutput(
			context.Background(),
			&velav1.ModelRuntimeServiceSealOutputRequest{Authority: fixture.authority},
		)
		if sealErr != nil || sealed.GetReceipt() == nil {
			t.Fatalf("SealOutput member %d = %#v error=%v", index, sealed, sealErr)
		}
		if receipt == nil {
			receipt = sealed.GetReceipt()
		}
	}
	control := &recordingStreamControl{decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	result, err := streamAgent.Reattach(
		context.Background(),
		fixture.authority,
		receipt.GetReceiptId(),
		receipt.GetManifestSha256(),
	)
	if err != nil || !result.Accepted {
		t.Fatalf("exact receipt Reattach = %#v error=%v", result, err)
	}
	calls := control.calls
	wrongDigest := append([]byte(nil), receipt.GetManifestSha256()...)
	wrongDigest[0] ^= 0xff
	if _, err := streamAgent.Reattach(
		context.Background(), fixture.authority, receipt.GetReceiptId(), wrongDigest,
	); err == nil {
		t.Fatal("Reattach accepted a mismatched local receipt")
	}
	if control.calls != calls {
		t.Fatalf("mismatched local receipt reached control: calls=%d want=%d", control.calls, calls)
	}
}

func TestStreamAgentConsumesUnsolicitedStopStage(t *testing.T) {
	fixture := newBarrierFixture(t, false)
	runtimeAgent, err := stageworkeragent.New(stageworkeragent.Config{
		Members: []stageworkeragent.RuntimeMember{
			{ID: fixture.memberIDs[0], Client: fixture.clients[0]},
			{ID: fixture.memberIDs[1], Client: fixture.clients[1]},
		},
	})
	if err != nil {
		t.Fatalf("New Agent: %v", err)
	}
	commands := make(chan *velav1.StageWorkerControlServiceConnectResponse, 1)
	control := &recordingStreamControl{
		decision: velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED,
		commands: commands,
	}
	streamAgent, err := stageworkeragent.NewStreamAgent(runtimeAgent, control)
	if err != nil {
		t.Fatalf("NewStreamAgent: %v", err)
	}
	if _, err := streamAgent.ExecuteAssignment(context.Background(), fixture.assignment); err != nil {
		t.Fatalf("ExecuteAssignment: %v", err)
	}
	commands <- &velav1.StageWorkerControlServiceConnectResponse{
		Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{
			StopStage: &velav1.StopStage{
				Authority: fixture.authority,
				Reason:    velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_AUTHORITY_REVOKED,
			},
		},
	}
	close(commands)
	if err := streamAgent.RunControlCommands(context.Background()); err != nil {
		t.Fatalf("RunControlCommands: %v", err)
	}
	status, err := runtimeAgent.Status(context.Background(), fixture.authority)
	if err != nil {
		t.Fatalf("Status after unsolicited StopStage: %v", err)
	}
	for memberID, state := range status.States {
		if state != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_CANCELING {
			t.Errorf("member %s state = %s, want CANCELING", memberID, state)
		}
	}
}

type recordingStreamControl struct {
	decision    velav1.StageWorkerCommandDecision
	lastRequest *velav1.StageWorkerControlServiceConnectRequest
	calls       int
	err         error
	commands    <-chan *velav1.StageWorkerControlServiceConnectResponse
}

func (control *recordingStreamControl) Commands() <-chan *velav1.StageWorkerControlServiceConnectResponse {
	return control.commands
}

func (control *recordingStreamControl) Exchange(
	_ context.Context,
	request *velav1.StageWorkerControlServiceConnectRequest,
) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	control.calls++
	control.lastRequest = request
	if control.err != nil {
		return nil, control.err
	}
	var operation velav1.StageWorkerOperation
	switch request.GetOperation().(type) {
	case *velav1.StageWorkerControlServiceConnectRequest_StartStage:
		operation = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_START_STAGE
	case *velav1.StageWorkerControlServiceConnectRequest_HeartbeatStage:
		operation = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_HEARTBEAT_STAGE
	case *velav1.StageWorkerControlServiceConnectRequest_ReattachStage:
		operation = velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_REATTACH_STAGE
	default:
		return nil, errors.New("unexpected Stage Worker operation")
	}
	return &velav1.StageWorkerControlServiceConnectResponse{
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{
			StageCommandResult: &velav1.StageCommandResult{
				Decision: control.decision, Operation: operation,
			},
		},
	}, nil
}
