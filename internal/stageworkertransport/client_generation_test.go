package stageworkertransport

import (
	"context"
	"testing"
	"testing/synctest"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

func TestCanceledClientReceiveExitsWhileCommandQueueIsFull(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		streamContext, cancelStream := context.WithCancel(ctx)
		stream := &generationTestStream{ctx: streamContext, response: &velav1.StageWorkerControlServiceConnectResponse{
			Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{StopStage: &velav1.StopStage{
				Authority: &velav1.StageAuthority{StageAttemptId: "old-attempt"},
				Reason:    velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_LEASE_EXPIRED,
			}},
		}}
		client := generationTestClient(ctx, stream, cancelStream)
		client.commands <- controlCommand{generation: 1, response: &velav1.StageWorkerControlServiceConnectResponse{}}
		ended := make(chan struct{})
		go func() { defer close(ended); client.receive(stream, 1) }()
		synctest.Wait()
		client.failStream(stream, 1, context.Canceled)
		synctest.Wait()
		select {
		case <-ended:
		default:
			t.Error("canceled receiver is still blocked by the command queue")
		}
		client.mu.Lock()
		client.stream = &generationTestStream{ctx: ctx}
		client.generation = 2
		client.mu.Unlock()
		<-client.commands
		synctest.Wait()
		select {
		case <-client.commands:
			t.Error("canceled receiver delivered its blocked command after reconnect")
		default:
		}
		cancel()
		synctest.Wait()
	})
}

func TestCanceledClientReceiveIgnoresLateResponses(t *testing.T) {
	for _, kind := range []string{"command result", "readiness", "stop"} {
		t.Run(kind, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				streamContext, cancelStream := context.WithCancel(ctx)
				requestID := "24000000-0000-0000-0000-000000000099"
				response := &velav1.StageWorkerControlServiceConnectResponse{RequestId: requestID}
				switch kind {
				case "command result":
					response.Result = &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{StageCommandResult: &velav1.StageCommandResult{}}
				case "readiness":
					response.Result = &velav1.StageWorkerControlServiceConnectResponse_WorkerReadinessDecision{WorkerReadinessDecision: &velav1.WorkerReadinessDecision{ControlSessionEpoch: 40}}
				case "stop":
					response.RequestId = ""
					response.Result = &velav1.StageWorkerControlServiceConnectResponse_StopStage{StopStage: &velav1.StopStage{}}
				}
				gate := make(chan struct{})
				stream := &generationTestStream{ctx: streamContext, gate: gate, response: response}
				client := generationTestClient(ctx, stream, cancelStream)
				epochs := &generationTestEpochs{}
				client.epochSource = epochs
				ended := make(chan struct{})
				go func() { defer close(ended); client.receive(stream, 1) }()
				synctest.Wait()
				client.failStream(stream, 1, context.Canceled)
				waiter := make(chan exchangeResult, 1)
				client.mu.Lock()
				current := &generationTestStream{ctx: ctx}
				client.stream, client.streamEpoch, client.generation = current, 61, 2
				client.pending[requestID] = waiter
				client.mu.Unlock()
				close(gate)
				synctest.Wait()
				select {
				case <-waiter:
					t.Error("old response completed the new connection's request")
				default:
				}
				select {
				case <-client.commands:
					t.Error("old response published a command on the new connection")
				default:
				}
				if len(client.pending) != 1 || client.stream != current || client.streamEpoch != 61 || epochs.observations != 0 {
					t.Errorf("old response changed connection state: epoch=%d observations=%d waiters=%d", client.streamEpoch, epochs.observations, len(client.pending))
				}
				select {
				case <-ended:
				default:
					t.Error("old receiver did not exit")
				}
			})
		})
	}
}

func generationTestClient(ctx context.Context, stream *generationTestStream, cancel context.CancelFunc) *Client {
	return &Client{ctx: ctx, stream: stream, streamCancel: cancel, streamEpoch: 60, generation: 1,
		pending: make(map[string]chan exchangeResult), commands: make(chan controlCommand, 1)}
}

type generationTestEpochs struct{ observations int }

func (*generationTestEpochs) NextControlSessionEpoch(context.Context) (int64, error) { return 61, nil }
func (epochs *generationTestEpochs) ObserveControlSessionEpoch(context.Context, int64) error {
	epochs.observations++
	return nil
}

type generationTestStream struct {
	grpc.ClientStream
	ctx      context.Context
	gate     <-chan struct{}
	response *velav1.StageWorkerControlServiceConnectResponse
}

func (*generationTestStream) Send(*velav1.StageWorkerControlServiceConnectRequest) error { return nil }
func (*generationTestStream) CloseSend() error                                           { return nil }
func (stream *generationTestStream) Context() context.Context                            { return stream.ctx }
func (stream *generationTestStream) Recv() (*velav1.StageWorkerControlServiceConnectResponse, error) {
	if stream.response != nil {
		response := stream.response
		stream.response = nil
		if stream.gate != nil {
			<-stream.gate
		}
		return response, nil
	}
	<-stream.ctx.Done()
	return nil, stream.ctx.Err()
}
