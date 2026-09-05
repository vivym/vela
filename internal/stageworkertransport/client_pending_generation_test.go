package stageworkertransport

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

func TestOldSendFailurePreservesReconnectedRequestWaiter(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	oldContext, cancelOld := context.WithCancel(ctx)
	oldStream := &pendingGenerationStream{
		ctx: oldContext, sendStarted: make(chan struct{}), releaseSend: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(oldStream.releaseSend) }) }
	t.Cleanup(release)
	client := &Client{
		service: &pendingGenerationService{}, ctx: ctx,
		stream: oldStream, streamEpoch: 70, streamCancel: cancelOld, generation: 1,
		pending: make(map[string]chan exchangeResult),
	}
	requestID := "24000000-0000-0000-0000-000000000097"
	finished := make(chan error, 1)
	go func() {
		_, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
			RequestId: requestID,
			Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
				AcquireStage: &velav1.AcquireStageRequest{},
			},
		})
		finished <- err
	}()
	select {
	case <-oldStream.sendStarted:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	client.failStream(oldStream, 1, context.Canceled)
	newStream := &pendingGenerationStream{ctx: ctx}
	newWaiter := make(chan exchangeResult, 1)
	// A replay registers its waiter before acquiring sendMu. Install that
	// state while the old Send still owns sendMu, then let its error arrive.
	client.mu.Lock()
	client.stream, client.streamEpoch, client.generation = newStream, 71, 2
	client.pending[requestID] = newWaiter
	client.mu.Unlock()
	release()
	select {
	case err := <-finished:
		if !errors.Is(err, errDelayedGenerationSend) {
			t.Fatalf("old Exchange error = %v, want delayed Send failure", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if client.stream != newStream || client.generation != 2 || client.pending[requestID] != newWaiter {
		t.Fatal("old Send failure removed the reconnected request's waiter or changed its stream")
	}
}

var errDelayedGenerationSend = errors.New("delayed old-generation Send failure")

type pendingGenerationService struct{}

func (*pendingGenerationService) Connect(
	context.Context,
	...grpc.CallOption,
) (grpc.BidiStreamingClient[
	velav1.StageWorkerControlServiceConnectRequest,
	velav1.StageWorkerControlServiceConnectResponse,
], error) {
	return nil, errors.New("test already installed its control stream")
}

type pendingGenerationStream struct {
	grpc.ClientStream
	ctx         context.Context
	sendStarted chan struct{}
	releaseSend chan struct{}
}

func (stream *pendingGenerationStream) Send(*velav1.StageWorkerControlServiceConnectRequest) error {
	close(stream.sendStarted)
	// The fake can report Send failure after CloseSend, as an outstanding
	// transport call may finish after the replacement stream is installed.
	<-stream.releaseSend
	return errDelayedGenerationSend
}

func (stream *pendingGenerationStream) Recv() (*velav1.StageWorkerControlServiceConnectResponse, error) {
	<-stream.ctx.Done()
	return nil, stream.ctx.Err()
}

func (*pendingGenerationStream) CloseSend() error { return nil }

func (stream *pendingGenerationStream) Context() context.Context { return stream.ctx }
