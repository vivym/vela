package stageworkertransport_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestClientReplaysStableCommandOnFreshStreamAfterUncertainResponse(t *testing.T) {
	for _, loseResponse := range []bool{true, false} {
		t.Run(map[bool]string{true: "response lost", false: "ACK received but journal failed"}[loseResponse], func(t *testing.T) {
			handler := &materializationResponseLossHandler{loseFirst: loseResponse, accepted: make(chan struct{})}
			server, err := stageworkertransport.NewServer(stageworkertransport.ServerConfig{
				Authenticator: stageworkertransport.AuthenticatorFunc(func(context.Context) (stageworkertransport.Identity, error) {
					return stageworkertransport.Identity{SPIFFEID: "spiffe://vela/worker/member-1"}, nil
				}),
				Handler: handler, StopSource: noStops,
			})
			if err != nil {
				t.Fatal(err)
			}
			counted := &countingStageServer{Server: server}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
				Address: serveStageControl(t, counted), TransportCredentials: insecure.NewCredentials(), InitialControlSessionEpoch: 60,
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = client.Close() })
			request := &velav1.StageWorkerControlServiceConnectRequest{RequestId: "24000000-0000-0000-0000-000000000099",
				Operation: &velav1.StageWorkerControlServiceConnectRequest_CommitStageMaterialization{
					CommitStageMaterialization: &velav1.CommitStageMaterializationRequest{ObjectVersion: "durable-version"},
				}}
			if loseResponse {
				firstContext, cancelFirst := context.WithCancel(ctx)
				defer cancelFirst()
				finished := make(chan error, 1)
				go func() {
					_, err := client.Exchange(firstContext, request)
					finished <- err
				}()
				select {
				case <-handler.accepted:
				case <-ctx.Done():
					t.Fatal(ctx.Err())
				}
				cancelFirst()
				if err := <-finished; !errors.Is(err, context.Canceled) || client.HasActiveControlSession() {
					t.Fatalf("uncertain response kept old stream: %v active=%t", err, client.HasActiveControlSession())
				}
			} else {
				if _, err := client.Exchange(ctx, request); err != nil {
					t.Fatal(err)
				}
				if _, err := client.Exchange(ctx, request); status.Code(err) != codes.AlreadyExists {
					t.Fatalf("same-stream duplicate must retain protocol rejection: %v", err)
				}
			}
			response, err := client.Exchange(ctx, request)
			if err != nil || response.GetRequestId() != request.GetRequestId() ||
				response.GetStageCommandResult().GetDecision() != velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED {
				t.Fatalf("stable replay on new stream failed: %v %v", response, err)
			}
			handler.mu.Lock()
			defer handler.mu.Unlock()
			if counted.ConnectCount() != 2 || len(handler.ids) != 2 || handler.ids[0] != handler.ids[1] ||
				handler.epochs[0] != 60 || handler.epochs[1] != 61 {
				t.Fatalf("replay changed ID or failed to reconnect: connects=%d IDs=%v epochs=%v", counted.ConnectCount(), handler.ids, handler.epochs)
			}
		})
	}
}

type materializationResponseLossHandler struct {
	mu        sync.Mutex
	loseFirst bool
	accepted  chan struct{}
	ids       []string
	epochs    []int64
}

func (handler *materializationResponseLossHandler) Handle(ctx context.Context, _ stageworkertransport.Identity, epoch int64, request *velav1.StageWorkerControlServiceConnectRequest) (*velav1.StageWorkerControlServiceConnectResponse, error) {
	handler.mu.Lock()
	handler.ids = append(handler.ids, request.GetRequestId())
	handler.epochs = append(handler.epochs, epoch)
	first := len(handler.ids) == 1
	handler.mu.Unlock()
	if first {
		close(handler.accepted)
		if handler.loseFirst {
			<-ctx.Done()
			return nil, ctx.Err()
		}
	}
	decision := velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_REPLAYED
	if first {
		decision = velav1.StageWorkerCommandDecision_STAGE_WORKER_COMMAND_DECISION_ACCEPTED
	}
	return &velav1.StageWorkerControlServiceConnectResponse{RequestId: request.GetRequestId(),
		Result: &velav1.StageWorkerControlServiceConnectResponse_StageCommandResult{StageCommandResult: &velav1.StageCommandResult{
			Operation: velav1.StageWorkerOperation_STAGE_WORKER_OPERATION_COMMIT_STAGE_MATERIALIZATION, Decision: decision,
		}}}, nil
}
