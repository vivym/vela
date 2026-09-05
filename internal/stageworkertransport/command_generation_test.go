package stageworkertransport_test

import (
	"context"
	"testing"
	"time"

	"github.com/vivym/vela/internal/stageworkertransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestClientDropsQueuedStopFromReplacedGeneration(t *testing.T) {
	for _, oldAttemptID := range []string{"current-attempt", "previous-attempt"} {
		t.Run(oldAttemptID, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			address := serveStageControl(t, &queuedGenerationServer{oldAttemptID: oldAttemptID})
			client, err := stageworkertransport.DialClient(ctx, stageworkertransport.ClientConfig{
				Address: address, TransportCredentials: insecure.NewCredentials(),
				InitialControlSessionEpoch: 70,
			})
			if err != nil {
				t.Fatalf("DialClient: %v", err)
			}
			t.Cleanup(func() { _ = client.Close() })
			exchange := func() error {
				_, err := client.Exchange(ctx, &velav1.StageWorkerControlServiceConnectRequest{
					Operation: &velav1.StageWorkerControlServiceConnectRequest_AcquireStage{
						AcquireStage: &velav1.AcquireStageRequest{},
					},
				})
				return err
			}
			// The server sends Stop before the correlated ACK. Receiving that ACK
			// proves the old Stop has already entered the client's command queue.
			if err := exchange(); err != nil {
				t.Fatalf("queue old-generation Stop: %v", err)
			}
			if err := exchange(); status.Code(err) != codes.Unavailable {
				t.Fatalf("replace old generation: %v, want Unavailable", err)
			}
			if client.HasActiveControlSession() {
				t.Fatal("failed generation remained active")
			}
			if err := exchange(); err != nil {
				t.Fatalf("queue current-generation Stop: %v", err)
			}
			if epoch := client.CurrentControlSessionEpoch(); epoch != 71 {
				t.Fatalf("current control session epoch = %d, want 71", epoch)
			}
			command, err := client.NextCommand(ctx)
			if err != nil {
				t.Fatalf("consume current-generation Stop: %v", err)
			}
			stop := command.GetStopStage()
			if command.GetRequestId() != "" || stop.GetAuthority().GetStageAttemptId() != "current-attempt" ||
				stop.GetReason() != velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_LEASE_EXPIRED {
				t.Fatalf("consumed superseded Stop instead of current-generation Stop: %v", command)
			}
		})
	}
}

type queuedGenerationServer struct {
	velav1.UnimplementedStageWorkerControlServiceServer
	oldAttemptID string
}

func (server *queuedGenerationServer) Connect(
	stream grpc.BidiStreamingServer[
		velav1.StageWorkerControlServiceConnectRequest,
		velav1.StageWorkerControlServiceConnectResponse,
	],
) error {
	request, err := stream.Recv()
	if err != nil {
		return err
	}
	attemptID := "current-attempt"
	reason := velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_LEASE_EXPIRED
	switch request.GetControlSessionEpoch() {
	case 70:
		attemptID = server.oldAttemptID
		reason = velav1.StageWorkerStopReason_STAGE_WORKER_STOP_REASON_AUTHORITY_REVOKED
	case 71:
	default:
		return status.Error(codes.InvalidArgument, "unexpected test control session epoch")
	}
	if err := stream.Send(&velav1.StageWorkerControlServiceConnectResponse{
		Result: &velav1.StageWorkerControlServiceConnectResponse_StopStage{StopStage: &velav1.StopStage{
			Authority: &velav1.StageAuthority{StageAttemptId: attemptID}, Reason: reason,
		}},
	}); err != nil {
		return err
	}
	if err := stream.Send(&velav1.StageWorkerControlServiceConnectResponse{
		RequestId: request.GetRequestId(),
		Result:    &velav1.StageWorkerControlServiceConnectResponse_NoWork{NoWork: &velav1.NoStageWork{}},
	}); err != nil {
		return err
	}
	if request.GetControlSessionEpoch() == 70 {
		if _, err := stream.Recv(); err != nil {
			return err
		}
		return status.Error(codes.Unavailable, "replace generation with a queued Stop")
	}
	<-stream.Context().Done()
	return stream.Context().Err()
}
