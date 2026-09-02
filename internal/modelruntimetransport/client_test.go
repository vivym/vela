package modelruntimetransport_test

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

func TestClientUsesPrivateUnixSocketForVersionedModelRuntimeService(t *testing.T) {
	directory := shortRuntimeDirectory(t)
	socketPath := filepath.Join(directory, "runtime.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		t.Fatalf("chmod socket: %v", err)
	}
	server := grpc.NewServer()
	recorder := &recordingRuntimeServer{}
	velav1.RegisterModelRuntimeServiceServer(server, recorder)
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
		if serveErr := <-done; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("serve ModelRuntime: %v", serveErr)
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := modelruntimetransport.Dial(ctx, modelruntimetransport.Config{
		SocketPath: socketPath, ExpectedUID: uint32(os.Geteuid()),
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	response, err := client.PrepareStage(ctx, &velav1.ModelRuntimeServicePrepareStageRequest{
		Authority: &velav1.StageAuthority{StageLeaseId: "lease-through-local-interface"},
	})
	if err != nil || response.GetDetail() != "received" ||
		recorder.request.GetAuthority().GetStageLeaseId() != "lease-through-local-interface" {
		t.Fatalf("PrepareStage = %#v error=%v request=%#v", response, err, recorder.request)
	}
}

func TestClientRejectsUntrustedModelRuntimeSocket(t *testing.T) {
	path := filepath.Join(shortRuntimeDirectory(t), "runtime.sock")
	if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
		t.Fatalf("write regular file: %v", err)
	}
	if _, err := modelruntimetransport.Dial(context.Background(), modelruntimetransport.Config{
		SocketPath: path, ExpectedUID: uint32(os.Geteuid()),
	}); err == nil {
		t.Fatal("Dial accepted a regular file as ModelRuntime socket")
	}
}

type recordingRuntimeServer struct {
	velav1.UnimplementedModelRuntimeServiceServer
	request *velav1.ModelRuntimeServicePrepareStageRequest
}

func (server *recordingRuntimeServer) PrepareStage(
	_ context.Context,
	request *velav1.ModelRuntimeServicePrepareStageRequest,
) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	server.request = request
	return &velav1.ModelRuntimeServicePrepareStageResponse{Detail: "received"}, nil
}

func shortRuntimeDirectory(t *testing.T) string {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "vela-model-runtime-")
	if err != nil {
		t.Fatalf("create runtime directory: %v", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatalf("chmod runtime directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	return directory
}
