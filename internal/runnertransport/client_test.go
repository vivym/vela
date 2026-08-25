package runnertransport

import (
	"context"
	"encoding/json"
	"math"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
)

func TestPrepareUsesPrivateUnixSocketAndCarriesImmutableExecutionAuthority(t *testing.T) {
	identity := AttemptIdentity{
		AttemptID:   uuid.MustParse("10000000-0000-0000-0000-000000000001"),
		JobID:       uuid.MustParse("20000000-0000-0000-0000-000000000002"),
		WorkerID:    uuid.MustParse("30000000-0000-0000-0000-000000000003"),
		WorkerEpoch: 7,
		LeaseFence:  4,
	}
	spec := ExecutionSpec{
		ModelRevisionID:            uuid.MustParse("40000000-0000-0000-0000-000000000004"),
		GenerationPresetRevisionID: uuid.MustParse("50000000-0000-0000-0000-000000000005"),
		ExecutionProfileRevisionID: uuid.MustParse("60000000-0000-0000-0000-000000000006"),
		OutputSpecID:               uuid.MustParse("70000000-0000-0000-0000-000000000007"),
		RequestContent:             json.RawMessage(`{"prompt":"private customer request"}`),
	}
	server := &recordingRunnerServer{prepareResponse: &velav1.PrepareResponse{
		Identity:          protoIdentity(identity),
		Decision:          velav1.RunnerCommandDecision_RUNNER_COMMAND_DECISION_ACCEPTED,
		ResumedLocalState: true,
		Detail:            "ready",
	}}
	client := startRunnerClient(t, server)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result, err := client.Prepare(ctx, identity, spec, true)
	if err != nil {
		t.Fatalf("Prepare: %v", err)
	}
	if result.Decision != CommandAccepted || !result.ResumedLocalState || result.Detail != "ready" {
		t.Fatalf("Prepare result = %#v", result)
	}
	request := server.prepareRequest
	if request == nil || request.GetIdentity().GetAttemptId() != identity.AttemptID.String() ||
		request.GetIdentity().GetJobId() != identity.JobID.String() ||
		request.GetIdentity().GetWorkerId() != identity.WorkerID.String() ||
		request.GetIdentity().GetWorkerEpoch() != identity.WorkerEpoch ||
		request.GetIdentity().GetLeaseFence() != identity.LeaseFence ||
		request.GetExecutionSpec().GetModelRevisionId() != spec.ModelRevisionID.String() ||
		request.GetExecutionSpec().GetGenerationPresetRevisionId() != spec.GenerationPresetRevisionID.String() ||
		request.GetExecutionSpec().GetExecutionProfileRevisionId() != spec.ExecutionProfileRevisionID.String() ||
		request.GetExecutionSpec().GetOutputSpecId() != spec.OutputSpecID.String() ||
		string(request.GetExecutionSpec().GetRequestContentJson()) != string(spec.RequestContent) ||
		!request.GetSameAuthorityLocalRecovery() {
		t.Fatalf("Prepare request = %#v", request)
	}
}

func TestStartRequiresRunnerToEchoTheExactAttemptAuthority(t *testing.T) {
	identity := AttemptIdentity{
		AttemptID:   uuid.MustParse("81000000-0000-0000-0000-000000000001"),
		JobID:       uuid.MustParse("82000000-0000-0000-0000-000000000002"),
		WorkerID:    uuid.MustParse("83000000-0000-0000-0000-000000000003"),
		WorkerEpoch: 9,
		LeaseFence:  6,
	}
	server := &recordingRunnerServer{startResponse: &velav1.StartResponse{
		Identity: protoIdentity(identity),
		Decision: velav1.RunnerCommandDecision_RUNNER_COMMAND_DECISION_ACCEPTED,
		Detail:   "running",
	}}
	client := startRunnerClient(t, server)

	result, err := client.Start(context.Background(), identity)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if result.Decision != CommandAccepted || result.Detail != "running" {
		t.Fatalf("Start result = %#v", result)
	}
	if server.startRequest == nil ||
		server.startRequest.GetIdentity().GetAttemptId() != identity.AttemptID.String() ||
		server.startRequest.GetIdentity().GetLeaseFence() != identity.LeaseFence {
		t.Fatalf("Start request = %#v", server.startRequest)
	}

	server.startResponse.Identity.LeaseFence++
	if _, err := client.Start(context.Background(), identity); err == nil {
		t.Fatal("Start accepted a response for a different Lease fence")
	}
}

func TestCancelCarriesABoundedReasonForTheExactAttemptAuthority(t *testing.T) {
	identity := AttemptIdentity{
		AttemptID:   uuid.MustParse("91000000-0000-0000-0000-000000000001"),
		JobID:       uuid.MustParse("92000000-0000-0000-0000-000000000002"),
		WorkerID:    uuid.MustParse("93000000-0000-0000-0000-000000000003"),
		WorkerEpoch: 11,
		LeaseFence:  8,
	}
	server := &recordingRunnerServer{cancelResponse: &velav1.CancelResponse{
		Identity: protoIdentity(identity),
		Decision: velav1.RunnerCommandDecision_RUNNER_COMMAND_DECISION_ACCEPTED,
		Detail:   "stopped",
	}}
	client := startRunnerClient(t, server)

	result, err := client.Cancel(context.Background(), identity, CancelLeaseDeadline)
	if err != nil {
		t.Fatalf("Cancel: %v", err)
	}
	if result.Decision != CommandAccepted || result.Detail != "stopped" {
		t.Fatalf("Cancel result = %#v", result)
	}
	if server.cancelRequest == nil ||
		server.cancelRequest.GetReason() != velav1.RunnerCancelReason_RUNNER_CANCEL_REASON_LEASE_DEADLINE ||
		server.cancelRequest.GetIdentity().GetAttemptId() != identity.AttemptID.String() {
		t.Fatalf("Cancel request = %#v", server.cancelRequest)
	}
	if _, err := client.Cancel(context.Background(), identity, CancelReason("free-form")); err == nil {
		t.Fatal("Cancel accepted an unrecognized cancellation reason")
	}
}

func TestStatusReturnsBoundedBackendNeutralProgressForHeartbeat(t *testing.T) {
	identity := AttemptIdentity{
		AttemptID:   uuid.MustParse("a1000000-0000-0000-0000-000000000001"),
		JobID:       uuid.MustParse("a2000000-0000-0000-0000-000000000002"),
		WorkerID:    uuid.MustParse("a3000000-0000-0000-0000-000000000003"),
		WorkerEpoch: 13,
		LeaseFence:  10,
	}
	progress := 0.375
	remaining := int64(420)
	server := &recordingRunnerServer{statusResponse: &velav1.StatusResponse{
		Identity:                  protoIdentity(identity),
		State:                     velav1.RunnerExecutionState_RUNNER_EXECUTION_STATE_RUNNING,
		Sequence:                  12,
		BackendStage:              "dit",
		BackendStageProgress:      &progress,
		EstimatedRemainingSeconds: &remaining,
		GpuHealthJson:             []byte(`{"healthy":true}`),
		LocalArtifactStateJson:    []byte(`{"encoder":"ready"}`),
	}}
	client := startRunnerClient(t, server)

	status, err := client.Status(context.Background(), identity)
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.State != ExecutionRunning || status.Sequence != 12 || status.BackendStage != "dit" ||
		status.BackendStageProgress == nil || *status.BackendStageProgress != progress ||
		status.EstimatedRemainingSeconds == nil || *status.EstimatedRemainingSeconds != remaining ||
		string(status.GPUHealth) != `{"healthy":true}` ||
		string(status.LocalArtifactState) != `{"encoder":"ready"}` || status.Failure != nil {
		t.Fatalf("Status result = %#v", status)
	}
	if server.statusRequest == nil ||
		server.statusRequest.GetIdentity().GetLeaseFence() != identity.LeaseFence {
		t.Fatalf("Status request = %#v", server.statusRequest)
	}
}

func TestStatusRejectsProgressOutsideTheControlPlaneContract(t *testing.T) {
	identity := AttemptIdentity{
		AttemptID:   uuid.MustParse("a1000000-0000-0000-0000-000000000011"),
		JobID:       uuid.MustParse("a2000000-0000-0000-0000-000000000012"),
		WorkerID:    uuid.MustParse("a3000000-0000-0000-0000-000000000013"),
		WorkerEpoch: 13,
		LeaseFence:  10,
	}
	terminalProgress := 1.0
	tooLong := int64(math.MaxInt64/int64(time.Second) + 1)
	tests := []struct {
		name      string
		progress  *float64
		remaining *int64
	}{
		{name: "terminal stage progress", progress: &terminalProgress},
		{
			name:      "remaining time over duration limit",
			remaining: &tooLong,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := &recordingRunnerServer{statusResponse: &velav1.StatusResponse{
				Identity:                  protoIdentity(identity),
				State:                     velav1.RunnerExecutionState_RUNNER_EXECUTION_STATE_RUNNING,
				Sequence:                  12,
				BackendStage:              "dit",
				BackendStageProgress:      test.progress,
				EstimatedRemainingSeconds: test.remaining,
				GpuHealthJson:             []byte(`{"healthy":true}`),
				LocalArtifactStateJson:    []byte(`{"encoder":"ready"}`),
			}}
			client := startRunnerClient(t, server)

			if _, err := client.Status(context.Background(), identity); err == nil {
				t.Fatal("Status accepted progress outside the control-plane contract")
			}
		})
	}
}

func TestCollectOutputsReturnsValidatedImmutableLocalReceipts(t *testing.T) {
	identity := AttemptIdentity{
		AttemptID:   uuid.MustParse("b1000000-0000-0000-0000-000000000001"),
		JobID:       uuid.MustParse("b2000000-0000-0000-0000-000000000002"),
		WorkerID:    uuid.MustParse("b3000000-0000-0000-0000-000000000003"),
		WorkerEpoch: 15,
		LeaseFence:  12,
	}
	digest := make([]byte, 32)
	digest[0] = 0x5a
	server := &recordingRunnerServer{collectResponse: &velav1.CollectOutputsResponse{
		Identity: protoIdentity(identity),
		Decision: velav1.RunnerCommandDecision_RUNNER_COMMAND_DECISION_ACCEPTED,
		Outputs: []*velav1.RunnerOutput{{
			Kind: "VIDEO", Ordinal: 0, Path: "/run/vela/outputs/video-0.mp4",
			SizeBytes: 4096, Sha256: digest, ContentType: "video/mp4",
		}},
		Detail: "collected",
	}}
	client := startRunnerClient(t, server)

	result, err := client.CollectOutputs(context.Background(), identity)
	if err != nil {
		t.Fatalf("CollectOutputs: %v", err)
	}
	if result.Decision != CommandAccepted || result.Detail != "collected" || len(result.Outputs) != 1 {
		t.Fatalf("CollectOutputs result = %#v", result)
	}
	output := result.Outputs[0]
	if output.Kind != "VIDEO" || output.Ordinal != 0 ||
		output.Path != "/run/vela/outputs/video-0.mp4" || output.SizeBytes != 4096 ||
		output.SHA256[0] != 0x5a || output.ContentType != "video/mp4" {
		t.Fatalf("collected output = %#v", output)
	}
	if server.collectRequest == nil ||
		server.collectRequest.GetIdentity().GetAttemptId() != identity.AttemptID.String() {
		t.Fatalf("CollectOutputs request = %#v", server.collectRequest)
	}
}

func TestDialRejectsAnUntrustedRunnerSocket(t *testing.T) {
	newShortDirectory := func(t *testing.T) string {
		t.Helper()
		directory, err := os.MkdirTemp("/tmp", "vela-runner-invalid-")
		if err != nil {
			t.Fatalf("create runner test directory: %v", err)
		}
		t.Cleanup(func() { _ = os.RemoveAll(directory) })
		return directory
	}
	t.Run("regular file", func(t *testing.T) {
		path := filepath.Join(newShortDirectory(t), "runner.sock")
		if err := os.WriteFile(path, []byte("not a socket"), 0o600); err != nil {
			t.Fatalf("write regular file: %v", err)
		}
		if _, err := Dial(context.Background(), Config{
			SocketPath: path, ExpectedUID: uint32(os.Geteuid()),
		}); err == nil {
			t.Fatal("Dial accepted a regular file")
		}
	})
	t.Run("unexpected owner", func(t *testing.T) {
		path, closeSocket := listenTestSocket(t, newShortDirectory(t), 0o600)
		defer closeSocket()
		if _, err := Dial(context.Background(), Config{
			SocketPath: path, ExpectedUID: uint32(os.Geteuid() + 1),
		}); err == nil {
			t.Fatal("Dial accepted an unexpected socket owner")
		}
	})
	t.Run("public permissions", func(t *testing.T) {
		path, closeSocket := listenTestSocket(t, newShortDirectory(t), 0o666)
		defer closeSocket()
		if _, err := Dial(context.Background(), Config{
			SocketPath: path, ExpectedUID: uint32(os.Geteuid()),
		}); err == nil {
			t.Fatal("Dial accepted public socket permissions")
		}
	})
	t.Run("writable parent", func(t *testing.T) {
		directory := newShortDirectory(t)
		path, closeSocket := listenTestSocket(t, directory, 0o600)
		defer closeSocket()
		if err := os.Chmod(directory, 0o777); err != nil {
			t.Fatalf("make runner directory untrusted: %v", err)
		}
		if _, err := Dial(context.Background(), Config{
			SocketPath: path, ExpectedUID: uint32(os.Geteuid()),
		}); err == nil {
			t.Fatal("Dial accepted a writable non-sticky parent")
		}
	})
}

type recordingRunnerServer struct {
	velav1.UnimplementedRunnerServiceServer
	prepareRequest  *velav1.PrepareRequest
	prepareResponse *velav1.PrepareResponse
	startRequest    *velav1.StartRequest
	startResponse   *velav1.StartResponse
	cancelRequest   *velav1.CancelRequest
	cancelResponse  *velav1.CancelResponse
	statusRequest   *velav1.StatusRequest
	statusResponse  *velav1.StatusResponse
	collectRequest  *velav1.CollectOutputsRequest
	collectResponse *velav1.CollectOutputsResponse
}

func (server *recordingRunnerServer) Start(
	_ context.Context,
	request *velav1.StartRequest,
) (*velav1.StartResponse, error) {
	server.startRequest = request
	return server.startResponse, nil
}

func (server *recordingRunnerServer) Cancel(
	_ context.Context,
	request *velav1.CancelRequest,
) (*velav1.CancelResponse, error) {
	server.cancelRequest = request
	return server.cancelResponse, nil
}

func (server *recordingRunnerServer) Status(
	_ context.Context,
	request *velav1.StatusRequest,
) (*velav1.StatusResponse, error) {
	server.statusRequest = request
	return server.statusResponse, nil
}

func (server *recordingRunnerServer) CollectOutputs(
	_ context.Context,
	request *velav1.CollectOutputsRequest,
) (*velav1.CollectOutputsResponse, error) {
	server.collectRequest = request
	return server.collectResponse, nil
}

func (server *recordingRunnerServer) Prepare(
	_ context.Context,
	request *velav1.PrepareRequest,
) (*velav1.PrepareResponse, error) {
	server.prepareRequest = request
	return server.prepareResponse, nil
}

func startRunnerClient(t *testing.T, server velav1.RunnerServiceServer) *Client {
	t.Helper()
	directory, err := os.MkdirTemp("/tmp", "vela-runner-")
	if err != nil {
		t.Fatalf("create short runner socket directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(directory) })
	socketPath := filepath.Join(directory, "runner.sock")
	listener, err := net.Listen("unix", socketPath)
	if err != nil {
		t.Fatalf("listen on runner socket: %v", err)
	}
	if err := os.Chmod(socketPath, 0o600); err != nil {
		t.Fatalf("chmod runner socket: %v", err)
	}
	grpcServer := grpc.NewServer()
	velav1.RegisterRunnerServiceServer(grpcServer, server)
	serveDone := make(chan error, 1)
	go func() { serveDone <- grpcServer.Serve(listener) }()
	t.Cleanup(func() {
		grpcServer.Stop()
		_ = listener.Close()
		if serveErr := <-serveDone; serveErr != nil && serveErr != grpc.ErrServerStopped {
			t.Errorf("serve runner: %v", serveErr)
		}
	})

	client, err := Dial(context.Background(), Config{
		SocketPath:  socketPath,
		ExpectedUID: uint32(os.Geteuid()),
	})
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func listenTestSocket(t *testing.T, directory string, mode os.FileMode) (string, func()) {
	t.Helper()
	path := filepath.Join(directory, "runner.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on test runner socket: %v", err)
	}
	if err := os.Chmod(path, mode); err != nil {
		_ = listener.Close()
		t.Fatalf("chmod test runner socket: %v", err)
	}
	return path, func() { _ = listener.Close() }
}
