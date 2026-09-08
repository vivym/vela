package modelruntime

import (
	"context"

	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type JournalCommandWriter interface {
	Apply(context.Context, JournalCommand) (JournalMutationReceipt, error)
}

type journalWorkerClient struct {
	velav1.ModelRuntimeServiceClient
	writer JournalCommandWriter
}

// NewJournalWorkerClient preserves the existing Worker/Runtime RPC protocol,
// recording Worker-owned restrictions first through its own authenticated Node
// identity. An uncertain journal result stops the RPC; no mutation is retried.
func NewJournalWorkerClient(client velav1.ModelRuntimeServiceClient, writer JournalCommandWriter) (velav1.ModelRuntimeServiceClient, error) {
	if client == nil || writer == nil {
		return nil, ErrJournalCommand
	}
	return &journalWorkerClient{ModelRuntimeServiceClient: client, writer: writer}, nil
}

func (client *journalWorkerClient) InstallStageExecutionFloor(ctx context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest, opts ...grpc.CallOption) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
	if request == nil || request.GetDisposition() == nil {
		return nil, ErrJournalCommand
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(request.GetDisposition())
	if err != nil {
		return nil, err
	}
	if _, err := client.writer.Apply(ctx, JournalCommand{SchemaVersion: 1, Floor: &JournalFloorCommand{Disposition: wire}}); err != nil {
		return nil, err
	}
	return client.ModelRuntimeServiceClient.InstallStageExecutionFloor(ctx, request, opts...)
}

func (client *journalWorkerClient) CheckpointStageNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceCheckpointStageNonAdmissionRequest, opts ...grpc.CallOption) (*velav1.ModelRuntimeServiceCheckpointStageNonAdmissionResponse, error) {
	if request == nil || request.GetScope().GetAuthority() == nil {
		return nil, ErrJournalCommand
	}
	if _, err := client.writer.Apply(ctx, JournalCommand{SchemaVersion: 1, NonAdmission: &JournalAuthorityCommand{Authority: journalAuthorityWire(request.GetScope().GetAuthority())}}); err != nil {
		return nil, err
	}
	return client.ModelRuntimeServiceClient.CheckpointStageNonAdmission(ctx, request, opts...)
}

func (client *journalWorkerClient) CheckpointStageTerminalNonAdmission(ctx context.Context, request *velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionRequest, opts ...grpc.CallOption) (*velav1.ModelRuntimeServiceCheckpointStageTerminalNonAdmissionResponse, error) {
	if request == nil || request.GetScope().GetDisposition() == nil {
		return nil, ErrJournalCommand
	}
	wire, err := proto.MarshalOptions{Deterministic: true}.Marshal(request.GetScope().GetDisposition())
	if err != nil {
		return nil, err
	}
	if _, err := client.writer.Apply(ctx, JournalCommand{SchemaVersion: 1, TerminalNonAdmission: &JournalTerminalNonAdmissionCommand{
		Disposition: wire, Allocation: request.GetScope().GetStageAllocationId()}}); err != nil {
		return nil, err
	}
	return client.ModelRuntimeServiceClient.CheckpointStageTerminalNonAdmission(ctx, request, opts...)
}
