package modelruntimetransport

import (
	"bytes"
	"context"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

// InstallExecutionFloor validates a durable admission acknowledgement from the
// trusted local Runtime socket. It is not a writer-drain or deletion receipt.
func (client *Client) InstallExecutionFloor(
	ctx context.Context, validator *stageauthority.Validator, identity *velav1.ModelRuntimeIdentity,
	disposition *velav1.StageTerminalDisposition,
) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
	if client == nil || client.ModelRuntimeServiceClient == nil || ctx == nil || validator == nil || identity == nil ||
		len(identity.ProtoReflect().GetUnknown()) != 0 {
		return nil, errors.New("execution floor client is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	verified, err := validator.ValidateTerminalDispositionEnvelope(disposition)
	if err != nil {
		return nil, err
	}
	if identity.GetWorkerInstanceId() != verified.Disposition.GetWorkerInstanceId() ||
		identity.GetWorkerInstanceEpoch() != verified.Disposition.GetWorkerInstanceEpoch() ||
		!bytes.Equal(identity.GetDeviceSetDigest(), verified.Disposition.GetDeviceSetDigest()) ||
		!bytes.Equal(identity.GetMembershipDigest(), verified.Disposition.GetMembershipDigest()) {
		return nil, errors.New("execution floor target does not match signed Worker scope")
	}
	for _, allocation := range verified.Disposition.GetAllocations() {
		found := false
		for _, member := range allocation.GetMembers() {
			found = found || member.GetWorkerMemberId() == identity.GetWorkerMemberId() && member.GetMemberEpoch() == identity.GetWorkerMemberEpoch()
		}
		if !found {
			return nil, errors.New("execution floor target is not a signed Worker member")
		}
	}
	request := &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{
		SchemaVersion: 1, Identity: proto.Clone(identity).(*velav1.ModelRuntimeIdentity), Disposition: verified.Disposition,
	}
	response, err := client.InstallStageExecutionFloor(ctx, request, grpc.MaxCallSendMsgSize(4<<20))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if response == nil || response.GetSchemaVersion() != 1 || len(response.ProtoReflect().GetUnknown()) != 0 ||
		!proto.Equal(response.GetIdentity(), request.GetIdentity()) ||
		response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		!response.GetDurable() || response.GetInstalledCutoff() < verified.Disposition.GetCutoff() ||
		!bytes.Equal(response.GetDispositionDigest(), verified.Digest[:]) {
		return nil, errors.New("execution floor acknowledgement does not match durable installation request")
	}
	return proto.Clone(response).(*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse), nil
}
