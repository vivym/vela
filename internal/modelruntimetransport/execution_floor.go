package modelruntimetransport

import (
	"bytes"
	"context"
	"crypto/sha256"
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
	if client == nil || client.ModelRuntimeServiceClient == nil || ctx == nil {
		return nil, errors.New("execution floor client is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	request := &velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest{
		SchemaVersion: 1, Identity: identity, Disposition: disposition,
	}
	verified, err := ValidateExecutionFloorRequest(validator, request)
	if err != nil {
		return nil, err
	}
	request = proto.Clone(request).(*velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest)
	request.Disposition = verified.Disposition
	response, err := client.InstallStageExecutionFloor(ctx, request, grpc.MaxCallSendMsgSize(4<<20))
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := ValidateExecutionFloorAcknowledgement(request.GetIdentity(), verified.Digest, verified.Disposition.GetCutoff(), response); err != nil {
		return nil, err
	}
	return proto.Clone(response).(*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse), nil
}

// ValidateExecutionFloorRequest authenticates a current restriction and its
// target scope. V1 requires resident historical routes at the receiving Runtime;
// V2 restricts a current durable journal owner, independently of writer drain.
func ValidateExecutionFloorRequest(validator *stageauthority.Validator, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest) (stageauthority.VerifiedTerminalDisposition, error) {
	identity := request.GetIdentity()
	if validator == nil || request == nil || (request.GetSchemaVersion() != 1 && request.GetSchemaVersion() != 2) || identity == nil ||
		len(request.ProtoReflect().GetUnknown()) != 0 || len(identity.ProtoReflect().GetUnknown()) != 0 {
		return stageauthority.VerifiedTerminalDisposition{}, errors.New("execution floor request is invalid")
	}
	verified, err := validator.ValidateTerminalDispositionEnvelope(request.GetDisposition())
	if err != nil {
		return stageauthority.VerifiedTerminalDisposition{}, err
	}
	if identity.GetWorkerInstanceId() != verified.Disposition.GetWorkerInstanceId() ||
		identity.GetWorkerInstanceEpoch() != verified.Disposition.GetWorkerInstanceEpoch() ||
		!bytes.Equal(identity.GetDeviceSetDigest(), verified.Disposition.GetDeviceSetDigest()) ||
		!bytes.Equal(identity.GetMembershipDigest(), verified.Disposition.GetMembershipDigest()) {
		return stageauthority.VerifiedTerminalDisposition{}, errors.New("execution floor target does not match signed Worker scope")
	}
	for _, allocation := range verified.Disposition.GetAllocations() {
		found := false
		for _, member := range allocation.GetMembers() {
			found = found || member.GetWorkerMemberId() == identity.GetWorkerMemberId() && member.GetMemberEpoch() == identity.GetWorkerMemberEpoch()
		}
		if !found {
			return stageauthority.VerifiedTerminalDisposition{}, errors.New("execution floor target is not a signed Worker member")
		}
	}
	return verified, nil
}

// ValidateExecutionFloorAcknowledgement binds a response to an independently
// verified request. A valid acknowledgement proves admission exclusion, not drain.
func ValidateExecutionFloorAcknowledgement(identity *velav1.ModelRuntimeIdentity, digest [sha256.Size]byte, cutoff int64, response *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse) error {
	return ValidateExecutionFloorAcknowledgementForVersion(1, identity, digest, cutoff, response)
}

// ValidateExecutionFloorAcknowledgementForVersion prevents cross-version reply
// substitution. Both versions prove only a durable admission restriction.
func ValidateExecutionFloorAcknowledgementForVersion(version uint32, identity *velav1.ModelRuntimeIdentity, digest [sha256.Size]byte, cutoff int64, response *velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse) error {
	if (version != 1 && version != 2) || response == nil || response.GetSchemaVersion() != version || len(response.ProtoReflect().GetUnknown()) != 0 ||
		identity == nil || cutoff <= 0 || !proto.Equal(response.GetIdentity(), identity) ||
		response.GetDecision() != velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED ||
		!response.GetDurable() || response.GetInstalledCutoff() < cutoff ||
		!bytes.Equal(response.GetDispositionDigest(), digest[:]) {
		return errors.New("execution floor acknowledgement does not match durable installation request")
	}
	return nil
}
