package modelruntime

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (supervisor *Supervisor) InstallStageExecutionFloor(
	ctx context.Context, request *velav1.ModelRuntimeServiceInstallStageExecutionFloorRequest,
) (*velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse, error) {
	response := &velav1.ModelRuntimeServiceInstallStageExecutionFloorResponse{
		SchemaVersion: 1, Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED,
	}
	if request == nil || request.GetSchemaVersion() != 1 || request.GetIdentity() == nil ||
		len(request.ProtoReflect().GetUnknown()) != 0 || len(request.GetIdentity().ProtoReflect().GetUnknown()) != 0 {
		response.Detail = "execution floor request is invalid"
		return response, nil
	}
	service := supervisor.routeIdentity(request.GetIdentity())
	if service == nil || !matchesRuntimeIdentity(request.GetIdentity(), service.binding) {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		response.Detail = "execution floor target is not a current resident runtime"
		return response, nil
	}
	response.Identity = runtimeIdentityProto(service.binding)
	if supervisor.floor == nil || supervisor.admission == nil || supervisor.admission.store == nil {
		response.Detail = "durable execution floor is not configured"
		return response, nil
	}
	installation, err := supervisor.InstallExecutionFloor(ctx, request.GetDisposition())
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, status.FromContextError(err).Err()
		}
		if errors.Is(err, ErrExecutionStateRecovery) {
			return nil, status.Error(codes.FailedPrecondition, "execution admission state requires recovery")
		}
		if errors.Is(err, stageauthority.ErrStale) || errors.Is(err, stageauthority.ErrRuntimeMismatch) {
			response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE
		}
		response.Detail = "signed execution floor was not accepted"
		return response, nil
	}
	response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	response.DispositionDigest = append([]byte(nil), installation.DispositionDigest[:]...)
	response.InstalledCutoff, response.Durable = installation.Cutoff, installation.Durable
	return response, nil
}
