package modelruntime

import (
	"context"

	"github.com/vivym/vela/internal/modelruntimetransport"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

func (supervisor *Supervisor) InspectStageAllocationAuthorities(ctx context.Context, request *velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesRequest) (*velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse, error) {
	if request == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		return nil, status.Error(codes.InvalidArgument, "allocation authorities request is invalid")
	}
	if ctx == nil || supervisor == nil || supervisor.floor == nil || supervisor.admission == nil || supervisor.admission.store == nil {
		return nil, status.Error(codes.FailedPrecondition, "durable allocation history is not configured")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	scope := request.GetScope()
	verified, err := modelruntimetransport.ValidateExecutionDrainScope(supervisor.floor.validator, scope, supervisor.services[0].maxClockSkew, true)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "allocation authorities scope is invalid")
	}
	if supervisor.routeIdentity(scope.GetIdentity()) == nil || supervisor.matchRetainedExecutionScope(verified.Authority) != nil {
		return nil, status.Error(codes.FailedPrecondition, "allocation history target or topology is stale")
	}
	history, err := supervisor.InspectRetainedAllocationAuthorities(ctx, verified.Authority)
	if ctx.Err() != nil {
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	if err != nil {
		return nil, status.Error(codes.FailedPrecondition, "allocation history journal requires recovery")
	}
	response := &velav1.ModelRuntimeServiceInspectStageAllocationAuthoritiesResponse{
		SchemaVersion: 1, Identity: proto.Clone(scope.GetIdentity()).(*velav1.ModelRuntimeIdentity),
		AuthorityDigest: append([]byte(nil), verified.Digest[:]...),
		Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
		Detail:          "retained allocation authorities are unknown",
	}
	if history != nil {
		response.Authorities = &velav1.ModelRuntimeRetainedAllocationAuthorities{
			Original: history.Original, Accepted: history.Accepted, Confirmed: history.Confirmed,
		}
		response.Detail = "retained allocation authorities are journal history only"
	}
	if modelruntimetransport.ValidateAllocationAuthoritiesResponse(supervisor.floor.validator, scope, verified.Digest, response) != nil {
		return nil, status.Error(codes.DataLoss, "persisted allocation authorities are invalid")
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return response, nil
}
