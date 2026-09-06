package modelruntime

import (
	"context"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func (service *Service) InspectAllocationExecution(ctx context.Context, request *velav1.ModelRuntimeServiceInspectAllocationExecutionRequest) (*velav1.ModelRuntimeServiceInspectAllocationExecutionResponse, error) {
	response := &velav1.ModelRuntimeServiceInspectAllocationExecutionResponse{
		SchemaVersion: 1, Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED,
	}
	if service == nil || service.validator == nil || ctx == nil || request == nil || request.GetSchemaVersion() != 1 ||
		len(request.ProtoReflect().GetUnknown()) != 0 || proto.Size(request.GetAuthority()) > maxExecutionWireBytes {
		response.Detail = "allocation execution inspection request is invalid"
		return response, nil
	}
	response.RuntimeIdentity = runtimeIdentityProto(service.binding)
	verified, err := service.validator.ValidateSignature(request.GetAuthority(), service.binding)
	if err == nil {
		_, err = service.validator.ValidateEnvelopeForReplay(verified.Authority, service.maxClockSkew)
	}
	if err != nil {
		response.Decision, response.Detail = authorityDecision(err), boundedDetail(err.Error())
		return response, nil
	}
	response.AuthorityDigest = append([]byte(nil), verified.Digest[:]...)
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case <-service.closed:
		response.Detail = "ModelRuntime service is closed"
		return response, nil
	default:
	}
	// Snapshot only active candidates. No operation/admission lock may delay
	// cancellation behind a reader, and this query must never install authority.
	service.mu.Lock()
	active := service.active
	generation := service.generation
	var candidates []stageauthority.Verified
	if active != nil && stageauthority.ValidateSameExecution(verified.Authority, active.verified.Authority) == nil {
		candidates = append(candidates, copyVerified(active.verified))
		if active.backendAuthority != nil && active.backendAuthority.Digest != active.verified.Digest {
			candidates = append(candidates, copyVerified(*active.backendAuthority))
		}
	}
	service.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
		response.Detail = "live allocation execution is unknown"
		return response, nil
	}
	inspector, ok := service.backend.(BackendExecutionInspector)
	if !ok {
		response.Detail = "backend does not support read-only execution inspection"
		return response, nil
	}
	ctx, cancel := context.WithTimeout(ctx, service.cancelTimeout)
	defer cancel()
	var observed *stageauthority.Verified
	var observation ExecutionInspection
	for _, candidate := range candidates {
		read, err := inspector.InspectExecution(ctx, copyVerified(candidate))
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err == nil {
			err = validateExecutionInspection(read)
		}
		if err != nil || read.Known && observed != nil {
			response.Detail = "allocation execution observation is ambiguous or unavailable"
			return response, nil
		}
		if read.Known {
			observed, observation = &candidate, read
		}
	}
	service.mu.Lock()
	unchanged := service.generation == generation && service.active == active && active.verified.Digest == candidates[0].Digest
	if unchanged && len(candidates) == 2 {
		unchanged = active.backendAuthority != nil && active.backendAuthority.Digest == candidates[1].Digest
	}
	service.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !unchanged {
		response.Detail = "allocation execution changed during inspection"
		return response, nil
	}
	response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	response.Detail = "live allocation execution is unknown"
	if observed != nil {
		response.ObservedAuthority = proto.Clone(observed.Authority).(*velav1.StageAuthority)
		response.Inspection = &velav1.ModelRuntimeServiceInspectExecutionResponse{
			SchemaVersion: 1, AuthorityDigest: append([]byte(nil), observed.Digest[:]...),
			RuntimeIdentity: proto.Clone(response.RuntimeIdentity).(*velav1.ModelRuntimeIdentity),
			Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED,
			Known:           true, State: observation.State, Sequence: observation.Sequence, ObservedAt: timestamppb.New(service.clock.Now()),
			Detail: "exact backend state observed; writer drain is unproven",
		}
		response.Detail = "backend envelope observed; exact cancellation and drain remain separate operations"
	}
	return response, nil
}

func (supervisor *Supervisor) InspectAllocationExecution(ctx context.Context, request *velav1.ModelRuntimeServiceInspectAllocationExecutionRequest) (*velav1.ModelRuntimeServiceInspectAllocationExecutionResponse, error) {
	if service := supervisor.routeAuthority(request.GetAuthority()); service != nil {
		return service.InspectAllocationExecution(ctx, request)
	}
	return &velav1.ModelRuntimeServiceInspectAllocationExecutionResponse{
		SchemaVersion: 1, Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE,
		Detail: "StageAuthority does not name a resident runtime",
	}, nil
}
