package modelruntime

import (
	"context"
	"errors"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ExecutionInspection is an observation, not durable proof of writer exclusion.
type ExecutionInspection struct {
	Known    bool
	State    velav1.ModelRuntimeExecutionState
	Sequence int64
}

// BackendExecutionInspector must only read the exact envelope's existing state.
// It must not install/renew authority, progress cancellation, or stop a resident
// driver on query timeout. Backends without this capability have no fallback.
type BackendExecutionInspector interface {
	InspectExecution(context.Context, stageauthority.Verified) (ExecutionInspection, error)
}

func (service *Service) InspectExecution(
	ctx context.Context, request *velav1.ModelRuntimeServiceInspectExecutionRequest,
) (*velav1.ModelRuntimeServiceInspectExecutionResponse, error) {
	response := &velav1.ModelRuntimeServiceInspectExecutionResponse{
		SchemaVersion: 1, Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED,
	}
	if service == nil || service.validator == nil || ctx == nil || request == nil ||
		request.GetSchemaVersion() != 1 || len(request.ProtoReflect().GetUnknown()) != 0 {
		response.Detail = "execution inspection request is invalid"
		return response, nil
	}
	response.RuntimeIdentity = runtimeIdentityProto(service.binding)
	verified, err := service.validator.ValidateSignature(request.GetAuthority(), service.binding)
	if err == nil {
		// Historical identity grants no lifetime; enforce the future-issue bound.
		_, err = service.validator.ValidateEnvelopeForReplay(verified.Authority, service.maxClockSkew)
	}
	if err != nil {
		response.Decision = authorityDecision(err)
		response.Detail = boundedDetail(err.Error())
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

	// Do not enter execution admission or requireActive: both participate in
	// execution authority. A floor must permit this read without being changed.
	// Do not hold operationMu across inspection: a slow reader cannot delay
	// cancellation or the watchdog. Backend lookup must also require exact identity.
	inspection := ExecutionInspection{}
	if service.sealedReceipt(verified.Digest) != nil {
		inspection.Known = true
		inspection.State = velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_OUTPUT_SEALED
	} else {
		service.mu.Lock()
		exact := service.active != nil && service.active.verified.Digest == verified.Digest
		service.mu.Unlock()
		if exact {
			inspector, ok := service.backend.(BackendExecutionInspector)
			if !ok {
				response.Detail = "backend does not support read-only execution inspection"
				return response, nil
			}
			inspection, err = inspector.InspectExecution(ctx, verified)
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err == nil {
				err = validateExecutionInspection(inspection)
			}
			if err != nil {
				response.Detail = boundedDetail(err.Error())
				return response, nil
			}
		}
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	response.Decision = velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_ACCEPTED
	response.Known, response.State, response.Sequence = inspection.Known, inspection.State, inspection.Sequence
	response.ObservedAt = timestamppb.New(service.clock.Now())
	response.Detail = "exact execution state is unknown"
	if inspection.Known {
		response.Detail = "execution state observed; writer drain is unproven"
	}
	return response, nil
}

func validateExecutionInspection(inspection ExecutionInspection) error {
	if inspection.Sequence < 0 || (inspection.Known && !validRuntimeState(inspection.State)) ||
		(!inspection.Known && (inspection.State != velav1.ModelRuntimeExecutionState_MODEL_RUNTIME_EXECUTION_STATE_UNSPECIFIED || inspection.Sequence != 0)) {
		return errors.New("backend execution inspection is invalid")
	}
	return nil
}

func (runtime *FakeRuntime) InspectExecution(
	ctx context.Context, authority stageauthority.Verified,
) (ExecutionInspection, error) {
	if err := ctx.Err(); err != nil {
		return ExecutionInspection{}, err
	}
	runtime.mu.Lock()
	defer runtime.mu.Unlock()
	if runtime.activeDigest == ([32]byte{}) || runtime.activeDigest != authority.Digest {
		return ExecutionInspection{}, nil
	}
	return ExecutionInspection{Known: true, State: runtime.state, Sequence: runtime.sequence}, nil
}

func (supervisor *Supervisor) InspectExecution(
	ctx context.Context, request *velav1.ModelRuntimeServiceInspectExecutionRequest,
) (*velav1.ModelRuntimeServiceInspectExecutionResponse, error) {
	if service := supervisor.routeAuthority(request.GetAuthority()); service != nil {
		return service.InspectExecution(ctx, request)
	}
	return &velav1.ModelRuntimeServiceInspectExecutionResponse{
		SchemaVersion: 1, Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE,
		Detail: "StageAuthority does not name a resident runtime",
	}, nil
}
