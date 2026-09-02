package modelruntime

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sort"
	"sync"

	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/protobuf/proto"
)

type Supervisor struct {
	velav1.UnimplementedModelRuntimeServiceServer

	services    []*Service
	identities  []*velav1.ModelRuntimeIdentity
	routes      map[runtimeRoute]*Service
	admissionMu sync.Mutex
}

type runtimeRoute struct {
	modelResidencyID       string
	runtimeIdentity        string
	modelRuntimeEpoch      int64
	stageProfileRevisionID string
}

func NewSupervisor(services ...*Service) (*Supervisor, error) {
	if len(services) == 0 || len(services) > maxLaunchRuntimes {
		return nil, errors.New("ModelRuntime supervisor service set is invalid")
	}
	ordered := append([]*Service(nil), services...)
	for _, service := range ordered {
		if service == nil {
			return nil, errors.New("ModelRuntime supervisor contains no service")
		}
	}
	sort.Slice(ordered, func(left, right int) bool {
		return ordered[left].binding.ModelRuntimeIdentity < ordered[right].binding.ModelRuntimeIdentity
	})
	baseline := ordered[0].binding
	supervisor := &Supervisor{
		services: ordered,
		routes:   make(map[runtimeRoute]*Service, len(ordered)),
	}
	for _, service := range ordered {
		if !sameWorkerMemberTopology(service.binding, baseline) {
			return nil, errors.New("ModelRuntime supervisor services do not share one WorkerInstance member")
		}
		route := routeForBinding(service.binding)
		if _, duplicate := supervisor.routes[route]; duplicate {
			return nil, errors.New("ModelRuntime supervisor route is duplicated")
		}
		supervisor.routes[route] = service
		supervisor.identities = append(supervisor.identities, runtimeIdentityProto(service.binding))
	}
	return supervisor, nil
}

func (supervisor *Supervisor) Close() {
	_ = supervisor.Shutdown()
}

func (supervisor *Supervisor) Shutdown() error {
	if supervisor == nil {
		return nil
	}
	var shutdownErr error
	for _, service := range supervisor.services {
		shutdownErr = errors.Join(shutdownErr, service.Shutdown())
	}
	return shutdownErr
}

func (supervisor *Supervisor) DiscoverRuntimeIdentities(
	_ context.Context,
	request *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest,
) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error) {
	response := &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse{}
	if supervisor == nil || len(supervisor.identities) == 0 || request == nil {
		response.Detail = "ModelRuntime identity discovery request is invalid"
		return response, nil
	}
	baseline := supervisor.identities[0]
	if request.GetWorkerInstanceId() != baseline.GetWorkerInstanceId() ||
		request.GetWorkerInstanceEpoch() != baseline.GetWorkerInstanceEpoch() ||
		request.GetWorkerMemberId() != baseline.GetWorkerMemberId() ||
		request.GetWorkerMemberEpoch() != baseline.GetWorkerMemberEpoch() {
		response.Detail = "resident runtimes do not match the requested WorkerInstance member"
		return response, nil
	}
	for _, identity := range supervisor.identities {
		response.Identities = append(
			response.Identities,
			proto.Clone(identity).(*velav1.ModelRuntimeIdentity),
		)
	}
	response.Detail = "resident runtime identities discovered"
	return response, nil
}

func (supervisor *Supervisor) ProbeReadiness(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceProbeReadinessRequest,
) (*velav1.ModelRuntimeServiceProbeReadinessResponse, error) {
	if service := supervisor.routeIdentity(request.GetIdentity()); service != nil {
		return service.ProbeReadiness(ctx, request)
	}
	response := &velav1.ModelRuntimeServiceProbeReadinessResponse{}
	if request != nil {
		response.Check = request.GetCheck()
	}
	response.Detail = "resident runtime identity is missing, ambiguous, or unknown"
	return response, nil
}

func (supervisor *Supervisor) PrepareStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServicePrepareStageRequest,
) (*velav1.ModelRuntimeServicePrepareStageResponse, error) {
	service := supervisor.routeAuthority(request.GetAuthority())
	if service == nil {
		return &velav1.ModelRuntimeServicePrepareStageResponse{
			Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE,
			Detail:   "StageAuthority does not name a resident runtime",
		}, nil
	}
	supervisor.admissionMu.Lock()
	defer supervisor.admissionMu.Unlock()
	for _, resident := range supervisor.services {
		if resident != service && resident.hasActiveExecution() {
			return &velav1.ModelRuntimeServicePrepareStageResponse{
				RuntimeIdentity: runtimeIdentityProto(service.binding),
				Decision:        velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_REJECTED,
				Detail:          "WorkerInstance member shared slot is held by another resident runtime",
			}, nil
		}
	}
	return service.PrepareStage(ctx, request)
}

func (supervisor *Supervisor) StartStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceStartStageRequest,
) (*velav1.ModelRuntimeServiceStartStageResponse, error) {
	if service := supervisor.routeAuthority(request.GetAuthority()); service != nil {
		return service.StartStage(ctx, request)
	}
	return &velav1.ModelRuntimeServiceStartStageResponse{
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE,
		Detail:   "StageAuthority does not name a resident runtime",
	}, nil
}

func (supervisor *Supervisor) CancelStage(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceCancelStageRequest,
) (*velav1.ModelRuntimeServiceCancelStageResponse, error) {
	if service := supervisor.routeAuthority(request.GetAuthority()); service != nil {
		return service.CancelStage(ctx, request)
	}
	return &velav1.ModelRuntimeServiceCancelStageResponse{
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE,
		Detail:   "StageAuthority does not name a resident runtime",
	}, nil
}

func (supervisor *Supervisor) Status(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceStatusRequest,
) (*velav1.ModelRuntimeServiceStatusResponse, error) {
	if service := supervisor.routeAuthority(request.GetAuthority()); service != nil {
		return service.Status(ctx, request)
	}
	return &velav1.ModelRuntimeServiceStatusResponse{
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE,
		Detail:   "StageAuthority does not name a resident runtime",
	}, nil
}

func (supervisor *Supervisor) SealOutput(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceSealOutputRequest,
) (*velav1.ModelRuntimeServiceSealOutputResponse, error) {
	if service := supervisor.routeAuthority(request.GetAuthority()); service != nil {
		return service.SealOutput(ctx, request)
	}
	return &velav1.ModelRuntimeServiceSealOutputResponse{
		Decision: velav1.ModelRuntimeCommandDecision_MODEL_RUNTIME_COMMAND_DECISION_STALE,
		Detail:   "StageAuthority does not name a resident runtime",
	}, nil
}

func (supervisor *Supervisor) routeIdentity(identity *velav1.ModelRuntimeIdentity) *Service {
	if supervisor == nil || len(supervisor.services) == 0 {
		return nil
	}
	if identity == nil {
		if len(supervisor.services) == 1 {
			return supervisor.services[0]
		}
		return nil
	}
	service := supervisor.routes[runtimeRoute{
		modelResidencyID:       identity.GetModelResidencyId(),
		runtimeIdentity:        identity.GetRuntimeIdentity(),
		modelRuntimeEpoch:      identity.GetModelRuntimeEpoch(),
		stageProfileRevisionID: identity.GetStageProfileRevisionId(),
	}]
	if service == nil || !matchesRuntimeIdentity(identity, service.binding) {
		return nil
	}
	return service
}

func (supervisor *Supervisor) routeAuthority(authority *velav1.StageAuthority) *Service {
	if supervisor == nil || authority == nil {
		return nil
	}
	for _, member := range authority.GetMembers() {
		if member.GetWorkerMemberId() != supervisor.identities[0].GetWorkerMemberId() {
			continue
		}
		return supervisor.routes[runtimeRoute{
			modelResidencyID:       authority.GetModelResidencyId(),
			runtimeIdentity:        authority.GetModelRuntimeIdentity(),
			modelRuntimeEpoch:      member.GetModelRuntimeEpoch(),
			stageProfileRevisionID: authority.GetStageProfileRevisionId(),
		}]
	}
	return nil
}

func routeForBinding(binding stageauthority.RuntimeBinding) runtimeRoute {
	return runtimeRoute{
		modelResidencyID:       binding.ModelResidencyID,
		runtimeIdentity:        binding.ModelRuntimeIdentity,
		modelRuntimeEpoch:      binding.ModelRuntimeEpoch,
		stageProfileRevisionID: binding.StageProfileRevisionID,
	}
}

func sameWorkerMemberTopology(left, right stageauthority.RuntimeBinding) bool {
	return left.WorkerInstanceID == right.WorkerInstanceID &&
		left.WorkerInstanceEpoch == right.WorkerInstanceEpoch &&
		left.WorkerMemberID == right.WorkerMemberID &&
		left.WorkerMemberEpoch == right.WorkerMemberEpoch &&
		bytes.Equal(left.DeviceSetDigest, right.DeviceSetDigest) &&
		bytes.Equal(left.MembershipDigest, right.MembershipDigest) &&
		slices.Equal(left.Devices, right.Devices) && slices.Equal(left.Members, right.Members)
}
