package modelruntime

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"sort"
	"sync"

	"github.com/vivym/vela/internal/journalbinding"
	"github.com/vivym/vela/internal/stageauthority"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type Supervisor struct {
	velav1.UnimplementedModelRuntimeServiceServer

	services         []*Service
	identities       []*velav1.ModelRuntimeIdentity
	routes           map[runtimeRoute]*Service
	admission        *executionAdmission
	floor            *executionFloorVerifier
	registryBinding  *velav1.WorkerBootstrapBinding
	registryVerifier *journalbinding.Verifier
}

var supervisorConstructionMu sync.Mutex

type runtimeRoute struct {
	modelResidencyID       string
	runtimeIdentity        string
	modelRuntimeEpoch      int64
	stageProfileRevisionID string
}

func NewSupervisor(services ...*Service) (*Supervisor, error) {
	return newSupervisor(nil, services...)
}

func newSupervisor(floor *ExecutionFloorConfig, services ...*Service) (*Supervisor, error) {
	return newSupervisorWithState(floor, nil, services...)
}

func newSupervisorWithState(floor *ExecutionFloorConfig, opened *executionStateFile, services ...*Service) (*Supervisor, error) {
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
	if floor != nil {
		var err error
		supervisor.floor, err = newExecutionFloorVerifier(*floor, baseline)
		if err != nil {
			return nil, err
		}
	}
	// Construction may race a direct Service call. No used or already attached
	// Service can be moved to a fresh gate and lose its allocation watermark.
	supervisorConstructionMu.Lock()
	defer supervisorConstructionMu.Unlock()
	for _, service := range ordered {
		service.mu.Lock()
		defer service.mu.Unlock()
	}
	for _, service := range ordered {
		if service.admissionUsed || service.supervised {
			return nil, errors.New("ModelRuntime supervisor requires unused, unsupervised services")
		}
	}
	supervisor.admission = newExecutionAdmission(ordered)
	if floor != nil && floor.State != nil {
		store := opened
		if store == nil {
			var err error
			store, err = openExecutionState(*floor.State, supervisor.journalScope())
			if err != nil {
				return nil, err
			}
		} else {
			scope, err := supervisor.journalScope().digest()
			if err != nil || scope != store.state.Scope || store.path != floor.State.Directory {
				return nil, errors.New("ModelRuntime execution journal does not match started services")
			}
			if err := store.check(); err != nil {
				return nil, err
			}
		}
		supervisor.admission.store = store
		supervisor.admission.highest, supervisor.admission.floor = store.state.Highest, store.state.Floor
	} else if opened != nil {
		return nil, errors.New("ModelRuntime execution journal requires configured durable floor")
	}
	for _, service := range ordered {
		service.admission = supervisor.admission
		service.supervised = true
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
	if supervisor.admission != nil {
		supervisor.admission.stopAdmission()
	}
	var shutdownErr error
	for _, service := range supervisor.services {
		shutdownErr = errors.Join(shutdownErr, service.Shutdown())
	}
	if shutdownErr == nil && supervisor.admission != nil {
		shutdownErr = supervisor.admission.finishShutdown()
	}
	return shutdownErr
}

func (supervisor *Supervisor) DiscoverRuntimeIdentities(
	ctx context.Context,
	request *velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesRequest,
) (*velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse, error) {
	response := &velav1.ModelRuntimeServiceDiscoverRuntimeIdentitiesResponse{}
	if supervisor == nil || len(supervisor.identities) == 0 || request == nil || ctx == nil || len(request.ProtoReflect().GetUnknown()) != 0 {
		response.Detail = "ModelRuntime identity discovery request is invalid"
		return response, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	baseline := supervisor.identities[0]
	if request.GetWorkerInstanceId() != baseline.GetWorkerInstanceId() ||
		request.GetWorkerInstanceEpoch() != baseline.GetWorkerInstanceEpoch() ||
		request.GetWorkerMemberId() != baseline.GetWorkerMemberId() ||
		request.GetWorkerMemberEpoch() != baseline.GetWorkerMemberEpoch() {
		response.Detail = "resident runtimes do not match the requested WorkerInstance member"
		return response, nil
	}
	admission := supervisor.admission
	admission.mu.Lock()
	defer admission.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	// Discovery remains available during recovery drain or full history. Those
	// conditions block readiness, but do not invalidate held journal ownership.
	if err := admission.checkStateLocked(); err != nil {
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	if supervisor.registryBinding != nil {
		if admission.store == nil {
			return nil, status.Error(codes.FailedPrecondition, "Registry-bound Runtime journal is unavailable")
		}
		if err := supervisor.registryVerifier.VerifyJournal(supervisor.registryBinding, journalbinding.RuntimeJournal, journalbinding.Journal{
			WorkerInstanceID: baseline.GetWorkerInstanceId(), WorkerInstanceEpoch: baseline.GetWorkerInstanceEpoch(),
			WorkerMemberID: baseline.GetWorkerMemberId(), WorkerMemberEpoch: baseline.GetWorkerMemberEpoch(),
			JournalID: admission.store.state.ID, Scope: admission.store.state.Scope,
		}); err != nil {
			return nil, status.Error(codes.FailedPrecondition, admission.failStateLocked(err).Error())
		}
		response.JournalBinding = proto.Clone(supervisor.registryBinding).(*velav1.WorkerBootstrapBinding)
	}
	for _, identity := range supervisor.identities {
		response.Identities = append(
			response.Identities,
			proto.Clone(identity).(*velav1.ModelRuntimeIdentity),
		)
	}
	response.Detail = "runtime endpoint identities discovered"
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
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
