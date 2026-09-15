package fleettransport

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const maximumRuntimeStartupBytes = 64 << 10

type RuntimeStartupService interface {
	ReserveRuntimeStartup(context.Context, fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error)
	LookupRuntimeStartup(context.Context, fleet.WorkerBootstrapLookup) (fleet.RuntimeStartupReservation, error)
}

func (server *Server) authenticateRuntimeStartup(ctx context.Context) (nodeAgentPrincipal, error) {
	principal, err := server.authenticateBootstrap(ctx)
	if err != nil {
		return nodeAgentPrincipal{}, err
	}
	if server.runtimeStartup == nil {
		return nodeAgentPrincipal{}, status.Error(codes.Unimplemented, "Runtime startup reservation is not configured")
	}
	return principal, nil
}

func (server *Server) ReserveRuntimeStartup(ctx context.Context, request *velav1.ReserveRuntimeStartupRequest) (*velav1.ReserveRuntimeStartupResponse, error) {
	principal, err := server.authenticateRuntimeStartup(ctx)
	if err != nil {
		return nil, err
	}
	parsed, err := decodeRuntimeStartupRequest(request, principal)
	if err != nil {
		return nil, invalidRequest("Runtime startup reservation")
	}
	// Scope-check immutable bootstrap history before consuming any permission.
	// The SQL transaction independently repeats current-state and pair checks.
	history, err := server.lookupBootstrap(ctx, parsed.BootstrapRequestID, principal)
	if err != nil {
		return nil, err
	}
	if history.Receipt == nil || history.Abandonment != nil || history.Receipt.RuntimeJournalID != parsed.RuntimeJournalID ||
		!bytes.Equal(history.Receipt.RuntimeScope, parsed.RuntimeScope) {
		return nil, status.Error(codes.FailedPrecondition, "Runtime startup requires its retained journal pair")
	}
	result, err := server.runtimeStartup.ReserveRuntimeStartup(ctx, parsed)
	if err != nil {
		return nil, mapServiceError("reserve Runtime startup", err)
	}
	wire := encodeRuntimeStartupReservation(result)
	checked, err := decodeRuntimeStartupReservation(wire, principal)
	if err != nil || !sameRuntimeStartupRequest(checked.RuntimeStartupRequest, parsed) {
		return nil, invalidAuthoritativeResult("Runtime startup reservation")
	}
	return &velav1.ReserveRuntimeStartupResponse{Reservation: wire, Fresh: result.Fresh}, nil
}

func (server *Server) LookupRuntimeStartup(ctx context.Context, request *velav1.LookupRuntimeStartupRequest) (*velav1.LookupRuntimeStartupResponse, error) {
	principal, err := server.authenticateRuntimeStartup(ctx)
	if err != nil {
		return nil, err
	}
	if !knownBootstrapMessage(request) {
		return nil, invalidRequest("Runtime startup lookup")
	}
	id, err := bootstrapUUID(request.GetRequestId())
	if err != nil {
		return nil, invalidRequest("Runtime startup lookup")
	}
	result, err := server.runtimeStartup.LookupRuntimeStartup(ctx, fleet.WorkerBootstrapLookup{
		RequestID: id, NodeIdentity: principal.NodeIdentity, ActorIdentity: principal.actorIdentity()})
	if err != nil {
		return nil, mapServiceError("lookup Runtime startup", err)
	}
	wire := encodeRuntimeStartupReservation(result)
	checked, err := decodeRuntimeStartupReservation(wire, principal)
	if err != nil || checked.RequestID != id || result.Fresh {
		return nil, invalidAuthoritativeResult("Runtime startup history")
	}
	return &velav1.LookupRuntimeStartupResponse{Reservation: wire}, nil
}

// ReserveRuntimeStartup exposes committed first-use reservation only. The
// authenticated response is not a Node grant, live pidfd or recovery token.
// No application retry or history-to-Fresh conversion occurs in this client.
func (client *BootstrapClient) ReserveRuntimeStartup(ctx context.Context, request fleet.RuntimeStartupRequest) (fleet.RuntimeStartupReservation, error) {
	if !client.configured(ctx) || request.NodeIdentity != client.NodeIdentity() || request.ActorIdentity != client.ActorIdentity() {
		return fleet.RuntimeStartupReservation{}, errors.New("runtime startup client principal is invalid")
	}
	wire := encodeRuntimeStartupRequest(request)
	expected, err := decodeRuntimeStartupRequest(wire, client.principal)
	if err != nil {
		return fleet.RuntimeStartupReservation{}, err
	}
	response, err := client.service.ReserveRuntimeStartup(ctx, wire,
		grpc.MaxCallSendMsgSize(maximumRuntimeStartupBytes), grpc.MaxCallRecvMsgSize(maximumRuntimeStartupBytes))
	if err != nil {
		return fleet.RuntimeStartupReservation{}, err
	}
	if !knownBootstrapMessage(response) {
		return fleet.RuntimeStartupReservation{}, errors.New("runtime startup response is invalid")
	}
	result, err := decodeRuntimeStartupReservation(response.GetReservation(), client.principal)
	if err != nil || !sameRuntimeStartupRequest(result.RuntimeStartupRequest, expected) {
		return fleet.RuntimeStartupReservation{}, errors.New("runtime startup response changed the requested identity")
	}
	result.Fresh = response.GetFresh()
	return result, nil
}

// LookupRuntimeStartup reads a reservation ID, not a bootstrap request ID.
// Its wire response has no Fresh field and never reconstructs permission.
func (client *BootstrapClient) LookupRuntimeStartup(ctx context.Context, requestID uuid.UUID) (fleet.RuntimeStartupReservation, error) {
	if !client.configured(ctx) || requestID == uuid.Nil {
		return fleet.RuntimeStartupReservation{}, errors.New("runtime startup lookup identity is invalid")
	}
	response, err := client.service.LookupRuntimeStartup(ctx, &velav1.LookupRuntimeStartupRequest{RequestId: requestID.String()},
		grpc.MaxCallRecvMsgSize(maximumRuntimeStartupBytes))
	if err != nil {
		return fleet.RuntimeStartupReservation{}, err
	}
	if !knownBootstrapMessage(response) {
		return fleet.RuntimeStartupReservation{}, errors.New("runtime startup history response is invalid")
	}
	result, err := decodeRuntimeStartupReservation(response.GetReservation(), client.principal)
	if err != nil || result.RequestID != requestID {
		return fleet.RuntimeStartupReservation{}, errors.New("runtime startup history changed the requested identity")
	}
	return result, nil
}

func decodeRuntimeStartupRequest(wire *velav1.ReserveRuntimeStartupRequest, principal nodeAgentPrincipal) (fleet.RuntimeStartupRequest, error) {
	invalid := errors.New("runtime startup request is invalid")
	if !knownBootstrapMessage(wire) || proto.Size(wire) > maximumRuntimeStartupBytes ||
		len(wire.GetEpochs()) == 0 || len(wire.GetEpochs()) > 64 || !validBootstrapDigest(wire.GetRuntimeScope()) ||
		!validBootstrapDigest(wire.GetLaunchDigest()) || !validBootstrapDigest(wire.GetOwnerObservationDigest()) {
		return fleet.RuntimeStartupRequest{}, invalid
	}
	id, e1 := bootstrapUUID(wire.GetRequestId())
	bootstrap, e2 := bootstrapUUID(wire.GetBootstrapRequestId())
	journal, e3 := bootstrapUUID(wire.GetRuntimeJournalId())
	incarnation, e4 := bootstrapUUID(wire.GetIncarnationId())
	if errors.Join(e1, e2, e3, e4) != nil || incarnation.Version() != 4 || incarnation.Variant() != uuid.RFC4122 {
		return fleet.RuntimeStartupRequest{}, invalid
	}
	result := fleet.RuntimeStartupRequest{RequestID: id, BootstrapRequestID: bootstrap, RuntimeJournalID: journal,
		IncarnationID: incarnation, NodeIdentity: principal.NodeIdentity, ActorIdentity: principal.actorIdentity(),
		RuntimeScope: bytes.Clone(wire.GetRuntimeScope()), LaunchDigest: bytes.Clone(wire.GetLaunchDigest()),
		OwnerObservationDigest: bytes.Clone(wire.GetOwnerObservationDigest())}
	seen := make(map[uuid.UUID]bool)
	for _, epoch := range wire.GetEpochs() {
		if !knownBootstrapMessage(epoch) || epoch.GetModelRuntimeEpoch() <= 0 || !validText(epoch.GetModelRuntimeIdentity(), 300) {
			return fleet.RuntimeStartupRequest{}, invalid
		}
		residency, e1 := bootstrapUUID(epoch.GetModelResidencyId())
		profile, e2 := bootstrapUUID(epoch.GetStageProfileRevisionId())
		if errors.Join(e1, e2) != nil || seen[residency] {
			return fleet.RuntimeStartupRequest{}, invalid
		}
		seen[residency] = true
		result.Epochs = append(result.Epochs, fleet.RuntimeStartupEpoch{ModelResidencyID: residency,
			StageProfileRevisionID: profile, ModelRuntimeIdentity: epoch.GetModelRuntimeIdentity(), ModelRuntimeEpoch: epoch.GetModelRuntimeEpoch()})
	}
	sortRuntimeStartupEpochs(result.Epochs)
	return result, nil
}

func encodeRuntimeStartupRequest(request fleet.RuntimeStartupRequest) *velav1.ReserveRuntimeStartupRequest {
	wire := &velav1.ReserveRuntimeStartupRequest{RequestId: request.RequestID.String(), BootstrapRequestId: request.BootstrapRequestID.String(),
		RuntimeJournalId: request.RuntimeJournalID.String(), RuntimeScope: bytes.Clone(request.RuntimeScope), IncarnationId: request.IncarnationID.String(),
		LaunchDigest: bytes.Clone(request.LaunchDigest), OwnerObservationDigest: bytes.Clone(request.OwnerObservationDigest)}
	epochs := slices.Clone(request.Epochs)
	sortRuntimeStartupEpochs(epochs)
	for _, epoch := range epochs {
		wire.Epochs = append(wire.Epochs, &velav1.RuntimeStartupEpoch{ModelResidencyId: epoch.ModelResidencyID.String(),
			StageProfileRevisionId: epoch.StageProfileRevisionID.String(), ModelRuntimeIdentity: epoch.ModelRuntimeIdentity, ModelRuntimeEpoch: epoch.ModelRuntimeEpoch})
	}
	return wire
}

func sortRuntimeStartupEpochs(epochs []fleet.RuntimeStartupEpoch) {
	slices.SortFunc(epochs, func(a, b fleet.RuntimeStartupEpoch) int {
		return strings.Compare(a.ModelResidencyID.String(), b.ModelResidencyID.String())
	})
}

func encodeRuntimeStartupReservation(result fleet.RuntimeStartupReservation) *velav1.RuntimeStartupReservation {
	return &velav1.RuntimeStartupReservation{Request: encodeRuntimeStartupRequest(result.RuntimeStartupRequest),
		NodeIdentity: result.NodeIdentity, ActorIdentity: result.ActorIdentity, ReservedAt: timestamppb.New(result.ReservedAt), PolicyAuthorization: bytes.Clone(result.PolicyAuthorization)}
}

func decodeRuntimeStartupReservation(wire *velav1.RuntimeStartupReservation, principal nodeAgentPrincipal) (fleet.RuntimeStartupReservation, error) {
	if !knownBootstrapMessage(wire) || wire.GetNodeIdentity() != principal.NodeIdentity || wire.GetActorIdentity() != principal.actorIdentity() ||
		!validBootstrapTime(wire.GetReservedAt()) {
		return fleet.RuntimeStartupReservation{}, errors.New("runtime startup reservation principal or timestamp is invalid")
	}
	request, err := decodeRuntimeStartupRequest(wire.GetRequest(), principal)
	if err != nil {
		return fleet.RuntimeStartupReservation{}, err
	}
	return fleet.RuntimeStartupReservation{RuntimeStartupRequest: request, ReservedAt: wire.GetReservedAt().AsTime(), PolicyAuthorization: bytes.Clone(wire.GetPolicyAuthorization())}, nil
}

func sameRuntimeStartupRequest(a, b fleet.RuntimeStartupRequest) bool {
	return a.NodeIdentity == b.NodeIdentity && a.ActorIdentity == b.ActorIdentity &&
		proto.Equal(encodeRuntimeStartupRequest(a), encodeRuntimeStartupRequest(b))
}
