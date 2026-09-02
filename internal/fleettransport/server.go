package fleettransport

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/url"
	"strings"
	"unicode/utf8"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/fleet"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

const (
	maximumActorBytes                 = 500
	maximumIdentityBytes              = 500
	maximumRequestUIDBytes            = 200
	maximumNameBytes                  = 253
	maximumWorkerRegistryPayloadBytes = 1 << 20
	nodeAgentSPIFFEPrefix             = "spiffe://vela.internal/node-agent/"
	nodeAgentActorPrefix              = "node-agent/"
)

type Service interface {
	Apply(context.Context, fleet.ApprovedResidencyPlan) (fleet.ActuationPlan, error)
	Observe(context.Context, fleet.WorkerInstanceEvidence) (fleet.WorkerInstanceDecision, error)
	AuthorizeMutation(
		context.Context,
		fleet.MutationAuthorizationRequest,
	) (fleet.MutationAuthorizationResult, error)
}

type Config struct {
	SPIFFEIdentity         string
	ActorIdentity          string
	NodeAgentRegistrations []NodeAgentRegistration
}

type NodeAgentRegistration struct {
	NodeIdentity   string
	AgentID        uuid.UUID
	SPIFFEIdentity string
}

type workerInstanceObserverPrincipal struct {
	ActorIdentity string
	NodeIdentity  string
}

type nodeAgentPrincipal struct {
	SPIFFEIdentity string
	NodeIdentity   string
	AgentID        uuid.UUID
}

func (principal nodeAgentPrincipal) actorIdentity() string {
	return nodeAgentActorPrefix + strings.TrimPrefix(principal.SPIFFEIdentity, nodeAgentSPIFFEPrefix)
}

type Server struct {
	velav1.UnimplementedFleetMaintenanceServiceServer
	service             Service
	spiffeIdentity      string
	actorIdentity       string
	nodeAgentPrincipals map[string]nodeAgentPrincipal
}

func NewServer(service Service, config Config) (*Server, error) {
	if service == nil {
		return nil, errors.New("fleet maintenance service is required")
	}
	if !validSPIFFEIdentity(config.SPIFFEIdentity) ||
		!validText(config.ActorIdentity, maximumActorBytes) {
		return nil, errors.New("fleet maintenance server identity is invalid")
	}
	principals := make(map[string]nodeAgentPrincipal, len(config.NodeAgentRegistrations))
	for _, registration := range config.NodeAgentRegistrations {
		principal, ok := parseNodeAgentSPIFFEIdentity(registration.SPIFFEIdentity)
		if !ok || principal.NodeIdentity != registration.NodeIdentity ||
			principal.AgentID != registration.AgentID {
			return nil, errors.New("node agent registration is invalid")
		}
		if _, exists := principals[registration.SPIFFEIdentity]; exists {
			return nil, errors.New("node agent registration is duplicated")
		}
		principals[registration.SPIFFEIdentity] = principal
	}
	return &Server{
		service: service, spiffeIdentity: config.SPIFFEIdentity,
		actorIdentity: config.ActorIdentity, nodeAgentPrincipals: principals,
	}, nil
}

func (server *Server) ApplyResidencyPlan(
	ctx context.Context,
	request *velav1.ApplyResidencyPlanRequest,
) (*velav1.ApplyResidencyPlanResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("ResidencyPlan apply")
	}
	var plan fleet.ApprovedResidencyPlan
	if !decodeWorkerRegistryPayload(request.GetApprovedPlanJson(), &plan) {
		return nil, invalidRequest("ResidencyPlan apply")
	}
	result, err := server.service.Apply(ctx, plan)
	if err != nil {
		return nil, mapServiceError("apply approved ResidencyPlan", err)
	}
	if result.PlanRevisionID != plan.ID || result.WorkerInstanceCount <= 0 {
		return nil, invalidAuthoritativeResult("ResidencyPlan apply")
	}
	return &velav1.ApplyResidencyPlanResponse{
		PlanRevisionId:      result.PlanRevisionID.String(),
		WorkerInstanceCount: int32(result.WorkerInstanceCount),
	}, nil
}

func (server *Server) ObserveWorkerInstance(
	ctx context.Context,
	request *velav1.ObserveWorkerInstanceRequest,
) (*velav1.ObserveWorkerInstanceResponse, error) {
	principal, err := server.authenticateWorkerInstanceObserver(ctx)
	if err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("WorkerInstance observation")
	}
	var evidence fleet.WorkerInstanceEvidence
	if !decodeWorkerRegistryPayload(request.GetEvidenceJson(), &evidence) {
		return nil, invalidRequest("WorkerInstance observation")
	}
	if principal.NodeIdentity != "" &&
		!singleNodeWorkerInstanceEvidenceBelongsToNode(evidence, principal.NodeIdentity) {
		return nil, status.Error(
			codes.PermissionDenied,
			"WorkerInstance evidence does not belong to the authenticated Node Agent",
		)
	}
	evidence.ObservedBy = principal.ActorIdentity
	decision, err := server.service.Observe(ctx, evidence)
	if err != nil {
		return nil, mapServiceError("observe WorkerInstance", err)
	}
	if decision.WorkerInstanceID != evidence.WorkerInstanceID || decision.InstanceEpoch <= 0 ||
		decision.ControlSessionEpoch <= 0 || decision.ModelRuntimeEpoch <= 0 {
		return nil, invalidAuthoritativeResult("WorkerInstance observation")
	}
	return &velav1.ObserveWorkerInstanceResponse{
		WorkerInstanceId:    decision.WorkerInstanceID.String(),
		InstanceEpoch:       decision.InstanceEpoch,
		ControlSessionEpoch: decision.ControlSessionEpoch,
		ModelRuntimeEpoch:   decision.ModelRuntimeEpoch,
		Readiness:           string(decision.Readiness),
	}, nil
}

func (server *Server) AuthorizeMutation(
	ctx context.Context,
	request *velav1.AuthorizeMutationRequest,
) (*velav1.AuthorizeMutationResponse, error) {
	if err := server.authenticate(ctx); err != nil {
		return nil, err
	}
	if request == nil {
		return nil, invalidRequest("WorkerInstance Pod mutation authorization")
	}
	operation, operationOK := mutationOperationFromProto(request.GetOperation())
	workerInstanceID, instanceErr := parseUUID(request.GetWorkerInstanceId())
	residencyPlanID, planErr := parseUUID(request.GetResidencyPlanRevisionId())
	workerBundleID, bundleErr := parseUUID(request.GetWorkerBundleId())
	workerMemberID, memberErr := parseUUID(request.GetWorkerMemberId())
	parsed := fleet.MutationAuthorizationRequest{
		RequestUID: request.GetRequestUid(), ActorIdentity: server.actorIdentity,
		Operation: operation, KubernetesUID: request.GetKubernetesUid(),
		Namespace: request.GetNamespace(), Name: request.GetName(),
		WorkerInstanceID:        workerInstanceID,
		WorkerInstanceEpoch:     request.GetWorkerInstanceEpoch(),
		ResidencyPlanRevisionID: residencyPlanID, WorkerBundleID: workerBundleID,
		WorkerMemberID: workerMemberID,
		RequestDigest:  append([]byte(nil), request.GetRequestDigest()...),
	}
	if !operationOK || instanceErr != nil || planErr != nil || bundleErr != nil ||
		memberErr != nil || !validText(parsed.RequestUID, maximumRequestUIDBytes) ||
		!validText(parsed.KubernetesUID, maximumRequestUIDBytes) ||
		!validText(parsed.Namespace, maximumNameBytes) ||
		!validText(parsed.Name, maximumNameBytes) || parsed.WorkerInstanceEpoch <= 0 ||
		len(parsed.RequestDigest) != 32 {
		return nil, invalidRequest("WorkerInstance Pod mutation authorization")
	}
	result, err := server.service.AuthorizeMutation(ctx, parsed)
	if err != nil {
		return nil, mapServiceError("authorize WorkerInstance Pod mutation", err)
	}
	if result.RequestUID != parsed.RequestUID || !result.Authorized {
		return nil, invalidAuthoritativeResult("WorkerInstance Pod mutation authorization")
	}
	return &velav1.AuthorizeMutationResponse{
		RequestUid: result.RequestUID, Replayed: result.Replayed, Authorized: result.Authorized,
	}, nil
}

func decodeWorkerRegistryPayload(encoded []byte, target any) bool {
	if len(encoded) == 0 || len(encoded) > maximumWorkerRegistryPayloadBytes || target == nil {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return false
	}
	return errors.Is(decoder.Decode(&struct{}{}), io.EOF)
}

func (server *Server) authenticate(ctx context.Context) error {
	if server == nil || server.service == nil || !validSPIFFEIdentity(server.spiffeIdentity) ||
		!validText(server.actorIdentity, maximumActorBytes) {
		return status.Error(codes.FailedPrecondition, "Fleet maintenance server is not configured")
	}
	identity, ok := verifiedPeerSPIFFEIdentity(ctx)
	if !ok || identity != server.spiffeIdentity {
		return status.Error(codes.Unauthenticated, "verified Fleet Controller mTLS identity is required")
	}
	return nil
}

func (server *Server) authenticateWorkerInstanceObserver(
	ctx context.Context,
) (workerInstanceObserverPrincipal, error) {
	if server == nil || server.service == nil {
		return workerInstanceObserverPrincipal{}, status.Error(
			codes.FailedPrecondition, "Fleet maintenance server is not configured",
		)
	}
	identity, ok := verifiedPeerSPIFFEIdentity(ctx)
	if !ok {
		return workerInstanceObserverPrincipal{}, status.Error(
			codes.Unauthenticated,
			"verified Fleet Controller or Node Agent mTLS identity is required",
		)
	}
	if identity == server.spiffeIdentity {
		return workerInstanceObserverPrincipal{ActorIdentity: server.actorIdentity}, nil
	}
	nodeAgent, parsed := parseNodeAgentSPIFFEIdentity(identity)
	registered, exists := server.nodeAgentPrincipals[identity]
	if !parsed || !exists || registered != nodeAgent {
		return workerInstanceObserverPrincipal{}, status.Error(
			codes.Unauthenticated,
			"verified Fleet Controller or Node Agent mTLS identity is required",
		)
	}
	return workerInstanceObserverPrincipal{
		ActorIdentity: nodeAgent.actorIdentity(), NodeIdentity: nodeAgent.NodeIdentity,
	}, nil
}

func verifiedPeerSPIFFEIdentity(ctx context.Context) (string, bool) {
	if ctx == nil {
		return "", false
	}
	connectionPeer, ok := peer.FromContext(ctx)
	if !ok || connectionPeer.AuthInfo == nil {
		return "", false
	}
	var tlsInfo credentials.TLSInfo
	switch typed := connectionPeer.AuthInfo.(type) {
	case credentials.TLSInfo:
		tlsInfo = typed
	case *credentials.TLSInfo:
		if typed == nil {
			return "", false
		}
		tlsInfo = *typed
	default:
		return "", false
	}
	state := tlsInfo.State
	if !state.HandshakeComplete || len(state.PeerCertificates) == 0 ||
		len(state.VerifiedChains) == 0 {
		return "", false
	}
	leaf := state.PeerCertificates[0]
	verifiedLeaf := false
	for _, chain := range state.VerifiedChains {
		if len(chain) != 0 && bytes.Equal(chain[0].Raw, leaf.Raw) {
			verifiedLeaf = true
			break
		}
	}
	if !verifiedLeaf || len(leaf.URIs) != 1 || leaf.URIs[0] == nil {
		return "", false
	}
	return leaf.URIs[0].String(), true
}

func parseNodeAgentSPIFFEIdentity(value string) (nodeAgentPrincipal, bool) {
	identity, err := url.Parse(value)
	if err != nil || identity == nil || identity.Scheme != "spiffe" ||
		identity.Host != "vela.internal" || identity.User != nil ||
		identity.RawQuery != "" || identity.Fragment != "" ||
		!strings.HasPrefix(value, nodeAgentSPIFFEPrefix) {
		return nodeAgentPrincipal{}, false
	}
	segments := strings.Split(strings.TrimPrefix(value, nodeAgentSPIFFEPrefix), "/")
	if len(segments) != 2 || segments[0] == "" || segments[1] == "" {
		return nodeAgentPrincipal{}, false
	}
	decodedNode, err := base64.RawURLEncoding.DecodeString(segments[0])
	if err != nil || !utf8.Valid(decodedNode) {
		return nodeAgentPrincipal{}, false
	}
	nodeIdentity := string(decodedNode)
	agentID, err := uuid.Parse(segments[1])
	if err != nil || agentID == uuid.Nil || !validText(nodeIdentity, maximumIdentityBytes) {
		return nodeAgentPrincipal{}, false
	}
	canonical := nodeAgentSPIFFEPrefix +
		base64.RawURLEncoding.EncodeToString(decodedNode) + "/" + agentID.String()
	principal := nodeAgentPrincipal{
		SPIFFEIdentity: canonical, NodeIdentity: nodeIdentity, AgentID: agentID,
	}
	if canonical != value || !validText(principal.actorIdentity(), maximumActorBytes) {
		return nodeAgentPrincipal{}, false
	}
	return principal, true
}

func singleNodeWorkerInstanceEvidenceBelongsToNode(
	evidence fleet.WorkerInstanceEvidence,
	nodeIdentity string,
) bool {
	if !validText(nodeIdentity, maximumIdentityBytes) ||
		len(evidence.DeviceSet.Devices) == 0 || len(evidence.Members) == 0 {
		return false
	}
	computeNodeID := uuid.Nil
	devices := make(map[uuid.UUID]struct{}, len(evidence.DeviceSet.Devices))
	for _, device := range evidence.DeviceSet.Devices {
		if device.ID == uuid.Nil || device.ComputeNodeID == uuid.Nil ||
			device.NodeIdentity != nodeIdentity ||
			computeNodeID != uuid.Nil && device.ComputeNodeID != computeNodeID {
			return false
		}
		computeNodeID = device.ComputeNodeID
		devices[device.ID] = struct{}{}
	}
	for _, member := range evidence.Members {
		if member.ComputeNodeID != computeNodeID || len(member.DeviceIDs) == 0 {
			return false
		}
		for _, deviceID := range member.DeviceIDs {
			if _, ok := devices[deviceID]; !ok {
				return false
			}
		}
	}
	return true
}

func mutationOperationFromProto(
	value velav1.FleetMutationOperation,
) (fleet.MutationOperation, bool) {
	switch value {
	case velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_DELETE:
		return fleet.MutationDelete, true
	case velav1.FleetMutationOperation_FLEET_MUTATION_OPERATION_REMOVE_FINALIZER:
		return fleet.MutationRemoveFinalizer, true
	default:
		return "", false
	}
}

func parseUUID(value string) (uuid.UUID, error) {
	parsed, err := uuid.Parse(value)
	if err != nil || parsed == uuid.Nil {
		return uuid.Nil, errors.New("UUID is invalid")
	}
	return parsed, nil
}

func validSPIFFEIdentity(value string) bool {
	identity, err := url.Parse(value)
	return err == nil && identity != nil && identity.Scheme == "spiffe" &&
		identity.Host != "" && identity.Path != "" && identity.Path != "/" &&
		identity.User == nil && identity.RawQuery == "" && identity.Fragment == "" &&
		identity.String() == value
}

func validText(value string, maximum int) bool {
	return value != "" && len(value) <= maximum && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func invalidRequest(operation string) error {
	return status.Error(codes.InvalidArgument, "Fleet "+operation+" request is invalid")
}

func invalidAuthoritativeResult(name string) error {
	return status.Error(codes.Internal, "authoritative "+name+" result is invalid")
}

func mapServiceError(operation string, err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, operation+" was canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, operation+" exceeded its deadline")
	}
	var failure *fleet.Failure
	if errors.As(err, &failure) {
		switch failure.Code {
		case fleet.FailureInvalid:
			return status.Error(codes.InvalidArgument, operation+" was rejected")
		case fleet.FailureConflict:
			return status.Error(codes.Aborted, operation+" conflicted with current authority")
		case fleet.FailureNotFound:
			return status.Error(codes.NotFound, operation+" target does not exist")
		}
	}
	return status.Error(codes.Unavailable, operation+" is temporarily unavailable")
}
