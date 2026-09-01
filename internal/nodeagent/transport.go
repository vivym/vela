package nodeagent

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/vivym/vela/internal/remediation"
	velav1 "github.com/vivym/vela/proto/gen/vela/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	maxIdentityText     = 500
	maxDetailText       = 2000
	maxDeadline         = 24 * time.Hour
	receiptWriteTimeout = 5 * time.Second
)

type NodeAgentIdentity struct {
	NodeIdentity string
	AgentID      uuid.UUID
	AgentEpoch   int64
}

type ControllerIdentity struct {
	SPIFFEIdentity string
	ActorIdentity  string
}

type ControllerIdentityResolver interface {
	ResolveController(context.Context, string) (ControllerIdentity, error)
}

type Executor interface {
	Execute(context.Context, remediation.Plan) (remediation.ExecutionResult, error)
}

// Authorizer binds the transport request to the control-plane operation state.
// Implementations must check the current Worker epoch, Lease fencing, action
// certification, and any device/capability policy before host execution.
type Authorizer interface {
	Authorize(context.Context, Request) error
}

type Receipt struct {
	RequestHash   [sha256.Size]byte
	ActorIdentity string
	Result        Result
}

type Ledger interface {
	Load(context.Context, uuid.UUID) (Receipt, bool, error)
	Save(context.Context, Receipt) error
}

type ExecutionIntent struct {
	OperationID   uuid.UUID
	RequestHash   [sha256.Size]byte
	ActorIdentity string
	StartedAt     time.Time
}

type ExecutionIntentResult struct {
	Acquired  bool
	StartedAt time.Time
	release   func() error
}

func (result ExecutionIntentResult) releaseExecution() error {
	if result.release == nil {
		return nil
	}
	return result.release()
}

type HostLedger interface {
	Ledger
	Begin(context.Context, ExecutionIntent) (ExecutionIntentResult, error)
}

type Request struct {
	OperationID           uuid.UUID
	ExecutionClaimID      uuid.UUID
	WorkerInstanceID      uuid.UUID
	WorkerInstanceEpoch   int64
	DeviceID              uuid.UUID
	DeviceEpoch           int64
	NodeIdentity          string
	DeviceIdentity        string
	FailureClass          string
	ActionLevel           remediation.ActionLevel
	CertificationRevision string
	FailureEvidenceDigest []byte
	DeadlineAt            time.Time
}

type Result struct {
	OperationID   uuid.UUID
	Success       bool
	ResultCode    string
	ResultDetail  string
	PostcheckHash []byte
	StartedAt     time.Time
	FinishedAt    time.Time
}

type Server struct {
	velav1.UnimplementedNodeAgentServiceServer
	localIdentity      NodeAgentIdentity
	controllerResolver ControllerIdentityResolver
	executor           Executor
	ledger             HostLedger
	clock              func() time.Time
	mu                 sync.Mutex
}

func NewServer(
	localIdentity NodeAgentIdentity,
	controllerResolver ControllerIdentityResolver,
	executor Executor,
	ledger HostLedger,
) (*Server, error) {
	if !validIdentity(localIdentity) {
		return nil, errors.New("node Agent local identity is invalid")
	}
	if controllerResolver == nil {
		return nil, errors.New("node Agent controller identity resolver is required")
	}
	if executor == nil {
		return nil, errors.New("node Agent executor is required")
	}
	if ledger == nil {
		return nil, errors.New("node Agent receipt ledger is required")
	}
	return &Server{
		localIdentity:      localIdentity,
		controllerResolver: controllerResolver,
		executor:           executor,
		ledger:             ledger,
		clock:              time.Now,
	}, nil
}

func (server *Server) ExecuteRemediation(
	ctx context.Context,
	request *velav1.ExecuteRemediationRequest,
) (*velav1.ExecuteRemediationResponse, error) {
	if server == nil || !validIdentity(server.localIdentity) || server.controllerResolver == nil ||
		server.executor == nil || server.ledger == nil {
		return nil, status.Error(codes.FailedPrecondition, "node Agent server is not configured")
	}
	if ctx == nil {
		return nil, status.Error(codes.InvalidArgument, "node Agent context is required")
	}
	controller, err := authenticatedController(ctx, server.controllerResolver)
	if err != nil {
		return nil, status.Error(codes.Unauthenticated, "verified control-plane mTLS identity is required")
	}
	parsed, deadline, err := parseRequest(request, server.clock().UTC())
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
	if server.localIdentity.NodeIdentity != parsed.NodeIdentity {
		return nil, status.Error(codes.PermissionDenied, "remediation target does not belong to the local node Agent")
	}
	if parsed.ActionLevel == remediation.ActionL6BMCPowerCycle ||
		parsed.ActionLevel == remediation.ActionL7Quarantine {
		return nil, status.Error(codes.PermissionDenied, "node Agent cannot directly execute privileged remediation")
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	requestHash := hashRequest(parsed)
	prior, found, err := server.ledger.Load(ctx, parsed.OperationID)
	if err != nil {
		return nil, status.Error(codes.Internal, "node Agent receipt lookup failed")
	}
	if found {
		if prior.RequestHash != requestHash || prior.ActorIdentity != controller.ActorIdentity {
			return nil, status.Error(codes.AlreadyExists, "node Agent operation id was reused with different input")
		}
		return responseFromResult(prior.Result), nil
	}
	startedAt := server.clock().UTC()
	intent, err := server.ledger.Begin(ctx, ExecutionIntent{
		OperationID: parsed.OperationID, RequestHash: requestHash,
		ActorIdentity: controller.ActorIdentity, StartedAt: startedAt,
	})
	if err != nil || intent.StartedAt.IsZero() {
		return nil, status.Error(codes.Internal, "node Agent execution intent write failed")
	}
	defer func() { _ = intent.releaseExecution() }()
	startedAt = intent.StartedAt
	if !intent.Acquired {
		// A different Agent process can publish the receipt between the initial
		// lookup and acquisition of the durable execution lock.
		prior, found, loadErr := server.ledger.Load(ctx, parsed.OperationID)
		if loadErr != nil {
			return nil, status.Error(codes.Internal, "node Agent receipt recheck failed")
		}
		if found {
			if prior.RequestHash != requestHash || prior.ActorIdentity != controller.ActorIdentity {
				return nil, status.Error(codes.AlreadyExists, "node Agent operation id was reused with different input")
			}
			return responseFromResult(prior.Result), nil
		}
		result := Result{
			OperationID: parsed.OperationID, Success: false,
			ResultCode:   "EXECUTION_OUTCOME_UNKNOWN",
			ResultDetail: "durable execution intent exists without a terminal receipt",
			StartedAt:    startedAt, FinishedAt: server.clock().UTC(),
		}
		if saveErr := saveReceipt(server.ledger, ctx, Receipt{RequestHash: requestHash, ActorIdentity: controller.ActorIdentity, Result: result}); saveErr != nil {
			return nil, status.Error(codes.Internal, "node Agent interrupted execution receipt write failed")
		}
		return responseFromResult(result), nil
	}
	executionContext, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	executionResult, executionErr := server.executor.Execute(executionContext, remediation.Plan{
		OperationID:           parsed.OperationID,
		ExecutionClaimID:      parsed.ExecutionClaimID,
		WorkerInstanceID:      parsed.WorkerInstanceID,
		WorkerInstanceEpoch:   parsed.WorkerInstanceEpoch,
		DeviceID:              parsed.DeviceID,
		DeviceEpoch:           parsed.DeviceEpoch,
		ActionLevel:           parsed.ActionLevel,
		NodeIdentity:          parsed.NodeIdentity,
		DeviceIdentity:        parsed.DeviceIdentity,
		FailureClass:          parsed.FailureClass,
		DeadlineAt:            parsed.DeadlineAt,
		CertificationRevision: parsed.CertificationRevision,
		FailureEvidenceDigest: append([]byte(nil), parsed.FailureEvidenceDigest...),
	})
	finishedAt := server.clock().UTC()
	if finishedAt.After(deadline) || errors.Is(executionErr, context.DeadlineExceeded) {
		result := Result{
			OperationID: parsed.OperationID, Success: false,
			ResultCode: "DEADLINE_EXCEEDED", ResultDetail: "node Agent remediation exceeded its deadline",
			StartedAt: startedAt, FinishedAt: finishedAt,
		}
		if saveErr := saveReceipt(server.ledger, ctx, Receipt{RequestHash: requestHash, ActorIdentity: controller.ActorIdentity, Result: result}); saveErr != nil {
			return nil, status.Error(codes.Internal, "node Agent receipt write failed")
		}
		return responseFromResult(result), nil
	}
	if executionErr != nil {
		result := Result{
			OperationID: parsed.OperationID, Success: false,
			ResultCode: "EXECUTION_FAILED", ResultDetail: boundedDetail(executionErr.Error()),
			StartedAt: startedAt, FinishedAt: finishedAt,
		}
		if saveErr := saveReceipt(server.ledger, ctx, Receipt{RequestHash: requestHash, ActorIdentity: controller.ActorIdentity, Result: result}); saveErr != nil {
			return nil, status.Error(codes.Internal, "node Agent receipt write failed")
		}
		return responseFromResult(result), nil
	}
	if !executionResult.PostcheckVerified || executionResult.ResultCode == "" || executionResult.PostcheckDigest == [sha256.Size]byte{} {
		resultCode := executionResult.ResultCode
		if resultCode == "" || resultCode == "POSTCHECK_OK" {
			resultCode = "INVALID_POSTCHECK"
		}
		resultDetail := boundedDetail(executionResult.Detail)
		if resultDetail == "node Agent executor failed" {
			resultDetail = "executor returned no certified post-check"
		}
		result := Result{
			OperationID: parsed.OperationID, Success: false,
			ResultCode: resultCode, ResultDetail: resultDetail,
			StartedAt: startedAt, FinishedAt: finishedAt,
		}
		if saveErr := saveReceipt(server.ledger, ctx, Receipt{RequestHash: requestHash, ActorIdentity: controller.ActorIdentity, Result: result}); saveErr != nil {
			return nil, status.Error(codes.Internal, "node Agent receipt write failed")
		}
		return responseFromResult(result), nil
	}
	result := Result{
		OperationID: parsed.OperationID, Success: true,
		ResultCode: executionResult.ResultCode, ResultDetail: boundedDetail(executionResult.Detail),
		PostcheckHash: append([]byte(nil), executionResult.PostcheckDigest[:]...),
		StartedAt:     startedAt, FinishedAt: finishedAt,
	}
	if saveErr := saveReceipt(server.ledger, ctx, Receipt{RequestHash: requestHash, ActorIdentity: controller.ActorIdentity, Result: result}); saveErr != nil {
		return nil, status.Error(codes.Internal, "node Agent receipt write failed")
	}
	return responseFromResult(result), nil
}

type Client struct {
	client        velav1.NodeAgentServiceClient
	actorIdentity string
}

func NewClient(client velav1.NodeAgentServiceClient, actorIdentity string) (*Client, error) {
	if client == nil {
		return nil, errors.New("node Agent gRPC client is required")
	}
	if !validText(actorIdentity, maxIdentityText) {
		return nil, errors.New("control-plane actor identity is invalid")
	}
	return &Client{client: client, actorIdentity: actorIdentity}, nil
}

func (client *Client) Execute(ctx context.Context, request Request) (Result, error) {
	if client == nil || client.client == nil {
		return Result{}, errors.New("node Agent client is not configured")
	}
	if ctx == nil {
		return Result{}, errors.New("node Agent execution context is required")
	}
	if err := validateClientRequest(request); err != nil {
		return Result{}, err
	}
	response, err := client.client.ExecuteRemediation(ctx, &velav1.ExecuteRemediationRequest{
		OperationId: request.OperationID.String(), WorkerInstanceId: request.WorkerInstanceID.String(),
		WorkerInstanceEpoch: request.WorkerInstanceEpoch, NodeIdentity: request.NodeIdentity,
		DeviceIdentity: request.DeviceIdentity, DeviceId: request.DeviceID.String(),
		DeviceEpoch: request.DeviceEpoch, FailureClass: request.FailureClass,
		ActionLevel:           string(request.ActionLevel),
		CertificationRevision: request.CertificationRevision,
		FailureEvidenceDigest: append([]byte(nil), request.FailureEvidenceDigest...),
		DeadlineAt:            timestamppb.New(request.DeadlineAt),
		ExecutionClaimId:      request.ExecutionClaimID.String(),
	})
	if err != nil {
		return Result{}, fmt.Errorf("execute remediation through node Agent: %w", err)
	}
	return parseResponse(response, request.OperationID)
}

func parseRequest(
	request *velav1.ExecuteRemediationRequest,
	now time.Time,
) (Request, time.Time, error) {
	if request == nil {
		return Request{}, time.Time{}, errors.New("node Agent remediation request is required")
	}
	operationID, err := uuid.Parse(request.GetOperationId())
	if err != nil || operationID == uuid.Nil {
		return Request{}, time.Time{}, errors.New("node Agent operation id is invalid")
	}
	claimID, err := uuid.Parse(request.GetExecutionClaimId())
	if err != nil || claimID == uuid.Nil {
		return Request{}, time.Time{}, errors.New("node Agent execution claim id is invalid")
	}
	workerID, err := uuid.Parse(request.GetWorkerInstanceId())
	if err != nil || workerID == uuid.Nil {
		return Request{}, time.Time{}, errors.New("node Agent Worker id is invalid")
	}
	deviceID, err := uuid.Parse(request.GetDeviceId())
	if err != nil || deviceID == uuid.Nil {
		return Request{}, time.Time{}, errors.New("node Agent Device id is invalid")
	}
	deadline, err := parseDeadline(request.GetDeadlineAt(), now)
	if err != nil {
		return Request{}, time.Time{}, err
	}
	parsed := Request{
		OperationID: operationID, ExecutionClaimID: claimID,
		WorkerInstanceID: workerID, WorkerInstanceEpoch: request.GetWorkerInstanceEpoch(),
		DeviceID: deviceID, DeviceEpoch: request.GetDeviceEpoch(),
		NodeIdentity: request.GetNodeIdentity(), DeviceIdentity: request.GetDeviceIdentity(),
		FailureClass:          request.GetFailureClass(),
		ActionLevel:           remediation.ActionLevel(request.GetActionLevel()),
		CertificationRevision: request.GetCertificationRevision(),
		FailureEvidenceDigest: append([]byte(nil), request.GetFailureEvidenceDigest()...),
		DeadlineAt:            deadline,
	}
	if err := validateRequest(parsed); err != nil {
		return Request{}, time.Time{}, err
	}
	return parsed, deadline, nil
}

func parseResponse(response *velav1.ExecuteRemediationResponse, operationID uuid.UUID) (Result, error) {
	if response == nil {
		return Result{}, errors.New("node Agent returned no remediation response")
	}
	returnedID, err := uuid.Parse(response.GetOperationId())
	if err != nil || returnedID != operationID || !validText(response.GetResultCode(), 200) ||
		!validText(response.GetResultDetail(), maxDetailText) {
		return Result{}, errors.New("node Agent remediation response is invalid")
	}
	startedAt, err := validTimestamp(response.GetStartedAt())
	if err != nil {
		return Result{}, err
	}
	finishedAt, err := validTimestamp(response.GetFinishedAt())
	if err != nil || finishedAt.Before(startedAt) {
		return Result{}, errors.New("node Agent remediation response timestamps are invalid")
	}
	postcheck := append([]byte(nil), response.GetPostcheckSha256()...)
	if response.GetSuccess() && len(postcheck) != sha256.Size {
		return Result{}, errors.New("successful node Agent remediation lacks post-check digest")
	}
	if len(postcheck) != 0 && len(postcheck) != sha256.Size {
		return Result{}, errors.New("node Agent post-check digest is invalid")
	}
	return Result{
		OperationID: operationID, Success: response.GetSuccess(), ResultCode: response.GetResultCode(),
		ResultDetail: response.GetResultDetail(), PostcheckHash: postcheck,
		StartedAt: startedAt, FinishedAt: finishedAt,
	}, nil
}

func validateRequest(request Request) error {
	if request.OperationID == uuid.Nil || request.ExecutionClaimID == uuid.Nil ||
		request.WorkerInstanceID == uuid.Nil || request.WorkerInstanceEpoch <= 0 ||
		request.DeviceID == uuid.Nil || request.DeviceEpoch <= 0 ||
		!validText(request.NodeIdentity, maxIdentityText) || !validText(request.DeviceIdentity, maxIdentityText) ||
		!validText(request.FailureClass, 200) ||
		!remediation.IsActionLevel(request.ActionLevel) ||
		(request.ActionLevel != remediation.ActionL7Quarantine && !validText(request.CertificationRevision, 200)) ||
		len(request.FailureEvidenceDigest) != sha256.Size ||
		request.DeadlineAt.IsZero() {
		return errors.New("node Agent remediation request is invalid")
	}
	return nil
}

func validateClientRequest(request Request) error {
	if err := validateRequest(request); err != nil {
		return err
	}
	now := time.Now().UTC()
	if !request.DeadlineAt.After(now) || request.DeadlineAt.After(now.Add(maxDeadline)) {
		return errors.New("node Agent remediation deadline is outside the allowed window")
	}
	if request.ActionLevel == remediation.ActionL6BMCPowerCycle ||
		request.ActionLevel == remediation.ActionL7Quarantine {
		return errors.New("node Agent client cannot directly execute privileged remediation")
	}
	return nil
}

func hashRequest(request Request) [sha256.Size]byte {
	canonical := struct {
		OperationID           string `json:"operation_id"`
		ExecutionClaimID      string `json:"execution_claim_id"`
		WorkerInstanceID      string `json:"worker_instance_id"`
		WorkerInstanceEpoch   int64  `json:"worker_instance_epoch"`
		DeviceID              string `json:"device_id"`
		DeviceEpoch           int64  `json:"device_epoch"`
		NodeIdentity          string `json:"node_identity"`
		DeviceIdentity        string `json:"device_identity"`
		FailureClass          string `json:"failure_class"`
		ActionLevel           string `json:"action_level"`
		CertificationRevision string `json:"certification_revision"`
		FailureEvidenceDigest []byte `json:"failure_evidence_digest"`
		DeadlineUnixNano      int64  `json:"deadline_unix_nano"`
	}{
		OperationID:           request.OperationID.String(),
		ExecutionClaimID:      request.ExecutionClaimID.String(),
		WorkerInstanceID:      request.WorkerInstanceID.String(),
		WorkerInstanceEpoch:   request.WorkerInstanceEpoch,
		DeviceID:              request.DeviceID.String(),
		DeviceEpoch:           request.DeviceEpoch,
		NodeIdentity:          request.NodeIdentity,
		DeviceIdentity:        request.DeviceIdentity,
		FailureClass:          request.FailureClass,
		ActionLevel:           string(request.ActionLevel),
		CertificationRevision: request.CertificationRevision,
		FailureEvidenceDigest: append([]byte(nil), request.FailureEvidenceDigest...),
		DeadlineUnixNano:      request.DeadlineAt.UTC().UnixNano(),
	}
	payload, err := json.Marshal(canonical)
	if err != nil {
		panic(fmt.Sprintf("marshal node Agent request hash: %v", err))
	}
	return sha256.Sum256(payload)
}

func responseFromResult(result Result) *velav1.ExecuteRemediationResponse {
	return &velav1.ExecuteRemediationResponse{
		OperationId:     result.OperationID.String(),
		Success:         result.Success,
		ResultCode:      result.ResultCode,
		ResultDetail:    result.ResultDetail,
		PostcheckSha256: append([]byte(nil), result.PostcheckHash...),
		StartedAt:       timestamppb.New(result.StartedAt),
		FinishedAt:      timestamppb.New(result.FinishedAt),
	}
}

func parseDeadline(timestamp *timestamppb.Timestamp, now time.Time) (time.Time, error) {
	if timestamp == nil || !timestamp.IsValid() {
		return time.Time{}, errors.New("node Agent remediation deadline is invalid")
	}
	deadline := timestamp.AsTime().UTC()
	if !deadline.After(now) || deadline.After(now.Add(maxDeadline)) {
		return time.Time{}, errors.New("node Agent remediation deadline is outside the allowed window")
	}
	return deadline, nil
}

func authenticatedController(ctx context.Context, resolver ControllerIdentityResolver) (ControllerIdentity, error) {
	connectionPeer, ok := peer.FromContext(ctx)
	if !ok || connectionPeer.AuthInfo == nil {
		return ControllerIdentity{}, errors.New("mTLS control-plane peer is absent")
	}
	var tlsInfo credentials.TLSInfo
	switch typed := connectionPeer.AuthInfo.(type) {
	case credentials.TLSInfo:
		tlsInfo = typed
	case *credentials.TLSInfo:
		if typed == nil {
			return ControllerIdentity{}, errors.New("mTLS control-plane peer is absent")
		}
		tlsInfo = *typed
	default:
		return ControllerIdentity{}, errors.New("control-plane peer authentication is not TLS")
	}
	state := tlsInfo.State
	if !state.HandshakeComplete || len(state.PeerCertificates) == 0 || len(state.VerifiedChains) == 0 {
		return ControllerIdentity{}, errors.New("control-plane certificate chain is not verified")
	}
	leaf := state.PeerCertificates[0]
	verifiedLeaf := false
	for _, chain := range state.VerifiedChains {
		if len(chain) != 0 && bytes.Equal(chain[0].Raw, leaf.Raw) {
			verifiedLeaf = true
			break
		}
	}
	if !verifiedLeaf || len(leaf.URIs) != 1 || !validSPIFFEID(leaf.URIs[0]) {
		return ControllerIdentity{}, errors.New("control-plane certificate has no unique SPIFFE ID")
	}
	identity, err := resolver.ResolveController(ctx, leaf.URIs[0].String())
	if err != nil || !validControllerIdentity(identity, leaf.URIs[0].String()) {
		return ControllerIdentity{}, errors.New("control-plane SPIFFE ID is not registered")
	}
	return identity, nil
}

func validIdentity(identity NodeAgentIdentity) bool {
	return identity.AgentID != uuid.Nil && identity.AgentEpoch > 0 &&
		validText(identity.NodeIdentity, maxIdentityText)
}

func validControllerIdentity(identity ControllerIdentity, expectedSPIFFE string) bool {
	return identity.SPIFFEIdentity == expectedSPIFFE && validText(identity.ActorIdentity, maxIdentityText)
}

func validSPIFFEID(identity *url.URL) bool {
	return identity != nil && identity.Scheme == "spiffe" && identity.Host != "" &&
		identity.Path != "" && identity.User == nil && identity.RawQuery == "" && identity.Fragment == ""
}

func validTimestamp(value *timestamppb.Timestamp) (time.Time, error) {
	if value == nil || !value.IsValid() {
		return time.Time{}, errors.New("node Agent timestamp is invalid")
	}
	return value.AsTime().UTC(), nil
}

func validText(value string, max int) bool {
	return value != "" && len(value) <= max && strings.TrimSpace(value) == value &&
		!strings.ContainsRune(value, '\x00')
}

func boundedDetail(value string) string {
	if !validText(value, maxDetailText) {
		if value == "" {
			return "node Agent executor failed"
		}
		value = strings.TrimSpace(strings.ReplaceAll(value, "\x00", ""))
		if value == "" {
			return "node Agent executor failed"
		}
	}
	if len(value) > maxDetailText {
		return value[:maxDetailText]
	}
	return value
}

func saveReceipt(ledger Ledger, ctx context.Context, receipt Receipt) error {
	persistContext, cancel := context.WithTimeout(context.WithoutCancel(ctx), receiptWriteTimeout)
	defer cancel()
	return ledger.Save(persistContext, receipt)
}

var _ velav1.NodeAgentServiceServer = (*Server)(nil)
